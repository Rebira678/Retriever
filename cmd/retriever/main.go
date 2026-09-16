// Package main is the entry point for the Retriever RAG Ingestion Pipeline.
//
// Retriever ingests documents, chunks them using configurable strategies,
// generates embeddings via a concurrent worker pool, stores vectors in
// Postgres (pgvector), and exposes a semantic search API.
//
// Architecture: Pipeline-based — Ingestion → Chunking → Embedding → Storage → Search
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Rebira678/Retriever/internal/chunker"
	"github.com/Rebira678/Retriever/internal/config"
	"github.com/Rebira678/Retriever/internal/embedder"
	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/pipeline"
	"github.com/Rebira678/Retriever/internal/server"
	"github.com/Rebira678/Retriever/internal/storage"
	searchv1 "github.com/Rebira678/Retriever/pkg/api/search/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"net"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	// ─── Structured Logger ───────────────────────────────────────────────
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	slog.Info("🚀 Retriever RAG Ingestion Pipeline starting",
		"version", "0.1.0",
		"day", 36,
	)

	// ─── Load Configuration ─────────────────────────────────────────────
	cfg := config.FromEnv()
	slog.Info("Configuration loaded",
		"chunk_size", cfg.ChunkSize,
		"chunk_overlap", cfg.ChunkOverlap,
		"strategy", cfg.ChunkStrategy,
	)

	// ─── Demo: Chunking a Sample Document ───────────────────────────────
	sampleDoc := `Retrieval-Augmented Generation (RAG) is an AI framework that enhances
large language model responses by incorporating external knowledge retrieval.
Instead of relying solely on trained parameters, RAG systems fetch relevant
documents from a vector database and include them as context in the prompt.

The RAG pipeline typically consists of three phases:
1. Ingestion — documents are loaded, split into chunks, and embedded as vectors.
2. Retrieval — a user query is embedded and compared against stored vectors
   using similarity search (cosine or L2 distance).
3. Generation — the retrieved chunks are appended to the LLM prompt, grounding
   the model's response in factual, up-to-date information.

This architecture solves the "knowledge cutoff" problem inherent in static LLMs
and dramatically reduces hallucination by providing verifiable source material.
RAG is now the dominant pattern for production AI applications that need
accurate, domain-specific answers — from customer support bots to legal
research tools to medical diagnosis assistants.`

	// Create the chunker based on configuration
	c := chunker.New(cfg.ChunkSize, cfg.ChunkOverlap)

	chunks := c.Chunk(sampleDoc)
	slog.Info("Document chunked successfully",
		"input_length", len(sampleDoc),
		"num_chunks", len(chunks),
		"strategy", "fixed_size_overlap",
	)

	for i, chunk := range chunks {
		fmt.Printf("\n─── Chunk %d (len=%d) ───\n%s\n", i+1, len(chunk.Text), chunk.Text)
	}

	// ─── Demo: Concurrent Embedding with Worker Pool ───────────────────────────────
	var emb embedder.Embedder
	dimension := cfg.EmbeddingDimension

	if cfg.GeminiAPIKey != "" {
		slog.Info("Using Gemini Embedder")
		emb = embedder.NewGeminiEmbedder(cfg.GeminiAPIKey, "gemini-embedding-2")
	} else if cfg.OpenAIAPIKey != "" {
		slog.Info("Using OpenAI Embedder")
		emb = embedder.NewOpenAIEmbedder(cfg.OpenAIAPIKey, cfg.EmbeddingAPIURL, cfg.EmbeddingModel)
	}

	if emb != nil {
		pool := pipeline.NewEmbedPool(emb, pipeline.WithWorkers(cfg.WorkerPoolSize))

		// Initialize Database Connection earlier to support Idempotency & DLQ during embedding
		slog.Info("Connecting to PostgreSQL...", "url", cfg.DatabaseURL, "dimension", dimension)
		var store storage.Storage
		store, err := storage.NewPostgresStorage(context.Background(), cfg.DatabaseURL, dimension)
		if err != nil {
			slog.Error("Failed to connect to database", "error", err)
			return
		}
		defer store.Close()

		docHash := fmt.Sprintf("%x", sha256.Sum256([]byte(sampleDoc)))
		docID := "sample-doc-rag"
		
		// Check for Idempotency using StartIngestion (atomic lock)
		acquired, err := store.StartIngestion(context.Background(), docHash, docID)
		if err != nil {
			slog.Error("Failed to check idempotency", "error", err)
			return
		}

		if !acquired {
			slog.Info("Document already ingested or in progress (Idempotency hit), skipping processing", "hash", docHash, "document_id", docID)
		} else {
			// Set up channels with backpressure
			chunkChan := make(chan models.Chunk, len(chunks))
			resultsChan := make(chan pipeline.EmbedResult, len(chunks))

			// Feed chunks into the pipeline
			for _, chunk := range chunks {
				chunk.DocumentID = docID // Link chunk to document ID for DLQ
				chunkChan <- chunk
			}
			close(chunkChan)

			slog.Info("Starting concurrent embedding worker pool...", "workers", cfg.WorkerPoolSize)
			pool.Run(context.Background(), chunkChan, resultsChan)

			var generatedEmbeddings []models.Embedding
			var deadLetters []models.DeadLetter

			for res := range resultsChan {
				if res.Err != nil {
					slog.Error("Failed to embed chunk", "chunk_index", res.Chunk.Index, "error", res.Err)
					deadLetters = append(deadLetters, models.DeadLetter{
						DocumentID: docID,
						ChunkIndex: res.Chunk.Index,
						Error:      res.Err.Error(),
						Payload:    res.Chunk.Text,
					})
					continue
				}
				
				generatedEmbeddings = append(generatedEmbeddings, res.Embedding)
				if len(generatedEmbeddings) == 1 {
					fmt.Printf("\n─── First Chunk Embedding (Sample) ───\n[%f, %f, %f, ...]\n",
						res.Embedding.Vector[0], res.Embedding.Vector[1], res.Embedding.Vector[2])
				}
			}
			slog.Info("Concurrent embedding completed", "total", len(generatedEmbeddings))

			// Process Dead-Letters efficiently in batch
			if len(deadLetters) > 0 {
				if err := store.SaveDeadLetters(context.Background(), deadLetters); err != nil {
					slog.Error("Failed to save to Dead Letter Queue", "error", err)
				} else {
					slog.Info("Saved failed chunks to Dead Letter Queue", "count", len(deadLetters))
				}
			}

			if len(generatedEmbeddings) > 0 {
				if err := store.SaveEmbeddings(context.Background(), generatedEmbeddings); err != nil {
					slog.Error("Failed to save embeddings to database", "error", err)
					_ = store.CompleteIngestion(context.Background(), docHash, models.StatusFailed)
				} else {
					slog.Info("Successfully saved embeddings to pgvector!", "count", len(generatedEmbeddings))
					if err := store.CompleteIngestion(context.Background(), docHash, models.StatusCompleted); err != nil {
						slog.Error("Failed to mark document as completed", "error", err)
					}
				}
			} else {
				// If nothing was generated (all failed), mark as FAILED
				_ = store.CompleteIngestion(context.Background(), docHash, models.StatusFailed)
			}
		}

		// ─── Demo: Similarity Search ─────────────────────────────────────────────
			query := "What is the second phase of the RAG pipeline?"
			slog.Info("Embedding search query...", "query", query)
			
			queryChunk := models.Chunk{Text: query, Index: 0, DocumentID: "query"}
			queryEmbedding, err := emb.EmbedChunk(context.Background(), queryChunk)
			if err != nil {
				slog.Error("Failed to embed query", "error", err)
			} else {
				slog.Info("Searching for most similar chunks...")
				results, err := store.SearchSimilar(context.Background(), queryEmbedding.Vector, 2)
				if err != nil {
					slog.Error("Search failed", "error", err)
				} else {
					fmt.Println("\n─── Search Results ───")
					for i, res := range results {
						fmt.Printf("%d. [Score: %.3f] (Doc: %s, Chunk: %d)\n%s\n\n", 
							i+1, res.Score, res.DocumentID, res.ChunkIndex, res.ChunkText)
					}
				}
			}
			
			// ─── Start gRPC Server ──────────────────────────────────────────────
			lis, err := net.Listen("tcp", ":50051")
			if err != nil {
				slog.Error("Failed to listen for gRPC", "error", err)
			} else {
				grpcServer := grpc.NewServer(
					grpc.ChainUnaryInterceptor(
						server.LoggingInterceptor(slog.Default()),
						server.RecoveryInterceptor(slog.Default()),
					),
				)
				searchSrv := server.NewSearchServer(store, emb, server.WithLogger(slog.Default()))
				searchv1.RegisterSearchServiceServer(grpcServer, searchSrv)

				// Use errgroup for decoupled lifecycle management
				g, gCtx := errgroup.WithContext(context.Background())
				ctx, stop := signal.NotifyContext(gCtx, syscall.SIGINT, syscall.SIGTERM)
				defer stop()

				g.Go(func() error {
					slog.Info("Starting gRPC search server", "port", 50051)
					if err := grpcServer.Serve(lis); err != nil {
						return fmt.Errorf("gRPC server failed: %w", err)
					}
					return nil
				})

				g.Go(func() error {
					<-ctx.Done()
					slog.Info("Shutting down gRPC server gracefully...")
					grpcServer.GracefulStop()
					return nil
				})

				slog.Info("Retriever is ready. Press Ctrl+C to exit.")
				if err := g.Wait(); err != nil {
					slog.Error("Server exited with error", "error", err)
				}
				slog.Info("Retriever shutdown complete.")
				return
			}

	} else {
		slog.Info("Skipping concurrent embedding generation (neither RETRIEVER_GEMINI_API_KEY nor RETRIEVER_OPENAI_API_KEY is set)")
	}

	// ─── Graceful Shutdown ──────────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Retriever is ready. Press Ctrl+C to exit.")
	<-ctx.Done()
	slog.Info("Shutting down gracefully...")
}
