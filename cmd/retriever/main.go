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

	// ─── Demo: Concurrent Embedding with Worker Pool ───────────────────────────────
	var emb embedder.Embedder
	dimension := cfg.EmbeddingDimension

	var provider string
	var modelName string

	if cfg.GeminiAPIKey != "" {
		slog.Info("Using Gemini Embedder")
		emb = embedder.NewGeminiEmbedder(cfg.GeminiAPIKey, "gemini-embedding-2")
		provider = "gemini"
		modelName = "gemini-embedding-2"
	} else if cfg.OpenAIAPIKey != "" {
		slog.Info("Using OpenAI Embedder")
		emb = embedder.NewOpenAIEmbedder(cfg.OpenAIAPIKey, cfg.EmbeddingAPIURL, cfg.EmbeddingModel)
		provider = "openai"
		modelName = cfg.EmbeddingModel
	}

	if emb != nil {
		// Initialize Database Connection
		slog.Info("Connecting to PostgreSQL...", "url", cfg.DatabaseURL, "dimension", dimension)
		var store storage.Storage
		store, err := storage.NewPostgresStorage(context.Background(), cfg.DatabaseURL, dimension)
		if err != nil {
			slog.Error("Failed to connect to database", "error", err)
			return
		}
		defer store.Close()

		// ─── Senior Level: Orchestrator Pattern ───────────────────────────
		// Encapsulate the entire complex ingestion logic (Idempotency, 
		// Garbage Collection, Channels, DLQ) into a single reusable Pipeline struct.
		p := pipeline.NewPipeline(cfg, store, emb, c)

		doc := models.Document{
			ID:      "sample-doc-rag",
			Content: sampleDoc,
		}

		if err := p.RunIngestion(context.Background(), doc, provider, modelName); err != nil {
			slog.Error("Pipeline ingestion failed", "error", err)
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
				results, err := store.SearchSimilar(context.Background(), queryEmbedding.Vector, modelName, 2)
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
