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
	"github.com/Rebira678/Retriever/internal/storage"

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
		"day", 29,
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

		// Set up channels with backpressure
		chunkChan := make(chan models.Chunk, len(chunks))
		resultsChan := make(chan pipeline.EmbedResult, len(chunks))

		// Feed chunks into the pipeline
		for _, chunk := range chunks {
			chunkChan <- chunk
		}
		close(chunkChan)

		slog.Info("Starting concurrent embedding worker pool...", "workers", cfg.WorkerPoolSize)

		// Run the worker pool (blocks until all chunks are embedded)
		pool.Run(context.Background(), chunkChan, resultsChan)

		// Read the results into a slice for batch saving
		var generatedEmbeddings []models.Embedding
		for res := range resultsChan {
			if res.Err != nil {
				slog.Error("Failed to embed chunk", "chunk_index", res.Chunk.Index, "error", res.Err)
				continue
			}
			
			generatedEmbeddings = append(generatedEmbeddings, res.Embedding)
			if len(generatedEmbeddings) == 1 {
				fmt.Printf("\n─── First Chunk Embedding (Sample) ───\n[%f, %f, %f, ...]\n",
					res.Embedding.Vector[0], res.Embedding.Vector[1], res.Embedding.Vector[2])
			}
		}
		slog.Info("Concurrent embedding completed successfully", "total_embeddings_generated", len(generatedEmbeddings))

		// ─── Demo: Save to PostgreSQL (pgvector) ──────────────────────────────────
		actualDimension := dimension
		if len(generatedEmbeddings) > 0 {
			actualDimension = len(generatedEmbeddings[0].Vector)
		}

		slog.Info("Connecting to PostgreSQL to save vectors...", "url", cfg.DatabaseURL, "detected_dimension", actualDimension)
		store, err := storage.NewPostgresStorage(context.Background(), cfg.DatabaseURL, actualDimension)
		if err != nil {
			slog.Error("Failed to connect to database", "error", err)
		} else {
			defer store.Close()
			if err := store.SaveEmbeddings(context.Background(), generatedEmbeddings); err != nil {
				slog.Error("Failed to save embeddings to database", "error", err)
			} else {
				slog.Info("Successfully saved embeddings to pgvector!", "count", len(generatedEmbeddings))
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
