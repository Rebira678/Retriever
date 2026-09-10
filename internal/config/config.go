// Package config provides configuration for the Retriever pipeline.
//
// Configuration follows the 12-Factor App methodology — all settings can be
// overridden via environment variables. Defaults are tuned for a reasonable
// RAG ingestion workload.
package config

import (
	"os"
	"strconv"
)

// Config holds all Retriever pipeline configuration.
type Config struct {
	// ─── Chunking ────────────────────────────────────────────────────────
	// ChunkSize is the maximum number of characters per chunk.
	ChunkSize int

	// ChunkOverlap is the number of overlapping characters between consecutive chunks.
	// Overlap preserves context across chunk boundaries, preventing information loss
	// when a sentence or concept spans two chunks.
	ChunkOverlap int

	// ChunkStrategy selects the chunking algorithm: "fixed_size", "sentence", "paragraph".
	ChunkStrategy string

	// ─── Embedding ───────────────────────────────────────────────────────
	// EmbeddingModel is the model ID for the embedding API (e.g., "text-embedding-3-small").
	EmbeddingModel string

	// EmbeddingDimension is the vector dimension of the embedding output.
	EmbeddingDimension int

	// WorkerPoolSize is the number of concurrent embedding workers.
	WorkerPoolSize int

	// ─── Database ────────────────────────────────────────────────────────
	// DatabaseURL is the Postgres connection string with pgvector extension.
	DatabaseURL string

	// ─── Server ──────────────────────────────────────────────────────────
	// GRPCPort is the port for the gRPC search API.
	GRPCPort string

	// HTTPPort is the port for the HTTP health/metrics API.
	HTTPPort string
}

// Default returns a Config with sensible defaults for local development.
func Default() *Config {
	return &Config{
		ChunkSize:          512,
		ChunkOverlap:       64,
		ChunkStrategy:      "fixed_size",
		EmbeddingModel:     "text-embedding-3-small",
		EmbeddingDimension: 1536,
		WorkerPoolSize:     4,
		DatabaseURL:        "postgres://retriever:retriever@localhost:5432/retriever?sslmode=disable",
		GRPCPort:           ":50051",
		HTTPPort:           ":8080",
	}
}

// FromEnv loads configuration from environment variables, falling back to defaults.
func FromEnv() *Config {
	cfg := Default()

	if v := os.Getenv("RETRIEVER_CHUNK_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ChunkSize = n
		}
	}
	if v := os.Getenv("RETRIEVER_CHUNK_OVERLAP"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.ChunkOverlap = n
		}
	}
	if v := os.Getenv("RETRIEVER_CHUNK_STRATEGY"); v != "" {
		cfg.ChunkStrategy = v
	}
	if v := os.Getenv("RETRIEVER_EMBEDDING_MODEL"); v != "" {
		cfg.EmbeddingModel = v
	}
	if v := os.Getenv("RETRIEVER_EMBEDDING_DIMENSION"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.EmbeddingDimension = n
		}
	}
	if v := os.Getenv("RETRIEVER_WORKER_POOL_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.WorkerPoolSize = n
		}
	}
	if v := os.Getenv("RETRIEVER_DATABASE_URL"); v != "" {
		cfg.DatabaseURL = v
	}
	if v := os.Getenv("RETRIEVER_GRPC_PORT"); v != "" {
		cfg.GRPCPort = v
	}
	if v := os.Getenv("RETRIEVER_HTTP_PORT"); v != "" {
		cfg.HTTPPort = v
	}

	return cfg
}
