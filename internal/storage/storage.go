package storage

import (
	"context"

	"github.com/Rebira678/Retriever/internal/models"
)

// Storage defines the interface for persisting vectors and their metadata.
// Using an interface allows us to swap PostgreSQL for another database (like Milvus or Pinecone)
// later if we choose, without changing the core pipeline logic.
type Storage interface {
	// SaveEmbeddings takes a batch of successfully generated embeddings
	// and persists them to the vector database.
	SaveEmbeddings(ctx context.Context, embeddings []models.Embedding) error

	// SearchSimilar performs a vector similarity search to find the topK closest chunks.
	SearchSimilar(ctx context.Context, queryEmbedding []float32, topK int) ([]models.SearchResult, error)

	// StartIngestion attempts to acquire an idempotency lock for a document hash.
	// Returns true if the lock was acquired (document is new/failed previously), or false if it's already processing/completed.
	StartIngestion(ctx context.Context, hash, documentID string) (bool, error)

	// CompleteIngestion updates the idempotency record to reflect the final state (COMPLETED or FAILED).
	CompleteIngestion(ctx context.Context, hash string, status models.IngestionStatus) error

	// SaveDeadLetters persists a batch of failed chunks to the dead-letter queue efficiently.
	SaveDeadLetters(ctx context.Context, dlqs []models.DeadLetter) error
	
	// Close gracefully shuts down the database connection.
	Close() error
}

