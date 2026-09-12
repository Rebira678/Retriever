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
	
	// Close gracefully shuts down the database connection.
	Close() error
}
