package storage

import (
	"context"
	"fmt"

	"github.com/Rebira678/Retriever/internal/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
)

// PostgresStorage implements the Storage interface using PostgreSQL and pgvector.
type PostgresStorage struct {
	pool *pgxpool.Pool
}

// NewPostgresStorage initializes a connection pool and ensures the database schema exists.
func NewPostgresStorage(ctx context.Context, dbURL string, dimension int) (*PostgresStorage, error) {
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to database: %w", err)
	}

	// Verify connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("database ping failed: %w", err)
	}

	// Initialize the schema automatically
	if err := initializeSchema(ctx, pool, dimension); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return &PostgresStorage{pool: pool}, nil
}

func initializeSchema(ctx context.Context, pool *pgxpool.Pool, dimension int) error {
	// Enable pgvector extension
	if _, err := pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS vector;"); err != nil {
		return err
	}

	// Create the chunks table with the specified vector dimension
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS chunks (
			id SERIAL PRIMARY KEY,
			document_id TEXT NOT NULL,
			chunk_index INT NOT NULL,
			model TEXT NOT NULL,
			embedding vector(%d) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`, dimension)

	_, err := pool.Exec(ctx, createTableSQL)
	return err
}

// SaveEmbeddings persists a batch of embeddings using high-performance pgx.Batch.
func (s *PostgresStorage) SaveEmbeddings(ctx context.Context, embeddings []models.Embedding) error {
	if len(embeddings) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	
	insertSQL := `
		INSERT INTO chunks (document_id, chunk_index, model, embedding, created_at)
		VALUES ($1, $2, $3, $4, $5)
	`
	
	// Queue all inserts into a single network round-trip batch
	for _, emb := range embeddings {
		// pgvector.NewVector converts []float32 to the custom vector type for Postgres
		vec := pgvector.NewVector(emb.Vector)
		batch.Queue(insertSQL, emb.DocumentID, emb.ChunkIndex, emb.Model, vec, emb.CreatedAt)
	}
	
	// Send the batch
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	
	// Ensure all inserts succeeded
	for i := 0; i < len(embeddings); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to insert embedding at index %d: %w", i, err)
		}
	}
	
	return nil
}

// Close gracefully shuts down the database connection pool.
func (s *PostgresStorage) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}
