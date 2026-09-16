package storage

import (
	"context"
	"fmt"
	"time"

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
			content TEXT NOT NULL,
			embedding vector(%d) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`, dimension)

	_, err := pool.Exec(ctx, createTableSQL)
	if err != nil {
		return err
	}

	// Create idempotency tracking table
	createIdempotencySQL := `
		CREATE TABLE IF NOT EXISTS ingested_documents (
			hash TEXT PRIMARY KEY,
			document_id TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`
	if _, err := pool.Exec(ctx, createIdempotencySQL); err != nil {
		return err
	}

	// Create dead-letter queue table
	createDLQSQL := `
		CREATE TABLE IF NOT EXISTS dead_letters (
			id SERIAL PRIMARY KEY,
			document_id TEXT NOT NULL,
			chunk_index INT NOT NULL,
			error_message TEXT NOT NULL,
			payload TEXT NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		);
	`
	if _, err := pool.Exec(ctx, createDLQSQL); err != nil {
		return err
	}

	// pgvector hnsw indexes support up to 2000 dimensions by default.
	// If the dimension is greater than 2000, we skip creating the index and rely on exact nearest neighbor search (sequential scan), which is fine for small/medium datasets.
	if dimension <= 2000 {
		// We create an index for cosine distance ('vector_cosine_ops') to optimize search speed
		indexSQL := `CREATE INDEX IF NOT EXISTS chunks_embedding_idx ON chunks USING hnsw (embedding vector_cosine_ops);`
		_, err = pool.Exec(ctx, indexSQL)
		return err
	}
	
	return nil
}

// SaveEmbeddings persists a batch of embeddings using high-performance pgx.Batch.
func (s *PostgresStorage) SaveEmbeddings(ctx context.Context, embeddings []models.Embedding) error {
	if len(embeddings) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	
	insertSQL := `
		INSERT INTO chunks (document_id, chunk_index, model, content, embedding, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`
	
	// Queue all inserts into a single network round-trip batch
	for _, emb := range embeddings {
		vec := pgvector.NewVector(emb.Vector)
		batch.Queue(insertSQL, emb.DocumentID, emb.ChunkIndex, emb.Model, emb.Text, vec, emb.CreatedAt)
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

// SearchSimilar uses the pgvector cosine distance operator (<=>) to find the most relevant chunks.
func (s *PostgresStorage) SearchSimilar(ctx context.Context, queryEmbedding []float32, topK int) ([]models.SearchResult, error) {
	// The <=> operator computes cosine distance. Lower distance means higher similarity.
	// We sort by distance ASC to get the most similar vectors.
	query := `
		SELECT document_id, chunk_index, content, (embedding <=> $1) AS distance
		FROM chunks
		ORDER BY embedding <=> $1 ASC
		LIMIT $2
	`

	vec := pgvector.NewVector(queryEmbedding)
	rows, err := s.pool.Query(ctx, query, vec, topK)
	if err != nil {
		return nil, fmt.Errorf("failed to execute similarity search query: %w", err)
	}
	defer rows.Close()

	var results []models.SearchResult
	for rows.Next() {
		var res models.SearchResult
		var distance float64
		if err := rows.Scan(&res.DocumentID, &res.ChunkIndex, &res.ChunkText, &distance); err != nil {
			return nil, fmt.Errorf("failed to scan search result row: %w", err)
		}
		
		// Convert distance to similarity score (e.g. 1 - distance)
		res.Score = 1.0 - distance
		results = append(results, res)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration error: %w", err)
	}

	return results, nil
}

// StartIngestion attempts to lock the document hash for processing.
// Returns true if the lock is acquired (i.e., document is new or previously failed),
// and false if the document is already in progress or completed.
func (s *PostgresStorage) StartIngestion(ctx context.Context, hash, documentID string) (bool, error) {
	query := `
		INSERT INTO ingested_documents (hash, document_id, status)
		VALUES ($1, $2, $3)
		ON CONFLICT (hash) DO UPDATE 
		SET status = $3, created_at = CURRENT_TIMESTAMP
		WHERE ingested_documents.status = $4
		RETURNING hash;
	`
	// We only want to acquire if it doesn't exist, OR if it exists but failed previously.
	// The ON CONFLICT DO UPDATE WHERE status = 'FAILED' allows retry.
	// If it's already 'STARTED' or 'COMPLETED', the WHERE clause fails, returning no rows.
	
	var returnedHash string
	err := s.pool.QueryRow(ctx, query, hash, documentID, string(models.StatusStarted), string(models.StatusFailed)).Scan(&returnedHash)
	
	if err != nil {
		if err == pgx.ErrNoRows {
			// Lock not acquired: it's already processing or completed.
			return false, nil
		}
		return false, fmt.Errorf("failed to acquire idempotency lock: %w", err)
	}
	
	return true, nil
}

// CompleteIngestion transitions a document's status to COMPLETED or FAILED.
func (s *PostgresStorage) CompleteIngestion(ctx context.Context, hash string, status models.IngestionStatus) error {
	query := `
		UPDATE ingested_documents
		SET status = $1
		WHERE hash = $2
	`
	tag, err := s.pool.Exec(ctx, query, string(status), hash)
	if err != nil {
		return fmt.Errorf("failed to update ingestion status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("idempotency record not found for hash: %s", hash)
	}
	return nil
}

// SaveDeadLetters writes a batch of failed items to the DLQ table efficiently.
func (s *PostgresStorage) SaveDeadLetters(ctx context.Context, dlqs []models.DeadLetter) error {
	if len(dlqs) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `
		INSERT INTO dead_letters (document_id, chunk_index, error_message, payload, created_at)
		VALUES ($1, $2, $3, $4, COALESCE($5, CURRENT_TIMESTAMP))
	`
	
	for _, dl := range dlqs {
		createdAt := dl.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now()
		}
		batch.Queue(query, dl.DocumentID, dl.ChunkIndex, dl.Error, dl.Payload, createdAt)
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for i := 0; i < len(dlqs); i++ {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("failed to save dead letter at index %d: %w", i, err)
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
