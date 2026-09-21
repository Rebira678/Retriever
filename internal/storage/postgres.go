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
			content_hash TEXT NOT NULL,
			embedding vector(%d) NOT NULL,
			created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
			UNIQUE (document_id, content_hash, model)
		);
	`, dimension)

	_, err := pool.Exec(ctx, createTableSQL)
	if err != nil {
		return err
	}

	// For robust backward compatibility during local development, ensure new columns are added
	// if the table was created before Day 36 idempotency updates.
	_, _ = pool.Exec(ctx, "ALTER TABLE chunks ADD COLUMN IF NOT EXISTS content_hash TEXT DEFAULT '';")
	_, _ = pool.Exec(ctx, "ALTER TABLE chunks ADD COLUMN IF NOT EXISTS chunk_index INT DEFAULT 0;")

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

	// We intentionally do NOT create the vector index synchronously during schema initialization.
	// For production datasets, index creation is extremely heavy, generates massive WAL activity,
	// and should be handled asynchronously via OptimizeIndex() to avoid blocking deployments.
	// Furthermore, we may want to tune maintenance_work_mem before building.
	
	return nil
}

// SaveEmbeddings persists a batch of embeddings using high-performance pgx.Batch.
func (s *PostgresStorage) SaveEmbeddings(ctx context.Context, embeddings []models.Embedding) error {
	if len(embeddings) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	
	insertSQL := `
		INSERT INTO chunks (document_id, chunk_index, model, content, content_hash, embedding, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (document_id, content_hash, model) DO UPDATE 
		SET chunk_index = EXCLUDED.chunk_index,
		    content = EXCLUDED.content, 
		    embedding = EXCLUDED.embedding,
		    created_at = EXCLUDED.created_at
	`
	
	// Queue all inserts into a single network round-trip batch
	for _, emb := range embeddings {
		vec := pgvector.NewVector(emb.Vector)
		batch.Queue(insertSQL, emb.DocumentID, emb.ChunkIndex, emb.Model, emb.Text, emb.ContentHash, vec, emb.CreatedAt)
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

// SearchSimilar uses pgvector's (<=>) operator (cosine distance) to find the most relevant chunks.
func (s *PostgresStorage) SearchSimilar(ctx context.Context, queryEmbedding []float32, modelName string, topK int, efSearch int) ([]models.SearchResult, error) {
	// Dynamically configure ef_search for this specific transaction.
	// ef_search controls the size of the dynamic candidate list during HNSW traversal.
	// Default is 40. Higher = better recall but higher latency.
	if efSearch > 0 {
		_, err := s.pool.Exec(ctx, fmt.Sprintf("SET LOCAL hnsw.ef_search = %d", efSearch))
		if err != nil {
			return nil, fmt.Errorf("failed to set ef_search: %w", err)
		}
	}

	// We MUST filter by model to isolate vector spaces (e.g. OpenAI vs Gemini vs Local).
	query := `
		SELECT document_id, chunk_index, content, (embedding <=> $1) AS distance
		FROM chunks
		WHERE model = $2
		ORDER BY embedding <=> $1 ASC
		LIMIT $3
	`

	vec := pgvector.NewVector(queryEmbedding)
	rows, err := s.pool.Query(ctx, query, vec, modelName, topK)
	if err != nil {
		return nil, fmt.Errorf("failed to execute similarity search query: %w", err)
	}
	defer rows.Close()

	results := make([]models.SearchResult, 0, topK)
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

// SweepOldChunks deletes orphaned vectors that belong to an older model,
// but ONLY if the newer model's vectors have existed longer than the safeWindow.
func (s *PostgresStorage) SweepOldChunks(ctx context.Context, safeWindow time.Duration) error {
	query := `
		DELETE FROM chunks
		WHERE id IN (
			SELECT old.id
			FROM chunks old
			JOIN chunks new ON old.document_id = new.document_id
			WHERE old.model != new.model
			  AND old.created_at < new.created_at
			  AND new.created_at <= $1
		)
	`
	cutoff := time.Now().Add(-safeWindow)
	tag, err := s.pool.Exec(ctx, query, cutoff)
	if err != nil {
		return fmt.Errorf("failed to sweep old chunks: %w", err)
	}
	
	if tag.RowsAffected() > 0 {
		// Log conditionally to avoid spamming the logs when nothing is deleted
		fmt.Printf("🧹 [Garbage Collector] Swept %d old chunks (safely past rollback window)\n", tag.RowsAffected())
	}
	return nil
}

// OptimizeIndex forces an expert-level HNSW index creation.
// In production, this should run asynchronously. We temporarily bump maintenance_work_mem
// to accelerate graph construction and minimize WAL bloat.
func (s *PostgresStorage) OptimizeIndex(ctx context.Context, dimension int, m int, efConstruction int) error {
	if dimension > 2000 {
		return fmt.Errorf("pgvector HNSW index supports max 2000 dimensions, got %d", dimension)
	}

	// Acquire a dedicated connection to manipulate session variables safely
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("failed to acquire connection for indexing: %w", err)
	}
	defer conn.Release()

	// 1. Temporarily boost maintenance memory for this session (accelerates HNSW graph construction)
	// Requires postgres role privileges.
	_, err = conn.Exec(ctx, "SET maintenance_work_mem = '1GB';")
	if err != nil {
		// We log but do not fail if privileges are insufficient
		fmt.Println("⚠️ [Optimization] Could not set maintenance_work_mem to 1GB. Ensure DB user has sufficient privileges for max performance.")
	}

	// 2. Build the HNSW Index
	// m: limits maximum outbound connections per node in the graph (16-64 is typical).
	// ef_construction: controls the breadth of search during index build (higher = better quality, slower build).
	fmt.Printf("🚀 [Optimization] Building HNSW index (m=%d, ef_construction=%d). This may take a while depending on row count...\n", m, efConstruction)
	
	indexSQL := fmt.Sprintf(`
		CREATE INDEX IF NOT EXISTS chunks_embedding_idx 
		ON chunks 
		USING hnsw (embedding vector_cosine_ops) 
		WITH (m = %d, ef_construction = %d);
	`, m, efConstruction)

	_, err = conn.Exec(ctx, indexSQL)
	if err != nil {
		return fmt.Errorf("failed to create HNSW index: %w", err)
	}

	fmt.Println("✅ [Optimization] HNSW Index successfully built.")
	return nil
}

