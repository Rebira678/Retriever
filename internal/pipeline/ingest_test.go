package pipeline_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Rebira678/Retriever/internal/chunker"
	"github.com/Rebira678/Retriever/internal/config"
	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/pipeline"
)

// MockStorage implements storage.Storage for testing.
type MockStorage struct {
	startIngestionFunc    func(ctx context.Context, hash, docID string) (bool, error)
	saveEmbeddingsFunc    func(ctx context.Context, embeddings []models.Embedding) error
	completeIngestionFunc func(ctx context.Context, hash string, status models.IngestionStatus) error
	saveDeadLettersFunc   func(ctx context.Context, dlqs []models.DeadLetter) error
	
	EmbeddingsSaved []models.Embedding
	DeadLettersSaved []models.DeadLetter
	CompletedStatus models.IngestionStatus
}

func (m *MockStorage) StartIngestion(ctx context.Context, hash, docID string) (bool, error) {
	if m.startIngestionFunc != nil {
		return m.startIngestionFunc(ctx, hash, docID)
	}
	return true, nil
}
func (m *MockStorage) SaveEmbeddings(ctx context.Context, embeddings []models.Embedding) error {
	if m.saveEmbeddingsFunc != nil {
		return m.saveEmbeddingsFunc(ctx, embeddings)
	}
	m.EmbeddingsSaved = append(m.EmbeddingsSaved, embeddings...)
	return nil
}
func (m *MockStorage) CompleteIngestion(ctx context.Context, hash string, status models.IngestionStatus) error {
	m.CompletedStatus = status
	if m.completeIngestionFunc != nil {
		return m.completeIngestionFunc(ctx, hash, status)
	}
	return nil
}
func (m *MockStorage) SaveDeadLetters(ctx context.Context, dlqs []models.DeadLetter) error {
	m.DeadLettersSaved = append(m.DeadLettersSaved, dlqs...)
	if m.saveDeadLettersFunc != nil {
		return m.saveDeadLettersFunc(ctx, dlqs)
	}
	return nil
}
func (m *MockStorage) SearchSimilar(ctx context.Context, queryEmbedding []float32, modelName string, topK int) ([]models.SearchResult, error) { return nil, nil }
func (m *MockStorage) SweepOldChunks(ctx context.Context, safeWindow time.Duration) error { return nil }
func (m *MockStorage) Close() error { return nil }

// MockEmbedder implements embedder.Embedder for testing.
type MockEmbedder struct {
	embedChunkFunc func(ctx context.Context, chunk models.Chunk) (models.Embedding, error)
}

func (m *MockEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	if m.embedChunkFunc != nil {
		return m.embedChunkFunc(ctx, chunk)
	}
	return models.Embedding{Vector: []float32{1.0, 2.0}}, nil
}

func TestRunIngestion_Success(t *testing.T) {
	cfg := config.Default()
	cfg.WorkerPoolSize = 2
	cfg.IngestionQueueSize = 10
	
	store := &MockStorage{}
	emb := &MockEmbedder{}
	c := chunker.New(10, 0)
	
	p := pipeline.NewPipeline(cfg, store, emb, c)
	
	doc := models.Document{ID: "test-doc", Content: "This is a simple test document"}
	err := p.RunIngestion(context.Background(), doc, "mock", "mock-model")
	
	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
	if len(store.EmbeddingsSaved) == 0 {
		t.Fatalf("Expected embeddings to be saved")
	}
	if store.CompletedStatus != models.StatusCompleted {
		t.Fatalf("Expected status to be completed")
	}
}

func TestRunIngestion_IdempotencyHit(t *testing.T) {
	cfg := config.Default()
	store := &MockStorage{
		startIngestionFunc: func(ctx context.Context, hash, docID string) (bool, error) {
			return false, nil // Lock taken
		},
	}
	emb := &MockEmbedder{}
	c := chunker.New(10, 0)
	p := pipeline.NewPipeline(cfg, store, emb, c)
	
	err := p.RunIngestion(context.Background(), models.Document{ID: "1", Content: "A"}, "m", "m")
	if err != nil {
		t.Fatalf("Expected no error on idempotency hit, got: %v", err)
	}
	if len(store.EmbeddingsSaved) > 0 {
		t.Fatalf("Expected no embeddings saved due to idempotency")
	}
}

func TestRunIngestion_PartialEmbedderFailure(t *testing.T) {
	cfg := config.Default()
	store := &MockStorage{}
	emb := &MockEmbedder{
		embedChunkFunc: func(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
			if chunk.Index == 1 {
				return models.Embedding{}, errors.New("simulated embedding error")
			}
			return models.Embedding{Vector: []float32{0.5}}, nil
		},
	}
	c := chunker.New(10, 0) // Should generate about 3-4 chunks for "Line 1. Line 2. Line 3."
	p := pipeline.NewPipeline(cfg, store, emb, c)
	
	doc := models.Document{ID: "test-doc", Content: "1234567890abcdefghij"} // 2 chunks
	err := p.RunIngestion(context.Background(), doc, "mock", "mock-model")
	
	if err != nil {
		t.Fatalf("Expected pipeline to complete despite partial failure, got error: %v", err)
	}
	
	if len(store.DeadLettersSaved) != 1 {
		t.Fatalf("Expected exactly 1 dead letter, got %d", len(store.DeadLettersSaved))
	}
	if len(store.EmbeddingsSaved) != 1 {
		t.Fatalf("Expected exactly 1 successful embedding, got %d", len(store.EmbeddingsSaved))
	}
}

func TestRunIngestion_DatabaseFailureLeaksGoroutines(t *testing.T) {
	cfg := config.Default()
	store := &MockStorage{
		saveEmbeddingsFunc: func(ctx context.Context, embeddings []models.Embedding) error {
			return errors.New("simulated DB error")
		},
	}
	emb := &MockEmbedder{}
	c := chunker.New(2, 0) // Tiny chunk size to force multiple batches
	p := pipeline.NewPipeline(cfg, store, emb, c)
	
	doc := models.Document{ID: "test-doc", Content: "This long string will create many tiny chunks"} 
	
	start := time.Now()
	err := p.RunIngestion(context.Background(), doc, "mock", "mock-model")
	duration := time.Since(start)
	
	if err == nil {
		t.Fatalf("Expected database error to bubble up")
	}
	
	// If the pipeline leaked goroutines or deadlocked, it would take a long time or hang.
	// Since we cancel the context immediately on DB error, it should exit almost instantly.
	if duration > 100*time.Millisecond {
		t.Fatalf("Pipeline took too long to exit on error (%v), suggesting a goroutine leak or deadlock", duration)
	}
	
	if store.CompletedStatus != models.StatusFailed {
		t.Fatalf("Expected ingestion to be marked as FAILED")
	}
}
