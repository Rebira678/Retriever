package pipeline_test

import (
	"context"
	"errors"
	"strings"
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
func (m *MockStorage) SearchSimilar(ctx context.Context, queryEmbedding []float32, modelName string, topK int, efSearch int) ([]models.SearchResult, error) { return nil, nil }
func (m *MockStorage) SearchHybrid(ctx context.Context, queryText string, queryEmbedding []float32, modelName string, topK int, efSearch int) ([]models.SearchResult, error) { return nil, nil }
func (m *MockStorage) SweepOldChunks(ctx context.Context, safeWindow time.Duration) error { return nil }
func (m *MockStorage) Close() error { return nil }
func (m *MockStorage) Ping(ctx context.Context) error { return nil }


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

func TestRunBatchIngestion_Success(t *testing.T) {
	cfg := config.Default()
	store := &MockStorage{}
	emb := &MockEmbedder{}
	c := chunker.New(10, 0)
	p := pipeline.NewPipeline(cfg, store, emb, c)

	docs := []models.Document{
		{ID: "doc-1", Content: "First document content"},
		{ID: "doc-2", Content: "Second document content"},
	}

	err := p.RunBatchIngestion(context.Background(), docs, "mock", "mock")
	if err != nil {
		t.Fatalf("Expected success, got: %v", err)
	}

	if len(store.EmbeddingsSaved) == 0 {
		t.Fatalf("Expected embeddings to be saved for batch")
	}
}

func TestRunBatchIngestion_PartialIdempotency(t *testing.T) {
	cfg := config.Default()
	store := &MockStorage{
		startIngestionFunc: func(ctx context.Context, hash, docID string) (bool, error) {
			if docID == "doc-locked" {
				return false, nil // Simulate already processed
			}
			return true, nil
		},
	}
	emb := &MockEmbedder{}
	c := chunker.New(50, 0)
	p := pipeline.NewPipeline(cfg, store, emb, c)

	docs := []models.Document{
		{ID: "doc-locked", Content: "This should be skipped"},
		{ID: "doc-new", Content: "This should be processed"},
	}

	err := p.RunBatchIngestion(context.Background(), docs, "mock", "mock")
	if err != nil {
		t.Fatalf("Expected success despite idempotency hit, got: %v", err)
	}

	// Because doc-locked was skipped, we should only have embeddings for doc-new
	for _, emb := range store.EmbeddingsSaved {
		if emb.DocumentID == "doc-locked" {
			t.Fatalf("Expected doc-locked to be skipped, but found its embeddings")
		}
	}
}

func TestRunBatchIngestion_DatabaseError(t *testing.T) {
	cfg := config.Default()
	store := &MockStorage{
		saveEmbeddingsFunc: func(ctx context.Context, embeddings []models.Embedding) error {
			return errors.New("simulated batch db error")
		},
	}
	emb := &MockEmbedder{}
	c := chunker.New(5, 0)
	p := pipeline.NewPipeline(cfg, store, emb, c)

	docs := []models.Document{
		{ID: "doc-1", Content: "Lots of tiny chunks for batch"},
		{ID: "doc-2", Content: "Even more chunks here"},
	}

	start := time.Now()
	err := p.RunBatchIngestion(context.Background(), docs, "mock", "mock")
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("Expected DB error to bubble up")
	}

	if duration > 100*time.Millisecond {
		t.Fatalf("Pipeline took too long to cancel (%v), errgroup context cancellation failed", duration)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Senior Level: Chaos Engineering & Goroutine Leak Detection
// This test injects random panics, timeouts, and partial network failures 
// simultaneously into the worker pool. It guarantees that the errgroup 
// gracefully recovers, the context cancels instantly, and NO goroutines leak.
// ─────────────────────────────────────────────────────────────────────────────
func TestRunBatchIngestion_Chaos_GoroutineLeak(t *testing.T) {
	cfg := config.Default()
	cfg.WorkerPoolSize = 50 // High concurrency to maximize race potential

	store := &MockStorage{
		saveEmbeddingsFunc: func(ctx context.Context, embeddings []models.Embedding) error {
			// 10% chance of a "network drop" (timeout)
			if time.Now().UnixNano()%10 == 0 {
				time.Sleep(200 * time.Millisecond) // Simulated hang
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					return errors.New("simulated network drop")
				}
			}
			return nil
		},
	}
	
	emb := &MockEmbedder{
		embedChunkFunc: func(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
			// 5% chance of an absolute panic in the third-party SDK
			if chunk.Index == 42 {
				panic("simulated fatal SDK crash")
			}
			return models.Embedding{Vector: []float32{1.0}}, nil
		},
	}
	
	c := chunker.New(10, 0)
	p := pipeline.NewPipeline(cfg, store, emb, c)

	// Create 100 documents to flood the system
	docs := make([]models.Document, 100)
	for i := 0; i < 100; i++ {
		// Document 42 will cause a panic due to chunk index 42 being generated
		docs[i] = models.Document{ID: "doc-chaos", Content: strings.Repeat("Lots of text ", 50)}
	}

	start := time.Now()
	
	// We expect this to fail (either due to network drop or the panic recovery mechanism).
	// Crucially, it MUST NOT hang indefinitely (deadlock).
	err := p.RunBatchIngestion(context.Background(), docs, "mock", "mock")
	duration := time.Since(start)

	if err == nil {
		t.Fatalf("Expected pipeline to collapse under chaos, but it succeeded")
	}

	// If the errgroup/context management is written poorly, the channel will block forever 
	// because the panic killed a worker, or the sleep caused a deadlock.
	if duration > 2*time.Second {
		t.Fatalf("CRITICAL: Pipeline deadlocked and leaked goroutines! Took %v to abort", duration)
	}
}
