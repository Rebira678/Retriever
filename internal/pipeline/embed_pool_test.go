package pipeline

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rebira678/Retriever/internal/models"
)

// mockEmbedder simulates an embedding API with a configurable delay
type mockEmbedder struct {
	delay     time.Duration
	callCount int32
}

func (m *mockEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	atomic.AddInt32(&m.callCount, 1)
	if m.delay > 0 {
		time.Sleep(m.delay)
	}
	return models.Embedding{
		ChunkIndex: chunk.Index,
		Vector:     []float32{1.0, 2.0, 3.0},
		Model:      "mock-model",
	}, nil
}

func TestEmbedPool_Run(t *testing.T) {
	embedder := &mockEmbedder{delay: 10 * time.Millisecond}
	// Use functional options
	pool := NewEmbedPool(embedder, WithWorkers(4))

	chunkChan := make(chan models.Chunk, 100)
	resultsChan := make(chan EmbedResult, 100)

	// Enqueue 20 chunks
	for i := 0; i < 20; i++ {
		chunkChan <- models.Chunk{Index: i, Text: "test data"}
	}
	close(chunkChan)

	start := time.Now()
	
	// Run the pool (blocks until done)
	pool.Run(context.Background(), chunkChan, resultsChan)

	duration := time.Since(start)

	// Single worker: ~200ms. 4 workers: ~50ms.
	if duration >= 100*time.Millisecond {
		t.Errorf("Expected duration < 100ms due to concurrency, got %v", duration)
	}

	if embedder.callCount != 20 {
		t.Errorf("Expected 20 calls to EmbedChunk, got %d", embedder.callCount)
	}

	count := 0
	for res := range resultsChan {
		if res.Err != nil {
			t.Errorf("Unexpected error: %v", res.Err)
		}
		if res.Embedding.Model != "mock-model" {
			t.Errorf("Unexpected model in embedding")
		}
		count++
	}
	if count != 20 {
		t.Errorf("Expected 20 results in output channel, got %d", count)
	}
}

func TestEmbedPool_ContextCancellation(t *testing.T) {
	embedder := &mockEmbedder{delay: 50 * time.Millisecond}
	pool := NewEmbedPool(embedder, WithWorkers(2))

	chunkChan := make(chan models.Chunk, 10)
	resultsChan := make(chan EmbedResult, 10)

	for i := 0; i < 10; i++ {
		chunkChan <- models.Chunk{Index: i}
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	pool.Run(ctx, chunkChan, resultsChan)

	// Drain any results that might have slipped through before the context was completely closed
	for range resultsChan {
	}
	// If the channel was not closed, the loop above would hang forever causing a test timeout.
}
