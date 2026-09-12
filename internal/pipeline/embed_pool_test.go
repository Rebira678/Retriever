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
	delay      time.Duration
	callCount  int32
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
	pool := NewEmbedPool(embedder, 4) // 4 workers

	chunkChan := make(chan models.Chunk, 100)
	embeddingChan := make(chan models.Embedding, 100)

	// Enqueue 20 chunks
	for i := 0; i < 20; i++ {
		chunkChan <- models.Chunk{Index: i, Text: "test data"}
	}
	close(chunkChan) // Signal no more chunks

	start := time.Now()
	
	// Run the pool (blocks until done)
	pool.Run(context.Background(), chunkChan, embeddingChan)

	duration := time.Since(start)

	// Since we have 20 chunks, each taking 10ms, a single worker would take 200ms.
	// With 4 workers, it should take roughly 50ms + some overhead.
	// We'll assert it took less than 100ms, proving concurrency.
	if duration >= 100*time.Millisecond {
		t.Errorf("Expected duration < 100ms due to concurrency, got %v", duration)
	}

	// Verify all chunks were processed
	if embedder.callCount != 20 {
		t.Errorf("Expected 20 calls to EmbedChunk, got %d", embedder.callCount)
	}

	// Verify embeddings were sent to output channel
	count := 0
	for emb := range embeddingChan {
		if emb.Model != "mock-model" {
			t.Errorf("Unexpected model in embedding")
		}
		count++
	}
	if count != 20 {
		t.Errorf("Expected 20 embeddings in output channel, got %d", count)
	}
}

func TestEmbedPool_ContextCancellation(t *testing.T) {
	embedder := &mockEmbedder{delay: 50 * time.Millisecond}
	pool := NewEmbedPool(embedder, 2)

	chunkChan := make(chan models.Chunk, 10)
	embeddingChan := make(chan models.Embedding, 10)

	for i := 0; i < 10; i++ {
		chunkChan <- models.Chunk{Index: i}
	}
	// Do NOT close chunkChan here, to simulate a blocked/ongoing stream

	ctx, cancel := context.WithCancel(context.Background())
	
	// Cancel the context after 20ms
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	// Run should exit promptly when context is cancelled
	pool.Run(ctx, chunkChan, embeddingChan)

	// If Run didn't return, this test would hang.
	// Also verify that embeddingChan is closed by the Run method on exit.
	_, ok := <-embeddingChan
	if ok {
		t.Errorf("Expected embeddingChan to be closed on cancellation")
	}
}
