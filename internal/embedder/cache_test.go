package embedder

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Rebira678/Retriever/internal/models"
)

// MockEmbedder allows us to test the cache layer without hitting external APIs.
type MockEmbedder struct {
	callCount int32
	delay     time.Duration
	err       error
}

func (m *MockEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	atomic.AddInt32(&m.callCount, 1)

	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return models.Embedding{}, ctx.Err()
		}
	}

	if m.err != nil {
		return models.Embedding{}, m.err
	}

	return models.Embedding{
		Vector: []float32{1.0, 2.0},
		Model:  "mock-model",
	}, nil
}

func TestCachedEmbedder_HitAndMiss(t *testing.T) {
	mock := &MockEmbedder{}
	cache := NewCachedEmbedder(mock, 10, 1*time.Hour)
	ctx := context.Background()
	chunk := models.Chunk{Text: "hello world"}

	// 1st call: Cache Miss
	emb1, err := cache.EmbedChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 2nd call: Cache Hit
	emb2, err := cache.EmbedChunk(ctx, chunk)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if emb1.Model != emb2.Model {
		t.Errorf("expected same embedding")
	}

	hits, misses := cache.Metrics()
	if hits != 1 || misses != 1 {
		t.Errorf("expected 1 hit, 1 miss, got %d hits, %d misses", hits, misses)
	}

	if mock.callCount != 1 {
		t.Errorf("expected underlying to be called 1 time, got %d", mock.callCount)
	}
}

func TestCachedEmbedder_TTL(t *testing.T) {
	mock := &MockEmbedder{}
	cache := NewCachedEmbedder(mock, 10, 10*time.Millisecond)
	ctx := context.Background()
	chunk := models.Chunk{Text: "ephemeral"}

	_, _ = cache.EmbedChunk(ctx, chunk) // Miss (call 1)
	time.Sleep(15 * time.Millisecond) // Wait for expiration
	_, _ = cache.EmbedChunk(ctx, chunk) // Miss due to TTL (call 2)

	if mock.callCount != 2 {
		t.Errorf("expected underlying to be called 2 times due to expiration, got %d", mock.callCount)
	}
}

func TestCachedEmbedder_SingleflightStampede(t *testing.T) {
	mock := &MockEmbedder{delay: 50 * time.Millisecond}
	cache := NewCachedEmbedder(mock, 100, 1*time.Hour)
	
	concurrency := 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	chunk := models.Chunk{Text: "stampede test"}

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			_, err := cache.EmbedChunk(context.Background(), chunk)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}

	wg.Wait()

	hits, misses := cache.Metrics()
	if misses != 100 {
		t.Errorf("expected exactly 100 cache misses (since they all hit simultaneously), got %d", misses)
	}
	if hits != 0 {
		t.Errorf("expected 0 hits from LRU (all resolved via singleflight), got %d", hits)
	}
	if mock.callCount != 1 {
		t.Errorf("Singleflight failed: external API called %d times instead of 1", mock.callCount)
	}
}

func TestCachedEmbedder_ContextCancellation(t *testing.T) {
	mock := &MockEmbedder{delay: 100 * time.Millisecond}
	cache := NewCachedEmbedder(mock, 100, 1*time.Hour)
	chunk := models.Chunk{Text: "context test"}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel1()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel2()

	var wg sync.WaitGroup
	wg.Add(2)
	var err1, err2 error

	go func() {
		defer wg.Done()
		_, err1 = cache.EmbedChunk(ctx1, chunk)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_, err2 = cache.EmbedChunk(ctx2, chunk)
	}()

	wg.Wait()

	if !errors.Is(err1, context.DeadlineExceeded) {
		t.Errorf("Caller 1 expected context timeout error, got: %v", err1)
	}

	if err2 != nil {
		t.Errorf("Caller 2 expected success, got error: %v (Cascading Context Cancellation bug!)", err2)
	}
}

// TestCachedEmbedder_ErrorNotCached ensures that external API errors (like 429 Rate Limits)
// are not accidentally cached as valid responses.
func TestCachedEmbedder_ErrorNotCached(t *testing.T) {
	mockErr := errors.New("HTTP 429 Too Many Requests")
	mock := &MockEmbedder{err: mockErr}
	cache := NewCachedEmbedder(mock, 10, 1*time.Hour)
	chunk := models.Chunk{Text: "error test"}
	
	// 1st call: Fails
	_, err := cache.EmbedChunk(context.Background(), chunk)
	if !errors.Is(err, mockErr) {
		t.Errorf("expected mock error, got %v", err)
	}
	
	// Fix the mock to succeed on the second try
	mock.err = nil
	
	// 2nd call: Should succeed
	_, err = cache.EmbedChunk(context.Background(), chunk)
	if err != nil {
		t.Errorf("expected success on retry, got %v", err)
	}
	
	if mock.callCount != 2 {
		t.Errorf("expected 2 API calls (1 fail, 1 success), got %d", mock.callCount)
	}
}

// TestCachedEmbedder_TableDrivenEdgeCases uses table-driven testing to assert
// behavior across various malformed or extreme inputs.
func TestCachedEmbedder_TableDrivenEdgeCases(t *testing.T) {
	tests := []struct {
		name      string
		chunkText string
		expectErr bool
	}{
		{"Empty String", "", false},
		{"Massive String", string(make([]byte, 1024*1024)), false}, // 1MB text
		{"Special Unicode", "ñ, 漢字, 🚀", false},
	}

	mock := &MockEmbedder{}
	cache := NewCachedEmbedder(mock, 10, 1*time.Hour)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := cache.EmbedChunk(context.Background(), models.Chunk{Text: tt.chunkText})
			if (err != nil) != tt.expectErr {
				t.Errorf("expected error: %v, got: %v", tt.expectErr, err)
			}
		})
	}
}

// TestCachedEmbedder_DeterministicHashing proves that exact string matches
// hit the cache while minute differences result in distinct keys.
func TestCachedEmbedder_DeterministicHashing(t *testing.T) {
	mock := &MockEmbedder{}
	cache := NewCachedEmbedder(mock, 10, 1*time.Hour)
	ctx := context.Background()

	cache.EmbedChunk(ctx, models.Chunk{Text: "Hello World"})
	
	// Identical text -> Cache Hit
	cache.EmbedChunk(ctx, models.Chunk{Text: "Hello World"})
	
	// Changed casing -> Cache Miss
	cache.EmbedChunk(ctx, models.Chunk{Text: "hello world"})
	
	// Changed whitespace -> Cache Miss
	cache.EmbedChunk(ctx, models.Chunk{Text: "Hello World "})

	if mock.callCount != 3 {
		t.Errorf("expected exactly 3 distinct hash evaluations (API calls), got %d", mock.callCount)
	}
}
