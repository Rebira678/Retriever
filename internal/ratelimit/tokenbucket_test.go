package ratelimit

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestTokenBucket_Burst verifies that the bucket allows an immediate burst of requests up to its capacity.
func TestTokenBucket_Burst(t *testing.T) {
	// Capacity 3, Refill 1 per second (slow refill)
	bucket := NewTokenBucket(3, 1)

	ctx := context.Background()
	start := time.Now()

	// The first 3 should be allowed immediately (Burst)
	for i := 0; i < 3; i++ {
		err := bucket.Wait(ctx)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	elapsed := time.Since(start)
	if elapsed > 50*time.Millisecond {
		t.Errorf("Expected burst to complete immediately, took %v", elapsed)
	}
}

// TestTokenBucket_RateLimit verifies that requests exceeding the capacity are throttled
// and wait for exactly the correct refill duration.
func TestTokenBucket_RateLimit(t *testing.T) {
	// Capacity 1, Refill 10 per second (100ms per token)
	bucket := NewTokenBucket(1, 10)

	ctx := context.Background()

	// Drain the initial 1 token
	_ = bucket.Wait(ctx)

	start := time.Now()
	// This request must wait for a refill
	err := bucket.Wait(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	elapsed := time.Since(start)
	// It should take ~100ms. Allow some margin for scheduler jitter.
	if elapsed < 80*time.Millisecond || elapsed > 150*time.Millisecond {
		t.Errorf("Expected wait to be around 100ms, got %v", elapsed)
	}
}

// TestTokenBucket_ContextCancellation verifies that if a context is cancelled,
// the wait aborts immediately and the reserved token is properly refunded.
func TestTokenBucket_ContextCancellation(t *testing.T) {
	// Capacity 1, Refill 1 per second (1s per token)
	bucket := NewTokenBucket(1, 1)

	// Drain the initial token
	_ = bucket.Wait(context.Background())

	// Create a context that is already cancelled
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := bucket.Wait(ctx)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}

	elapsed := time.Since(start)
	if elapsed > 10*time.Millisecond {
		t.Errorf("Expected immediate return on canceled context, took %v", elapsed)
	}

	// Verify refund: If we hadn't refunded, the next token would be -1, 
	// requiring 2 seconds to get back to 1.
	// We'll advance time slightly to test that the bucket state wasn't permanently broken.
	bucket.mu.Lock()
	tokens := bucket.tokens
	bucket.mu.Unlock()

	// It should be roughly 0 (since it was refunded back from -1)
	if tokens < -0.1 || tokens > 0.1 {
		t.Errorf("Expected tokens to be refunded to ~0, got %f", tokens)
	}
}

// TestTokenBucket_ConcurrentWaiters verifies that multiple goroutines requesting
// tokens concurrently are queued up and served sequentially according to the refill rate.
func TestTokenBucket_ConcurrentWaiters(t *testing.T) {
	// Capacity 1, Refill 20 per second (50ms per token)
	bucket := NewTokenBucket(1, 20)

	// Drain initial token
	_ = bucket.Wait(context.Background())

	var wg sync.WaitGroup
	// We will launch 3 waiters.
	// Waiter 1 should take ~50ms
	// Waiter 2 should take ~100ms
	// Waiter 3 should take ~150ms
	start := time.Now()

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := bucket.Wait(context.Background())
			if err != nil {
				t.Errorf("Waiter %d failed: %v", id, err)
			}
		}(i)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// All 3 should complete in exactly ~150ms (3 * 50ms). 
	// The reservation math ensures they queue perfectly behind each other.
	if elapsed < 130*time.Millisecond || elapsed > 250*time.Millisecond {
		t.Errorf("Expected total concurrent wait to be ~150ms, got %v", elapsed)
	}
}
