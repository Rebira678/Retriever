package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Clock allows injecting a custom time source for deterministic testing.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// TokenBucket holds the rate-limiting state for outbound API calls.
// It implements an expert-level reservation-based algorithm to avoid polling,
// yielding exact wait times and O(1) complexity per request.
type TokenBucket struct {
	mu sync.Mutex

	capacity   float64 // maximum tokens the bucket can ever hold (burst allowance)
	tokens     float64 // tokens currently available (can go negative if reserved)
	refillRate float64 // tokens added per second, applied continuously
	lastRefill time.Time
	clock      Clock
}

// Option configures the TokenBucket.
type Option func(*TokenBucket)

// WithClock injects a custom clock for testing.
func WithClock(c Clock) Option {
	return func(b *TokenBucket) {
		b.clock = c
	}
}

// NewTokenBucket creates a bucket that starts completely full.
func NewTokenBucket(capacity, refillRate float64, opts ...Option) *TokenBucket {
	b := &TokenBucket{
		capacity:   capacity,
		tokens:     capacity,
		refillRate: refillRate,
		clock:      realClock{},
	}
	for _, opt := range opts {
		opt(b)
	}
	b.lastRefill = b.clock.Now()
	return b
}

// Wait blocks until one token is available, or the context is canceled.
// Instead of busy-polling, it mathematically computes the exact time until
// a token becomes available, reserves it, and sleeps exactly once.
func (b *TokenBucket) Wait(ctx context.Context) error {
	b.mu.Lock()

	now := b.clock.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()

	// Refill based on time elapsed
	b.tokens += elapsed * b.refillRate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.lastRefill = now

	// If we have a token, take it immediately and return
	if b.tokens >= 1.0 {
		b.tokens -= 1.0
		b.mu.Unlock()
		return nil
	}

	// Not enough tokens. Calculate exactly how long until 1 token is available.
	// Since tokens might be negative (due to queued reservations), this math
	// safely pushes later callers further into the future.
	tokensNeeded := 1.0 - b.tokens
	waitDuration := time.Duration((tokensNeeded / b.refillRate) * float64(time.Second))

	// Pre-deduct the token (Reservation) so the next caller is queued behind us.
	b.tokens -= 1.0
	b.mu.Unlock()

	// Wait for the exact time required
	timer := time.NewTimer(waitDuration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		// The request was cancelled while waiting in the queue.
		// We must refund our reserved token so we don't penalize other workers.
		b.refundToken()
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// refundToken returns a reserved token back to the bucket upon cancellation.
func (b *TokenBucket) refundToken() {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := b.clock.Now()
	elapsed := now.Sub(b.lastRefill).Seconds()

	b.tokens += elapsed * b.refillRate
	b.lastRefill = now

	// Refund the token
	b.tokens += 1.0
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
}
