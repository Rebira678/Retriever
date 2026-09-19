package ratelimit

import (
	"context"
	"golang.org/x/time/rate"
)

// TokenBucket wraps the robust, well-tested standard library rate.Limiter.
// It replaces the previous manual timer-based generic cell rate algorithm implementation
// which suffered from an unbounded wait duration bug during mass context cancellations.
type TokenBucket struct {
	limiter *rate.Limiter
}

// NewTokenBucket creates a new token bucket with the specified burst capacity and refill rate.
func NewTokenBucket(capacity, refillRate float64) *TokenBucket {
	// rate.Limit is the float64 representation of events per second.
	// We use the standard library here because handling context cancellation safely
	// and accurately in a distributed rate limiter is notoriously difficult.
	return &TokenBucket{
		limiter: rate.NewLimiter(rate.Limit(refillRate), int(capacity)),
	}
}

// Wait blocks until one token is available, or the context is canceled.
// It delegates to the highly optimized standard library implementation,
// which perfectly handles wake-up ordering, cancellation, and precise timings.
func (b *TokenBucket) Wait(ctx context.Context) error {
	return b.limiter.Wait(ctx)
}
