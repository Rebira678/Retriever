package ratelimit

import (
	"context"

	"github.com/Rebira678/Retriever/internal/embedder"
	"github.com/Rebira678/Retriever/internal/models"
)

// RateLimitedEmbedder wraps an Embedder and enforces a rate limit
// using the TokenBucket algorithm before forwarding the request.
// It acts as a client-side throttle, blocking efficiently until a token is available.
type RateLimitedEmbedder struct {
	inner  embedder.Embedder
	bucket *TokenBucket
}

// NewRateLimitedEmbedder creates a new RateLimitedEmbedder.
func NewRateLimitedEmbedder(inner embedder.Embedder, bucket *TokenBucket) *RateLimitedEmbedder {
	return &RateLimitedEmbedder{
		inner:  inner,
		bucket: bucket,
	}
}

// EmbedChunk blocks until the rate limiter allows the request, then delegates to the inner embedder.
func (r *RateLimitedEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	if err := r.bucket.Wait(ctx); err != nil {
		return models.Embedding{}, err
	}
	return r.inner.EmbedChunk(ctx, chunk)
}
