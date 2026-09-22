package embedder

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/Rebira678/Retriever/internal/models"
	"golang.org/x/sync/singleflight"
)

// CachedEmbedder wraps an Embedder with an LRU cache and Singleflight pattern.
// Senior Architecture Patterns Applied:
// 1. Singleflight: Prevents Cache Stampedes (Thundering Herd) during high-RPS spikes.
// 2. Bounded LRU: Prevents Out-Of-Memory (OOM) crashes by strictly capping cache size.
// 3. Telemetry: Exposes cache hit/miss metrics for observability.
type CachedEmbedder struct {
	underlying Embedder
	lru        *LRUCache[string, cacheEntry]
	sf         singleflight.Group
	ttl        time.Duration

	// Observability metrics (Atomic to prevent race conditions without locking)
	hits   atomic.Uint64
	misses atomic.Uint64
}

type cacheEntry struct {
	embedding models.Embedding
	expiresAt time.Time
}

// NewCachedEmbedder creates a production-grade CachedEmbedder.
func NewCachedEmbedder(underlying Embedder, capacity int, ttl time.Duration) *CachedEmbedder {
	return &CachedEmbedder{
		underlying: underlying,
		lru:        NewLRUCache[string, cacheEntry](capacity),
		ttl:        ttl,
	}
}

// EmbedChunk retrieves an embedding from the cache, or computes it ensuring only
// one in-flight request goes to the external LLM provider per identical query.
func (c *CachedEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	hash := sha256.Sum256([]byte(chunk.Text))
	key := hex.EncodeToString(hash[:])

	// 1. Fast Path: Check LRU Cache
	if entry, found := c.lru.Get(key); found {
		if time.Now().Before(entry.expiresAt) {
			c.hits.Add(1)
			return entry.embedding, nil
		}
	}

	c.misses.Add(1)

	// 2. Slow Path: Singleflight mechanism with Context Awareness
	// EXPERT ARCHITECTURE: We use DoChan to allow the caller to abort if their specific request
	// times out. We MUST use `context.WithoutCancel(ctx)` for the underlying API call so that if the 
	// FIRST caller disconnects, it doesn't cancel the inflight request for the other 99 waiting callers.
	ch := c.sf.DoChan(key, func() (interface{}, error) {
		// Use a detached context for the network request to prevent cascading failures
		detachedCtx := context.WithoutCancel(ctx)
		
		emb, err := c.underlying.EmbedChunk(detachedCtx, chunk)
		if err != nil {
			return nil, err
		}

		c.lru.Add(key, cacheEntry{
			embedding: emb,
			expiresAt: time.Now().Add(c.ttl),
		})

		return emb, nil
	})

	select {
	case <-ctx.Done():
		// If this specific caller times out or disconnects, immediately return to free up the goroutine,
		// without waiting for the singleflight to finish.
		return models.Embedding{}, ctx.Err()
	case res := <-ch:
		if res.Err != nil {
			return models.Embedding{}, res.Err
		}
		return res.Val.(models.Embedding), nil
	}
}

// Metrics returns the cache hit/miss statistics.
func (c *CachedEmbedder) Metrics() (hits, misses uint64) {
	return c.hits.Load(), c.misses.Load()
}
