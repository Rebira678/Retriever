package pipeline

import (
	"context"
	"sync"
	"time"

	"github.com/Rebira678/Retriever/internal/embedder"
	"github.com/Rebira678/Retriever/internal/models"
)

// EmbedResult encapsulates the output of the embedding stage.
// It wraps both the successful Embedding and any errors, allowing downstream
// consumers (like a Dead Letter Queue) to handle failures gracefully rather
// than silently dropping data on the floor.
type EmbedResult struct {
	Embedding models.Embedding
	Err       error
	Chunk     models.Chunk // Kept for retry logic downstream
}

// EmbedPool orchestrates concurrent embedding of document chunks.
type EmbedPool struct {
	embedder   embedder.Embedder
	numWorkers int
}

// PoolOption applies configuration to an EmbedPool.
type PoolOption func(*EmbedPool)

// WithWorkers configures the number of concurrent workers in the pool.
func WithWorkers(n int) PoolOption {
	return func(p *EmbedPool) {
		if n > 0 {
			p.numWorkers = n
		}
	}
}

// NewEmbedPool initializes a worker pool using the functional options pattern.
func NewEmbedPool(emb embedder.Embedder, opts ...PoolOption) *EmbedPool {
	p := &EmbedPool{
		embedder:   emb,
		numWorkers: 4, // Sensible default throughput
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Run executes the worker pool, reading from chunksIn and writing to resultsOut.
// It blocks until chunksIn is closed and all workers have cleanly shut down.
func (p *EmbedPool) Run(ctx context.Context, chunksIn <-chan models.Chunk, resultsOut chan<- EmbedResult) {
	var wg sync.WaitGroup
	wg.Add(p.numWorkers)

	for i := 0; i < p.numWorkers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					// Context cancelled, initiate clean shutdown
					return
				case chunk, ok := <-chunksIn:
					if !ok {
						// Channel closed, drain complete
						return
					}

					emb, err := p.embedder.EmbedChunk(ctx, chunk)
					if err == nil && emb.CreatedAt.IsZero() {
						emb.CreatedAt = time.Now()
					}

					res := EmbedResult{
						Embedding: emb,
						Err:       err,
						Chunk:     chunk,
					}

					// Route the result downstream with context awareness
					select {
					case <-ctx.Done():
						return
					case resultsOut <- res:
					}
				}
			}
		}()
	}

	// Wait for all workers to finish processing their current chunks
	wg.Wait()
	close(resultsOut)
}
