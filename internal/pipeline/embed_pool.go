package pipeline

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/Rebira678/Retriever/internal/embedder"
	"github.com/Rebira678/Retriever/internal/models"
)

// EmbedPool manages a pool of workers that embed chunks concurrently.
// It acts as a backpressure-aware pipeline stage.
type EmbedPool struct {
	embedder   embedder.Embedder
	numWorkers int
}

// NewEmbedPool creates a new EmbedPool with the given embedder and worker count.
func NewEmbedPool(emb embedder.Embedder, numWorkers int) *EmbedPool {
	if numWorkers <= 0 {
		numWorkers = 1
	}
	return &EmbedPool{
		embedder:   emb,
		numWorkers: numWorkers,
	}
}

// Run starts the worker pool. It reads chunks from chunkChan, embeds them,
// and sends the resulting embeddings to embeddingChan.
// It blocks until chunkChan is closed and all workers finish, then it closes embeddingChan.
func (p *EmbedPool) Run(ctx context.Context, chunkChan <-chan models.Chunk, embeddingChan chan<- models.Embedding) {
	var wg sync.WaitGroup

	for i := 0; i < p.numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					// Context cancelled, stop worker
					return
				case chunk, ok := <-chunkChan:
					if !ok {
						// chunkChan closed, no more work
						return
					}
					
					// Embed the chunk
					emb, err := p.embedder.EmbedChunk(ctx, chunk)
					if err != nil {
						// For now, log the error and drop the chunk.
						// Day 39 will introduce a Dead Letter Queue (DLQ) for failed embeddings.
						slog.Error("Worker failed to embed chunk", "worker_id", workerID, "chunk_index", chunk.Index, "error", err)
						continue
					}

					// Ensure timestamp is set
					if emb.CreatedAt.IsZero() {
						emb.CreatedAt = time.Now()
					}

					// Send the embedding to the next stage
					select {
					case <-ctx.Done():
						return
					case embeddingChan <- emb:
						// Successfully sent to the next stage
					}
				}
			}
		}(i)
	}

	// Wait for all workers to finish processing
	wg.Wait()
	// Close the output channel to signal downstream stages that embedding is complete
	close(embeddingChan)
}
