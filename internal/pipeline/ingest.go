package pipeline

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"

	"github.com/Rebira678/Retriever/internal/chunker"
	"github.com/Rebira678/Retriever/internal/config"
	"github.com/Rebira678/Retriever/internal/embedder"
	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/storage"
)

// Pipeline orchestrates the end-to-end RAG ingestion process.
type Pipeline struct {
	cfg      *config.Config
	store    storage.Storage
	embedder embedder.Embedder
	chunker  *chunker.Chunker
	pool     *EmbedPool
}

// NewPipeline constructs a new Ingestion Pipeline with necessary dependencies.
func NewPipeline(cfg *config.Config, store storage.Storage, emb embedder.Embedder, c *chunker.Chunker) *Pipeline {
	return &Pipeline{
		cfg:      cfg,
		store:    store,
		embedder: emb,
		chunker:  c,
		pool:     NewEmbedPool(emb, WithWorkers(cfg.WorkerPoolSize)),
	}
}

// RunIngestion executes the full RAG ingestion pipeline for a single document.
func (p *Pipeline) RunIngestion(parentCtx context.Context, doc models.Document, provider string, modelName string) error {
	// 0. Prevent Goroutine Leaks
	// Create a derived context so that if the pipeline errors out early, 
	// all spawned worker goroutines are explicitly cancelled and cleaned up.
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()
	// 1. Calculate Estimated Chunk Count (Zero Allocation)
	estimatedChunks := p.chunker.EstimatedChunkCount(len(doc.Content))
	slog.Info("Prepared document for streaming", "document_id", doc.ID, "estimated_chunks", estimatedChunks)

	// 2. Check Idempotency Lock
	docHashRaw := fmt.Sprintf("%s:%s:%s", provider, modelName, doc.Content)
	docHash := fmt.Sprintf("%x", sha256.Sum256([]byte(docHashRaw)))

	acquired, err := p.store.StartIngestion(ctx, docHash, doc.ID)
	if err != nil {
		return fmt.Errorf("failed to check idempotency: %w", err)
	}
	if !acquired {
		slog.Info("Document already ingested or in progress (Idempotency hit), skipping processing", "hash", docHash, "document_id", doc.ID)
		return nil
	}

	// 3. Backpressure Queues
	chunkChan := make(chan models.Chunk, p.cfg.IngestionQueueSize)
	resultsChan := make(chan EmbedResult, p.cfg.IngestionQueueSize)

	// 5. Producer Goroutine (Zero Allocation Streaming)
	go func() {
		p.chunker.StreamChunks(ctx, doc.Content, doc.ID, chunkChan)
	}()

	// 6. Consumer Worker Pool
	go func() {
		p.pool.Run(ctx, chunkChan, resultsChan)
	}()

	// 7. Aggregate and Batch Persist (Streaming Architecture)
	const batchSize = 100
	var batch []models.Embedding
	var deadLetters []models.DeadLetter
	batch = make([]models.Embedding, 0, batchSize)

	totalSaved := 0

	for res := range resultsChan {
		if res.Err != nil {
			slog.Error("Failed to embed chunk", "chunk_index", res.Chunk.Index, "error", res.Err)
			deadLetters = append(deadLetters, models.DeadLetter{
				DocumentID: doc.ID,
				ChunkIndex: res.Chunk.Index,
				Error:      res.Err.Error(),
				Payload:    res.Chunk.Text,
			})
			continue
		}
		
		batch = append(batch, res.Embedding)
		
		// Flush batch when it reaches capacity
		if len(batch) >= batchSize {
			if err := p.store.SaveEmbeddings(ctx, batch); err != nil {
				slog.Error("Failed to save batch of embeddings to database", "error", err)
				_ = p.store.CompleteIngestion(ctx, docHash, models.StatusFailed)
				return err
			}
			totalSaved += len(batch)
			// Reuse the same backing array for zero-allocation batching
			batch = batch[:0] 
		}
	}

	// Persist remaining items in the final partial batch
	if len(batch) > 0 {
		if err := p.store.SaveEmbeddings(ctx, batch); err != nil {
			slog.Error("Failed to save final batch of embeddings to database", "error", err)
			_ = p.store.CompleteIngestion(ctx, docHash, models.StatusFailed)
			return err
		}
		totalSaved += len(batch)
	}

	slog.Info("Concurrent embedding and batch persistence completed", "total_saved", totalSaved)

	// 8. Handle DLQ
	if len(deadLetters) > 0 {
		if err := p.store.SaveDeadLetters(ctx, deadLetters); err != nil {
			slog.Error("Failed to save to Dead Letter Queue", "error", err)
		} else {
			slog.Info("Saved failed chunks to Dead Letter Queue", "count", len(deadLetters))
		}
	}

	// 9. Mark Status
	if totalSaved > 0 {
		if err := p.store.CompleteIngestion(ctx, docHash, models.StatusCompleted); err != nil {
			slog.Error("Failed to mark document as completed", "error", err)
			return err
		}
	} else {
		_ = p.store.CompleteIngestion(ctx, docHash, models.StatusFailed)
		return fmt.Errorf("all chunks failed to embed")
	}

	return nil
}
