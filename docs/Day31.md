# Day 31: Worker Pool for Batch Processing

Welcome to Day 31 of the roadmap! Today we built the engine that powers high-throughput embedding: the **Worker Pool**.

## The Problem: The Factory Floor Bottleneck

As established in our `project_with_scenario.md`, our ingestion pipeline resembles a factory floor. 
1. **Chunking** is incredibly fast (nanoseconds per chunk).
2. **Embedding** is incredibly slow (network requests to OpenAI take 100ms to 2s).

If we process chunks sequentially in a loop, the fast chunker has to sit and wait for each slow embedding API call to complete. This is like a high-speed factory conveyor belt stopping completely while one worker slowly inspects a single product.

## The Solution: Concurrent Go Workers

Today, we built `pipeline.EmbedPool` in `internal/pipeline/embed_pool.go` to solve this using Go's concurrency primitives: Goroutines and Channels.

Instead of one worker, we now have a **Worker Pool**:
- `chunkChan <-chan models.Chunk`: The bounded channel that receives chunks from the chunking stage.
- `embeddingChan chan<- models.Embedding`: The bounded channel where completed embeddings are sent for the storage stage.
- `numWorkers`: A configurable number of concurrent goroutines (defaulting to 4) that pull from the `chunkChan`, make the API call in parallel, and push to `embeddingChan`.

This allows us to process 4 (or N) embeddings simultaneously, effectively multiplying our throughput and fully utilizing the network bandwidth while providing **Backpressure**. If the `embeddingChan` gets full, the workers pause, and subsequently, the `chunkChan` gets full, making the chunker pause — automatically preventing memory explosions without complex logic.

## What was built:
1. **`internal/pipeline/embed_pool.go`**: The core worker logic utilizing `sync.WaitGroup` and `select` statements for clean context cancellation.
2. **`internal/pipeline/embed_pool_test.go`**: Unit tests that use a mock embedder with an artificial delay to mathematically prove that the pool is processing concurrently and respecting context timeouts.
3. **`cmd/retriever/main.go`**: Wired up the worker pool to replace the single-chunk demo from yesterday. Now the demo concurrent embeds all chunks in parallel!

## Commands to Verify
Run the test suite to see the concurrent timing logic in action:
```bash
make test
```

Run the pipeline (requires the API key):
```bash
export RETRIEVER_OPENAI_API_KEY="sk-..."
make run
```
