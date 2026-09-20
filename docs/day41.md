# Day 41: Batch vs. Streaming Ingestion Trade-offs

## The Core Problem
In the previous days, we built a highly optimized **O(1) Streaming Ingestion Pipeline**. That pipeline was designed for a very specific use case: ingesting a single, massive document (like a 10,000-page PDF). By streaming the chunks as they were generated, we kept memory usage completely flat. 

However, system architecture is all about trade-offs. What happens when a cron job attempts to ingest **1,000 small, one-page documents** all at once? 

If we use the streaming endpoint (`RunIngestion`), the system loops through the documents sequentially. It establishes a database lock, sets up a producer goroutine, initializes a consumer worker pool, waits for it to finish, and tears it down—**1,000 separate times**. This sequential processing creates massive overhead and completely bottlenecks throughput.

## The Senior-Level Solution: A Dedicated Batch Endpoint
To solve this, we implemented `RunBatchIngestion`. This endpoint shifts the architecture from "O(1) memory conservation" to **"Maximum CPU and DB Throughput."**

### 1. Bounded Fan-Out (The Producer)
Instead of processing documents sequentially, we fan out. We iterate through the array of documents and spawn lightweight goroutines to chunk them concurrently. However, if we spawned 1,000 goroutines that instantly queried Postgres for an idempotency lock, we would cause **connection pool exhaustion** and crash the database. 

To prevent this, we introduced a **Bounded Semaphore**:
```go
sem := make(chan struct{}, 10)
```
This physical gate ensures that no matter how large the batch is, exactly 10 goroutines are allowed to check the database lock at any given microsecond. 

### 2. Fan-In to a Shared Worker Pool
Rather than building 1,000 separate channels and worker pools, we funnel every single chunk from all 1,000 documents into one centralized `chunkChan`. 

This fully saturates the concurrent embedding workers. The workers don't care which document a chunk came from; they just pull from the channel, embed it, and pass it down the line. 

### 3. Structured Concurrency (`errgroup`)
Finally, we wrapped the entire pipeline in `golang.org/x/sync/errgroup`. 
```go
g, ctx := errgroup.WithContext(parentCtx)
```
If any stage of this massive batch operation fails catastrophically (e.g., the Postgres server crashes), the `errgroup` instantly cancels the context. Every single producer and consumer goroutine receives the cancellation signal and shuts down gracefully in less than a millisecond, preventing memory leaks and zombie processes. 

## The Takeaway
A junior engineer looks for the "best" way to ingest data. A senior engineer understands that there is no "best" way—only trade-offs. By supporting both O(1) streaming and high-throughput batching, our system is now flexible enough to handle any scale of workload efficiently.
