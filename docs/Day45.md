# Day 45: Senior-Level Load Testing & System Architecture

## The Goal
To properly load-test the semantic search gRPC endpoint at increasing Requests Per Second (RPS) using `k6`, measuring P95 and P99 latencies.

However, a naive load test against an endpoint that relies on an external LLM embedding API (like OpenAI or Gemini) will immediately fail due to API rate limits (HTTP 429) or network bottlenecks. To conduct a *true* load test of our `pgvector` HNSW database index, we needed to architect an expert-level caching layer that intercepts duplicate queries without buckling under massive concurrent load.

## Architectural Deep Dive

### 1. Bounded Generic LRU Cache (OOM Prevention)
Using an unbounded Go map for caching is a memory leak waiting to happen. Under production load, storing every unique embedding query will eventually crash the container due to an Out-Of-Memory (OOM) error.
- **Solution:** Implemented a custom, thread-safe Least Recently Used (LRU) Cache (`internal/embedder/lru.go`) using Go 1.18+ Generics (`[K comparable, V any]`), `container/list`, and `sync.RWMutex`.
- **Result:** The cache strictly caps memory consumption (e.g., 10,000 items). Accessing an existing item promotes it to the front of the eviction list, ensuring popular queries remain in memory while the long tail is safely garbage-collected.

### 2. Singleflight: Cache Stampede Protection
During a high-traffic spike, 500 identical queries might hit the server simultaneously. Without protection, all 500 requests would miss the cache and hit the LLM API, resulting in a "Thundering Herd" or "Cache Stampede".
- **Solution:** Wrapped the cache lookup with `golang.org/x/sync/singleflight`. 
- **Result:** Only the *first* request executes the network call. The other 499 requests block gracefully and wait. Once the first request returns, all 500 requests share the single embedding result.

### 3. The "Context Cancellation" Gotcha
A major pitfall of `Singleflight` is handling client timeouts. If Caller A initiates the Singleflight request but disconnects, their canceled `context.Context` will abort the underlying HTTP call—causing Caller B, C, and D (who are waiting patiently) to fail as well.
- **Expert Fix:** We used `sf.DoChan()` combined with a `select` statement. The underlying API call is passed a detached context (`context.WithoutCancel(ctx)`). If Caller A drops, they immediately receive a `context.Canceled` error, but the background request completes successfully for the other waiting clients.

### 4. Connection Pool & gRPC Tuning
To survive 100+ RPS from `k6`:
- **PostgreSQL:** The default `pgxpool` limits are too low, causing massive queueing latency. We explicitly tuned `MaxConns=100`, `MinConns=10`, and `MaxConnIdleTime` to ensure the DB scales dynamically.
- **gRPC Server:** Added `grpc.MaxConcurrentStreams(1000)` and aggressive `keepalive.ServerParameters` to drop dead HTTP/2 streams from disconnected k6 simulated users, preventing file descriptor exhaustion.

### 5. Randomized `k6` Load Testing
If a load test sends the exact same query in a loop, it yields a 100% cache hit rate—meaning we aren't testing the database at all!
- **Solution:** The `k6-search-loadtest.js` script was written to cycle through a seeded array of randomized enterprise queries. This forces LRU cache evictions and guarantees we are measuring the true traversal latency of the `pgvector` HNSW index.

## Rigorous Testing Methodologies

To prove the architecture is flawless, we wrote a test suite employing advanced Go testing patterns:
1. **Fuzz Testing (`FuzzLRU`):** Uses the Go runtime to hammer the LRU cache with dynamically generated, malformed bytes and special Unicode to mathematically prove it will never panic.
2. **Race Detection:** Executed `go test -race` against 1,000 concurrent goroutines mutating the LRU cache, confirming zero deadlocks or data races.
3. **Error Cache Prevention:** Wrote tests asserting that transient external errors (e.g., HTTP 429) are *never* stored in the cache, allowing immediate retries once the API recovers.
4. **Context Propagation:** Proved via `TestCachedEmbedder_ContextCancellation` that impatient clients abort cleanly without polluting the Singleflight response for others.

## Conclusion
Today transformed a basic gRPC search endpoint into a resilient, highly concurrent, production-grade API capable of absorbing massive traffic spikes while fiercely protecting both database and external API resources.
