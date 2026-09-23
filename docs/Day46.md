# Day 46: Expert-Level Resilience — Circuit Breakers & Load Shedding

## The Scenario: The Breaking Point 📉

Imagine this:

You've built the world's most efficient car factory (the RAG pipeline). You have bounded queues, parallel workers, and an optimized HNSW map (pgvector index). During your load test on Day 45, you successfully hit 100 Requests Per Second (RPS) with a 99% cache hit rate.

But what happens when the factory experiences a catastrophic external event?

### The Two Catastrophes:

1. **The DB Outage (The Broken Conveyor Belt):** 
   Suddenly, your database connection pool dies or PostgreSQL undergoes maintenance. Every single search request now stalls for 5 seconds waiting for a connection timeout. 
   **The Result:** Your gRPC server accumulates 5,000 stalled connections. The server runs out of RAM, completely crashes, and cannot even serve cache hits!

2. **The Traffic Tsunami (The Black Friday Event):**
   A viral news event occurs. Instead of 100 RPS, you receive 10,000 RPS. Even with your Singleflight LRU Cache, the 10,000 concurrent gRPC requests overwhelm your CPU and memory allocator.
   **The Result:** The server becomes unresponsive.

## Expert System Architecture Solutions

To solve these, we implemented two expert-level architectural patterns today:

### 1. The Circuit Breaker Pattern (Failing Fast)

When a downstream service (like PostgreSQL) is failing, continuing to send it requests will only make it worse (and burn up your own server's resources waiting for timeouts). 

**The Expert Fix:** We implemented a highly-concurrent, **lock-free** `CircuitBreaker` using Go's `sync/atomic` state machines in `internal/circuitbreaker/circuitbreaker.go`.
- **Closed (Normal):** Requests flow freely. State checks use atomic operations (`atomic.LoadUint32`), avoiding expensive mutex locks on the hot path (crucial for high RPS).
- **Open (Tripped):** After 5 consecutive database errors, the circuit *opens*. New requests are instantly rejected with `Unavailable` *without* contacting the database.
- **Half-Open (Testing):** After a timeout, the circuit allows *one* probe request through using atomic Compare-And-Swap (CAS). If it succeeds, the circuit closes. If it fails, it opens again.

By wrapping our `store.SearchSimilar` call in the circuit breaker, a database failure no longer takes down the gRPC server. The server instantly sheds the database queries and can continue serving 100% of cache hits!

### 2. Adaptive Load Shedding (AIMD Concurrency Limiter)

You can tune your connection pool and your max streams, but a static concurrency limit (e.g., 500) is a guess. If the system is fast, it might handle 1000. If it's degraded, 500 might be too much.

**The Expert Fix:** We added an `AdaptiveLoadSheddingInterceptor` using the **AIMD (Additive Increase, Multiplicative Decrease)** algorithm — the same algorithm that powers TCP congestion control and Netflix's load limiters.
- It constantly monitors the Exponential Moving Average (EMA) of request latency (Round Trip Time).
- If latencies are healthy (< 200ms) and requests succeed, the concurrency limit **Additively Increases** (probes for more capacity).
- If latencies spike (> 200ms) or requests fail due to timeouts, the concurrency limit **Multiplicatively Decreases** (drops by 10%).

This dynamic mechanism guarantees that the server always operates at optimal throughput without degrading into queueing theory collapse. When a traffic tsunami hits, excess requests are instantly rejected with `codes.ResourceExhausted`, keeping the server perfectly stable.

## Conclusion

By combining the **Bounded LRU Cache**, **Singleflight**, **Keepalives** (Day 45) with **Circuit Breakers** and **Load Shedding Semaphores** (Day 46), our Retriever Search API has evolved from a basic endpoint into an industrial-grade, highly resilient microservice. It can absorb massive traffic spikes and gracefully degrade during infrastructure failures.
