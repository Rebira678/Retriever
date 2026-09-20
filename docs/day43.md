# Day 43: Horizontal Scaling, Distributed Locks, & Edge-Case Testing

## The Core Problem
On Day 42, we decoupled our business logic (pushing cryptographic hashing out of the database adapter and up to the domain models). Today, the focus shifted to **Distributed Systems Safety**.

If you run one instance of a Go pipeline, `sync.Mutex` and channels are enough to keep data safe. But what happens when you deploy 5 instances of the Retriever API behind a load balancer, and they all receive a massive batch of documents to ingest?

Without cross-process coordination, multiple worker instances will attempt to chunk and embed the exact same documents, resulting in:
1. Massive financial waste on duplicate LLM API calls.
2. Corrupted search results due to duplicated vectors in the database.

## The Senior-Level Solution: Postgres as a Distributed Lock
To safely scale our consumers horizontally, we utilized our PostgreSQL database as the central source of truth for idempotency. 

In `storage/postgres.go`, the `StartIngestion` function uses the `ON CONFLICT DO UPDATE` pattern:
```sql
INSERT INTO ingested_documents (hash, document_id, status)
VALUES ($1, $2, $3)
ON CONFLICT (hash) DO UPDATE 
SET status = $3, created_at = CURRENT_TIMESTAMP
WHERE ingested_documents.status = $4
RETURNING hash;
```
If two entirely separate Go servers try to process the same document at the exact same millisecond, the PostgreSQL transaction acts as the traffic cop. Server A successfully inserts the row and begins processing. Server B hits the `ON CONFLICT` rule, realizes the document is already in progress, and gracefully backs off without spending a single API token. 

## Proving it with Rigorous Edge-Case Testing
Building a distributed lock is step one. Proving it survives chaos is the actual job. We added an extensive suite of edge-case tests in `internal/pipeline/ingest_test.go`:

### 1. `TestRunBatchIngestion_PartialIdempotency`
We simulated a scenario where a batch of documents is submitted, but half of them have already been processed by another worker. 
**The Result:** The pipeline safely isolates the locked documents, skips them instantly, and successfully embeds and persists the new ones. Zero data collision.

### 2. `TestRunBatchIngestion_DatabaseError` (The Chaos Test)
We simulated a scenario where the database violently crashes in the middle of a massive batch ingestion while dozens of goroutines are spinning. 
**The Result:** Thanks to our `errgroup` architecture, the moment the database returns a failure, the pipeline context is cancelled. The test proves that the entire pipeline tears down and exits in less than 100 milliseconds. No zombie goroutines, no infinite hangs, no memory leaks.

## The Takeaway
A junior developer stops coding when the "happy path" works on their local machine. A senior engineer assumes the network will fail, the database will crash, and duplicate requests will spam the API. By relying on atomic distributed locks and structured `errgroup` concurrency, the system is now mathematically safe to scale horizontally across any number of nodes.
