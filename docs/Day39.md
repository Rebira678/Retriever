# Day 39: Dead Letter Queue (DLQ)

Today's focus was on the **Dead Letter Queue (DLQ)** pattern, addressing what happens when a chunk persistently fails to embed (due to malformed text, repeated network timeouts, or unrecoverable rate limits).

## The Problem: Silent Data Loss
Before today, if 99 chunks of a document embedded successfully, but 1 chunk failed, that chunk was simply logged as an error and dropped. The document would be marked as "COMPLETED" in our idempotency tracker, but a piece of its context was permanently missing. We had a "silent hole" in our search index.

## What We Built
Instead of dropping failed chunks, we implemented a system to catch them:
- **`models.DeadLetter` Struct**: A new domain model to capture the failed `ChunkIndex`, `DocumentID`, the raw `Payload` (text), and the `ErrorMessage`.
- **`dead_letters` Postgres Table**: A dedicated table initialized in `postgres.go` to securely persist these failures.
- **Pipeline Integration**: In the `internal/pipeline/ingest.go` Orchestrator, the worker pool's `resultsChan` now separates successes from failures. If the embedder returns an error, the chunk is wrapped in a `DeadLetter` struct.
- **Batch Processing**: At the end of the pipeline run, all accumulated `DeadLetter`s are efficiently flushed to Postgres in a single transaction using `pgx.Batch` inside `SaveDeadLetters()`.

## Why This Matters
In distributed systems, **engineering for failure is more important than the happy path**. 
By capturing failed chunks in a DLQ, we achieve **Zero Chunk Loss**. If an external API goes down during ingestion, the system doesn't crash, and we don't lose the data. Instead, the failed payloads are safely waiting in the "Lost & Found" (the database) to be reviewed or re-processed later. This represents a mature, production-grade approach to data integrity.
