# Deep Architectural Analysis: Production-Grade RAG Observability

As a Senior AI Infrastructure Engineer, I am evaluating our current Day 50 OpenTelemetry implementation. While we have successfully added Spans and Context Propagation (including asynchronous Span Linking), this is only the first pillar of observability. A true production system requires a triad of Logs, Metrics, and Traces, intertwined intelligently.

## Current Architectural Deficiencies

1. **The Database Blind Spot (No SQL Correlation):**
   - **Problem:** When a database query is slow, a DBA looking at PostgreSQL's `pg_stat_statements` only sees the SQL string. They have no way to map that slow query back to the specific HTTP request or RAG document ingestion trace that caused it.
   - **Senior Solution:** Implement **pgx tracing hooks**. We must inject the W3C `traceparent` or natively trace DB connections so that the DB layer emits spans automatically for every query. This bridges the gap between Go tracing and DB infrastructure.

2. **Metrics & Tracing Disconnect (No Exemplars):**
   - **Problem:** We have spans, but no RED (Rate, Errors, Duration) metrics. If the P99 latency spikes, we have to search blindly through Jaeger for a slow trace.
   - **Senior Solution:** Implement **OTel Metrics** (Meters, Histograms). We need an active counter of processed chunks and a histogram of latency to alert on.

3. **Resilience Blindness (Unmonitored Circuit Breakers):**
   - **Problem:** We have a Circuit Breaker protecting the LLM provider, but its state transitions (Closed -> Open) are invisible to our telemetry. If the AI gateway starts failing over, we won't know until users complain.
   - **Senior Solution:** Instrument the `circuitbreaker.go`. Every time a request is blocked (Circuit Open), we must record a metric `circuit_breaker_drops_total` and append a span event to the active trace.

4. **Multi-Tenant State Pollution (Lack of Baggage):**
   - **Problem:** In a multi-tenant RAG system, every metric and span must be tagged with the `tenant_id` or `document_id`. Passing this down through 10 layers of function signatures is junior-level pollution.
   - **Senior Solution:** Utilize **OpenTelemetry Baggage**. We inject the `document_id` at the pipeline's entry point, and OTel automatically propagates this key-value pair to every downstream network call, database query, and metric.

## Implementation Execution Plan

1. **Upgrade `telemetry/tracer.go`**: Add OTel Meter Provider setup to export metrics alongside traces.
2. **Instrument `internal/storage/postgres.go`**: Add `exatract/inject` logic or `otelsql`/`pgx` tracer to automatically generate DB spans.
3. **Instrument `internal/circuitbreaker/circuitbreaker.go`**: Add span events and status updates for circuit breaker state transitions.
4. **Instrument `internal/pipeline/ingest.go`**: Add OTel Baggage injection for seamless `document_id` propagation.

I will now proceed to implement these expert-level patterns directly into the codebase.
