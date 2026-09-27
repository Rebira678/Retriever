# Day 50: Distributed Tracing & Observability with OpenTelemetry

## Concept to Master: Distributed Tracing (Spans, Context Propagation & Span Linking)
In microservice and high-throughput concurrent architectures, observing request flow across boundaries is critical for diagnosing latency bottlenecks and errors. A **Distributed Trace** is a collection of spans representing the end-to-end journey of a request.
- A **Span** is a single unit of work (e.g., embedding a chunk, a database transaction).
- **Trace Context** is propagated across service boundaries (via HTTP Headers, gRPC metadata, or Go context) to stitch spans together under a single Trace ID.
- **Span Links** connect causally related spans that don't have a direct parent-child relationship (crucial for Scatter-Gather or batch persistence architectures).

## Architecture & Senior-Level Trade-offs

### 1. Context Propagation Across Worker Pools (Channel Boundaries)
A junior approach to tracing merely passes the parent request context into a worker pool. This breaks down when tracing individual items being processed asynchronously.
- **Senior Implementation:** I introduced a `ChunkTask` struct that wraps both the `models.Chunk` and its dedicated `trace.SpanContext`. When the producer generates chunks, it instantiates a discrete `Pipeline.ProcessChunk` span for each chunk. This context is carried *over the channel* to the worker pool, ensuring the Gemini API `EmbedChunk` network call correctly appears as a child of that specific chunk's span.

### 2. Expert Pattern: Span Linking for Scatter-Gather Bulk Inserts
Our pipeline batches 100 chunk embeddings into a single PostgreSQL `pgx.Batch` insert for maximum throughput.
- **The Problem:** A single bulk insert represents the culmination of 100 independent chunk processing spans. A standard parent-child span structure cannot represent 1 child having 100 parents.
- **Senior Implementation:** I utilized OpenTelemetry **Span Links** (`trace.WithLinks`). When the batch buffer flushes, we extract the `SpanContext` from all 100 chunks and link them to the `Pipeline.BatchSave` span. In Jaeger, this correctly visualizes the causal fan-in graph, mapping the database latency back to the exact chunks it persisted.

### 3. Vendor-Neutral Telemetry (OTLP)
Instead of coupling our pipeline to Datadog or New Relic SDKs, we implemented the OpenTelemetry (OTLP) standard.
- **Trade-off:** slightly steeper learning curve and setup overhead than vendor-specific agents.
- **Benefit:** zero vendor lock-in. We export via gRPC (`localhost:4327`) to Jaeger for local development, but in production, we can swap to any OTLP-compliant collector (Grafana Tempo, Honeycomb) by merely changing an environment variable, without modifying code.

### 4. Sampling Strategy vs. Performance
Currently, we use `AlwaysSample()` for local visibility.
- **Production Trade-off:** At massive scale (millions of chunks/day), sampling 100% of traces causes severe CPU/memory overhead and inflates storage costs in the telemetry backend.
- **Future Path:** In production, we would deploy an OpenTelemetry Collector acting as a gateway using **Tail-based Sampling** (sampling only traces that contain errors or exhibit high p99 latency), completely offloading the sampling decision from our application CPU.

## Implementation Details

1. **`telemetry` Package:** Encapsulates the OTLP initialization boilerplate, setting the global `TracerProvider` and configuring W3C `TraceContext` and `Baggage` propagators.
2. **Ingestion Pipeline (`internal/pipeline/ingest.go` & `embed_pool.go`):** Tracks the overarching ingestion process. Overhauled channel structures to pass `ChunkTask` structs. Built sophisticated batch span-linking for PostgreSQL inserts.
3. **Embedder (`internal/embedder/gemini.go`):** Traces external HTTP requests. This allows us to observe the exact API latency imposed by Gemini, differentiating internal DB slowness from external API throttling.
4. **Storage (`internal/storage/postgres.go`):** Wraps high-latency operations like `SaveEmbeddings`, `SearchSimilar`, and `SearchHybrid` with spans. Tracks query length and the number of results found to correlate batch size with database latency.
5. **Infrastructure (`docker-compose.yml`):** Added `jaegertracing/all-in-one` to effortlessly collect and visualize traces locally.

## What's Next
Run `docker-compose up -d` to launch the database and Jaeger collector. Fire an ingestion request or perform a hybrid search, then navigate to `http://localhost:16686` to visualize the intricate, linked fan-out/fan-in graph of your pipeline execution in real-time.
