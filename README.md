# Retriever 🚀

[![Go Reference](https://pkg.go.dev/badge/github.com/Rebira678/Retriever.svg)](https://pkg.go.dev/github.com/Rebira678/Retriever)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Rebira678/Retriever)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

**Retriever** is a high-throughput, fault-tolerant Retrieval-Augmented Generation (RAG) backend engineered in Go. It is designed to handle massive document ingestion and complex vector searches while maintaining strict SLA latencies and unbreakable resilience under load.

## 🎯 Architecture at a Glance

Retriever utilizes a **Scatter-Gather** pattern for search and a **Decoupled Worker Pool** for ingestion, heavily backed by OpenTelemetry for observability.

```mermaid
flowchart LR
    A[Client Request] -->|gRPC| B(Adaptive Load Shedder)
    B --> C{Hybrid Search}
    C -->|Vector <=>| D[(PostgreSQL pgvector)]
    C -->|Keyword @@| D
    C -->|Fuse| E(Reciprocal Rank Fusion)
    E --> F[Response]
```

## 🌟 Key Capabilities

### 1. High-Performance Ingestion Pipeline
* **Concurrent Worker Pools:** Aggressively parallelizes document chunking and LLM embedding using Go channels.
* **Token-Bucket Rate Limiting:** Actively prevents upstream API throttling (OpenAI/Gemini) before requests leave the server.

### 2. Fault Tolerance & Resilience
* **Atomic Circuit Breaker:** Lock-free (`sync/atomic`) state machine (`CLOSED` → `OPEN` → `HALF-OPEN`) that drops traffic at the edge during upstream outages, preventing catastrophic connection exhaustion.
* **Adaptive Load Shedding:** gRPC interceptors that dynamically reject requests based on active concurrent streams, protecting the server from HTTP/2 exhaustion.

### 3. Advanced Hybrid Search (RRF)
* **Scatter-Gather Execution:** Utilizes `errgroup` to simultaneously query `pgvector` for cosine similarity and `tsvector` for keyword relevance.
* **In-Memory Fusion:** Ranks and fuses results at the Go application layer using the Reciprocal Rank Fusion (RRF) algorithm, drastically reducing database CPU load.

### 4. Unified Observability Stack
* **The Holy Trinity:** Traces (Tempo), Metrics (Prometheus), and Logs (Loki) routed through an OpenTelemetry Collector.
* **Log-to-Trace Correlation:** A custom `slog.Handler` automatically injects W3C Trace IDs into JSON logs (1.5µs overhead) for flawless debugging.

---

## ⚡ Quick Start

### 1. Launch the Observability Stack
Start PostgreSQL (`pgvector`), Grafana, Loki, Tempo, Prometheus, and the OTel Collector:
```bash
docker-compose up -d
```

### 2. Configure Environment
```bash
cp .env.example .env
# Set RETRIEVER_GEMINI_API_KEY or RETRIEVER_OPENAI_API_KEY
```

### 3. Start the RAG Server
```bash
go run ./cmd/retriever
```

### 4. View Telemetry (Grafana)
Run the load generator to simulate traffic and trigger the circuit breaker:
```bash
go run ./cmd/trace-generator
```
Navigate to **http://localhost:3000** to view the correlated Service Maps, Traces, and Logs.

---

## 📚 Deep Dives

- 🏗 **[System Architecture (ADRs)](ARCHITECTURE.md)**: Detailed breakdown of the pipeline topology and engineering decisions.
- 📊 **[Performance Benchmarks](BENCHMARKS.md)**: CPU, Memory, and Latency profiling results.

## 📜 License
MIT License. See [LICENSE](LICENSE) for details.
