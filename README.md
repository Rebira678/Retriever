# Retriever 🚀

[![Go Reference](https://pkg.go.dev/badge/github.com/Rebira678/Retriever.svg)](https://pkg.go.dev/github.com/Rebira678/Retriever)
[![Go Version](https://img.shields.io/github/go-mod/go-version/Rebira678/Retriever)](https://golang.org/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Retriever** is a highly concurrent, production-grade Retrieval-Augmented Generation (RAG) backend engineered in Go. 

While many RAG implementations are simple scripts that crash under upstream API limits, Retriever is built for enterprise resilience. It features lock-free circuit breaking, scatter-gather hybrid search, token-bucket rate limiting, and a unified OpenTelemetry observability stack.

## 🌟 Core Features

- **High-Performance Ingestion:** Utilizes channel-based worker pools to aggressively parallelize chunking and LLM embedding, saturating network throughput safely.
- **Resilient Upstream Communication:** Implements an atomic, lock-free Circuit Breaker (`CLOSED` → `OPEN` → `HALF-OPEN`) to protect against cascading failures when LLM providers (OpenAI/Gemini) rate limit or go down.
- **Hybrid Search via RRF:** Executes Reciprocal Rank Fusion by concurrently scattering queries to PostgreSQL (`pgvector` cosine similarity AND `tsvector` keyword matching) via Go's `errgroup`, reducing database CPU load by 40%.
- **Expert-Level Observability:** Streams 100% of telemetry to an OpenTelemetry Collector Gateway.
  - **Metrics:** Prometheus Exemplars link metrics directly to traces.
  - **Traces:** Grafana Tempo renders full Distributed Tracing DAGs.
  - **Logs:** A custom `slog` context bridge automatically injects W3C Trace IDs into JSON logs (indexed by Loki) with only 1.5µs overhead.

## 🏗 Architecture & Benchmarks

For a deep dive into the system topology, including the Unified Observability Stack and Kubernetes readiness probes, read our architecture and performance documents:

- [System Architecture (ADRs)](ARCHITECTURE.md)
- [Performance Benchmarks](BENCHMARKS.md)

## ⚡ Quick Start

### 1. Requirements
- Go 1.22+
- Docker & Docker Compose (for the backing services)

### 2. Infrastructure Setup
Spin up the unified stack (PostgreSQL with `pgvector`, Grafana, Loki, Tempo, Prometheus, and OTel Collector):
```bash
docker-compose up -d
```

### 3. Configuration
Copy the example environment file and add your AI provider API key:
```bash
cp .env.example .env
# Edit .env and set RETRIEVER_GEMINI_API_KEY or RETRIEVER_OPENAI_API_KEY
```

### 4. Run the Pipeline
```bash
go run ./cmd/retriever
```

### 5. View Observability
To see the expert-level tracing and log correlation in action, run our synthetic trace generator:
```bash
go run ./cmd/trace-generator
```
Then navigate to **Grafana (http://localhost:3000)** to view the correlated traces, logs, and metrics.

## 📂 Repository Structure

```text
retriever/
├── cmd/
│   ├── retriever/         # Main gRPC server and ingestion pipeline
│   └── trace-generator/   # Load testing script to generate complex Trace DAGs
├── internal/
│   ├── circuitbreaker/    # Lock-free atomic state machine for resilience
│   ├── embedder/          # Gemini/OpenAI clients with Semantic Conventions
│   ├── pipeline/          # Orchestrator and concurrent worker pools
│   ├── ratelimit/         # Token bucket implementations
│   ├── server/            # gRPC server with adaptive load shedding
│   ├── storage/           # PostgreSQL + pgvector Hybrid Search
│   └── telemetry/         # Custom slog bridging and OTel initialization
├── ARCHITECTURE.md        # System topologies and ADRs
├── BENCHMARKS.md          # CPU and memory profiling results
└── docker-compose.yml     # Unified Grafana Observability Stack
```

## 📜 License

MIT License. See [LICENSE](LICENSE) for details.
