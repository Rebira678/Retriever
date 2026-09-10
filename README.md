# Retriever — RAG Ingestion Pipeline

A production-grade RAG (Retrieval-Augmented Generation) ingestion pipeline built in Go. Ingests documents, chunks them using configurable strategies, generates embeddings through a concurrent worker pool, stores vectors in Postgres (pgvector), and exposes a semantic search API.

**Project 2 of 2** in a 60-day public build sprint. Companion project: [Router](https://github.com/Rebira678/router) (AI Inference Gateway).

## Architecture

```
Document → Chunker → Embedding Worker Pool → pgvector Storage → Search API
              ↑              ↑                      ↑
         Fixed-size     Concurrent with         HNSW/IVFFlat
         + Overlap      backpressure            indexed
```

## Current Status: Day 29 (Week 5)

- [x] Project scaffold (`/cmd`, `/internal`, `/pkg`)
- [x] Fixed-size + overlap text chunking
- [x] Domain models (Document, Chunk, Embedding, SearchResult)
- [x] Configuration with env var overrides
- [x] Test suite with benchmarks
- [ ] Embedding API integration (Day 30)
- [ ] Concurrent worker pool (Day 31)
- [ ] pgvector storage (Day 32)
- [ ] Similarity search (Day 33)
- [ ] gRPC search API (Day 34)

## Quick Start

```bash
# Run the pipeline demo
make run

# Run tests
make test

# Run benchmarks
make bench
```

## Project Structure

```
retriever/
├── cmd/
│   └── retriever/
│       └── main.go          # Entry point
├── internal/
│   ├── chunker/
│   │   ├── chunker.go       # Text chunking strategies
│   │   └── chunker_test.go  # Chunker test suite
│   ├── config/
│   │   ├── config.go        # Configuration management
│   │   └── config_test.go   # Config tests
│   └── models/
│       └── models.go        # Domain models
├── docs/
│   └── Day1.md              # Day 29 deep-dive
├── Makefile
├── go.mod
└── README.md
```

## License

MIT
