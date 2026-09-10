# Day 1 — Retriever RAG Ingestion Pipeline (Sprint Day 29)

**Date:** September 10, 2026  
**Concept to Master:** Document Chunking Strategies  
**Build:** Scaffold Retriever repo, implement basic text chunking (fixed-size + overlap)

---

## 🎯 What We Built Today

Today marks the beginning of **Project 2: Retriever** — a RAG (Retrieval-Augmented Generation) ingestion pipeline. While Router (Project 1) handles *how AI products serve requests reliably*, Retriever handles *how they ingest and retrieve data reliably*.

### What was accomplished:

1. **Project scaffold** — Clean Go project structure following standard layout
2. **Domain models** — Core types for the entire pipeline (Document, Chunk, Embedding, SearchResult)
3. **Fixed-size + overlap chunker** — The foundation of the ingestion pipeline
4. **12-Factor configuration** — Environment-variable driven settings
5. **Comprehensive test suite** — 16 tests + 2 benchmarks, all passing
6. **Architecture decision** — Pipeline-based architecture (documented below)

---

## 🏗️ System Architecture: Why Pipeline-Based?

### The Decision

We chose a **pipeline-based architecture** for Retriever. Here's what that means and why:

```
┌──────────┐    ┌──────────┐    ┌───────────────┐    ┌──────────────┐    ┌────────────┐
│ Document │───▶│ Chunker  │───▶│ Embedding     │───▶│ pgvector     │───▶│ Search API │
│ Ingestion│    │          │    │ Worker Pool   │    │ Storage      │    │ (gRPC)     │
└──────────┘    └──────────┘    └───────────────┘    └──────────────┘    └────────────┘
     ▲                               ▲                     ▲
     │                               │                     │
  HTTP/gRPC                    Concurrent +            HNSW/IVFFlat
  endpoint                     backpressure            indexed
```

### Alternatives Considered

| Architecture | Pros | Cons | Why Not? |
|---|---|---|---|
| **Monolithic handler** | Simple, one file | No separation of concerns, impossible to scale embedding independently | Doesn't scale when embedding API is the bottleneck |
| **Event-driven (Kafka/NATS)** | Fully decoupled, replay-able | Massive operational overhead for a single-team project, eventual consistency complicates search freshness | Over-engineering — we don't need multi-service decoupling yet |
| **Pipeline-based** ✅ | Clear data flow, each stage testable independently, backpressure natural via channels | Stages are coupled in-process | Perfect fit for Go channels, easy to extract to microservices later |
| **Actor model (Erlang-style)** | Fault-isolated stages | Unidiomatic in Go, complex supervision trees | Go's CSP model (goroutines + channels) gives us 90% of the benefit without the complexity |

### Why Pipeline Wins for RAG

1. **Data flows in one direction** — Document → Chunks → Embeddings → Storage. This is inherently a pipeline.
2. **Backpressure is natural** — Go's bounded channels are literally built for pipeline backpressure. When the embedding API is slow, the chunker automatically blocks.
3. **Each stage is independently testable** — Today we tested chunking in isolation. Tomorrow we test embedding in isolation. They compose cleanly.
4. **Horizontal scaling at the bottleneck** — The embedding stage (Day 31) will use a worker pool. We can scale workers independently without touching chunking or storage.

---

## 📚 Concept Deep-Dive: Document Chunking

### Why Chunking Exists

LLMs have finite context windows. You can't feed a 100-page PDF into an embedding model — you need to break it into smaller pieces. But *how* you break it matters enormously for search quality.

### The Three Main Strategies

#### 1. Fixed-Size + Overlap (What we built today)

```
Document:  "ABCDEFGHIJKLMNOPQRST"  (20 chars)

chunkSize=10, overlap=3:
  Chunk 1:  "ABCDEFGHIJ"   [0:10]
  Chunk 2:  "HIJKLMNOPQ"   [7:17]   ← "HIJ" overlaps
  Chunk 3:  "OPQRST"       [14:20]  ← "OPQ" overlaps
```

**How it works:**
- Start at position 0
- Take `chunkSize` characters
- Advance by `chunkSize - overlap` characters
- Repeat until end

**Why overlap?** Without overlap, a sentence that spans a chunk boundary gets split across two chunks. Neither chunk contains the full sentence, so neither chunk gets a good embedding for that concept. Overlap ensures every sentence appears in full in at least one chunk.

**Trade-offs:**
- ✅ Predictable chunk sizes → consistent embedding costs
- ✅ Simple to implement, easy to reason about
- ✅ Works well for uniform text (docs, articles)
- ❌ May split mid-sentence (a semantic gap)
- ❌ Doesn't respect document structure (headers, lists)

#### 2. Sentence-Based Chunking (Future: Day 33+)

Split at sentence boundaries. Each chunk contains N complete sentences.

- ✅ Never splits mid-sentence
- ❌ Variable chunk sizes → unpredictable embedding costs
- ❌ Requires a sentence tokenizer (not trivial for all languages)

#### 3. Recursive/Semantic Chunking (Future: Advanced)

Split hierarchically: first by paragraphs, then by sentences, then by fixed-size. Respects document structure.

- ✅ Best search quality
- ❌ Most complex to implement
- ❌ Requires understanding document format (Markdown, HTML, PDF)

### Our Benchmark Results

```
BenchmarkChunk_SmallDoc    1,588,118 ops    772 ns/op     752 B/op    3 allocs
BenchmarkChunk_LargeDoc        7,639 ops    133 µs/op    179 KB/op   10 allocs
```

**Key insight:** The chunker is not the bottleneck. At 133µs for a 10KB document, we can chunk ~7,500 documents/second on a single core. The bottleneck will be the embedding API (Day 30-31), which is why we need a concurrent worker pool with backpressure.

---

## 🔧 Trade-offs Faced Today

### 1. Chunk Size: 512 vs 256 vs 1024

| Size | Embedding Quality | Cost | Search Precision |
|------|------------------|------|-----------------|
| 256 | Each chunk is very focused | 2x more API calls | High precision, low recall |
| **512** ✅ | Good balance | Moderate | Good balance |
| 1024 | More context per chunk | Fewer API calls | High recall, lower precision |

**Decision:** 512 characters as default. This aligns with OpenAI's `text-embedding-3-small` sweet spot and is the most common setting in production RAG systems. Configurable via `RETRIEVER_CHUNK_SIZE` env var.

### 2. Overlap Size: 64 characters (12.5% of chunk size)

Research suggests 10-20% overlap is optimal. Below 10%, you lose context at boundaries. Above 20%, you waste embedding compute on redundant text.

### 3. Character-Based vs Token-Based Chunking

We chunk by **characters**, not tokens. Why?

- Token boundaries differ between models (GPT vs Claude vs local)
- Character-based is model-agnostic
- We normalize to the embedding model's token limit at the embedding stage (Day 30)
- Simpler implementation, fewer dependencies

### 4. Whitespace Normalization

We normalize whitespace (`\n\n\t  → single space`) before chunking. This ensures consistent chunk sizes regardless of source formatting. The trade-off: we lose paragraph structure, which we'll need sentence/paragraph-aware chunking to preserve.

---

## 📁 Project Structure Created

```
retriever/
├── cmd/
│   └── retriever/
│       └── main.go              # Entry point with demo
├── internal/
│   ├── chunker/
│   │   ├── chunker.go           # Fixed-size + overlap chunker
│   │   └── chunker_test.go      # 14 tests + 2 benchmarks
│   ├── config/
│   │   ├── config.go            # 12-Factor configuration
│   │   └── config_test.go       # 3 tests
│   └── models/
│       └── models.go            # Domain models
├── docs/
│   └── Day1.md                  # This file
├── Makefile                     # Dev workflow targets
├── README.md                    # Project overview
├── go.mod
└── roadmap_text.txt
```

**Why `/internal`?** Go's `internal` directory is access-restricted by the compiler. Packages under `internal/` can only be imported by code in the parent tree. This prevents external packages from depending on our implementation details — only `cmd/retriever` and future `pkg/` exports can use them.

**Why `/cmd/retriever`?** Standard Go convention for multi-binary projects. Even though we only have one binary now, this structure scales when we add CLI tools later.

---

## ✅ Test Results

```
16 tests passed | 0 failed | 2 benchmarks

Chunker Tests:
  ✓ Default values on invalid input (5 sub-tests)
  ✓ Empty input handling (2 sub-tests)
  ✓ Single chunk fits exactly
  ✓ Fixed-size without overlap — contiguous chunks
  ✓ Fixed-size with overlap — overlap correctness verified
  ✓ Overlap preserves context at boundaries
  ✓ Sequential chunk indexes
  ✓ Valid offsets (no out-of-bounds)
  ✓ Last chunk can be shorter
  ✓ Full document coverage — every character in at least one chunk
  ✓ Whitespace normalization
  ✓ Large document handling (~10KB → 22 chunks)
  ✓ CreatedAt timestamps set

Config Tests:
  ✓ Default config is valid
  ✓ Environment variable overrides work
  ✓ Invalid env values fall back to defaults
```

---

## 🔜 What's Next: Day 2 (Sprint Day 30)

**Concept:** Embedding APIs — how vector embeddings work  
**Build:** Wire a call to an embedding API (OpenAI/local model), embed a single chunk  
**Key question:** How do you turn text into a 1536-dimensional vector, and why does cosine similarity on those vectors find semantically similar documents?

---

## 🧠 Key Takeaway

> The chunker is the first stage in a RAG pipeline, and it's deceptively important. Bad chunking = bad embeddings = bad search results. We start with the simplest strategy (fixed-size + overlap) because it's predictable and sufficient for most use cases. The real complexity comes in the next stages: concurrent embedding with backpressure, vector storage with proper indexing, and semantic search with ranking.
