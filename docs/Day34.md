# Day 34 — Retriever RAG Ingestion Pipeline (Sprint Day 6)

**Date:** September 14, 2026
**Concept to Master:** gRPC Service Design for Search API
**Build:** Define `.proto` for the search endpoint, implement the gRPC server

---

## 🎯 What We Built Today

Today we exposed our vector database via a high-performance **gRPC Search API**. Instead of querying the database directly or relying on a REST API, we defined a strongly typed `.proto` contract and implemented a gRPC server that integrates our `embedder.Embedder` and `storage.Storage`.

### What was accomplished:

1. **Protobuf Definition** — Defined `search.proto` with `SearchRequest` and `SearchResponse` messages.
2. **Code Generation** — Used `protoc` and `protoc-gen-go-grpc` to generate Go server/client stubs.
3. **gRPC Server Implementation** — Created `internal/server/grpc.go` to handle requests by embedding the incoming query and querying pgvector via cosine similarity.
4. **App Integration** — Wired the gRPC server into `main.go`, enabling it to run continuously and serve incoming requests.

---

## 🏗️ Expert-Level System Architecture: The Search API

```
┌──────────────┐     gRPC     ┌────────────────────────┐      ┌─────────────┐
│              │ ───────────▶ │ 1. gRPC Server (Port)  │ ───▶ │ 2. Embedder │
│  gRPC Client │              │    Search(req)         │ ◀─── │  (OpenAI/   │
│ (grpcurl/app)│ ◀─────────── │                        │      │   Gemini)   │
└──────────────┘              └────────────────────────┘      └─────────────┘
                                        │ ▲
                            3. Search   │ │ 4. []SearchResult
                               Similar  │ │
                                        ▼ │
                              ┌────────────────────────┐
                              │ storage.PostgresStorage│
                              │ (pgvector cosine dist) │
                              └────────────────────────┘
```

### Production-Grade Reliability Patterns

When moving from a junior implementation (a simple handler in a goroutine) to an **expert-level** system architecture, we introduced several critical reliability patterns today:

1. **Decoupled Lifecycle Management (errgroup):**
   Instead of launching a "fire-and-forget" goroutine for the gRPC server (`go func() { grpcServer.Serve() }`), we use `golang.org/x/sync/errgroup`. This guarantees that if the server crashes, the entire application shuts down predictably. It also ensures that the context cancellation signal triggers a graceful shutdown (`GracefulStop()`), draining active connections before terminating.

2. **Resilient Middleware (Interceptors):**
   We implemented `grpc.ChainUnaryInterceptor` with two custom interceptors:
   - **LoggingInterceptor:** Injects structured logging (`slog`) for every request, tracking method names, durations, and response codes.
   - **RecoveryInterceptor:** Wraps every handler in a `defer recover()` block. If the embedding logic or database driver panics, the interceptor catches it, logs a stack trace, and returns an `Internal` gRPC error to the client, preventing the entire process from crashing.

3. **Bounded Execution (Context Deadlines):**
   A common junior mistake is assuming upstream APIs never hang. Our `Search` handler enforces a strict context deadline. If the client doesn't provide one, we explicitly enforce a 5-second timeout (`context.WithTimeout`). If the OpenAI/Gemini API takes longer than 5 seconds, the context cancels, we halt execution, and instantly return a `DeadlineExceeded` error to the client.

4. **Dependency Injection & Functional Options:**
   Instead of hardcoding global loggers or database connections, the `SearchServer` is constructed using the Functional Options pattern (`SearchServerOption`). This makes the server highly modular and easily testable (e.g., injecting mock loggers during unit testing).

### Execution Flow

When a user submits a search query:
1. The gRPC server receives `SearchRequest{ Query: "...", TopK: 2 }`.
2. It calls the **Embedder** to convert the query string into a 1536-dimensional float32 vector.
3. It passes the query vector to **pgvector** using the `<=>` operator (cosine similarity) to find the nearest `TopK` chunks.
4. It maps the results back to the protobuf `SearchResult` format and responds.

---

## 📚 Concept Deep-Dive: gRPC vs REST for Internal APIs

### Why gRPC for the Retriever Search API?

For an internal component in a larger AI platform (e.g., accessed by the Router or a frontend backend-for-frontend), gRPC offers distinct advantages over REST:

1. **Strong Typing:** The `.proto` file serves as the single source of truth. Both client and server code are auto-generated, eliminating mismatch bugs.
2. **Performance:** Protocol Buffers are a binary format, which is smaller and faster to serialize/deserialize than JSON. This matters when returning thousands of floats (embeddings) or large blocks of chunk text.
3. **HTTP/2 Native:** gRPC uses HTTP/2 multiplexing, allowing multiple concurrent search queries over a single persistent TCP connection.

### The Search Protobuf Definition

```protobuf
syntax = "proto3";
package search.v1;
option go_package = "github.com/Rebira678/Retriever/pkg/api/search/v1;searchv1";

service SearchService {
    rpc Search(SearchRequest) returns (SearchResponse);
}

message SearchRequest {
    string query = 1;
    int32 top_k = 2;
}

message SearchResponse {
    repeated SearchResult results = 1;
}

message SearchResult {
    string document_id = 1;
    int32 chunk_index = 2;
    string chunk_text = 3;
    float score = 4;
}
```

---

## ✅ Test Results (via grpcurl)

We queried the live gRPC server directly from the terminal using `grpcurl`:

**Command:**
```bash
grpcurl -plaintext -import-path ./proto -proto search/v1/search.proto \
  -d '{"query":"How does RAG solve hallucination?", "top_k": 2}' \
  localhost:50051 search.v1.SearchService/Search
```

**Output:**
```json
{
  "results": [
    {
      "chunkIndex": 1,
      "chunkText": " — a user query is embedded and compared against stored vectors using similarity search (cosine or L2 distance). 3. Generation — the retrieved chunks are appended to the LLM prompt, grounding the model's response in factual, up-to-date information. This architecture solves the \"knowledge cutoff\" problem inherent in static LLMs and dramatically reduces hallucination by providing verifiable source material. RAG is now the dominant pattern for production AI applications that need accurate, domain-specific ",
      "score": 0.80064815
    },
    {
      "chunkIndex": 1,
      "chunkText": " — a user query is embedded and compared against stored vectors using similarity search (cosine or L2 distance). 3. Generation — the retrieved chunks are appended to the LLM prompt, grounding the model's response in factual, up-to-date information. This architecture solves the \"knowledge cutoff\" problem inherent in static LLMs and dramatically reduces hallucination by providing verifiable source material. RAG is now the dominant pattern for production AI applications that need accurate, domain-specific ",
      "score": 0.80064815
    }
  ]
}
```

*Note: The duplicate results are expected in this test because the demo ingestion chunker runs and inserts the same text chunks every time the server starts. We'll solve this with idempotency in Day 36.*

---

## 📱 Social Media Angles

### LinkedIn Version

**Hook:** Today I wired up a production-grade gRPC Search API for my RAG ingestion pipeline. 🚀

**Body:** 
Instead of a simple REST endpoint, I defined a strictly-typed Protobuf contract to expose the vector database to other microservices. When a request hits the gRPC server, it embeds the query in real-time and uses pgvector’s `<=>` cosine distance operator to return the top semantically matching chunks. 

Using HTTP/2 multiplexing and binary serialization means the backend can serve search results incredibly fast—crucial when the next step is feeding these chunks into an LLM context window.

**Result:** Check out the screenshot below of `grpcurl` hitting the live server and pulling back ranked, vectorized text chunks perfectly mapped from the DB. 

**Close:** Tomorrow, I’m wrapping up the week with a deep dive into the entire RAG ingestion pipeline build so far. Are you team gRPC or REST for internal AI tooling?

*(Attach Screenshot of the `grpcurl` terminal output)*

---

### X (Twitter) Version

Exposed my pgvector database via a gRPC Search API today in Go. 🧵👇

Request comes in → query gets embedded → cosine similarity search against Postgres → ranked results returned via Protocol Buffers. 

Binary serialization + HTTP/2 makes this lightning fast for internal microservices. Here is the `grpcurl` test returning semantically similar chunks:

*(Attach Screenshot of the `grpcurl` terminal output)*
