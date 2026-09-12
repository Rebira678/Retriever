# Retriever — Understanding Through Real-World Scenarios

This document explains every concept in the Retriever RAG pipeline using real-world scenarios. Each scenario tells a story that makes the technical concept click.

---

## Table of Contents

1. [What is RAG? — The Library Analogy](#scenario-1-what-is-rag)
2. [Why Chunking? — The Index Card System](#scenario-2-why-chunking)
3. [Fixed-Size vs Overlap — The Photocopier Problem](#scenario-3-fixed-size-vs-overlap)
4. [Why 512 Characters? — The Goldilocks Zone](#scenario-4-why-512-characters)
5. [Pipeline Architecture — The Factory Floor](#scenario-5-pipeline-architecture)
6. [Backpressure — The Restaurant Kitchen](#scenario-6-backpressure)
7. [Embeddings — The Map of Meaning](#scenario-7-embeddings)
8. [Vector Search — Finding Your Neighbor](#scenario-8-vector-search)
9. [Idempotency — The Package Delivery](#scenario-9-idempotency)
10. [The Full Flow — A Customer Support Bot](#scenario-10-the-full-flow)

---

## Scenario 1: What is RAG?
### The Library Analogy 📚

**Imagine this:**

You're a student preparing for an exam. You have two options:

**Option A: Pure Memory (Standard LLM)**
> You memorized the entire textbook last year. When asked "What causes inflation?", you answer from memory. But the textbook was published in 2022 — you don't know about the 2024 economic events. You might even "remember" things incorrectly because your memory isn't perfect.

**Option B: Open-Book Exam (RAG)**
> You have the textbook on your desk. When asked "What causes inflation?", you:
> 1. Look at the **index** (vector search) to find relevant pages
> 2. Flip to those pages and **read the relevant paragraphs** (retrieval)
> 3. **Answer in your own words**, citing the specific pages (generation)

**RAG is Option B.** Instead of relying solely on what the LLM "memorized" during training, we fetch relevant documents and include them in the prompt. This:
- ✅ Eliminates the "knowledge cutoff" — your data is always current
- ✅ Reduces hallucination — the model can cite sources
- ✅ Works with private data — your company's internal docs, not just public internet

**What Retriever does:** It's the system that creates and maintains the "index" — ingesting documents, breaking them into searchable pieces, and making them findable via similarity search.

---

## Scenario 2: Why Chunking?
### The Index Card System 📇

**Imagine this:**

You're organizing a 300-page legal contract for quick reference. You have two approaches:

**Approach A: No Chunking**
> You photocopy the entire 300-page contract and file it as one item. When a lawyer asks "What does section 14.3 say about termination?", you hand them all 300 pages. They have to read the whole thing to find the answer.

**Approach B: Chunking**
> You summarize each section onto an index card. Each card has:
> - A brief excerpt (the chunk text)
> - A reference back to the original page (the offset)
> - A number so you can put them back in order (the index)
> 
> When the lawyer asks about termination, you quickly flip through the cards, find the relevant ones, and hand them just those 3-4 cards.

**In Retriever terms:**
```go
// The "300-page contract" is a Document
doc := models.Document{
    Content: entireContractText,  // 300 pages of text
}

// The "index cards" are Chunks
chunks := chunker.Chunk(doc.Content)
// Result: ~600 chunks, each ~512 characters
// Each chunk has: Index, Text, StartOffset, EndOffset
```

**Why not just send the whole document to the LLM?**
- LLMs have a context window limit (e.g., 128K tokens for GPT-4)
- Embedding models have even smaller limits (e.g., 8192 tokens)
- Even if it fits, the LLM performs worse with irrelevant context (the "needle in a haystack" problem)
- Chunking lets us send only the relevant pieces

---

## Scenario 3: Fixed-Size vs Overlap
### The Photocopier Problem 📄

**Imagine this:**

You're photocopying a book, but your copier can only handle one page at a time. The problem: some sentences span two pages.

**Without Overlap:**
```
Page 1: "...the rate of inflation was increasing rapidly during"
Page 2: "the second quarter of 2024, reaching 4.7% by June."
```
Neither page contains the complete sentence. If someone searches for "inflation rate 2024", neither page is a great match.

**With Overlap (our approach):**
```
Page 1: "...the rate of inflation was increasing rapidly during"
Page 2: "increasing rapidly during the second quarter of 2024, reaching 4.7% by June."
         ^^^^^^^^^^^^^^^^^^^^^ ← this part appears on BOTH pages
```
Now Page 2 contains the complete sentence. A search for "inflation rate 2024" will find it.

**In code:**
```go
// Without overlap — information at boundaries is lost
chunker := chunker.New(512, 0)

// With overlap — boundaries are preserved
chunker := chunker.New(512, 64)
// The last 64 chars of chunk N appear as the first 64 chars of chunk N+1
```

**The trade-off:** Overlap means ~12.5% more chunks (and thus ~12.5% more embedding API calls and storage). But the search quality improvement is worth it — without overlap, you'll get mysterious "near-miss" search results where the relevant sentence was split across two chunks.

---

## Scenario 4: Why 512 Characters?
### The Goldilocks Zone 🧸

**Imagine three different index card sizes:**

**Too Small (256 chars) — The Post-It Note:**
> "The rate of inflation was"

This is too short to be meaningful on its own. The embedding will capture some meaning, but it lacks context. You'll get very precise matches but miss the big picture. And you'll need 2x more cards (= 2x embedding cost).

**Too Large (1024 chars) — The Full Page:**
> "The rate of inflation was increasing rapidly during the second quarter of 2024, reaching 4.7% by June. This was primarily driven by supply chain disruptions, wage growth in the service sector, and..."

This contains multiple topics (rate, causes, timeline). When embedded, the vector represents an average of all these topics. If someone searches for "inflation causes", this chunk will be a mediocre match because the embedding is diluted by the rate and timeline information.

**Just Right (512 chars) — The Index Card:** ✅
> "The rate of inflation was increasing rapidly during the second quarter of 2024, reaching 4.7% by June. This was primarily driven by supply chain disruptions and..."

Enough context to be meaningful. Focused enough for a good embedding. This is why 512 is the industry default.

---

## Scenario 5: Pipeline Architecture
### The Factory Floor 🏭

**Imagine this:**

You're building a car factory. Cars go through stations:

```
Steel → Stamping → Welding → Painting → Assembly → Quality Check → Shipping
```

Each station:
- Takes input from the previous station
- Does one specific job
- Passes output to the next station
- Can be scaled independently (add more welding robots if welding is the bottleneck)

**Retriever works the same way:**

```
Document → Chunking → Embedding → Storage → Search
```

**Why not do everything in one big function?**

```go
// ❌ Monolithic approach — don't do this
func processDocument(doc string) {
    chunks := splitIntoChunks(doc)
    for _, chunk := range chunks {
        embedding := callOpenAIAPI(chunk)      // This is SLOW (network call)
        saveToPostgres(embedding)
    }
}
// Problem: If the OpenAI API is slow, you can't chunk other documents
// Problem: If Postgres is down, you lose the embedding you already computed
// Problem: You can't scale embedding independently from chunking
```

```go
// ✅ Pipeline approach — what we're building
// Each stage communicates via Go channels

chunkChan := make(chan Chunk, 100)        // Bounded buffer = backpressure
embeddingChan := make(chan Embedding, 50)

// Stage 1: Chunker (fast)
go func() { chunker.StreamChunks(doc, chunkChan) }()

// Stage 2: Embedding workers (slow — multiple workers in parallel)
for i := 0; i < 4; i++ {
    go func() { embedWorker(chunkChan, embeddingChan) }()
}

// Stage 3: Storage writer (medium)
go func() { storageWriter(embeddingChan) }()
```

**Why this matters:** The embedding API is 1000x slower than chunking. With the pipeline, we can have 4 embedding workers processing chunks concurrently while the chunker feeds them. Without the pipeline, we'd process one chunk at a time.

---

## Scenario 6: Backpressure
### The Restaurant Kitchen 🍳

**Imagine this:**

You're running a restaurant. Orders come in from the dining room (ingestion), the kitchen cooks them (embedding), and servers deliver them (storage).

**Without Backpressure:**
> It's Friday night. 200 orders flood in. The kitchen can only handle 20 orders at a time. The remaining 180 orders pile up on the counter. The kitchen runs out of space. Orders fall on the floor (out of memory). Chaos.

**With Backpressure:**
> The kitchen has a queue rack that holds exactly 50 orders. When the rack is full, the waiter can't place more orders — they tell the customer "we're at capacity, please wait 5 minutes." The kitchen processes at its own pace. No orders fall on the floor.

**In Go code:**
```go
// The "50-slot order rack" is a bounded channel
chunkQueue := make(chan Chunk, 50)  // At most 50 chunks waiting

// When the channel is full, this BLOCKS automatically
// The chunker waits until a worker picks up a chunk
chunkQueue <- newChunk  // Blocks if channel has 50 items

// Workers pull at their own pace
chunk := <-chunkQueue   // Blocks if channel is empty
```

This is why Go channels are perfect for pipeline architectures. Backpressure is built into the language — you don't need a separate library or framework.

---

## Scenario 7: Embeddings
### The Map of Meaning 🗺️

**Imagine this:**

You have a map where every concept in the world has a coordinate:

```
                    Animals
                      │
            ┌─────────┼─────────┐
           Cats      Dogs     Birds
          (2,3)     (2.5,3)   (5,7)

    Vehicles                    Food
      │                          │
  ┌───┼───┐               ┌─────┼─────┐
 Car  Bus Bike           Pizza Sushi Pasta
(8,1)(8.2,1)(7,2)       (1,8) (3,9) (1.5,8.5)
```

Notice:
- "Cat" and "Dog" are close together (they're both pets)
- "Pizza" and "Pasta" are close (both Italian food)
- "Cat" and "Car" are far apart (unrelated concepts)

**Embeddings do this, but in 1536 dimensions instead of 2.** Each chunk of text gets converted to a point in 1536-dimensional space. Similar chunks end up near each other.

**How search works:**
1. User asks: "What are common pets?"
2. The query gets embedded to a point: e.g., (2.2, 3.1)
3. We find the nearest points: Cat (2,3) and Dog (2.5,3)
4. We return those chunks as search results

**This is why chunking quality matters:** If a chunk contains "cats are great pets and also the weather was nice today", its embedding will be a point halfway between "pets" and "weather" — not close to either. Bad chunk = bad embedding = bad search.

---

## Scenario 8: Vector Search
### Finding Your Neighbor 🏘️

**Imagine this:**

You live in a city. You want to find the 3 closest coffee shops to your house.

**Brute-Force Search (what we build first):**
> Walk to every single coffee shop in the city, measure the distance, then pick the 3 closest. Works perfectly, but slow for a city with 10,000 coffee shops.

**Indexed Search (what we build later with HNSW/IVFFlat):**
> The city has a map divided into neighborhoods. You look up your neighborhood, check the coffee shops in your neighborhood and adjacent ones. Much faster — you only check ~50 shops instead of 10,000.

**In Postgres/pgvector:**
```sql
-- Brute-force (Day 33): scan all vectors
SELECT chunk_text, 1 - (embedding <=> query_embedding) AS similarity
FROM embeddings
ORDER BY embedding <=> query_embedding
LIMIT 5;

-- Indexed (Day 44): use HNSW index for approximate nearest neighbors
CREATE INDEX ON embeddings USING hnsw (embedding vector_cosine_ops);
-- Same query, but now 100x faster because it uses the index
```

**The trade-off:** Indexed search is approximate — it might miss the #4 closest result. But it's 100x faster. For RAG, approximate is fine because we're retrieving context, not doing exact matching.

---

## Scenario 9: Idempotency
### The Package Delivery 📦

**Imagine this:**

Amazon delivers a package to your door. The delivery driver's phone glitches and sends the "delivered" confirmation twice. Without idempotency, Amazon charges you twice for the same order.

**In Retriever's context:**

You upload the same document twice (maybe a retry after a timeout, maybe a cron job re-runs). Without idempotency:
```
Upload 1: "What is RAG?" → Chunk → Embed → Store (vector_id: 001)
Upload 2: "What is RAG?" → Chunk → Embed → Store (vector_id: 002)  ← DUPLICATE!
```

Now when someone searches, they get the same chunk twice in results. The search is polluted.

**With idempotency (content hashing):**
```go
hash := sha256.Sum256([]byte(document.Content))
// hash = "a3f2b1c4..."

// Before processing, check: does this hash already exist?
exists := db.Query("SELECT 1 FROM documents WHERE content_hash = ?", hash)
if exists {
    log.Info("Document already processed, skipping")
    return  // No duplicate work
}
```

This is why our `Document` model has a `ContentHash` field — it's the foundation for idempotent re-ingestion, which we'll implement on Day 36.

---

## Scenario 10: The Full Flow
### A Customer Support Bot 🤖

Let's trace a real-world scenario end-to-end.

### Setup: A Company Ingests Its Knowledge Base

**Step 1: Ingestion**
The company uploads 500 support articles to Retriever.

**Step 2: Chunking (Today's work!)**
Each article is split into ~10 chunks of 512 characters with 64-char overlap.
→ 500 articles × 10 chunks = 5,000 chunks

**Step 3: Embedding (Day 30-31)**
Each chunk is sent to OpenAI's embedding API, returning a 1536-dimensional vector.
→ 5,000 API calls (parallelized via worker pool: ~20 seconds with 4 workers)

**Step 4: Storage (Day 32)**
5,000 vectors stored in Postgres with pgvector extension.
→ Each vector: 1536 × 4 bytes = ~6KB. Total: ~30MB.

### Runtime: A Customer Asks a Question

```
Customer: "How do I reset my password?"
    │
    ▼
┌──────────────────────────────────────────┐
│ 1. Embed the query                       │
│    "How do I reset my password?"         │
│    → [0.023, -0.156, 0.891, ...]         │
│      (1536-dimensional vector)           │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│ 2. Vector Search (pgvector)              │
│    Find 5 nearest chunks by cosine       │
│    similarity                            │
│                                          │
│    Result:                               │
│    ┌─ Chunk 47: "To reset your password, │
│    │  go to Settings > Security > ..."   │
│    │  Score: 0.94                         │
│    ├─ Chunk 48: "If you've forgotten     │
│    │  your password, click 'Forgot...'   │
│    │  Score: 0.91                         │
│    └─ Chunk 203: "For two-factor auth    │
│       password resets, contact..."        │
│       Score: 0.87                         │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│ 3. Generation (LLM via Router)           │
│                                          │
│    Prompt to LLM:                        │
│    "Using these support articles:        │
│     [Chunk 47] [Chunk 48] [Chunk 203]    │
│     Answer: How do I reset my password?" │
│                                          │
│    LLM Response:                         │
│    "To reset your password:              │
│     1. Go to Settings > Security         │
│     2. Click 'Reset Password'            │
│     3. Check your email for a link       │
│     Source: Article #12"                  │
└──────────────────────────────────────────┘
```

**Notice:** Retriever handled steps 1-2 (embedding + search). Router (Project 1) handled step 3 (forwarding the prompt to the LLM with rate limiting, failover, etc.). Together, they form a complete production RAG system.

---

## Scenario 11: The Embedder Interface
### The Interchangeable Translators 🗣️

**Imagine this:**

You run a global business. You need all your English documents translated to French. You hire a translator named OpenAI. 

**Without an Interface (Tight Coupling):**
> You build your entire office around OpenAI. Your forms only accept OpenAI's specific handwriting. Your filing cabinets are sized exactly for OpenAI's paper. 
> One day, OpenAI becomes too expensive, and you want to hire a free translator named Gemini. You have to tear down your entire office and rebuild it because it only works with OpenAI.

**With an Interface (Decoupling):**
> You define a standard `Translator` job description: "Takes English text, returns French text." You don't care *how* they do it.
> You give OpenAI a standard desk. When you want to switch to Gemini, you just tell Gemini to sit at that desk and follow the same job description. Your office doesn't change at all.

**In Go code:**
```go
// The standard job description (Interface)
type Embedder interface {
    EmbedChunk(ctx context.Context, chunk Chunk) (Embedding, error)
}

// In our pipeline, we just use the interface. We don't care who the translator is!
// When the user provides a Gemini key, we swap out OpenAI for Gemini seamlessly:
var emb embedder.Embedder
if config.GeminiAPIKey != "" {
    emb = embedder.NewGeminiEmbedder(config.GeminiAPIKey, "text-embedding-004")
} else {
    emb = embedder.NewOpenAIEmbedder(config.OpenAIAPIKey, ...)
}
```
This is why interfaces are the most powerful feature in Go. They make systems future-proof.

---

## Day-by-Day Scenario Map

Here's how each remaining day connects to a real scenario:

| Day | Concept | Scenario |
|-----|---------|----------|
| **29 (Today)** | Chunking | The index card system — breaking documents into searchable pieces |
| **30** | Embeddings | The map of meaning — turning text into coordinates |
| **31** | Worker Pool | The factory floor — parallel processing for speed |
| **32** | pgvector | The filing cabinet — storing vectors efficiently |
| **33** | Similarity Search | Finding your neighbor — nearest-vector queries |
| **34** | gRPC API | The service window — exposing search to external clients |
| **35** | Review | Quality inspection — testing the full pipeline |
| **36** | Idempotency | The package delivery — preventing duplicate processing |
| **37** | Backpressure | The restaurant kitchen — handling capacity limits |
| **38** | Rate Limiting | The traffic light — protecting external APIs |
| **39** | Dead Letter Queue | The lost & found — handling persistent failures |
| **40** | Escape Analysis | The factory efficiency audit — reducing waste |
| **41** | Batch Ingestion | The bulk shipment — processing many documents at once |
| **42** | Review | Full factory inspection |
| **43** | Horizontal Scaling | Adding more factories — running multiple instances |
| **44** | Index Tuning | Reorganizing the filing cabinet — HNSW/IVFFlat |
| **45-46** | Load Testing | Stress testing the factory — finding the breaking point |
| **47** | Hybrid Search | The dictionary + thesaurus — keyword + vector |
| **48** | Kubernetes | The warehouse network — deploying across servers |
| **49** | Review | Final inspection |
| **50-51** | Tracing | The security camera — seeing what happens inside |
| **52** | Launch | Grand opening 🚀 |

---

## Architecture Decision Record (ADR)

### ADR-001: Pipeline Architecture for RAG Ingestion

**Status:** Accepted  
**Date:** 2026-09-10  
**Context:** We need to process documents through multiple stages (chunk → embed → store) with different performance characteristics at each stage.

**Decision:** Use a pipeline architecture with Go channels connecting stages, bounded channels for backpressure, and a worker pool pattern for the embedding bottleneck.

**Consequences:**
- ✅ Each stage is independently testable and benchmarkable
- ✅ Backpressure is automatic via Go's channel mechanics
- ✅ The bottleneck (embedding API) can be scaled independently
- ✅ Easy to add new stages (e.g., content deduplication before chunking)
- ⚠️ Stages are coupled in-process (acceptable for a single-team project)
- ⚠️ No replay capability (would need Kafka/NATS for that — currently unnecessary)

### ADR-002: Fixed-Size + Overlap as Default Chunking Strategy

**Status:** Accepted  
**Date:** 2026-09-10  
**Context:** Multiple chunking strategies exist with different quality/complexity trade-offs.

**Decision:** Start with fixed-size (512 chars) + overlap (64 chars) as the default. Design the `Chunker` interface to support future strategies (sentence-based, recursive) without changing downstream stages.

**Consequences:**
- ✅ Predictable performance characteristics
- ✅ Simple to test and reason about
- ✅ Sufficient for most document types
- ⚠️ May split mid-sentence (acceptable trade-off for Day 1)
- 📋 TODO: Add sentence-aware chunking in Week 6+

---

> **Remember:** Every concept in software engineering is just a real-world process with a fancy name. When you understand the scenario, you understand the system.
