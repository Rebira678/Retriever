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
11. [The Embedder Interface — The Interchangeable Translators](#scenario-11-the-embedder-interface)
12. [gRPC and Resilient Systems — The Drive-Thru Window](#scenario-12-grpc-and-resilient-systems)
13. [Rate Limiting — The Traffic Light](#scenario-13-rate-limiting-day-38)
14. [Dead Letter Queue — The Lost & Found](#scenario-14-dead-letter-queue-day-39)
15. [Escape Analysis — The Factory Efficiency Audit](#scenario-15-escape-analysis-day-40)
16. [Batch Ingestion — The Bulk Shipment](#scenario-16-batch-ingestion-day-41)
17. [Horizontal Scaling — Adding More Factories](#scenario-17-horizontal-scaling-day-43)
18. [Index Tuning — Reorganizing the Filing Cabinet](#scenario-18-index-tuning-day-44)
19. [Singleflight & Cache Stampedes — The Shared Cab](#scenario-19-singleflight-and-cache-stampedes-day-45)
20. [Circuit Breakers & Load Shedding — The Factory Fuse Box](#scenario-20-circuit-breakers-and-load-shedding-day-46)

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
    emb = embedder.NewGeminiEmbedder(config.GeminiAPIKey, "gemini-embedding-2")
} else {
    emb = embedder.NewOpenAIEmbedder(config.OpenAIAPIKey, ...)
}
```
This is why interfaces are the most powerful feature in Go. They make systems future-proof.

---

## Scenario 12: gRPC and Resilient Systems
### The Drive-Thru Window 🚗

**Imagine this:**

You run a fast-food restaurant with a drive-thru window. This window is how external customers (other microservices) talk to your kitchen (the vector database). 

**The Junior Approach (REST + No Timeouts):**
> A customer pulls up to the window and speaks English (JSON). The cashier has to translate English into kitchen shorthand (serialization). 
> The customer orders a burger, but the grill is broken. The cashier just stands there staring at the kitchen forever. The customer waits forever. The line behind them wraps around the block. Eventually, the entire restaurant shuts down.

**The Expert Approach (gRPC + Errgroup + Interceptors):**
> 1. **gRPC (Binary Protocol):** The customer pulls up and punches their order directly into a digital keypad using kitchen shorthand (Protobuf). No translation needed. It's instantly beamed to the kitchen. Lightning fast.
> 2. **Context Deadlines (Timeouts):** The cashier tells the kitchen, "You have exactly 5 seconds to make this burger." If the grill is broken and 5 seconds pass, the cashier immediately tells the customer, "Sorry, we can't fulfill this right now," and takes the next order. No line backs up.
> 3. **Interceptors (Middleware):** A safety inspector (Recovery Interceptor) stands behind the cashier. If the kitchen catches on fire (a panic), the inspector safely extinguishes it, apologizes to the customer, and keeps the window open for the next car. The restaurant doesn't burn down.
> 4. **Errgroup (Lifecycle):** If the restaurant *must* close for the night, the manager (Errgroup) ensures all current cars finish getting their food before locking the doors (Graceful Shutdown). No one is left stranded.

**In Go code:**
This is exactly why we built the Day 34 Search API using `gRPC` over REST, enforced `context.WithTimeout`, chained `RecoveryInterceptor`, and wrapped the server in an `errgroup` in `main.go`. It turns a fragile endpoint into an industrial-grade drive-thru.

---

## Scenario 13: Rate Limiting (Day 38)
### The Traffic Light 🚦

**Imagine this:**

You live in a city with a bridge that can only handle 100 cars per minute. 

**Without a Traffic Light:**
> 500 cars try to cross the bridge at once. The bridge's structural integrity fails, and the bridge collapses. Nobody gets across for weeks while it's repaired.

**With a Traffic Light (Rate Limiting):**
> A traffic light only lets 100 cars through per minute. If you are car #101, you have to wait. It's slightly annoying for you, but the bridge never collapses, and eventually, everyone gets across safely.

**In Retriever's context:**
OpenAI's embedding API is the bridge. It has strict limits (e.g., 3,000 requests per minute). If our pipeline sends 10,000 chunks simultaneously, OpenAI will return `429 Too Many Requests` errors, and might even temporarily ban our API key.
By using Go's `golang.org/x/time/rate`, we put a traffic light on our embedding workers. They will smoothly pause and wait for the "green light" before sending the next batch, ensuring we never overwhelm the external API.

---

## Scenario 14: Dead Letter Queue (Day 39)
### The Lost & Found 🗃️

**Imagine this:**

You work at a post office sorting facility. Millions of valid letters go through the sorting machine every day. 
Suddenly, a letter arrives wrapped in sticky tape that completely jams the machine. 

**Without a Dead Letter Queue:**
> The machine stops. The worker tries to push the sticky letter through again. It jams again. The worker keeps trying forever (infinite retry loop). Meanwhile, 50,000 normal letters pile up and miss their delivery trucks.

**With a Dead Letter Queue (DLQ):**
> The worker tries twice. When it fails, they toss the sticky letter into a bin labeled "Lost & Found" (The DLQ). The machine immediately restarts and processes the 50,000 normal letters. At the end of the day, an engineer looks at the Lost & Found bin, realizes the tape is the problem, fixes it manually, and updates the machine's rules so it doesn't jam next time.

**In Go code:**
If a specific chunk constantly causes a database serialization error or an API crash, we don't want it to block the entire channel and crash the system. We catch the persistent error, log the chunk to a separate `dead_letters` database table, and let the pipeline continue processing the other 99.9% of healthy chunks.

---

## Scenario 15: Escape Analysis (Day 40)
### The Factory Efficiency Audit 🏭🔍

**Imagine this:**

You run a factory building cars. You give workers tools to do their jobs.
- **The Stack (Local Toolbelt):** The worker pulls a wrench from their own toolbelt. When their shift is over, they just take the toolbelt home. Zero cleanup required. Extremely fast.
- **The Heap (Central Warehouse):** The worker has to walk to a massive central warehouse, check out a giant machine, use it, and then wait for the cleanup crew (Garbage Collector) to come put it back later.

**The Problem:**
Junior developers write code that constantly allocates tiny things (like chunks and arrays) to the Heap (the warehouse). The Garbage Collector (GC) crew gets so overwhelmed cleaning up millions of tiny objects that the actual factory workers have to stop working just so the GC can catch up. This is called "GC thrashing."

**The Solution (Escape Analysis):**
Go has a built-in auditor (`go build -gcflags="-m"`). It looks at your code and says: "Hey, you are sending this `Chunk` to the Heap, but it never actually leaves this function. Keep it on the Stack!" 
By removing pointers (`*Chunk`) in critical loops and using value types (`Chunk`), we keep the data on the local toolbelt. The GC has zero work to do, and our pipeline's CPU usage drops dramatically.

---

## Scenario 16: Batch Ingestion (Day 41)
### The Bulk Shipment 🚚

**Imagine this:**

You are a supplier delivering 500 laptops to an office building.

**The Inefficient Way (Single Inserts):**
> You put 1 laptop in a Prius, drive across town, park, hand it to the receptionist, get a receipt, drive back to the warehouse, and repeat 500 times. It takes 3 days.

**The Efficient Way (Batching):**
> You put all 500 laptops in one delivery truck. You drive across town *once*. You drop them all off *once*. You get one receipt. It takes 1 hour.

**In Postgres/pgvector:**
Opening a database transaction, sending a network packet, and committing takes time (overhead). If we insert 5,000 chunks one-by-one:
```sql
INSERT INTO embeddings (id, vector) VALUES (1, '[...]'); -- 5,000 times
```
It will take minutes.

Instead, we collect chunks in memory until we have 500 of them, and send them in one giant batch:
```sql
INSERT INTO embeddings (id, vector) VALUES (1, '[...]'), (2, '[...]'), ... (500, '[...]');
```
This reduces database network overhead by 99% and makes storage ingestion nearly instant.

---

## Scenario 17: Horizontal Scaling (Day 43)
### Adding More Factories 🏢🏢🏢

**Imagine this:**

Your car factory (Scenario 5) is perfectly optimized. It runs 24/7. But demand triples. You physically cannot fit another worker inside the building.

**The Solution:**
You build two *more* identical factories. But now you have a new problem: If a truck of steel arrives, which factory gets it? If two factories try to process the exact same order, you'll make duplicate cars.

**In Retriever's context:**
We run 3 instances of our Go backend in Kubernetes. If a user uploads a massive document, we want to split the work.
But if all 3 instances try to process a cron job (like "cleanup deleted files"), they will step on each other's toes. 
Horizontal scaling introduces the need for **Distributed Locks** (using Postgres or Redis) so that Instance A can say: "I am handling the cleanup job, Instances B and C, please ignore it."

---

## Scenario 18: Index Tuning (Day 44)
### Reorganizing the Filing Cabinet 🗄️

**Imagine this:**

*(Recalling Scenario 8: Finding Your Neighbor).*
You have 1 million index cards (vectors). 
Brute force means checking every single card. 

**The Junior Solution (IVFFlat - The Folders):**
> You group the 1 million cards into 1,000 folders (clusters). When a user searches, you find the most relevant *folder*, then only check the 1,000 cards inside that folder. 
> *Problem:* What if the answer was actually right on the edge of a different folder? You miss it completely.

**The Expert Solution (HNSW - The Highway System):**
> **H**ierarchical **N**avigable **S**mall **W**orld.
> Imagine a map of the USA. 
> 1. Start zoomed out on the Interstates (Layer 3). Jump quickly from New York to California.
> 2. Zoom in to State Highways (Layer 2). Navigate to Los Angeles.
> 3. Zoom in to City Streets (Layer 1). Navigate to the exact neighborhood.
> 4. Zoom in to Local Roads (Layer 0). Find the exact house.

**In pgvector:**
```sql
CREATE INDEX ON embeddings USING hnsw (embedding vector_cosine_ops) 
WITH (m = 16, ef_construction = 64);
```
HNSW builds this multi-layered graph in the background. It takes more RAM to hold the map, and it takes longer to insert new vectors (because you have to update the map), but search is **blisteringly fast** and highly accurate, even with hundreds of millions of vectors.

---

## Scenario 19: Singleflight and Cache Stampedes (Day 45)
### The Shared Cab 🚕

**Imagine this:**

It is raining heavily in New York City. 50 people exit a train station simultaneously. They all want to go to the exact same hotel. 

**The Junior Approach (No Cache Protection):**
> All 50 people pull out their phones and order 50 separate Uber rides to the same destination. 50 cars arrive, clogging the street (network congestion). They all pay full price (API limits exceeded), and 50 cars drive to the exact same hotel at the exact same time.

**The Expert Approach (Singleflight):**
> One person orders a large bus. The other 49 people see the bus going to their destination and just wait for it. One vehicle arrives, everyone gets on, they share the ride, and they all arrive together. Only one trip was paid for.

**In Retriever's context:**
During a traffic spike, 500 identical search queries might hit our gRPC API at the exact same millisecond. 
If we just use a basic LRU cache, all 500 requests check the cache simultaneously, see it's empty (cache miss), and fire 500 parallel HTTP requests to the OpenAI Embedding API. We instantly get rate-limited. This is a **Cache Stampede**.

By wrapping our cache lookup in Go's `golang.org/x/sync/singleflight`, the first request executes the network call. The other 499 requests automatically block and wait. When the first request returns, all 500 requests instantly share the single resulting embedding vector. The external API only sees 1 request instead of 500.

---

## Scenario 20: Circuit Breakers and Load Shedding (Day 46)
### The Factory Fuse Box 🔌

**Imagine this:**

You run an automated factory where machines are powered by a central electrical grid. 

**The Catastrophe:**
A massive power surge hits the grid. 

**The Junior Approach (No Protection):**
> The factory has no fuse box. The surge hits every single machine. The machines catch fire, melt down, and the entire factory burns to the ground. Even when the power company fixes the surge, you have no factory left. (This is what happens when a database dies, and thousands of goroutines stall waiting for TCP timeouts, exhausting all RAM until the OOM Killer terminates your server).

**The Expert Approach (Circuit Breakers & AIMD):**
> **1. The Circuit Breaker:** You install a master fuse box (`sync/atomic` lock-free state machine). When the database fails 5 times in a row, the fuse *trips* (Circuit OPEN). Any new requests are instantly rejected at the door in 0 milliseconds. The server's memory and CPU stay perfectly flat. When the database comes back online, the fuse carefully tests the connection (HALF-OPEN) and then restores full power (CLOSED).
> 
> **2. Load Shedding:** You have a smart bouncer at the door tracking the speed of the assembly line (AIMD - Additive Increase, Multiplicative Decrease). If the line is running fast, the bouncer lets more people in. If the line slows down (latency spikes), the bouncer instantly drops the entry limit by 10%. The factory dynamically protects itself from being overwhelmed by a traffic tsunami.

**In Go code:**
By combining these patterns in `internal/circuitbreaker/circuitbreaker.go` and as gRPC Interceptors, our Search API actively defends itself. It can absorb a 10,000 RPS attack, shed the excess load, and gracefully degrade to serve cache-hits even while the underlying database is completely offline.

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

### ADR-003: gRPC and Resilient Lifecycle Management for Search API

**Status:** Accepted  
**Date:** 2026-09-14 (Day 34)  
**Context:** We need to expose the vector search capabilities to other internal microservices. The endpoint will transmit large float arrays (embeddings) and requires high resilience against upstream timeouts or panics.

**Decision:** Implement the Search API using gRPC/Protobuf. Manage the server lifecycle using `errgroup` for synchronized graceful shutdowns. Chain Unary Interceptors for logging and panic recovery. Enforce context deadlines defensively.

**Consequences:**
- ✅ Binary serialization (Protobuf) vastly reduces CPU overhead compared to JSON.
- ✅ HTTP/2 multiplexing allows high concurrency.
- ✅ `errgroup` eliminates orphaned goroutines and ensures clean shutdowns.
- ✅ Interceptors protect the main process from crashing due to unexpected panics.
- ⚠️ gRPC requires compiled client stubs, slightly increasing integration complexity over standard REST APIs.

### ADR-004: Lock-Free Resilience Patterns for High-RPS Endpoints

**Status:** Accepted  
**Date:** 2026-09-23 (Day 46)  
**Context:** High-throughput endpoints (Search API) are vulnerable to cascading failures when downstream dependencies (PostgreSQL/pgvector) experience outages. Standard `sync.RWMutex` implementations create CPU cache-line bouncing and hot-path bottlenecks.

**Decision:** Implement a lock-free Circuit Breaker using `sync/atomic` (Compare-And-Swap) and an Adaptive Load Shedding Interceptor using the AIMD algorithm.

**Consequences:**
- ✅ Zero-allocation, lock-free hot path for state evaluation (`atomic.LoadUint32`).
- ✅ Server safely degrades and sheds load in O(1) time (0ms) during outages.
- ✅ Completely eliminates the "Thundering Herd" state transition problem.
- ⚠️ Adds complexity to the gRPC interceptor chain.

---

> **Remember:** Every concept in software engineering is just a real-world process with a fancy name. When you understand the scenario, you understand the system.
