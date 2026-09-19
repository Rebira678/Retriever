# Day 40: Escape Analysis (The Factory Efficiency Audit)

Today's focus was on **Escape Analysis** and reducing memory allocations across the RAG pipeline.

## The Problem: Memory Thrashing
In a high-throughput pipeline, every memory allocation adds up. When the Go compiler cannot guarantee that a variable's lifetime is confined to a single function stack frame, it "escapes" that variable to the heap. Frequent heap allocations force the Garbage Collector (GC) to run constantly, causing latency spikes and high CPU utilization.
This is similar to a factory wasting time constantly ordering and throwing away new boxes instead of reusing them or properly estimating box sizes upfront.

## What We Optimized
Using the `go build -gcflags="-m"` tool, we audited the pipeline and discovered multiple inefficient heap allocations caused by dynamically growing slices.

We implemented the following senior-level memory optimizations:
1. **Pre-allocated Chunks (`internal/chunker/chunker.go`)**: We now pre-calculate the expected number of chunks based on document length and overlap size (`estimatedChunks = (len(text) / step) + 1`), explicitly initializing the slice capacity `make([]models.Chunk, 0, estimatedChunks)`. This prevents the slice from repeatedly escaping to the heap during `append()` reallocations.
2. **Pre-allocated Results (`internal/storage/postgres.go`)**: When scanning vector similarity search results, we now pre-allocate the slice using the `topK` parameter `make([]models.SearchResult, 0, topK)`.
3. **Pre-allocated Embeddings & Dead Letters (`internal/pipeline/ingest.go`)**: We pre-allocate both arrays during aggregation using the known `len(chunks)`.
4. **Fixed-Size Array over Slices (`internal/embedder/gemini.go`)**: In our Gemini API request payload, we replaced the dynamic `Parts []struct` slice with a fixed-size `Parts [1]struct` array. This completely eliminated a heap allocation on every single API request while still marshaling perfectly to the expected JSON array structure.

## Why This Matters
Performance isn't just about concurrency; it's about mechanical sympathy with the runtime. By eliminating hundreds of thousands of heap allocations during batch ingestion runs, we dramatically reduced the GC pressure. The pipeline now runs cooler, faster, and consumes less memory at peak concurrency.
