# Day 40: Escape Analysis (The Factory Efficiency Audit)

Today's focus was on **Escape Analysis** and reducing memory allocations across the RAG pipeline.

## The Problem: Memory Thrashing
In a high-throughput pipeline, every memory allocation adds up. When the Go compiler cannot guarantee that a variable's lifetime is confined to a single function stack frame, it "escapes" that variable to the heap. Frequent heap allocations force the Garbage Collector (GC) to run constantly, causing latency spikes and high CPU utilization.
This is similar to a factory wasting time constantly ordering and throwing away new boxes instead of reusing them or properly estimating box sizes upfront.

## What We Optimized
Using the `go build -gcflags="-m"` tool, we audited the pipeline and discovered multiple inefficient heap allocations caused by dynamically growing slices.

We implemented the following senior-level memory optimizations:
1. **Zero-Allocation Chunk Streaming (`internal/chunker/chunker.go`)**: Previously, the `Chunk()` method allocated an entire slice of all chunks in memory, which is a classic junior mistake that causes massive memory spikes for large documents. We implemented a `StreamChunks` generator pattern that computes chunks dynamically and yields them directly to the orchestrator channel. This turns memory consumption from `O(N)` into **`O(1)`**, meaning the pipeline uses the exact same amount of memory regardless of document size!
2. **Pre-allocated Embeddings & Dead Letters (`internal/pipeline/ingest.go`)**: Using the new `EstimatedChunkCount` mathematical formula, we now pre-allocate the final aggregation arrays before the worker pool even starts, eliminating dynamic growth allocations.
3. **Pointer Boxing Fix for `json.Encode` (`internal/embedder/*.go`)**: We noticed that passing `reqBody` by value to `json.NewEncoder().Encode(reqBody)` forced the Go runtime to box the struct into an `interface{}`, which always triggers a heap allocation. By passing a pointer `Encode(&reqBody)`, escape analysis keeps the struct safely on the stack.
4. **Fixed-Size Array over Slices (`internal/embedder/gemini.go`)**: In our Gemini API request payload, we replaced the dynamic `Parts []struct` slice with a fixed-size `Parts [1]struct` array. This completely eliminated a heap allocation on every single API request while still marshaling perfectly to the expected JSON array structure.

## Why This Matters
Performance isn't just about concurrency; it's about mechanical sympathy with the runtime. By eliminating hundreds of thousands of heap allocations during batch ingestion runs, we dramatically reduced the GC pressure. The pipeline now runs cooler, faster, and consumes less memory at peak concurrency.
