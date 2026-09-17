# Day 37 — System Reliability: Bounded Queues & Backpressure

**Date:** September 17, 2026
**Concept to Master:** Backpressure theory, Bounded Channels, Blocking Sends
**Build:** Refactored ingestion pipeline to use bounded queues to prevent OOM errors and apply backpressure when embedding workers are slow.

---

## 🎯 What We Built Today

Unbounded queues in a distributed system are ticking time bombs. Today, we introduced **Backpressure** into the Retriever pipeline using bounded Go channels.

### 1. Bounded Queues & Backpressure
* **What we did:** Replaced unbounded `chunkChan` and `resultsChan` slices/buffers with bounded channels (`cfg.IngestionQueueSize`). The chunk producer now runs in a separate goroutine.
* **Why it matters:** If the embedding API (OpenAI/Gemini) rate-limits our worker pool, the consumer workers slow down. With unbounded queues, the ingestion process would blindly continue loading thousands of chunks into memory, eventually crashing the server with an OOM (Out Of Memory) error. With bounded channels, a full queue blocks the producer's `send` operation. This propagates backpressure up the stack—gracefully slowing down ingestion to match the API's throughput.
* **Concurrency Fixes:** Moved the producer loop and the `pool.Run` consumer into their own goroutines, allowing the main thread to process completed results concurrently and avoid deadlocking when the results queue fills up.

---

## 📱 Social Media Angles

### LinkedIn Version

Day 37: Unbounded queues are a ticking time bomb in any distributed system. 💣

In my Go RAG pipeline, the ingestion process splits massive documents into thousands of chunks and hands them to a concurrent worker pool for vector embeddings. 

But what happens when the OpenAI API applies a rate-limit and the worker pool slows down?

If you use unbounded queues (or slices), the ingestion process doesn't care. It keeps blindly stuffing chunks into RAM until the server crashes from an OOM (Out of Memory) error. 

Today, I engineered the pipeline to use **Backpressure**:
1️⃣ Configured bounded channels (`IngestionQueueSize = 100`) between the producer and consumer.
2️⃣ When the embedding workers get rate-limited, the channel fills up.
3️⃣ The full channel naturally blocks the producer's `send` operation.
4️⃣ The pipeline gracefully slows down to match the exact throughput of the downstream API.

Zero dropped data. Zero OOM crashes. Just a system that breathes in and out based on downstream capacity. 

Have you ever brought down a production service with an unbounded queue? Let's hear the war stories below. 👇

#golang #backendengineering #systemarchitecture #rag #distributedsystems #softwareengineering

### X (Twitter) Version

Day 37: Unbounded queues are a ticking time bomb. 💣

If your embedding API rate-limits you, an unbounded RAG ingestion pipeline will keep stuffing chunks into memory until it OOM crashes.

Today I implemented Backpressure in Go:
🧱 Bounded channels (QueueSize=100)
🛑 Blocking sends when workers slow down

The pipeline now gracefully throttles to match API throughput. Zero memory leaks, zero dropped data. 🚀
