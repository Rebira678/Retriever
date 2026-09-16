# Day 36 — Resilient RAG Ingestion (Idempotency & DLQ)

**Date:** September 16, 2026
**Concept to Master:** System Resiliency, Idempotency, Dead-Letter Queues (DLQ)
**Build:** Hardening the Pipeline

---

## 🎯 What We Built Today

A senior-level distributed system must expect failures and handle duplicates gracefully. Today, we fortified the Retriever pipeline by implementing Idempotency and a Dead-Letter Queue (DLQ). 

### 1. Idempotent Document Ingestion
* **What we did:** Added a content hashing mechanism (`SHA-256`) that checks PostgreSQL (`ingested_documents` table) before launching the worker pool. If the document exists, we skip it.
* **Why it matters:** In distributed systems, retries are inevitable. Without idempotency, a retry would duplicate 1536-dimensional vectors for every chunk, destroying our vector database's search accuracy and wasting expensive API credits.

### 2. Dead-Letter Queue (DLQ) for Failed Embeddings
* **What we did:** Modified our concurrent embedding pool to capture errors instead of quietly dropping them. Failed chunks are now serialized into a `dead_letters` PostgreSQL table.
* **Why it matters:** Dropping data is unacceptable in production. If OpenAI or Gemini APIs rate-limit our pipeline, the failed chunks land safely in the DLQ, allowing an operator or an automated cron job to replay them later without re-processing the entire document.

---

## 📱 Social Media Angles

### LinkedIn Version

Day 36: I realized my Go RAG pipeline was a house of cards. 😅

Sure, it could embed documents concurrently, and it looked great on the "happy path". But what happens when the OpenAI API randomly rate-limits a request? Or what if a network blip causes the system to retry an ingestion?

I’ll tell you what happens: 
- Dropped data.
- Duplicate vectors destroying the search accuracy.
- Wasted API credits.

Today, I stopped building new features and focused entirely on the "unhappy path" to make this architecture actually production-ready.

First, I implemented strict Idempotency using PostgreSQL atomic locks (`ON CONFLICT DO UPDATE`). Before any document hits the worker pool, it's SHA-256 hashed and verified. If it’s already processing or completed, the system skips it entirely. No more race conditions or TOCTOU (Time-of-Check to Time-of-Use) bugs.

Second, I built a high-performance Dead-Letter Queue (DLQ). Instead of silently dropping failed chunks during a rate-limit, the pipeline accumulates them and uses `pgx.Batch` to efficiently serialize the payloads into a Postgres DLQ in a single database round-trip. Zero data loss. 

Building the happy path is the easy part of software engineering. Designing a system that gracefully catches its own failures and protects its own data? That's where the real fun begins.

Next up, I'm diving into deploying this beast and adding observability. 

Have you ever lost production data because of a silent failure? (We've all been there, right? 😂) Let me know below! 👇

#golang #backendengineering #systemarchitecture #rag #artificialintelligence #distributedsystems #softwareengineering 

### X (Twitter) Version

Day 36: Realized my Go RAG pipeline was a house of cards. 😅 

Sure, the happy path worked perfectly, but a single API rate-limit would drop data silently. Not good enough.

Today, I engineered for the unhappy path:
🔒 Idempotency via atomic Postgres locks (zero duplicate vectors)
📥 High-performance Dead-Letter Queue for failed chunks (zero data loss)

Building features is easy. Engineering for failure is where the real fun begins. Next up: deployment! 🚀
