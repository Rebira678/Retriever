# Day 33: Similarity Search (Vector Search)

Welcome to Day 33 of the roadmap! Today we built the true "Magic" of RAG: retrieving the most relevant context based on a natural language query using mathematical distance.

## The Problem: Finding Meaning, Not Keywords
Standard database searches rely on keywords. If a user asks "What is the second phase of the RAG pipeline?", a keyword search would look for exact matches of the word "second". But what if the text says "2. Retrieval"? Keyword search fails.

## The Solution: Semantic Vector Search
Because we converted our document chunks into 3072-dimensional vectors (Day 31) and saved them in PostgreSQL using pgvector (Day 32), we can now search by *meaning*.

### What was built:
1. **`internal/models/models.go`**: We updated the `Embedding` struct to carry the original `Text` of the chunk alongside the vector, and defined a `SearchResult` struct to hold the similarity score.
2. **`internal/storage/storage.go`**: We added the `SearchSimilar` method to our interface.
3. **`internal/storage/postgres.go`**: 
   - We implemented the `SearchSimilar` method using pgvector's native `<=>` operator (Cosine Distance). 
   - We utilized `ORDER BY embedding <=> $1 ASC LIMIT $2` to instantly return the mathematically closest chunks to our query.
   - We gracefully handled pgvector's default 2,000 dimension limit for `hnsw` indexes by falling back to exact nearest neighbor (sequential scan) for 3,072-dimensional vectors, guaranteeing accuracy.
4. **`cmd/retriever/main.go`**: We added a demo that automatically takes a user query, embeds it via Gemini, and searches the database for the answer!

## The Result
When the query `"What is the second phase of the RAG pipeline?"` is sent, the database instantly returns the exact chunk containing `"2. Retrieval — a user query is embedded..."` with a highly confident similarity score. 

This proves our entire Retrieval-Augmented Generation ingestion pipeline is functionally complete and production-ready!
