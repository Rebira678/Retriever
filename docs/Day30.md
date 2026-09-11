# Day 30: Embedding APIs and Conceptual Underpinnings

Welcome to Day 30 of the roadmap! Today we focused on integrating our ingestion pipeline with an **Embedding API**. Specifically, we implemented the OpenAI Embeddings API to convert our chunks of text into high-dimensional vectors (arrays of floats).

## What are Vector Embeddings?

Vector embeddings are at the core of any RAG (Retrieval-Augmented Generation) pipeline. They serve as the "Map of Meaning" for text.

Instead of matching exact keywords (e.g., searching for "dog" and missing "puppy"), vector embeddings capture the semantic meaning of the text. An embedding model (like OpenAI's `text-embedding-3-small`) takes a string of text and outputs a list of 1,536 floating-point numbers.

In this 1536-dimensional space, strings with similar meaning are located close to each other. For example, the vector for "How do I reset my password?" will be mathematically very close to the vector for "I forgot my login credentials", even though they share almost no common words.

## Today's Implementation

1.  **Configuration**: We updated `internal/config/config.go` to handle `RETRIEVER_OPENAI_API_KEY` and `RETRIEVER_EMBEDDING_API_URL`. This allows our application to read credentials securely without hardcoding them, adhering to the 12-Factor App methodology.
2.  **Embedder Package**: We created `internal/embedder/embedder.go` containing:
    *   An `Embedder` interface to abstract the embedding generation.
    *   An `OpenAIEmbedder` implementation that makes HTTP POST requests to the OpenAI API (or any compatible endpoint).
    *   Proper error handling, serialization of requests, and parsing of JSON responses.
3.  **Testing**: We wrote `internal/embedder/embedder_test.go` using a mock HTTP server (`httptest.NewServer`) to simulate the OpenAI API. This allows us to test our client logic without making real API calls, saving costs and ensuring our test suite runs fast and deterministically.
4.  **Main Wiring**: We updated `cmd/retriever/main.go` to wire up the new Embedder. If a user sets the `RETRIEVER_OPENAI_API_KEY` environment variable, the demo will actually call the API and display a sample of the returned 1536-dimensional vector for the first chunk.

## Why use an Interface?
By defining the `Embedder` interface:
```go
type Embedder interface {
	EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error)
}
```
We decoupled our pipeline from OpenAI. Tomorrow, if we want to run a local embedding model via Ollama or HuggingFace, we simply write a new struct that implements `EmbedChunk` and inject it into our pipeline. The rest of the codebase remains entirely unchanged!

## Running the Code

To run the tests:
```bash
make test
```
*(or `go test ./...`)*

To run the demo (without an API key):
```bash
make run
```
*(You'll see a log saying "Skipping embedding generation")*

To run the demo (with an API key):
```bash
export RETRIEVER_OPENAI_API_KEY="your-sk-key"
make run
```
*(You'll see the chunked documents and a snippet of the generated embedding array!)*
