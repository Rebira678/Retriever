// Package models defines the core domain types for the Retriever pipeline.
//
// These models represent the data flowing through each stage:
//   Ingestion → Chunking → Embedding → Storage → Search
//
// Each model is intentionally simple and serializable — no database-specific
// tags here. Storage adapters handle the mapping.
package models

import "time"

// Document represents a raw document ingested into the pipeline.
type Document struct {
	// ID is the unique identifier for the document (UUID).
	ID string `json:"id"`

	// Source is the origin of the document (e.g., file path, URL, API).
	Source string `json:"source"`

	// Content is the raw text content of the document.
	Content string `json:"content"`

	// ContentHash is the SHA-256 hash of Content, used for idempotent re-ingestion.
	// If a document with the same hash already exists, we skip re-processing.
	ContentHash string `json:"content_hash"`

	// Metadata holds arbitrary key-value pairs (e.g., author, date, tags).
	Metadata map[string]string `json:"metadata,omitempty"`

	// CreatedAt is the timestamp when the document was ingested.
	CreatedAt time.Time `json:"created_at"`
}

// Chunk represents a segment of a document after chunking.
type Chunk struct {
	// Index is the sequential position of this chunk within its parent document.
	Index int `json:"index"`

	// Text is the chunk content.
	Text string `json:"text"`

	// StartOffset is the character offset where this chunk begins in the original document.
	StartOffset int `json:"start_offset"`

	// EndOffset is the character offset where this chunk ends in the original document.
	EndOffset int `json:"end_offset"`

	// DocumentID links this chunk back to its source document.
	DocumentID string `json:"document_id,omitempty"`

	// CreatedAt is the timestamp when the chunk was created.
	CreatedAt time.Time `json:"created_at"`
}

// Embedding represents a vector embedding of a chunk.
type Embedding struct {
	// ID is the unique identifier for this embedding.
	ID string `json:"id"`

	// ChunkIndex is the index of the chunk this embedding represents.
	ChunkIndex int `json:"chunk_index"`

	// Text is the actual content of the chunk, saved alongside the vector.
	Text string `json:"text"`

	// DocumentID links back to the source document.
	DocumentID string `json:"document_id"`

	// Vector is the embedding vector (e.g., 1536-dimensional for OpenAI text-embedding-3-small).
	Vector []float32 `json:"vector"`

	// Model is the embedding model used (e.g., "text-embedding-3-small").
	Model string `json:"model"`

	// CreatedAt is the timestamp when the embedding was generated.
	CreatedAt time.Time `json:"created_at"`
}

// SearchResult represents a single result from a similarity search query.
type SearchResult struct {
	// ChunkText is the text of the matched chunk.
	ChunkText string `json:"chunk_text"`

	// DocumentID links back to the source document.
	DocumentID string `json:"document_id"`

	// Score is the similarity score (higher = more similar for cosine, lower for L2).
	Score float64 `json:"score"`

	// ChunkIndex is the position of this chunk in the original document.
	ChunkIndex int `json:"chunk_index"`
}

// IngestionStatus defines the current state of a document in the idempotency store.
type IngestionStatus string

const (
	StatusStarted   IngestionStatus = "STARTED"
	StatusCompleted IngestionStatus = "COMPLETED"
	StatusFailed    IngestionStatus = "FAILED"
)

// DeadLetter represents a failed ingestion or embedding task that couldn't be processed.
type DeadLetter struct {
	// ID is the unique identifier for the dead letter entry.
	ID string `json:"id"`

	// DocumentID links back to the source document, if known.
	DocumentID string `json:"document_id"`

	// ChunkIndex is the index of the chunk that failed, if applicable.
	ChunkIndex int `json:"chunk_index"`

	// Error is the stringified error message explaining why processing failed.
	Error string `json:"error"`

	// Payload is the raw data (e.g., chunk text) that caused the failure.
	Payload string `json:"payload"`

	// CreatedAt is the timestamp when the failure occurred.
	CreatedAt time.Time `json:"created_at"`
}
