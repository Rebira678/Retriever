// Package chunker implements document chunking strategies for RAG pipelines.
//
// # Why Chunking Matters
//
// LLMs have finite context windows. When you feed documents into a RAG system,
// you can't send the entire document — you need to break it into smaller pieces
// (chunks) that:
//   1. Fit within embedding model input limits (typically 512–8192 tokens)
//   2. Contain enough context to be semantically meaningful on their own
//   3. Overlap slightly so information at chunk boundaries isn't lost
//
// # Fixed-Size + Overlap Strategy
//
// This is the simplest and most predictable chunking strategy:
//   - Split text into chunks of N characters
//   - Each chunk overlaps with the previous by M characters
//   - The overlap ensures that sentences spanning a chunk boundary appear
//     in at least one chunk in their entirety
//
// Trade-offs vs other strategies:
//   - ✅ Predictable chunk sizes → consistent embedding costs
//   - ✅ Simple to implement and reason about
//   - ✅ Works well for uniform text (articles, documentation)
//   - ❌ May split mid-sentence (semantic chunking avoids this)
//   - ❌ Doesn't respect document structure (paragraph chunking does)
//
// We start with fixed-size because it's the industry baseline. We'll layer
// sentence-aware and paragraph-aware strategies in later days.
package chunker

import (
	"strings"
	"time"

	"github.com/Rebira678/Retriever/internal/models"
)

// Chunker splits text documents into overlapping chunks.
type Chunker struct {
	chunkSize int
	overlap   int
}

// New creates a Chunker with the given chunk size and overlap.
//
// Constraints:
//   - chunkSize must be > 0 (defaults to 512 if invalid)
//   - overlap must be >= 0 and < chunkSize (defaults to 0 if invalid)
func New(chunkSize, overlap int) *Chunker {
	if chunkSize <= 0 {
		chunkSize = 512
	}
	if overlap < 0 || overlap >= chunkSize {
		overlap = 0
	}
	return &Chunker{
		chunkSize: chunkSize,
		overlap:   overlap,
	}
}

// Chunk splits the input text into overlapping chunks using the fixed-size strategy.
//
// Algorithm:
//   1. Start at position 0
//   2. Take chunkSize characters from current position
//   3. Advance position by (chunkSize - overlap) characters
//   4. Repeat until end of text
//
// Example with chunkSize=10, overlap=3:
//
//   Text:     "ABCDEFGHIJKLMNOPQRST" (20 chars)
//   Chunk 1:  "ABCDEFGHIJ"           [0:10]
//   Chunk 2:  "HIJKLMNOPQ"           [7:17]   ← "HIJ" overlaps
//   Chunk 3:  "OPQRST"               [14:20]  ← "OPQ" overlaps
//
// Returns a slice of Chunk models with metadata (index, offsets, timestamps).
func (c *Chunker) Chunk(text string) []models.Chunk {
	// Normalize whitespace — collapse multiple spaces/newlines but preserve words
	text = normalizeWhitespace(text)

	if len(text) == 0 {
		return nil
	}

	var chunks []models.Chunk
	step := c.chunkSize - c.overlap
	if step <= 0 {
		step = 1 // Safety: prevent infinite loop
	}

	now := time.Now()
	index := 0

	for start := 0; start < len(text); start += step {
		end := start + c.chunkSize
		if end > len(text) {
			end = len(text)
		}

		chunkText := text[start:end]

		// Skip empty or whitespace-only chunks
		if strings.TrimSpace(chunkText) == "" {
			continue
		}

		chunks = append(chunks, models.Chunk{
			Index:       index,
			Text:        chunkText,
			StartOffset: start,
			EndOffset:   end,
			CreatedAt:   now,
		})
		index++

		// If we've reached the end, stop
		if end >= len(text) {
			break
		}
	}

	return chunks
}

// ChunkSize returns the configured chunk size.
func (c *Chunker) ChunkSize() int {
	return c.chunkSize
}

// Overlap returns the configured overlap size.
func (c *Chunker) Overlap() int {
	return c.overlap
}

// normalizeWhitespace collapses consecutive whitespace characters into single spaces
// and trims leading/trailing whitespace.
func normalizeWhitespace(s string) string {
	fields := strings.Fields(s)
	return strings.Join(fields, " ")
}
