package chunker

import (
	"strings"
	"testing"
)

// ─────────────────────────────────────────────────────────────────────────────
// Day 29 Tests: Document Chunking (Fixed-Size + Overlap)
//
// These tests validate the core chunking algorithm that every subsequent day
// depends on. If chunking is wrong, embeddings are wrong, search is wrong,
// and the entire RAG pipeline is useless.
// ─────────────────────────────────────────────────────────────────────────────

func TestNew_DefaultsOnInvalidInput(t *testing.T) {
	t.Run("negative chunk size defaults to 512", func(t *testing.T) {
		c := New(-1, 10)
		if c.ChunkSize() != 512 {
			t.Errorf("expected default chunk size 512, got %d", c.ChunkSize())
		}
	})

	t.Run("zero chunk size defaults to 512", func(t *testing.T) {
		c := New(0, 10)
		if c.ChunkSize() != 512 {
			t.Errorf("expected default chunk size 512, got %d", c.ChunkSize())
		}
	})

	t.Run("negative overlap defaults to 0", func(t *testing.T) {
		c := New(100, -5)
		if c.Overlap() != 0 {
			t.Errorf("expected overlap 0, got %d", c.Overlap())
		}
	})

	t.Run("overlap >= chunkSize defaults to 0", func(t *testing.T) {
		c := New(100, 100)
		if c.Overlap() != 0 {
			t.Errorf("expected overlap 0 when overlap >= chunkSize, got %d", c.Overlap())
		}
	})

	t.Run("overlap > chunkSize defaults to 0", func(t *testing.T) {
		c := New(100, 200)
		if c.Overlap() != 0 {
			t.Errorf("expected overlap 0, got %d", c.Overlap())
		}
	})
}

func TestChunk_EmptyInput(t *testing.T) {
	c := New(100, 10)

	t.Run("empty string returns nil", func(t *testing.T) {
		chunks := c.Chunk("")
		if chunks != nil {
			t.Errorf("expected nil for empty string, got %d chunks", len(chunks))
		}
	})

	t.Run("whitespace-only string returns nil", func(t *testing.T) {
		chunks := c.Chunk("   \n\t\n   ")
		if chunks != nil {
			t.Errorf("expected nil for whitespace-only string, got %d chunks", len(chunks))
		}
	})
}

func TestChunk_SingleChunkFitsExactly(t *testing.T) {
	// Text shorter than chunkSize → single chunk, no splitting needed
	c := New(100, 10)
	text := "Hello world this is a short document."

	chunks := c.Chunk(text)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk, got %d", len(chunks))
	}
	if chunks[0].Index != 0 {
		t.Errorf("expected index 0, got %d", chunks[0].Index)
	}
	if chunks[0].StartOffset != 0 {
		t.Errorf("expected start offset 0, got %d", chunks[0].StartOffset)
	}
}

func TestChunk_FixedSizeNoOverlap(t *testing.T) {
	// With overlap=0, chunks should be contiguous with no repetition
	c := New(10, 0)
	text := "ABCDEFGHIJKLMNOPQRST" // 20 chars

	chunks := c.Chunk(text)
	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks, got %d", len(chunks))
	}

	if chunks[0].Text != "ABCDEFGHIJ" {
		t.Errorf("chunk 0: expected 'ABCDEFGHIJ', got '%s'", chunks[0].Text)
	}
	if chunks[1].Text != "KLMNOPQRST" {
		t.Errorf("chunk 1: expected 'KLMNOPQRST', got '%s'", chunks[1].Text)
	}

	// Verify no overlap — chunk 1 starts where chunk 0 ends
	if chunks[1].StartOffset != chunks[0].EndOffset {
		t.Errorf("expected contiguous chunks: chunk0.end=%d, chunk1.start=%d",
			chunks[0].EndOffset, chunks[1].StartOffset)
	}
}

func TestChunk_FixedSizeWithOverlap(t *testing.T) {
	// With overlap=3, the last 3 chars of chunk N should appear at the start of chunk N+1
	c := New(10, 3)
	text := "ABCDEFGHIJKLMNOPQRST" // 20 chars

	chunks := c.Chunk(text)

	t.Logf("Got %d chunks:", len(chunks))
	for i, ch := range chunks {
		t.Logf("  Chunk %d: [%d:%d] '%s'", i, ch.StartOffset, ch.EndOffset, ch.Text)
	}

	if len(chunks) < 2 {
		t.Fatalf("expected at least 2 chunks, got %d", len(chunks))
	}

	// Verify overlap: last 3 chars of chunk 0 == first 3 chars of chunk 1
	chunk0Tail := chunks[0].Text[len(chunks[0].Text)-3:]
	chunk1Head := chunks[1].Text[:3]
	if chunk0Tail != chunk1Head {
		t.Errorf("expected overlap: chunk0 tail '%s' should equal chunk1 head '%s'",
			chunk0Tail, chunk1Head)
	}
}

func TestChunk_OverlapPreservesContext(t *testing.T) {
	// Scenario: A sentence spans a chunk boundary.
	// With overlap, the sentence should appear in full in at least one chunk.
	c := New(30, 10)
	text := "The quick brown fox jumps over the lazy dog and runs away fast"

	chunks := c.Chunk(text)

	// The word "over" appears around position 26-30 which is near the chunk boundary
	// With overlap, "over" should not be split
	allText := ""
	for _, ch := range chunks {
		allText += ch.Text + " "
	}

	if !strings.Contains(allText, "over") {
		t.Error("overlap failed to preserve the word 'over' at chunk boundary")
	}
}

func TestChunk_IndexesAreSequential(t *testing.T) {
	c := New(20, 5)
	text := strings.Repeat("A sentence of text. ", 10) // ~200 chars

	chunks := c.Chunk(text)

	for i, ch := range chunks {
		if ch.Index != i {
			t.Errorf("chunk %d has index %d, expected %d", i, ch.Index, i)
		}
	}
}

func TestChunk_OffsetsAreValid(t *testing.T) {
	c := New(50, 10)
	text := strings.Repeat("Word ", 100) // 500 chars

	chunks := c.Chunk(text)

	for i, ch := range chunks {
		if ch.StartOffset < 0 {
			t.Errorf("chunk %d: negative start offset %d", i, ch.StartOffset)
		}
		if ch.EndOffset <= ch.StartOffset {
			t.Errorf("chunk %d: end offset %d <= start offset %d", i, ch.EndOffset, ch.StartOffset)
		}
		if ch.EndOffset > len(normalizeWhitespace(text)) {
			t.Errorf("chunk %d: end offset %d exceeds text length", i, ch.EndOffset)
		}
	}
}

func TestChunk_LastChunkCanBeShorter(t *testing.T) {
	// When text length is not evenly divisible by (chunkSize - overlap),
	// the last chunk should be shorter
	c := New(10, 0)
	text := "ABCDEFGHIJKLM" // 13 chars

	chunks := c.Chunk(text)
	lastChunk := chunks[len(chunks)-1]

	if len(lastChunk.Text) > c.ChunkSize() {
		t.Errorf("last chunk exceeds chunk size: len=%d, max=%d",
			len(lastChunk.Text), c.ChunkSize())
	}
}

func TestChunk_CoversEntireDocument(t *testing.T) {
	// Every character in the original text must appear in at least one chunk
	c := New(15, 3)
	text := "ABCDEFGHIJKLMNOPQRSTUVWXYZ"

	chunks := c.Chunk(text)

	// Build a coverage map
	covered := make([]bool, len(text))
	for _, ch := range chunks {
		for i := ch.StartOffset; i < ch.EndOffset; i++ {
			covered[i] = true
		}
	}

	for i, ok := range covered {
		if !ok {
			t.Errorf("character at position %d ('%c') is not covered by any chunk",
				i, text[i])
		}
	}
}

func TestChunk_WhitespaceNormalization(t *testing.T) {
	c := New(100, 10)
	text := "Hello    world\n\n\n\tthis   is    messy    text"

	chunks := c.Chunk(text)
	if len(chunks) != 1 {
		t.Fatalf("expected 1 chunk after normalization, got %d", len(chunks))
	}

	expected := "Hello world this is messy text"
	if chunks[0].Text != expected {
		t.Errorf("expected normalized text '%s', got '%s'", expected, chunks[0].Text)
	}
}

func TestChunk_LargeDocument(t *testing.T) {
	// Simulate a real document: ~10KB of text
	c := New(512, 64)
	text := strings.Repeat("This is a paragraph of text for testing. It has multiple sentences. Each sentence adds context. ", 100)

	chunks := c.Chunk(text)

	if len(chunks) == 0 {
		t.Fatal("expected non-zero chunks for large document")
	}

	// Each chunk should be at most chunkSize
	for i, ch := range chunks {
		if len(ch.Text) > c.ChunkSize() {
			t.Errorf("chunk %d exceeds max size: %d > %d", i, len(ch.Text), c.ChunkSize())
		}
	}

	t.Logf("Large document: %d chars → %d chunks (size=%d, overlap=%d)",
		len(normalizeWhitespace(text)), len(chunks), c.ChunkSize(), c.Overlap())
}

func TestChunk_CreatedAtIsSet(t *testing.T) {
	c := New(100, 10)
	chunks := c.Chunk("Some text that needs chunking.")

	for i, ch := range chunks {
		if ch.CreatedAt.IsZero() {
			t.Errorf("chunk %d has zero CreatedAt timestamp", i)
		}
	}
}

func TestChunk_ContentHash(t *testing.T) {
	c := New(50, 0)
	// These two texts will result in identical chunks after whitespace normalization
	text1 := "The quick brown fox jumps over the lazy dog."
	text2 := "The    quick   brown   fox jumps over the lazy dog."
	
	chunks1 := c.Chunk(text1)
	chunks2 := c.Chunk(text2)
	
	if len(chunks1) == 0 || len(chunks2) == 0 {
		t.Fatal("expected chunks, got none")
	}
	
	// Hashes should match for identical content
	if chunks1[0].ContentHash != chunks2[0].ContentHash {
		t.Errorf("expected identical hashes for identical text, got %s and %s", chunks1[0].ContentHash, chunks2[0].ContentHash)
	}
	
	// Different content should have different hashes
	text3 := "A completely different document text."
	chunks3 := c.Chunk(text3)
	
	if chunks1[0].ContentHash == chunks3[0].ContentHash {
		t.Errorf("expected different hashes for different text, got same hash %s", chunks1[0].ContentHash)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Benchmarks — important for Day 31+ when we need throughput numbers
// ─────────────────────────────────────────────────────────────────────────────

func BenchmarkChunk_SmallDoc(b *testing.B) {
	c := New(512, 64)
	text := strings.Repeat("A short document. ", 10)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Chunk(text)
	}
}

func BenchmarkChunk_LargeDoc(b *testing.B) {
	c := New(512, 64)
	text := strings.Repeat("A sentence in a very large document. ", 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Chunk(text)
	}
}
