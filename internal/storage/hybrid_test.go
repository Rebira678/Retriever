package storage_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/Rebira678/Retriever/internal/config"
	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/storage"
)

func generateTestVector(dimension int, primaryIndex int, weight float32) []float32 {
	vec := make([]float32, dimension)
	if primaryIndex >= 0 && primaryIndex < dimension {
		vec[primaryIndex] = weight
	}
	return vec
}

func setupHybridTestDB(t *testing.T) (*storage.PostgresStorage, context.Context, string) {
	cfg := config.FromEnv()
	ctx := context.Background()

	// Use the production dimension to avoid schema conflicts with existing DB tables
	dimension := 1536
	store, err := storage.NewPostgresStorage(ctx, cfg.DatabaseURL, dimension)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}

	modelName := "test-hybrid-model-" + time.Now().Format("20060102150405")

	// Insert test data
	chunks := []models.Embedding{
		{
			DocumentID:  "doc-apple",
			ChunkIndex:  1,
			Model:       modelName,
			Text:        "Apple Q3 revenue exceeded expectations with strong iPhone sales.",
			ContentHash: "hash1",
			Vector:      generateTestVector(dimension, 0, 1.0),
			CreatedAt:   time.Now(),
		},
		{
			DocumentID:  "doc-apple",
			ChunkIndex:  2,
			Model:       modelName,
			Text:        "The company's third-quarter earnings were incredibly impressive.",
			ContentHash: "hash2",
			Vector:      generateTestVector(dimension, 0, 0.9), // Semantically similar to Q3 revenue
			CreatedAt:   time.Now(),
		},
		{
			DocumentID:  "doc-fox",
			ChunkIndex:  1,
			Model:       modelName,
			Text:        "The quick brown fox jumps over the lazy dog.",
			ContentHash: "hash3",
			Vector:      generateTestVector(dimension, 1, 1.0),
			CreatedAt:   time.Now(),
		},
		{
			DocumentID:  "doc-random",
			ChunkIndex:  1,
			Model:       modelName,
			Text:        "Just some random words about nothing in particular.",
			ContentHash: "hash4",
			Vector:      generateTestVector(dimension, 2, 1.0),
			CreatedAt:   time.Now(),
		},
	}

	err = store.SaveEmbeddings(ctx, chunks)
	if err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	return store, ctx, modelName
}

func TestHybridSearch_Correctness_RRF(t *testing.T) {
	store, ctx, modelName := setupHybridTestDB(t)
	defer store.Close()

	// Scenario: We search for "Apple Q3 revenue".
	// The vector should match both doc-apple chunks heavily.
	// But the keyword search should perfectly match Chunk 1 ("Apple Q3 revenue") and ignore Chunk 2.
	// The RRF score of Chunk 1 should be boosted to the absolute top.
	queryText := "Apple Q3 revenue"
	queryVec := generateTestVector(1536, 0, 1.0) // Matches doc-apple concept

	results, err := store.SearchHybrid(ctx, queryText, queryVec, modelName, 5, 0)
	if err != nil {
		t.Fatalf("Hybrid search failed: %v", err)
	}

	if len(results) < 2 {
		t.Fatalf("Expected at least 2 results, got %d", len(results))
	}

	// Chunk 1 MUST be the top result due to the exact keyword match fusing with high vector similarity.
	if !strings.Contains(results[0].ChunkText, "Apple Q3") {
		t.Errorf("Expected exact match to be boosted to top. Top result was: %s", results[0].ChunkText)
	}

	// Score validation (RRF is typically > 0.0)
	if results[0].Score <= 0 {
		t.Errorf("Expected positive RRF score, got %f", results[0].Score)
	}
}

func TestHybridSearch_EdgeCase_OnlyVectorMatches(t *testing.T) {
	store, ctx, modelName := setupHybridTestDB(t)
	defer store.Close()

	// Scenario: Query has keywords that do NOT exist in the DB, 
	// but the vector implies semantic similarity.
	// This tests if Scatter-Gather safely handles one empty set (Keyword returns 0).
	queryText := "supercalifragilisticexpialidocious earnings" 
	queryVec := generateTestVector(1536, 0, 0.95) // High similarity to doc-apple

	results, err := store.SearchHybrid(ctx, queryText, queryVec, modelName, 5, 0)
	if err != nil {
		t.Fatalf("Hybrid search failed: %v", err)
	}

	if len(results) == 0 {
		t.Fatalf("Expected results from vector search despite no keyword match")
	}

	if !strings.Contains(results[0].ChunkText, "third-quarter earnings") && !strings.Contains(results[0].ChunkText, "Apple Q3") {
		t.Errorf("Expected fallback to semantic vector match, got: %s", results[0].ChunkText)
	}
}

func TestHybridSearch_EdgeCase_TopKTruncation(t *testing.T) {
	store, ctx, modelName := setupHybridTestDB(t)
	defer store.Close()

	// We inserted 4 chunks. We'll search with a vector that slightly matches everything.
	queryText := "fox random apple dog"
	queryVec := generateTestVector(1536, 0, 0.5)
	queryVec[1] = 0.5
	queryVec[2] = 0.5

	// Ask for only 2 results
	results, err := store.SearchHybrid(ctx, queryText, queryVec, modelName, 2, 0)
	if err != nil {
		t.Fatalf("Hybrid search failed: %v", err)
	}

	if len(results) != 2 {
		t.Fatalf("Expected exactly 2 results due to TopK truncation, got %d", len(results))
	}

	// Ask for 10 results (more than exists in DB for this model)
	resultsMax, err := store.SearchHybrid(ctx, queryText, queryVec, modelName, 10, 0)
	if err != nil {
		t.Fatalf("Hybrid search failed: %v", err)
	}

	if len(resultsMax) != 4 {
		t.Fatalf("Expected exactly 4 results (all in DB), got %d", len(resultsMax))
	}
}

func TestHybridSearch_EdgeCase_ContextTimeout(t *testing.T) {
	store, ctx, modelName := setupHybridTestDB(t)
	defer store.Close()

	// Create a context that is ALREADY canceled/timed out.
	timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	// Give it a tiny sleep to guarantee timeout fires
	time.Sleep(10 * time.Millisecond)

	queryText := "Apple"
	queryVec := generateTestVector(1536, 0, 1.0)

	// The errgroup MUST detect the context cancellation and abort execution immediately.
	_, err := store.SearchHybrid(timeoutCtx, queryText, queryVec, modelName, 5, 0)
	
	if err == nil {
		t.Fatalf("Expected error due to context timeout, but got nil")
	}

	// Check if the error string contains typical context cancellation messages from Postgres/errgroup
	if !strings.Contains(err.Error(), "context deadline exceeded") && !strings.Contains(err.Error(), "canceled") {
		t.Errorf("Expected context timeout error, got: %v", err)
	}
}

func TestHybridSearch_SpecialCharacters(t *testing.T) {
	store, ctx, modelName := setupHybridTestDB(t)
	defer store.Close()

	// Using websearch_to_tsquery allows Google-like operators.
	// We'll test with a quote and a minus sign.
	queryText := `"quick brown" -apple`
	queryVec := generateTestVector(1536, 1, 1.0)

	results, err := store.SearchHybrid(ctx, queryText, queryVec, modelName, 5, 0)
	if err != nil {
		t.Fatalf("Hybrid search failed with special chars: %v", err)
	}

	// Shouldn't panic or crash, and should find the fox.
	foundFox := false
	for _, r := range results {
		if strings.Contains(r.ChunkText, "quick brown") {
			foundFox = true
			break
		}
	}

	if !foundFox {
		t.Errorf("Expected to find the fox document using websearch syntax")
	}
}
