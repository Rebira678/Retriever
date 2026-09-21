package storage_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/Rebira678/Retriever/internal/config"
	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHNSWOptimizationAndSearch(t *testing.T) {
	cfg := config.FromEnv()
	dbURL := cfg.DatabaseURL

	ctx := context.Background()

	// 1. Initialize DB and drop index if exists to start fresh
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()
	
	// Ensure we can connect. If not, skip (e.g. running in CI without postgres)
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}

	_, _ = pool.Exec(ctx, "DROP INDEX IF EXISTS chunks_embedding_idx;")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS chunks CASCADE;")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS ingested_documents CASCADE;")
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS dead_letters CASCADE;")

	dimension := cfg.EmbeddingDimension
	store, err := storage.NewPostgresStorage(ctx, dbURL, dimension)
	if err != nil {
		t.Fatalf("Failed to init storage: %v", err)
	}
	defer store.Close()

	// 2. Insert dummy vectors
	docID := "test-index-doc"
	modelName := "test-model-1"
	var embeddings []models.Embedding
	for i := 0; i < 50; i++ {
		embeddings = append(embeddings, models.Embedding{
			DocumentID:  docID,
			ChunkIndex:  i,
			Model:       modelName,
			Text:        "Test chunk content",
			ContentHash: fmt.Sprintf("hash-%d", i),
			Vector:      generateRandomVector(dimension),
			CreatedAt:   time.Now(),
		})
	}

	if err := store.SaveEmbeddings(ctx, embeddings); err != nil {
		t.Fatalf("Failed to save embeddings: %v", err)
	}

	// 3. Search without index (Sequential scan)
	queryVec := generateRandomVector(dimension)
	results, err := store.SearchSimilar(ctx, queryVec, modelName, 5, 0)
	if err != nil {
		t.Fatalf("SearchSimilar without index failed: %v", err)
	}
	if len(results) != 5 {
		t.Fatalf("Expected 5 results, got %d", len(results))
	}

	// 4. Optimize Index
	if err := store.OptimizeIndex(ctx, dimension, 16, 64); err != nil {
		t.Fatalf("OptimizeIndex failed: %v", err)
	}

	// Ensure second call doesn't fail (IF NOT EXISTS should protect it)
	if err := store.OptimizeIndex(ctx, dimension, 16, 64); err != nil {
		t.Fatalf("OptimizeIndex second call failed: %v", err)
	}

	// 5. Search with index, normal ef_search
	resultsWithIndex, err := store.SearchSimilar(ctx, queryVec, modelName, 5, 40)
	if err != nil {
		t.Fatalf("SearchSimilar with index failed: %v", err)
	}
	if len(resultsWithIndex) != 5 {
		t.Fatalf("Expected 5 results with index, got %d", len(resultsWithIndex))
	}

	// 6. Search with index, high ef_search (dynamic injection)
	resultsHighEf, err := store.SearchSimilar(ctx, queryVec, modelName, 5, 150)
	if err != nil {
		t.Fatalf("SearchSimilar with high ef_search failed: %v", err)
	}
	if len(resultsHighEf) != 5 {
		t.Fatalf("Expected 5 results with high ef_search, got %d", len(resultsHighEf))
	}

	// Cleanup
	_, _ = pool.Exec(ctx, "DROP INDEX IF EXISTS chunks_embedding_idx;")
}

func TestOptimizeIndexMaxDimension(t *testing.T) {
	cfg := config.FromEnv()
	dbURL := cfg.DatabaseURL

	ctx := context.Background()
	
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	pool.Close()

	// High dimension
	dimension := 2500
	store, err := storage.NewPostgresStorage(ctx, dbURL, dimension)
	if err != nil {
		t.Fatalf("Failed to init storage: %v", err)
	}
	defer store.Close()

	// Should fail because pgvector HNSW max is 2000
	err = store.OptimizeIndex(ctx, dimension, 16, 64)
	if err == nil {
		t.Fatalf("OptimizeIndex should have failed for dimension > 2000")
	}
}

func generateRandomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		vec[i] = rand.Float32()
	}
	return vec
}
