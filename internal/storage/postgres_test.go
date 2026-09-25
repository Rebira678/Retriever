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

func TestLostAckIdempotency(t *testing.T) {
	// Only run this test if a database is available
	cfg := config.FromEnv()
	ctx := context.Background()
	store, err := storage.NewPostgresStorage(ctx, cfg.DatabaseURL, 1536)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	defer store.Close()

	hash := "test-lost-ack-hash-" + time.Now().String()
	docID := "doc-lost-ack"

	// 1. Initial processing starts (Lock Acquired)
	acquired, err := store.StartIngestion(ctx, hash, docID)
	if err != nil {
		t.Fatalf("Failed to acquire initial lock: %v", err)
	}
	if !acquired {
		t.Fatalf("Should acquire lock on first attempt")
	}

	// 2. Simulate "Lost Ack" scenario:
	// The embeddings are saved successfully, but the network drops before
	// CompleteIngestion is called. The database still thinks status = STARTED.

	// 3. System retries the exact same document
	acquiredRetry, err := store.StartIngestion(ctx, hash, docID)
	if err != nil {
		t.Fatalf("Failed to acquire retry lock: %v", err)
	}
	
	// PROOF: The retry MUST STOP (return false) because the lock is still held.
	// It acts as a dead man's switch: without proof of failure, it safely halts
	// instead of blindly re-processing and causing duplicate vectors.
	if acquiredRetry {
		t.Fatalf("Should NOT acquire lock on retry if status is still STARTED")
	}

	// 4. Contrast: What if it actually FAILED and the ack was recorded?
	hashFail := "test-failed-hash-" + time.Now().String()
	acquiredFail, err := store.StartIngestion(ctx, hashFail, docID)
	if err != nil {
		t.Fatalf("Failed to acquire fail lock: %v", err)
	}
	if !acquiredFail {
		t.Fatalf("Should acquire fail lock")
	}

	err = store.CompleteIngestion(ctx, hashFail, models.StatusFailed)
	if err != nil {
		t.Fatalf("Failed to complete ingestion with failure: %v", err)
	}

	// System retries the failed document
	acquiredRetryFail, err := store.StartIngestion(ctx, hashFail, docID)
	if err != nil {
		t.Fatalf("Failed to acquire retry lock after failure: %v", err)
	}
	
	// PROOF: It MUST CONTINUE (return true) because we explicitly recorded the failure.
	if !acquiredRetryFail {
		t.Fatalf("Should acquire lock on retry if previous status was FAILED")
	}
}

func TestPing_Senior_ContextTimeout(t *testing.T) {
	cfg := config.FromEnv()
	ctx := context.Background()
	store, err := storage.NewPostgresStorage(ctx, cfg.DatabaseURL, 1536)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	defer store.Close()

	// 1. Happy path
	if err := store.Ping(ctx); err != nil {
		t.Fatalf("Ping failed on healthy database: %v", err)
	}

	// 2. Expert Edge Case: Context Timeout Propagation
	// Ensure that if the database hangs, our probe doesn't hang indefinitely.
	timeoutCtx, cancel := context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()
	
	time.Sleep(10 * time.Millisecond) // Ensure context expires
	
	err = store.Ping(timeoutCtx)
	if err == nil {
		t.Fatalf("Expected Ping to fail immediately due to expired context, but it succeeded")
	}
}

func TestSaveEmbeddings_Senior_HighConcurrency_Race(t *testing.T) {
	cfg := config.FromEnv()
	ctx := context.Background()
	store, err := storage.NewPostgresStorage(ctx, cfg.DatabaseURL, 1536)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	defer store.Close()

	modelName := "test-race-model"
	docID := "test-race-doc-" + time.Now().String()

	// Expert Architecture: Test high-concurrency race conditions.
	// We simulate 100 concurrent workers trying to save identical chunks 
	// simultaneously to ensure ON CONFLICT DO UPDATE handles race conditions
	// gracefully without triggering unique constraint violations or deadlocks.
	workers := 100
	errCh := make(chan error, workers)
	
	vec := make([]float32, 1536)
	vec[0] = 0.5
	
	emb := models.Embedding{
		DocumentID:  docID,
		ChunkIndex:  0,
		Model:       modelName,
		Text:        "Concurrent access test chunk.",
		ContentHash: "hash-race",
		Vector:      vec,
		CreatedAt:   time.Now(),
	}

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			// We pass a copy of the slice to avoid data races in the test itself
			errCh <- store.SaveEmbeddings(ctx, []models.Embedding{emb})
		}(i)
	}

	for i := 0; i < workers; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("Concurrency failure: expected ON CONFLICT to handle race conditions gracefully, got: %v", err)
		}
	}
}

func TestSearchSimilar_Senior_TableDriven_EdgeCases(t *testing.T) {
	cfg := config.FromEnv()
	ctx := context.Background()
	store, err := storage.NewPostgresStorage(ctx, cfg.DatabaseURL, 1536)
	if err != nil {
		t.Skipf("Skipping integration test; database not available: %v", err)
	}
	defer store.Close()

	modelName := "test-table-model"
	
	// Setup test data
	validVec := make([]float32, 1536)
	validVec[0] = 1.0
	
	_ = store.SaveEmbeddings(ctx, []models.Embedding{{
		DocumentID:  "doc-valid",
		ChunkIndex:  0,
		Model:       modelName,
		Text:        "Valid chunk",
		ContentHash: "hash-valid",
		Vector:      validVec,
	}})

	// Senior level: Table-Driven Testing covering adversarial inputs
	tests := []struct {
		name        string
		queryVec    []float32
		model       string
		topK        int
		expectError bool
		errContains string
	}{
		{
			name:        "Valid Request",
			queryVec:    validVec,
			model:       modelName,
			topK:        5,
			expectError: false,
		},
		{
			name:        "Zero TopK Truncation",
			queryVec:    validVec,
			model:       modelName,
			topK:        0,
			expectError: false, // PostgreSQL handles LIMIT 0 gracefully
		},
		{
			name:        "Negative TopK Protection",
			queryVec:    validVec,
			model:       modelName,
			topK:        -1,
			expectError: true,
			errContains: "failed to execute similarity search", // Postgres syntax error
		},
		{
			name:        "Model Isolation (Wrong Model)",
			queryVec:    validVec,
			model:       "completely-different-model",
			topK:        5,
			expectError: false, // Shouldn't error, just return 0 results
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results, err := store.SearchSimilar(ctx, tt.queryVec, tt.model, tt.topK, 0)
			
			if tt.expectError {
				if err == nil {
					t.Fatalf("Expected error containing '%s', got nil", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("Expected error containing '%s', got: %v", tt.errContains, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			
			// If wrong model, ensure 0 results
			if tt.model != modelName && len(results) > 0 {
				t.Errorf("Model isolation failed: expected 0 results for unknown model, got %d", len(results))
			}
		})
	}
}
