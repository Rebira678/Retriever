package storage_test

import (
	"context"
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
