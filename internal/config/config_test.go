package config

import (
	"os"
	"testing"
)

func TestDefault_ReturnsValidConfig(t *testing.T) {
	cfg := Default()

	if cfg.ChunkSize <= 0 {
		t.Errorf("default ChunkSize should be positive, got %d", cfg.ChunkSize)
	}
	if cfg.ChunkOverlap < 0 {
		t.Errorf("default ChunkOverlap should be non-negative, got %d", cfg.ChunkOverlap)
	}
	if cfg.ChunkOverlap >= cfg.ChunkSize {
		t.Errorf("ChunkOverlap (%d) must be < ChunkSize (%d)", cfg.ChunkOverlap, cfg.ChunkSize)
	}
	if cfg.ChunkStrategy == "" {
		t.Error("default ChunkStrategy should not be empty")
	}
	if cfg.WorkerPoolSize <= 0 {
		t.Errorf("default WorkerPoolSize should be positive, got %d", cfg.WorkerPoolSize)
	}
	if cfg.DatabaseURL == "" {
		t.Error("default DatabaseURL should not be empty")
	}
	if cfg.GRPCPort == "" {
		t.Error("default GRPCPort should not be empty")
	}
	if cfg.HTTPPort == "" {
		t.Error("default HTTPPort should not be empty")
	}
}

func TestFromEnv_OverridesDefaults(t *testing.T) {
	// Set environment variables
	os.Setenv("RETRIEVER_CHUNK_SIZE", "256")
	os.Setenv("RETRIEVER_CHUNK_OVERLAP", "32")
	os.Setenv("RETRIEVER_CHUNK_STRATEGY", "sentence")
	defer func() {
		os.Unsetenv("RETRIEVER_CHUNK_SIZE")
		os.Unsetenv("RETRIEVER_CHUNK_OVERLAP")
		os.Unsetenv("RETRIEVER_CHUNK_STRATEGY")
	}()

	cfg := FromEnv()

	if cfg.ChunkSize != 256 {
		t.Errorf("expected ChunkSize 256, got %d", cfg.ChunkSize)
	}
	if cfg.ChunkOverlap != 32 {
		t.Errorf("expected ChunkOverlap 32, got %d", cfg.ChunkOverlap)
	}
	if cfg.ChunkStrategy != "sentence" {
		t.Errorf("expected ChunkStrategy 'sentence', got '%s'", cfg.ChunkStrategy)
	}
}

func TestFromEnv_InvalidEnvUsesDefaults(t *testing.T) {
	os.Setenv("RETRIEVER_CHUNK_SIZE", "not_a_number")
	defer os.Unsetenv("RETRIEVER_CHUNK_SIZE")

	cfg := FromEnv()
	defaultCfg := Default()

	if cfg.ChunkSize != defaultCfg.ChunkSize {
		t.Errorf("invalid env should use default: expected %d, got %d",
			defaultCfg.ChunkSize, cfg.ChunkSize)
	}
}
