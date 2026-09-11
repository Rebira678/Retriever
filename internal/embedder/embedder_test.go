package embedder

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Rebira678/Retriever/internal/models"
)

func TestOpenAIEmbedder_EmbedChunk(t *testing.T) {
	// Create a mock HTTP server that simulates the OpenAI embeddings API
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify headers
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("Expected Bearer test-key, got %s", r.Header.Get("Authorization"))
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Expected application/json, got %s", r.Header.Get("Content-Type"))
		}

		// Verify request body
		var req embedRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("Failed to decode request body: %v", err)
		}
		if req.Input != "test chunk text" {
			t.Errorf("Expected input 'test chunk text', got %s", req.Input)
		}
		if req.Model != "mock-model" {
			t.Errorf("Expected model 'mock-model', got %s", req.Model)
		}

		// Send mock response
		resp := embedResponse{
			Data: []struct {
				Embedding []float32 `json:"embedding"`
			}{
				{
					Embedding: []float32{0.1, 0.2, 0.3},
				},
			},
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	}))
	defer mockServer.Close()

	// Initialize the embedder to point to the mock server
	embedder := NewOpenAIEmbedder("test-key", mockServer.URL, "mock-model")

	// Create a mock chunk
	chunk := models.Chunk{
		Index:      1,
		Text:       "test chunk text",
		DocumentID: "doc-123",
	}

	// Call EmbedChunk
	embedding, err := embedder.EmbedChunk(context.Background(), chunk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	// Verify the embedding result
	if embedding.ChunkIndex != 1 {
		t.Errorf("Expected ChunkIndex 1, got %d", embedding.ChunkIndex)
	}
	if embedding.DocumentID != "doc-123" {
		t.Errorf("Expected DocumentID 'doc-123', got %s", embedding.DocumentID)
	}
	if embedding.Model != "mock-model" {
		t.Errorf("Expected Model 'mock-model', got %s", embedding.Model)
	}
	if len(embedding.Vector) != 3 {
		t.Fatalf("Expected vector length 3, got %d", len(embedding.Vector))
	}
	if embedding.Vector[0] != 0.1 || embedding.Vector[1] != 0.2 || embedding.Vector[2] != 0.3 {
		t.Errorf("Expected vector [0.1, 0.2, 0.3], got %v", embedding.Vector)
	}
}

func TestOpenAIEmbedder_APIError(t *testing.T) {
	// Create a mock HTTP server that returns a 500 error
	mockServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error": {"message": "internal server error"}}`))
	}))
	defer mockServer.Close()

	embedder := NewOpenAIEmbedder("test-key", mockServer.URL, "mock-model")
	chunk := models.Chunk{Text: "test chunk"}

	_, err := embedder.EmbedChunk(context.Background(), chunk)
	if err == nil {
		t.Fatal("Expected an error from API, got nil")
	}
}
