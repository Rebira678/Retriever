package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/Rebira678/Retriever/internal/models"
)

// mockHTTPClient implements HTTPClient for testing without starting a real server
type mockHTTPClient struct {
	doFunc func(req *http.Request) (*http.Response, error)
}

func (m *mockHTTPClient) Do(req *http.Request) (*http.Response, error) {
	return m.doFunc(req)
}

func TestOpenAIEmbedder_EmbedChunk(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			// Verify headers
			if req.Header.Get("Authorization") != "Bearer test-key" {
				t.Errorf("Expected Bearer test-key, got %s", req.Header.Get("Authorization"))
			}

			// Verify request body
			var payload embedRequest
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				t.Fatalf("Failed to decode request body: %v", err)
			}
			if payload.Input != "test chunk text" {
				t.Errorf("Expected input 'test chunk text', got %s", payload.Input)
			}

			// Mock response body
			respBody := `{"data":[{"embedding":[0.1, 0.2, 0.3]}]}`
			
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(respBody)),
			}, nil
		},
	}

	// Initialize using functional options
	embedder := NewOpenAIEmbedder("test-key", "https://api.mock.com", "mock-model", WithHTTPClient(client))

	chunk := models.Chunk{
		Index:      1,
		Text:       "test chunk text",
		DocumentID: "doc-123",
	}

	embedding, err := embedder.EmbedChunk(context.Background(), chunk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if embedding.ChunkIndex != 1 {
		t.Errorf("Expected ChunkIndex 1, got %d", embedding.ChunkIndex)
	}
	if len(embedding.Vector) != 3 {
		t.Fatalf("Expected vector length 3, got %d", len(embedding.Vector))
	}
	if embedding.Vector[0] != 0.1 {
		t.Errorf("Expected vector[0] to be 0.1, got %f", embedding.Vector[0])
	}
}
