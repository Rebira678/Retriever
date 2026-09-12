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

func TestGeminiEmbedder_EmbedChunk(t *testing.T) {
	client := &mockHTTPClient{
		doFunc: func(req *http.Request) (*http.Response, error) {
			var payload geminiEmbedRequest
			if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
				t.Fatalf("Failed to decode request body: %v", err)
			}
			if len(payload.Content.Parts) == 0 || payload.Content.Parts[0].Text != "test chunk text" {
				t.Errorf("Expected input 'test chunk text', got %v", payload.Content.Parts)
			}

			respBody := `{"embedding":{"values":[0.1, 0.2, 0.3]}}`
			
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewBufferString(respBody)),
			}, nil
		},
	}

	embedder := NewGeminiEmbedder("test-key", "text-embedding-004", WithGeminiHTTPClient(client))

	chunk := models.Chunk{
		Index:      1,
		Text:       "test chunk text",
		DocumentID: "doc-123",
	}

	embedding, err := embedder.EmbedChunk(context.Background(), chunk)
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if len(embedding.Vector) != 3 {
		t.Fatalf("Expected vector length 3, got %d", len(embedding.Vector))
	}
	if embedding.Vector[0] != 0.1 {
		t.Errorf("Expected vector[0] to be 0.1, got %f", embedding.Vector[0])
	}
}
