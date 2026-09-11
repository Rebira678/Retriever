package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Rebira678/Retriever/internal/models"
)

// Embedder defines the interface for generating vector embeddings from text.
type Embedder interface {
	// EmbedChunk takes a single Chunk and returns an Embedding.
	EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error)
}

// OpenAIEmbedder implements Embedder using the OpenAI compatible embeddings API.
type OpenAIEmbedder struct {
	apiKey     string
	apiURL     string
	model      string
	httpClient *http.Client
}

// NewOpenAIEmbedder creates a new OpenAIEmbedder.
func NewOpenAIEmbedder(apiKey, apiURL, model string) *OpenAIEmbedder {
	if apiURL == "" {
		apiURL = "https://api.openai.com/v1/embeddings"
	}
	return &OpenAIEmbedder{
		apiKey:     apiKey,
		apiURL:     apiURL,
		model:      model,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

// embedRequest is the payload sent to the API.
type embedRequest struct {
	Input string `json:"input"`
	Model string `json:"model"`
}

// embedResponse is the structure expected back from the API.
type embedResponse struct {
	Data []struct {
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// EmbedChunk implements the Embedder interface.
func (e *OpenAIEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	reqBody := embedRequest{
		Input: chunk.Text,
		Model: e.model,
	}

	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return models.Embedding{}, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", e.apiURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return models.Embedding{}, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	if e.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+e.apiKey)
	}

	resp, err := e.httpClient.Do(req)
	if err != nil {
		return models.Embedding{}, fmt.Errorf("api request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return models.Embedding{}, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return models.Embedding{}, fmt.Errorf("api error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var result embedResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		return models.Embedding{}, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	if result.Error != nil {
		return models.Embedding{}, fmt.Errorf("api returned error: %s", result.Error.Message)
	}

	if len(result.Data) == 0 {
		return models.Embedding{}, fmt.Errorf("no embeddings returned in response")
	}

	return models.Embedding{
		ChunkIndex: chunk.Index,
		DocumentID: chunk.DocumentID,
		Vector:     result.Data[0].Embedding,
		Model:      e.model,
		CreatedAt:  time.Now(),
	}, nil
}
