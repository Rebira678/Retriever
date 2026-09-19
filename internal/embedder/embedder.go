package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Rebira678/Retriever/internal/models"
)

// HTTPClient defines the interface required for making HTTP requests.
// This allows for zero-network mocking in tests and custom client injection.
type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

// Embedder defines the interface for generating vector embeddings from text.
type Embedder interface {
	EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error)
}

// OpenAIEmbedder implements Embedder using the OpenAI compatible embeddings API.
type OpenAIEmbedder struct {
	apiKey     string
	apiURL     string
	model      string
	httpClient HTTPClient

	// bufPool reduces heap allocations during JSON marshaling,
	// which is critical for high-throughput pipeline stages.
	bufPool *sync.Pool
}

// Option defines a functional option for configuring OpenAIEmbedder.
type Option func(*OpenAIEmbedder)

// WithHTTPClient allows injecting a custom HTTPClient.
func WithHTTPClient(client HTTPClient) Option {
	return func(e *OpenAIEmbedder) {
		e.httpClient = client
	}
}

// NewOpenAIEmbedder creates a new OpenAIEmbedder utilizing functional options
// for extensibility and a tuned http.Transport for connection reuse.
func NewOpenAIEmbedder(apiKey, apiURL, model string, opts ...Option) *OpenAIEmbedder {
	if apiURL == "" {
		apiURL = "https://api.openai.com/v1/embeddings"
	}

	e := &OpenAIEmbedder{
		apiKey: apiKey,
		apiURL: apiURL,
		model:  model,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100, // Crucial for concurrent API calls
				IdleConnTimeout:     90 * time.Second,
			},
		},
		bufPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
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

// EmbedChunk executes the network request to fetch vector embeddings.
func (e *OpenAIEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	// Borrow a buffer from the pool to avoid allocating a new byte slice per chunk.
	buf := e.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer e.bufPool.Put(buf)

	reqBody := embedRequest{
		Input: chunk.Text,
		Model: e.model,
	}

	if err := json.NewEncoder(buf).Encode(&reqBody); err != nil {
		return models.Embedding{}, fmt.Errorf("failed to encode request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.apiURL, buf)
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

	if resp.StatusCode != http.StatusOK {
		return models.Embedding{}, fmt.Errorf("api error (status %d)", resp.StatusCode)
	}

	var result embedResponse
	// Stream the decode directly from the body to avoid reading the whole payload into memory
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return models.Embedding{}, fmt.Errorf("failed to decode response: %w", err)
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
