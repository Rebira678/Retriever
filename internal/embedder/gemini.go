package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/Rebira678/Retriever/internal/models"
)

// GeminiEmbedder implements Embedder using the Google Gemini API.
type GeminiEmbedder struct {
	apiKey     string
	apiURL     string
	model      string
	httpClient HTTPClient

	// bufPool reduces heap allocations during JSON marshaling
	bufPool *sync.Pool
}

// GeminiOption defines a functional option for configuring GeminiEmbedder.
type GeminiOption func(*GeminiEmbedder)

// WithGeminiHTTPClient allows injecting a custom HTTPClient.
func WithGeminiHTTPClient(client HTTPClient) GeminiOption {
	return func(e *GeminiEmbedder) {
		e.httpClient = client
	}
}

// NewGeminiEmbedder creates a new GeminiEmbedder for the free Gemini API.
func NewGeminiEmbedder(apiKey, model string, opts ...GeminiOption) *GeminiEmbedder {
	if model == "" {
		model = "gemini-embedding-2"
	}
	// Gemini API format
	apiURL := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:embedContent?key=%s", model, apiKey)

	e := &GeminiEmbedder{
		apiKey: apiKey,
		apiURL: apiURL,
		model:  model,
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
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

// geminiEmbedRequest is the payload sent to the API.
type geminiEmbedRequest struct {
	Content struct {
		Parts [1]struct {
			Text string `json:"text"`
		} `json:"parts"`
	} `json:"content"`
}

// geminiEmbedResponse is the structure expected back from the API.
type geminiEmbedResponse struct {
	Embedding struct {
		Values []float32 `json:"values"`
	} `json:"embedding"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

// EmbedChunk executes the network request to fetch vector embeddings from Gemini.
func (e *GeminiEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	tracer := otel.Tracer("retriever/embedder/gemini")
	spanCtx, span := tracer.Start(ctx, "HTTP POST "+e.apiURL,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(
			semconv.HTTPRequestMethodKey.String("POST"),
			semconv.URLFullKey.String(e.apiURL),
			attribute.String("gen_ai.system", "gemini"),
			attribute.String("gen_ai.request.model", e.model),
			attribute.String("document.id", chunk.DocumentID),
			attribute.Int("chunk.index", chunk.Index),
			attribute.Int("chunk.length", len(chunk.Text)),
		),
	)
	defer span.End()

	buf := e.bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer e.bufPool.Put(buf)

	reqBody := geminiEmbedRequest{}
	reqBody.Content.Parts = [1]struct {
		Text string `json:"text"`
	}{{Text: chunk.Text}}

	if err := json.NewEncoder(buf).Encode(&reqBody); err != nil {
		err = fmt.Errorf("failed to encode gemini request: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return models.Embedding{}, err
	}

	req, err := http.NewRequestWithContext(spanCtx, http.MethodPost, e.apiURL, buf)
	if err != nil {
		err = fmt.Errorf("failed to create request: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return models.Embedding{}, err
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := e.httpClient.Do(req)
	if err != nil {
		err = fmt.Errorf("gemini api request failed: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return models.Embedding{}, err
	}
	defer resp.Body.Close()

	span.SetAttributes(semconv.HTTPResponseStatusCodeKey.Int(resp.StatusCode))

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		err = fmt.Errorf("gemini api error (status %d): %s", resp.StatusCode, string(bodyBytes))
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return models.Embedding{}, err
	}

	var result geminiEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		err = fmt.Errorf("failed to decode gemini response: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return models.Embedding{}, err
	}

	if result.Error != nil {
		err = fmt.Errorf("gemini api returned error: %s", result.Error.Message)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return models.Embedding{}, err
	}

	if len(result.Embedding.Values) == 0 {
		err = fmt.Errorf("no embeddings returned in gemini response")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return models.Embedding{}, err
	}

	span.SetStatus(codes.Ok, "Embedding successful")

	return models.Embedding{
		ChunkIndex: chunk.Index,
		DocumentID: chunk.DocumentID,
		Vector:     result.Embedding.Values,
		Model:      e.model,
		CreatedAt:  time.Now(),
	}, nil
}
