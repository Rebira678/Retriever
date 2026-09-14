package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Rebira678/Retriever/internal/embedder"
	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/storage"
	searchv1 "github.com/Rebira678/Retriever/pkg/api/search/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SearchServer implements the gRPC SearchService with production-grade reliability.
type SearchServer struct {
	searchv1.UnimplementedSearchServiceServer
	store    storage.Storage
	embedder embedder.Embedder
	logger   *slog.Logger
}

// SearchServerOption defines functional options for configuring the SearchServer.
type SearchServerOption func(*SearchServer)

// WithLogger injects a custom structured logger.
func WithLogger(logger *slog.Logger) SearchServerOption {
	return func(s *SearchServer) {
		s.logger = logger
	}
}

// NewSearchServer constructs a new SearchServer utilizing functional options.
func NewSearchServer(store storage.Storage, emb embedder.Embedder, opts ...SearchServerOption) *SearchServer {
	s := &SearchServer{
		store:    store,
		embedder: emb,
		logger:   slog.Default(),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Search handles semantic similarity requests, embedding the query and retrieving top-K results.
func (s *SearchServer) Search(ctx context.Context, req *searchv1.SearchRequest) (*searchv1.SearchResponse, error) {
	// 1. Input Validation
	if err := s.validateSearchRequest(req); err != nil {
		s.logger.Warn("invalid search request", "error", err, "query", req.Query)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	topK := int(req.TopK)
	if topK <= 0 {
		topK = 5
	} else if topK > 100 {
		topK = 100 // Impose a hard ceiling on TopK to prevent resource exhaustion
	}

	// 2. Enforce Deadline (Bounded Execution)
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}

	// 3. Query Embedding (External API Call)
	queryChunk := models.Chunk{
		Text:       req.Query,
		Index:      0,
		DocumentID: "query",
	}

	emb, err := s.embedder.EmbedChunk(ctx, queryChunk)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			s.logger.Warn("query embedding timed out or canceled", "error", err)
			return nil, status.Error(codes.DeadlineExceeded, "embedding upstream timed out")
		}
		s.logger.Error("failed to embed search query", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to process query embedding")
	}

	// 4. Vector Similarity Search (Database Call)
	results, err := s.store.SearchSimilar(ctx, emb.Vector, topK)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			s.logger.Warn("database search timed out or canceled", "error", err)
			return nil, status.Error(codes.DeadlineExceeded, "database search timed out")
		}
		s.logger.Error("failed to search database", "error", err)
		return nil, status.Errorf(codes.Internal, "failed to retrieve search results")
	}

	// 5. Response Mapping
	resp := &searchv1.SearchResponse{
		Results: make([]*searchv1.SearchResult, 0, len(results)),
	}

	for _, r := range results {
		resp.Results = append(resp.Results, &searchv1.SearchResult{
			DocumentId: r.DocumentID,
			ChunkIndex: int32(r.ChunkIndex),
			ChunkText:  r.ChunkText,
			Score:      float32(r.Score),
		})
	}

	return resp, nil
}

func (s *SearchServer) validateSearchRequest(req *searchv1.SearchRequest) error {
	if req == nil {
		return errors.New("request cannot be nil")
	}
	if len(req.Query) == 0 {
		return errors.New("query cannot be empty")
	}
	if len(req.Query) > 4096 {
		return errors.New("query exceeds maximum length of 4096 characters")
	}
	return nil
}

// LoggingInterceptor returns a UnaryServerInterceptor that provides structured logging.
func LoggingInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		
		resp, err := handler(ctx, req)
		
		duration := time.Since(start)
		code := status.Code(err)
		
		logArgs := []any{
			"method", info.FullMethod,
			"duration_ms", duration.Milliseconds(),
			"code", code.String(),
		}
		
		if err != nil {
			logArgs = append(logArgs, "error", err.Error())
			if code == codes.Internal {
				logger.Error("gRPC request failed", logArgs...)
			} else {
				logger.Warn("gRPC request returned non-OK", logArgs...)
			}
		} else {
			logger.Info("gRPC request successful", logArgs...)
		}
		
		return resp, err
	}
}

// RecoveryInterceptor returns a UnaryServerInterceptor that recovers from panics.
func RecoveryInterceptor(logger *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
		defer func() {
			if r := recover(); r != nil {
				logger.Error("panic recovered in gRPC handler", "method", info.FullMethod, "panic", r)
				err = status.Errorf(codes.Internal, "internal server error")
			}
		}()
		return handler(ctx, req)
	}
}
