package server

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Rebira678/Retriever/internal/circuitbreaker"
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
	dbCb     circuitbreaker.CircuitBreaker
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
		dbCb: circuitbreaker.New(
			circuitbreaker.WithFailureThreshold(5),
			circuitbreaker.WithTimeout(10*time.Second),
		),
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

	// 4. Vector Similarity Search (Database Call) with Circuit Breaker
	var results []models.SearchResult
	err = s.dbCb.Execute(ctx, func(innerCtx context.Context) error {
		var dbErr error
		results, dbErr = s.store.SearchSimilar(innerCtx, emb.Vector, emb.Model, topK, 0)
		return dbErr
	})

	if err != nil {
		if errors.Is(err, circuitbreaker.ErrCircuitOpen) {
			s.logger.Warn("database circuit breaker is open, shedding load")
			return nil, status.Error(codes.Unavailable, "service temporarily overloaded")
		}
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

// AdaptiveLimiter implements an AIMD (Additive Increase, Multiplicative Decrease) 
// concurrency limiter to maximize throughput while preventing latency degradation.
type AdaptiveLimiter struct {
	mu        sync.Mutex
	limit     float64
	inFlight  atomic.Int64
	
	minLimit  float64
	maxLimit  float64
	
	targetLat time.Duration
	rtt       time.Duration // Exponential Moving Average of RTT
	alpha     float64       // EMA smoothing factor
}

// NewAdaptiveLimiter constructs a concurrency limiter that dynamically adjusts its capacity based on latency.
func NewAdaptiveLimiter(initialLimit, min, max int, targetLatency time.Duration) *AdaptiveLimiter {
	return &AdaptiveLimiter{
		limit:     float64(initialLimit),
		minLimit:  float64(min),
		maxLimit:  float64(max),
		targetLat: targetLatency,
		rtt:       targetLatency,
		alpha:     0.1, // EMA weight
	}
}

// Acquire attempts to claim a concurrency slot.
func (l *AdaptiveLimiter) Acquire() bool {
	curr := l.inFlight.Load()
	
	l.mu.Lock()
	limit := int64(math.Floor(l.limit))
	l.mu.Unlock()
	
	if curr >= limit {
		return false
	}
	
	l.inFlight.Add(1)
	return true
}

// Release frees the slot and triggers the AIMD algorithm based on the observed latency and success state.
func (l *AdaptiveLimiter) Release(d time.Duration, success bool) {
	l.inFlight.Add(-1)
	
	l.mu.Lock()
	defer l.mu.Unlock()
	
	// Update EMA of RTT
	l.rtt = time.Duration(float64(l.rtt)*(1-l.alpha) + float64(d)*l.alpha)
	
	if !success || l.rtt > l.targetLat {
		// Multiplicative Decrease (Backoff when latency degrades or infra fails)
		l.limit *= 0.9 
		if l.limit < l.minLimit {
			l.limit = l.minLimit
		}
	} else {
		// Additive Increase (Probe for more capacity when healthy)
		l.limit += 0.1 
		if l.limit > l.maxLimit {
			l.limit = l.maxLimit
		}
	}
}

// AdaptiveLoadSheddingInterceptor returns a UnaryServerInterceptor that rejects requests
// dynamically based on real-time server latency using the AIMD algorithm.
func AdaptiveLoadSheddingInterceptor(limiter *AdaptiveLimiter) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !limiter.Acquire() {
			return nil, status.Errorf(codes.ResourceExhausted, "service temporarily overloaded (adaptive shed)")
		}
		
		start := time.Now()
		resp, err := handler(ctx, req)
		
		// Determine if this failure was due to infrastructure/overload (e.g., Timeout)
		success := err == nil
		if err != nil {
			code := status.Code(err)
			// Business logic errors (InvalidArgument) don't trigger backoff, 
			// only infrastructure pressure (DeadlineExceeded, Unavailable, Internal)
			if code == codes.DeadlineExceeded || code == codes.Unavailable || code == codes.Internal {
				success = false
			} else {
				success = true 
			}
		}
		
		limiter.Release(time.Since(start), success)
		
		return resp, err
	}
}
