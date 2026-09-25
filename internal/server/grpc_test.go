package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/server"
	searchv1 "github.com/Rebira678/Retriever/pkg/api/search/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/codes"
)

// MockStore for the Search Server
type MockStore struct{}
func (m *MockStore) SearchSimilar(ctx context.Context, queryEmbedding []float32, modelName string, topK int, efSearch int) ([]models.SearchResult, error) {
	// Simulate DB latency to ensure concurrent requests actually queue up and trigger load shedding
	time.Sleep(50 * time.Millisecond)
	return []models.SearchResult{{DocumentID: "mock-doc", ChunkText: "mock content", Score: 0.99}}, nil
}
func (m *MockStore) SearchHybrid(ctx context.Context, queryText string, queryEmbedding []float32, modelName string, topK int, efSearch int) ([]models.SearchResult, error) {
	return nil, nil
}
func (m *MockStore) SaveEmbeddings(ctx context.Context, embeddings []models.Embedding) error { return nil }
func (m *MockStore) SweepOldChunks(ctx context.Context, safeWindow time.Duration) error { return nil }
func (m *MockStore) StartIngestion(ctx context.Context, hash, documentID string) (bool, error) { return true, nil }
func (m *MockStore) CompleteIngestion(ctx context.Context, hash string, status models.IngestionStatus) error { return nil }
func (m *MockStore) SaveDeadLetters(ctx context.Context, dlqs []models.DeadLetter) error { return nil }
func (m *MockStore) Close() error { return nil }
func (m *MockStore) Ping(ctx context.Context) error { return nil }

// MockEmbedder for the Search Server
type MockEmbedder struct{}
func (m *MockEmbedder) EmbedChunk(ctx context.Context, chunk models.Chunk) (models.Embedding, error) {
	return models.Embedding{Vector: []float32{1.0, 0.0}}, nil
}

func setupTestServer(t *testing.T) (string, *grpc.Server, *health.Server, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0") // Random port
	if err != nil {
		t.Fatalf("Failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			// We inject a very aggressive rate limiter to test load shedding explicitly
			server.AdaptiveLoadSheddingInterceptor(
				server.NewAdaptiveLimiter(1, 1, 10, 10*time.Millisecond),
			),
		),
	)

	store := &MockStore{}
	emb := &MockEmbedder{}
	searchSrv := server.NewSearchServer(store, emb)
	searchv1.RegisterSearchServiceServer(grpcServer, searchSrv)

	// Attach native gRPC Health Server
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	cleanup := func() {
		grpcServer.Stop()
		lis.Close()
	}

	return lis.Addr().String(), grpcServer, healthServer, cleanup
}

// Senior Test: End-to-End gRPC Health Probe
// Validates that the health checks correctly report state over the wire to a client,
// mimicking exactly what the Kubernetes kubelet does.
func TestGRPC_HealthCheck_OverTheWire(t *testing.T) {
	addr, _, healthServer, cleanup := setupTestServer(t)
	defer cleanup()

	// Initial State: NOT_SERVING
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

	// Dial the server
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial test server: %v", err)
	}
	defer conn.Close()

	client := grpc_health_v1.NewHealthClient(conn)
	
	// Test NOT_SERVING
	req := &grpc_health_v1.HealthCheckRequest{Service: ""}
	resp, err := client.Check(context.Background(), req)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("Expected NOT_SERVING, got %v", resp.Status)
	}

	// Change State to SERVING (simulating DB reconnection)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	resp, err = client.Check(context.Background(), req)
	if err != nil {
		t.Fatalf("Health check failed: %v", err)
	}
	if resp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("Expected SERVING, got %v", resp.Status)
	}
}

// Senior Test: End-to-End Interceptor Rate Limiting (Load Shedding)
// Validates that the interceptor pipeline correctly sheds load and returns ResourceExhausted
func TestGRPC_LoadShedding_Interceptor(t *testing.T) {
	addr, _, _, cleanup := setupTestServer(t)
	defer cleanup()

	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to dial test server: %v", err)
	}
	defer conn.Close()

	client := searchv1.NewSearchServiceClient(conn)
	req := &searchv1.SearchRequest{Query: "test", TopK: 1}

	// The server is configured with 1 token capacity.
	// Sending multiple concurrent requests should trigger ResourceExhausted on at least one.
	concurrency := 10
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		go func() {
			_, err := client.Search(context.Background(), req)
			errCh <- err
		}()
	}

	shedCount := 0
	for i := 0; i < concurrency; i++ {
		err := <-errCh
		if err != nil {
			st, ok := status.FromError(err)
			if ok && st.Code() == codes.ResourceExhausted {
				shedCount++
			}
		}
	}

	if shedCount == 0 {
		t.Fatalf("Expected load shedding (ResourceExhausted) under extreme concurrency, but none occurred. Interceptors failed to trap.")
	}
}
