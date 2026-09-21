package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/Rebira678/Retriever/internal/config"
	"github.com/Rebira678/Retriever/internal/storage"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	dimension     = 1536
	modelName     = "benchmark-model"
	concurrency   = 20
	requestsCount = 500
)

func main() {
	cfg := config.FromEnv()
	dbURL := cfg.DatabaseURL

	ctx := context.Background()
	
	// Create storage instance (which uses pgxpool internally)
	store, err := storage.NewPostgresStorage(ctx, dbURL, dimension)
	if err != nil {
		log.Fatalf("Failed to initialize storage: %v", err)
	}
	defer store.Close()

	// Ensure we start with a clean slate
	pool, _ := pgxpool.New(ctx, dbURL)
	defer pool.Close()
	_, _ = pool.Exec(ctx, "DROP INDEX IF EXISTS chunks_embedding_idx;")

	fmt.Println("==============================================")
	fmt.Printf("🚀 Starting Expert pgvector HNSW Benchmark\n")
	fmt.Println("==============================================")
	fmt.Printf("Vector Dimensions: %d\n", dimension)
	fmt.Printf("Concurrency Level: %d workers\n", concurrency)
	fmt.Printf("Total Requests: %d\n", requestsCount)

	// Pre-generate random vectors to avoid PRNG overhead in the hot path
	queries := make([][]float32, requestsCount)
	for i := range queries {
		queries[i] = generateRandomVector(dimension)
	}

	// 1. Measure Sequential Scan (No Index)
	fmt.Println("\n[1/3] Benchmarking Sequential Scan (No Index)...")
	seqScanStats := runLoadTest(ctx, store, queries, 0) // 0 implies no ef_search setting needed

	// 2. Build Index Optimally
	fmt.Println("\n[2/3] Building HNSW Index (m=24, ef_construction=100)...")
	startIdx := time.Now()
	// Using the expert method we added to storage
	if err := store.OptimizeIndex(ctx, dimension, 24, 100); err != nil {
		log.Fatalf("Failed to optimize index: %v", err)
	}
	fmt.Printf("✅ Index built in %v\n", time.Since(startIdx))

	// 3. Measure HNSW Index Scan
	fmt.Println("\n[3/3] Benchmarking HNSW Index Scan (ef_search=64)...")
	hnswStats := runLoadTest(ctx, store, queries, 64)

	printReport(seqScanStats, hnswStats)
}

type BenchStats struct {
	P50 time.Duration
	P90 time.Duration
	P99 time.Duration
	Avg time.Duration
	Max time.Duration
	RPS float64
}

// runLoadTest simulates concurrent search requests
func runLoadTest(ctx context.Context, store *storage.PostgresStorage, queries [][]float32, efSearch int) BenchStats {
	var wg sync.WaitGroup
	reqChan := make(chan []float32, len(queries))
	durChan := make(chan time.Duration, len(queries))

	for _, q := range queries {
		reqChan <- q
	}
	close(reqChan)

	start := time.Now()

	// Launch worker pool
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for q := range reqChan {
				reqStart := time.Now()
				
				// TopK=10
				_, err := store.SearchSimilar(ctx, q, modelName, 10, efSearch)
				if err != nil {
					log.Printf("Query failed: %v", err)
					continue
				}
				
				durChan <- time.Since(reqStart)
			}
		}()
	}

	wg.Wait()
	close(durChan)
	totalTime := time.Since(start)

	var durations []time.Duration
	var sum time.Duration
	for d := range durChan {
		durations = append(durations, d)
		sum += d
	}

	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })

	l := len(durations)
	if l == 0 {
		return BenchStats{}
	}

	return BenchStats{
		P50: durations[l*50/100],
		P90: durations[l*90/100],
		P99: durations[l*99/100],
		Max: durations[l-1],
		Avg: sum / time.Duration(l),
		RPS: float64(l) / totalTime.Seconds(),
	}
}

func printReport(seq, hnsw BenchStats) {
	fmt.Println("\n==============================================")
	fmt.Println("📊 BENCHMARK REPORT")
	fmt.Println("==============================================")
	fmt.Printf("%-15s | %-15s | %-15s\n", "Metric", "Seq Scan", "HNSW Index")
	fmt.Printf("----------------------------------------------\n")
	fmt.Printf("%-15s | %-15v | %-15v\n", "RPS", fmt.Sprintf("%.2f req/s", seq.RPS), fmt.Sprintf("%.2f req/s", hnsw.RPS))
	fmt.Printf("%-15s | %-15v | %-15v\n", "P50 Latency", seq.P50, hnsw.P50)
	fmt.Printf("%-15s | %-15v | %-15v\n", "P90 Latency", seq.P90, hnsw.P90)
	fmt.Printf("%-15s | %-15v | %-15v\n", "P99 Latency", seq.P99, hnsw.P99)
	fmt.Printf("%-15s | %-15v | %-15v\n", "Max Latency", seq.Max, hnsw.Max)
	
	if hnsw.P99 > 0 {
		speedup := float64(seq.P99) / float64(hnsw.P99)
		fmt.Printf("\n🚀 LinkedIn / X Angle: %.1fx improvement in P99 query latency with HNSW indexing.\n", speedup)
	}
}

func generateRandomVector(dim int) []float32 {
	vec := make([]float32, dim)
	for i := 0; i < dim; i++ {
		val, _ := rand.Int(rand.Reader, big.NewInt(100))
		vec[i] = float32(val.Int64()) / 100.0
	}
	return vec
}
