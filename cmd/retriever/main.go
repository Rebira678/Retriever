// Package main is the entry point for the Retriever RAG Ingestion Pipeline.
//
// Retriever ingests documents, chunks them using configurable strategies,
// generates embeddings via a concurrent worker pool, stores vectors in
// Postgres (pgvector), and exposes a semantic search API.
//
// Architecture: Pipeline-based — Ingestion → Chunking → Embedding → Storage → Search
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Rebira678/Retriever/internal/chunker"
	"github.com/Rebira678/Retriever/internal/config"
	"github.com/Rebira678/Retriever/internal/embedder"
	"github.com/Rebira678/Retriever/internal/models"
	"github.com/Rebira678/Retriever/internal/pipeline"
	"github.com/Rebira678/Retriever/internal/ratelimit"
	"github.com/Rebira678/Retriever/internal/server"
	"github.com/Rebira678/Retriever/internal/storage"
	"github.com/Rebira678/Retriever/internal/telemetry"
	searchv1 "github.com/Rebira678/Retriever/pkg/api/search/v1"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"net"

	_ "github.com/joho/godotenv/autoload"
)

func main() {
	// ─── Structured Logger ───────────────────────────────────────────────
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	logger := slog.New(telemetry.NewOTelSlogHandler(jsonHandler))
	slog.SetDefault(logger)

	slog.Info("🚀 Retriever RAG Ingestion Pipeline starting",
		"version", "0.1.0",
		"day", 36,
	)

	// ─── Load Configuration ─────────────────────────────────────────────
	cfg := config.FromEnv()
	slog.Info("Configuration loaded",
		"chunk_size", cfg.ChunkSize,
		"chunk_overlap", cfg.ChunkOverlap,
		"strategy", cfg.ChunkStrategy,
		"otlp_endpoint", cfg.OTLPEndpoint,
	)

	// ─── Initialize OpenTelemetry Tracing & Metrics ───────────────────────────────
	tp, mp, err := telemetry.InitTelemetry(context.Background(), "retriever", cfg.OTLPEndpoint)
	if err != nil {
		slog.Error("Failed to initialize OpenTelemetry telemetry", "error", err)
	} else {
		defer func() {
			if err := tp.Shutdown(context.Background()); err != nil {
				slog.Error("Failed to shutdown tracer", "error", err)
			}
			if err := mp.Shutdown(context.Background()); err != nil {
				slog.Error("Failed to shutdown meter", "error", err)
			}
		}()
		slog.Info("OpenTelemetry telemetry initialized", "endpoint", cfg.OTLPEndpoint)
	}

	// ─── Knowledge Base: Diverse Document Corpus ───────────────────────────────
	// A rich, varied knowledge base spanning technology, geography, science, history,
	// and programming so the RAG pipeline returns genuinely different results per query.
	distinctDocs := []models.Document{
		// --- AI & RAG ---
		{
			ID: "rag-architecture",
			Content: `Retrieval-Augmented Generation (RAG) is an AI framework that enhances large language model responses by incorporating external knowledge retrieval. Instead of relying solely on trained parameters, RAG systems fetch relevant documents from a vector database and include them as context in the prompt. This architecture solves the "knowledge cutoff" problem inherent in static LLMs and dramatically reduces hallucination by providing verifiable source material. The RAG pipeline typically consists of three phases: 1. Ingestion — documents are loaded, split into chunks, and embedded as vectors. 2. Retrieval — a user query is embedded and compared against stored vectors using similarity search. 3. Generation — the retrieved context is injected into the LLM prompt to produce a grounded response.`,
		},
		{
			ID: "pgvector-hnsw",
			Content: `pgvector is a PostgreSQL extension that adds support for vector similarity search. It introduces data types for storing embeddings and indexing algorithms like HNSW (Hierarchical Navigable Small World) and IVFFlat. HNSW provides faster search speeds and better recall at the cost of higher memory consumption compared to IVFFlat. Vector search calculates distance metrics such as cosine similarity, L2 distance, or inner product.`,
		},
		{
			ID: "hybrid-search-rrf",
			Content: `Reciprocal Rank Fusion (RRF) is an algorithm used in Hybrid Search to combine the results of multiple search strategies, typically dense vector search (semantic) and sparse keyword search (BM25). By calculating a fused score based on the reciprocal of the rank from each individual search result, RRF ensures that documents scoring high in both semantic meaning and exact keyword match bubble to the very top of the retrieved context.`,
		},
		// --- Observability ---
		{
			ID: "opentelemetry-tracing",
			Content: `OpenTelemetry is an open-source observability framework for cloud-native software. It provides standardized APIs and SDKs to instrument, generate, collect, and export telemetry data (metrics, logs, and traces). Distributed tracing allows developers to track the progression of a single user request as it traverses multiple microservices, assigning a unique Trace ID to visualize latency bottlenecks in tools like Grafana Tempo or Jaeger.`,
		},
		{
			ID: "circuit-breaker-pattern",
			Content: `The Circuit Breaker pattern is a resilient software design pattern used to prevent catastrophic cascading failures in distributed systems. When a downstream service experiences high latency or outages, the circuit breaker trips to an "OPEN" state, immediately rejecting new requests to shed load. After a cooldown period, it enters a "HALF-OPEN" state to test if the failing service has recovered before fully closing again.`,
		},
		// --- Geography & Countries ---
		{
			ID: "ethiopia-overview",
			Content: `Ethiopia, officially the Federal Democratic Republic of Ethiopia, is a landlocked country in the Horn of Africa. It is the second-most populous nation in Africa with over 120 million people. Addis Ababa is the capital and largest city, serving as the headquarters of the African Union. Ethiopia is one of the oldest countries in the world, with a history spanning over 3,000 years. It was never colonized by European powers, making it unique among African nations. The country is known for its ancient Aksumite civilization, the rock-hewn churches of Lalibela (a UNESCO World Heritage Site), and as the origin of coffee. Ethiopia has diverse geography ranging from the Simien Mountains to the Danakil Depression, one of the hottest places on Earth.`,
		},
		{
			ID: "ethiopia-culture",
			Content: `Ethiopian culture is one of the richest in Africa. The country has its own unique calendar (13 months), its own ancient script called Ge'ez, and its own time system. Ethiopian cuisine is famous for injera, a sourdough flatbread made from teff flour, served with various stews called wot. The Ethiopian Orthodox Tewahedo Church is one of the oldest Christian denominations in the world, dating back to the 4th century. Ethiopia is also the birthplace of coffee — legend says a goat herder named Kaldi discovered it in the Kaffa region. The traditional coffee ceremony is an important social ritual. Amharic is the official language, but over 80 languages are spoken across the country including Oromo, Tigrinya, and Somali.`,
		},
		// --- Programming Languages ---
		{
			ID: "golang-overview",
			Content: `Go (also known as Golang) is a statically typed, compiled programming language designed by Robert Griesemer, Rob Pike, and Ken Thompson at Google in 2009. Go is known for its simplicity, fast compilation, built-in concurrency via goroutines and channels, and excellent standard library. It compiles to native machine code and produces single static binaries with no external dependencies. Go is widely used for building microservices, CLI tools, cloud infrastructure (Docker, Kubernetes, and Terraform are written in Go), and high-performance backend systems. Key features include garbage collection, structural typing, and CSP-style concurrency primitives.`,
		},
		{
			ID: "python-overview",
			Content: `Python is a high-level, interpreted, general-purpose programming language created by Guido van Rossum and first released in 1991. Python emphasizes code readability with its use of significant indentation. It supports multiple programming paradigms including procedural, object-oriented, and functional programming. Python has become the dominant language for data science, machine learning (with libraries like TensorFlow, PyTorch, and scikit-learn), web development (Django, Flask), and automation scripting. The Python Package Index (PyPI) hosts over 400,000 packages. Python 3 is the current major version, with Python 2 reaching end-of-life in January 2020.`,
		},
		// --- Cloud & Infrastructure ---
		{
			ID: "kubernetes-overview",
			Content: `Kubernetes (K8s) is an open-source container orchestration platform originally designed by Google and now maintained by the Cloud Native Computing Foundation (CNCF). It automates the deployment, scaling, and management of containerized applications. Core concepts include Pods (the smallest deployable unit), Services (network abstraction), Deployments (declarative updates), and Namespaces (virtual clusters). Kubernetes uses a declarative configuration model where users specify desired state in YAML manifests, and the control plane continuously reconciles actual state to match. Key components include the API server, etcd (distributed key-value store), kubelet, and kube-proxy.`,
		},
		{
			ID: "docker-containers",
			Content: `Docker is a platform for developing, shipping, and running applications inside lightweight, portable containers. A container packages an application with all its dependencies, libraries, and configuration files, ensuring consistent behavior across development, staging, and production environments. Docker uses a layered filesystem (Union FS) and Linux kernel features like namespaces and cgroups for isolation. A Dockerfile defines the build instructions, and Docker Compose allows multi-container application orchestration. Docker Hub is the default public registry for container images. Containers differ from virtual machines in that they share the host OS kernel, making them significantly lighter and faster to start.`,
		},
		// --- Science ---
		{
			ID: "solar-system",
			Content: `The Solar System consists of the Sun and the celestial objects gravitationally bound to it. There are eight planets: Mercury, Venus, Earth, Mars, Jupiter, Saturn, Uranus, and Neptune. The four inner planets (Mercury, Venus, Earth, Mars) are terrestrial with solid rocky surfaces. The four outer planets are gas and ice giants. Jupiter is the largest planet, with a mass more than twice that of all other planets combined. Earth is the only known planet to harbor life, with liquid water on its surface and a protective magnetic field. The asteroid belt between Mars and Jupiter contains millions of rocky bodies. Pluto was reclassified as a dwarf planet in 2006 by the International Astronomical Union.`,
		},
		{
			ID: "photosynthesis",
			Content: `Photosynthesis is the biological process by which green plants, algae, and certain bacteria convert light energy (usually from the Sun) into chemical energy stored in glucose. The overall equation is: 6CO2 + 6H2O + light energy → C6H12O6 + 6O2. The process occurs primarily in chloroplasts, using the pigment chlorophyll to absorb light. It consists of two stages: the light-dependent reactions (in thylakoid membranes, producing ATP and NADPH) and the Calvin cycle (in the stroma, fixing carbon dioxide into sugar). Photosynthesis is fundamental to life on Earth as it produces oxygen and forms the base of most food chains.`,
		},
		// --- History ---
		{
			ID: "world-war-2",
			Content: `World War II (1939-1945) was the deadliest and most widespread conflict in human history, involving over 30 countries and resulting in an estimated 70-85 million fatalities. The war began with Nazi Germany's invasion of Poland on September 1, 1939. The major Allied powers included the United States, the Soviet Union, the United Kingdom, and China, while the Axis powers were led by Germany, Italy, and Japan. Key turning points included the Battle of Stalingrad, D-Day (the Normandy landings on June 6, 1944), and the atomic bombings of Hiroshima and Nagasaki. The war ended with Germany's surrender in May 1945 and Japan's surrender in August 1945. It led to the creation of the United Nations and the beginning of the Cold War.`,
		},
		{
			ID: "ancient-egypt",
			Content: `Ancient Egypt was a civilization in northeastern Africa concentrated along the lower Nile River, lasting from approximately 3100 BCE to 30 BCE. It is famous for its monumental architecture including the Great Pyramids of Giza and the Sphinx, built as tombs for pharaohs. The ancient Egyptians developed hieroglyphic writing, advanced mathematics, medicine, and irrigation systems. The Rosetta Stone, discovered in 1799, was key to deciphering hieroglyphs. Egyptian society was highly stratified with the pharaoh at the top, considered a living god. The civilization went through periods known as the Old Kingdom, Middle Kingdom, and New Kingdom, with intermediate periods of political fragmentation.`,
		},
		// --- Mathematics & Computer Science ---
		{
			ID: "machine-learning-basics",
			Content: `Machine Learning (ML) is a subset of artificial intelligence that enables systems to learn and improve from experience without being explicitly programmed. There are three main types: supervised learning (trained on labeled data to predict outcomes, e.g., classification and regression), unsupervised learning (finds hidden patterns in unlabeled data, e.g., clustering and dimensionality reduction), and reinforcement learning (learns optimal actions through trial and error with rewards). Common algorithms include linear regression, decision trees, random forests, support vector machines, and neural networks. Deep learning, a subset of ML using multi-layered neural networks, has achieved breakthroughs in image recognition, natural language processing, and game playing.`,
		},
		{
			ID: "database-systems",
			Content: `A database is an organized collection of structured data stored electronically. Relational databases (RDBMS) like PostgreSQL, MySQL, and Oracle use tables with rows and columns, enforcing data integrity through schemas and ACID transactions (Atomicity, Consistency, Isolation, Durability). SQL (Structured Query Language) is the standard language for querying relational databases. NoSQL databases like MongoDB (document), Redis (key-value), Cassandra (wide-column), and Neo4j (graph) offer flexible schemas and horizontal scalability for specific use cases. NewSQL databases like CockroachDB attempt to combine the scalability of NoSQL with the ACID guarantees of traditional RDBMS.`,
		},
		// --- Health & Biology ---
		{
			ID: "human-immune-system",
			Content: `The human immune system is a complex network of cells, tissues, and organs that defends the body against pathogens including bacteria, viruses, fungi, and parasites. It consists of two main branches: innate immunity (non-specific, immediate response including skin barriers, inflammation, and phagocytes) and adaptive immunity (specific, slower response involving T cells and B cells that create immunological memory). Vaccines work by training the adaptive immune system to recognize specific pathogens without causing disease. White blood cells (leukocytes) are the primary defenders, produced in bone marrow. The lymphatic system, including lymph nodes, spleen, and thymus, plays a crucial role in immune response coordination.`,
		},
		// --- Economics & Business ---
		{
			ID: "blockchain-crypto",
			Content: `Blockchain is a distributed, decentralized, immutable digital ledger that records transactions across a network of computers. Each block contains a cryptographic hash of the previous block, a timestamp, and transaction data, forming a chain. Bitcoin, created by the pseudonymous Satoshi Nakamoto in 2008, was the first cryptocurrency built on blockchain technology. Ethereum introduced smart contracts — self-executing programs stored on the blockchain. Consensus mechanisms include Proof of Work (PoW, used by Bitcoin) and Proof of Stake (PoS, used by Ethereum after "The Merge"). Blockchain applications extend beyond cryptocurrency to supply chain management, digital identity, decentralized finance (DeFi), and non-fungible tokens (NFTs).`,
		},
		// --- Space & Astronomy ---
		{
			ID: "black-holes",
			Content: `A black hole is a region of spacetime where gravity is so intense that nothing, not even light or electromagnetic radiation, can escape once past the event horizon. Black holes are predicted by Einstein's general theory of relativity. They form when massive stars (typically over 25 solar masses) exhaust their nuclear fuel and undergo gravitational collapse in a supernova explosion. There are three main types: stellar black holes (a few to tens of solar masses), intermediate-mass black holes, and supermassive black holes (millions to billions of solar masses, found at the centers of most galaxies). In 2019, the Event Horizon Telescope captured the first-ever image of a black hole's shadow in galaxy M87.`,
		},
	}

	// Create the chunker based on configuration
	c := chunker.New(cfg.ChunkSize, cfg.ChunkOverlap)

	// ─── Demo: Concurrent Embedding with Worker Pool ───────────────────────────────
	var emb embedder.Embedder
	dimension := cfg.EmbeddingDimension

	var provider string
	var modelName string

	if cfg.GeminiAPIKey != "" {
		slog.Info("Using Gemini Embedder")
		emb = embedder.NewGeminiEmbedder(cfg.GeminiAPIKey, "gemini-embedding-2")
		provider = "gemini"
		modelName = "gemini-embedding-2"
	} else if cfg.OpenAIAPIKey != "" {
		slog.Info("Using OpenAI Embedder")
		emb = embedder.NewOpenAIEmbedder(cfg.OpenAIAPIKey, cfg.EmbeddingAPIURL, cfg.EmbeddingModel)
		provider = "openai"
		modelName = cfg.EmbeddingModel
	}

	if emb != nil {
		slog.Info("Wrapping embedder with outbound rate limiter", 
			"capacity", cfg.RateLimitBurstCapacity, 
			"refill_rate", cfg.RateLimitTokensPerSecond,
		)
		bucket := ratelimit.NewTokenBucket(cfg.RateLimitBurstCapacity, cfg.RateLimitTokensPerSecond)
		emb = ratelimit.NewRateLimitedEmbedder(emb, bucket)

		// Initialize Database Connection
		slog.Info("Connecting to PostgreSQL...", "url", cfg.DatabaseURL, "dimension", dimension)
		var store storage.Storage
		store, err := storage.NewPostgresStorage(context.Background(), cfg.DatabaseURL, dimension)
		if err != nil {
			slog.Error("Failed to connect to database", "error", err)
			return
		}
		defer store.Close()

		// ─── Senior Level: Orchestrator Pattern ───────────────────────────
		// Encapsulate the entire complex ingestion logic (Idempotency, 
		// Channels, DLQ) into a single reusable Pipeline struct.
		p := pipeline.NewPipeline(cfg, store, emb, c)

		// Start Background Garbage Collector (Delayed Sweeper)
		go func() {
			slog.Info("Starting background garbage collector (sweeps old chunks after 24h safety window)")
			ticker := time.NewTicker(1 * time.Hour)
			defer ticker.Stop()
			for {
				// Sweep chunks that are safely past the 24 hour rollback window
				if err := store.SweepOldChunks(context.Background(), 24*time.Hour); err != nil {
					slog.Warn("Background sweep encountered an error", "error", err)
				}
				<-ticker.C
			}
		}()

		// Ingest multiple DISTINCT sample documents so the database has varied chunks for Top-K testing
		slog.Info("Ingesting DISTINCT sample documents to populate vector database...")
		for _, doc := range distinctDocs {
			if err := p.RunIngestion(context.Background(), doc, provider, modelName); err != nil {
				slog.Error("Pipeline ingestion failed", "error", err)
			}
		}

		// ─── Demo: Similarity Search ─────────────────────────────────────────────
			query := "What is the second phase of the RAG pipeline?"
			slog.Info("Embedding search query...", "query", query)
			
			queryChunk := models.Chunk{Text: query, Index: 0, DocumentID: "query"}
			queryEmbedding, err := emb.EmbedChunk(context.Background(), queryChunk)
			if err != nil {
				slog.Error("Failed to embed query", "error", err)
			} else {
				slog.Info("Searching for most similar chunks (Hybrid RRF)...")
				results, err := store.SearchHybrid(context.Background(), query, queryEmbedding.Vector, modelName, 2, 0)
				if err != nil {
					slog.Error("Search failed", "error", err)
				} else {
					fmt.Println("\n─── Search Results ───")
					for i, res := range results {
						fmt.Printf("%d. [Score: %.3f] (Doc: %s, Chunk: %d)\n%s\n\n", 
							i+1, res.Score, res.DocumentID, res.ChunkIndex, res.ChunkText)
					}
				}
			}
			
			// ─── Start gRPC Server ──────────────────────────────────────────────
			lis, err := net.Listen("tcp", ":50051")
			if err != nil {
				slog.Error("Failed to listen for gRPC", "error", err)
			} else {
				grpcServer := grpc.NewServer(
					grpc.StatsHandler(otelgrpc.NewServerHandler()),
					grpc.ChainUnaryInterceptor(
						server.LoggingInterceptor(slog.Default()),
						server.RecoveryInterceptor(slog.Default()),
						server.AdaptiveLoadSheddingInterceptor(
							server.NewAdaptiveLimiter(100, 10, 1000, 200*time.Millisecond),
						),
					),
					// EXPERT ARCHITECTURE: Protect the server from HTTP/2 stream exhaustion 
					// and dead connections during intense load testing.
					grpc.MaxConcurrentStreams(1000),
					grpc.KeepaliveParams(keepalive.ServerParameters{
						MaxConnectionIdle: 5 * time.Minute,
						Time:              2 * time.Hour,
						Timeout:           20 * time.Second,
					}),
				)
				// Wrap the embedder with an in-memory Bounded LRU cache and Singleflight
				// to prevent external API rate-limiting and cache stampedes during gRPC load tests (Day 45).
				cachedEmb := embedder.NewCachedEmbedder(emb, 10000, 1*time.Hour)
				searchSrv := server.NewSearchServer(store, cachedEmb, server.WithLogger(slog.Default()))
				searchv1.RegisterSearchServiceServer(grpcServer, searchSrv)

				// EXPERT ARCHITECTURE: Register native gRPC Health Server for Kubernetes Probes
				healthServer := health.NewServer()
				grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
				
				// Dynamic Health Checking Goroutine
				// Ties the Kubernetes Readiness Probe directly to the Postgres connection pool
				go func() {
					slog.Info("Starting dynamic health checker for Kubernetes probes")
					ticker := time.NewTicker(5 * time.Second)
					defer ticker.Stop()
					
					// Initial state
					healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
					
					for {
						<-ticker.C
						ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
						err := store.Ping(ctx)
						cancel()
						
						if err != nil {
							slog.Warn("Health check failed, marking NOT_SERVING", "error", err)
							healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
						} else {
							healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
						}
					}
				}()


				// ─── Start HTTP Server (REST bridge) ──────────────────────────
				mux := http.NewServeMux()
				mux.HandleFunc("/v1/search", func(w http.ResponseWriter, r *http.Request) {
					// Strict CORS Headers for frontend connection
					w.Header().Set("Access-Control-Allow-Origin", "*")
					w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
					w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

					if r.Method == http.MethodOptions {
						w.WriteHeader(http.StatusOK)
						return
					}

					if r.Method != http.MethodPost {
						http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
						return
					}

					// Start an OTel span so we get a real Trace ID
					tracer := otel.Tracer("retriever/http-bridge")
					reqCtx, span := tracer.Start(r.Context(), "HTTP /v1/search", trace.WithSpanKind(trace.SpanKindServer))
					defer span.End()

					totalStart := time.Now()

					var req searchv1.SearchRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}

					searchCtx, cancel := context.WithTimeout(reqCtx, 10*time.Second)
					defer cancel()

					// Measure the gRPC search call (includes embedding + hybrid search)
					searchStart := time.Now()
					resp, err := searchSrv.Search(searchCtx, &req)
					searchDuration := time.Since(searchStart)

					if err != nil {
						http.Error(w, err.Error(), http.StatusInternalServerError)
						return
					}

					totalDuration := time.Since(totalStart)

					// Extract the real W3C Trace ID from the active OTel span
					spanCtx := span.SpanContext()
					traceID := ""
					if spanCtx.HasTraceID() {
						traceID = spanCtx.TraceID().String()
					}

					// ─── Relevance-Aware Answer Construction ─────────────────────
					// Results are already filtered by absolute vector distance (< 0.65) in the database.
					// If we got results, they are semantically relevant.

					var answerText string
					isRelevant := len(resp.Results) > 0

					if isRelevant {
						topChunk := resp.Results[0].ChunkText
						if len(topChunk) > 300 {
							topChunk = topChunk[:300]
						}
						answerText = fmt.Sprintf("Based on the RAG pipeline retrieval, I found %d relevant chunks. The top result is from document '%s': \"%s...\"",
							len(resp.Results),
							resp.Results[0].DocumentId,
							topChunk,
						)
					} else {
						// Low-confidence: the system doesn't have relevant knowledge (distance > 0.65)
						answerText = "I don't have specific information about that topic in my knowledge base. Try asking about topics I know: RAG, Ethiopia, Go, Python, Kubernetes, Docker, machine learning, databases, blockchain, black holes, photosynthesis, World War II, ancient Egypt, or the solar system."
					}

					// REAL telemetry payload — zero hardcoded values
					type TelemetryPayload struct {
						LatencyTotalMs  float64 `json:"latency_total_ms"`
						LatencySearchMs float64 `json:"latency_search_ms"`
						TraceID         string  `json:"trace_id"`
						ResultCount     int     `json:"result_count"`
						Relevant        bool    `json:"relevant"`
						TopScore        float32 `json:"top_score,omitempty"`
					}

					type RESTResponse struct {
						Results   []*searchv1.SearchResult `json:"results"`
						Answer    string                   `json:"answer"`
						Telemetry TelemetryPayload         `json:"telemetry"`
					}

					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(RESTResponse{
						Results: resp.Results,
						Answer:  answerText,
						Telemetry: TelemetryPayload{
							LatencyTotalMs:  float64(totalDuration.Milliseconds()),
							LatencySearchMs: float64(searchDuration.Milliseconds()),
							TraceID:         traceID,
							ResultCount:     len(resp.Results),
							Relevant:        isRelevant,
						},
					})
				})

				httpServer := &http.Server{Addr: ":8080", Handler: mux}

				// Use errgroup for decoupled lifecycle management
				g, gCtx := errgroup.WithContext(context.Background())
				appCtx, stop := signal.NotifyContext(gCtx, syscall.SIGINT, syscall.SIGTERM)
				defer stop()

				g.Go(func() error {
					slog.Info("Starting HTTP bridge server", "port", 8080)
					if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
						return fmt.Errorf("HTTP server failed: %w", err)
					}
					return nil
				})

				g.Go(func() error {
					<-appCtx.Done()
					slog.Info("Shutting down HTTP server gracefully...")
					httpServer.Shutdown(context.Background())
					return nil
				})

				g.Go(func() error {
					slog.Info("Starting gRPC search server", "port", 50051)
					if err := grpcServer.Serve(lis); err != nil {
						return fmt.Errorf("gRPC server failed: %w", err)
					}
					return nil
				})

				g.Go(func() error {
					<-appCtx.Done()
					slog.Info("Shutting down gRPC server gracefully...")
					grpcServer.GracefulStop()
					return nil
				})

				slog.Info("Retriever is ready. Press Ctrl+C to exit.")
				if err := g.Wait(); err != nil {
					slog.Error("Server exited with error", "error", err)
				}
				slog.Info("Retriever shutdown complete.")
				return
			}

	} else {
		slog.Info("Skipping concurrent embedding generation (neither RETRIEVER_GEMINI_API_KEY nor RETRIEVER_OPENAI_API_KEY is set)")
	}

	// ─── Graceful Shutdown ──────────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("Retriever is ready. Press Ctrl+C to exit.")
	<-ctx.Done()
	slog.Info("Shutting down gracefully...")
}
