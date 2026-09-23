# Makefile for Retriever RAG Ingestion Pipeline
# Usage: make [target]

.PHONY: run test test-verbose bench lint clean help

# ─── Default ─────────────────────────────────────────────────────────────────
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ─── Development ─────────────────────────────────────────────────────────────
run: ## Run the Retriever pipeline
	go run ./cmd/retriever

test: ## Run all tests
	go test ./... -count=1

test-verbose: ## Run all tests with verbose output
	go test ./... -v -count=1

bench: ## Run benchmarks
	go test ./internal/chunker/... -bench=. -benchmem -count=3

bench-latency: ## Run pgvector latency benchmarks
	go run ./cmd/benchmark_latency/main.go

load-test: ## Run k6 load test against search gRPC endpoint
	docker run --rm -i --net=host -v "$$PWD:/scripts" -w /scripts grafana/k6 run k6-search-loadtest.js

lint: ## Run go vet
	go vet ./...

# ─── Build ───────────────────────────────────────────────────────────────────
build: ## Build the binary
	go build -o bin/retriever ./cmd/retriever

# ─── Cleanup ─────────────────────────────────────────────────────────────────
clean: ## Remove build artifacts
	rm -rf bin/
