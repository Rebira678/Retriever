package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Rebira678/Retriever/internal/circuitbreaker"
	"github.com/Rebira678/Retriever/internal/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

func main() {
	endpoint := os.Getenv("RETRIEVER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:4327"
	}

	tp, mp, err := telemetry.InitTelemetry(context.Background(), "trace-generator", endpoint)
	if err != nil {
		slog.Error("Failed to init telemetry", "err", err)
		os.Exit(1)
	}
	defer func() {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
	}()

	tracer := otel.Tracer("trace-generator")
	cb := circuitbreaker.New(circuitbreaker.WithFailureThreshold(2), circuitbreaker.WithTimeout(2*time.Second))

	slog.Info("Starting trace generator. Sending traces to OTel Collector...")

	// Generate 5 complex simulated requests
	for i := 1; i <= 5; i++ {
		simulateRAGRequest(tracer, cb, i)
	}

	slog.Info("Traces generated. Flushing to OTel Collector...")
	time.Sleep(2 * time.Second) // allow async flush
}

func simulateRAGRequest(tracer trace.Tracer, cb circuitbreaker.CircuitBreaker, id int) {
	ctx, span := tracer.Start(context.Background(), "POST /api/v1/search", trace.WithAttributes(
		attribute.Int("request.id", id),
	))
	defer span.End()

	// 1. Semantic Search
	_, searchSpan := tracer.Start(ctx, "Pipeline.SemanticSearch")
	time.Sleep(50 * time.Millisecond) // Simulating DB lookup
	searchSpan.End()

	// 2. LLM Synthesis (Protected by Circuit Breaker)
	err := cb.Execute(ctx, func(c context.Context) error {
		_, llmSpan := tracer.Start(c, "Upstream.LLMGenerate")
		defer llmSpan.End()
		
		time.Sleep(150 * time.Millisecond) // LLM latency

		// Simulate error on requests 2 and 3 to trip breaker
		if id == 2 || id == 3 {
			err := fmt.Errorf("upstream rate limit exceeded")
			llmSpan.RecordError(err)
			llmSpan.SetStatus(codes.Error, err.Error())
			return err
		}
		
		llmSpan.SetStatus(codes.Ok, "success")
		return nil
	})

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "Completed")
	}
}
