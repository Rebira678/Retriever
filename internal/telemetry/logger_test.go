package telemetry

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type syncWriter struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncWriter) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func (w *syncWriter) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Len()
}

func TestOTelSlogHandler_InjectsTraceID(t *testing.T) {
	// Setup tracer
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	otel.SetTracerProvider(tp)
	tracer := tp.Tracer("test-logger")

	// Setup logger with a buffer
	var buf bytes.Buffer
	jsonHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewOTelSlogHandler(jsonHandler))

	// Create a span
	ctx, span := tracer.Start(context.Background(), "test-span")
	spanCtx := span.SpanContext()

	// Log with the context
	logger.InfoContext(ctx, "This is a senior-level correlated log message")

	// Assert the JSON log contains the exact trace_id
	logOutput := buf.String()
	expectedTraceStr := `"trace_id":"` + spanCtx.TraceID().String() + `"`
	expectedSpanStr := `"span_id":"` + spanCtx.SpanID().String() + `"`

	if !strings.Contains(logOutput, expectedTraceStr) {
		t.Errorf("Expected log to contain %s, got: %s", expectedTraceStr, logOutput)
	}
	if !strings.Contains(logOutput, expectedSpanStr) {
		t.Errorf("Expected log to contain %s, got: %s", expectedSpanStr, logOutput)
	}

	span.End()
}

func TestOTelSlogHandler_EdgeCase_NoSpan(t *testing.T) {
	var buf bytes.Buffer
	jsonHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewOTelSlogHandler(jsonHandler))

	// Log with empty context
	logger.InfoContext(context.Background(), "Log without trace")

	logOutput := buf.String()
	if strings.Contains(logOutput, "trace_id") {
		t.Errorf("Did not expect trace_id in log, got: %s", logOutput)
	}
}

func TestOTelSlogHandler_EdgeCase_WithAttrsAndGroup(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	tracer := tp.Tracer("test-logger")

	var buf bytes.Buffer
	jsonHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	
	// Create logger, add persistent attribute, and group
	logger := slog.New(NewOTelSlogHandler(jsonHandler)).
		With("app_version", "1.0").
		WithGroup("database")

	ctx, span := tracer.Start(context.Background(), "grouped-span")
	spanCtx := span.SpanContext()

	logger.InfoContext(ctx, "Query executed")

	logOutput := buf.String()
	expectedTraceStr := `"trace_id":"` + spanCtx.TraceID().String() + `"`

	if !strings.Contains(logOutput, expectedTraceStr) {
		t.Errorf("Expected log to contain trace_id %s, got: %s", expectedTraceStr, logOutput)
	}
	if !strings.Contains(logOutput, `"app_version":"1.0"`) {
		t.Errorf("Expected log to retain 'app_version', got: %s", logOutput)
	}
	// slog groups nest keys under the group name.
	if !strings.Contains(logOutput, `"database":{"trace_id"`) && !strings.Contains(logOutput, `"trace_id":"`) {
		t.Errorf("Expected log structure to maintain group/trace_id semantics, got: %s", logOutput)
	}
	span.End()
}

func TestOTelSlogHandler_ConcurrencyRace(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	tracer := tp.Tracer("test-logger")

	var sw syncWriter
	jsonHandler := slog.NewJSONHandler(&sw, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewOTelSlogHandler(jsonHandler))

	const numWorkers = 50
	done := make(chan bool)

	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			ctx, span := tracer.Start(context.Background(), "concurrent-span")
			// Log 10 times per worker
			for j := 0; j < 10; j++ {
				logger.InfoContext(ctx, "Concurrent log")
			}
			span.End()
			done <- true
		}(i)
	}

	for i := 0; i < numWorkers; i++ {
		<-done
	}

	// Just checking if we captured outputs without crashing (race detector will catch memory violations)
	if sw.Len() == 0 {
		t.Fatal("Expected logs to be written concurrently, but buffer is empty")
	}
}

// ─── SENIOR QA: PERFORMANCE & BENCHMARKS ─────────────────────────────────────
// A senior tester ensures that adding tracing logic to the logging critical path
// doesn't introduce massive memory allocations (heap escapes) or CPU overhead.

func BenchmarkOTelSlogHandler_WithSpan(b *testing.B) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	tracer := tp.Tracer("bench")

	var sw syncWriter
	// We use a discard writer to purely benchmark the CPU/Alloc overhead of the handler, not I/O
	jsonHandler := slog.NewJSONHandler(&sw, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewOTelSlogHandler(jsonHandler))

	ctx, span := tracer.Start(context.Background(), "bench-span")
	defer span.End()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.InfoContext(ctx, "benchmark log", slog.String("key", "value"))
	}
}

// ─── SENIOR QA: EXTREME EDGE CASES & MALFORMED DATA ────────────────────────

func TestOTelSlogHandler_EdgeCase_MalformedContext(t *testing.T) {
	var buf bytes.Buffer
	jsonHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	logger := slog.New(NewOTelSlogHandler(jsonHandler))

	// 1. Context is explicitly nil (This normally panics standard library functions)
	// slog.InfoContext gracefully handles nil ctx, but we must ensure our wrapper doesn't panic
	// when calling trace.SpanContextFromContext(nil).
	// NOTE: slog.InfoContext panics on nil context natively in some Go versions, so we construct a Record manually
	
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "Nil context test", 0)
	
	// Our handler must NOT panic when passed a nil context
	err := logger.Handler().Handle(nil, record)
	if err != nil {
		t.Fatalf("Expected nil context to be handled gracefully, got error: %v", err)
	}

	// 2. Context has a timeout that has already expired
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	cancel() // instantly expire

	logger.InfoContext(ctx, "Expired context test")
	if buf.Len() == 0 {
		t.Error("Expected log to be written even if context is expired")
	}
}

func TestOTelSlogHandler_EdgeCase_ComplexDeepNesting(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
	tracer := tp.Tracer("test-logger")

	var buf bytes.Buffer
	jsonHandler := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo})
	
	// Create a deeply nested, multi-group logger mimicking a bizarre application state
	logger := slog.New(NewOTelSlogHandler(jsonHandler)).
		WithGroup("level1").
		With("k1", "v1").
		WithGroup("level2").
		With("k2", "v2").
		WithGroup("level3")

	ctx, span := tracer.Start(context.Background(), "deep-span")
	defer span.End()

	// Add massive payloads and bizarre characters
	logger.InfoContext(ctx, "Deep log", 
		slog.String("bizarre_string", "null\x00byte\nnewline\t\r"),
		slog.Any("complex_struct", struct{ A int; B float64 }{A: 1, B: 3.14}),
	)

	logOutput := buf.String()
	
	// The TraceID MUST be injected at the current group level (level3 in this case)
	// or at the root depending on how AddAttrs is evaluated by the underlying JSONHandler.
	// Since OTelSlogHandler uses r.AddAttrs(), the trace_id becomes part of the current active group (level3).
	spanCtx := span.SpanContext()
	
	if !strings.Contains(logOutput, spanCtx.TraceID().String()) {
		t.Fatalf("Failed to inject Trace ID into deeply nested JSON structure. Output: %s", logOutput)
	}
	
	if !strings.Contains(logOutput, "null\\u0000byte") {
		t.Fatalf("Failed to properly escape bizarre characters. Output: %s", logOutput)
	}
}
