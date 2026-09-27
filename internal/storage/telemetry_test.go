package storage

import (
	"context"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// TestInjectTraceComment verifies the SQL Commenter implementation for tracing.
func TestInjectTraceComment(t *testing.T) {
	// 1. Without Span: Should return unchanged
	t.Run("No Span Context", func(t *testing.T) {
		ctx := context.Background()
		query := "SELECT * FROM chunks"
		result := injectTraceComment(ctx, query)
		if result != query {
			t.Errorf("Expected query to be unchanged, got: %s", result)
		}
	})

	// 2. With Span: Should inject traceparent
	t.Run("With Span Context", func(t *testing.T) {
		// Create a mock tracer
		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
		tracer := tp.Tracer("test")
		ctx, span := tracer.Start(context.Background(), "test-span")
		defer span.End()

		query := "SELECT * FROM chunks"
		result := injectTraceComment(ctx, query)

		// W3C Trace Context format: 00-{trace_id}-{span_id}-{trace_flags}
		spanCtx := span.SpanContext()
		expectedComment := "/* traceparent='00-" + spanCtx.TraceID().String() + "-" + spanCtx.SpanID().String() + "-" + spanCtx.TraceFlags().String() + "' */"

		if !strings.Contains(result, expectedComment) {
			t.Errorf("Expected query to contain %s, got: %s", expectedComment, result)
		}
		if !strings.Contains(result, query) {
			t.Errorf("Expected query to contain original query %s, got: %s", query, result)
		}
	})

	// 3. Senior Edge Cases (Multi-line, existing comments)
	t.Run("Senior Edge Cases", func(t *testing.T) {
		exporter := tracetest.NewInMemoryExporter()
		tp := trace.NewTracerProvider(trace.WithSyncer(exporter))
		tracer := tp.Tracer("test-edge")
		ctx, span := tracer.Start(context.Background(), "test-span-edge")
		defer span.End()

		testCases := []struct {
			name  string
			query string
		}{
			{"Empty Query", ""},
			{"Multi-line Query", "\nSELECT *\nFROM chunks\n"},
			{"Existing Comment", "/* foo */ SELECT * FROM chunks"},
			{"Malicious Traceparent Injection Attempt", "SELECT * FROM chunks WHERE id = '/* traceparent='"},
		}

		spanCtx := span.SpanContext()
		expectedPrefix := "/* traceparent='00-" + spanCtx.TraceID().String() + "-" + spanCtx.SpanID().String() + "-" + spanCtx.TraceFlags().String() + "' */ "

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result := injectTraceComment(ctx, tc.query)
				if !strings.HasPrefix(result, expectedPrefix) {
					t.Errorf("Expected result to start with W3C comment. Got: %s", result)
				}
				if !strings.HasSuffix(result, tc.query) {
					t.Errorf("Expected result to end with the exact original query. Got: %s", result)
				}
			})
		}
	})
}
