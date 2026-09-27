package telemetry

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/trace"
)

// OTelSlogHandler wraps an existing slog.Handler to automatically extract
// OpenTelemetry TraceID and SpanID from the context and append them to log records.
// This enables flawless log-to-trace correlation in Grafana/Loki.
type OTelSlogHandler struct {
	slog.Handler
}

// NewOTelSlogHandler creates a new handler wrapping the provided one.
func NewOTelSlogHandler(h slog.Handler) *OTelSlogHandler {
	return &OTelSlogHandler{Handler: h}
}

// Handle implements slog.Handler. It intercepts the log record, pulls the active
// span context from the context, and injects trace_id and span_id into the log.
func (h *OTelSlogHandler) Handle(ctx context.Context, r slog.Record) error {
	spanCtx := trace.SpanContextFromContext(ctx)
	if spanCtx.IsValid() {
		r.AddAttrs(
			slog.String("trace_id", spanCtx.TraceID().String()),
			slog.String("span_id", spanCtx.SpanID().String()),
			slog.Bool("trace_sampled", spanCtx.IsSampled()),
		)
	}
	return h.Handler.Handle(ctx, r)
}

// WithAttrs implements slog.Handler.
func (h *OTelSlogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return NewOTelSlogHandler(h.Handler.WithAttrs(attrs))
}

// WithGroup implements slog.Handler.
func (h *OTelSlogHandler) WithGroup(name string) slog.Handler {
	return NewOTelSlogHandler(h.Handler.WithGroup(name))
}
