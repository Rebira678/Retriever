package pipeline

import (
	"context"
	"testing"

	"github.com/Rebira678/Retriever/internal/models"
	"go.opentelemetry.io/otel/baggage"
)

// TestBaggagePropagation_EdgeCases ensures that tenant IDs and batch configurations
// injected into context Baggage correctly persist across worker pools and pipeline boundaries.
func TestBaggagePropagation_EdgeCases(t *testing.T) {
	t.Run("Valid Baggage Injection and Extraction", func(t *testing.T) {
		ctx := context.Background()
		docID := "doc-999-tenant-X"

		// Simulate Pipeline.RunIngestion baggage injection
		member, err := baggage.NewMember("document_id", docID)
		if err != nil {
			t.Fatalf("Failed to create baggage member: %v", err)
		}
		
		b, err := baggage.New(member)
		if err != nil {
			t.Fatalf("Failed to create baggage: %v", err)
		}
		
		baggageCtx := baggage.ContextWithBaggage(ctx, b)

		// Simulate passing to a ChunkTask
		task := ChunkTask{
			Ctx:   baggageCtx,
			Chunk: models.Chunk{DocumentID: docID, Text: "Test chunk"},
		}

		// Extract Baggage on the worker side
		extractedBaggage := baggage.FromContext(task.Ctx)
		extractedDocID := extractedBaggage.Member("document_id").Value()

		if extractedDocID != docID {
			t.Errorf("Expected document_id '%s' from baggage, got '%s'", docID, extractedDocID)
		}
	})

	t.Run("Empty Baggage Tolerance", func(t *testing.T) {
		ctx := context.Background()
		// No baggage injected
		
		task := ChunkTask{
			Ctx:   ctx,
			Chunk: models.Chunk{DocumentID: "doc-empty", Text: "Test chunk"},
		}

		extractedBaggage := baggage.FromContext(task.Ctx)
		if extractedBaggage.Len() != 0 {
			t.Errorf("Expected empty baggage, got length %d", extractedBaggage.Len())
		}
	})

	t.Run("Multi-Value Baggage (Batch Ingestion)", func(t *testing.T) {
		ctx := context.Background()

		m1, _ := baggage.NewMember("tenant_id", "t-100")
		m2, _ := baggage.NewMember("batch_size", "100")
		
		b, _ := baggage.New(m1, m2)
		baggageCtx := baggage.ContextWithBaggage(ctx, b)

		extracted := baggage.FromContext(baggageCtx)
		if extracted.Member("tenant_id").Value() != "t-100" {
			t.Errorf("Expected tenant_id t-100, got %s", extracted.Member("tenant_id").Value())
		}
		if extracted.Member("batch_size").Value() != "100" {
			t.Errorf("Expected batch_size 100, got %s", extracted.Member("batch_size").Value())
		}
	})
}
