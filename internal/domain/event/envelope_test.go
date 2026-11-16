package event

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestEnvelopeValidate(t *testing.T) {
	envelope := Envelope{
		ID: "event-1", Type: "order.created", SchemaVersion: CurrentSchemaVersion,
		CorrelationID: "order-1", OccurredAt: time.Now().UTC(), Data: json.RawMessage(`{"order_id":"order-1"}`),
	}
	if err := envelope.Validate("order.created"); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}

	tests := []struct {
		name     string
		mutate   func(*Envelope)
		expected string
	}{
		{name: "event type mismatch", mutate: func(e *Envelope) { e.Type = "order.paid" }, expected: "does not match"},
		{name: "unsupported schema version", mutate: func(e *Envelope) { e.SchemaVersion++ }, expected: "unsupported schema_version"},
		{name: "missing correlation ID", mutate: func(e *Envelope) { e.CorrelationID = "" }, expected: "correlation_id is required"},
		{name: "invalid data", mutate: func(e *Envelope) { e.Data = json.RawMessage("{") }, expected: "data must be valid JSON"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := envelope
			tt.mutate(&got)
			if err := got.Validate("order.created"); err == nil || !strings.Contains(err.Error(), tt.expected) {
				t.Fatalf("Validate() error = %v, want text %q", err, tt.expected)
			}
		})
	}
}
