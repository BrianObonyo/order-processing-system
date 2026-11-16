package event

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const CurrentSchemaVersion = 1

type Envelope struct {
	ID            string          `json:"event_id"`
	Type          string          `json:"event_type"`
	SchemaVersion int             `json:"schema_version"`
	CorrelationID string          `json:"correlation_id"`
	OccurredAt    time.Time       `json:"occurred_at"`
	Data          json.RawMessage `json:"data"`
}

func (e Envelope) Validate(expectedType string) error {
	switch {
	case strings.TrimSpace(e.ID) == "":
		return fmt.Errorf("event_id is required")
	case strings.TrimSpace(e.Type) == "":
		return fmt.Errorf("event_type is required")
	case e.Type != expectedType:
		return fmt.Errorf("event_type %q does not match message type %q", e.Type, expectedType)
	case e.SchemaVersion != CurrentSchemaVersion:
		return fmt.Errorf("unsupported schema_version %d", e.SchemaVersion)
	case strings.TrimSpace(e.CorrelationID) == "":
		return fmt.Errorf("correlation_id is required")
	case e.OccurredAt.IsZero():
		return fmt.Errorf("occurred_at is required")
	case len(e.Data) == 0 || !json.Valid(e.Data):
		return fmt.Errorf("data must be valid JSON")
	default:
		return nil
	}
}
