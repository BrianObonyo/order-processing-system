package repositories

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type OutboxEvent struct {
	ID          int64
	EventID     string
	AggregateID string
	EventType   string
	Payload     []byte
	CreatedAt   time.Time
}

func (q *Queries) InsertOutbox(ctx context.Context, aggregateID, eventType string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tag, err := q.db.Exec(ctx, `
		INSERT INTO outbox (tenant_id, aggregate_id, event_type, payload)
		SELECT tenant_id, id, $2, $3 FROM orders WHERE id = $1`,
		aggregateID, eventType, b)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("insert outbox event: order %s not found", aggregateID)
	}
	return nil
}

// FetchUnpublished locks a batch of pending events. SKIP LOCKED lets several
// relay instances run without blocking or double-claiming the same rows.
func (q *Queries) FetchUnpublished(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := q.db.Query(ctx, `
		SELECT id, event_id::text, aggregate_id::text, event_type, payload, created_at
		FROM outbox WHERE published_at IS NULL
		ORDER BY id LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []OutboxEvent
	for rows.Next() {
		var e OutboxEvent
		if err := rows.Scan(&e.ID, &e.EventID, &e.AggregateID, &e.EventType, &e.Payload, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (q *Queries) MarkPublished(ctx context.Context, ids []int64) error {
	_, err := q.db.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids)
	return err
}
