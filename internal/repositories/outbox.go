package repositories

import (
	"context"
	"encoding/json"
)

type OutboxEvent struct {
	ID          int64
	AggregateID string
	EventType   string
	Payload     []byte
}

func (q *Queries) InsertOutbox(ctx context.Context, aggregateID, eventType string, payload any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	_, err = q.db.Exec(ctx,
		`INSERT INTO outbox (aggregate_id, event_type, payload) VALUES ($1, $2, $3)`,
		aggregateID, eventType, b)
	return err
}

// FetchUnpublished locks a batch of pending events. SKIP LOCKED lets several
// relay instances run without blocking or double-claiming the same rows.
func (q *Queries) FetchUnpublished(ctx context.Context, limit int) ([]OutboxEvent, error) {
	rows, err := q.db.Query(ctx, `
		SELECT id, aggregate_id::text, event_type, payload
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
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.EventType, &e.Payload); err != nil {
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
