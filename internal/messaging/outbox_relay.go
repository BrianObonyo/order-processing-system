package messaging

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/metrics"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
)

// OutboxRelay moves committed outbox rows to the broker. A row is only marked
// published after the broker acknowledged it, so a crash between the two steps
// causes a duplicate delivery, never a lost event (at-least-once).
type OutboxRelay struct {
	pool      *pgxpool.Pool
	publisher Publisher
	interval  time.Duration
	batchSize int
}

func NewOutboxRelay(pool *pgxpool.Pool, p Publisher, interval time.Duration) *OutboxRelay {
	return &OutboxRelay{pool: pool, publisher: p, interval: interval, batchSize: 100}
}

func (r *OutboxRelay) Run(ctx context.Context) {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Drain until the backlog is empty or an error occurs.
			for {
				n, err := r.Flush(ctx)
				if err != nil {
					if ctx.Err() == nil {
						slog.Error("outbox flush failed", "err", err)
					}
					break
				}
				if n < r.batchSize {
					break
				}
			}
		}
	}
}

// Flush publishes one batch and returns how many events were relayed.
func (r *OutboxRelay) Flush(ctx context.Context) (int, error) {
	published := 0
	err := pgx.BeginFunc(ctx, r.pool, func(tx pgx.Tx) error {
		q := repositories.New(tx)
		events, err := q.FetchUnpublished(ctx, r.batchSize)
		if err != nil || len(events) == 0 {
			return err
		}

		msgs := make([]Message, len(events))
		ids := make([]int64, len(events))
		for i, e := range events {
			msgs[i] = Message{Key: e.AggregateID, Type: e.EventType, Payload: e.Payload}
			ids[i] = e.ID
		}

		pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := r.publisher.Publish(pubCtx, msgs); err != nil {
			return err
		}
		if err := q.MarkPublished(ctx, ids); err != nil {
			return err
		}
		published = len(events)
		return nil
	})
	if err == nil && published > 0 {
		metrics.OutboxPublished.Add(float64(published))
	}
	return published, err
}
