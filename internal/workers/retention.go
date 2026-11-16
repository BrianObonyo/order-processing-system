package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/repositories"
)

type RetentionWorker struct {
	pool                 *pgxpool.Pool
	interval             time.Duration
	outboxRetention      time.Duration
	idempotencyRetention time.Duration
	batchSize            int
}

func NewRetentionWorker(pool *pgxpool.Pool, interval, outboxRetention, idempotencyRetention time.Duration, batchSize int) *RetentionWorker {
	return &RetentionWorker{
		pool: pool, interval: interval, outboxRetention: outboxRetention,
		idempotencyRetention: idempotencyRetention, batchSize: batchSize,
	}
}

func (w *RetentionWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.Sweep(ctx); err != nil && ctx.Err() == nil {
				slog.Error("retention sweep failed", "err", err)
			}
		}
	}
}

func (w *RetentionWorker) Sweep(ctx context.Context) error {
	q := repositories.New(w.pool)
	outboxCutoff := time.Now().Add(-w.outboxRetention)
	idempotencyCutoff := time.Now().Add(-w.idempotencyRetention)

	outboxDeleted, err := q.DeletePublishedOutboxBefore(ctx, outboxCutoff, w.batchSize)
	if err != nil {
		return err
	}
	keysDeleted, err := q.DeleteIdempotencyKeysBefore(ctx, idempotencyCutoff, w.batchSize)
	if err != nil {
		return err
	}
	if outboxDeleted > 0 || keysDeleted > 0 {
		slog.Info("retention sweep completed", "outbox_deleted", outboxDeleted, "idempotency_keys_deleted", keysDeleted)
	}
	return nil
}
