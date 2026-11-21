package repositories

import (
	"context"
	"time"
)

func (q *Queries) DeletePublishedOutboxBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		WITH expired AS (
			SELECT id FROM outbox
			WHERE published_at IS NOT NULL AND published_at < $1
			ORDER BY id
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM outbox o USING expired e WHERE o.id = e.id`, cutoff, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func (q *Queries) DeleteIdempotencyKeysBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := q.db.Exec(ctx, `
		WITH expired AS (
			SELECT tenant_id, user_id, key FROM idempotency_keys
			WHERE created_at < $1
			ORDER BY created_at, tenant_id, user_id, key
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM idempotency_keys k
		USING expired e
		WHERE k.tenant_id = e.tenant_id AND k.user_id = e.user_id AND k.key = e.key`, cutoff, limit)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
