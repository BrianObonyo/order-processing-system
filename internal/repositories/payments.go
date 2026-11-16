package repositories

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

// InsertPayment creates the payment row for an order. UNIQUE (order_id) plus
// ON CONFLICT DO NOTHING makes this safe under duplicate event delivery.
func (q *Queries) InsertPayment(ctx context.Context, id, orderID string, amountCents int64) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO payments (id, order_id, amount_cents, status)
		VALUES ($1, $2, $3, 'PROCESSING')
		ON CONFLICT (order_id) DO NOTHING`, id, orderID, amountCents)
	return err
}

func (q *Queries) GetPaymentByOrder(ctx context.Context, orderID string) (*payment.Payment, error) {
	var p payment.Payment
	var status string
	err := q.db.QueryRow(ctx, `
		SELECT id::text, order_id::text, amount_cents, status, COALESCE(failure_reason, '')
		FROM payments WHERE order_id = $1`, orderID).
		Scan(&p.ID, &p.OrderID, &p.AmountCents, &status, &p.FailureReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, payment.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	p.Status = payment.Status(status)
	return &p, nil
}

func (q *Queries) GetPaymentByID(ctx context.Context, id string) (*payment.Payment, error) {
	var p payment.Payment
	var status string
	err := q.db.QueryRow(ctx, `
		SELECT id::text, order_id::text, amount_cents, status, COALESCE(failure_reason, '')
		FROM payments WHERE id = $1`, id).
		Scan(&p.ID, &p.OrderID, &p.AmountCents, &status, &p.FailureReason)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, payment.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	p.Status = payment.Status(status)
	return &p, nil
}

func (q *Queries) SetPaymentResult(ctx context.Context, id string, status payment.Status, reason string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE payments SET status = $2, failure_reason = NULLIF($3, ''), updated_at = now()
		WHERE id = $1`, id, string(status), reason)
	return err
}

func (q *Queries) TransitionPaymentStatus(ctx context.Context, id string, from, to payment.Status, reason string) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE payments SET status = $3, failure_reason = NULLIF($4, ''), updated_at = now()
		WHERE id = $1 AND status = $2`, id, string(from), string(to), reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (q *Queries) TouchPendingRefund(ctx context.Context, id string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE payments SET updated_at = now()
		WHERE id = $1 AND status = 'REFUND_PENDING'`, id)
	return err
}

func (q *Queries) ListStaleRefunds(ctx context.Context, updatedBefore time.Time, limit int) ([]string, error) {
	return q.listPaymentIDs(ctx, `
		SELECT id::text FROM payments
		WHERE status = 'REFUND_PENDING' AND updated_at < $1
		ORDER BY updated_at LIMIT $2`, updatedBefore, limit)
}

func (q *Queries) ScheduleRefundRetry(ctx context.Context, id string, updatedBefore time.Time) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE payments SET updated_at = now()
		WHERE id = $1 AND status = 'REFUND_PENDING' AND updated_at < $2`,
		id, updatedBefore)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// FailProcessingPayment closes an in-flight payment when its order is cancelled.
func (q *Queries) FailProcessingPayment(ctx context.Context, orderID, reason string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE payments SET status = 'FAILED', failure_reason = NULLIF($2, ''), updated_at = now()
		WHERE order_id = $1 AND status IN ('PROCESSING', 'RECONCILING')`, orderID, reason)
	return err
}

func (q *Queries) listPaymentIDs(ctx context.Context, sql string, args ...any) ([]string, error) {
	rows, err := q.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
