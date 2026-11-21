package repositories

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

// InsertPaymentAttempt appends an attempt. Callers hold the order lock so the
// next attempt number is allocated serially for that order.
func (q *Queries) InsertPaymentAttempt(ctx context.Context, id, tenantID, orderID string, amountCents int64) error {
	_, err := q.db.Exec(ctx, `
		INSERT INTO payment_attempts (id, tenant_id, order_id, attempt_number, amount_cents, status)
		SELECT $1, $2, $3, COALESCE(MAX(attempt_number), 0) + 1, $4, 'PROCESSING'
		FROM payment_attempts WHERE tenant_id = $2 AND order_id = $3`, id, tenantID, orderID, amountCents)
	return err
}

func (q *Queries) GetLatestPaymentAttemptByOrder(ctx context.Context, tenantID, orderID string) (*payment.Payment, error) {
	var p payment.Payment
	var status string
	err := q.db.QueryRow(ctx, `
		SELECT id::text, tenant_id, order_id::text, attempt_number, amount_cents, status, COALESCE(failure_reason, '')
		FROM payment_attempts WHERE tenant_id = $1 AND order_id = $2
		ORDER BY attempt_number DESC LIMIT 1`, tenantID, orderID).
		Scan(&p.ID, &p.TenantID, &p.OrderID, &p.AttemptNumber, &p.AmountCents, &status, &p.FailureReason)
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
		SELECT id::text, tenant_id, order_id::text, attempt_number, amount_cents, status, COALESCE(failure_reason, '')
		FROM payment_attempts WHERE id = $1`, id).
		Scan(&p.ID, &p.TenantID, &p.OrderID, &p.AttemptNumber, &p.AmountCents, &status, &p.FailureReason)
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
		UPDATE payment_attempts SET status = $2, failure_reason = NULLIF($3, ''), updated_at = now()
		WHERE id = $1`, id, string(status), reason)
	return err
}

func (q *Queries) TransitionPaymentStatus(ctx context.Context, id string, from, to payment.Status, reason string) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE payment_attempts SET status = $3, failure_reason = NULLIF($4, ''), updated_at = now()
		WHERE id = $1 AND status = $2`, id, string(from), string(to), reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (q *Queries) TouchPendingRefund(ctx context.Context, id string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE payment_attempts SET updated_at = now()
		WHERE id = $1 AND status = 'REFUND_PENDING'`, id)
	return err
}

func (q *Queries) ListStaleRefunds(ctx context.Context, updatedBefore time.Time, limit int) ([]string, error) {
	return q.listPaymentIDs(ctx, `
		SELECT id::text FROM payment_attempts
		WHERE status = 'REFUND_PENDING' AND updated_at < $1
		ORDER BY updated_at LIMIT $2`, updatedBefore, limit)
}

func (q *Queries) ScheduleRefundRetry(ctx context.Context, id string, updatedBefore time.Time) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE payment_attempts SET updated_at = now()
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
		UPDATE payment_attempts SET status = 'FAILED', failure_reason = NULLIF($2, ''), updated_at = now()
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
