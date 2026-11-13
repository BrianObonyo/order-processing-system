package repositories

import (
	"context"
	"errors"

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
		return nil, errors.New("payment not found")
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

// FailProcessingPayment closes an in-flight payment when its order is cancelled.
func (q *Queries) FailProcessingPayment(ctx context.Context, orderID, reason string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE payments SET status = 'FAILED', failure_reason = NULLIF($2, ''), updated_at = now()
		WHERE order_id = $1 AND status = 'PROCESSING'`, orderID, reason)
	return err
}
