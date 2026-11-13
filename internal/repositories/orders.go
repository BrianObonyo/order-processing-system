package repositories

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
)

const selectOrder = `
	SELECT id::text, user_id, status, total_cents, COALESCE(cancel_reason, ''), created_at, updated_at
	FROM orders`

func scanOrder(row pgx.Row) (*order.Order, error) {
	var o order.Order
	var status string
	err := row.Scan(&o.ID, &o.UserID, &status, &o.TotalCents, &o.CancelReason, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, order.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	o.Status = order.Status(status)
	return &o, nil
}

func (q *Queries) InsertOrder(ctx context.Context, o *order.Order) error {
	_, err := q.db.Exec(ctx,
		`INSERT INTO orders (id, user_id, status, total_cents) VALUES ($1, $2, $3, $4)`,
		o.ID, o.UserID, string(o.Status), o.TotalCents)
	return err
}

func (q *Queries) InsertOrderItems(ctx context.Context, orderID string, items []order.Item) error {
	for _, it := range items {
		if _, err := q.db.Exec(ctx, `
			INSERT INTO order_items (order_id, product_id, quantity, unit_price_cents)
			VALUES ($1, $2, $3, $4)`, orderID, it.ProductID, it.Quantity, it.UnitPriceCents); err != nil {
			return err
		}
	}
	return nil
}

// GetOrder loads an order with its items.
func (q *Queries) GetOrder(ctx context.Context, id string) (*order.Order, error) {
	o, err := scanOrder(q.db.QueryRow(ctx, selectOrder+` WHERE id = $1`, id))
	if err != nil {
		return nil, err
	}
	o.Items, err = q.GetOrderItems(ctx, id)
	return o, err
}

// LockOrder takes a row lock; every lifecycle transition starts here so that
// payment completion and recovery cancellation serialise on the same row.
func (q *Queries) LockOrder(ctx context.Context, id string) (*order.Order, error) {
	return scanOrder(q.db.QueryRow(ctx, selectOrder+` WHERE id = $1 FOR UPDATE`, id))
}

// GetOrderItems returns items sorted by product ID (stable lock order).
func (q *Queries) GetOrderItems(ctx context.Context, orderID string) ([]order.Item, error) {
	rows, err := q.db.Query(ctx, `
		SELECT product_id::text, quantity, unit_price_cents
		FROM order_items WHERE order_id = $1 ORDER BY product_id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	items := []order.Item{}
	for rows.Next() {
		var it order.Item
		if err := rows.Scan(&it.ProductID, &it.Quantity, &it.UnitPriceCents); err != nil {
			return nil, err
		}
		items = append(items, it)
	}
	return items, rows.Err()
}

// SetOrderStatus is a compare-and-set: it only applies if the order is still in
// the expected state, which makes every transition safe to attempt twice.
func (q *Queries) SetOrderStatus(ctx context.Context, id string, from, to order.Status, reason string) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE orders
		SET status = $3, cancel_reason = NULLIF($4, ''), updated_at = now()
		WHERE id = $1 AND status = $2`, id, string(from), string(to), reason)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ListExpiredOrders finds pending orders older than createdBefore.
func (q *Queries) ListExpiredOrders(ctx context.Context, createdBefore time.Time, limit int) ([]string, error) {
	return q.listIDs(ctx, `
		SELECT id::text FROM orders
		WHERE status = 'PENDING_PAYMENT' AND created_at < $1
		ORDER BY created_at LIMIT $2`, createdBefore, limit)
}

// ListStalledOrders finds pending orders that made no progress since idleBefore
// and have retry budget left.
func (q *Queries) ListStalledOrders(ctx context.Context, idleBefore time.Time, maxAttempts, limit int) ([]string, error) {
	return q.listIDs(ctx, `
		SELECT id::text FROM orders
		WHERE status = 'PENDING_PAYMENT' AND updated_at < $1 AND recovery_attempts < $2
		ORDER BY updated_at LIMIT $3`, idleBefore, maxAttempts, limit)
}

// BumpRecovery records a retry attempt and resets the idle timer.
func (q *Queries) BumpRecovery(ctx context.Context, id string) error {
	_, err := q.db.Exec(ctx, `
		UPDATE orders SET recovery_attempts = recovery_attempts + 1, updated_at = now()
		WHERE id = $1 AND status = 'PENDING_PAYMENT'`, id)
	return err
}

func (q *Queries) listIDs(ctx context.Context, sql string, args ...any) ([]string, error) {
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
