package repositories

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
)

const selectOrder = `
	SELECT id::text, tenant_id, user_id, status, total_cents, COALESCE(cancel_reason, ''), created_at, updated_at
	FROM orders`

func scanOrder(row pgx.Row) (*order.Order, error) {
	var o order.Order
	var status string
	err := row.Scan(&o.ID, &o.TenantID, &o.UserID, &status, &o.TotalCents, &o.CancelReason, &o.CreatedAt, &o.UpdatedAt)
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
		`WITH inserted AS (
			INSERT INTO orders (id, tenant_id, user_id, status, total_cents)
			VALUES ($1, $2, $3, $4, $5)
			RETURNING id, tenant_id, status, created_at
		)
		INSERT INTO order_status_history (tenant_id, order_id, from_status, to_status, changed_at)
		SELECT tenant_id, id, NULL, status, created_at FROM inserted`,
		o.ID, o.TenantID, o.UserID, string(o.Status), o.TotalCents)
	return err
}

func (q *Queries) InsertOrderItems(ctx context.Context, tenantID, orderID string, items []order.Item) error {
	for _, it := range items {
		if _, err := q.db.Exec(ctx, `
			INSERT INTO order_items (tenant_id, order_id, product_id, quantity, unit_price_cents)
			VALUES ($1, $2, $3, $4, $5)`, tenantID, orderID, it.ProductID, it.Quantity, it.UnitPriceCents); err != nil {
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

func (q *Queries) GetOrderForTenant(ctx context.Context, tenantID, id string) (*order.Order, error) {
	o, err := scanOrder(q.db.QueryRow(ctx, selectOrder+` WHERE tenant_id = $1 AND id = $2`, tenantID, id))
	if err != nil {
		return nil, err
	}
	o.Items, err = q.GetOrderItems(ctx, id)
	return o, err
}

// ListOrders returns one user's orders, newest first, with a stable tie-breaker.
func (q *Queries) ListOrders(ctx context.Context, tenantID, userID string, limit, offset int) ([]*order.Order, int64, error) {
	var total int64
	if err := q.db.QueryRow(ctx, `
		SELECT count(*) FROM orders WHERE tenant_id = $1 AND ($2 = '' OR user_id = $2)`,
		tenantID, userID).Scan(&total); err != nil {
		return nil, 0, err
	}

	rows, err := q.db.Query(ctx, selectOrder+`
		WHERE tenant_id = $1 AND ($2 = '' OR user_id = $2)
		ORDER BY created_at DESC, id DESC
		LIMIT $3 OFFSET $4`, tenantID, userID, limit, offset)
	if err != nil {
		return nil, 0, err
	}
	var result []*order.Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			rows.Close()
			return nil, 0, err
		}
		result = append(result, o)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, 0, err
	}
	rows.Close()

	for _, o := range result {
		o.Items, err = q.GetOrderItems(ctx, o.ID)
		if err != nil {
			return nil, 0, err
		}
	}
	return result, total, nil
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
	if !from.CanTransitionTo(to) {
		return false, fmt.Errorf("%w: %s -> %s", order.ErrInvalidTransition, from, to)
	}
	tag, err := q.db.Exec(ctx, `
		WITH changed AS (
			UPDATE orders
			SET status = $3, cancel_reason = NULLIF($4, ''), updated_at = now()
			WHERE id = $1 AND status = $2
			RETURNING id, tenant_id, status, updated_at
		)
		INSERT INTO order_status_history (tenant_id, order_id, from_status, to_status, reason, changed_at)
		SELECT tenant_id, id, $2, status, NULLIF($4, ''), updated_at FROM changed`,
		id, string(from), string(to), reason)
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
		  AND NOT EXISTS (
		      SELECT 1 FROM payment_attempts p
		      WHERE p.order_id = orders.id AND p.status = 'RECONCILING'
		  )
		  AND NOT EXISTS (
		      SELECT 1 FROM payment_attempts p
		      WHERE p.order_id = orders.id
		        AND p.attempt_number = (
		            SELECT MAX(latest.attempt_number)
		            FROM payment_attempts latest WHERE latest.order_id = orders.id
		        )
		        AND p.status = 'FAILED'
		  )
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
