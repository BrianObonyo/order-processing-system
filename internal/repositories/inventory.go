package repositories

import (
	"context"
	"fmt"
)

// ReserveStock atomically claims qty units. The WHERE clause is the whole
// anti-overselling mechanism: the row is locked by the UPDATE, and the
// availability check and the increment happen as one statement, so two
// concurrent orders can never both claim the last unit.
func (q *Queries) ReserveStock(ctx context.Context, tenantID, productID string, qty int) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		UPDATE inventory
		SET reserved = reserved + $3, updated_at = now()
		WHERE tenant_id = $1 AND product_id = $2 AND on_hand - reserved >= $3`, tenantID, productID, qty)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// ReleaseStock returns a reservation to the available pool (cancellation).
func (q *Queries) ReleaseStock(ctx context.Context, tenantID, productID string, qty int) error {
	tag, err := q.db.Exec(ctx, `
		UPDATE inventory
		SET reserved = reserved - $3, updated_at = now()
		WHERE tenant_id = $1 AND product_id = $2`, tenantID, productID, qty)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("release stock: product %s has no inventory row", productID)
	}
	return nil
}

// CommitStock turns a reservation into a real stock deduction (payment succeeded).
func (q *Queries) CommitStock(ctx context.Context, tenantID, productID string, qty int) error {
	tag, err := q.db.Exec(ctx, `
		UPDATE inventory
		SET on_hand = on_hand - $3, reserved = reserved - $3, updated_at = now()
		WHERE tenant_id = $1 AND product_id = $2`, tenantID, productID, qty)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("commit stock: product %s has no inventory row", productID)
	}
	return nil
}
