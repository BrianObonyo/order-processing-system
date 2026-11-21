package repositories

import (
	"context"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
)

func (q *Queries) ListProductStock(ctx context.Context, tenantID string) ([]inventory.ProductStock, error) {
	rows, err := q.db.Query(ctx, `
		SELECT p.id::text, p.sku, p.name, p.price_cents, i.on_hand, i.reserved
		FROM products p JOIN inventory i ON i.tenant_id = p.tenant_id AND i.product_id = p.id
		WHERE p.tenant_id = $1
		ORDER BY p.sku`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []inventory.ProductStock{}
	for rows.Next() {
		var p inventory.ProductStock
		if err := rows.Scan(&p.ProductID, &p.SKU, &p.Name, &p.PriceCents, &p.OnHand, &p.Reserved); err != nil {
			return nil, err
		}
		p.Available = p.OnHand - p.Reserved
		out = append(out, p)
	}
	return out, rows.Err()
}

func (q *Queries) GetProductPrices(ctx context.Context, tenantID string, ids []string) (map[string]int64, error) {
	rows, err := q.db.Query(ctx,
		`SELECT id::text, price_cents FROM products WHERE tenant_id = $1 AND id = ANY($2::text[]::uuid[])`, tenantID, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	prices := make(map[string]int64, len(ids))
	for rows.Next() {
		var id string
		var price int64
		if err := rows.Scan(&id, &price); err != nil {
			return nil, err
		}
		prices[id] = price
	}
	return prices, rows.Err()
}
