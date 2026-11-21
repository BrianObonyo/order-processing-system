package repositories

import "context"

// ClaimIdempotencyKey tries to register the key. When another transaction holds
// the same key, the INSERT blocks until that transaction commits or rolls back,
// so concurrent duplicates serialise here instead of racing.
func (q *Queries) ClaimIdempotencyKey(ctx context.Context, tenantID, userID, key, requestHash, orderID string) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		INSERT INTO idempotency_keys (tenant_id, user_id, key, request_hash, order_id)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (tenant_id, user_id, key) DO NOTHING`, tenantID, userID, key, requestHash, orderID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (q *Queries) GetIdempotencyKey(ctx context.Context, tenantID, userID, key string) (requestHash, orderID string, err error) {
	err = q.db.QueryRow(ctx, `
		SELECT request_hash, order_id::text FROM idempotency_keys
		WHERE tenant_id = $1 AND user_id = $2 AND key = $3`, tenantID, userID, key).Scan(&requestHash, &orderID)
	return
}
