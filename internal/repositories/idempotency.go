package repositories

import "context"

// ClaimIdempotencyKey tries to register the key. When another transaction holds
// the same key, the INSERT blocks until that transaction commits or rolls back,
// so concurrent duplicates serialise here instead of racing.
func (q *Queries) ClaimIdempotencyKey(ctx context.Context, userID, key, requestHash, orderID string) (bool, error) {
	tag, err := q.db.Exec(ctx, `
		INSERT INTO idempotency_keys (user_id, key, request_hash, order_id)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (user_id, key) DO NOTHING`, userID, key, requestHash, orderID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (q *Queries) GetIdempotencyKey(ctx context.Context, userID, key string) (requestHash, orderID string, err error) {
	err = q.db.QueryRow(ctx, `
		SELECT request_hash, order_id::text FROM idempotency_keys
		WHERE user_id = $1 AND key = $2`, userID, key).Scan(&requestHash, &orderID)
	return
}
