DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM payment_attempts
        GROUP BY order_id HAVING count(*) > 1
    ) THEN
        RAISE EXCEPTION 'cannot roll back payment attempts while an order has multiple attempts';
    END IF;
END $$;

DROP INDEX payment_attempts_order_created_idx;
ALTER TABLE payment_attempts DROP CONSTRAINT payment_attempts_order_attempt_key;
ALTER TABLE payment_attempts DROP COLUMN attempt_number;
ALTER TABLE payment_attempts ADD CONSTRAINT payment_attempts_order_id_key UNIQUE (order_id);
ALTER TABLE payment_attempts RENAME TO payments;
