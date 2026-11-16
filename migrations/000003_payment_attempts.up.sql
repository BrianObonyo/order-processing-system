ALTER TABLE payments RENAME TO payment_attempts;
ALTER TABLE payment_attempts DROP CONSTRAINT payments_order_id_key;
ALTER TABLE payment_attempts
    ADD COLUMN attempt_number INT NOT NULL DEFAULT 1 CHECK (attempt_number > 0);
ALTER TABLE payment_attempts
    ADD CONSTRAINT payment_attempts_order_attempt_key UNIQUE (order_id, attempt_number);
CREATE INDEX payment_attempts_order_created_idx
    ON payment_attempts (order_id, attempt_number DESC);
