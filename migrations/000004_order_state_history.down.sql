DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM orders
        WHERE status NOT IN ('PENDING_PAYMENT', 'PAID', 'CANCELLED')
    ) THEN
        RAISE EXCEPTION 'cannot restore the original order status constraint while extended statuses are in use';
    END IF;
END $$;

DROP TABLE order_status_history;
ALTER TABLE orders DROP CONSTRAINT orders_status_fkey;
ALTER TABLE orders
    ADD CONSTRAINT orders_status_check
    CHECK (status IN ('PENDING_PAYMENT', 'PAID', 'CANCELLED'));
DROP TABLE order_statuses;
