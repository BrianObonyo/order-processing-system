DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM payments WHERE status IN ('RECONCILING', 'REFUND_PENDING')
    ) THEN
        RAISE EXCEPTION 'cannot roll back payment reconciliation while payments need reconciliation or refunds';
    END IF;
END $$;

ALTER TABLE payments DROP CONSTRAINT payments_status_check;
ALTER TABLE payments ADD CONSTRAINT payments_status_check
    CHECK (status IN ('PROCESSING', 'SUCCEEDED', 'FAILED', 'REFUNDED'));
