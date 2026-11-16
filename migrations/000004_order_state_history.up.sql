CREATE TABLE order_statuses (
    status TEXT PRIMARY KEY
);

INSERT INTO order_statuses (status) VALUES
    ('PENDING_PAYMENT'),
    ('PAID'),
    ('CANCELLED');

ALTER TABLE orders DROP CONSTRAINT orders_status_check;
ALTER TABLE orders
    ADD CONSTRAINT orders_status_fkey FOREIGN KEY (status) REFERENCES order_statuses (status);

CREATE TABLE order_status_history (
    id          BIGSERIAL PRIMARY KEY,
    order_id    UUID NOT NULL REFERENCES orders (id),
    from_status TEXT REFERENCES order_statuses (status),
    to_status   TEXT NOT NULL REFERENCES order_statuses (status),
    reason      TEXT,
    changed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX order_status_history_order_changed_idx
    ON order_status_history (order_id, changed_at, id);

-- Earlier status transitions cannot be reconstructed; record each existing
-- order's current state as the audit baseline at migration time.
INSERT INTO order_status_history (order_id, from_status, to_status, reason, changed_at)
SELECT id, NULL, status, COALESCE(cancel_reason, 'baseline during migration'), now()
FROM orders;
