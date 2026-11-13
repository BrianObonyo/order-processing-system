CREATE TABLE products (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    sku         TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    price_cents BIGINT NOT NULL CHECK (price_cents >= 0),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stock lives here. The CHECK constraints are the last line of defence against
-- overselling: even a buggy code path cannot drive stock negative.
CREATE TABLE inventory (
    product_id UUID PRIMARY KEY REFERENCES products (id),
    on_hand    INT NOT NULL CHECK (on_hand >= 0),
    reserved   INT NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT reserved_within_stock CHECK (reserved <= on_hand)
);

CREATE TABLE orders (
    id                UUID PRIMARY KEY,
    user_id           TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('PENDING_PAYMENT', 'PAID', 'CANCELLED')),
    total_cents       BIGINT NOT NULL CHECK (total_cents >= 0),
    cancel_reason     TEXT,
    recovery_attempts INT NOT NULL DEFAULT 0,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX orders_status_created_idx ON orders (status, created_at);
CREATE INDEX orders_user_created_idx ON orders (user_id, created_at DESC);

CREATE TABLE order_items (
    order_id         UUID NOT NULL REFERENCES orders (id),
    product_id       UUID NOT NULL REFERENCES products (id),
    quantity         INT NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents >= 0),
    PRIMARY KEY (order_id, product_id)
);

-- UNIQUE (order_id) guarantees at most one payment per order, no matter how
-- many times the order.created event is delivered.
CREATE TABLE payments (
    id             UUID PRIMARY KEY,
    order_id       UUID NOT NULL UNIQUE REFERENCES orders (id),
    amount_cents   BIGINT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('PROCESSING', 'SUCCEEDED', 'FAILED', 'REFUNDED')),
    failure_reason TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The key is claimed in the same transaction that creates the order. The FK is
-- deferred because the key row is inserted before the order row.
CREATE TABLE idempotency_keys (
    user_id      TEXT NOT NULL,
    key          TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    order_id     UUID NOT NULL REFERENCES orders (id) DEFERRABLE INITIALLY DEFERRED,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, key)
);

-- Transactional outbox: written in the same transaction as the state change,
-- relayed to Kafka by a separate worker.
CREATE TABLE outbox (
    id           BIGSERIAL PRIMARY KEY,
    aggregate_id UUID NOT NULL,
    event_type   TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
