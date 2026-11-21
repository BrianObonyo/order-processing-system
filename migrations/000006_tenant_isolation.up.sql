CREATE TABLE tenants (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO tenants (id, name) VALUES ('default', 'Default tenant');

ALTER TABLE products ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE products ADD CONSTRAINT products_tenant_fkey
    FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE products DROP CONSTRAINT products_sku_key;
ALTER TABLE products ADD CONSTRAINT products_tenant_sku_key UNIQUE (tenant_id, sku);
ALTER TABLE products ADD CONSTRAINT products_tenant_id_key UNIQUE (tenant_id, id);
ALTER TABLE products ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE inventory ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE inventory DROP CONSTRAINT inventory_product_id_fkey;
ALTER TABLE inventory DROP CONSTRAINT inventory_pkey;
ALTER TABLE inventory ADD CONSTRAINT inventory_tenant_product_pkey PRIMARY KEY (tenant_id, product_id);
ALTER TABLE inventory ADD CONSTRAINT inventory_tenant_product_fkey
    FOREIGN KEY (tenant_id, product_id) REFERENCES products (tenant_id, id);
ALTER TABLE inventory ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE orders ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE orders ADD CONSTRAINT orders_tenant_fkey
    FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE orders ADD CONSTRAINT orders_tenant_id_key UNIQUE (tenant_id, id);
ALTER TABLE orders ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE order_items ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE order_items DROP CONSTRAINT order_items_order_id_fkey;
ALTER TABLE order_items DROP CONSTRAINT order_items_product_id_fkey;
ALTER TABLE order_items ADD CONSTRAINT order_items_tenant_order_fkey
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, id);
ALTER TABLE order_items ADD CONSTRAINT order_items_tenant_product_fkey
    FOREIGN KEY (tenant_id, product_id) REFERENCES products (tenant_id, id);
ALTER TABLE order_items ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE payment_attempts ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE payment_attempts DROP CONSTRAINT payments_order_id_fkey;
ALTER TABLE payment_attempts ADD CONSTRAINT payment_attempts_tenant_order_fkey
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, id);
ALTER TABLE payment_attempts ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE idempotency_keys ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_pkey;
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_order_id_fkey;
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_tenant_key_pkey
    PRIMARY KEY (tenant_id, user_id, key);
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_tenant_order_fkey
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, id) DEFERRABLE INITIALLY DEFERRED;
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_tenant_fkey
    FOREIGN KEY (tenant_id) REFERENCES tenants (id);
ALTER TABLE idempotency_keys ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE order_status_history ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE order_status_history DROP CONSTRAINT order_status_history_order_id_fkey;
ALTER TABLE order_status_history ADD CONSTRAINT order_status_history_tenant_order_fkey
    FOREIGN KEY (tenant_id, order_id) REFERENCES orders (tenant_id, id);
ALTER TABLE order_status_history ALTER COLUMN tenant_id DROP DEFAULT;

ALTER TABLE outbox ADD COLUMN tenant_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE outbox ADD CONSTRAINT outbox_tenant_order_fkey
    FOREIGN KEY (tenant_id, aggregate_id) REFERENCES orders (tenant_id, id);
ALTER TABLE outbox ALTER COLUMN tenant_id DROP DEFAULT;

DROP INDEX orders_status_created_idx;
DROP INDEX orders_user_created_idx;
CREATE INDEX orders_tenant_status_created_idx ON orders (tenant_id, status, created_at);
CREATE INDEX orders_tenant_user_created_idx ON orders (tenant_id, user_id, created_at DESC);
CREATE INDEX orders_tenant_created_idx ON orders (tenant_id, created_at DESC);
CREATE INDEX products_tenant_sku_idx ON products (tenant_id, sku);
