DROP INDEX products_tenant_sku_idx;
DROP INDEX orders_tenant_created_idx;
DROP INDEX orders_tenant_user_created_idx;
DROP INDEX orders_tenant_status_created_idx;

ALTER TABLE outbox DROP CONSTRAINT outbox_tenant_order_fkey;
ALTER TABLE outbox DROP COLUMN tenant_id;

ALTER TABLE order_status_history DROP CONSTRAINT order_status_history_tenant_order_fkey;
ALTER TABLE order_status_history DROP COLUMN tenant_id;
ALTER TABLE order_status_history ADD CONSTRAINT order_status_history_order_id_fkey
    FOREIGN KEY (order_id) REFERENCES orders (id);

ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_tenant_fkey;
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_tenant_order_fkey;
ALTER TABLE idempotency_keys DROP CONSTRAINT idempotency_keys_tenant_key_pkey;
ALTER TABLE idempotency_keys DROP COLUMN tenant_id;
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_pkey PRIMARY KEY (user_id, key);
ALTER TABLE idempotency_keys ADD CONSTRAINT idempotency_keys_order_id_fkey
    FOREIGN KEY (order_id) REFERENCES orders (id) DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE payment_attempts DROP CONSTRAINT payment_attempts_tenant_order_fkey;
ALTER TABLE payment_attempts DROP COLUMN tenant_id;
ALTER TABLE payment_attempts ADD CONSTRAINT payments_order_id_fkey
    FOREIGN KEY (order_id) REFERENCES orders (id);

ALTER TABLE order_items DROP CONSTRAINT order_items_tenant_order_fkey;
ALTER TABLE order_items DROP CONSTRAINT order_items_tenant_product_fkey;
ALTER TABLE order_items DROP COLUMN tenant_id;
ALTER TABLE order_items ADD CONSTRAINT order_items_order_id_fkey
    FOREIGN KEY (order_id) REFERENCES orders (id);
ALTER TABLE order_items ADD CONSTRAINT order_items_product_id_fkey
    FOREIGN KEY (product_id) REFERENCES products (id);

ALTER TABLE inventory DROP CONSTRAINT inventory_tenant_product_fkey;
ALTER TABLE inventory DROP CONSTRAINT inventory_tenant_product_pkey;
ALTER TABLE inventory DROP COLUMN tenant_id;
ALTER TABLE inventory ADD CONSTRAINT inventory_pkey PRIMARY KEY (product_id);
ALTER TABLE inventory ADD CONSTRAINT inventory_product_id_fkey
    FOREIGN KEY (product_id) REFERENCES products (id);

ALTER TABLE products DROP CONSTRAINT products_tenant_fkey;
ALTER TABLE products DROP CONSTRAINT products_tenant_sku_key;
ALTER TABLE products DROP CONSTRAINT products_tenant_id_key;
ALTER TABLE products DROP COLUMN tenant_id;
ALTER TABLE products ADD CONSTRAINT products_sku_key UNIQUE (sku);

ALTER TABLE orders DROP CONSTRAINT orders_tenant_fkey;
ALTER TABLE orders DROP CONSTRAINT orders_tenant_id_key;
ALTER TABLE orders DROP COLUMN tenant_id;
CREATE INDEX orders_status_created_idx ON orders (status, created_at);
CREATE INDEX orders_user_created_idx ON orders (user_id, created_at DESC);

DROP TABLE tenants;
