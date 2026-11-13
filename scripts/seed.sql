-- Dev seed data with fixed IDs so curl examples and k6 scripts work as-is.
INSERT INTO products (id, sku, name, price_cents) VALUES
  ('11111111-1111-1111-1111-111111111111', 'WIDGET-001', 'Widget (plenty of stock)', 1999),
  ('22222222-2222-2222-2222-222222222222', 'GADGET-LOW', 'Gadget (only 10 in stock)', 4999)
ON CONFLICT DO NOTHING;

INSERT INTO inventory (product_id, on_hand, reserved) VALUES
  ('11111111-1111-1111-1111-111111111111', 100000, 0),
  ('22222222-2222-2222-2222-222222222222', 10, 0)
ON CONFLICT DO NOTHING;
