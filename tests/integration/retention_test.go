//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/services"
	"github.com/BrianObonyo/order-processing-system/internal/workers"
	"github.com/BrianObonyo/order-processing-system/tests/testutil"
)

func TestRetentionDeletesOnlyPublishedOutboxAndExpiredIdempotencyKeys(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 5)

	published, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "retention-published-1",
		[]order.CreateItem{{ProductID: product, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	unpublished, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "retention-pending-1",
		[]order.CreateItem{{ProductID: product, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE outbox SET published_at = now() - interval '45 days'
		WHERE aggregate_id = $1`, published.Order.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE idempotency_keys SET created_at = now() - interval '45 days'
		WHERE key IN ($1, $2)`, "retention-published-1", "retention-pending-1"); err != nil {
		t.Fatal(err)
	}

	retention := workers.NewRetentionWorker(pool, time.Hour, 14*24*time.Hour, 30*24*time.Hour, 1)
	if err := retention.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}

	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, published.Order.ID); n != 0 {
		t.Fatalf("expired published outbox rows=%d, want 0", n)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE aggregate_id = $1`, unpublished.Order.ID); n != 1 {
		t.Fatalf("unpublished outbox rows=%d, want 1", n)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM idempotency_keys`); n != 1 {
		t.Fatalf("expired idempotency keys after bounded sweep=%d, want 1", n)
	}
	if err := retention.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("expired idempotency keys after second sweep=%d, want 0", n)
	}
}

func TestRetentionScopesIdenticalIdempotencyKeysByTenant(t *testing.T) {
	pool := testutil.StartPostgres(t)
	testutil.SeedTenant(t, pool, "retention-other")
	store := testutil.Store(pool)
	orders := services.NewOrderService(store)
	defaultProduct := testutil.SeedProduct(t, pool, 2)
	otherProduct := testutil.SeedProductForTenant(t, pool, "retention-other", 2)

	if _, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("same-user"),
		"shared-key-for-tenants", []order.CreateItem{{ProductID: defaultProduct, Quantity: 1}}); err != nil {
		t.Fatal(err)
	}
	otherIdentity := testutil.UserIdentity("same-user")
	otherIdentity.TenantID = "retention-other"
	if _, err := orders.CreateOrder(context.Background(), otherIdentity,
		"shared-key-for-tenants", []order.CreateItem{{ProductID: otherProduct, Quantity: 1}}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE idempotency_keys SET created_at = now() - interval '45 days'
		WHERE tenant_id = 'default' AND user_id = 'same-user' AND key = 'shared-key-for-tenants'`); err != nil {
		t.Fatal(err)
	}

	retention := workers.NewRetentionWorker(pool, time.Hour, 14*24*time.Hour, 30*24*time.Hour, 10)
	if err := retention.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := testutil.Count(t, pool, `
		SELECT count(*) FROM idempotency_keys
		WHERE tenant_id = 'default' AND user_id = 'same-user' AND key = 'shared-key-for-tenants'`); n != 0 {
		t.Fatalf("expired default-tenant key rows=%d, want 0", n)
	}
	if n := testutil.Count(t, pool, `
		SELECT count(*) FROM idempotency_keys
		WHERE tenant_id = 'retention-other' AND user_id = 'same-user' AND key = 'shared-key-for-tenants'`); n != 1 {
		t.Fatalf("unexpired other-tenant key rows=%d, want 1", n)
	}
}
