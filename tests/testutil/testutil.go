//go:build integration

// Package testutil starts a throwaway PostgreSQL (Testcontainers) with the real
// migrations applied, so tests exercise the same SQL as production.
package testutil

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/infrastructure/postgres"
)

func Store(pool *pgxpool.Pool) *postgres.Store { return postgres.NewStore(pool) }

func StartPostgres(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcpostgres.Run(ctx, "postgres:16-alpine",
		tcpostgres.WithDatabase("orders"),
		tcpostgres.WithUsername("orders"),
		tcpostgres.WithPassword("orders"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(90*time.Second)),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = ctr.Terminate(context.Background()) })

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.MaxConns = 50
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	_, thisFile, _, _ := runtime.Caller(0)
	files, err := filepath.Glob(filepath.Join(filepath.Dir(thisFile), "..", "..", "migrations", "*.up.sql"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no migrations found: %v", err)
	}
	sort.Strings(files)
	for _, f := range files {
		sql, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply %s: %v", f, err)
		}
	}
	return pool
}

// SeedProduct inserts a product with the given stock and returns its ID.
func SeedProduct(t *testing.T, pool *pgxpool.Pool, stock int) string {
	return SeedProductForTenant(t, pool, "default", stock)
}

func SeedProductForTenant(t *testing.T, pool *pgxpool.Pool, tenantID string, stock int) string {
	t.Helper()
	id := uuid.NewString()
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`INSERT INTO products (id, tenant_id, sku, name, price_cents) VALUES ($1, $2, $3, 'test product', 1000)`,
		id, tenantID, "SKU-"+id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx,
		`INSERT INTO inventory (tenant_id, product_id, on_hand) VALUES ($1, $2, $3)`, tenantID, id, stock); err != nil {
		t.Fatal(err)
	}
	return id
}

func SeedTenant(t *testing.T, pool *pgxpool.Pool, tenantID string) {
	t.Helper()
	if _, err := pool.Exec(context.Background(),
		`INSERT INTO tenants (id, name) VALUES ($1, $1) ON CONFLICT (id) DO NOTHING`, tenantID); err != nil {
		t.Fatal(err)
	}
}

func UserIdentity(userID string) identity.Identity {
	return identity.Identity{TenantID: "default", UserID: userID, Role: identity.RoleUser}
}

func AdminIdentity(tenantID, userID string) identity.Identity {
	return identity.Identity{TenantID: tenantID, UserID: userID, Role: identity.RoleAdmin}
}

func Stock(t *testing.T, pool *pgxpool.Pool, productID string) (onHand, reserved int) {
	t.Helper()
	err := pool.QueryRow(context.Background(),
		`SELECT on_hand, reserved FROM inventory WHERE product_id = $1`, productID).Scan(&onHand, &reserved)
	if err != nil {
		t.Fatal(err)
	}
	return
}

func Count(t *testing.T, pool *pgxpool.Pool, sql string, args ...any) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), sql, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// StubGateway is a payment.Gateway with a fixed outcome.
type StubGateway struct {
	Err         error
	ChargeState payment.ChargeStatus
	RefundErr   error
}

func (g StubGateway) Charge(context.Context, string, int64) error { return g.Err }

func (g StubGateway) Refund(context.Context, string, int64) error { return g.RefundErr }

func (g StubGateway) GetCharge(context.Context, string) (payment.ChargeStatus, error) {
	if g.ChargeState != "" {
		return g.ChargeState, nil
	}
	if errors.Is(g.Err, payment.ErrDeclined) {
		return payment.ChargeStatusDeclined, nil
	}
	if g.Err == nil {
		return payment.ChargeStatusSucceeded, nil
	}
	return payment.ChargeStatusPending, nil
}

var _ payment.Gateway = StubGateway{}
