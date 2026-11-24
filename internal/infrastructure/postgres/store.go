package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func ValidateTenants(ctx context.Context, pool *pgxpool.Pool, keys map[string]identity.Identity) error {
	seen := make(map[string]struct{}, len(keys))
	for _, id := range keys {
		if _, ok := seen[id.TenantID]; ok {
			continue
		}
		var exists bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM tenants WHERE id = $1)`, id.TenantID).Scan(&exists); err != nil {
			return fmt.Errorf("validate configured tenant: %w", err)
		}
		if !exists {
			return fmt.Errorf("API key references unconfigured tenant %q", id.TenantID)
		}
		seen[id.TenantID] = struct{}{}
	}
	return nil
}

func (s *Store) WithinTransaction(ctx context.Context, fn func(services.Transaction) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(repositories.New(tx))
	})
}

func (s *Store) GetOrder(ctx context.Context, id string) (*order.Order, error) {
	return repositories.New(s.pool).GetOrder(ctx, id)
}

func (s *Store) GetOrderForTenant(ctx context.Context, tenantID, id string) (*order.Order, error) {
	return repositories.New(s.pool).GetOrderForTenant(ctx, tenantID, id)
}

func (s *Store) ListOrders(ctx context.Context, tenantID, userID string, status order.Status, limit, offset int) ([]*order.Order, int64, error) {
	return repositories.New(s.pool).ListOrders(ctx, tenantID, userID, status, limit, offset)
}

func (s *Store) ListProductStock(ctx context.Context, tenantID string) ([]inventory.ProductStock, error) {
	return repositories.New(s.pool).ListProductStock(ctx, tenantID)
}

func (s *Store) GetPaymentByID(ctx context.Context, id string) (*payment.Payment, error) {
	return repositories.New(s.pool).GetPaymentByID(ctx, id)
}

func (s *Store) TouchPendingRefund(ctx context.Context, id string) error {
	return repositories.New(s.pool).TouchPendingRefund(ctx, id)
}

func (s *Store) ListStaleRefunds(ctx context.Context, before time.Time, limit int) ([]string, error) {
	return repositories.New(s.pool).ListStaleRefunds(ctx, before, limit)
}

func (s *Store) ListExpiredOrders(ctx context.Context, before time.Time, limit int) ([]string, error) {
	return repositories.New(s.pool).ListExpiredOrders(ctx, before, limit)
}

func (s *Store) ListStalledOrders(ctx context.Context, before time.Time, retries, limit int) ([]string, error) {
	return repositories.New(s.pool).ListStalledOrders(ctx, before, retries, limit)
}

var _ services.Repository = (*Store)(nil)
var _ services.Transaction = (*repositories.Queries)(nil)
