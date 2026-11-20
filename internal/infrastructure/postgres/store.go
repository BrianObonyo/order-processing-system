package postgres

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

type Store struct{ pool *pgxpool.Pool }

func NewStore(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) WithinTransaction(ctx context.Context, fn func(services.Transaction) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return fn(repositories.New(tx))
	})
}

func (s *Store) GetOrder(ctx context.Context, id string) (*order.Order, error) {
	return repositories.New(s.pool).GetOrder(ctx, id)
}

func (s *Store) ListOrders(ctx context.Context, userID string, limit, offset int) ([]*order.Order, int64, error) {
	return repositories.New(s.pool).ListOrders(ctx, userID, limit, offset)
}

func (s *Store) ListProductStock(ctx context.Context) ([]inventory.ProductStock, error) {
	return repositories.New(s.pool).ListProductStock(ctx)
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
