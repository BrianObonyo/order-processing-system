package services

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
)

type ProductService struct{ pool *pgxpool.Pool }

func NewProductService(pool *pgxpool.Pool) *ProductService { return &ProductService{pool: pool} }

func (s *ProductService) List(ctx context.Context) ([]inventory.ProductStock, error) {
	return repositories.New(s.pool).ListProductStock(ctx)
}
