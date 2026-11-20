package services

import (
	"context"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
)

type ProductService struct{ repository Repository }

func NewProductService(repository Repository) *ProductService {
	return &ProductService{repository: repository}
}

func (s *ProductService) List(ctx context.Context) ([]inventory.ProductStock, error) {
	return s.repository.ListProductStock(ctx)
}
