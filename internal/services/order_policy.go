package services

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
)

type OrderPricingRequest struct {
	UserID       string
	Items        []order.CreateItem
	CatalogCents map[string]int64
}

type OrderPricingResult struct {
	Items      []order.Item
	TotalCents int64
}

// PricingPolicy runs inside the order transaction; implementations must use
// the supplied catalog data and avoid slow external calls.
type PricingPolicy interface {
	Price(context.Context, OrderPricingRequest) (OrderPricingResult, error)
}

type PricingFunc func(context.Context, OrderPricingRequest) (OrderPricingResult, error)

func (f PricingFunc) Price(ctx context.Context, request OrderPricingRequest) (OrderPricingResult, error) {
	return f(ctx, request)
}

type CatalogPricing struct{}

// Price receives catalog data inside the order transaction; implementations
// should calculate locally rather than make slow external calls.
func (CatalogPricing) Price(_ context.Context, request OrderPricingRequest) (OrderPricingResult, error) {
	result := OrderPricingResult{Items: make([]order.Item, 0, len(request.Items))}
	for _, item := range request.Items {
		price, ok := request.CatalogCents[item.ProductID]
		if !ok {
			return OrderPricingResult{}, fmt.Errorf("%w: %s", inventory.ErrUnknownProduct, item.ProductID)
		}
		if price < 0 || price > math.MaxInt64/int64(item.Quantity) {
			return OrderPricingResult{}, errors.New("invalid or overflowing catalog price")
		}
		lineTotal := price * int64(item.Quantity)
		if lineTotal > math.MaxInt64-result.TotalCents {
			return OrderPricingResult{}, errors.New("catalog order total overflows")
		}
		result.TotalCents += lineTotal
		result.Items = append(result.Items, order.Item{
			ProductID: item.ProductID, Quantity: item.Quantity, UnitPriceCents: price,
		})
	}
	return result, nil
}

func clonePrices(prices map[string]int64) map[string]int64 {
	cloned := make(map[string]int64, len(prices))
	for id, price := range prices {
		cloned[id] = price
	}
	return cloned
}

// OrderValidator runs inside the order transaction after idempotency is
// checked, so it should be fast and avoid external calls.
type OrderValidator interface {
	ValidateOrder(context.Context, string, []order.CreateItem) error
}

type OrderValidatorFunc func(context.Context, string, []order.CreateItem) error

func (f OrderValidatorFunc) ValidateOrder(ctx context.Context, userID string, items []order.CreateItem) error {
	return f(ctx, userID, items)
}

type OrderOption func(*OrderService)

func WithPricingPolicy(policy PricingPolicy) OrderOption {
	return func(service *OrderService) {
		if policy != nil {
			service.pricing = policy
		}
	}
}

func WithOrderValidator(validator OrderValidator) OrderOption {
	return func(service *OrderService) {
		if validator != nil {
			service.validator = validator
		}
	}
}

func validatePricing(items []order.CreateItem, result OrderPricingResult) error {
	if result.TotalCents < 0 {
		return errors.New("pricing policy returned a negative total")
	}
	expected := make(map[string]int, len(items))
	for _, item := range items {
		expected[item.ProductID] = item.Quantity
	}
	if len(result.Items) != len(expected) {
		return errors.New("pricing policy returned a different set of products")
	}
	for _, item := range result.Items {
		quantity, ok := expected[item.ProductID]
		if !ok || quantity != item.Quantity || item.UnitPriceCents < 0 {
			return errors.New("pricing policy returned invalid order items")
		}
		delete(expected, item.ProductID)
	}
	if len(expected) != 0 {
		return errors.New("pricing policy omitted order items")
	}
	return nil
}
