// Package services holds the use cases. Each one owns its transaction boundary.
package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/google/uuid"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/metrics"
)

type OrderService struct {
	repository Repository
	pricing    PricingPolicy
	validator  OrderValidator
}

func NewOrderService(repository Repository, options ...OrderOption) *OrderService {
	service := &OrderService{repository: repository, pricing: CatalogPricing{}}
	for _, option := range options {
		option(service)
	}
	return service
}

type CreateOrderResult struct {
	Order    *order.Order
	Replayed bool // true when the response comes from an earlier identical request
}

type OrderPage struct {
	Orders   []*order.Order `json:"orders"`
	Page     int            `json:"page"`
	PageSize int            `json:"page_size"`
	Total    int64          `json:"total"`
}

var (
	ErrInvalidPagination  = errors.New("invalid pagination")
	ErrOrderNotCancelable = errors.New("order cannot be cancelled in its current state")
)

// CreateOrder runs everything in one transaction: claim the idempotency key,
// reserve stock, insert the order and write the order.created outbox event.
// Either all of it commits or none of it does.
func (s *OrderService) CreateOrder(ctx context.Context, actor identity.Identity, idempotencyKey string, items []order.CreateItem) (*CreateOrderResult, error) {
	if !actor.IsValid() {
		return nil, identity.ErrInvalid
	}
	norm, err := order.NormalizeItems(items)
	if err != nil {
		metrics.OrdersRejected.WithLabelValues("invalid").Inc()
		return nil, err
	}
	hash, err := hashRequest(norm)
	if err != nil {
		return nil, err
	}
	orderID := uuid.NewString()

	var result *CreateOrderResult
	err = s.repository.WithinTransaction(ctx, func(q Transaction) error {
		claimed, err := q.ClaimIdempotencyKey(ctx, actor.TenantID, actor.UserID, idempotencyKey, hash, orderID)
		if err != nil {
			return err
		}
		if !claimed {
			storedHash, existingID, err := q.GetIdempotencyKey(ctx, actor.TenantID, actor.UserID, idempotencyKey)
			if err != nil {
				return err
			}
			if storedHash != hash {
				return order.ErrIdempotencyMismatch
			}
			existing, err := q.GetOrder(ctx, existingID)
			if err != nil {
				return err
			}
			result = &CreateOrderResult{Order: existing, Replayed: true}
			return nil
		}
		if s.validator != nil {
			if err := s.validator.ValidateOrder(ctx, actor.UserID, append([]order.CreateItem(nil), norm...)); err != nil {
				return err
			}
		}

		ids := make([]string, len(norm))
		for i, it := range norm {
			ids[i] = it.ProductID
		}
		prices, err := q.GetProductPrices(ctx, actor.TenantID, ids)
		if err != nil {
			return err
		}

		priced, err := s.pricing.Price(ctx, OrderPricingRequest{
			UserID: actor.UserID, Items: append([]order.CreateItem(nil), norm...), CatalogCents: clonePrices(prices),
		})
		if err != nil {
			return err
		}
		if err := validatePricing(norm, priced); err != nil {
			return err
		}

		o := &order.Order{
			ID: orderID, TenantID: actor.TenantID, UserID: actor.UserID, Status: order.StatusPendingPayment,
			Items: priced.Items, TotalCents: priced.TotalCents,
		}
		// norm is sorted by product ID, so stock rows are always locked in the same order.
		for _, it := range norm {
			reserved, err := q.ReserveStock(ctx, actor.TenantID, it.ProductID, it.Quantity)
			if err != nil {
				return err
			}
			if !reserved {
				return &inventory.InsufficientStockError{ProductID: it.ProductID, Requested: it.Quantity}
			}
		}

		if err := q.InsertOrder(ctx, o); err != nil {
			return err
		}
		if err := q.InsertOrderItems(ctx, actor.TenantID, o.ID, o.Items); err != nil {
			return err
		}
		if err := q.InsertOutbox(ctx, o.ID, order.EventCreated, order.Event{
			OrderID: o.ID, TenantID: actor.TenantID, UserID: actor.UserID, TotalCents: o.TotalCents,
		}); err != nil {
			return err
		}

		created, err := q.GetOrder(ctx, o.ID)
		if err != nil {
			return err
		}
		result = &CreateOrderResult{Order: created}
		return nil
	})
	if err != nil {
		recordRejection(err)
		return nil, err
	}

	if result.Replayed {
		metrics.IdempotentReplays.Inc()
	} else {
		metrics.OrdersCreated.Inc()
	}
	return result, nil
}

// GetOrder returns an order only if it belongs to userID.
func (s *OrderService) GetOrder(ctx context.Context, actor identity.Identity, id string) (*order.Order, error) {
	if !actor.IsValid() {
		return nil, identity.ErrInvalid
	}
	if _, err := uuid.Parse(id); err != nil {
		return nil, order.ErrNotFound
	}
	o, err := s.repository.GetOrderForTenant(ctx, actor.TenantID, id)
	if err != nil {
		return nil, err
	}
	if actor.Role != identity.RoleAdmin && o.UserID != actor.UserID {
		return nil, order.ErrNotFound // do not reveal other users' orders
	}
	return o, nil
}

func (s *OrderService) ListOrders(ctx context.Context, actor identity.Identity, page, pageSize int, status order.Status) (*OrderPage, error) {
	if !actor.IsValid() {
		return nil, identity.ErrInvalid
	}
	if page < 1 || pageSize < 1 || pageSize > 100 || page-1 > int(^uint(0)>>1)/pageSize {
		return nil, ErrInvalidPagination
	}
	if status != "" {
		if _, err := order.ParseStatus(string(status)); err != nil {
			return nil, err
		}
	}
	offset := (page - 1) * pageSize
	userID := actor.UserID
	if actor.Role == identity.RoleAdmin {
		userID = ""
	}
	orders, total, err := s.repository.ListOrders(ctx, actor.TenantID, userID, status, pageSize, offset)
	if err != nil {
		return nil, err
	}
	if orders == nil {
		orders = []*order.Order{}
	}
	return &OrderPage{Orders: orders, Page: page, PageSize: pageSize, Total: total}, nil
}

func (s *OrderService) CancelOrder(ctx context.Context, actor identity.Identity, id string) error {
	if !actor.IsValid() {
		return identity.ErrInvalid
	}
	if _, err := uuid.Parse(id); err != nil {
		return order.ErrNotFound
	}
	return s.repository.WithinTransaction(ctx, func(tx Transaction) error {
		o, err := tx.LockOrder(ctx, id)
		if err != nil {
			return err
		}
		if o.TenantID != actor.TenantID || (actor.Role != identity.RoleAdmin && o.UserID != actor.UserID) {
			return order.ErrNotFound
		}
		if o.Status != order.StatusPendingPayment {
			return ErrOrderNotCancelable
		}
		reason := "cancelled by customer"
		if actor.Role == identity.RoleAdmin {
			reason = "cancelled by tenant admin"
		}
		cancelled, err := cancelLockedPendingOrder(ctx, tx, o, reason)
		if err != nil {
			return err
		}
		if !cancelled {
			return ErrOrderNotCancelable
		}
		return nil
	})
}

func recordRejection(err error) {
	var stock *inventory.InsufficientStockError
	switch {
	case asError(err, &stock):
		metrics.OrdersRejected.WithLabelValues("insufficient_stock").Inc()
	case isErr(err, inventory.ErrUnknownProduct):
		metrics.OrdersRejected.WithLabelValues("unknown_product").Inc()
	case isErr(err, order.ErrIdempotencyMismatch):
		metrics.OrdersRejected.WithLabelValues("idempotency_mismatch").Inc()
	}
}

func hashRequest(items []order.CreateItem) (string, error) {
	b, err := json.Marshal(items)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
