// Package services holds the use cases. Each one owns its transaction boundary.
package services

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/metrics"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
)

type OrderService struct{ pool *pgxpool.Pool }

func NewOrderService(pool *pgxpool.Pool) *OrderService { return &OrderService{pool: pool} }

type CreateOrderResult struct {
	Order    *order.Order
	Replayed bool // true when the response comes from an earlier identical request
}

// CreateOrder runs everything in one transaction: claim the idempotency key,
// reserve stock, insert the order and write the order.created outbox event.
// Either all of it commits or none of it does.
func (s *OrderService) CreateOrder(ctx context.Context, userID, idempotencyKey string, items []order.CreateItem) (*CreateOrderResult, error) {
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
	err = pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := repositories.New(tx)

		claimed, err := q.ClaimIdempotencyKey(ctx, userID, idempotencyKey, hash, orderID)
		if err != nil {
			return err
		}
		if !claimed {
			storedHash, existingID, err := q.GetIdempotencyKey(ctx, userID, idempotencyKey)
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

		ids := make([]string, len(norm))
		for i, it := range norm {
			ids[i] = it.ProductID
		}
		prices, err := q.GetProductPrices(ctx, ids)
		if err != nil {
			return err
		}

		o := &order.Order{
			ID: orderID, UserID: userID, Status: order.StatusPendingPayment,
			Items: make([]order.Item, 0, len(norm)),
		}
		// norm is sorted by product ID, so stock rows are always locked in the same order.
		for _, it := range norm {
			price, ok := prices[it.ProductID]
			if !ok {
				return fmt.Errorf("%w: %s", inventory.ErrUnknownProduct, it.ProductID)
			}
			reserved, err := q.ReserveStock(ctx, it.ProductID, it.Quantity)
			if err != nil {
				return err
			}
			if !reserved {
				return &inventory.InsufficientStockError{ProductID: it.ProductID, Requested: it.Quantity}
			}
			o.TotalCents += price * int64(it.Quantity)
			o.Items = append(o.Items, order.Item{ProductID: it.ProductID, Quantity: it.Quantity, UnitPriceCents: price})
		}

		if err := q.InsertOrder(ctx, o); err != nil {
			return err
		}
		if err := q.InsertOrderItems(ctx, o.ID, o.Items); err != nil {
			return err
		}
		if err := q.InsertOutbox(ctx, o.ID, order.EventCreated, order.Event{
			OrderID: o.ID, UserID: userID, TotalCents: o.TotalCents, OccurredAt: time.Now().UTC(),
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
func (s *OrderService) GetOrder(ctx context.Context, userID, id string) (*order.Order, error) {
	if _, err := uuid.Parse(id); err != nil {
		return nil, order.ErrNotFound
	}
	o, err := repositories.New(s.pool).GetOrder(ctx, id)
	if err != nil {
		return nil, err
	}
	if o.UserID != userID {
		return nil, order.ErrNotFound // do not reveal other users' orders
	}
	return o, nil
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
