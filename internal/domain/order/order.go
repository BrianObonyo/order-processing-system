// Package order holds the order aggregate: statuses, the allowed lifecycle
// transitions and request normalisation. It has no infrastructure dependencies.
package order

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

type Status string

const (
	StatusPendingPayment Status = "PENDING_PAYMENT"
	StatusPaid           Status = "PAID"
	StatusCancelled      Status = "CANCELLED"
)

var (
	ErrNotFound            = errors.New("order not found")
	ErrInvalidItems        = errors.New("invalid order items")
	ErrInvalidTransition   = errors.New("invalid order status transition")
	ErrIdempotencyMismatch = errors.New("idempotency key was already used with a different request")
)

var transitions = map[Status]map[Status]struct{}{
	StatusPendingPayment: {
		StatusPaid:      {},
		StatusCancelled: {},
	},
}

func (s Status) CanTransitionTo(to Status) bool {
	_, allowed := transitions[s][to]
	return allowed
}

func (s Status) IsTerminal() bool { return len(transitions[s]) == 0 }

type Item struct {
	ProductID      string `json:"product_id"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

type Order struct {
	ID           string    `json:"id"`
	TenantID     string    `json:"tenant_id"`
	UserID       string    `json:"user_id"`
	Status       Status    `json:"status"`
	TotalCents   int64     `json:"total_cents"`
	CancelReason string    `json:"cancel_reason,omitempty"`
	Items        []Item    `json:"items"`
	CreatedAt    time.Time `json:"created_at"`
	UpdatedAt    time.Time `json:"updated_at"`
}

type CreateItem struct {
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
}

const (
	MaxItems           = 50
	MaxQuantityPerItem = 100
)

// NormalizeItems validates, merges duplicate products and sorts by product ID.
// The sort order matters: stock rows are always locked in the same order, which
// rules out deadlocks between concurrent orders.
func NormalizeItems(in []CreateItem) ([]CreateItem, error) {
	if len(in) == 0 {
		return nil, fmt.Errorf("%w: at least one item is required", ErrInvalidItems)
	}
	if len(in) > MaxItems {
		return nil, fmt.Errorf("%w: at most %d items per order", ErrInvalidItems, MaxItems)
	}
	merged := make(map[string]int, len(in))
	for _, it := range in {
		if it.ProductID == "" {
			return nil, fmt.Errorf("%w: product_id is required", ErrInvalidItems)
		}
		if it.Quantity <= 0 {
			return nil, fmt.Errorf("%w: quantity must be positive", ErrInvalidItems)
		}
		merged[it.ProductID] += it.Quantity
		if merged[it.ProductID] > MaxQuantityPerItem {
			return nil, fmt.Errorf("%w: at most %d units per product", ErrInvalidItems, MaxQuantityPerItem)
		}
	}
	out := make([]CreateItem, 0, len(merged))
	for id, q := range merged {
		out = append(out, CreateItem{ProductID: id, Quantity: q})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ProductID < out[j].ProductID })
	return out, nil
}
