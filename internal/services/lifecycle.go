package services

import (
	"context"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
)

// The two terminal transitions live here so that payment completion and
// recovery cancellation use exactly the same, race-safe code path. Both lock the
// order row first and re-check its status, so whichever transaction gets the
// lock first wins and the other becomes a no-op. Callers must pass a Queries
// bound to a transaction.

// cancelPendingOrder releases the stock reservation and cancels the order.
// It returns false when the order was no longer pending (nothing changed).
func cancelPendingOrder(ctx context.Context, q *repositories.Queries, orderID, reason string) (bool, error) {
	o, err := q.LockOrder(ctx, orderID)
	if err != nil {
		return false, err
	}
	if o.Status != order.StatusPendingPayment {
		return false, nil
	}
	items, err := q.GetOrderItems(ctx, orderID)
	if err != nil {
		return false, err
	}
	for _, it := range items {
		if err := q.ReleaseStock(ctx, it.ProductID, it.Quantity); err != nil {
			return false, err
		}
	}
	changed, err := q.SetOrderStatus(ctx, orderID, order.StatusPendingPayment, order.StatusCancelled, reason)
	if err != nil || !changed {
		return false, err
	}
	if err := q.FailProcessingPayment(ctx, orderID, reason); err != nil {
		return false, err
	}
	err = q.InsertOutbox(ctx, orderID, order.EventCancelled, order.Event{
		OrderID: orderID, UserID: o.UserID, TotalCents: o.TotalCents, Reason: reason, OccurredAt: time.Now().UTC(),
	})
	return err == nil, err
}

// markOrderPaid converts the reservation into a real stock deduction and marks
// the order paid. It returns false when the order was no longer pending.
func markOrderPaid(ctx context.Context, q *repositories.Queries, orderID string) (bool, error) {
	o, err := q.LockOrder(ctx, orderID)
	if err != nil {
		return false, err
	}
	if o.Status != order.StatusPendingPayment {
		return false, nil
	}
	items, err := q.GetOrderItems(ctx, orderID)
	if err != nil {
		return false, err
	}
	for _, it := range items {
		if err := q.CommitStock(ctx, it.ProductID, it.Quantity); err != nil {
			return false, err
		}
	}
	changed, err := q.SetOrderStatus(ctx, orderID, order.StatusPendingPayment, order.StatusPaid, "")
	if err != nil || !changed {
		return false, err
	}
	err = q.InsertOutbox(ctx, orderID, order.EventPaid, order.Event{
		OrderID: orderID, UserID: o.UserID, TotalCents: o.TotalCents, OccurredAt: time.Now().UTC(),
	})
	return err == nil, err
}
