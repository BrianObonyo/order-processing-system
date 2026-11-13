package services

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/metrics"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
)

type PaymentService struct {
	pool    *pgxpool.Pool
	gateway payment.Gateway
}

func NewPaymentService(pool *pgxpool.Pool, gw payment.Gateway) *PaymentService {
	return &PaymentService{pool: pool, gateway: gw}
}

var errSkip = errors.New("skip: order is not pending")

// ProcessOrder charges an order. It is safe to call any number of times for the
// same order (duplicate Kafka deliveries, recovery re-emits, worker restarts):
//
//  1. A payment row is created once per order (UNIQUE order_id).
//  2. The gateway is called outside any DB transaction, keyed by the payment ID,
//     so a repeated call can never charge twice.
//  3. The outcome is applied in one transaction that also moves the order and
//     the stock, guarded by a compare-and-set on the order status.
//
// A transient gateway failure returns an error without touching the order; the
// recovery worker re-emits the event later, and finally times the order out.
func (s *PaymentService) ProcessOrder(ctx context.Context, orderID string) error {
	var pay *payment.Payment
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := repositories.New(tx)
		o, err := q.LockOrder(ctx, orderID)
		if err != nil {
			return err
		}
		if o.Status != order.StatusPendingPayment {
			return errSkip
		}
		if err := q.InsertPayment(ctx, uuid.NewString(), orderID, o.TotalCents); err != nil {
			return err
		}
		pay, err = q.GetPaymentByOrder(ctx, orderID)
		return err
	})
	if errors.Is(err, errSkip) {
		return nil
	}
	if err != nil {
		return err
	}
	if pay.Status != payment.StatusProcessing {
		return nil
	}

	chargeErr := s.gateway.Charge(ctx, pay.ID, pay.AmountCents)
	switch {
	case chargeErr == nil:
		return s.finalizeSuccess(ctx, orderID, pay.ID)
	case errors.Is(chargeErr, payment.ErrDeclined):
		return s.finalizeFailure(ctx, orderID, "payment declined")
	default:
		metrics.PaymentsProcessed.WithLabelValues("transient_error").Inc()
		return chargeErr
	}
}

func (s *PaymentService) finalizeSuccess(ctx context.Context, orderID, paymentID string) error {
	refunded := false
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		q := repositories.New(tx)
		paid, err := markOrderPaid(ctx, q, orderID)
		if err != nil {
			return err
		}
		if paid {
			return q.SetPaymentResult(ctx, paymentID, payment.StatusSucceeded, "")
		}
		// The order was cancelled (e.g. payment timeout) while the charge was in
		// flight: compensate by refunding instead of keeping the money.
		refunded = true
		if err := q.SetPaymentResult(ctx, paymentID, payment.StatusRefunded, "order no longer pending"); err != nil {
			return err
		}
		return q.InsertOutbox(ctx, orderID, order.EventPaymentRefunded, order.Event{
			OrderID: orderID, Reason: "order no longer pending", OccurredAt: time.Now().UTC(),
		})
	})
	if err != nil {
		return err
	}
	if refunded {
		metrics.PaymentsProcessed.WithLabelValues("refunded").Inc()
	} else {
		metrics.PaymentsProcessed.WithLabelValues("succeeded").Inc()
	}
	return nil
}

func (s *PaymentService) finalizeFailure(ctx context.Context, orderID, reason string) error {
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		_, err := cancelPendingOrder(ctx, repositories.New(tx), orderID, reason)
		return err
	})
	if err == nil {
		metrics.PaymentsProcessed.WithLabelValues("declined").Inc()
	}
	return err
}
