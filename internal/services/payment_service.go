package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/metrics"
)

type PaymentService struct {
	repository Repository
	gateway    payment.Gateway
}

func NewPaymentService(repository Repository, gw payment.Gateway) *PaymentService {
	return &PaymentService{repository: repository, gateway: gw}
}

var errSkip = errors.New("skip: order is not pending")

// ProcessOrder charges an order. It is safe to call any number of times for the
// same order (duplicate Kafka deliveries, recovery re-emits, worker restarts):
//
//  1. A payment attempt is created once for each explicit retry.
//  2. The gateway is called outside any DB transaction, keyed by the payment ID,
//     so a repeated call can never charge twice.
//  3. The outcome is applied in one transaction that also moves the order and
//     the stock, guarded by a compare-and-set on the order status.
//
// A transient gateway failure returns an error without touching the order; the
// recovery worker re-emits the event later, and finally times the order out.
func (s *PaymentService) ProcessOrder(ctx context.Context, orderID string) error {
	var pay *payment.Payment
	err := s.repository.WithinTransaction(ctx, func(q Transaction) error {
		o, err := q.LockOrder(ctx, orderID)
		if err != nil {
			return err
		}
		if o.Status != order.StatusPendingPayment {
			return errSkip
		}
		pay, err = q.GetLatestPaymentAttemptByOrder(ctx, o.TenantID, orderID)
		if errors.Is(err, payment.ErrNotFound) {
			if err := q.InsertPaymentAttempt(ctx, uuid.NewString(), o.TenantID, orderID, o.TotalCents); err != nil {
				return err
			}
			pay, err = q.GetLatestPaymentAttemptByOrder(ctx, o.TenantID, orderID)
		}
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
		return s.finalizeFailure(ctx, pay.ID, "payment declined")
	default:
		metrics.PaymentsProcessed.WithLabelValues("transient_error").Inc()
		return chargeErr
	}
}

// RetryPayment starts a new attempt after a permanent decline. The order stays
// pending, and the previous attempt remains available for audit.
func (s *PaymentService) RetryPayment(ctx context.Context, actor identity.Identity, orderID string) (*payment.Payment, error) {
	if !actor.IsValid() {
		return nil, identity.ErrInvalid
	}
	if actor.Role != identity.RoleAdmin {
		return nil, identity.ErrForbidden
	}
	var attempt *payment.Payment
	err := s.repository.WithinTransaction(ctx, func(q Transaction) error {
		o, err := q.LockOrder(ctx, orderID)
		if err != nil {
			return err
		}
		if o.TenantID != actor.TenantID {
			return order.ErrNotFound
		}
		if o.Status != order.StatusPendingPayment {
			return payment.ErrNoRetryableAttempt
		}
		previous, err := q.GetLatestPaymentAttemptByOrder(ctx, actor.TenantID, orderID)
		if errors.Is(err, payment.ErrNotFound) {
			return payment.ErrNoRetryableAttempt
		}
		if err != nil {
			return err
		}
		if previous.Status != payment.StatusFailed {
			return payment.ErrNoRetryableAttempt
		}
		if err := q.InsertPaymentAttempt(ctx, uuid.NewString(), actor.TenantID, orderID, o.TotalCents); err != nil {
			return err
		}
		attempt, err = q.GetLatestPaymentAttemptByOrder(ctx, actor.TenantID, orderID)
		if err != nil {
			return err
		}
		return q.InsertOutbox(ctx, orderID, order.EventCreated, order.Event{
			OrderID: orderID, TenantID: o.TenantID, UserID: o.UserID, TotalCents: o.TotalCents,
		})
	})
	if err != nil {
		return nil, err
	}
	return attempt, nil
}

func (s *PaymentService) finalizeSuccess(ctx context.Context, orderID, paymentID string) error {
	refundPending := false
	err := s.repository.WithinTransaction(ctx, func(q Transaction) error {
		o, err := q.LockOrder(ctx, orderID)
		if err != nil {
			return err
		}
		switch o.Status {
		case order.StatusPendingPayment:
			paid, err := markOrderPaid(ctx, q, orderID)
			if err != nil {
				return err
			}
			if !paid {
				return nil
			}
			return q.SetPaymentResult(ctx, paymentID, payment.StatusSucceeded, "")
		case order.StatusPaid:
			return q.SetPaymentResult(ctx, paymentID, payment.StatusSucceeded, "")
		}
		// The order was cancelled (e.g. payment timeout) while the charge was in
		// flight: durably request a refund instead of keeping the money.
		refundPending = true
		pay, err := q.GetPaymentByID(ctx, paymentID)
		if err != nil {
			return err
		}
		if pay.Status == payment.StatusRefunded || pay.Status == payment.StatusRefundPending {
			return nil
		}
		changed, err := q.TransitionPaymentStatus(ctx, paymentID, pay.Status, payment.StatusRefundPending, "order no longer pending")
		if err != nil {
			return err
		}
		if !changed {
			pay, err = q.GetPaymentByID(ctx, paymentID)
			if err != nil {
				return err
			}
			if pay.Status == payment.StatusRefunded || pay.Status == payment.StatusRefundPending {
				return nil
			}
			return fmt.Errorf("payment %s changed state while scheduling refund", paymentID)
		}
		return q.InsertOutbox(ctx, orderID, order.EventPaymentRefundRequested, order.Event{
			OrderID: orderID, TenantID: o.TenantID, PaymentID: paymentID, TotalCents: pay.AmountCents,
			Reason: "order no longer pending",
		})
	})
	if err != nil {
		return err
	}
	if refundPending {
		return s.ProcessRefund(ctx, paymentID)
	}
	metrics.PaymentsProcessed.WithLabelValues("succeeded").Inc()
	return nil
}

// ProcessRefund performs a durable refund request. The provider operation is
// idempotent on paymentID, so a retry after a crash cannot refund twice.
func (s *PaymentService) ProcessRefund(ctx context.Context, paymentID string) error {
	pay, err := s.repository.GetPaymentByID(ctx, paymentID)
	if err != nil {
		return err
	}
	if pay.Status == payment.StatusRefunded {
		return nil
	}
	if pay.Status != payment.StatusRefundPending {
		return nil
	}
	if err := s.gateway.Refund(ctx, pay.ID, pay.AmountCents); err != nil {
		if touchErr := s.repository.TouchPendingRefund(ctx, paymentID); touchErr != nil {
			return errors.Join(err, touchErr)
		}
		return err
	}

	refunded := false
	err = s.repository.WithinTransaction(ctx, func(q Transaction) error {
		refunded, err = q.TransitionPaymentStatus(ctx, paymentID, payment.StatusRefundPending, payment.StatusRefunded, "")
		if err != nil || !refunded {
			return err
		}
		return q.InsertOutbox(ctx, pay.OrderID, order.EventPaymentRefunded, order.Event{
			OrderID: pay.OrderID, TenantID: pay.TenantID, PaymentID: pay.ID, TotalCents: pay.AmountCents,
			Reason: "order no longer pending",
		})
	})
	if err != nil {
		return err
	}
	if refunded {
		metrics.PaymentsProcessed.WithLabelValues("refunded").Inc()
	}
	return nil
}

// ReconcileExpiredOrder queries the provider before recovery cancels an order.
// It returns the order's resulting status; a pending status means the provider
// could not yet establish a final charge outcome and cancellation must wait.
func (s *PaymentService) ReconcileExpiredOrder(ctx context.Context, orderID string) (order.Status, error) {
	var pay *payment.Payment
	var currentStatus order.Status
	err := s.repository.WithinTransaction(ctx, func(q Transaction) error {
		o, err := q.LockOrder(ctx, orderID)
		if err != nil {
			return err
		}
		currentStatus = o.Status
		if o.Status != order.StatusPendingPayment {
			return nil
		}

		pay, err = q.GetLatestPaymentAttemptByOrder(ctx, o.TenantID, orderID)
		if errors.Is(err, payment.ErrNotFound) {
			cancelled, cancelErr := cancelPendingOrder(ctx, q, orderID, "payment timed out")
			if cancelled {
				currentStatus = order.StatusCancelled
			}
			return cancelErr
		}
		if err != nil {
			return err
		}

		switch pay.Status {
		case payment.StatusProcessing:
			_, err = q.TransitionPaymentStatus(ctx, pay.ID, payment.StatusProcessing, payment.StatusReconciling, "")
		case payment.StatusReconciling:
			return nil
		case payment.StatusFailed:
			cancelled, cancelErr := cancelPendingOrder(ctx, q, orderID, "payment failed")
			if cancelled {
				currentStatus = order.StatusCancelled
			}
			return cancelErr
		case payment.StatusSucceeded:
			paid, markErr := markOrderPaid(ctx, q, orderID)
			if markErr == nil && paid {
				currentStatus = order.StatusPaid
			}
			return markErr
		default:
			return nil
		}
		return err
	})
	if err != nil || currentStatus != order.StatusPendingPayment || pay == nil {
		return currentStatus, err
	}

	chargeStatus, err := s.gateway.GetCharge(ctx, pay.ID)
	if err != nil {
		return currentStatus, err
	}
	switch chargeStatus {
	case payment.ChargeStatusSucceeded:
		if err := s.finalizeSuccess(ctx, orderID, pay.ID); err != nil {
			return currentStatus, err
		}
		return s.currentOrderStatus(ctx, orderID)
	case payment.ChargeStatusDeclined, payment.ChargeStatusNotFound:
		err = s.repository.WithinTransaction(ctx, func(q Transaction) error {
			_, err := cancelPendingOrder(ctx, q, orderID, "payment timed out")
			return err
		})
		if err != nil {
			return currentStatus, err
		}
		return s.currentOrderStatus(ctx, orderID)
	case payment.ChargeStatusPending:
		return currentStatus, nil
	default:
		return currentStatus, fmt.Errorf("gateway returned unsupported charge status %q", chargeStatus)
	}
}

func (s *PaymentService) currentOrderStatus(ctx context.Context, orderID string) (order.Status, error) {
	o, err := s.repository.GetOrder(ctx, orderID)
	if err != nil {
		return "", err
	}
	return o.Status, nil
}

// RequeuePendingRefunds republishes durable refund requests after their retry
// delay. The payment's updated_at timestamp bounds retries when the provider is
// unavailable or a refund event cannot be processed.
func (s *PaymentService) RequeuePendingRefunds(ctx context.Context, retryAfter time.Duration) (int, error) {
	ids, err := s.repository.ListStaleRefunds(ctx, time.Now().Add(-retryAfter), 100)
	if err != nil {
		return 0, err
	}
	requeued := 0
	for _, id := range ids {
		scheduled := false
		err := s.repository.WithinTransaction(ctx, func(tq Transaction) error {
			claimed, err := tq.ScheduleRefundRetry(ctx, id, time.Now().Add(-retryAfter))
			if err != nil || !claimed {
				return err
			}
			pay, err := tq.GetPaymentByID(ctx, id)
			if err != nil {
				return err
			}
			if err := tq.InsertOutbox(ctx, pay.OrderID, order.EventPaymentRefundRequested, order.Event{
				OrderID: pay.OrderID, TenantID: pay.TenantID, PaymentID: pay.ID, TotalCents: pay.AmountCents,
				Reason: pay.FailureReason,
			}); err != nil {
				return err
			}
			scheduled = true
			return nil
		})
		if err != nil {
			return requeued, err
		}
		if scheduled {
			requeued++
		}
	}
	return requeued, nil
}

func (s *PaymentService) finalizeFailure(ctx context.Context, paymentID, reason string) error {
	err := s.repository.WithinTransaction(ctx, func(q Transaction) error {
		pay, err := q.GetPaymentByID(ctx, paymentID)
		if err != nil {
			return err
		}
		if pay.Status != payment.StatusProcessing && pay.Status != payment.StatusReconciling {
			return nil
		}
		_, err = q.TransitionPaymentStatus(ctx, paymentID, pay.Status, payment.StatusFailed, reason)
		return err
	})
	if err == nil {
		metrics.PaymentsProcessed.WithLabelValues("declined").Inc()
	}
	return err
}
