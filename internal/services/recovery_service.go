package services

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/metrics"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
)

// RecoveryService repairs orders that got stuck in PENDING_PAYMENT, for example
// because a consumer crashed or the payment gateway was down for a while.
type RecoveryService struct {
	pool        *pgxpool.Pool
	expireAfter time.Duration
	retryAfter  time.Duration
	maxRetries  int
}

func NewRecoveryService(pool *pgxpool.Pool, expireAfter, retryAfter time.Duration, maxRetries int) *RecoveryService {
	return &RecoveryService{pool: pool, expireAfter: expireAfter, retryAfter: retryAfter, maxRetries: maxRetries}
}

type SweepResult struct {
	Cancelled int
	Retried   int
}

// Sweep runs one recovery pass:
//  1. Compensation: cancel orders pending longer than expireAfter and release
//     their stock reservation.
//  2. Retry: re-emit order.created for orders idle longer than retryAfter that
//     still have retry budget.
//
// Every step re-checks the order under a row lock, so Sweep is safe to run
// concurrently with payment processing and on several instances at once.
func (s *RecoveryService) Sweep(ctx context.Context) (SweepResult, error) {
	var res SweepResult
	q := repositories.New(s.pool)

	expired, err := q.ListExpiredOrders(ctx, time.Now().Add(-s.expireAfter), 100)
	if err != nil {
		return res, err
	}
	for _, id := range expired {
		cancelled := false
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) (err error) {
			cancelled, err = cancelPendingOrder(ctx, repositories.New(tx), id, "payment timed out")
			return err
		})
		if err != nil {
			slog.Error("recovery: cancel failed", "order_id", id, "err", err)
			continue
		}
		if cancelled {
			res.Cancelled++
			metrics.RecoveryActions.WithLabelValues("cancelled").Inc()
			slog.Info("recovery: cancelled stale order", "order_id", id)
		}
	}

	stalled, err := q.ListStalledOrders(ctx, time.Now().Add(-s.retryAfter), s.maxRetries, 100)
	if err != nil {
		return res, err
	}
	for _, id := range stalled {
		retried := false
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			tq := repositories.New(tx)
			o, err := tq.LockOrder(ctx, id)
			if err != nil || o.Status != order.StatusPendingPayment {
				return err
			}
			if err := tq.BumpRecovery(ctx, id); err != nil {
				return err
			}
			retried = true
			return tq.InsertOutbox(ctx, id, order.EventCreated, order.Event{
				OrderID: id, UserID: o.UserID, TotalCents: o.TotalCents, OccurredAt: time.Now().UTC(),
			})
		})
		if err != nil {
			slog.Error("recovery: retry failed", "order_id", id, "err", err)
			continue
		}
		if retried {
			res.Retried++
			metrics.RecoveryActions.WithLabelValues("retried").Inc()
		}
	}
	return res, nil
}
