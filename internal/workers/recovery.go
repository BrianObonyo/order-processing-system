package workers

import (
	"context"
	"log/slog"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/services"
)

// Locker is the coordination primitive (backed by Redis). It only keeps several
// instances from sweeping at once; correctness never depends on it.
type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (release func(), ok bool, err error)
}

type RecoveryWorker struct {
	svc      *services.RecoveryService
	locker   Locker
	interval time.Duration
}

func NewRecoveryWorker(svc *services.RecoveryService, locker Locker, interval time.Duration) *RecoveryWorker {
	return &RecoveryWorker{svc: svc, locker: locker, interval: interval}
}

func (w *RecoveryWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.tick(ctx)
		}
	}
}

func (w *RecoveryWorker) tick(ctx context.Context) {
	if w.locker != nil {
		release, ok, err := w.locker.Acquire(ctx, "recovery:leader", w.interval*2)
		switch {
		case err != nil:
			slog.Warn("recovery: lock unavailable, sweeping anyway", "err", err) // fail open
		case !ok:
			return // another instance is sweeping
		default:
			defer release()
		}
	}
	res, err := w.svc.Sweep(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("recovery sweep failed", "err", err)
		}
		return
	}
	if res.Cancelled > 0 || res.Retried > 0 {
		slog.Info("recovery sweep", "cancelled", res.Cancelled, "retried", res.Retried)
	}
}
