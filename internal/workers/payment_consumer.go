// Package workers contains the long-running background loops.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/messaging"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

// PaymentConsumer reads order and payment events and applies them idempotently.
type PaymentConsumer struct {
	consumer    messaging.Consumer
	payments    *services.PaymentService
	maxAttempts int
}

func NewPaymentConsumer(c messaging.Consumer, p *services.PaymentService) *PaymentConsumer {
	return &PaymentConsumer{consumer: c, payments: p, maxAttempts: 3}
}

func (w *PaymentConsumer) Run(ctx context.Context) {
	for {
		d, err := w.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("payment consumer: fetch failed", "err", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		w.handle(ctx, d.Message)

		// Commit after handling. If we crash before this line the event is
		// delivered again, and ProcessOrder is idempotent.
		if err := w.consumer.Commit(ctx, d); err != nil && ctx.Err() == nil {
			slog.Error("payment consumer: commit failed", "err", err)
		}
	}
}

func (w *PaymentConsumer) handle(ctx context.Context, m messaging.Message) {
	if m.Type != order.EventCreated && m.Type != order.EventPaymentRefundRequested {
		return // other event types are for other consumers
	}
	var ev order.Event
	if err := json.Unmarshal(m.Payload, &ev); err != nil {
		slog.Error("payment consumer: dropping malformed event", "err", err)
		return
	}
	if m.Type == order.EventPaymentRefundRequested && ev.PaymentID == "" {
		slog.Error("payment consumer: dropping refund event without payment_id", "order_id", ev.OrderID)
		return
	}

	backoff := 200 * time.Millisecond
	for attempt := 1; attempt <= w.maxAttempts; attempt++ {
		var err error
		if m.Type == order.EventPaymentRefundRequested {
			err = w.payments.ProcessRefund(ctx, ev.PaymentID)
		} else {
			err = w.payments.ProcessOrder(ctx, ev.OrderID)
		}
		if err == nil || errors.Is(err, order.ErrNotFound) || errors.Is(err, payment.ErrNotFound) {
			return
		}
		slog.Warn("payment consumer: processing failed", "order_id", ev.OrderID, "payment_id", ev.PaymentID, "attempt", attempt, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	// Give up for now. The recovery worker re-emits the event or times the order out.
	slog.Error("payment consumer: giving up, recovery will retry", "order_id", ev.OrderID, "payment_id", ev.PaymentID)
}
