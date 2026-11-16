// Package workers contains the long-running background loops.
package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/BrianObonyo/order-processing-system/internal/domain/event"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/messaging"
)

const deadLetterEventType = "message.dead_letter"

type PaymentProcessor interface {
	ProcessOrder(ctx context.Context, orderID string) error
	ProcessRefund(ctx context.Context, paymentID string) error
}

type PaymentConsumer struct {
	consumer messaging.Consumer
	payments PaymentProcessor
	dlq      messaging.Publisher
}

func NewPaymentConsumer(c messaging.Consumer, p PaymentProcessor, dlq messaging.Publisher) *PaymentConsumer {
	return &PaymentConsumer{consumer: c, payments: p, dlq: dlq}
}

func (w *PaymentConsumer) Run(ctx context.Context) error {
	for {
		d, err := w.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			slog.Error("payment consumer: fetch failed", "err", err)
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}

		message := d.Message()
		if err := w.handle(ctx, message); err != nil {
			if publishErr := w.publishDeadLetter(ctx, message, err); publishErr != nil {
				return fmt.Errorf("payment consumer: publish dead letter: %w", publishErr)
			}
			slog.Warn("payment consumer: malformed event sent to dead-letter topic", "event_type", message.Type, "err", err)
		}

		if err := d.Ack(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("payment consumer: acknowledge message: %w", err)
		}
	}
}

func (w *PaymentConsumer) handle(ctx context.Context, m messaging.Message) error {
	if m.Type != order.EventCreated && m.Type != order.EventPaymentRefundRequested {
		if m.Type == "" {
			return fmt.Errorf("message has no event_type")
		}
		return nil
	}

	var envelope event.Envelope
	if err := json.Unmarshal(m.Payload, &envelope); err != nil {
		return fmt.Errorf("decode event envelope: %w", err)
	}
	if err := envelope.Validate(m.Type); err != nil {
		return err
	}

	var ev order.Event
	if err := json.Unmarshal(envelope.Data, &ev); err != nil {
		return fmt.Errorf("decode event data: %w", err)
	}
	if ev.OrderID == "" || ev.OrderID != m.Key || ev.OrderID != envelope.CorrelationID {
		return fmt.Errorf("event order_id must match its key and correlation_id")
	}
	if m.Type == order.EventPaymentRefundRequested && ev.PaymentID == "" {
		return fmt.Errorf("refund event is missing payment_id")
	}

	var err error
	if m.Type == order.EventPaymentRefundRequested {
		err = w.payments.ProcessRefund(ctx, ev.PaymentID)
	} else {
		err = w.payments.ProcessOrder(ctx, ev.OrderID)
	}
	if err != nil && !errors.Is(err, order.ErrNotFound) && !errors.Is(err, payment.ErrNotFound) {
		// Payment and refund recovery re-emits durable work with a bounded delay.
		// Acknowledge this delivery instead of blocking its Kafka partition.
		slog.Warn("payment consumer: processing failed; durable recovery will retry",
			"order_id", ev.OrderID, "payment_id", ev.PaymentID, "err", err)
	}
	return nil
}

type deadLetterData struct {
	OriginalType    string `json:"original_event_type"`
	OriginalKey     string `json:"original_key"`
	OriginalPayload []byte `json:"original_payload"`
	FailureReason   string `json:"failure_reason"`
}

func (w *PaymentConsumer) publishDeadLetter(ctx context.Context, original messaging.Message, cause error) error {
	data, err := json.Marshal(deadLetterData{
		OriginalType: original.Type, OriginalKey: original.Key,
		OriginalPayload: original.Payload, FailureReason: cause.Error(),
	})
	if err != nil {
		return err
	}
	id := uuid.NewString()
	correlationID := original.Key
	if correlationID == "" {
		correlationID = id
	}
	envelope := event.Envelope{
		ID: id, Type: deadLetterEventType, SchemaVersion: event.CurrentSchemaVersion,
		CorrelationID: correlationID, OccurredAt: time.Now().UTC(), Data: data,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	return w.dlq.Publish(ctx, []messaging.Message{{
		Key: original.Key, Type: deadLetterEventType, Payload: payload,
	}})
}
