package workers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/event"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/messaging"
)

func TestPaymentConsumerProcessesEnvelopedOrderOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	processor := &stubPaymentProcessor{}
	delivery := &stubDelivery{
		message: envelopedMessage(t, order.EventCreated, order.Event{OrderID: "order-1"}),
		ack:     cancel,
	}
	consumer := &stubConsumer{delivery: delivery}
	consumerWorker := NewPaymentConsumer(consumer, processor, &stubPublisher{})

	if err := consumerWorker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(processor.orders) != 1 || processor.orders[0] != "order-1" {
		t.Fatalf("processed orders=%v, want [order-1]", processor.orders)
	}
	if delivery.acks != 1 {
		t.Fatalf("acknowledgements=%d, want 1", delivery.acks)
	}
}

func TestPaymentConsumerUsesDurableRecoveryInsteadOfInPlaceRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	processor := &stubPaymentProcessor{err: errors.New("temporary payment error")}
	delivery := &stubDelivery{
		message: envelopedMessage(t, order.EventCreated, order.Event{OrderID: "order-2"}),
		ack:     cancel,
	}
	consumerWorker := NewPaymentConsumer(&stubConsumer{delivery: delivery}, processor, &stubPublisher{})

	if err := consumerWorker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if len(processor.orders) != 1 {
		t.Fatalf("processing calls=%d, want one attempt before durable recovery", len(processor.orders))
	}
	if delivery.acks != 1 {
		t.Fatalf("acknowledgements=%d, want 1", delivery.acks)
	}
}

func TestPaymentConsumerDeadLettersMalformedEventBeforeAcknowledging(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	message := messaging.Message{Type: order.EventCreated, Payload: []byte("{")}
	delivery := &stubDelivery{message: message, ack: cancel}
	publisher := &stubPublisher{}
	consumerWorker := NewPaymentConsumer(&stubConsumer{delivery: delivery}, &stubPaymentProcessor{}, publisher)

	if err := consumerWorker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if delivery.acks != 1 || len(publisher.messages) != 1 {
		t.Fatalf("acks=%d dead letters=%d, want 1 each", delivery.acks, len(publisher.messages))
	}
	var envelope event.Envelope
	if err := json.Unmarshal(publisher.messages[0].Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(deadLetterEventType); err != nil {
		t.Fatalf("dead-letter envelope invalid: %v", err)
	}
}

func TestPaymentConsumerDoesNotAcknowledgeWhenDeadLetterPublishFails(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery := &stubDelivery{
		message: messaging.Message{Key: "order-4", Type: order.EventCreated, Payload: []byte("{")},
	}
	consumerWorker := NewPaymentConsumer(
		&stubConsumer{delivery: delivery}, &stubPaymentProcessor{}, &stubPublisher{err: errors.New("broker unavailable")},
	)

	if err := consumerWorker.Run(ctx); err == nil {
		t.Fatal("Run() error = nil, want dead-letter publish error")
	}
	if delivery.acks != 0 {
		t.Fatalf("acknowledgements=%d, want 0", delivery.acks)
	}
}

func envelopedMessage(t *testing.T, typ string, data order.Event) messaging.Message {
	t.Helper()
	payload, err := json.Marshal(data)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := json.Marshal(event.Envelope{
		ID: "event-1", Type: typ, SchemaVersion: event.CurrentSchemaVersion,
		CorrelationID: data.OrderID, OccurredAt: time.Now().UTC(), Data: payload,
	})
	if err != nil {
		t.Fatal(err)
	}
	return messaging.Message{Key: data.OrderID, Type: typ, Payload: envelope}
}

type stubPaymentProcessor struct {
	orders  []string
	refunds []string
	err     error
}

func (s *stubPaymentProcessor) ProcessOrder(_ context.Context, id string) error {
	s.orders = append(s.orders, id)
	return s.err
}

func (s *stubPaymentProcessor) ProcessRefund(_ context.Context, id string) error {
	s.refunds = append(s.refunds, id)
	return s.err
}

type stubConsumer struct {
	delivery messaging.Delivery
}

func (s *stubConsumer) Fetch(ctx context.Context) (messaging.Delivery, error) {
	if s.delivery != nil {
		d := s.delivery
		s.delivery = nil
		return d, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*stubConsumer) Close() error { return nil }

type stubDelivery struct {
	message messaging.Message
	acks    int
	ack     context.CancelFunc
}

func (s *stubDelivery) Message() messaging.Message { return s.message }

func (s *stubDelivery) Ack(context.Context) error {
	s.acks++
	if s.ack != nil {
		s.ack()
	}
	return nil
}

type stubPublisher struct {
	messages []messaging.Message
	err      error
}

func (s *stubPublisher) Publish(_ context.Context, messages []messaging.Message) error {
	if s.err != nil {
		return s.err
	}
	s.messages = append(s.messages, messages...)
	return nil
}
