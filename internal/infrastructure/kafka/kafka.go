package kafka

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/BrianObonyo/order-processing-system/internal/messaging"
)

const headerEventType = "event_type"

// EnsureTopic creates the topic if needed, retrying while the broker boots.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		if lastErr = createTopic(ctx, brokers[0], topic, partitions); lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return fmt.Errorf("ensure topic %q: %w", topic, lastErr)
}

func createTopic(ctx context.Context, broker, topic string, partitions int) error {
	conn, err := (&kafkago.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	cc, err := (&kafkago.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "tcp",
		net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer cc.Close()
	return cc.CreateTopics(kafkago.TopicConfig{Topic: topic, NumPartitions: partitions, ReplicationFactor: 1})
}

// Producer implements messaging.Publisher.
type Producer struct{ w *kafkago.Writer }

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{w: &kafkago.Writer{
		Addr:         kafkago.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafkago.Hash{}, // same key -> same partition -> per-order ordering
		RequiredAcks: kafkago.RequireAll,
		BatchTimeout: 10 * time.Millisecond,
	}}
}

func (p *Producer) Publish(ctx context.Context, msgs []messaging.Message) error {
	out := make([]kafkago.Message, len(msgs))
	for i, m := range msgs {
		out[i] = kafkago.Message{
			Key:     []byte(m.Key),
			Value:   m.Payload,
			Headers: []kafkago.Header{{Key: headerEventType, Value: []byte(m.Type)}},
		}
	}
	return p.w.WriteMessages(ctx, out...)
}

func (p *Producer) Close() error { return p.w.Close() }

// Consumer implements messaging.Consumer with manual offset commits.
type Consumer struct{ r *kafkago.Reader }

func NewConsumer(brokers []string, topic, groupID string) *Consumer {
	return &Consumer{r: kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     groupID,
		MinBytes:    1,
		MaxBytes:    10 << 20,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafkago.FirstOffset,
	})}
}

func (c *Consumer) Fetch(ctx context.Context) (messaging.Delivery, error) {
	m, err := c.r.FetchMessage(ctx)
	if err != nil {
		return messaging.Delivery{}, err
	}
	var typ string
	for _, h := range m.Headers {
		if h.Key == headerEventType {
			typ = string(h.Value)
		}
	}
	return messaging.Delivery{
		Message: messaging.Message{Key: string(m.Key), Type: typ, Payload: m.Value},
		Raw:     m,
	}, nil
}

func (c *Consumer) Commit(ctx context.Context, d messaging.Delivery) error {
	m, ok := d.Raw.(kafkago.Message)
	if !ok {
		return errors.New("kafka commit: unexpected delivery handle")
	}
	return c.r.CommitMessages(ctx, m)
}

func (c *Consumer) Close() error { return c.r.Close() }
