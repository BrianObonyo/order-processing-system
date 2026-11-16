// Package messaging defines broker-agnostic types and the outbox relay.
package messaging

import "context"

type Message struct {
	Key     string // partition key: the aggregate (order) ID
	Type    string // event type, e.g. "order.created"
	Payload []byte
}

type Publisher interface {
	Publish(ctx context.Context, msgs []Message) error
}

// Delivery is a received message that can be acknowledged without exposing
// broker-specific offset or receipt types to consumers.
type Delivery interface {
	Message() Message
	Ack(ctx context.Context) error
}

type Consumer interface {
	Fetch(ctx context.Context) (Delivery, error)
	Close() error
}
