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

// Delivery is a received message plus the broker-specific handle needed to commit it.
type Delivery struct {
	Message Message
	Raw     any
}

type Consumer interface {
	Fetch(ctx context.Context) (Delivery, error)
	Commit(ctx context.Context, d Delivery) error
	Close() error
}
