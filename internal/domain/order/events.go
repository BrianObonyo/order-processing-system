package order

const (
	EventCreated                = "order.created"
	EventPaid                   = "order.paid"
	EventCancelled              = "order.cancelled"
	EventPaymentRefundRequested = "payment.refund_requested"
	EventPaymentRefunded        = "payment.refunded"
)

// Event is the payload written to the outbox and published to Kafka.
// Consumers must treat delivery as at-least-once.
type Event struct {
	OrderID    string `json:"order_id"`
	TenantID   string `json:"tenant_id"`
	PaymentID  string `json:"payment_id,omitempty"`
	UserID     string `json:"user_id"`
	TotalCents int64  `json:"total_cents"`
	Reason     string `json:"reason,omitempty"`
}
