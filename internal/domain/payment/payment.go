package payment

import (
	"context"
	"errors"
)

type Status string

const (
	StatusProcessing Status = "PROCESSING"
	StatusSucceeded  Status = "SUCCEEDED"
	StatusFailed     Status = "FAILED"
	StatusRefunded   Status = "REFUNDED"
)

var (
	// ErrDeclined is permanent: retrying will not help.
	ErrDeclined = errors.New("payment declined")
	// ErrTransient is temporary: the charge may succeed if retried.
	ErrTransient = errors.New("transient payment gateway error")
)

type Payment struct {
	ID            string
	OrderID       string
	AmountCents   int64
	Status        Status
	FailureReason string
}

// Gateway charges a payment. Implementations must be idempotent on
// idempotencyKey: repeating a call with the same key never charges twice.
type Gateway interface {
	Charge(ctx context.Context, idempotencyKey string, amountCents int64) error
}
