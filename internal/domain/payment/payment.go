package payment

import (
	"context"
	"errors"
)

type Status string

const (
	StatusProcessing    Status = "PROCESSING"
	StatusReconciling   Status = "RECONCILING"
	StatusSucceeded     Status = "SUCCEEDED"
	StatusFailed        Status = "FAILED"
	StatusRefundPending Status = "REFUND_PENDING"
	StatusRefunded      Status = "REFUNDED"
)

type ChargeStatus string

const ProviderSimulated = "simulated"
const ProviderPayPal = "paypal"

const (
	ChargeStatusNotFound  ChargeStatus = "NOT_FOUND"
	ChargeStatusPending   ChargeStatus = "PENDING"
	ChargeStatusSucceeded ChargeStatus = "SUCCEEDED"
	ChargeStatusDeclined  ChargeStatus = "DECLINED"
)

var (
	ErrNotFound           = errors.New("payment not found")
	ErrNoRetryableAttempt = errors.New("payment has no failed attempt to retry")
	// ErrDeclined is permanent: retrying will not help.
	ErrDeclined = errors.New("payment declined")
	// ErrTransient is temporary: the charge may succeed if retried.
	ErrTransient = errors.New("transient payment gateway error")
)

type Payment struct {
	ID            string
	OrderID       string
	TenantID      string
	AttemptNumber int
	AmountCents   int64
	Status        Status
	FailureReason string
}

// Gateway operations must be idempotent: Charge uses idempotencyKey, and Refund
// uses paymentID as its idempotency key. GetCharge must distinguish a
// definitively absent charge from one that is still processing.
type Gateway interface {
	Charge(ctx context.Context, idempotencyKey string, amountCents int64) error
	Refund(ctx context.Context, paymentID string, amountCents int64) error
	GetCharge(ctx context.Context, idempotencyKey string) (ChargeStatus, error)
}
