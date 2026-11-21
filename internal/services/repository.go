package services

import (
	"context"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

// Repository is the persistence port used by application services. Production
// uses PostgreSQL; tests or deployments can provide another implementation.
type Repository interface {
	WithinTransaction(context.Context, func(Transaction) error) error
	GetOrder(context.Context, string) (*order.Order, error)
	GetOrderForTenant(context.Context, string, string) (*order.Order, error)
	ListOrders(context.Context, string, string, int, int) ([]*order.Order, int64, error)
	ListProductStock(context.Context, string) ([]inventory.ProductStock, error)
	GetPaymentByID(context.Context, string) (*payment.Payment, error)
	TouchPendingRefund(context.Context, string) error
	ListStaleRefunds(context.Context, time.Time, int) ([]string, error)
	ListExpiredOrders(context.Context, time.Time, int) ([]string, error)
	ListStalledOrders(context.Context, time.Time, int, int) ([]string, error)
}

// Transaction exposes only the operations use cases need inside an atomic
// database transaction.
type Transaction interface {
	ClaimIdempotencyKey(context.Context, string, string, string, string, string) (bool, error)
	GetIdempotencyKey(context.Context, string, string, string) (string, string, error)
	GetOrder(context.Context, string) (*order.Order, error)
	GetProductPrices(context.Context, string, []string) (map[string]int64, error)
	ReserveStock(context.Context, string, string, int) (bool, error)
	InsertOrder(context.Context, *order.Order) error
	InsertOrderItems(context.Context, string, string, []order.Item) error
	InsertOutbox(context.Context, string, string, any) error
	LockOrder(context.Context, string) (*order.Order, error)
	GetOrderItems(context.Context, string) ([]order.Item, error)
	ReleaseStock(context.Context, string, string, int) error
	CommitStock(context.Context, string, string, int) error
	SetOrderStatus(context.Context, string, order.Status, order.Status, string) (bool, error)
	FailProcessingPayment(context.Context, string, string) error
	BumpRecovery(context.Context, string) error
	GetLatestPaymentAttemptByOrder(context.Context, string, string) (*payment.Payment, error)
	InsertPaymentAttempt(context.Context, string, string, string, int64) error
	GetPaymentByID(context.Context, string) (*payment.Payment, error)
	SetPaymentResult(context.Context, string, payment.Status, string) error
	TransitionPaymentStatus(context.Context, string, payment.Status, payment.Status, string) (bool, error)
	ScheduleRefundRetry(context.Context, string, time.Time) (bool, error)
}
