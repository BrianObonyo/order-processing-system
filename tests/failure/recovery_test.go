//go:build integration

package failure

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/services"
	"github.com/BrianObonyo/order-processing-system/tests/testutil"
)

// A stuck order is cancelled and its reserved stock goes back on sale.
func TestRecoveryCancelsStaleOrderAndReleasesStock(t *testing.T) {
	pool := testutil.StartPostgres(t)
	store := testutil.Store(pool)
	orders := services.NewOrderService(store)
	payments := services.NewPaymentService(store, testutil.StubGateway{})
	recovery := services.NewRecoveryService(store, payments, 2*time.Minute, 30*time.Second, 3)
	product := testutil.SeedProduct(t, pool, 5)

	res, err := orders.CreateOrder(context.Background(), "user-1", "key-recover-1",
		[]order.CreateItem{{ProductID: product, Quantity: 5}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(),
		`UPDATE orders SET created_at = now() - interval '10 minutes' WHERE id = $1`, res.Order.ID); err != nil {
		t.Fatal(err)
	}

	sweep, err := recovery.Sweep(context.Background())
	if err != nil || sweep.Cancelled != 1 {
		t.Fatalf("sweep=%+v err=%v, want 1 cancelled", sweep, err)
	}
	got, _ := orders.GetOrder(context.Background(), "user-1", res.Order.ID)
	if got.Status != order.StatusCancelled {
		t.Fatalf("status=%s, want CANCELLED", got.Status)
	}
	if _, reserved := testutil.Stock(t, pool, product); reserved != 0 {
		t.Fatalf("reserved=%d, want 0", reserved)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE event_type = 'order.cancelled'`); n != 1 {
		t.Fatalf("cancel events=%d, want 1", n)
	}
	// Running again changes nothing.
	if sweep, _ := recovery.Sweep(context.Background()); sweep.Cancelled != 0 {
		t.Fatalf("second sweep cancelled %d, want 0", sweep.Cancelled)
	}
}

// Idle orders get their event re-emitted, but only up to the retry budget.
func TestRecoveryRetriesStalledOrdersWithinBudget(t *testing.T) {
	pool := testutil.StartPostgres(t)
	store := testutil.Store(pool)
	orders := services.NewOrderService(store)
	payments := services.NewPaymentService(store, testutil.StubGateway{})
	recovery := services.NewRecoveryService(store, payments, time.Hour, 30*time.Second, 2)
	product := testutil.SeedProduct(t, pool, 5)

	res, _ := orders.CreateOrder(context.Background(), "user-1", "key-retry-001",
		[]order.CreateItem{{ProductID: product, Quantity: 1}})

	for i := 0; i < 4; i++ {
		if _, err := pool.Exec(context.Background(),
			`UPDATE orders SET updated_at = now() - interval '5 minutes' WHERE id = $1`, res.Order.ID); err != nil {
			t.Fatal(err)
		}
		if _, err := recovery.Sweep(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// 1 original + 2 retries (budget), not 4.
	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE event_type = 'order.created'`); n != 3 {
		t.Fatalf("order.created events=%d, want 3", n)
	}
}

func TestRecoveryWaitsForChargeReconciliationBeforeCancellation(t *testing.T) {
	pool := testutil.StartPostgres(t)
	store := testutil.Store(pool)
	orders := services.NewOrderService(store)
	gateway := &reconciliationGateway{chargeErr: payment.ErrTransient, lookupErr: payment.ErrTransient}
	payments := services.NewPaymentService(store, gateway)
	recovery := services.NewRecoveryService(store, payments, time.Minute, 30*time.Second, 3)
	product := testutil.SeedProduct(t, pool, 2)

	res, err := orders.CreateOrder(context.Background(), "user-1", "key-unknown-charge-1",
		[]order.CreateItem{{ProductID: product, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); !errors.Is(err, payment.ErrTransient) {
		t.Fatalf("ProcessOrder err=%v, want transient gateway error", err)
	}
	if _, err := pool.Exec(context.Background(),
		`UPDATE orders SET created_at = now() - interval '10 minutes' WHERE id = $1`, res.Order.ID); err != nil {
		t.Fatal(err)
	}

	if _, err := recovery.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err := orders.GetOrder(context.Background(), "user-1", res.Order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != order.StatusPendingPayment {
		t.Fatalf("status=%s, want PENDING_PAYMENT while charge outcome is unknown", got.Status)
	}
	if _, reserved := testutil.Stock(t, pool, product); reserved != 1 {
		t.Fatalf("reserved=%d, want 1 while charge outcome is unknown", reserved)
	}

	gateway.lookupErr = nil
	gateway.chargeStatus = payment.ChargeStatusSucceeded
	if _, err := recovery.Sweep(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, err = orders.GetOrder(context.Background(), "user-1", res.Order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != order.StatusPaid {
		t.Fatalf("status=%s, want PAID after provider confirms the charge", got.Status)
	}
	if onHand, reserved := testutil.Stock(t, pool, product); onHand != 1 || reserved != 0 {
		t.Fatalf("on_hand=%d reserved=%d, want 1/0", onHand, reserved)
	}
}

// Payment succeeds after the order was already cancelled: the money is refunded,
// stock is not deducted twice.
func TestLatePaymentAfterCancellationIsRefunded(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 5)

	res, _ := orders.CreateOrder(context.Background(), "user-1", "key-late-0001",
		[]order.CreateItem{{ProductID: product, Quantity: 2}})

	// The gateway call "takes a long time"; meanwhile recovery cancels the order.
	slow := &cancelDuringCharge{}
	store := testutil.Store(pool)
	payments := services.NewPaymentService(store, slow)
	slow.recovery = services.NewRecoveryService(store, payments, 0, time.Hour, 0)
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); err != nil {
		t.Fatal(err)
	}

	got, _ := orders.GetOrder(context.Background(), "user-1", res.Order.ID)
	if got.Status != order.StatusCancelled {
		t.Fatalf("status=%s, want CANCELLED", got.Status)
	}
	var status string
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM payment_attempts WHERE order_id = $1`, res.Order.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if payment.Status(status) != payment.StatusRefunded {
		t.Fatalf("payment status=%s, want REFUNDED", status)
	}
	if slow.refunds != 1 {
		t.Fatalf("refund calls=%d, want 1", slow.refunds)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE event_type = 'payment.refunded'`); n != 1 {
		t.Fatalf("payment.refunded events=%d, want 1", n)
	}
	if onHand, reserved := testutil.Stock(t, pool, product); onHand != 5 || reserved != 0 {
		t.Fatalf("on_hand=%d reserved=%d, want 5/0", onHand, reserved)
	}
}

func TestFailedLateRefundStaysPendingAndIsRequeued(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 2)
	res, err := orders.CreateOrder(context.Background(), "user-1", "key-refund-retry-1",
		[]order.CreateItem{{ProductID: product, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}

	gateway := &cancelDuringCharge{refundFailures: 1}
	store := testutil.Store(pool)
	payments := services.NewPaymentService(store, gateway)
	recovery := services.NewRecoveryService(store, payments, 0, time.Minute, 0)
	gateway.recovery = recovery
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); !errors.Is(err, payment.ErrTransient) {
		t.Fatalf("ProcessOrder err=%v, want transient refund error", err)
	}

	var paymentID, status string
	if err := pool.QueryRow(context.Background(),
		`SELECT id::text, status FROM payment_attempts WHERE order_id = $1`, res.Order.ID).Scan(&paymentID, &status); err != nil {
		t.Fatal(err)
	}
	if payment.Status(status) != payment.StatusRefundPending {
		t.Fatalf("payment status=%s, want REFUND_PENDING after provider failure", status)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE event_type = 'payment.refunded'`); n != 0 {
		t.Fatalf("payment.refunded events=%d, want 0 before provider confirms refund", n)
	}
	if _, err := pool.Exec(context.Background(),
		`UPDATE payment_attempts SET updated_at = now() - interval '10 minutes' WHERE id = $1`, paymentID); err != nil {
		t.Fatal(err)
	}
	sweep, err := recovery.Sweep(context.Background())
	if err != nil || sweep.RefundRetried != 1 {
		t.Fatalf("sweep=%+v err=%v, want one refund retry", sweep, err)
	}
	if err := payments.ProcessRefund(context.Background(), paymentID); err != nil {
		t.Fatal(err)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE event_type = 'payment.refunded'`); n != 1 {
		t.Fatalf("payment.refunded events=%d, want 1 after provider confirms refund", n)
	}
}

// cancelDuringCharge simulates a slow gateway call: while the charge is in
// flight, the recovery worker times the order out. The charge then succeeds.
type cancelDuringCharge struct {
	recovery       *services.RecoveryService
	charged        bool
	refunds        int
	refundFailures int
}

func (g *cancelDuringCharge) Charge(ctx context.Context, _ string, _ int64) error {
	_, err := g.recovery.Sweep(ctx)
	g.charged = true
	return err
}

func (g *cancelDuringCharge) Refund(context.Context, string, int64) error {
	g.refunds++
	if g.refunds <= g.refundFailures {
		return payment.ErrTransient
	}
	return nil
}

func (g *cancelDuringCharge) GetCharge(context.Context, string) (payment.ChargeStatus, error) {
	if g.charged {
		return payment.ChargeStatusSucceeded, nil
	}
	return payment.ChargeStatusNotFound, nil
}

var _ payment.Gateway = (*cancelDuringCharge)(nil)

type reconciliationGateway struct {
	chargeErr    error
	chargeStatus payment.ChargeStatus
	lookupErr    error
}

func (g *reconciliationGateway) Charge(context.Context, string, int64) error {
	return g.chargeErr
}

func (g *reconciliationGateway) Refund(context.Context, string, int64) error {
	return nil
}

func (g *reconciliationGateway) GetCharge(context.Context, string) (payment.ChargeStatus, error) {
	return g.chargeStatus, g.lookupErr
}

var _ payment.Gateway = (*reconciliationGateway)(nil)
