//go:build integration

package failure

import (
	"context"
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
	orders := services.NewOrderService(pool)
	recovery := services.NewRecoveryService(pool, 2*time.Minute, 30*time.Second, 3)
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
	orders := services.NewOrderService(pool)
	recovery := services.NewRecoveryService(pool, time.Hour, 30*time.Second, 2)
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

// Payment succeeds after the order was already cancelled: the money is refunded,
// stock is not deducted twice.
func TestLatePaymentAfterCancellationIsRefunded(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(pool)
	product := testutil.SeedProduct(t, pool, 5)

	res, _ := orders.CreateOrder(context.Background(), "user-1", "key-late-0001",
		[]order.CreateItem{{ProductID: product, Quantity: 2}})

	// The gateway call "takes a long time"; meanwhile recovery cancels the order.
	slow := &cancelDuringCharge{recovery: services.NewRecoveryService(pool, 0, time.Hour, 0)}
	payments := services.NewPaymentService(pool, slow)
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); err != nil {
		t.Fatal(err)
	}

	got, _ := orders.GetOrder(context.Background(), "user-1", res.Order.ID)
	if got.Status != order.StatusCancelled {
		t.Fatalf("status=%s, want CANCELLED", got.Status)
	}
	var status string
	if err := pool.QueryRow(context.Background(),
		`SELECT status FROM payments WHERE order_id = $1`, res.Order.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if payment.Status(status) != payment.StatusRefunded {
		t.Fatalf("payment status=%s, want REFUNDED", status)
	}
	if onHand, reserved := testutil.Stock(t, pool, product); onHand != 5 || reserved != 0 {
		t.Fatalf("on_hand=%d reserved=%d, want 5/0", onHand, reserved)
	}
}

// cancelDuringCharge simulates a slow gateway call: while the charge is in
// flight, the recovery worker times the order out. The charge then succeeds.
type cancelDuringCharge struct{ recovery *services.RecoveryService }

func (g *cancelDuringCharge) Charge(ctx context.Context, _ string, _ int64) error {
	_, err := g.recovery.Sweep(ctx)
	return err
}

var _ payment.Gateway = (*cancelDuringCharge)(nil)
