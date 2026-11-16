//go:build integration

package integration

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/messaging"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
	"github.com/BrianObonyo/order-processing-system/internal/services"
	"github.com/BrianObonyo/order-processing-system/tests/testutil"
)

// The same Idempotency-Key sent 20 times in parallel creates exactly one order.
func TestIdempotentCreateUnderConcurrency(t *testing.T) {
	pool := testutil.StartPostgres(t)
	svc := services.NewOrderService(pool)
	product := testutil.SeedProduct(t, pool, 100)
	items := []order.CreateItem{{ProductID: product, Quantity: 3}}
	key := "idem-" + uuid.NewString()

	const calls = 20
	ids := make(chan string, calls)
	var wg sync.WaitGroup
	for i := 0; i < calls; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := svc.CreateOrder(context.Background(), "user-1", key, items)
			if err != nil {
				t.Errorf("create: %v", err)
				return
			}
			ids <- res.Order.ID
		}()
	}
	wg.Wait()
	close(ids)

	seen := map[string]bool{}
	for id := range ids {
		seen[id] = true
	}
	if len(seen) != 1 {
		t.Fatalf("got %d distinct orders for one key, want 1", len(seen))
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM orders`); n != 1 {
		t.Fatalf("orders=%d, want 1", n)
	}
	if _, reserved := testutil.Stock(t, pool, product); reserved != 3 {
		t.Fatalf("reserved=%d, want 3 (stock must be reserved once)", reserved)
	}

	// Same key, different body: rejected.
	_, err := svc.CreateOrder(context.Background(), "user-1", key, []order.CreateItem{{ProductID: product, Quantity: 5}})
	if !errors.Is(err, order.ErrIdempotencyMismatch) {
		t.Fatalf("err=%v, want ErrIdempotencyMismatch", err)
	}
}

// Rejected orders leave no trace: stock, order rows and the idempotency key roll back together.
func TestRejectedOrderRollsBackEverything(t *testing.T) {
	pool := testutil.StartPostgres(t)
	svc := services.NewOrderService(pool)
	plenty := testutil.SeedProduct(t, pool, 100)
	scarce := testutil.SeedProduct(t, pool, 1)

	_, err := svc.CreateOrder(context.Background(), "user-1", "key-rollback-1", []order.CreateItem{
		{ProductID: plenty, Quantity: 5}, {ProductID: scarce, Quantity: 2},
	})
	if err == nil {
		t.Fatal("expected insufficient stock error")
	}
	if _, reserved := testutil.Stock(t, pool, plenty); reserved != 0 {
		t.Fatalf("reserved=%d, want 0 after rollback", reserved)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("idempotency keys=%d, want 0 after rollback", n)
	}
}

func TestPaymentSuccessAndDuplicateDelivery(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(pool)
	payments := services.NewPaymentService(pool, testutil.StubGateway{})
	product := testutil.SeedProduct(t, pool, 10)

	res, err := orders.CreateOrder(context.Background(), "user-1", "key-pay-0001",
		[]order.CreateItem{{ProductID: product, Quantity: 4}})
	if err != nil {
		t.Fatal(err)
	}
	// Kafka may deliver the same event several times.
	for i := 0; i < 3; i++ {
		if err := payments.ProcessOrder(context.Background(), res.Order.ID); err != nil {
			t.Fatal(err)
		}
	}

	got, _ := orders.GetOrder(context.Background(), "user-1", res.Order.ID)
	if got.Status != order.StatusPaid {
		t.Fatalf("status=%s, want PAID", got.Status)
	}
	if onHand, reserved := testutil.Stock(t, pool, product); onHand != 6 || reserved != 0 {
		t.Fatalf("on_hand=%d reserved=%d, want 6/0", onHand, reserved)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM payment_attempts`); n != 1 {
		t.Fatalf("payment attempts=%d, want exactly 1", n)
	}
}

func TestPaymentDeclinedLeavesOrderRetryable(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(pool)
	payments := services.NewPaymentService(pool, testutil.StubGateway{Err: payment.ErrDeclined})
	product := testutil.SeedProduct(t, pool, 10)

	res, _ := orders.CreateOrder(context.Background(), "user-1", "key-decl-001",
		[]order.CreateItem{{ProductID: product, Quantity: 4}})
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); err != nil {
		t.Fatal(err)
	}

	got, _ := orders.GetOrder(context.Background(), "user-1", res.Order.ID)
	if got.Status != order.StatusPendingPayment {
		t.Fatalf("status=%s, want PENDING_PAYMENT for another payment attempt", got.Status)
	}
	if onHand, reserved := testutil.Stock(t, pool, product); onHand != 10 || reserved != 4 {
		t.Fatalf("on_hand=%d reserved=%d, want 10/4 while retry is possible", onHand, reserved)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM payment_attempts WHERE order_id = $1 AND status = 'FAILED'`, res.Order.ID); n != 1 {
		t.Fatalf("failed attempts=%d, want 1", n)
	}
}

func TestPaymentRetryCreatesNewAttemptAndKeepsHistory(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(pool)
	gateway := &sequenceGateway{outcomes: []error{payment.ErrDeclined, nil}}
	payments := services.NewPaymentService(pool, gateway)
	product := testutil.SeedProduct(t, pool, 10)

	res, err := orders.CreateOrder(context.Background(), "user-1", "key-payment-retry-1",
		[]order.CreateItem{{ProductID: product, Quantity: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); err != nil {
		t.Fatalf("first ProcessOrder err=%v, want nil for decline outcome", err)
	}

	first := testutil.Count(t, pool, `SELECT count(*) FROM payment_attempts WHERE order_id = $1 AND attempt_number = 1 AND status = 'FAILED'`, res.Order.ID)
	if first != 1 {
		t.Fatalf("failed first attempt rows=%d, want 1", first)
	}
	retry, err := payments.RetryPayment(context.Background(), res.Order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retry.AttemptNumber != 2 || retry.Status != payment.StatusProcessing {
		t.Fatalf("retry=%+v, want attempt 2 in PROCESSING", retry)
	}
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); err != nil {
		t.Fatal(err)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM payment_attempts WHERE order_id = $1`, res.Order.ID); n != 2 {
		t.Fatalf("attempts=%d, want 2", n)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM payment_attempts WHERE order_id = $1 AND status = 'SUCCEEDED'`, res.Order.ID); n != 1 {
		t.Fatalf("successful attempts=%d, want 1", n)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM order_status_history WHERE order_id = $1 AND from_status = 'PENDING_PAYMENT' AND to_status = 'PAID'`, res.Order.ID); n != 1 {
		t.Fatalf("PENDING_PAYMENT -> PAID history rows=%d, want 1", n)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM outbox WHERE aggregate_id = $1 AND event_type = 'order.created'`, res.Order.ID); n != 2 {
		t.Fatalf("order.created events=%d, want original and payment retry", n)
	}
}

func TestSetOrderStatusRejectsInvalidTransitionWithoutHistory(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(pool)
	product := testutil.SeedProduct(t, pool, 1)
	res, err := orders.CreateOrder(context.Background(), "user-1", "key-status-history-1",
		[]order.CreateItem{{ProductID: product, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}

	changed, err := repositories.New(pool).SetOrderStatus(
		context.Background(), res.Order.ID, order.StatusPaid, order.StatusCancelled, "invalid")
	if !errors.Is(err, order.ErrInvalidTransition) || changed {
		t.Fatalf("invalid transition changed=%t err=%v, want false and ErrInvalidTransition", changed, err)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM order_status_history WHERE order_id = $1`, res.Order.ID); n != 1 {
		t.Fatalf("status history rows=%d after rejection, want 1 initial entry", n)
	}
}

type recordingPublisher struct {
	mu   sync.Mutex
	msgs []messaging.Message
}

type sequenceGateway struct {
	outcomes []error
	next     int
}

func (g *sequenceGateway) Charge(context.Context, string, int64) error {
	outcome := g.outcomes[g.next]
	g.next++
	return outcome
}

func (*sequenceGateway) Refund(context.Context, string, int64) error { return nil }

func (g *sequenceGateway) GetCharge(context.Context, string) (payment.ChargeStatus, error) {
	if g.next > 0 && g.outcomes[g.next-1] == nil {
		return payment.ChargeStatusSucceeded, nil
	}
	return payment.ChargeStatusDeclined, nil
}

var _ payment.Gateway = (*sequenceGateway)(nil)

func (p *recordingPublisher) Publish(_ context.Context, m []messaging.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.msgs = append(p.msgs, m...)
	return nil
}

// An order and its event commit together; the relay then publishes each event once.
func TestOutboxRelayPublishesEachEventOnce(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(pool)
	product := testutil.SeedProduct(t, pool, 10)

	res, err := orders.CreateOrder(context.Background(), "user-1", "key-outbox-1",
		[]order.CreateItem{{ProductID: product, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}

	pub := &recordingPublisher{}
	relay := messaging.NewOutboxRelay(pool, pub, 0)
	for i := 0; i < 3; i++ {
		if _, err := relay.Flush(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(pub.msgs) != 1 || pub.msgs[0].Type != order.EventCreated || pub.msgs[0].Key != res.Order.ID {
		t.Fatalf("published %+v, want exactly one order.created for %s", pub.msgs, res.Order.ID)
	}
}
