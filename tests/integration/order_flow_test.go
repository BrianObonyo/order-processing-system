//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/BrianObonyo/order-processing-system/internal/domain/event"
	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/infrastructure/postgres"
	"github.com/BrianObonyo/order-processing-system/internal/messaging"
	"github.com/BrianObonyo/order-processing-system/internal/repositories"
	"github.com/BrianObonyo/order-processing-system/internal/services"
	"github.com/BrianObonyo/order-processing-system/tests/testutil"
)

// The same Idempotency-Key sent 20 times in parallel creates exactly one order.
func TestIdempotentCreateUnderConcurrency(t *testing.T) {
	pool := testutil.StartPostgres(t)
	svc := services.NewOrderService(testutil.Store(pool))
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
			res, err := svc.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), key, items)
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
	_, err := svc.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), key, []order.CreateItem{{ProductID: product, Quantity: 5}})
	if !errors.Is(err, order.ErrIdempotencyMismatch) {
		t.Fatalf("err=%v, want ErrIdempotencyMismatch", err)
	}
}

// Rejected orders leave no trace: stock, order rows and the idempotency key roll back together.
func TestRejectedOrderRollsBackEverything(t *testing.T) {
	pool := testutil.StartPostgres(t)
	svc := services.NewOrderService(testutil.Store(pool))
	plenty := testutil.SeedProduct(t, pool, 100)
	scarce := testutil.SeedProduct(t, pool, 1)

	_, err := svc.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-rollback-1", []order.CreateItem{
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

func TestCustomPricingPolicyIsAppliedToOrderTotal(t *testing.T) {
	pool := testutil.StartPostgres(t)
	store := testutil.Store(pool)
	orders := services.NewOrderService(store, services.WithPricingPolicy(services.PricingFunc(
		func(ctx context.Context, request services.OrderPricingRequest) (services.OrderPricingResult, error) {
			result, err := (services.CatalogPricing{}).Price(ctx, request)
			if err != nil {
				return services.OrderPricingResult{}, err
			}
			result.TotalCents += 250
			return result, nil
		},
	)))
	product := testutil.SeedProduct(t, pool, 5)

	result, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-custom-pricing-1",
		[]order.CreateItem{{ProductID: product, Quantity: 2}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Order.TotalCents != 2250 {
		t.Fatalf("total_cents=%d, want 2250 including policy surcharge", result.Order.TotalCents)
	}
	if result.Order.Items[0].UnitPriceCents != 1000 {
		t.Fatalf("unit_price_cents=%d, want catalog price 1000", result.Order.Items[0].UnitPriceCents)
	}
	if _, reserved := testutil.Stock(t, pool, product); reserved != 2 {
		t.Fatalf("reserved=%d, want 2", reserved)
	}
}

func TestOrderValidatorCanRejectBeforePersistence(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(testutil.Store(pool), services.WithOrderValidator(
		services.OrderValidatorFunc(func(context.Context, string, []order.CreateItem) error {
			return errors.New("order rejected by policy")
		}),
	))
	product := testutil.SeedProduct(t, pool, 5)

	_, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-policy-reject-1",
		[]order.CreateItem{{ProductID: product, Quantity: 2}})
	if err == nil {
		t.Fatal("expected policy rejection")
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM orders`); n != 0 {
		t.Fatalf("orders=%d, want no persisted order after rejection", n)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM idempotency_keys`); n != 0 {
		t.Fatalf("idempotency keys=%d, want transaction rollback after rejection", n)
	}
	if _, reserved := testutil.Stock(t, pool, product); reserved != 0 {
		t.Fatalf("reserved=%d, want no stock reservation after rejection", reserved)
	}
}

func TestListOrdersPaginatesAndScopesToUser(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 10)
	for _, userID := range []string{"user-1", "user-1", "user-1", "user-2"} {
		if _, err := orders.CreateOrder(context.Background(), testutil.UserIdentity(userID), uuid.NewString(),
			[]order.CreateItem{{ProductID: product, Quantity: 1}}); err != nil {
			t.Fatal(err)
		}
	}

	firstPage, err := orders.ListOrders(context.Background(), testutil.UserIdentity("user-1"), 1, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if firstPage.Total != 3 || len(firstPage.Orders) != 2 || firstPage.Page != 1 {
		t.Fatalf("first page=%+v, want total 3 and 2 orders", firstPage)
	}
	secondPage, err := orders.ListOrders(context.Background(), testutil.UserIdentity("user-1"), 2, 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if secondPage.Total != 3 || len(secondPage.Orders) != 1 || secondPage.Page != 2 {
		t.Fatalf("second page=%+v, want total 3 and 1 order", secondPage)
	}
	pendingOnly, err := orders.ListOrders(context.Background(), testutil.UserIdentity("user-1"), 1, 10, order.StatusPendingPayment)
	if err != nil {
		t.Fatal(err)
	}
	if pendingOnly.Total != 3 || len(pendingOnly.Orders) != 3 {
		t.Fatalf("pending-only page=%+v, want total 3", pendingOnly)
	}
	paidOnly, err := orders.ListOrders(context.Background(), testutil.UserIdentity("user-1"), 1, 10, order.StatusPaid)
	if err != nil {
		t.Fatal(err)
	}
	if paidOnly.Total != 0 || len(paidOnly.Orders) != 0 {
		t.Fatalf("paid-only page=%+v, want empty result", paidOnly)
	}
	for _, listed := range append(firstPage.Orders, secondPage.Orders...) {
		if listed.UserID != "user-1" {
			t.Fatalf("listed order belongs to %q, want user-1", listed.UserID)
		}
	}
}

func TestTenantIsolationAcrossCatalogOrdersAndIdempotency(t *testing.T) {
	pool := testutil.StartPostgres(t)
	testutil.SeedTenant(t, pool, "tenant-alpha")
	testutil.SeedTenant(t, pool, "tenant-beta")
	store := testutil.Store(pool)
	orders := services.NewOrderService(store)
	products := services.NewProductService(store)
	alphaProduct := testutil.SeedProductForTenant(t, pool, "tenant-alpha", 5)
	betaProduct := testutil.SeedProductForTenant(t, pool, "tenant-beta", 5)

	alphaUser := identity.Identity{TenantID: "tenant-alpha", UserID: "owner", Role: identity.RoleUser}
	betaUser := identity.Identity{TenantID: "tenant-beta", UserID: "owner", Role: identity.RoleUser}
	alphaOrder, err := orders.CreateOrder(context.Background(), alphaUser, "tenant-shared-key-001",
		[]order.CreateItem{{ProductID: alphaProduct, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	betaOrder, err := orders.CreateOrder(context.Background(), betaUser, "tenant-shared-key-001",
		[]order.CreateItem{{ProductID: betaProduct, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if alphaOrder.Order.ID == betaOrder.Order.ID {
		t.Fatal("same idempotency key in different tenants returned the same order")
	}

	alphaOtherUser := testutil.UserIdentity("buyer-2")
	alphaOtherUser.TenantID = "tenant-alpha"
	otherOrder, err := orders.CreateOrder(context.Background(), alphaOtherUser, "tenant-alpha-order-002",
		[]order.CreateItem{{ProductID: alphaProduct, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := orders.CreateOrder(context.Background(), alphaUser, "tenant-cross-product-001",
		[]order.CreateItem{{ProductID: betaProduct, Quantity: 1}}); !errors.Is(err, inventory.ErrUnknownProduct) {
		t.Fatalf("cross-tenant product create error=%v, want unknown product", err)
	}
	if _, err := orders.GetOrder(context.Background(), alphaUser, betaOrder.Order.ID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("cross-tenant user lookup error=%v, want not found", err)
	}
	alphaAdmin := testutil.AdminIdentity("tenant-alpha", "admin-1")
	if _, err := orders.GetOrder(context.Background(), alphaAdmin, betaOrder.Order.ID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("cross-tenant admin lookup error=%v, want not found", err)
	}
	if _, err := orders.GetOrder(context.Background(), alphaUser, otherOrder.Order.ID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("other user's order lookup error=%v, want not found", err)
	}
	if _, err := orders.GetOrder(context.Background(), alphaAdmin, otherOrder.Order.ID); err != nil {
		t.Fatalf("same-tenant admin lookup error=%v, want success", err)
	}

	alphaProducts, err := products.List(context.Background(), "tenant-alpha")
	if err != nil {
		t.Fatal(err)
	}
	betaProducts, err := products.List(context.Background(), "tenant-beta")
	if err != nil {
		t.Fatal(err)
	}
	if len(alphaProducts) != 1 || alphaProducts[0].ProductID != alphaProduct ||
		len(betaProducts) != 1 || betaProducts[0].ProductID != betaProduct {
		t.Fatalf("product lists crossed tenant boundary: alpha=%+v beta=%+v", alphaProducts, betaProducts)
	}

	alphaPage, err := orders.ListOrders(context.Background(), alphaAdmin, 1, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	betaPage, err := orders.ListOrders(context.Background(), testutil.AdminIdentity("tenant-beta", "admin-2"), 1, 10, "")
	if err != nil {
		t.Fatal(err)
	}
	if alphaPage.Total != 2 || betaPage.Total != 1 {
		t.Fatalf("tenant order totals alpha=%d beta=%d, want 2/1", alphaPage.Total, betaPage.Total)
	}

	if err := orders.CancelOrder(context.Background(), alphaAdmin, otherOrder.Order.ID); err != nil {
		t.Fatal(err)
	}
	if err := orders.CancelOrder(context.Background(), alphaAdmin, betaOrder.Order.ID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("cross-tenant admin cancel error=%v, want not found", err)
	}
	var tenantID, reason string
	if err := pool.QueryRow(context.Background(),
		`SELECT tenant_id, reason FROM order_status_history WHERE order_id = $1 AND to_status = 'CANCELLED'`,
		otherOrder.Order.ID).Scan(&tenantID, &reason); err != nil {
		t.Fatal(err)
	}
	if tenantID != "tenant-alpha" || reason != "cancelled by tenant admin" {
		t.Fatalf("cancel history tenant=%q reason=%q, want tenant-alpha/admin reason", tenantID, reason)
	}
	var alphaReserved, betaReserved int
	if err := pool.QueryRow(context.Background(), `
		SELECT reserved FROM inventory WHERE tenant_id = $1 AND product_id = $2`, "tenant-alpha", alphaProduct).
		Scan(&alphaReserved); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `
		SELECT reserved FROM inventory WHERE tenant_id = $1 AND product_id = $2`, "tenant-beta", betaProduct).
		Scan(&betaReserved); err != nil {
		t.Fatal(err)
	}
	if alphaReserved != 1 || betaReserved != 1 {
		t.Fatalf("reserved inventory alpha=%d beta=%d, want 1/1", alphaReserved, betaReserved)
	}
}

func TestStartupRejectsUnprovisionedTenant(t *testing.T) {
	pool := testutil.StartPostgres(t)
	err := postgres.ValidateTenants(context.Background(), pool, map[string]identity.Identity{
		"valid": {TenantID: "default", UserID: "user-1", Role: identity.RoleUser},
		"missing": {
			TenantID: "not-provisioned", UserID: "user-2", Role: identity.RoleUser,
		},
	})
	if err == nil || !strings.Contains(err.Error(), "not-provisioned") {
		t.Fatalf("ValidateTenants() error=%v, want unknown tenant rejected", err)
	}
	if err := postgres.ValidateTenants(context.Background(), pool, map[string]identity.Identity{
		"valid": {TenantID: "default", UserID: "user-1", Role: identity.RoleUser},
	}); err != nil {
		t.Fatalf("ValidateTenants() error=%v for migrated default tenant", err)
	}
}

func TestCancelPendingOrderReleasesStockAndIsRecorded(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 5)
	result, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-cancel-order-1",
		[]order.CreateItem{{ProductID: product, Quantity: 2}})
	if err != nil {
		t.Fatal(err)
	}

	if err := orders.CancelOrder(context.Background(), testutil.UserIdentity("user-1"), result.Order.ID); err != nil {
		t.Fatal(err)
	}
	cancelled, err := orders.GetOrder(context.Background(), testutil.UserIdentity("user-1"), result.Order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != order.StatusCancelled {
		t.Fatalf("status=%s, want CANCELLED", cancelled.Status)
	}
	if _, reserved := testutil.Stock(t, pool, product); reserved != 0 {
		t.Fatalf("reserved=%d, want 0 after cancellation", reserved)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM order_status_history WHERE order_id = $1 AND to_status = 'CANCELLED'`, result.Order.ID); n != 1 {
		t.Fatalf("cancel status history rows=%d, want 1", n)
	}
	if err := orders.CancelOrder(context.Background(), testutil.UserIdentity("user-1"), result.Order.ID); !errors.Is(err, services.ErrOrderNotCancelable) {
		t.Fatalf("second cancel error=%v, want ErrOrderNotCancelable", err)
	}
	if err := orders.CancelOrder(context.Background(), testutil.UserIdentity("other-user"), result.Order.ID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("other user's cancel error=%v, want not found", err)
	}
}

func TestPaymentSuccessAndDuplicateDelivery(t *testing.T) {
	pool := testutil.StartPostgres(t)
	orders := services.NewOrderService(testutil.Store(pool))
	payments := services.NewPaymentService(testutil.Store(pool), testutil.StubGateway{})
	product := testutil.SeedProduct(t, pool, 10)

	res, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-pay-0001",
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

	got, _ := orders.GetOrder(context.Background(), testutil.UserIdentity("user-1"), res.Order.ID)
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
	orders := services.NewOrderService(testutil.Store(pool))
	payments := services.NewPaymentService(testutil.Store(pool), testutil.StubGateway{Err: payment.ErrDeclined})
	product := testutil.SeedProduct(t, pool, 10)

	res, _ := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-decl-001",
		[]order.CreateItem{{ProductID: product, Quantity: 4}})
	if err := payments.ProcessOrder(context.Background(), res.Order.ID); err != nil {
		t.Fatal(err)
	}

	got, _ := orders.GetOrder(context.Background(), testutil.UserIdentity("user-1"), res.Order.ID)
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
	orders := services.NewOrderService(testutil.Store(pool))
	gateway := &sequenceGateway{outcomes: []error{payment.ErrDeclined, nil}}
	payments := services.NewPaymentService(testutil.Store(pool), gateway)
	product := testutil.SeedProduct(t, pool, 10)

	res, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-payment-retry-1",
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
	if _, err := payments.RetryPayment(context.Background(), testutil.UserIdentity("user-1"), res.Order.ID); !errors.Is(err, identity.ErrForbidden) {
		t.Fatalf("user retry error=%v, want admin-only restriction", err)
	}
	if _, err := payments.RetryPayment(context.Background(), testutil.AdminIdentity("another-tenant", "admin-1"), res.Order.ID); !errors.Is(err, order.ErrNotFound) {
		t.Fatalf("cross-tenant admin retry error=%v, want not found", err)
	}
	retry, err := payments.RetryPayment(context.Background(), testutil.AdminIdentity("default", "admin-1"), res.Order.ID)
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
	orders := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 1)
	res, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-status-history-1",
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
	orders := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 10)

	res, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), "key-outbox-1",
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
	var envelope event.Envelope
	if err := json.Unmarshal(pub.msgs[0].Payload, &envelope); err != nil {
		t.Fatal(err)
	}
	if err := envelope.Validate(order.EventCreated); err != nil {
		t.Fatalf("published envelope invalid: %v", err)
	}
	if envelope.CorrelationID != res.Order.ID {
		t.Fatalf("correlation_id=%q, want %q", envelope.CorrelationID, res.Order.ID)
	}
	var data order.Event
	if err := json.Unmarshal(envelope.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.OrderID != res.Order.ID {
		t.Fatalf("event data order_id=%q, want %q", data.OrderID, res.Order.ID)
	}
	var outboxEventID string
	if err := pool.QueryRow(context.Background(),
		`SELECT event_id::text FROM outbox WHERE aggregate_id = $1 AND event_type = $2`,
		res.Order.ID, order.EventCreated).Scan(&outboxEventID); err != nil {
		t.Fatal(err)
	}
	if envelope.ID != outboxEventID {
		t.Fatalf("event_id=%q, want stable outbox ID %q", envelope.ID, outboxEventID)
	}
}

func TestOutboxEventIDMigrationCanRollback(t *testing.T) {
	pool := testutil.StartPostgres(t)
	_, currentFile, _, _ := runtime.Caller(0)
	downPath := filepath.Join(filepath.Dir(currentFile), "..", "..", "migrations", "000005_event_envelopes.down.sql")
	sql, err := os.ReadFile(downPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), string(sql)); err != nil {
		t.Fatalf("run event envelope down migration: %v", err)
	}

	var exists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'outbox' AND column_name = 'event_id'
		)`,
	).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("event_id column still exists after rollback")
	}
}

func TestTenantMigrationCanRollback(t *testing.T) {
	pool := testutil.StartPostgres(t)
	product := testutil.SeedProduct(t, pool, 2)
	store := testutil.Store(pool)
	orders := services.NewOrderService(store)
	result, err := orders.CreateOrder(context.Background(), testutil.UserIdentity("migration-user"),
		"tenant-migration-rollback-1", []order.CreateItem{{ProductID: product, Quantity: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if err := services.NewPaymentService(store, testutil.StubGateway{}).ProcessOrder(context.Background(), result.Order.ID); err != nil {
		t.Fatal(err)
	}
	_, currentFile, _, _ := runtime.Caller(0)
	downPath := filepath.Join(filepath.Dir(currentFile), "..", "..", "migrations", "000006_tenant_isolation.down.sql")
	sql, err := os.ReadFile(downPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), string(sql)); err != nil {
		t.Fatalf("run tenant isolation down migration: %v", err)
	}

	var tenantColumnExists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_name = 'orders' AND column_name = 'tenant_id'
		)`,
	).Scan(&tenantColumnExists); err != nil {
		t.Fatal(err)
	}
	if tenantColumnExists {
		t.Fatal("tenant_id column still exists after rollback")
	}
	var tenantsTableExists bool
	if err := pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables
			WHERE table_name = 'tenants'
		)`,
	).Scan(&tenantsTableExists); err != nil {
		t.Fatal(err)
	}
	if tenantsTableExists {
		t.Fatal("tenants table still exists after rollback")
	}
	for _, table := range []string{"orders", "order_items", "payment_attempts", "idempotency_keys", "outbox", "order_status_history"} {
		if count := testutil.Count(t, pool, `SELECT count(*) FROM `+table); count == 0 {
			t.Errorf("%s rows were lost during migration rollback", table)
		}
	}
	var oldInventoryKey string
	if err := pool.QueryRow(context.Background(),
		`SELECT product_id::text FROM inventory WHERE product_id = $1`, product).Scan(&oldInventoryKey); err != nil {
		t.Fatalf("inventory row was lost during migration rollback: %v", err)
	}
}
