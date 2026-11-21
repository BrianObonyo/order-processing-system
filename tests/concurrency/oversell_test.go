//go:build integration

package concurrency

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/services"
	"github.com/BrianObonyo/order-processing-system/tests/testutil"
)

// 100 concurrent buyers compete for 10 units: exactly 10 may succeed.
func TestNoOverselling(t *testing.T) {
	pool := testutil.StartPostgres(t)
	svc := services.NewOrderService(testutil.Store(pool))
	product := testutil.SeedProduct(t, pool, 10)

	const buyers = 100
	var ok, soldOut, other atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < buyers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := svc.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), uuid.NewString(),
				[]order.CreateItem{{ProductID: product, Quantity: 1}})
			var stock *inventory.InsufficientStockError
			switch {
			case err == nil:
				ok.Add(1)
			case errors.As(err, &stock):
				soldOut.Add(1)
			default:
				other.Add(1)
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if ok.Load() != 10 || soldOut.Load() != buyers-10 || other.Load() != 0 {
		t.Fatalf("ok=%d soldOut=%d other=%d, want 10/%d/0", ok.Load(), soldOut.Load(), other.Load(), buyers-10)
	}
	onHand, reserved := testutil.Stock(t, pool, product)
	if onHand != 10 || reserved != 10 {
		t.Fatalf("on_hand=%d reserved=%d, want 10/10", onHand, reserved)
	}
	if n := testutil.Count(t, pool, `SELECT count(*) FROM orders`); n != 10 {
		t.Fatalf("orders=%d, want 10", n)
	}
}

// Orders that list the same products in opposite order must not deadlock.
func TestNoDeadlockOnMultiProductOrders(t *testing.T) {
	pool := testutil.StartPostgres(t)
	svc := services.NewOrderService(testutil.Store(pool))
	a := testutil.SeedProduct(t, pool, 500)
	b := testutil.SeedProduct(t, pool, 500)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		items := []order.CreateItem{{ProductID: a, Quantity: 1}, {ProductID: b, Quantity: 1}}
		if i%2 == 1 {
			items[0], items[1] = items[1], items[0]
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.CreateOrder(context.Background(), testutil.UserIdentity("user-1"), uuid.NewString(), items); err != nil {
				t.Errorf("create order: %v", err)
			}
		}()
	}
	wg.Wait()

	for _, p := range []string{a, b} {
		if _, reserved := testutil.Stock(t, pool, p); reserved != 100 {
			t.Fatalf("reserved=%d, want 100", reserved)
		}
	}
}
