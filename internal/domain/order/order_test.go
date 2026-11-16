package order

import (
	"errors"
	"testing"
)

func TestNormalizeItemsMergesAndSorts(t *testing.T) {
	got, err := NormalizeItems([]CreateItem{
		{ProductID: "b", Quantity: 1}, {ProductID: "a", Quantity: 2}, {ProductID: "b", Quantity: 3},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []CreateItem{{ProductID: "a", Quantity: 2}, {ProductID: "b", Quantity: 4}}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestNormalizeItemsRejectsBadInput(t *testing.T) {
	cases := map[string][]CreateItem{
		"empty":         nil,
		"zero quantity": {{ProductID: "a", Quantity: 0}},
		"no product":    {{ProductID: "", Quantity: 1}},
		"too many":      {{ProductID: "a", Quantity: MaxQuantityPerItem + 1}},
	}
	for name, in := range cases {
		if _, err := NormalizeItems(in); !errors.Is(err, ErrInvalidItems) {
			t.Errorf("%s: err=%v, want ErrInvalidItems", name, err)
		}
	}
}

func TestStatusTransitions(t *testing.T) {
	if !StatusPendingPayment.CanTransitionTo(StatusPaid) || !StatusPendingPayment.CanTransitionTo(StatusCancelled) {
		t.Fatal("pending must be able to move to paid or cancelled")
	}
	if StatusPendingPayment.CanTransitionTo(StatusPendingPayment) ||
		StatusPaid.CanTransitionTo(StatusCancelled) ||
		StatusCancelled.CanTransitionTo(StatusPaid) {
		t.Fatal("invalid transitions must be rejected")
	}
	if !StatusPaid.IsTerminal() || StatusPendingPayment.IsTerminal() {
		t.Fatal("IsTerminal mismatch")
	}
}
