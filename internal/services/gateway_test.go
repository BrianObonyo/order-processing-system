package services

import (
	"context"
	"errors"
	"testing"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

func TestSimulatedGatewayRefundIsIdempotent(t *testing.T) {
	ctx := context.Background()
	gateway := NewSimulatedGateway(0, 0, 0)

	if err := gateway.Charge(ctx, "payment-1", 2500); err != nil {
		t.Fatal(err)
	}
	status, err := gateway.GetCharge(ctx, "payment-1")
	if err != nil || status != payment.ChargeStatusSucceeded {
		t.Fatalf("GetCharge() = (%q, %v), want (%q, nil)", status, err, payment.ChargeStatusSucceeded)
	}
	if err := gateway.Refund(ctx, "payment-1", 2500); err != nil {
		t.Fatal(err)
	}
	if err := gateway.Refund(ctx, "payment-1", 2500); err != nil {
		t.Fatal(err)
	}
	if !gateway.refunds["payment-1"] {
		t.Fatal("successful refund was not recorded")
	}
}

func TestSimulatedGatewayDoesNotRefundUnknownCharge(t *testing.T) {
	gateway := NewSimulatedGateway(0, 0, 0)
	if err := gateway.Refund(context.Background(), "missing-payment", 2500); err == nil {
		t.Fatal("Refund() succeeded without a successful charge")
	}

	status, err := gateway.GetCharge(context.Background(), "missing-payment")
	if err != nil || status != payment.ChargeStatusNotFound {
		t.Fatalf("GetCharge() = (%q, %v), want (%q, nil)", status, err, payment.ChargeStatusNotFound)
	}
}

func TestSimulatedGatewayLookupReportsDecline(t *testing.T) {
	ctx := context.Background()
	gateway := NewSimulatedGateway(1, 0, 0)
	if err := gateway.Charge(ctx, "payment-declined", 2500); !errors.Is(err, payment.ErrDeclined) {
		t.Fatalf("Charge() err=%v, want ErrDeclined", err)
	}

	status, err := gateway.GetCharge(ctx, "payment-declined")
	if err != nil || status != payment.ChargeStatusDeclined {
		t.Fatalf("GetCharge() = (%q, %v), want (%q, nil)", status, err, payment.ChargeStatusDeclined)
	}
}
