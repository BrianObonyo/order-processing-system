package services

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
)

func TestPayPalGatewayContractWithFakeServer(t *testing.T) {
	var captureCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/v1/oauth2/token":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"access_token":"test-token","expires_in":3600}`))
		case r.URL.Path == "/v2/checkout/orders":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"order-1","status":"CREATED"}`))
		case r.URL.Path == "/v2/checkout/orders/order-1/capture":
			captureCount++
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"status":"COMPLETED","purchase_units":[{"payments":{"captures":[{"id":"capture-1"}]}}]}`))
		case r.URL.Path == "/v2/payments/captures/capture-1/refund":
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"id":"refund-1","status":"COMPLETED"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	gateway, err := NewPayPalGateway(server.URL, "client", "secret")
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := gateway.Charge(ctx, "paypal-contract", 125); err != nil {
		t.Fatalf("Charge() error=%v", err)
	}
	if err := gateway.Charge(ctx, "paypal-contract", 125); err != nil {
		t.Fatalf("replayed Charge() error=%v", err)
	}
	status, err := gateway.GetCharge(ctx, "paypal-contract")
	if err != nil || status != payment.ChargeStatusSucceeded {
		t.Fatalf("GetCharge()=(%q,%v), want succeeded,nil", status, err)
	}
	if captureCount != 1 {
		t.Fatalf("capture count=%d, want 1", captureCount)
	}
	if err := gateway.Refund(ctx, "paypal-contract", 125); err != nil {
		t.Fatalf("Refund() error=%v", err)
	}
}

func TestGatewayRegistryBuildsSelectedProvider(t *testing.T) {
	registry := NewGatewayRegistry()
	var received GatewaySettings
	if err := registry.Register("custom", func(settings GatewaySettings) (payment.Gateway, error) {
		received = settings
		return NewSimulatedGateway(0, 0, 0), nil
	}); err != nil {
		t.Fatal(err)
	}

	settings := GatewaySettings{DeclineRate: 0.1, TransientRate: 0.2, Latency: time.Second}
	gateway, err := registry.Build(" CUSTOM ", settings)
	if err != nil {
		t.Fatal(err)
	}
	if gateway == nil || received != settings {
		t.Fatalf("built gateway=%v received settings=%+v, want selected provider and %+v", gateway, received, settings)
	}
}

func TestGatewayRegistryRejectsInvalidRegistrationsAndUnknownProviders(t *testing.T) {
	registry := NewGatewayRegistry()
	for _, test := range []struct {
		name    string
		factory GatewayFactory
	}{
		{name: "", factory: func(GatewaySettings) (payment.Gateway, error) { return NewSimulatedGateway(0, 0, 0), nil }},
		{name: "nil factory"},
		{name: payment.ProviderSimulated, factory: func(GatewaySettings) (payment.Gateway, error) { return NewSimulatedGateway(0, 0, 0), nil }},
	} {
		if err := registry.Register(test.name, test.factory); err == nil {
			t.Errorf("Register(%q) succeeded, want error", test.name)
		}
	}
	if _, err := registry.Build("unregistered", GatewaySettings{}); err == nil {
		t.Fatal("Build() succeeded for an unregistered provider")
	}
}

func TestGatewayRegistryReportsFactoryErrorsAndNilResults(t *testing.T) {
	registry := NewGatewayRegistry()
	sentinel := errors.New("provider initialization failed")
	if err := registry.Register("error", func(GatewaySettings) (payment.Gateway, error) {
		return nil, sentinel
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Build("error", GatewaySettings{}); !errors.Is(err, sentinel) {
		t.Fatalf("Build() error=%v, want wrapped factory error", err)
	}
	if err := registry.Register("nil", func(GatewaySettings) (payment.Gateway, error) {
		return nil, nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Build("nil", GatewaySettings{}); err == nil {
		t.Fatal("Build() succeeded when provider returned a nil gateway")
	}
}

func TestGatewayContract(t *testing.T) {
	factories := map[string]func() (payment.Gateway, error){
		"simulated": func() (payment.Gateway, error) {
			return NewGatewayRegistry().Build(payment.ProviderSimulated, GatewaySettings{})
		},
		"resilient simulated": func() (payment.Gateway, error) {
			inner, err := NewGatewayRegistry().Build(payment.ProviderSimulated, GatewaySettings{})
			if err != nil {
				return nil, err
			}
			return NewResilientGateway(inner, 1, 0), nil
		},
	}

	for name, factory := range factories {
		t.Run(name, func(t *testing.T) {
			gateway, err := factory()
			if err != nil {
				t.Fatal(err)
			}
			assertGatewayContract(t, gateway)

			declining, err := NewGatewayRegistry().Build(payment.ProviderSimulated, GatewaySettings{DeclineRate: 1})
			if err != nil {
				t.Fatal(err)
			}
			if name == "resilient simulated" {
				declining = NewResilientGateway(declining, 1, 0)
			}
			if err := declining.Charge(context.Background(), "contract-declined", 100); !errors.Is(err, payment.ErrDeclined) {
				t.Fatalf("declined Charge() error=%v, want ErrDeclined", err)
			}
			status, err := declining.GetCharge(context.Background(), "contract-declined")
			if err != nil || status != payment.ChargeStatusDeclined {
				t.Fatalf("declined GetCharge()=(%q,%v), want declined,nil", status, err)
			}
		})
	}
}

func assertGatewayContract(t *testing.T, gateway payment.Gateway) {
	t.Helper()
	ctx := context.Background()

	if err := gateway.Charge(ctx, "contract-success", 100); err != nil {
		t.Fatalf("Charge() error=%v", err)
	}
	if err := gateway.Charge(ctx, "contract-success", 100); err != nil {
		t.Fatalf("replayed Charge() error=%v", err)
	}
	status, err := gateway.GetCharge(ctx, "contract-success")
	if err != nil || status != payment.ChargeStatusSucceeded {
		t.Fatalf("GetCharge()=(%q,%v), want succeeded,nil", status, err)
	}
	if err := gateway.Refund(ctx, "contract-success", 100); err != nil {
		t.Fatalf("Refund() error=%v", err)
	}
	if err := gateway.Refund(ctx, "contract-success", 100); err != nil {
		t.Fatalf("replayed Refund() error=%v", err)
	}
	status, err = gateway.GetCharge(ctx, "contract-missing")
	if err != nil || status != payment.ChargeStatusNotFound {
		t.Fatalf("missing GetCharge()=(%q,%v), want not found,nil", status, err)
	}
}
