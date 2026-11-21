package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

func TestOrderBodyLimitReturns413(t *testing.T) {
	router := NewRouter(Deps{
		APIKeys:             map[string]identity.Identity{"test-key": {TenantID: "default", UserID: "user-1", Role: identity.RoleUser}},
		MaxRequestBodyBytes: 16,
	})
	request := httptest.NewRequest(http.MethodPost, "/api/v1/orders", strings.NewReader(`{"items":[{"quantity":1}]}`))
	request.Header.Set("X-API-Key", "test-key")
	request.Header.Set("Idempotency-Key", "body-limit-test")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()

	router.ServeHTTP(response, request)

	if response.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status=%d body=%s, want 413", response.Code, response.Body.String())
	}
}

func TestReadinessDoesNotExposeDependencyError(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := NewRouter(Deps{
		Readiness: map[string]func(context.Context) error{
			"database": func(context.Context) error { return errors.New("password=secret-value") },
		},
	})
	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d, want 503", response.Code)
	}
	if strings.Contains(response.Body.String(), "secret-value") || !strings.Contains(response.Body.String(), "unavailable") {
		t.Fatalf("readiness response leaks error or lacks generic status: %s", response.Body.String())
	}
}

func TestMetricsRequiresConfiguredToken(t *testing.T) {
	router := NewRouter(Deps{MetricsToken: "long-test-token"})

	response := httptest.NewRecorder()
	router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status=%d, want 401", response.Code)
	}
}

type orderAPIFake struct {
	listActor   identity.Identity
	page        int
	pageSize    int
	cancelActor identity.Identity
	cancelID    string
}

type paymentAPIFake struct {
	actor   identity.Identity
	orderID string
}

func (f *paymentAPIFake) RetryPayment(_ context.Context, actor identity.Identity, orderID string) (*payment.Payment, error) {
	f.actor, f.orderID = actor, orderID
	return &payment.Payment{ID: "payment-1", AttemptNumber: 2, Status: payment.StatusProcessing}, nil
}

func (*orderAPIFake) CreateOrder(context.Context, identity.Identity, string, []order.CreateItem) (*services.CreateOrderResult, error) {
	return nil, nil
}
func (*orderAPIFake) GetOrder(context.Context, identity.Identity, string) (*order.Order, error) {
	return nil, nil
}
func (f *orderAPIFake) ListOrders(_ context.Context, actor identity.Identity, page, pageSize int) (*services.OrderPage, error) {
	f.listActor, f.page, f.pageSize = actor, page, pageSize
	return &services.OrderPage{Orders: []*order.Order{}, Page: page, PageSize: pageSize}, nil
}
func (f *orderAPIFake) CancelOrder(_ context.Context, actor identity.Identity, id string) error {
	f.cancelActor, f.cancelID = actor, id
	return nil
}

func TestOrderListPaginationAndAuthentication(t *testing.T) {
	fake := &orderAPIFake{}
	paymentFake := &paymentAPIFake{}
	router := NewRouter(Deps{
		Orders:   fake,
		Payments: paymentFake,
		APIKeys: map[string]identity.Identity{
			"test-key":  {TenantID: "default", UserID: "user-1", Role: identity.RoleUser},
			"admin-key": {TenantID: "default", UserID: "admin-1", Role: identity.RoleAdmin},
		},
	})

	invalid := httptest.NewRecorder()
	router.ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, "/api/v1/orders?page=0", nil))
	if invalid.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d, want 401", invalid.Code)
	}

	request := httptest.NewRequest(http.MethodGet, "/api/v1/orders?page=2&page_size=5", nil)
	request.Header.Set("X-API-Key", "test-key")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s, want 200", response.Code, response.Body.String())
	}
	if fake.listActor != (identity.Identity{TenantID: "default", UserID: "user-1", Role: identity.RoleUser}) ||
		fake.page != 2 || fake.pageSize != 5 {
		t.Fatalf("list args=(%+v,%d,%d), want user identity and page 2/5", fake.listActor, fake.page, fake.pageSize)
	}

	badPage := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/v1/orders?page=0", nil)
	request.Header.Set("X-API-Key", "test-key")
	router.ServeHTTP(badPage, request)
	if badPage.Code != http.StatusBadRequest {
		t.Fatalf("invalid page status=%d, want 400", badPage.Code)
	}

	badPageSize := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/v1/orders?page_size=101", nil)
	request.Header.Set("X-API-Key", "test-key")
	router.ServeHTTP(badPageSize, request)
	if badPageSize.Code != http.StatusBadRequest {
		t.Fatalf("invalid page_size status=%d, want 400", badPageSize.Code)
	}

	cancel := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/v1/orders/order-id/cancel", nil)
	request.Header.Set("X-API-Key", "test-key")
	router.ServeHTTP(cancel, request)
	if cancel.Code != http.StatusNoContent || fake.cancelActor.UserID != "user-1" || fake.cancelID != "order-id" {
		t.Fatalf("cancel status=%d args=(%+v,%q), want 204 for user-1/order-id", cancel.Code, fake.cancelActor, fake.cancelID)
	}

	forbidden := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/orders", nil)
	request.Header.Set("X-API-Key", "test-key")
	router.ServeHTTP(forbidden, request)
	if forbidden.Code != http.StatusForbidden {
		t.Fatalf("user calling admin route status=%d, want 403", forbidden.Code)
	}

	adminResponse := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/api/v1/admin/orders", nil)
	request.Header.Set("X-API-Key", "admin-key")
	router.ServeHTTP(adminResponse, request)
	if adminResponse.Code != http.StatusOK || fake.listActor.Role != identity.RoleAdmin {
		t.Fatalf("admin list status=%d actor=%+v, want 200 and admin identity", adminResponse.Code, fake.listActor)
	}

	userRetry := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/v1/admin/orders/order-id/retry-payment", nil)
	request.Header.Set("X-API-Key", "test-key")
	router.ServeHTTP(userRetry, request)
	if userRetry.Code != http.StatusForbidden {
		t.Fatalf("user retry status=%d, want 403", userRetry.Code)
	}

	adminRetry := httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodPost, "/api/v1/admin/orders/order-id/retry-payment", nil)
	request.Header.Set("X-API-Key", "admin-key")
	router.ServeHTTP(adminRetry, request)
	if adminRetry.Code != http.StatusAccepted || paymentFake.actor.Role != identity.RoleAdmin || paymentFake.orderID != "order-id" {
		t.Fatalf("admin retry status=%d actor=%+v id=%q, want 202 for tenant admin", adminRetry.Code, paymentFake.actor, paymentFake.orderID)
	}
}
