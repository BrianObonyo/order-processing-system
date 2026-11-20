package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

func TestOrderBodyLimitReturns413(t *testing.T) {
	router := NewRouter(Deps{
		APIKeys:             map[string]string{"test-key": "user-1"},
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
	listUser   string
	page       int
	pageSize   int
	cancelUser string
	cancelID   string
}

func (*orderAPIFake) CreateOrder(context.Context, string, string, []order.CreateItem) (*services.CreateOrderResult, error) {
	return nil, nil
}
func (*orderAPIFake) GetOrder(context.Context, string, string) (*order.Order, error) {
	return nil, nil
}
func (f *orderAPIFake) ListOrders(_ context.Context, userID string, page, pageSize int) (*services.OrderPage, error) {
	f.listUser, f.page, f.pageSize = userID, page, pageSize
	return &services.OrderPage{Orders: []*order.Order{}, Page: page, PageSize: pageSize}, nil
}
func (f *orderAPIFake) CancelOrder(_ context.Context, userID, id string) error {
	f.cancelUser, f.cancelID = userID, id
	return nil
}

func TestOrderListPaginationAndAuthentication(t *testing.T) {
	fake := &orderAPIFake{}
	router := NewRouter(Deps{
		Orders:  fake,
		APIKeys: map[string]string{"test-key": "user-1"},
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
	if fake.listUser != "user-1" || fake.page != 2 || fake.pageSize != 5 {
		t.Fatalf("list args=(%q,%d,%d), want (user-1,2,5)", fake.listUser, fake.page, fake.pageSize)
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
	if cancel.Code != http.StatusNoContent || fake.cancelUser != "user-1" || fake.cancelID != "order-id" {
		t.Fatalf("cancel status=%d args=(%q,%q), want 204 for user-1/order-id", cancel.Code, fake.cancelUser, fake.cancelID)
	}
}
