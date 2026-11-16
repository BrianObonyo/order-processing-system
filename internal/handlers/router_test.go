package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
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
