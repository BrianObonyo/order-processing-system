package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

func TestRequestIDIsReturnedAndRateLimitIsApplied(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(RequestID(), RateLimit(1, 1))
	router.GET("/resource", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	router.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusNoContent) })

	first := httptest.NewRecorder()
	router.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/resource", nil))
	if first.Code != http.StatusNoContent {
		t.Fatalf("first request status=%d, want 204", first.Code)
	}
	if _, err := uuid.Parse(first.Header().Get("X-Request-ID")); err != nil {
		t.Fatalf("X-Request-ID is not a UUID: %v", err)
	}

	second := httptest.NewRecorder()
	router.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/resource", nil))
	if second.Code != http.StatusTooManyRequests || second.Header().Get("Retry-After") == "" {
		t.Fatalf("second request status=%d Retry-After=%q, want 429 and Retry-After", second.Code, second.Header().Get("Retry-After"))
	}

	health := httptest.NewRecorder()
	router.ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusNoContent {
		t.Fatalf("health request status=%d, want 204 despite exhausted limiter", health.Code)
	}
}

func TestMetricsTokenAuth(t *testing.T) {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/metrics", MetricsTokenAuth("long-test-token"), func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	unauthorized := httptest.NewRecorder()
	router.ServeHTTP(unauthorized, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status=%d, want 401", unauthorized.Code)
	}

	authorized := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	request.Header.Set("Authorization", "Bearer long-test-token")
	router.ServeHTTP(authorized, request)
	if authorized.Code != http.StatusNoContent {
		t.Fatalf("authorized status=%d, want 204", authorized.Code)
	}
}
