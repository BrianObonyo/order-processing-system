package handlers

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/middleware"
)

type Deps struct {
	Orders              OrderAPI
	Products            ProductAPI
	Payments            PaymentAPI
	APIKeys             map[string]identity.Identity
	MetricsToken        string
	Locker              middleware.Locker // may be nil
	MaxRequestBodyBytes int64
	HTTPRateLimit       float64
	HTTPRateBurst       int
	// Readiness checks run by /readyz, keyed by dependency name.
	Readiness map[string]func(context.Context) error
}

func NewRouter(d Deps) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(
		middleware.RequestID(),
		middleware.AccessLog(),
		gin.CustomRecovery(func(c *gin.Context, recovered any) {
			slog.Error("http panic recovered", "request_id", middleware.RequestIDValue(c), "err", recovered)
			respondError(c, http.StatusInternalServerError, "internal_error", "something went wrong")
		}),
		middleware.RateLimit(defaultRateLimit(d.HTTPRateLimit, d.HTTPRateBurst)),
		middleware.Metrics(),
	)

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		status, code := gin.H{}, http.StatusOK
		for name, check := range d.Readiness {
			if err := check(ctx); err != nil {
				slog.Error("readiness check failed",
					"request_id", middleware.RequestIDValue(c), "dependency", name, "err", err)
				status[name], code = "unavailable", http.StatusServiceUnavailable
			} else {
				status[name] = "ok"
			}
		}
		c.JSON(code, status)
	})
	metricsHandler := gin.WrapH(promhttp.Handler())
	if d.MetricsToken != "" {
		r.GET("/metrics", middleware.MetricsTokenAuth(d.MetricsToken), metricsHandler)
	} else {
		r.GET("/metrics", metricsHandler)
	}

	orders := &OrderHandler{svc: d.Orders}
	products := &ProductHandler{svc: d.Products}

	v1 := r.Group("/api/v1", middleware.APIKeyAuth(d.APIKeys))
	v1.GET("/products", products.List)
	maxBodyBytes := d.MaxRequestBodyBytes
	if maxBodyBytes <= 0 {
		maxBodyBytes = 1 << 20
	}
	v1.POST("/orders", middleware.MaxRequestBodyBytes(maxBodyBytes), middleware.Idempotency(d.Locker), orders.Create)
	v1.GET("/orders", orders.List)
	v1.POST("/orders/:id/cancel", orders.Cancel)
	v1.GET("/orders/:id", orders.Get)
	admin := v1.Group("/admin", middleware.RequireRole(identity.RoleAdmin))
	admin.GET("/orders", orders.List)
	admin.GET("/orders/:id", orders.Get)
	admin.POST("/orders/:id/cancel", orders.Cancel)
	if d.Payments != nil {
		admin.POST("/orders/:id/retry-payment", func(c *gin.Context) { orders.RetryPayment(c, d.Payments) })
	}
	return r
}

func defaultRateLimit(rate float64, burst int) (float64, int) {
	if rate <= 0 || burst <= 0 {
		return 200, 400
	}
	return rate, burst
}
