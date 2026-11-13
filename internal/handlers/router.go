package handlers

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/BrianObonyo/order-processing-system/internal/middleware"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

type Deps struct {
	Orders   *services.OrderService
	Products *services.ProductService
	APIKeys  map[string]string
	Locker   middleware.Locker // may be nil
	// Readiness checks run by /readyz, keyed by dependency name.
	Readiness map[string]func(context.Context) error
}

func NewRouter(d Deps) *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery(), gin.Logger(), middleware.Metrics())

	r.GET("/healthz", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/readyz", func(c *gin.Context) {
		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()
		status, code := gin.H{}, http.StatusOK
		for name, check := range d.Readiness {
			if err := check(ctx); err != nil {
				status[name], code = err.Error(), http.StatusServiceUnavailable
			} else {
				status[name] = "ok"
			}
		}
		c.JSON(code, status)
	})
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	orders := &OrderHandler{svc: d.Orders}
	products := &ProductHandler{svc: d.Products}

	v1 := r.Group("/api/v1", middleware.APIKeyAuth(d.APIKeys))
	v1.GET("/products", products.List)
	v1.POST("/orders", middleware.Idempotency(d.Locker), orders.Create)
	v1.GET("/orders/:id", orders.Get)
	return r
}
