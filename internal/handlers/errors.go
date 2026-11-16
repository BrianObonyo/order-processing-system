package handlers

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/middleware"
)

func respondError(c *gin.Context, status int, code, msg string, extra ...any) {
	body := gin.H{"code": code, "message": msg}
	for i := 0; i+1 < len(extra); i += 2 {
		body[extra[i].(string)] = extra[i+1]
	}
	c.JSON(status, gin.H{"error": body})
}

func handleServiceError(c *gin.Context, err error) {
	var stock *inventory.InsufficientStockError
	switch {
	case errors.As(err, &stock):
		respondError(c, http.StatusConflict, "insufficient_stock", err.Error(), "product_id", stock.ProductID)
	case errors.Is(err, inventory.ErrUnknownProduct):
		respondError(c, http.StatusUnprocessableEntity, "unknown_product", err.Error())
	case errors.Is(err, order.ErrInvalidItems):
		respondError(c, http.StatusBadRequest, "invalid_items", err.Error())
	case errors.Is(err, order.ErrIdempotencyMismatch):
		respondError(c, http.StatusUnprocessableEntity, "idempotency_key_reuse", err.Error())
	case errors.Is(err, order.ErrNotFound):
		respondError(c, http.StatusNotFound, "order_not_found", "order not found")
	default:
		slog.Error("unhandled error", "request_id", middleware.RequestIDValue(c), "path", c.FullPath(), "err", err)
		respondError(c, http.StatusInternalServerError, "internal_error", "something went wrong")
	}
}
