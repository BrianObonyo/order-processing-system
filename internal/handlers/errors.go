package handlers

import (
	"errors"
	"log/slog"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/domain/payment"
	"github.com/BrianObonyo/order-processing-system/internal/middleware"
	"github.com/BrianObonyo/order-processing-system/internal/services"
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
	case errors.Is(err, order.ErrInvalidStatus):
		respondError(c, http.StatusBadRequest, "invalid_status", "status must be one of PENDING_PAYMENT, PAID, CANCELLED")
	case errors.Is(err, order.ErrIdempotencyMismatch):
		respondError(c, http.StatusUnprocessableEntity, "idempotency_key_reuse", err.Error())
	case errors.Is(err, order.ErrNotFound):
		respondError(c, http.StatusNotFound, "order_not_found", "order not found")
	case errors.Is(err, services.ErrOrderNotCancelable):
		respondError(c, http.StatusConflict, "order_not_cancelable", "order cannot be cancelled in its current state")
	case errors.Is(err, services.ErrInvalidPagination):
		respondError(c, http.StatusBadRequest, "invalid_pagination", "pagination values are out of range")
	case errors.Is(err, payment.ErrNoRetryableAttempt):
		respondError(c, http.StatusConflict, "payment_not_retryable", "order has no declined payment attempt to retry")
	case errors.Is(err, identity.ErrForbidden):
		respondError(c, http.StatusForbidden, "forbidden", "this API key does not have permission for this operation")
	default:
		slog.Error("unhandled error", "request_id", middleware.RequestIDValue(c), "path", c.FullPath(), "err", err)
		respondError(c, http.StatusInternalServerError, "internal_error", "something went wrong")
	}
}
