package handlers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/middleware"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

type OrderHandler struct{ svc *services.OrderService }

type orderItemRequest struct {
	ProductID string `json:"product_id" binding:"required,uuid"`
	Quantity  int    `json:"quantity" binding:"required,min=1,max=100"`
}

type createOrderRequest struct {
	Items []orderItemRequest `json:"items" binding:"required,min=1,max=50,dive"`
}

func (h *OrderHandler) Create(c *gin.Context) {
	var req createOrderRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			respondError(c, http.StatusRequestEntityTooLarge, "request_too_large", "request body exceeds the allowed size")
			return
		}
		respondError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	items := make([]order.CreateItem, len(req.Items))
	for i, it := range req.Items {
		items[i] = order.CreateItem{ProductID: it.ProductID, Quantity: it.Quantity}
	}

	res, err := h.svc.CreateOrder(c.Request.Context(), middleware.UserID(c), middleware.IdempotencyKey(c), items)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	status := http.StatusCreated
	if res.Replayed {
		status = http.StatusOK
		c.Header("Idempotent-Replayed", "true")
	}
	c.JSON(status, res.Order)
}

func (h *OrderHandler) Get(c *gin.Context) {
	o, err := h.svc.GetOrder(c.Request.Context(), middleware.UserID(c), c.Param("id"))
	if err != nil {
		handleServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, o)
}
