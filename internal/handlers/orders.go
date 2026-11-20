package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/order"
	"github.com/BrianObonyo/order-processing-system/internal/middleware"
	"github.com/BrianObonyo/order-processing-system/internal/services"
)

type OrderAPI interface {
	CreateOrder(context.Context, string, string, []order.CreateItem) (*services.CreateOrderResult, error)
	GetOrder(context.Context, string, string) (*order.Order, error)
	ListOrders(context.Context, string, int, int) (*services.OrderPage, error)
	CancelOrder(context.Context, string, string) error
}

type OrderHandler struct{ svc OrderAPI }

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

func (h *OrderHandler) List(c *gin.Context) {
	page, pageSize, ok := parsePagination(c)
	if !ok {
		return
	}
	result, err := h.svc.ListOrders(c.Request.Context(), middleware.UserID(c), page, pageSize)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, result)
}

func parsePagination(c *gin.Context) (int, int, bool) {
	page, err := parsePositiveQuery(c, "page", 1)
	if err != nil {
		respondError(c, http.StatusBadRequest, "invalid_pagination", "page must be a positive integer")
		return 0, 0, false
	}
	pageSize, err := parsePositiveQuery(c, "page_size", 20)
	if err != nil || pageSize > 100 {
		respondError(c, http.StatusBadRequest, "invalid_pagination", "page_size must be an integer from 1 to 100")
		return 0, 0, false
	}
	return page, pageSize, true
}

func parsePositiveQuery(c *gin.Context, key string, fallback int) (int, error) {
	value := c.Query(key)
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < 1 {
		return 0, errors.New("must be a positive integer")
	}
	return parsed, nil
}

func (h *OrderHandler) Cancel(c *gin.Context) {
	if err := h.svc.CancelOrder(c.Request.Context(), middleware.UserID(c), c.Param("id")); err != nil {
		handleServiceError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
