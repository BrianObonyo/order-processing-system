package handlers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
)

type ProductAPI interface {
	List(context.Context) ([]inventory.ProductStock, error)
}

type ProductHandler struct{ svc ProductAPI }

func (h *ProductHandler) List(c *gin.Context) {
	products, err := h.svc.List(c.Request.Context())
	if err != nil {
		handleServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"products": products})
}
