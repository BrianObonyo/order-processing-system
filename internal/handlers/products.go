package handlers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/inventory"
	"github.com/BrianObonyo/order-processing-system/internal/middleware"
)

type ProductAPI interface {
	List(context.Context, string) ([]inventory.ProductStock, error)
}

type ProductHandler struct{ svc ProductAPI }

func (h *ProductHandler) List(c *gin.Context) {
	products, err := h.svc.List(c.Request.Context(), middleware.RequestIdentity(c).TenantID)
	if err != nil {
		handleServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"products": products})
}
