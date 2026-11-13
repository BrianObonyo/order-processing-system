package handlers

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/services"
)

type ProductHandler struct{ svc *services.ProductService }

func (h *ProductHandler) List(c *gin.Context) {
	products, err := h.svc.List(c.Request.Context())
	if err != nil {
		handleServiceError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"products": products})
}
