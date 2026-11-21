package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
)

const (
	identityKey = "identity"
	idemKey     = "idempotency_key"
)

func abort(c *gin.Context, status int, code, msg string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": msg}})
}

// APIKeyAuth authenticates the X-API-Key header to a tenant-scoped identity.
func APIKeyAuth(keys map[string]identity.Identity) gin.HandlerFunc {
	return func(c *gin.Context) {
		id, ok := keys[c.GetHeader("X-API-Key")]
		if !ok || !id.IsValid() {
			abort(c, http.StatusUnauthorized, "unauthorized", "missing or invalid X-API-Key header")
			return
		}
		c.Set(identityKey, id)
		c.Next()
	}
}

func RequestIdentity(c *gin.Context) identity.Identity {
	id, _ := c.Get(identityKey)
	result, _ := id.(identity.Identity)
	return result
}

func RequireRole(role identity.Role) gin.HandlerFunc {
	return func(c *gin.Context) {
		if RequestIdentity(c).Role != role {
			abort(c, http.StatusForbidden, "forbidden", "this API key does not have permission for this operation")
			return
		}
		c.Next()
	}
}
