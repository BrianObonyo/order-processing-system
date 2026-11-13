package middleware

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

const (
	userKey = "user_id"
	idemKey = "idempotency_key"
)

func abort(c *gin.Context, status int, code, msg string) {
	c.AbortWithStatusJSON(status, gin.H{"error": gin.H{"code": code, "message": msg}})
}

// APIKeyAuth authenticates the X-API-Key header against a key -> user map.
func APIKeyAuth(keys map[string]string) gin.HandlerFunc {
	return func(c *gin.Context) {
		user, ok := keys[c.GetHeader("X-API-Key")]
		if !ok {
			abort(c, http.StatusUnauthorized, "unauthorized", "missing or invalid X-API-Key header")
			return
		}
		c.Set(userKey, user)
		c.Next()
	}
}

func UserID(c *gin.Context) string { return c.GetString(userKey) }
