package middleware

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type Locker interface {
	Acquire(ctx context.Context, key string, ttl time.Duration) (release func(), ok bool, err error)
}

// Idempotency requires an Idempotency-Key header and takes a short Redis lock on
// (user, key) so duplicate in-flight requests are rejected cheaply with 409.
// This is an optimisation only: the database claims the key transactionally, so
// duplicates stay safe even if Redis is down (the lock fails open).
func Idempotency(locker Locker) gin.HandlerFunc {
	return func(c *gin.Context) {
		key := strings.TrimSpace(c.GetHeader("Idempotency-Key"))
		if len(key) < 8 || len(key) > 128 {
			abort(c, http.StatusBadRequest, "invalid_idempotency_key",
				"Idempotency-Key header is required and must be 8-128 characters")
			return
		}
		c.Set(idemKey, key)

		if locker != nil {
			release, ok, err := locker.Acquire(c.Request.Context(), "idem:"+UserID(c)+":"+key, 30*time.Second)
			switch {
			case err != nil:
				slog.Warn("idempotency lock unavailable, relying on database", "err", err)
			case !ok:
				abort(c, http.StatusConflict, "request_in_progress",
					"a request with this Idempotency-Key is already being processed")
				return
			default:
				defer release()
			}
		}
		c.Next()
	}
}

func IdempotencyKey(c *gin.Context) string { return c.GetString(idemKey) }
