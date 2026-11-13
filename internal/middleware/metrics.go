package middleware

import (
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/metrics"
)

func Metrics() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()
		route := c.FullPath()
		if route == "" {
			route = "unmatched"
		}
		metrics.HTTPDuration.WithLabelValues(c.Request.Method, route, strconv.Itoa(c.Writer.Status())).
			Observe(time.Since(start).Seconds())
	}
}
