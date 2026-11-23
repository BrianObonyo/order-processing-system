package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/gin-gonic/gin"

	"github.com/BrianObonyo/order-processing-system/internal/domain/identity"
)

func OIDCAuth(issuer, audience string) (gin.HandlerFunc, error) {
	provider, err := oidc.NewProvider(context.Background(), issuer)
	if err != nil {
		return nil, err
	}
	verifier := provider.Verifier(&oidc.Config{ClientID: audience})

	return func(c *gin.Context) {
		authorization := strings.TrimSpace(c.GetHeader("Authorization"))
		const bearerPrefix = "Bearer "
		if len(authorization) <= len(bearerPrefix) || !strings.EqualFold(authorization[:len(bearerPrefix)], bearerPrefix) {
			abort(c, http.StatusUnauthorized, "unauthorized", "missing or invalid bearer token")
			return
		}
		token, err := verifier.Verify(c.Request.Context(), strings.TrimSpace(authorization[len(bearerPrefix):]))
		if err != nil {
			abort(c, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
			return
		}
		var claims struct {
			Subject  string   `json:"sub"`
			UserID   string   `json:"user_id"`
			TenantID string   `json:"tenant_id"`
			TID      string   `json:"tid"`
			Role     string   `json:"role"`
			Roles    []string `json:"roles"`
		}
		if err := token.Claims(&claims); err != nil {
			abort(c, http.StatusUnauthorized, "unauthorized", "invalid token claims")
			return
		}
		tenantID := strings.TrimSpace(claims.TenantID)
		if tenantID == "" {
			tenantID = strings.TrimSpace(claims.TID)
		}
		userID := strings.TrimSpace(claims.UserID)
		if userID == "" {
			userID = strings.TrimSpace(claims.Subject)
		}
		roleName := strings.ToLower(strings.TrimSpace(claims.Role))
		if roleName == "" {
			for _, role := range claims.Roles {
				roleName = strings.ToLower(strings.TrimSpace(role))
				if roleName == "admin" || roleName == "user" {
					break
				}
			}
		}
		id := identity.Identity{TenantID: tenantID, UserID: userID, Role: identity.Role(roleName)}
		if !id.IsValid() {
			abort(c, http.StatusForbidden, "forbidden", "token is missing tenant_id, user_id, or a valid role")
			return
		}
		c.Set(identityKey, id)
		c.Next()
	}, nil
}
