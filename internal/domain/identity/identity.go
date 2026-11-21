package identity

import "errors"

type Role string

const (
	RoleUser  Role = "user"
	RoleAdmin Role = "admin"
)

var (
	ErrInvalid   = errors.New("invalid request identity")
	ErrForbidden = errors.New("request identity is not permitted")
)

type Identity struct {
	TenantID string
	UserID   string
	Role     Role
}

func (i Identity) IsValid() bool {
	return i.TenantID != "" && i.UserID != "" && (i.Role == RoleUser || i.Role == RoleAdmin)
}
