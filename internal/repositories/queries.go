// Package repositories contains all SQL. Queries works against either a pool or
// a transaction, so services decide the transaction boundary.
package repositories

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type DBTX interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Queries struct{ db DBTX }

func New(db DBTX) *Queries { return &Queries{db: db} }
