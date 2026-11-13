package redis

import (
	"context"
	"time"

	"github.com/google/uuid"
	goredis "github.com/redis/go-redis/v9"
)

// Client wraps Redis for coordination only (short-lived locks). Nothing stored
// here is a source of truth, so callers fail open when Redis is unavailable.
type Client struct{ rdb *goredis.Client }

func New(addr string) *Client {
	return &Client{rdb: goredis.NewClient(&goredis.Options{
		Addr:         addr,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})}
}

func (c *Client) Ping(ctx context.Context) error { return c.rdb.Ping(ctx).Err() }
func (c *Client) Close() error                   { return c.rdb.Close() }

var releaseScript = goredis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0`)

// Acquire takes a lock with a TTL. The returned release function only deletes
// the key if this caller still owns it.
func (c *Client) Acquire(ctx context.Context, key string, ttl time.Duration) (func(), bool, error) {
	token := uuid.NewString()
	ok, err := c.rdb.SetNX(ctx, key, token, ttl).Result()
	if err != nil || !ok {
		return nil, false, err
	}
	release := func() {
		rctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = releaseScript.Run(rctx, c.rdb, []string{key}, token).Err()
	}
	return release, true, nil
}
