// Package redis wraps go-redis with functional options and a small helper
// for distributed-lock operations used by the scheduler.
package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/go-redis/redis/v8"
)

// Client wraps goredis.Client with application-specific helpers.
type Client struct {
	*goredis.Client
}

// Options configures the Redis client.
type Options struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// Option is a functional option for Options.
type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		Addr:         "localhost:6379",
		DB:           0,
		PoolSize:     20,
		MinIdleConns: 5,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// WithAddr sets the Redis server address.
func WithAddr(addr string) Option { return func(o *Options) { o.Addr = addr } }

// WithPassword sets the Redis AUTH password.
func WithPassword(pwd string) Option { return func(o *Options) { o.Password = pwd } }

// WithDB selects the Redis database index.
func WithDB(db int) Option { return func(o *Options) { o.DB = db } }

// WithPoolSize sets the connection pool size.
func WithPoolSize(n int) Option { return func(o *Options) { o.PoolSize = n } }

// WithMinIdleConns sets the minimum number of idle connections.
func WithMinIdleConns(n int) Option { return func(o *Options) { o.MinIdleConns = n } }

// WithDialTimeout sets the connection dial timeout.
func WithDialTimeout(d time.Duration) Option { return func(o *Options) { o.DialTimeout = d } }

// WithReadTimeout sets the read timeout.
func WithReadTimeout(d time.Duration) Option { return func(o *Options) { o.ReadTimeout = d } }

// WithWriteTimeout sets the write timeout.
func WithWriteTimeout(d time.Duration) Option { return func(o *Options) { o.WriteTimeout = d } }

// New creates and pings a new Redis Client.
func New(opts ...Option) (*Client, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	rdb := goredis.NewClient(&goredis.Options{
		Addr:         o.Addr,
		Password:     o.Password,
		DB:           o.DB,
		PoolSize:     o.PoolSize,
		MinIdleConns: o.MinIdleConns,
		DialTimeout:  o.DialTimeout,
		ReadTimeout:  o.ReadTimeout,
		WriteTimeout: o.WriteTimeout,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis: ping failed: %w", err)
	}

	return &Client{Client: rdb}, nil
}

// MustNew is like New but panics on error.
func MustNew(opts ...Option) *Client {
	c, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return c
}

// ─── Distributed lock helpers ─────────────────────────────────────────────────

const lockScript = `
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end`

// TryLock attempts to acquire a Redis SET NX PX lock.
// Returns true if the lock was acquired, false otherwise.
func (c *Client) TryLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	ok, err := c.SetNX(ctx, key, value, ttl).Result()
	return ok, err
}

// ReleaseLock releases a lock only if the stored value matches (prevents releasing someone else's lock).
func (c *Client) ReleaseLock(ctx context.Context, key, value string) error {
	script := goredis.NewScript(lockScript)
	result, err := script.Run(ctx, c.Client, []string{key}, value).Int()
	if err != nil {
		return fmt.Errorf("redis: release lock %q: %w", key, err)
	}
	if result == 0 {
		return fmt.Errorf("redis: lock %q not owned by %q", key, value)
	}
	return nil
}

// ExtendLock resets the TTL of an existing lock (only if still owned).
func (c *Client) ExtendLock(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	extendScript := goredis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end`)
	result, err := extendScript.Run(ctx, c.Client, []string{key}, value, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("redis: extend lock %q: %w", key, err)
	}
	return result == 1, nil
}
