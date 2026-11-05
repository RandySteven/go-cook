// Package redis_client provides a Redis and Valkey client wrapper with caching,
// rate limiting, and connection management. Valkey speaks the same RESP
// protocol as Redis, so both servers use this client. JSON serialization is
// used for stored values.
package redis_client

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

var (
	redisTimeout = os.Getenv("REDIS_EXPIRATION")
	client       *redis.Client
	limiter      *redis_rate.Limiter
	rateLimiter  = os.Getenv("RATE_LIMITER")
)

type (
	RedisConfig struct {
		Username string
		Host     string
		Port     string
		Password string

		// DB selects the logical database. Zero uses database 0, matching
		// earlier releases.
		DB int

		// Protocol is the RESP version to negotiate: 2 or 3.
		// Zero keeps the default of 3. Redis 6+ and Valkey accept HELLO;
		// a server that rejects HELLO stays on RESP2, so Redis 5 still works.
		Protocol int

		// TLSConfig enables TLS when set. Nil keeps a plaintext connection,
		// which is the default for Redis and Valkey.
		TLSConfig *tls.Config

		// DisableIdentity skips CLIENT SETINFO on connect. Redis before 7.2
		// does not implement that command. Valkey 7.2+ and Redis 7.2+ do.
		// Leave false unless a proxy treats the unsupported command as fatal.
		DisableIdentity bool

		// MaintNotifications turns on Redis Cloud maintenance push notifications.
		// Valkey and Redis OSS do not implement CLIENT MAINT_NOTIFICATIONS, so
		// the default leaves that handshake off.
		MaintNotifications bool

		PoolSize    int
		MinIdleConn int
		MaxIdleConn int
		PoolTimeout int

		DialTimeout  int
		ReadTimeout  int
		WriteTimeout int

		MaxRetries      int
		MinRetryBackoff int
		MaxRetryBackoff int

		ConnMaxIdleTime int
		ConnMaxLifeTime int
	}

	// redisClient is the internal implementation of the Redis interface.
	redisClient struct {
		client  *redis.Client
		limiter *redis_rate.Limiter
	}
)

// NewRedisClient creates a client for Redis. The same options work for Valkey;
// NewValkeyClient is the named constructor for that server.
func NewRedisClient(redisCfg *RedisConfig) (*redisClient, error) {
	return newClient(redisCfg)
}

// NewValkeyClient creates a client for Valkey. Cache, rate-limit, and geo calls
// match NewRedisClient. Existing RedisConfig values keep their meaning: plaintext
// unless TLSConfig is set, RESP3 unless Protocol is 2, and no Redis Cloud
// maintenance handshake unless MaintNotifications is set.
func NewValkeyClient(cfg *RedisConfig) (*redisClient, error) {
	return newClient(cfg)
}

func newClient(cfg *RedisConfig) (*redisClient, error) {
	if cfg == nil {
		return nil, errors.New("redis config is nil")
	}
	opt, err := cfg.clientOptions()
	if err != nil {
		return nil, err
	}
	log.Println("connecting to redis : ", opt.Addr)

	client = redis.NewClient(opt)
	limiter = redis_rate.NewLimiter(client)
	return &redisClient{
		client:  client,
		limiter: limiter,
	}, nil
}

func (cfg *RedisConfig) clientOptions() (*redis.Options, error) {
	switch cfg.Protocol {
	case 0, 2, 3:
	default:
		return nil, fmt.Errorf("redis protocol must be 2 or 3, got %d", cfg.Protocol)
	}

	protocol := cfg.Protocol
	if protocol == 0 {
		protocol = 3
	}

	maint := &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	if cfg.MaintNotifications {
		maint = &maintnotifications.Config{Mode: maintnotifications.ModeAuto}
	}

	return &redis.Options{
		Addr:     fmt.Sprintf("%s:%s", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.DB,
		Username: cfg.Username,

		Protocol: protocol,

		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConn,
		MaxIdleConns: cfg.MaxIdleConn,
		PoolTimeout:  time.Duration(cfg.PoolTimeout) * time.Second,

		DialTimeout:  time.Duration(cfg.DialTimeout) * time.Second,
		ReadTimeout:  time.Duration(cfg.ReadTimeout) * time.Millisecond,
		WriteTimeout: time.Duration(cfg.WriteTimeout) * time.Millisecond,

		MaxRetries:      cfg.MaxRetries,
		MinRetryBackoff: time.Duration(cfg.MinRetryBackoff) * time.Millisecond,
		MaxRetryBackoff: time.Duration(cfg.MaxRetryBackoff) * time.Millisecond,
		ConnMaxIdleTime: time.Duration(cfg.ConnMaxIdleTime) * time.Minute,
		ConnMaxLifetime: time.Duration(cfg.ConnMaxLifeTime) * time.Minute,

		TLSConfig:                cfg.TLSConfig,
		DisableIdentity:          cfg.DisableIdentity,
		MaintNotificationsConfig: maint,
	}, nil
}

// Ping checks if the Redis connection is alive by sending a PING command.
func (c *redisClient) Ping() error {
	return c.client.Ping(context.Background()).Err()
}

// Client returns the underlying *redis.Client for direct Redis access.
func (c *redisClient) Client() *redis.Client {
	return c.client
}

// ClearCache flushes all keys from the current Redis database.
func (c *redisClient) ClearCache(ctx context.Context) error {
	return c.client.FlushDB(ctx).Err()
}

// RateLimiter enforces rate limiting for the most recently created client.
// Prefer (*redisClient).RateLimiter when a process talks to more than one
// Redis or Valkey server. The limit is RATE_LIMITER requests per minute,
// keyed by the X-Client-IP context value.
func RateLimiter(ctx context.Context) error {
	return allow(ctx, limiter)
}

// RateLimiter enforces the same per-IP limit against this client.
func (c *redisClient) RateLimiter(ctx context.Context) error {
	return allow(ctx, c.limiter)
}

func allow(ctx context.Context, lim *redis_rate.Limiter) error {
	rateLimiterInt, _ := strconv.Atoi(rateLimiter)
	clientIP := ctx.Value("X-Client-IP").(string)
	res, err := lim.Allow(ctx, clientIP, redis_rate.PerMinute(rateLimiterInt))
	if err != nil {
		return err
	}
	if res.Allowed == 0 {
		return errors.New("Rate limit exceeded")
	}
	return nil
}
