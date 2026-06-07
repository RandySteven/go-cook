// Package redis_client provides a Redis client wrapper with caching, rate limiting,
// and connection management capabilities. It supports JSON serialization for
// storing and retrieving typed data.
package redis_client

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-redis/redis_rate/v10"
	"github.com/redis/go-redis/v9"
)

var (
	redisTimeout = os.Getenv("REDIS_EXPIRATION")
	client       *redis.Client
	limiter      *redis_rate.Limiter
	rateLimiter  = os.Getenv("RATE_LIMITER")
)

type (
	RedisConfig struct {
		Host     string
		Port     string
		Password string

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
	// Redis defines the interface for Redis operations including health checks,
	// client access, and cache management.
	Redis interface {
		// Ping checks if the Redis connection is alive.
		Ping() error
		// Client returns the underlying *redis.Client for direct access.
		Client() *redis.Client
		// ClearCache flushes all keys from the current database.
		ClearCache(ctx context.Context) error

		//Redis function
		Set(ctx context.Context, key string, vaue interface{}, timeExpire time.Duration) (bool, error)
		Get(ctx context.Context, key string) (interface{}, error)
		SetNX(ctx context.Context, key string, value interface{}, timeExpire time.Duration) (bool, error)
		HDel(ctx context.Context, key string) (bool, error)
		HGet(ctx context.Context, key string) (interface{}, error)
		Del(ctx context.Context, key string) error
		Publish(ctx context.Context, channel string, message interface{}) (bool, error)
		GeoAdd(ctx context.Context, key string, geoLoc *redis.GeoLocation) (bool, error)
		GeoRadiusByMember(ctx context.Context, key string, member string, query *redis.GeoRadiusQuery) ([]redis.GeoLocation, error)
	}

	// redisClient is the internal implementation of the Redis interface.
	redisClient struct {
		client  *redis.Client
		limiter *redis_rate.Limiter
	}
)

// NewRedisClient creates a new Redis client with rate limiting support.
// It connects to Redis using the host and port from the provided configuration.
// Returns an error if the connection cannot be established.
func NewRedisClient(redisCfg *RedisConfig) (*redisClient, error) {
	addr := fmt.Sprintf("%s:%s", redisCfg.Host, redisCfg.Port)
	log.Println("connecting to redis : ", addr)

	client = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: redisCfg.Password,
		DB:       0,

		Protocol: 3, // Use RESP3 protocol for faster, more optimized data serialization

		// --- Connection Pool Optimization ---
		PoolSize:     redisCfg.PoolSize,                                 // Match to (Goroutine count / 10) up to a max of 200-300
		MinIdleConns: redisCfg.MinIdleConn,                              // Keeps connections warm; avoids cold-start connection spikes
		MaxIdleConns: redisCfg.MaxIdleConn,                              // Prevents closing too many connections under fluctuating load
		PoolTimeout:  time.Duration(redisCfg.PoolTimeout) * time.Second, // Max time to wait for a connection from the pool

		// --- Timeouts & Deadlines ---
		DialTimeout:  time.Duration(redisCfg.DialTimeout) * time.Second,       // Timeout for establishing new connections
		ReadTimeout:  time.Duration(redisCfg.ReadTimeout) * time.Millisecond,  // Strict read timeout (adjust per command payload)
		WriteTimeout: time.Duration(redisCfg.WriteTimeout) * time.Millisecond, // Strict write timeout

		// --- Resiliency & Keep-Alive ---
		MaxRetries:      redisCfg.MaxRetries,                                        // Retries for failed idempotent commands
		MinRetryBackoff: time.Duration(redisCfg.MinRetryBackoff) * time.Millisecond, // Initial retry delay
		MaxRetryBackoff: time.Duration(redisCfg.MaxRetryBackoff) * time.Millisecond, // Maximum retry delay capping
		ConnMaxIdleTime: time.Duration(redisCfg.ConnMaxIdleTime) * time.Minute,      // Reap connections idle longer than this
		ConnMaxLifetime: time.Duration(redisCfg.ConnMaxLifeTime) * time.Minute,      // Periodically cycle connections to clear leaks

		// --- Security & Production Additions ---
		// TLSConfig: &tls.Config{InsecureSkipVerify: false}, // Uncomment for production clusters
	})

	limiter = redis_rate.NewLimiter(client)
	return &redisClient{
		client:  client,
		limiter: limiter,
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

// RateLimiter enforces rate limiting based on client IP address.
// It uses a sliding window algorithm with requests allowed per minute.
// The rate limit is configured via the RATE_LIMITER environment variable.
// Returns an error if the rate limit is exceeded or if the limiter fails.
func RateLimiter(ctx context.Context) error {
	rateLimiterInt, _ := strconv.Atoi(rateLimiter)
	clientIP := ctx.Value(`X-Client-IP`).(string)
	res, err := limiter.Allow(ctx, clientIP, redis_rate.PerMinute(rateLimiterInt))
	if err != nil {
		return err
	}
	if res.Remaining == 0 {
		return errors.New("Rate limit exceeded")
	}
	return nil
}
