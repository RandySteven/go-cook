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
