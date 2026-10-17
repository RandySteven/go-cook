package redis_client

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

type (
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
)
