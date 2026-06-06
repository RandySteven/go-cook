package redis_client

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// getRedisTimeout returns the cache expiration duration from the REDIS_EXPIRATION
// environment variable. Defaults to 0 if not set or invalid.
func getRedisTimeout() time.Duration {
	redisDurrTime, _ := strconv.Atoi(redisTimeout)
	redisDurrTime64 := int64(redisDurrTime)
	return time.Duration(redisDurrTime64) * time.Second
}

// Set stores a typed value in Redis with JSON serialization.
// The key expires after the configured timeout duration.
//
// Example:
//
//	err := Set(ctx, client, "user:1", &user)
func Set[T any](ctx context.Context, redis *redis.Client, key string, value *T) (err error) {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json marshal err: %v", err)
	}
	return redis.Set(ctx, key, jsonData, getRedisTimeout()).Err()
}

// Get retrieves and deserializes a typed value from Redis.
// Returns an error if the key doesn't exist or deserialization fails.
//
// Example:
//
//	user, err := Get[User](ctx, client, "user:1")
func Get[T any](ctx context.Context, client *redis.Client, key string) (value *T, err error) {
	val, err := client.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(val, &value)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal err: %v", err)
	}
	return value, nil
}

// SetMultiple stores a slice of typed values in Redis with JSON serialization.
// Useful for caching lists or collections.
//
// Example:
//
//	err := SetMultiple(ctx, client, "users:all", users)
func SetMultiple[T any](ctx context.Context, redis *redis.Client, key string, value []*T) (err error) {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("json marshal err: %v", err)
	}
	return redis.Set(ctx, key, jsonData, getRedisTimeout()).Err()
}

// GetMultiple retrieves and deserializes a slice of typed values from Redis.
// Returns an error if the key doesn't exist or deserialization fails.
//
// Example:
//
//	users, err := GetMultiple[User](ctx, client, "users:all")
func GetMultiple[T any](ctx context.Context, redis *redis.Client, key string) (value []*T, err error) {
	val, err := redis.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(val, &value)
	if err != nil {
		return nil, fmt.Errorf("json unmarshal err: %v", err)
	}
	return value, nil
}

// Del removes a key from Redis.
// Returns an error if the deletion fails.
func Del(ctx context.Context, redis *redis.Client, key string) (err error) {
	return redis.Del(ctx, key).Err()
}
