package redis_client

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

var _ Redis = &redisClient{}

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
func (r *redisClient) Set(ctx context.Context, key string, value interface{}, timeExpire time.Duration) (bool, error) {
	jsonData, err := json.Marshal(value)
	if err != nil {
		return false, err
	}
	err = r.client.Set(ctx, key, jsonData, timeExpire).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// Get retrieves and deserializes a typed value from Redis.
// Returns an error if the key doesn't exist or deserialization fails.
//
// Example:
//
//	user, err := Get[User](ctx, client, "user:1")
func (r *redisClient) Get(ctx context.Context, key string) (value interface{}, err error) {
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

// Del removes a key from Redis.
// Returns an error if the deletion fails.
func (r *redisClient) Del(ctx context.Context, key string) (err error) {
	return r.client.Del(ctx, key).Err()
}

// HDel implements [Redis].
func (r *redisClient) HDel(ctx context.Context, key string) (bool, error) {
	panic("unimplemented")
}

// HGet implements [Redis].
func (r *redisClient) HGet(ctx context.Context, key string) (interface{}, error) {
	panic("unimplemented")
}

// SetNX implements [Redis].
func (r *redisClient) SetNX(ctx context.Context, key string, value interface{}, timeExpire time.Duration) (bool, error) {
	err := r.client.SetNX(ctx, key, value, timeExpire).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// GeoAdd implements [Redis].
func (r *redisClient) GeoAdd(ctx context.Context, key string, geoLoc *redis.GeoLocation) (bool, error) {
	err := r.client.GeoAdd(ctx, key, geoLoc).Err()
	if err != nil {
		return false, err
	}
	return true, nil
}

// GeoRadius implements [Redis].
func (r *redisClient) GeoRadiusByMember(ctx context.Context, key string, member string, query *redis.GeoRadiusQuery) ([]redis.GeoLocation, error) {
	cmd := r.client.GeoRadiusByMember(ctx, key, member, query)
	if cmd.Err() != nil {
		return nil, cmd.Err()
	}
	return cmd.Val(), nil
}

// Publish implements [Redis].
func (r *redisClient) Publish(ctx context.Context, channel string, message interface{}) (bool, error) {
	panic("unimplemented")
}

