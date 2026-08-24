package redis_client

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func testRedis(t *testing.T) (*redisClient, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client, err := NewRedisClient(&RedisConfig{
		Host:            mr.Host(),
		Port:            mr.Port(),
		PoolSize:        5,
		MinIdleConn:     1,
		MaxIdleConn:     2,
		PoolTimeout:     2,
		DialTimeout:     2,
		ReadTimeout:     500,
		WriteTimeout:    500,
		MaxRetries:      1,
		MinRetryBackoff: 10,
		MaxRetryBackoff: 50,
		ConnMaxIdleTime: 1,
		ConnMaxLifeTime: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	return client, mr
}

func TestGetRedisTimeout(t *testing.T) {
	orig := redisTimeout
	t.Cleanup(func() { redisTimeout = orig })

	redisTimeout = ""
	if getRedisTimeout() != 0 {
		t.Fatal("empty env should be 0")
	}

	redisTimeout = "not-a-number"
	if getRedisTimeout() != 0 {
		t.Fatal("invalid env should be 0")
	}

	redisTimeout = "15"
	if getRedisTimeout() != 15*time.Second {
		t.Fatalf("got %v, want 15s", getRedisTimeout())
	}
}

func TestRedisPingSetGetDel(t *testing.T) {
	c, _ := testRedis(t)
	ctx := context.Background()

	if err := c.Ping(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	if c.Client() == nil {
		t.Fatal("Client() is nil")
	}

	ok, err := c.Set(ctx, "user:1", map[string]string{"name": "ada"}, time.Minute)
	if err != nil || !ok {
		t.Fatalf("Set: ok=%v err=%v", ok, err)
	}

	val, err := c.Get(ctx, "user:1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	m, ok := val.(map[string]interface{})
	if !ok || m["name"] != "ada" {
		t.Fatalf("Get value = %#v", val)
	}

	if err := c.Del(ctx, "user:1"); err != nil {
		t.Fatalf("Del: %v", err)
	}
	if _, err := c.Get(ctx, "user:1"); err == nil {
		t.Fatal("expected missing key error")
	}
}

func TestRedisSetMarshalError(t *testing.T) {
	c, _ := testRedis(t)
	ok, err := c.Set(context.Background(), "k", make(chan int), time.Second)
	if err == nil || ok {
		t.Fatal("expected json marshal error")
	}
}

func TestRedisSetNXAndClearCache(t *testing.T) {
	c, _ := testRedis(t)
	ctx := context.Background()

	ok, err := c.SetNX(ctx, "lock", "1", time.Minute)
	if err != nil || !ok {
		t.Fatalf("first SetNX: ok=%v err=%v", ok, err)
	}
	ok, err = c.SetNX(ctx, "lock", "2", time.Minute)
	if err != nil {
		t.Fatalf("second SetNX err: %v", err)
	}
	if ok {
		t.Fatal("second SetNX should not set an existing key")
	}

	if err := c.ClearCache(ctx); err != nil {
		t.Fatalf("ClearCache: %v", err)
	}
	if _, err := c.Get(ctx, "lock"); err == nil {
		t.Fatal("expected key to be flushed")
	}
}

func TestRedisGeo(t *testing.T) {
	c, _ := testRedis(t)
	ctx := context.Background()

	ok, err := c.GeoAdd(ctx, "places", &redis.GeoLocation{
		Name:      "office",
		Longitude: 106.8,
		Latitude:  -6.2,
	})
	if err != nil || !ok {
		t.Fatalf("GeoAdd: ok=%v err=%v", ok, err)
	}

	ok, err = c.GeoAdd(ctx, "places", &redis.GeoLocation{
		Name:      "cafe",
		Longitude: 106.81,
		Latitude:  -6.21,
	})
	if err != nil || !ok {
		t.Fatalf("GeoAdd cafe: ok=%v err=%v", ok, err)
	}

	locs, err := c.GeoRadiusByMember(ctx, "places", "office", &redis.GeoRadiusQuery{
		Radius: 50,
		Unit:   "km",
	})
	if err != nil {
		t.Fatalf("GeoRadiusByMember: %v", err)
	}
	if len(locs) == 0 {
		t.Fatal("expected nearby locations")
	}
}

func TestRedisUnimplementedPanics(t *testing.T) {
	c, _ := testRedis(t)
	ctx := context.Background()

	assertPanic(t, func() { _, _ = c.HDel(ctx, "k") })
	assertPanic(t, func() { _, _ = c.HGet(ctx, "k") })
	assertPanic(t, func() { _, _ = c.Publish(ctx, "ch", "msg") })
}

func TestRateLimiter(t *testing.T) {
	_, _ = testRedis(t)
	orig := rateLimiter
	t.Cleanup(func() { rateLimiter = orig })
	rateLimiter = "1"

	ctx := context.WithValue(context.Background(), "X-Client-IP", "10.0.0.1")
	if err := RateLimiter(ctx); err != nil {
		t.Fatalf("first allow: %v", err)
	}
	if err := RateLimiter(ctx); err == nil {
		t.Fatal("expected rate limit exceeded")
	}

	defer func() {
		if recover() == nil {
			t.Fatal("expected panic without X-Client-IP")
		}
	}()
	_ = RateLimiter(context.Background())
}

func assertPanic(t *testing.T, fn func()) {
	t.Helper()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	fn()
}
