package redis_client

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
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
}

func TestRedisPublish(t *testing.T) {
	c, _ := testRedis(t)
	ok, err := c.Publish(context.Background(), "orders", "paid")
	if err != nil || !ok {
		t.Fatalf("Publish: ok=%v err=%v", ok, err)
	}
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

func TestValkeyAndRedisShareCommands(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg := &RedisConfig{
		Host:            mr.Host(),
		Port:            mr.Port(),
		PoolSize:        2,
		DialTimeout:     2,
		ReadTimeout:     500,
		WriteTimeout:    500,
		MaxRetries:      1,
		MinRetryBackoff: 8,
		MaxRetryBackoff: 20,
	}

	valkey, err := NewValkeyClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	redisClient, err := NewRedisClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if err := valkey.Ping(); err != nil {
		t.Fatalf("valkey ping: %v", err)
	}
	if _, err := valkey.Set(ctx, "shared", map[string]string{"engine": "valkey"}, time.Minute); err != nil {
		t.Fatal(err)
	}
	val, err := redisClient.Get(ctx, "shared")
	if err != nil {
		t.Fatalf("redis read of valkey write: %v", err)
	}
	m, ok := val.(map[string]interface{})
	if !ok || m["engine"] != "valkey" {
		t.Fatalf("shared value = %#v", val)
	}

	opt := valkey.Client().Options()
	if opt.TLSConfig != nil {
		t.Fatal("default connection should be plaintext")
	}
	if opt.Protocol != 3 {
		t.Fatalf("default protocol = %d, want 3", opt.Protocol)
	}
	if opt.MaintNotificationsConfig == nil || opt.MaintNotificationsConfig.Mode != maintnotifications.ModeDisabled {
		t.Fatalf("maint notifications = %#v", opt.MaintNotificationsConfig)
	}
}

func TestClientsStayOnTheirServer(t *testing.T) {
	a, _ := testRedis(t)
	b, _ := testRedis(t)
	ctx := context.Background()

	if _, err := a.Set(ctx, "only-a", "a", time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Get(ctx, "only-a"); err == nil {
		t.Fatal("second client read the first client's key")
	}
}

func TestProtocol2AndOptionOverrides(t *testing.T) {
	mr := miniredis.RunT(t)
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	c, err := NewRedisClient(&RedisConfig{
		Host:               mr.Host(),
		Port:               mr.Port(),
		Protocol:           2,
		DB:                 0,
		DisableIdentity:    true,
		MaintNotifications: true,
		TLSConfig:          tlsCfg,
		DialTimeout:        1,
		ReadTimeout:        200,
		WriteTimeout:       200,
	})
	if err != nil {
		t.Fatal(err)
	}
	opt := c.Client().Options()
	if opt.Protocol != 2 {
		t.Fatalf("protocol = %d", opt.Protocol)
	}
	if opt.TLSConfig != tlsCfg {
		t.Fatal("TLSConfig was not applied")
	}
	if !opt.DisableIdentity {
		t.Fatal("DisableIdentity was not applied")
	}
	if opt.MaintNotificationsConfig == nil || opt.MaintNotificationsConfig.Mode != maintnotifications.ModeAuto {
		t.Fatalf("maint mode = %#v", opt.MaintNotificationsConfig)
	}

	plain, err := NewValkeyClient(&RedisConfig{
		Host:         mr.Host(),
		Port:         mr.Port(),
		Protocol:     2,
		DialTimeout:  2,
		ReadTimeout:  500,
		WriteTimeout: 500,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.Ping(); err != nil {
		t.Fatalf("RESP2 ping: %v", err)
	}
}

func TestInvalidClientConfig(t *testing.T) {
	if _, err := NewRedisClient(nil); err == nil {
		t.Fatal("expected nil config error")
	}
	if _, err := NewValkeyClient(&RedisConfig{Protocol: 1}); err == nil {
		t.Fatal("expected protocol error")
	}
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
