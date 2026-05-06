package redis

// redis_test.go
//
// 覆盖 redis.go 的全部可测逻辑。
//
// 无需真实 Redis 的测试（纯内存 / 快速失败路径）：
//
// defaultOptions
//   1.  Addr 默认 "localhost:6379"
//   2.  DB 默认 0
//   3.  PoolSize 默认 20
//   4.  MinIdleConns 默认 5
//   5.  DialTimeout 默认 5s
//   6.  ReadTimeout 默认 3s
//   7.  WriteTimeout 默认 3s
//   8.  Password 默认 ""
//
// Option 工厂函数
//   9.  WithAddr
//  10.  WithPassword
//  11.  WithDB
//  12.  WithPoolSize
//  13.  WithMinIdleConns
//  14.  WithDialTimeout
//  15.  WithReadTimeout
//  16.  WithWriteTimeout
//  17.  多个选项叠加全部生效
//
// New —— 不可达地址快速失败
//  18.  不可达地址 + 短超时 → Ping 失败，返回 error
//  19.  error 消息包含 "redis: ping failed"
//  20.  失败时返回 nil Client
//
// MustNew
//  21.  不可达地址 → panic
//  22.  panic 值来自 New 的 error
//
// lockScript（包内常量）
//  23.  lockScript 非空字符串，包含 "GET" 和 "DEL"
//
// 集成测试（需要 REDIS_ADDR 环境变量）：
//  24.  New 成功返回非 nil Client
//  25.  TryLock 第一次加锁成功
//  26.  TryLock 同一 key 第二次加锁失败（已被占用）
//  27.  ReleaseLock 用正确 value 释放成功
//  28.  ReleaseLock 用错误 value 返回 error
//  29.  ExtendLock 用正确 value 续期成功
//  30.  ExtendLock 用错误 value 返回 false
//  31.  TryLock + ReleaseLock 完整生命周期
//  32.  并发 TryLock 只有一个成功

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── 集成测试辅助 ─────────────────────────────────────────────────────────────

// realClient 获取真实 Redis Client，若未设置 REDIS_ADDR 则 Skip。
func realClient(t *testing.T) *Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		t.Skip("REDIS_ADDR not set, skipping Redis integration test")
	}
	c, err := New(WithAddr(addr), WithDialTimeout(3*time.Second))
	require.NoError(t, err, "连接真实 Redis 失败")
	t.Cleanup(func() { c.Close() })
	return c
}

// uniqueKey 生成测试用的唯一 key，避免测试间互相干扰。
func uniqueKey(t *testing.T, suffix string) string {
	t.Helper()
	return fmt.Sprintf("go-jobs:test:%s:%d", suffix, time.Now().UnixNano())
}

// ─── 1-8. defaultOptions ─────────────────────────────────────────────────────

func TestDefaultOptions_Addr(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, "localhost:6379", o.Addr)
}

func TestDefaultOptions_DB(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 0, o.DB)
}

func TestDefaultOptions_PoolSize(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 20, o.PoolSize)
}

func TestDefaultOptions_MinIdleConns(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 5, o.MinIdleConns)
}

func TestDefaultOptions_DialTimeout(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 5*time.Second, o.DialTimeout)
}

func TestDefaultOptions_ReadTimeout(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 3*time.Second, o.ReadTimeout)
}

func TestDefaultOptions_WriteTimeout(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 3*time.Second, o.WriteTimeout)
}

func TestDefaultOptions_Password_Empty(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, "", o.Password)
}

// ─── 9-17. Option 工厂函数 ────────────────────────────────────────────────────

func TestWithAddr_SetsField(t *testing.T) {
	o := defaultOptions()
	WithAddr("10.0.0.1:6380")(o)
	assert.Equal(t, "10.0.0.1:6380", o.Addr)
}

func TestWithPassword_SetsField(t *testing.T) {
	o := defaultOptions()
	WithPassword("secret123")(o)
	assert.Equal(t, "secret123", o.Password)
}

func TestWithDB_SetsField(t *testing.T) {
	o := defaultOptions()
	WithDB(3)(o)
	assert.Equal(t, 3, o.DB)
}

func TestWithPoolSize_SetsField(t *testing.T) {
	o := defaultOptions()
	WithPoolSize(50)(o)
	assert.Equal(t, 50, o.PoolSize)
}

func TestWithMinIdleConns_SetsField(t *testing.T) {
	o := defaultOptions()
	WithMinIdleConns(10)(o)
	assert.Equal(t, 10, o.MinIdleConns)
}

func TestWithDialTimeout_SetsField(t *testing.T) {
	o := defaultOptions()
	WithDialTimeout(2 * time.Second)(o)
	assert.Equal(t, 2*time.Second, o.DialTimeout)
}

func TestWithReadTimeout_SetsField(t *testing.T) {
	o := defaultOptions()
	WithReadTimeout(1 * time.Second)(o)
	assert.Equal(t, 1*time.Second, o.ReadTimeout)
}

func TestWithWriteTimeout_SetsField(t *testing.T) {
	o := defaultOptions()
	WithWriteTimeout(1 * time.Second)(o)
	assert.Equal(t, 1*time.Second, o.WriteTimeout)
}

func TestOptions_MultipleOptions_AllApplied(t *testing.T) {
	o := defaultOptions()
	WithAddr("redis-cluster:6379")(o)
	WithPassword("mypassword")(o)
	WithDB(2)(o)
	WithPoolSize(30)(o)
	WithMinIdleConns(3)(o)
	WithDialTimeout(10 * time.Second)(o)
	WithReadTimeout(5 * time.Second)(o)
	WithWriteTimeout(5 * time.Second)(o)

	assert.Equal(t, "redis-cluster:6379", o.Addr)
	assert.Equal(t, "mypassword", o.Password)
	assert.Equal(t, 2, o.DB)
	assert.Equal(t, 30, o.PoolSize)
	assert.Equal(t, 3, o.MinIdleConns)
	assert.Equal(t, 10*time.Second, o.DialTimeout)
	assert.Equal(t, 5*time.Second, o.ReadTimeout)
	assert.Equal(t, 5*time.Second, o.WriteTimeout)
}

// ─── 18-20. New —— 不可达地址快速失败 ────────────────────────────────────────

func TestNew_UnreachableAddr_ReturnsError(t *testing.T) {
	// 使用极短超时确保快速失败
	_, err := New(
		WithAddr("127.0.0.1:16399"), // 不存在的端口
		WithDialTimeout(200*time.Millisecond),
		WithReadTimeout(200*time.Millisecond),
	)
	require.Error(t, err)
}

func TestNew_UnreachableAddr_ErrorContainsPingFailed(t *testing.T) {
	_, err := New(
		WithAddr("127.0.0.1:16399"),
		WithDialTimeout(200*time.Millisecond),
		WithReadTimeout(200*time.Millisecond),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "redis: ping failed")
}

func TestNew_UnreachableAddr_ReturnsNilClient(t *testing.T) {
	c, _ := New(
		WithAddr("127.0.0.1:16399"),
		WithDialTimeout(200*time.Millisecond),
		WithReadTimeout(200*time.Millisecond),
	)
	assert.Nil(t, c)
}

// ─── 21-22. MustNew —— panic 路径 ────────────────────────────────────────────

func TestMustNew_UnreachableAddr_Panics(t *testing.T) {
	assert.Panics(t, func() {
		MustNew(
			WithAddr("127.0.0.1:16399"),
			WithDialTimeout(200*time.Millisecond),
			WithReadTimeout(200*time.Millisecond),
		)
	})
}

func TestMustNew_PanicValue_ContainsPingFailed(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "MustNew 应当 panic")
		switch v := r.(type) {
		case error:
			assert.Contains(t, v.Error(), "redis: ping failed")
		case string:
			assert.Contains(t, v, "redis: ping failed")
		default:
			t.Fatalf("unexpected panic type: %T", r)
		}
	}()
	MustNew(
		WithAddr("127.0.0.1:16399"),
		WithDialTimeout(200*time.Millisecond),
		WithReadTimeout(200*time.Millisecond),
	)
}

// ─── 23. lockScript 常量 ──────────────────────────────────────────────────────

func TestLockScript_ContainsGetAndDel(t *testing.T) {
	assert.NotEmpty(t, lockScript)
	assert.Contains(t, lockScript, "GET")
	assert.Contains(t, lockScript, "DEL")
}

func TestLockScript_ContainsARGV1(t *testing.T) {
	assert.Contains(t, lockScript, "ARGV[1]")
}

// ─── Client 结构体 ───────────────────────────────────────────────────────────

func TestClient_WrapsGoRedisClient(t *testing.T) {
	// 直接构造 Client（不走 New），验证嵌入关系
	inner := goredis.NewClient(&goredis.Options{Addr: "localhost:6379"})
	c := &Client{Client: inner}
	assert.NotNil(t, c.Client)
	inner.Close()
}

// ─── Options 结构体零值 ───────────────────────────────────────────────────────

func TestOptions_ZeroValue_NoCrash(t *testing.T) {
	assert.NotPanics(t, func() {
		var o Options
		_ = o.Addr
		_ = o.Password
		_ = o.DB
		_ = o.PoolSize
	})
}

// ─── 24. 集成测试：New 成功 ───────────────────────────────────────────────────

func TestNew_WithRealRedis_ReturnsClient(t *testing.T) {
	c := realClient(t)
	assert.NotNil(t, c)
	assert.NotNil(t, c.Client)
}

// ─── 25-26. TryLock ───────────────────────────────────────────────────────────

func TestTryLock_FirstLock_Succeeds(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "trylock")
	defer c.Del(ctx, key)

	ok, err := c.TryLock(ctx, key, "val-1", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, ok, "第一次加锁应成功")
}

func TestTryLock_SecondLock_SameKey_Fails(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "trylock2")
	defer c.Del(ctx, key)

	ok1, err := c.TryLock(ctx, key, "val-1", 5*time.Second)
	require.NoError(t, err)
	require.True(t, ok1)

	ok2, err := c.TryLock(ctx, key, "val-2", 5*time.Second)
	require.NoError(t, err)
	assert.False(t, ok2, "同一 key 已被锁定，第二次加锁应失败")
}

// ─── 27-28. ReleaseLock ──────────────────────────────────────────────────────

func TestReleaseLock_CorrectValue_Succeeds(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "release")
	defer c.Del(ctx, key)

	_, err := c.TryLock(ctx, key, "my-val", 5*time.Second)
	require.NoError(t, err)

	err = c.ReleaseLock(ctx, key, "my-val")
	assert.NoError(t, err, "用正确 value 释放锁应成功")
}

func TestReleaseLock_WrongValue_ReturnsError(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "release-wrong")
	defer c.Del(ctx, key)

	_, err := c.TryLock(ctx, key, "real-owner", 5*time.Second)
	require.NoError(t, err)

	err = c.ReleaseLock(ctx, key, "not-the-owner")
	assert.Error(t, err, "用错误 value 释放锁应返回 error")
	assert.Contains(t, err.Error(), "not owned")
}

// ─── 29-30. ExtendLock ───────────────────────────────────────────────────────

func TestExtendLock_CorrectValue_ReturnsTrue(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "extend")
	defer c.Del(ctx, key)

	_, err := c.TryLock(ctx, key, "owner", 5*time.Second)
	require.NoError(t, err)

	extended, err := c.ExtendLock(ctx, key, "owner", 10*time.Second)
	require.NoError(t, err)
	assert.True(t, extended, "用正确 value 续期应返回 true")
}

func TestExtendLock_WrongValue_ReturnsFalse(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "extend-wrong")
	defer c.Del(ctx, key)

	_, err := c.TryLock(ctx, key, "real-owner", 5*time.Second)
	require.NoError(t, err)

	extended, err := c.ExtendLock(ctx, key, "not-owner", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, extended, "用错误 value 续期应返回 false")
}

// ─── 31. TryLock + ReleaseLock 完整生命周期 ──────────────────────────────────

func TestLock_FullLifecycle(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "lifecycle")
	const val = "node-1:abc123"
	defer c.Del(ctx, key)

	// 加锁
	ok, err := c.TryLock(ctx, key, val, 10*time.Second)
	require.NoError(t, err)
	require.True(t, ok, "加锁应成功")

	// 同名 key 不能重复加锁
	ok2, err := c.TryLock(ctx, key, "other-val", 10*time.Second)
	require.NoError(t, err)
	assert.False(t, ok2, "锁被占用时不能重复加锁")

	// 续期
	extended, err := c.ExtendLock(ctx, key, val, 20*time.Second)
	require.NoError(t, err)
	assert.True(t, extended)

	// 释放
	err = c.ReleaseLock(ctx, key, val)
	require.NoError(t, err)

	// 释放后可以重新加锁
	ok3, err := c.TryLock(ctx, key, "new-owner", 5*time.Second)
	require.NoError(t, err)
	assert.True(t, ok3, "释放后应能重新加锁")
}

// ─── 32. 并发 TryLock 只有一个成功 ───────────────────────────────────────────

func TestTryLock_Concurrent_OnlyOneSucceeds(t *testing.T) {
	c := realClient(t)
	ctx := context.Background()
	key := uniqueKey(t, "concurrent-lock")
	defer c.Del(ctx, key)

	const goroutines = 20
	var successCount int64
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			val := fmt.Sprintf("node-%d", id)
			ok, err := c.TryLock(ctx, key, val, 5*time.Second)
			if err == nil && ok {
				atomic.AddInt64(&successCount, 1)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(1), successCount,
		"并发 TryLock 只有一个 goroutine 应该成功获取锁")
}

// ─── Benchmark（仅在有真实 Redis 时运行）─────────────────────────────────────

func BenchmarkTryLock(b *testing.B) {
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		b.Skip("REDIS_ADDR not set")
	}
	c, err := New(WithAddr(addr))
	require.NoError(b, err)
	defer c.Close()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("bench:lock:%d", i)
		c.TryLock(ctx, key, "v", time.Second)
		c.Del(ctx, key)
	}
}
