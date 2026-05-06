package ratelimit

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── TokenBucket ─────────────────────────────────────────────────────────────

func TestTokenBucket_InitiallyFull(t *testing.T) {
	tb := NewTokenBucket(10, 10)
	assert.InDelta(t, 10.0, tb.Available(), 0.1)
}

func TestTokenBucket_DefaultBurstEqualsRate(t *testing.T) {
	tb := NewTokenBucket(5, 0)
	assert.Equal(t, 5.0, tb.Capacity())
}

func TestTokenBucket_Allow_ConsumesToken(t *testing.T) {
	tb := NewTokenBucket(10, 10)
	require.NoError(t, tb.Allow())
	assert.InDelta(t, 9.0, tb.Available(), 0.2)
}

func TestTokenBucket_Allow_FailsWhenEmpty(t *testing.T) {
	tb := NewTokenBucket(100, 3)
	tb.Allow(); tb.Allow(); tb.Allow()
	assert.Equal(t, ErrRateLimitExceeded, tb.Allow())
}

func TestTokenBucket_AllowN_BulkConsume(t *testing.T) {
	tb := NewTokenBucket(100, 10)
	require.NoError(t, tb.AllowN(10))
	assert.Equal(t, ErrRateLimitExceeded, tb.AllowN(1))
}

func TestTokenBucket_LazyRefill_OverTime(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	tb := NewTokenBucket(100, 5)
	for i := 0; i < 5; i++ {
		tb.Allow()
	}
	assert.Equal(t, ErrRateLimitExceeded, tb.Allow())
	time.Sleep(40 * time.Millisecond) // 100 token/s => ~4 tokens in 40ms
	assert.NoError(t, tb.Allow(), "应已补充令牌")
}

func TestTokenBucket_Reset(t *testing.T) {
	tb := NewTokenBucket(10, 10)
	for i := 0; i < 10; i++ {
		tb.Allow()
	}
	assert.Equal(t, ErrRateLimitExceeded, tb.Allow())
	tb.Reset()
	assert.NoError(t, tb.Allow())
}

func TestTokenBucket_Accessors(t *testing.T) {
	tb := NewTokenBucket(7.5, 15)
	assert.Equal(t, 7.5, tb.Rate())
	assert.Equal(t, 15.0, tb.Capacity())
}

func TestTokenBucket_ConcurrentAllow_ExactlyCapacityPass(t *testing.T) {
	const cap = 100
	tb := NewTokenBucket(1e9, cap)
	var passed, failed int64
	var wg sync.WaitGroup
	for i := 0; i < cap*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tb.Allow() == nil {
				atomic.AddInt64(&passed, 1)
			} else {
				atomic.AddInt64(&failed, 1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(cap), passed, "恰好容量数量的请求应通过")
	assert.Equal(t, int64(cap), failed)
}

// ─── FixedWindowQuota ─────────────────────────────────────────────────────────

func TestFixedWindowQuota_InitialRemaining(t *testing.T) {
	q := NewFixedWindowQuota(time.Hour, 100)
	assert.Equal(t, int64(100), q.Remaining())
}

func TestFixedWindowQuota_Allow_Consumes(t *testing.T) {
	q := NewFixedWindowQuota(time.Hour, 10)
	require.NoError(t, q.Allow())
	assert.Equal(t, int64(9), q.Remaining())
}

func TestFixedWindowQuota_Allow_ExhaustLimit(t *testing.T) {
	q := NewFixedWindowQuota(time.Hour, 3)
	q.Allow(); q.Allow(); q.Allow()
	assert.Equal(t, ErrQuotaExceeded, q.Allow())
	assert.Equal(t, int64(0), q.Remaining())
}

func TestFixedWindowQuota_Count_Increments(t *testing.T) {
	q := NewFixedWindowQuota(time.Hour, 100)
	for i := 0; i < 7; i++ {
		q.Allow()
	}
	assert.Equal(t, int64(7), q.Count())
}

func TestFixedWindowQuota_WindowReset(t *testing.T) {
	if testing.Short() {
		t.Skip("timing test")
	}
	q := NewFixedWindowQuota(40*time.Millisecond, 3)
	q.Allow(); q.Allow(); q.Allow()
	assert.Equal(t, ErrQuotaExceeded, q.Allow())
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.Allow(), "新窗口应重置配额")
	assert.Equal(t, int64(2), q.Remaining())
}

func TestFixedWindowQuota_Accessors(t *testing.T) {
	q := NewFixedWindowQuota(24*time.Hour, 5000)
	assert.Equal(t, int64(5000), q.Limit())
	assert.Equal(t, 24*time.Hour, q.Window())
}

func TestFixedWindowQuota_ConcurrentAllow_ExactLimit(t *testing.T) {
	const limit = 50
	q := NewFixedWindowQuota(time.Hour, limit)
	var passed, failed int64
	var wg sync.WaitGroup
	for i := 0; i < limit*2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if q.Allow() == nil {
				atomic.AddInt64(&passed, 1)
			} else {
				atomic.AddInt64(&failed, 1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(limit), passed)
	assert.Equal(t, int64(limit), failed)
}

// ─── Limiter ──────────────────────────────────────────────────────────────────

func TestLimiter_EmptyConfig_AlwaysAllows(t *testing.T) {
	l := NewLimiter("test", LimiterConfig{})
	for i := 0; i < 1000; i++ {
		assert.NoError(t, l.Allow())
	}
}

func TestLimiter_BucketOnly_LimitsRate(t *testing.T) {
	l := NewLimiter("test", LimiterConfig{Rate: 100, Burst: 3})
	l.Allow(); l.Allow(); l.Allow()
	assert.Equal(t, ErrRateLimitExceeded, l.Allow())
}

func TestLimiter_QuotaOnly_LimitsTotal(t *testing.T) {
	l := NewLimiter("test", LimiterConfig{QuotaWindow: time.Hour, QuotaLimit: 2})
	assert.NoError(t, l.Allow())
	assert.NoError(t, l.Allow())
	assert.Equal(t, ErrQuotaExceeded, l.Allow())
}

func TestLimiter_BothEnabled_BucketCheckedFirst(t *testing.T) {
	l := NewLimiter("test", LimiterConfig{
		Rate: 100, Burst: 1,
		QuotaWindow: time.Hour, QuotaLimit: 1000,
	})
	l.Allow() // 消耗唯一令牌
	err := l.Allow()
	assert.Equal(t, ErrRateLimitExceeded, err, "令牌桶应先于配额检查")
	// 配额未被消耗（令牌桶先失败）
	s := l.Stats()
	assert.Equal(t, int64(999), s.QuotaRemaining)
}

func TestLimiter_BothEnabled_QuotaFails(t *testing.T) {
	l := NewLimiter("test", LimiterConfig{
		Rate: 1e6, Burst: 1e6,
		QuotaWindow: time.Hour, QuotaLimit: 3,
	})
	l.Allow(); l.Allow(); l.Allow()
	assert.Equal(t, ErrQuotaExceeded, l.Allow())
}

func TestLimiter_Stats_AllowedAndRejected(t *testing.T) {
	l := NewLimiter("k1", LimiterConfig{Rate: 100, Burst: 2})
	l.Allow(); l.Allow()
	l.Allow() // rejected

	s := l.Stats()
	assert.Equal(t, "k1", s.Key)
	assert.Equal(t, int64(2), s.TotalAllowed)
	assert.Equal(t, int64(1), s.TotalRejected)
	assert.True(t, s.HasBucket)
	assert.False(t, s.HasQuota)
	assert.Equal(t, 100.0, s.BucketRate)
	assert.Equal(t, 2.0, s.BucketCapacity)
}

func TestLimiter_Stats_QuotaFields(t *testing.T) {
	l := NewLimiter("k2", LimiterConfig{QuotaWindow: time.Hour, QuotaLimit: 100})
	l.Allow()
	s := l.Stats()
	assert.True(t, s.HasQuota)
	assert.False(t, s.HasBucket)
	assert.Equal(t, int64(99), s.QuotaRemaining)
	assert.Equal(t, int64(100), s.QuotaLimit)
	assert.Equal(t, time.Hour, s.QuotaWindow)
}

// ─── Registry ─────────────────────────────────────────────────────────────────

func TestRegistry_RegisterAndGet(t *testing.T) {
	reg := NewRegistry()
	reg.Register("app:x", LimiterConfig{Rate: 10, Burst: 10})
	l, ok := reg.Get("app:x")
	require.True(t, ok)
	assert.Equal(t, "app:x", l.Key())
}

func TestRegistry_Get_Missing(t *testing.T) {
	reg := NewRegistry()
	_, ok := reg.Get("app:missing")
	assert.False(t, ok)
}

func TestRegistry_Register_Replaces(t *testing.T) {
	reg := NewRegistry()
	reg.Register("job:1", LimiterConfig{Rate: 1, Burst: 1})
	reg.Register("job:1", LimiterConfig{Rate: 999, Burst: 999})
	l, _ := reg.Get("job:1")
	assert.Equal(t, 999.0, l.bucket.Rate())
}

func TestRegistry_Deregister(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterJob(1, LimiterConfig{Rate: 5, Burst: 5})
	reg.Deregister(JobKey(1))
	_, ok := reg.Get(JobKey(1))
	assert.False(t, ok)
}

func TestRegistry_RegisterApp_KeyFormat(t *testing.T) {
	reg := NewRegistry()
	l := reg.RegisterApp("mailer", LimiterConfig{})
	assert.Equal(t, "app:mailer", l.Key())
}

func TestRegistry_RegisterJob_KeyFormat(t *testing.T) {
	reg := NewRegistry()
	l := reg.RegisterJob(42, LimiterConfig{})
	assert.Equal(t, "job:42", l.Key())
}

func TestRegistry_CheckJob_NoLimiters_Passes(t *testing.T) {
	reg := NewRegistry()
	assert.NoError(t, reg.CheckJob("myapp", 99))
}

func TestRegistry_CheckJob_JobLimiter_Denies(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterJob(1, LimiterConfig{Rate: 100, Burst: 1})
	l, _ := reg.Get(JobKey(1))
	l.Allow() // exhaust

	err := reg.CheckJob("myapp", 1)
	assert.ErrorIs(t, err, ErrRateLimitExceeded)
}

func TestRegistry_CheckJob_AppLimiter_Denies(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterApp("slowapp", LimiterConfig{Rate: 100, Burst: 1})
	l, _ := reg.Get(AppKey("slowapp"))
	l.Allow() // exhaust

	err := reg.CheckJob("slowapp", 999)
	assert.ErrorIs(t, err, ErrRateLimitExceeded)
}

func TestRegistry_CheckJob_JobDenied_AppNotConsumed(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterJob(7, LimiterConfig{Rate: 100, Burst: 1})
	reg.RegisterApp("app", LimiterConfig{Rate: 100, Burst: 100})

	l, _ := reg.Get(JobKey(7))
	l.Allow() // exhaust job limiter

	reg.CheckJob("app", 7)

	appL, _ := reg.Get(AppKey("app"))
	assert.Equal(t, int64(0), appL.Stats().TotalAllowed, "Job 被拦截后 App 配额不应被消耗")
}

func TestRegistry_CheckJob_BothPass(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterJob(5, LimiterConfig{Rate: 1e6, Burst: 1e6})
	reg.RegisterApp("fast", LimiterConfig{Rate: 1e6, Burst: 1e6})
	assert.NoError(t, reg.CheckJob("fast", 5))
}

func TestRegistry_AllStats_ReturnsAll(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterApp("a", LimiterConfig{Rate: 10, Burst: 10})
	reg.RegisterJob(1, LimiterConfig{Rate: 5, Burst: 5})
	reg.RegisterJob(2, LimiterConfig{QuotaWindow: time.Hour, QuotaLimit: 100})
	assert.Len(t, reg.AllStats(), 3)
}

func TestRegistry_Len(t *testing.T) {
	reg := NewRegistry()
	assert.Equal(t, 0, reg.Len())
	reg.RegisterApp("a", LimiterConfig{})
	reg.RegisterJob(1, LimiterConfig{})
	assert.Equal(t, 2, reg.Len())
	reg.Deregister(JobKey(1))
	assert.Equal(t, 1, reg.Len())
}

func TestRegistry_ConcurrentRegisterAndCheck(t *testing.T) {
	reg := NewRegistry()
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			reg.RegisterJob(int64(i), LimiterConfig{Rate: float64(i+1) * 10, Burst: float64(i+1) * 10})
		}(i)
		go func(i int) {
			defer wg.Done()
			reg.CheckJob(fmt.Sprintf("app%d", i%10), int64(i))
		}(i)
	}
	wg.Wait()
}

// ─── Key helpers ─────────────────────────────────────────────────────────────

func TestAppKey(t *testing.T) { assert.Equal(t, "app:email", AppKey("email")) }
func TestJobKey(t *testing.T) { assert.Equal(t, "job:42", JobKey(42)) }

// ─── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkTokenBucket_Allow(b *testing.B) {
	tb := NewTokenBucket(1e9, 1e9)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			tb.Allow()
		}
	})
}

func BenchmarkRegistry_CheckJob(b *testing.B) {
	reg := NewRegistry()
	reg.RegisterApp("bench", LimiterConfig{Rate: 1e9, Burst: 1e9})
	reg.RegisterJob(1, LimiterConfig{Rate: 1e9, Burst: 1e9})
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			reg.CheckJob("bench", 1)
		}
	})
}
