package idempotency

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── TryAcquire ───────────────────────────────────────────────────────────────

func TestTryAcquire_FirstCall_ReturnsAcquired(t *testing.T) {
	tb := New()
	defer tb.Stop()

	rec, acquired := tb.TryAcquire(1001)
	require.True(t, acquired, "首次调用应获得执行权")
	require.NotNil(t, rec)
	assert.Equal(t, int64(1001), rec.LogID)
	assert.Equal(t, StateRunning, rec.State)
	assert.False(t, rec.StartTime.IsZero(), "StartTime 应被记录")
}

func TestTryAcquire_SecondCall_SameLogID_ReturnsNotAcquired(t *testing.T) {
	tb := New()
	defer tb.Stop()

	rec1, acquired1 := tb.TryAcquire(1001)
	require.True(t, acquired1)

	rec2, acquired2 := tb.TryAcquire(1001)
	assert.False(t, acquired2, "重复调用不应获得执行权")
	assert.Same(t, rec1, rec2, "应返回同一个 Record 指针")
}

func TestTryAcquire_DifferentLogIDs_BothAcquired(t *testing.T) {
	tb := New()
	defer tb.Stop()

	_, a1 := tb.TryAcquire(1001)
	_, a2 := tb.TryAcquire(1002)
	_, a3 := tb.TryAcquire(1003)

	assert.True(t, a1)
	assert.True(t, a2)
	assert.True(t, a3)
	assert.Equal(t, 3, tb.Len())
}

func TestTryAcquire_AfterSuccess_ReturnsExistingRecord(t *testing.T) {
	tb := New()
	defer tb.Stop()

	_, _ = tb.TryAcquire(1001)
	tb.Complete(1001, nil)

	rec, acquired := tb.TryAcquire(1001)
	assert.False(t, acquired, "成功完成后重复请求不应获得执行权")
	assert.Equal(t, StateSuccess, rec.State)
}

func TestTryAcquire_AfterFailed_ReturnsExistingRecord(t *testing.T) {
	tb := New()
	defer tb.Stop()

	_, _ = tb.TryAcquire(1001)
	tb.Complete(1001, errors.New("handler error"))

	rec, acquired := tb.TryAcquire(1001)
	assert.False(t, acquired, "失败完成后重复请求不应获得执行权")
	assert.Equal(t, StateFailed, rec.State)
	assert.EqualError(t, rec.Err, "handler error")
}

// ─── Complete ─────────────────────────────────────────────────────────────────

func TestComplete_Success_UpdatesState(t *testing.T) {
	tb := New()
	defer tb.Stop()

	_, _ = tb.TryAcquire(1001)
	tb.Complete(1001, nil)

	rec := tb.Get(1001)
	require.NotNil(t, rec)
	assert.Equal(t, StateSuccess, rec.State)
	assert.Nil(t, rec.Err)
	assert.False(t, rec.EndTime.IsZero())
	assert.GreaterOrEqual(t, rec.DurationMs, int64(0))
	assert.False(t, rec.expireAt.IsZero(), "完成后应设置 expireAt")
}

func TestComplete_Failed_StoresError(t *testing.T) {
	tb := New()
	defer tb.Stop()

	_, _ = tb.TryAcquire(1001)
	customErr := errors.New("custom handler failure")
	tb.Complete(1001, customErr)

	rec := tb.Get(1001)
	require.NotNil(t, rec)
	assert.Equal(t, StateFailed, rec.State)
	assert.Equal(t, customErr, rec.Err)
}

func TestComplete_UnknownLogID_Noop(t *testing.T) {
	tb := New()
	defer tb.Stop()

	// 未 TryAcquire 直接 Complete，应静默忽略
	require.NotPanics(t, func() {
		tb.Complete(9999, nil)
	})
}

func TestComplete_Idempotent_SecondCallIgnored(t *testing.T) {
	tb := New()
	defer tb.Stop()

	_, _ = tb.TryAcquire(1001)
	tb.Complete(1001, nil)
	// 再次调用不应改变状态
	tb.Complete(1001, errors.New("late error"))

	rec := tb.Get(1001)
	assert.Equal(t, StateSuccess, rec.State, "重复 Complete 不应覆盖已有状态")
	assert.Nil(t, rec.Err)
}

func TestComplete_DurationCalculated(t *testing.T) {
	tb := New()
	defer tb.Stop()

	_, _ = tb.TryAcquire(1001)
	time.Sleep(10 * time.Millisecond)
	tb.Complete(1001, nil)

	rec := tb.Get(1001)
	assert.GreaterOrEqual(t, rec.DurationMs, int64(5), "耗时应至少 5ms")
}

// ─── Record 辅助方法 ──────────────────────────────────────────────────────────

func TestRecord_StateHelpers(t *testing.T) {
	tb := New()
	defer tb.Stop()

	rec, _ := tb.TryAcquire(1001)
	assert.True(t, rec.Running())
	assert.False(t, rec.Succeeded())
	assert.False(t, rec.Failed())

	tb.Complete(1001, nil)
	assert.False(t, rec.Running())
	assert.True(t, rec.Succeeded())
	assert.False(t, rec.Failed())
}

func TestRecord_FailedState(t *testing.T) {
	tb := New()
	defer tb.Stop()

	rec, _ := tb.TryAcquire(1001)
	tb.Complete(1001, errors.New("err"))
	assert.True(t, rec.Failed())
	assert.False(t, rec.Succeeded())
}

func TestState_String(t *testing.T) {
	assert.Equal(t, "running", StateRunning.String())
	assert.Equal(t, "success", StateSuccess.String())
	assert.Equal(t, "failed", StateFailed.String())
	assert.Equal(t, "unknown", State(99).String())
}

// ─── Get ──────────────────────────────────────────────────────────────────────

func TestGet_ExistingRecord(t *testing.T) {
	tb := New()
	defer tb.Stop()

	tb.TryAcquire(1001)
	rec := tb.Get(1001)
	assert.NotNil(t, rec)
	assert.Equal(t, int64(1001), rec.LogID)
}

func TestGet_NonExistentRecord(t *testing.T) {
	tb := New()
	defer tb.Stop()

	rec := tb.Get(9999)
	assert.Nil(t, rec)
}

// ─── ForceDelete ──────────────────────────────────────────────────────────────

func TestForceDelete(t *testing.T) {
	tb := New()
	defer tb.Stop()

	tb.TryAcquire(1001)
	assert.Equal(t, 1, tb.Len())

	tb.ForceDelete(1001)
	assert.Equal(t, 0, tb.Len())
	assert.Nil(t, tb.Get(1001))
}

func TestForceDelete_NonExistent_Noop(t *testing.T) {
	tb := New()
	defer tb.Stop()

	require.NotPanics(t, func() {
		tb.ForceDelete(9999)
	})
}

// ─── Len / Stats ──────────────────────────────────────────────────────────────

func TestLen(t *testing.T) {
	tb := New()
	defer tb.Stop()

	assert.Equal(t, 0, tb.Len())
	tb.TryAcquire(1)
	tb.TryAcquire(2)
	assert.Equal(t, 2, tb.Len())
}

func TestStats_AllStates(t *testing.T) {
	tb := New()
	defer tb.Stop()

	// 3 running
	tb.TryAcquire(1)
	tb.TryAcquire(2)
	tb.TryAcquire(3)
	// 1 success
	tb.TryAcquire(4)
	tb.Complete(4, nil)
	// 1 failed
	tb.TryAcquire(5)
	tb.Complete(5, errors.New("err"))

	s := tb.Stats()
	assert.Equal(t, 5, s.Total)
	assert.Equal(t, 3, s.Running)
	assert.Equal(t, 1, s.Success)
	assert.Equal(t, 1, s.Failed)
}

func TestStats_Empty(t *testing.T) {
	tb := New()
	defer tb.Stop()

	s := tb.Stats()
	assert.Equal(t, TableStats{}, s)
}

// ─── GC ───────────────────────────────────────────────────────────────────────

func TestGC_ExpiredRecordsRemoved(t *testing.T) {
	if testing.Short() {
		t.Skip("GC 测试在 -short 模式下跳过")
	}

	tb := New(
		WithTTL(50*time.Millisecond),
		WithGCInterval(20*time.Millisecond),
	)
	defer tb.Stop()

	tb.TryAcquire(1001)
	tb.Complete(1001, nil)

	assert.Equal(t, 1, tb.Len(), "完成后记录应存在")

	// 等待 TTL + GC 周期
	time.Sleep(150 * time.Millisecond)
	assert.Equal(t, 0, tb.Len(), "TTL 到期后记录应被 GC 删除")
}

func TestGC_RunningRecordNotRemoved(t *testing.T) {
	if testing.Short() {
		t.Skip("GC 测试在 -short 模式下跳过")
	}

	tb := New(
		WithTTL(10*time.Millisecond),
		WithGCInterval(10*time.Millisecond),
	)
	defer tb.Stop()

	tb.TryAcquire(1001) // Running 状态

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, 1, tb.Len(), "Running 状态的记录不应被 GC 删除")
}

func TestGC_ManualTrigger(t *testing.T) {
	tb := New(
		WithTTL(1*time.Millisecond),
		WithGCInterval(10*time.Minute), // 不自动触发
	)
	defer tb.Stop()

	tb.TryAcquire(1001)
	tb.Complete(1001, nil)

	time.Sleep(5 * time.Millisecond) // 等待 TTL 过期

	tb.gc() // 手动触发
	assert.Equal(t, 0, tb.Len(), "手动 GC 应删除过期记录")
}

func TestGC_MixedRecords(t *testing.T) {
	tb := New(
		WithTTL(20*time.Millisecond),
		WithGCInterval(10*time.Minute),
	)
	defer tb.Stop()

	tb.TryAcquire(1001)
	tb.Complete(1001, nil) // 已完成，会过期

	tb.TryAcquire(1002) // Running，不过期

	time.Sleep(50 * time.Millisecond)
	tb.gc()

	assert.Equal(t, 1, tb.Len(), "只有 Running 记录应保留")
	assert.NotNil(t, tb.Get(1002))
	assert.Nil(t, tb.Get(1001))
}

// ─── 使用模式测试（模拟完整 Runner.Run 流程）─────────────────────────────────

// TestUsagePattern_NormalExecution 模拟正常执行流程。
func TestUsagePattern_NormalExecution(t *testing.T) {
	tb := New()
	defer tb.Stop()

	logID := int64(5001)

	// 第一步：Handler 收到请求，注册幂等
	rec, acquired := tb.TryAcquire(logID)
	require.True(t, acquired)
	assert.Equal(t, StateRunning, rec.State)

	// 第二步：执行任务（模拟耗时）
	time.Sleep(5 * time.Millisecond)

	// 第三步：完成
	tb.Complete(logID, nil)

	// 验证：任务成功
	final := tb.Get(logID)
	require.NotNil(t, final)
	assert.Equal(t, StateSuccess, final.State)
	assert.GreaterOrEqual(t, final.DurationMs, int64(1))
}

// TestUsagePattern_DuplicateWhileRunning 模拟网络重试、任务正在执行时的重复请求。
func TestUsagePattern_DuplicateWhileRunning(t *testing.T) {
	tb := New()
	defer tb.Stop()

	logID := int64(5001)

	// 第一个请求获得执行权
	_, acquired := tb.TryAcquire(logID)
	require.True(t, acquired)

	// 第二个请求（网络重试）
	dupRec, dupAcquired := tb.TryAcquire(logID)
	assert.False(t, dupAcquired, "重试请求不应获得执行权")
	assert.Equal(t, StateRunning, dupRec.State, "重试请求应看到 Running 状态")
	// Handler 层应返回 ErrAlreadyRunning
}

// TestUsagePattern_DuplicateAfterSuccess 模拟任务完成后的重复请求。
func TestUsagePattern_DuplicateAfterSuccess(t *testing.T) {
	tb := New()
	defer tb.Stop()

	logID := int64(5001)

	_, _ = tb.TryAcquire(logID)
	tb.Complete(logID, nil)

	// 重复请求（例如：调度器延迟重试）
	dupRec, dupAcquired := tb.TryAcquire(logID)
	assert.False(t, dupAcquired)
	assert.Equal(t, StateSuccess, dupRec.State)
	// Handler 层可直接返回 nil（已成功，视为幂等成功）
}

// TestUsagePattern_FailureThenRetryWithForceDelete 模拟任务失败后，
// 调度器删除幂等记录以允许合法重试（Retry 触发器）。
func TestUsagePattern_FailureThenRetryWithForceDelete(t *testing.T) {
	tb := New()
	defer tb.Stop()

	logID := int64(5001)

	// 第一次执行失败
	_, _ = tb.TryAcquire(logID)
	tb.Complete(logID, errors.New("transient error"))

	// 重试触发器使用新的 LogID（调度器每次触发都生成新 LogID）
	// 所以一般不需要 ForceDelete；但若需要强制重新执行同一 LogID：
	tb.ForceDelete(logID)

	// 重新执行
	_, reAcquired := tb.TryAcquire(logID)
	assert.True(t, reAcquired, "ForceDelete 后应能重新获得执行权")
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestConcurrentTryAcquire_OnlyOneSucceeds(t *testing.T) {
	tb := New()
	defer tb.Stop()

	const goroutines = 50
	logID := int64(9001)

	var (
		acquiredCount int64
		mu            sync.Mutex
		wg            sync.WaitGroup
	)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok := tb.TryAcquire(logID)
			if ok {
				mu.Lock()
				acquiredCount++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), acquiredCount, "并发 TryAcquire 只有一个应成功")
	assert.Equal(t, 1, tb.Len())
}

func TestConcurrentCompleteAndRead(t *testing.T) {
	tb := New()
	defer tb.Stop()

	const N = 200
	var wg sync.WaitGroup

	// 注册 N 个 logID
	for i := 0; i < N; i++ {
		tb.TryAcquire(int64(i))
	}

	// 并发完成 + 读取
	for i := 0; i < N; i++ {
		wg.Add(2)
		go func(id int64) {
			defer wg.Done()
			tb.Complete(id, nil)
		}(int64(i))
		go func(id int64) {
			defer wg.Done()
			_ = tb.Get(id)
		}(int64(i))
	}
	wg.Wait()
}

func TestConcurrentMixedOperations(t *testing.T) {
	tb := New(WithGCInterval(5 * time.Millisecond))
	defer tb.Stop()

	const N = 100
	var wg sync.WaitGroup

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logID := int64(i)
			_, acquired := tb.TryAcquire(logID)
			if acquired {
				if i%3 == 0 {
					tb.Complete(logID, fmt.Errorf("error %d", i))
				} else {
					tb.Complete(logID, nil)
				}
			}
		}(i)
	}
	wg.Wait()

	stats := tb.Stats()
	assert.Equal(t, N, stats.Total)
	assert.Equal(t, 0, stats.Running)
}

// ─── 大容量性能测试 ───────────────────────────────────────────────────────────

func TestHighThroughput_TryAcquireComplete(t *testing.T) {
	if testing.Short() {
		t.Skip("性能测试在 -short 模式下跳过")
	}

	tb := New(WithInitialCapacity(10000))
	defer tb.Stop()

	const N = 10000
	start := time.Now()

	for i := 0; i < N; i++ {
		logID := int64(i)
		_, acquired := tb.TryAcquire(logID)
		if acquired {
			tb.Complete(logID, nil)
		}
	}

	elapsed := time.Since(start)
	t.Logf("10000 次 TryAcquire+Complete 耗时: %v (%.0f ops/s)",
		elapsed, float64(N)/elapsed.Seconds())

	assert.Equal(t, N, tb.Len())
}

// ─── Options ─────────────────────────────────────────────────────────────────

func TestOptions_CustomTTL(t *testing.T) {
	tb := New(WithTTL(1 * time.Millisecond))
	defer tb.Stop()

	assert.Equal(t, 1*time.Millisecond, tb.opts.TTL)
}

func TestOptions_CustomGCInterval(t *testing.T) {
	tb := New(WithGCInterval(1 * time.Minute))
	defer tb.Stop()

	assert.Equal(t, 1*time.Minute, tb.opts.GCInterval)
}

func TestOptions_CustomInitialCapacity(t *testing.T) {
	tb := New(WithInitialCapacity(512))
	defer tb.Stop()

	assert.Equal(t, 512, tb.opts.InitialCapacity)
}
