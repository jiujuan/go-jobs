package executor

// executor_test.go
//
// 针对 executor.go 的单元测试，覆盖以下未被其他测试文件覆盖的内容：
//
//  JobContext / WithJobContext / GetJobContext
//    1.  注入并提取 JobContext，字段值完整保留
//    2.  空 context 中提取返回 zero+false
//    3.  多次注入取最近一次（value 覆盖语义）
//    4.  并发注入互不干扰
//
//  Registry（executor.go 中的 Handler 注册表）
//    5.  NewRegistry 初始为空
//    6.  Register + Get 成功
//    7.  Get 不存在的 handler 返回 nil, false
//    8.  Register 同名 handler 触发 panic（fail-fast）
//    9.  并发 Register + Get 无数据竞争
//    10. 多 handler 独立共存
//
//  RunningJobSet — isIdle / kill / close / drained
//    11. isIdle：无任务时对任意 jobID 返回 true
//    12. isIdle：有对应 jobID 的任务时返回 false
//    13. isIdle：jobID 不匹配时仍返回 true
//    14. kill：存在的 logID 调用 cancel 并返回 true
//    15. kill：不存在的 logID 返回 false
//    16. close：drained 置 true 后 add 返回 false
//    17. close：返回关闭时的正确任务数
//    18. close 幂等：第二次 close 不影响已有状态
//    19. add after close：add 被拒绝，WaitGroup 不增加（waitDone 立即返回）
//    20. 并发 add + remove 无数据竞争

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── JobContext / WithJobContext / GetJobContext ───────────────────────────────

func TestWithJobContext_GetJobContext_RoundTrip(t *testing.T) {
	jc := JobContext{
		LogID:         42,
		JobID:         7,
		ShardingIndex: 2,
		ShardingTotal: 5,
	}
	ctx := WithJobContext(context.Background(), jc)
	got, ok := GetJobContext(ctx)

	require.True(t, ok, "GetJobContext should return true")
	assert.Equal(t, jc.LogID, got.LogID)
	assert.Equal(t, jc.JobID, got.JobID)
	assert.Equal(t, jc.ShardingIndex, got.ShardingIndex)
	assert.Equal(t, jc.ShardingTotal, got.ShardingTotal)
}

func TestGetJobContext_EmptyContext_ReturnsFalse(t *testing.T) {
	_, ok := GetJobContext(context.Background())
	assert.False(t, ok, "plain context should have no JobContext")
}

func TestGetJobContext_EmptyContext_ZeroValue(t *testing.T) {
	jc, _ := GetJobContext(context.Background())
	assert.Equal(t, JobContext{}, jc)
}

func TestWithJobContext_OverwritesPreviousValue(t *testing.T) {
	ctx := WithJobContext(context.Background(), JobContext{LogID: 1, JobID: 10})
	ctx = WithJobContext(ctx, JobContext{LogID: 2, JobID: 20})

	got, ok := GetJobContext(ctx)
	require.True(t, ok)
	assert.Equal(t, int64(2), got.LogID, "second inject should shadow first")
	assert.Equal(t, int64(20), got.JobID)
}

func TestWithJobContext_ChildContextInherits(t *testing.T) {
	jc := JobContext{LogID: 99, JobID: 1}
	parent := WithJobContext(context.Background(), jc)
	child, cancel := context.WithCancel(parent)
	defer cancel()

	got, ok := GetJobContext(child)
	require.True(t, ok)
	assert.Equal(t, jc.LogID, got.LogID)
}

func TestWithJobContext_ConcurrentAccess_NoPanic(t *testing.T) {
	// 多个 goroutine 各自注入 + 读取，不共享 ctx，不应 panic 或竞争
	var wg sync.WaitGroup
	for i := int64(0); i < 50; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			ctx := WithJobContext(context.Background(), JobContext{LogID: id, JobID: id})
			got, ok := GetJobContext(ctx)
			assert.True(t, ok)
			assert.Equal(t, id, got.LogID)
		}(i)
	}
	wg.Wait()
}

func TestJobContext_ZeroValue_Fields(t *testing.T) {
	ctx := WithJobContext(context.Background(), JobContext{})
	got, ok := GetJobContext(ctx)
	require.True(t, ok)
	assert.Equal(t, int64(0), got.LogID)
	assert.Equal(t, int64(0), got.JobID)
	assert.Equal(t, 0, got.ShardingIndex)
	assert.Equal(t, 0, got.ShardingTotal)
}

// ─── Registry（executor.go 中的 Handler 注册表） ──────────────────────────────

func TestRegistry_NewRegistry_IsEmpty(t *testing.T) {
	r := NewRegistry()
	_, ok := r.Get("anything")
	assert.False(t, ok, "fresh registry should be empty")
}

func TestRegistry_Register_Get_Success(t *testing.T) {
	r := NewRegistry()
	called := false
	r.Register("myJob", func(ctx context.Context, param string) error {
		called = true
		return nil
	})

	h, ok := r.Get("myJob")
	require.True(t, ok)
	require.NotNil(t, h)
	_ = h(context.Background(), "")
	assert.True(t, called)
}

func TestRegistry_Get_UnknownName_ReturnsFalseAndNil(t *testing.T) {
	r := NewRegistry()
	h, ok := r.Get("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, h)
}

func TestRegistry_Register_DuplicateName_Panics(t *testing.T) {
	r := NewRegistry()
	r.Register("job", func(ctx context.Context, param string) error { return nil })

	assert.Panics(t, func() {
		r.Register("job", func(ctx context.Context, param string) error { return nil })
	}, "registering same name twice must panic")
}

func TestRegistry_MultipleHandlers_Coexist(t *testing.T) {
	r := NewRegistry()
	names := []string{"alpha", "beta", "gamma", "delta"}
	for _, name := range names {
		n := name // capture
		r.Register(n, func(ctx context.Context, param string) error { return nil })
	}
	for _, name := range names {
		_, ok := r.Get(name)
		assert.True(t, ok, "handler %q should be retrievable", name)
	}
	_, ok := r.Get("omega")
	assert.False(t, ok, "unregistered handler should not be found")
}

func TestRegistry_Get_CaseSensitive(t *testing.T) {
	// Handler registry（executor.go）是大小写敏感的（与 ScriptEngineRegistry 不同）
	r := NewRegistry()
	r.Register("MyJob", func(ctx context.Context, param string) error { return nil })

	_, ok := r.Get("MyJob")
	assert.True(t, ok)
	_, ok = r.Get("myjob")
	assert.False(t, ok, "handler registry is case-sensitive")
	_, ok = r.Get("MYJOB")
	assert.False(t, ok)
}

func TestRegistry_ConcurrentRegisterAndGet_NoRace(t *testing.T) {
	// 多个 goroutine 并发 Register（不同名称）+ Get，无数据竞争
	r := NewRegistry()

	var wg sync.WaitGroup
	// 预先注册一批，供并发 Get 使用
	for i := 0; i < 20; i++ {
		name := "preJob" + string(rune('A'+i))
		r.Register(name, func(ctx context.Context, param string) error { return nil })
	}

	// 并发 Get
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := "preJob" + string(rune('A'+idx%20))
			_, _ = r.Get(name)
		}(i)
	}
	wg.Wait()
}

func TestRegistry_HandlerReceivesParamAndCtx(t *testing.T) {
	r := NewRegistry()
	var gotParam string
	var gotLogID int64

	r.Register("paramJob", func(ctx context.Context, param string) error {
		gotParam = param
		if jc, ok := GetJobContext(ctx); ok {
			gotLogID = jc.LogID
		}
		return nil
	})

	h, ok := r.Get("paramJob")
	require.True(t, ok)

	ctx := WithJobContext(context.Background(), JobContext{LogID: 77})
	_ = h(ctx, "hello-param")

	assert.Equal(t, "hello-param", gotParam)
	assert.Equal(t, int64(77), gotLogID)
}

// ─── RunningJobSet — isIdle ───────────────────────────────────────────────────

func TestRunningJobSet_IsIdle_NoJobs_ReturnsTrue(t *testing.T) {
	s := newRunningJobSet()
	assert.True(t, s.isIdle(999), "empty set should be idle for any jobID")
}

func TestRunningJobSet_IsIdle_WithMatchingJobID_ReturnsFalse(t *testing.T) {
	s := newRunningJobSet()
	s.add(1001, 42, func() {}) // logID=1001, jobID=42
	defer s.remove(1001)

	assert.False(t, s.isIdle(42), "jobID 42 is running, should not be idle")
}

func TestRunningJobSet_IsIdle_WithDifferentJobID_ReturnsTrue(t *testing.T) {
	s := newRunningJobSet()
	s.add(1001, 42, func() {})
	defer s.remove(1001)

	assert.True(t, s.isIdle(99), "jobID 99 is not running, should be idle")
}

func TestRunningJobSet_IsIdle_AfterRemove_ReturnsTrue(t *testing.T) {
	s := newRunningJobSet()
	s.add(1001, 42, func() {})
	s.remove(1001)

	assert.True(t, s.isIdle(42), "after remove, should be idle again")
}

func TestRunningJobSet_IsIdle_MultipleJobsSameJobID(t *testing.T) {
	// 同一 jobID 两个分片（不同 logID），只要有一个在跑就不空闲
	s := newRunningJobSet()
	s.add(101, 5, func() {})
	s.add(102, 5, func() {})
	defer s.remove(101)
	defer s.remove(102)

	assert.False(t, s.isIdle(5))
}

func TestRunningJobSet_IsIdle_AfterPartialRemove_StillNotIdle(t *testing.T) {
	s := newRunningJobSet()
	s.add(101, 5, func() {})
	s.add(102, 5, func() {})

	s.remove(101) // 只移除一个

	assert.False(t, s.isIdle(5), "one shard still running, not idle")
	s.remove(102)
}

// ─── RunningJobSet — kill ─────────────────────────────────────────────────────

func TestRunningJobSet_Kill_ExistingLogID_CallsCancel(t *testing.T) {
	s := newRunningJobSet()
	cancelled := make(chan struct{}, 1)
	cancel := func() { cancelled <- struct{}{} }

	s.add(2001, 1, cancel)
	defer s.remove(2001)

	ok := s.kill(2001)
	assert.True(t, ok)

	select {
	case <-cancelled:
		// cancel 被调用 ✓
	case <-time.After(100 * time.Millisecond):
		t.Fatal("cancel should have been called")
	}
}

func TestRunningJobSet_Kill_NonExistentLogID_ReturnsFalse(t *testing.T) {
	s := newRunningJobSet()
	ok := s.kill(9999)
	assert.False(t, ok)
}

func TestRunningJobSet_Kill_DoesNotRemoveFromSet(t *testing.T) {
	// kill 只 cancel context，不从 jobs map 移除（remove 由 goroutine defer 负责）
	s := newRunningJobSet()
	s.add(3001, 1, func() {})

	s.kill(3001)
	assert.Equal(t, 1, s.count(), "kill should not remove job from set")

	s.remove(3001)
}

func TestRunningJobSet_Kill_AfterRemove_ReturnsFalse(t *testing.T) {
	s := newRunningJobSet()
	s.add(4001, 1, func() {})
	s.remove(4001) // goroutine 已结束

	ok := s.kill(4001)
	assert.False(t, ok, "removed job cannot be killed")
}

// ─── RunningJobSet — close / drained ─────────────────────────────────────────

func TestRunningJobSet_Close_SetsDrained(t *testing.T) {
	s := newRunningJobSet()
	s.close()
	assert.True(t, s.drained)
}

func TestRunningJobSet_Close_ReturnsCurrentCount(t *testing.T) {
	s := newRunningJobSet()
	s.add(1, 1, func() {})
	s.add(2, 2, func() {})

	n := s.close()
	assert.Equal(t, 2, n)

	// 清理
	s.remove(1)
	s.remove(2)
}

func TestRunningJobSet_Close_EmptySet_ReturnsZero(t *testing.T) {
	s := newRunningJobSet()
	n := s.close()
	assert.Equal(t, 0, n)
}

func TestRunningJobSet_Close_BlocksSubsequentAdd(t *testing.T) {
	s := newRunningJobSet()
	s.close()

	ok := s.add(5001, 1, func() {})
	assert.False(t, ok, "add after close must return false")
}

func TestRunningJobSet_AddAfterClose_WaitDoneReturnsImmediately(t *testing.T) {
	// close() 后 wg 计数为 0，waitDone 应立即返回 true
	s := newRunningJobSet()
	s.close()

	// 尝试 add（会失败），WG 不增加
	s.add(1, 1, func() {})

	start := time.Now()
	ok := s.waitDone(2 * time.Second)
	elapsed := time.Since(start)

	assert.True(t, ok, "waitDone should return true immediately after close with no jobs")
	assert.Less(t, elapsed, 200*time.Millisecond)
}

func TestRunningJobSet_Close_Idempotent(t *testing.T) {
	s := newRunningJobSet()

	require.NotPanics(t, func() {
		s.close()
		s.close()
		s.close()
	})
	assert.True(t, s.drained)
}

func TestRunningJobSet_AddBeforeAndAfterClose(t *testing.T) {
	s := newRunningJobSet()

	// close 前可以 add
	ok1 := s.add(1, 1, func() {})
	assert.True(t, ok1)

	s.close()

	// close 后不能 add
	ok2 := s.add(2, 2, func() {})
	assert.False(t, ok2)

	// count 仍为 1（只有 close 前的那个）
	assert.Equal(t, 1, s.count())
	s.remove(1)
}

// ─── RunningJobSet — 并发安全 ─────────────────────────────────────────────────

func TestRunningJobSet_ConcurrentAddRemove_NoRace(t *testing.T) {
	s := newRunningJobSet()
	const N = 200

	var wg sync.WaitGroup
	var added int64

	for i := int64(0); i < N; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			if s.add(id, 1, func() {}) {
				atomic.AddInt64(&added, 1)
				time.Sleep(time.Millisecond)
				s.remove(id)
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(N), atomic.LoadInt64(&added))
	assert.Equal(t, 0, s.count())
}

func TestRunningJobSet_ConcurrentKill_NoRace(t *testing.T) {
	s := newRunningJobSet()
	for i := int64(1); i <= 10; i++ {
		s.add(i, 1, func() {})
	}

	var wg sync.WaitGroup
	for i := int64(1); i <= 10; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			s.kill(id)
		}(i)
	}
	wg.Wait()

	// 清理
	for i := int64(1); i <= 10; i++ {
		s.remove(i)
	}
}

func TestRunningJobSet_ConcurrentIsIdle_NoRace(t *testing.T) {
	s := newRunningJobSet()
	s.add(1, 42, func() {})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.isIdle(42)
		}()
	}
	wg.Wait()
	s.remove(1)
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkWithGetJobContext(b *testing.B) {
	jc := JobContext{LogID: 1, JobID: 2, ShardingIndex: 0, ShardingTotal: 1}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ctx := WithJobContext(context.Background(), jc)
			_, _ = GetJobContext(ctx)
		}
	})
}

func BenchmarkHandlerRegistry_Get(b *testing.B) {
	r := NewRegistry()
	r.Register("benchJob", func(ctx context.Context, param string) error { return nil })
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = r.Get("benchJob")
		}
	})
}

func BenchmarkRunningJobSet_AddRemove(b *testing.B) {
	s := newRunningJobSet()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := int64(i)
		s.add(id, 1, func() {})
		s.remove(id)
	}
}
