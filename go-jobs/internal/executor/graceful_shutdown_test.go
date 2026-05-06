package executor

// graceful_shutdown_test.go
//
// 优雅关闭（两阶段 Stop）的专项单元测试。
//
// 覆盖场景：
//  1.  Stop()：无存量任务时立即返回
//  2.  Stop()：存量任务在超时前自然完成 → 干净退出
//  3.  Stop()：存量任务超时未完成 → 强制 cancel 后退出
//  4.  Run()：Stop() 调用后立即返回 ErrRunnerStopped
//  5.  Stop()：幂等，多次调用安全（不 panic，不死锁）
//  6.  IsStopped()：Stop 前返回 false，Stop 后返回 true
//  7.  Stop()：存量任务 cancel 后感知 ctx.Done 正确退出
//  8.  Stop()：并发 Run + Stop 不 panic、不数据竞争
//  9.  Stop()：WithGracefulTimeout(0) 直接强制关闭
// 10.  RunningJobSet.count()：正确统计并发任务数
// 11.  RunningJobSet.killAll()：cancel 所有在途任务
// 12.  RunningJobSet.waitDone()：超时场景返回 false
// 13.  RunningJobSet.waitDone()：正常完成返回 true
// 14.  Stop() 执行计时：干净关闭耗时 < gracefulTimeout
// 15.  Stop() 执行计时：超时关闭耗时 ≈ gracefulTimeout（有误差容忍）
// 16.  多任务并发：部分完成、部分超时，Stop 正确区分处理
// 17.  Stop 后 Kill() 调用安全（不 panic）
// 18.  Stop 后 IsIdle() 调用安全（不 panic）

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/pkg/idempotency"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

// newGSRunner 创建一个用于优雅关闭测试的 Runner，幂等 TTL 较短避免测试干扰。
func newGSRunner(gracefulTimeout time.Duration) *Runner {
	return NewRunner(NewRegistry(),
		WithIdempotencyTTL(10*time.Second),
		WithIdempotencyGCInterval(time.Minute),
		WithGracefulTimeout(gracefulTimeout),
	)
}

// newGSRunnerWithHandler 创建带指定 Handler 的 Runner。
func newGSRunnerWithHandler(name string, h Handler, gracefulTimeout time.Duration) *Runner {
	reg := NewRegistry()
	reg.Register(name, h)
	return NewRunner(reg,
		WithIdempotencyTTL(10*time.Second),
		WithIdempotencyGCInterval(time.Minute),
		WithGracefulTimeout(gracefulTimeout),
	)
}

// gsReq 快速构造 BEAN RunRequest，logID 唯一即可。
func gsReq(logID, jobID int64, handler string) *RunRequest {
	return &RunRequest{
		LogID:           logID,
		JobID:           jobID,
		ExecutorHandler: handler,
		ExecuteType:     "BEAN",
	}
}

// stopTimed 在 goroutine 里调用 r.Stop()，并通过 channel 返回实际耗时。
func stopTimed(r *Runner) <-chan time.Duration {
	ch := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		r.Stop()
		ch <- time.Since(start)
	}()
	return ch
}

// ─── 1. 无存量任务时 Stop 立即返回 ───────────────────────────────────────────

func TestStop_NoInflightJobs_ReturnsQuickly(t *testing.T) {
	r := newGSRunner(30 * time.Second)

	start := time.Now()
	r.Stop()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 200*time.Millisecond,
		"Stop with no in-flight jobs should return immediately")
}

// ─── 2. 存量任务在超时前自然完成 ─────────────────────────────────────────────

func TestStop_InflightJobsFinishBeforeTimeout_CleanExit(t *testing.T) {
	done := make(chan struct{})
	r := newGSRunnerWithHandler("quickJob", func(ctx context.Context, param string) error {
		time.Sleep(100 * time.Millisecond) // 模拟 100ms 工作
		close(done)
		return nil
	}, 5*time.Second)

	require.NoError(t, r.Run(context.Background(), gsReq(1, 1, "quickJob")))
	time.Sleep(10 * time.Millisecond) // 确保 goroutine 已启动

	start := time.Now()
	r.Stop()
	elapsed := time.Since(start)

	// 任务 100ms 完成，Stop 应在 ~100ms 返回，远小于 5s 超时
	assert.Less(t, elapsed, 2*time.Second,
		"Stop should return shortly after the job finishes")
	select {
	case <-done:
		// job 确实执行完了
	default:
		t.Error("handler should have completed")
	}
}

// ─── 3. 存量任务超时未完成 → 强制 cancel ─────────────────────────────────────

func TestStop_InflightJobsExceedTimeout_ForceCancelled(t *testing.T) {
	cancelReceived := make(chan struct{}, 1)
	r := newGSRunnerWithHandler("slowJob", func(ctx context.Context, param string) error {
		select {
		case <-time.After(10 * time.Second): // 远超 gracefulTimeout
			return nil
		case <-ctx.Done():
			cancelReceived <- struct{}{}
			return ctx.Err()
		}
	}, 300*time.Millisecond) // gracefulTimeout = 300ms

	require.NoError(t, r.Run(context.Background(), gsReq(2, 1, "slowJob")))
	time.Sleep(20 * time.Millisecond) // 确保 goroutine 已启动

	start := time.Now()
	r.Stop()
	elapsed := time.Since(start)

	// Stop 应在 gracefulTimeout + 强制 cancel 等待时间内返回
	assert.Less(t, elapsed, 2*time.Second,
		"Stop should return after force-cancel, not hang forever")

	select {
	case <-cancelReceived:
		// 任务感知到了 cancel ✓
	case <-time.After(500 * time.Millisecond):
		t.Error("slow job should have been force-cancelled")
	}
}

// ─── 4. Stop 后 Run 立即返回 ErrRunnerStopped ─────────────────────────────────

func TestRun_AfterStop_ReturnsErrRunnerStopped(t *testing.T) {
	r := newGSRunner(time.Second)
	r.Stop()

	err := r.Run(context.Background(), gsReq(3, 1, "anything"))
	assert.ErrorIs(t, err, ErrRunnerStopped,
		"Run() after Stop() must return ErrRunnerStopped")
}

func TestRun_AfterStop_NoGoroutineLaunched(t *testing.T) {
	var execCount int64
	r := newGSRunnerWithHandler("neverRun", func(ctx context.Context, param string) error {
		atomic.AddInt64(&execCount, 1)
		return nil
	}, time.Second)

	r.Stop()

	for i := int64(1); i <= 5; i++ {
		r.Run(context.Background(), gsReq(i+100, 1, "neverRun"))
	}
	time.Sleep(100 * time.Millisecond)

	assert.Equal(t, int64(0), atomic.LoadInt64(&execCount),
		"no handler should execute after Stop()")
}

// ─── 5. Stop 幂等：多次调用安全 ─────────────────────────────────────────────

func TestStop_Idempotent_MultipleCallsSafe(t *testing.T) {
	r := newGSRunner(time.Second)

	require.NotPanics(t, func() {
		r.Stop()
		r.Stop()
		r.Stop()
	})
}

func TestStop_Idempotent_ConcurrentCallsSafe(t *testing.T) {
	r := newGSRunner(time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r.Stop()
		}()
	}
	require.NotPanics(t, func() { wg.Wait() })
}

// ─── 6. IsStopped 状态正确 ───────────────────────────────────────────────────

func TestIsStopped_BeforeStop_ReturnsFalse(t *testing.T) {
	r := newGSRunner(time.Second)
	defer r.Stop()
	assert.False(t, r.IsStopped())
}

func TestIsStopped_AfterStop_ReturnsTrue(t *testing.T) {
	r := newGSRunner(time.Second)
	r.Stop()
	assert.True(t, r.IsStopped())
}

// ─── 7. 任务感知 ctx.Done 在 Stop 强制 cancel 后退出 ─────────────────────────

func TestStop_ForceCancelCtxDoneReceived(t *testing.T) {
	ctxErrCh := make(chan error, 1)
	r := newGSRunnerWithHandler("ctxJob", func(ctx context.Context, param string) error {
		select {
		case <-time.After(60 * time.Second):
			return nil
		case <-ctx.Done():
			ctxErrCh <- ctx.Err()
			return ctx.Err()
		}
	}, 100*time.Millisecond)

	require.NoError(t, r.Run(context.Background(), gsReq(4, 1, "ctxJob")))
	time.Sleep(20 * time.Millisecond)

	r.Stop() // 超时后强制 cancel

	select {
	case err := <-ctxErrCh:
		// context.Canceled（来自 killAll）或 DeadlineExceeded（来自 Timeout 设置）
		assert.Error(t, err, "context should have been cancelled")
	case <-time.After(2 * time.Second):
		t.Fatal("job context should have been cancelled by Stop()")
	}
}

// ─── 8. 并发 Run + Stop 无 panic、无数据竞争 ─────────────────────────────────

func TestStop_ConcurrentRunAndStop_NoPanic(t *testing.T) {
	var execCount int64
	r := newGSRunnerWithHandler("concJob", func(ctx context.Context, param string) error {
		atomic.AddInt64(&execCount, 1)
		select {
		case <-time.After(50 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, 500*time.Millisecond)

	var wg sync.WaitGroup
	// 并发发起 50 个 Run
	for i := int64(0); i < 50; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			r.Run(context.Background(), gsReq(id+1000, 1, "concJob"))
		}(i)
	}
	// 并发调用 Stop（多次）
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(10 * time.Millisecond) // 短暂延迟让 Run 先跑
			r.Stop()
		}()
	}

	require.NotPanics(t, func() { wg.Wait() })
	// 执行数 ≤ 50（部分可能被 stopped 拒绝）
	assert.LessOrEqual(t, atomic.LoadInt64(&execCount), int64(50))
}

// ─── 9. WithGracefulTimeout(0) 直接强制关闭 ──────────────────────────────────

func TestStop_ZeroGracefulTimeout_ImmediateForceCancel(t *testing.T) {
	cancelReceived := make(chan struct{}, 1)
	r := newGSRunnerWithHandler("slowJob2", func(ctx context.Context, param string) error {
		select {
		case <-time.After(10 * time.Second):
			return nil
		case <-ctx.Done():
			cancelReceived <- struct{}{}
			return ctx.Err()
		}
	}, 0) // gracefulTimeout = 0 → 立即强制

	require.NoError(t, r.Run(context.Background(), gsReq(5, 1, "slowJob2")))
	time.Sleep(20 * time.Millisecond)

	start := time.Now()
	r.Stop()
	elapsed := time.Since(start)

	// 应在 5s 内完成（killAll + waitDone 5s 上限）
	assert.Less(t, elapsed, 2*time.Second)

	select {
	case <-cancelReceived:
	case <-time.After(500 * time.Millisecond):
		t.Error("job should have been force-cancelled with timeout=0")
	}
}

// ─── 10. RunningJobSet.count() ────────────────────────────────────────────────

func TestRunningJobSet_Count_Accurate(t *testing.T) {
	s := newRunningJobSet()
	assert.Equal(t, 0, s.count())

	cancel1 := func() {}
	cancel2 := func() {}
	s.add(1, 10, cancel1)
	s.add(2, 10, cancel2)
	assert.Equal(t, 2, s.count())

	s.remove(1)
	assert.Equal(t, 1, s.count())

	s.remove(2)
	assert.Equal(t, 0, s.count())
}

// ─── 11. RunningJobSet.killAll() ─────────────────────────────────────────────

func TestRunningJobSet_KillAll_CancelsAllContexts(t *testing.T) {
	s := newRunningJobSet()

	var cancelCount int64
	for i := int64(1); i <= 5; i++ {
		id := i
		ctx, cancel := context.WithCancel(context.Background())
		wrappedCancel := func() {
			atomic.AddInt64(&cancelCount, 1)
			cancel()
		}
		s.add(id, 100, wrappedCancel)
		_ = ctx // 模拟 goroutine 持有
	}

	s.killAll()

	// killAll 只调用 cancel，不调用 remove，count 不变
	assert.Equal(t, int64(5), atomic.LoadInt64(&cancelCount),
		"killAll should cancel all 5 contexts")
}

// ─── 12. RunningJobSet.waitDone() 超时返回 false ─────────────────────────────

func TestRunningJobSet_WaitDone_TimeoutReturnsFalse(t *testing.T) {
	s := newRunningJobSet()

	// add 一个永不 remove 的 job
	s.add(1, 1, func() {})
	// 不 remove → WaitGroup 永不归零

	result := s.waitDone(100 * time.Millisecond)
	assert.False(t, result, "waitDone should return false when timeout elapses")

	// 清理：手动 remove，避免 WaitGroup 泄露
	s.remove(1)
}

// ─── 13. RunningJobSet.waitDone() 正常完成返回 true ──────────────────────────

func TestRunningJobSet_WaitDone_CleanReturnTrue(t *testing.T) {
	s := newRunningJobSet()
	s.add(1, 1, func() {})
	s.add(2, 2, func() {})

	go func() {
		time.Sleep(50 * time.Millisecond)
		s.remove(1)
		s.remove(2)
	}()

	result := s.waitDone(2 * time.Second)
	assert.True(t, result, "waitDone should return true when all jobs finish in time")
}

// ─── 14. Stop 执行计时：干净关闭耗时 < gracefulTimeout ───────────────────────

func TestStop_Timing_CleanShutdownFasterThanTimeout(t *testing.T) {
	const jobDuration = 150 * time.Millisecond
	const graceful = 5 * time.Second

	r := newGSRunnerWithHandler("timedJob", func(ctx context.Context, param string) error {
		time.Sleep(jobDuration)
		return nil
	}, graceful)

	require.NoError(t, r.Run(context.Background(), gsReq(6, 1, "timedJob")))
	time.Sleep(20 * time.Millisecond) // 确保已启动

	elapsed := <-stopTimed(r)

	// 应在任务完成后立即返回，远小于 gracefulTimeout
	assert.Greater(t, elapsed, jobDuration-50*time.Millisecond,
		"Stop should wait for the job to finish")
	assert.Less(t, elapsed, graceful/2,
		"Stop should not wait the full graceful timeout for a quick job")
}

// ─── 15. Stop 执行计时：超时关闭耗时 ≈ gracefulTimeout ───────────────────────

func TestStop_Timing_TimeoutShutdownTakesGracefulDuration(t *testing.T) {
	const graceful = 300 * time.Millisecond
	const margin = 500 * time.Millisecond // 容忍测试机器调度延迟

	r := newGSRunnerWithHandler("infiniteJob", func(ctx context.Context, param string) error {
		select {
		case <-time.After(60 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, graceful)

	require.NoError(t, r.Run(context.Background(), gsReq(7, 1, "infiniteJob")))
	time.Sleep(20 * time.Millisecond)

	elapsed := <-stopTimed(r)

	// 最少等待 gracefulTimeout，最多 gracefulTimeout + margin（强制 cancel 等待）
	assert.GreaterOrEqual(t, elapsed, graceful-50*time.Millisecond,
		"Stop should wait at least gracefulTimeout before force-cancel")
	assert.Less(t, elapsed, graceful+margin,
		"Stop should not take much longer than gracefulTimeout + force-cancel time")
}

// ─── 16. 多任务：部分快完成，部分超时被强制 cancel ───────────────────────────

func TestStop_MixedJobs_SomeFinishSomeForced(t *testing.T) {
	var quickDone int64
	var forcedCancel int64

	reg := NewRegistry()
	// 快任务：50ms 完成
	reg.Register("quickMix", func(ctx context.Context, param string) error {
		time.Sleep(50 * time.Millisecond)
		atomic.AddInt64(&quickDone, 1)
		return nil
	})
	// 慢任务：等 cancel
	reg.Register("slowMix", func(ctx context.Context, param string) error {
		select {
		case <-time.After(60 * time.Second):
			return nil
		case <-ctx.Done():
			atomic.AddInt64(&forcedCancel, 1)
			return ctx.Err()
		}
	})

	r := NewRunner(reg,
		WithIdempotencyTTL(10*time.Second),
		WithIdempotencyGCInterval(time.Minute),
		WithGracefulTimeout(200*time.Millisecond),
	)

	// 启动 3 个快任务 + 2 个慢任务
	for i := int64(0); i < 3; i++ {
		require.NoError(t, r.Run(context.Background(), &RunRequest{
			LogID: 100 + i, JobID: 1, ExecutorHandler: "quickMix", ExecuteType: "BEAN",
		}))
	}
	for i := int64(0); i < 2; i++ {
		require.NoError(t, r.Run(context.Background(), &RunRequest{
			LogID: 200 + i, JobID: 2, ExecutorHandler: "slowMix", ExecuteType: "BEAN",
		}))
	}
	time.Sleep(20 * time.Millisecond)

	r.Stop()

	// 快任务应完成
	assert.Equal(t, int64(3), atomic.LoadInt64(&quickDone),
		"3 quick jobs should finish naturally")
	// 慢任务应被强制 cancel
	assert.Equal(t, int64(2), atomic.LoadInt64(&forcedCancel),
		"2 slow jobs should be force-cancelled")
}

// ─── 17. Stop 后 Kill() 调用安全 ─────────────────────────────────────────────

func TestStop_ThenKill_Safe(t *testing.T) {
	r := newGSRunner(time.Second)
	r.Stop()

	require.NotPanics(t, func() {
		result := r.Kill(9999) // 不存在的 logID
		assert.False(t, result)
	})
}

// ─── 18. Stop 后 IsIdle() 调用安全 ───────────────────────────────────────────

func TestStop_ThenIsIdle_Safe(t *testing.T) {
	r := newGSRunner(time.Second)
	r.Stop()

	require.NotPanics(t, func() {
		result := r.IsIdle(1)
		assert.True(t, result) // 已停止，无任务运行
	})
}

// ─── 19. 存量任务的幂等记录正确标记为 Failed（被 cancel） ─────────────────────

func TestStop_ForcedCancel_JobRecordedAsFailed(t *testing.T) {
	r := newGSRunnerWithHandler("cancelledJob", func(ctx context.Context, param string) error {
		select {
		case <-time.After(60 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, 100*time.Millisecond)

	require.NoError(t, r.Run(context.Background(), gsReq(8, 1, "cancelledJob")))
	time.Sleep(20 * time.Millisecond)

	r.Stop()

	// 给 idempotency.Complete 一点时间写入
	time.Sleep(50 * time.Millisecond)
	rec := r.idempotency.Get(8)
	require.NotNil(t, rec, "idempotency record should exist")
	assert.Equal(t, idempotency.StateFailed, rec.State,
		"force-cancelled job should be recorded as Failed")
}

// ─── 20. WithGracefulTimeout 选项正确透传 ────────────────────────────────────

func TestWithGracefulTimeout_Option_Applied(t *testing.T) {
	r := NewRunner(NewRegistry(), WithGracefulTimeout(42*time.Second))
	defer r.Stop()
	assert.Equal(t, 42*time.Second, r.gracefulTimeout)
}

func TestWithGracefulTimeout_Default_Is30s(t *testing.T) {
	r := NewRunner(NewRegistry())
	defer r.Stop()
	assert.Equal(t, 30*time.Second, r.gracefulTimeout)
}

// ─── 21. 任务完成后 Stop 立即返回（WaitGroup 归零） ──────────────────────────

func TestStop_JobFinishedBeforeStop_WaitGroupAlreadyZero(t *testing.T) {
	r := newGSRunnerWithHandler("fastJob", func(ctx context.Context, param string) error {
		return nil
	}, 5*time.Second)

	require.NoError(t, r.Run(context.Background(), gsReq(9, 1, "fastJob")))

	// 等待任务完成
	time.Sleep(100 * time.Millisecond)

	start := time.Now()
	r.Stop()
	elapsed := time.Since(start)

	assert.Less(t, elapsed, 500*time.Millisecond,
		"Stop should be instant when all jobs already finished")
}

// ─── 22. Stop 阻塞等待直到 goroutine 真正退出（无僵尸任务） ─────────────────

func TestStop_BlocksUntilGoroutineActuallyExits(t *testing.T) {
	goroutineExited := make(chan struct{})

	r := newGSRunnerWithHandler("goroutineJob", func(ctx context.Context, param string) error {
		select {
		case <-time.After(60 * time.Second):
		case <-ctx.Done():
		}
		// 模拟 handler 退出后的清理工作（延迟 50ms）
		time.Sleep(50 * time.Millisecond)
		close(goroutineExited)
		return ctx.Err()
	}, 100*time.Millisecond)

	require.NoError(t, r.Run(context.Background(), gsReq(10, 1, "goroutineJob")))
	time.Sleep(20 * time.Millisecond)

	r.Stop() // 应等待 goroutine 真正退出（包括 50ms 清理）

	select {
	case <-goroutineExited:
		// goroutine 已完全退出 ✓
	default:
		t.Error("Stop() should block until goroutine fully exits")
	}
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

// BenchmarkStop_NoInflight 测量无任务时 Stop 的开销（应接近 0）。
func BenchmarkStop_NoInflight(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := newGSRunner(time.Second)
		r.Stop()
	}
}

// BenchmarkRunAfterStop 测量 Stop 后 Run() 的拒绝路径开销。
func BenchmarkRunAfterStop(b *testing.B) {
	r := newGSRunner(time.Second)
	r.Stop()
	req := gsReq(1, 1, "x")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Run(context.Background(), req)
		}
	})
}
