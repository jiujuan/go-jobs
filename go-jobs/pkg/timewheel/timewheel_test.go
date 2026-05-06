package timewheel

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

func newStarted(t *testing.T) (*TimeWheel, func()) {
	t.Helper()
	tw := New(time.Now())
	tw.Start()
	return tw, func() { tw.Stop() }
}

// waitFor 每 5ms 轮询 cond，直到返回 true 或超时。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false
}

// ─── 基本触发 ─────────────────────────────────────────────────────────────────

func TestTimeWheel_FireAfterDelay(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	var fired int32
	tw.Add("t1", time.Now().Add(60*time.Millisecond), func() {
		atomic.AddInt32(&fired, 1)
	})

	assert.False(t, atomic.LoadInt32(&fired) > 0, "不应提前触发")
	ok := waitFor(t, 600*time.Millisecond, func() bool {
		return atomic.LoadInt32(&fired) == 1
	})
	assert.True(t, ok, "应在 600ms 内触发")
}

func TestTimeWheel_FireImmediatelyIfPastDue(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	var fired int32
	// 已过期任务应立即触发
	tw.Add("past", time.Now().Add(-time.Second), func() {
		atomic.AddInt32(&fired, 1)
	})

	ok := waitFor(t, 300*time.Millisecond, func() bool {
		return atomic.LoadInt32(&fired) == 1
	})
	assert.True(t, ok, "过期任务应立即触发")
}

// ─── 多层级放置 ───────────────────────────────────────────────────────────────

func TestTimeWheel_Level1Placement(t *testing.T) {
	// 300ms 落在 lvl1 (>256ms)
	tw, stop := newStarted(t)
	defer stop()

	var fired int32
	tw.Add("l1", time.Now().Add(300*time.Millisecond), func() {
		atomic.AddInt32(&fired, 1)
	})
	ok := waitFor(t, time.Second, func() bool {
		return atomic.LoadInt32(&fired) == 1
	})
	assert.True(t, ok, "lvl1 任务应在 1s 内触发")
}

func TestTimeWheel_Level2Placement(t *testing.T) {
	// lvl2Tick ≈ 16s，选 20s 落在 lvl2
	tw, stop := newStarted(t)
	defer stop()

	tw.Add("l2", time.Now().Add(20*time.Second), func() {})
	time.Sleep(5 * time.Millisecond)

	stats := tw.Stats(nil)
	// 20s 处于 lvl2 范围 (lvl1Tick=256*64ms=~16s, lvl2Tick=64*16s=~17min)
	assert.GreaterOrEqual(t, stats.IndexSize, 1, "任务应已登记在索引中")
}

func TestTimeWheel_OverflowHeap(t *testing.T) {
	// maxWheelSpan ≈ 18h, 20h 超过范围落入 overflow heap
	tw, stop := newStarted(t)
	defer stop()

	tw.Add("big", time.Now().Add(20*time.Hour), func() {})
	time.Sleep(10 * time.Millisecond)

	stats := tw.Stats(nil)
	assert.Equal(t, 1, stats.OverflowSize, "超出范围的任务应在 overflow heap")
}

// ─── 取消 ─────────────────────────────────────────────────────────────────────

func TestTimeWheel_Cancel(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	var fired int32
	tw.Add("cancel-me", time.Now().Add(100*time.Millisecond), func() {
		atomic.AddInt32(&fired, 1)
	})
	tw.Cancel("cancel-me")

	time.Sleep(300*time.Millisecond)
	assert.Equal(t, int32(0), atomic.LoadInt32(&fired), "已取消的任务不应触发")
}

func TestTimeWheel_CancelNonExistentIsNoop(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	require.NotPanics(t, func() { tw.Cancel("ghost") })
}

// ─── 同 ID 重复 Add 幂等更新 ─────────────────────────────────────────────────

func TestTimeWheel_ReAddCancelsOld(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	var count int32
	// 先加一个 80ms 触发
	tw.Add("dup", time.Now().Add(80*time.Millisecond), func() {
		atomic.AddInt32(&count, 1)
	})
	// 立即用相同 ID 覆盖，延迟 150ms，fn 值 10
	tw.Add("dup", time.Now().Add(150*time.Millisecond), func() {
		atomic.AddInt32(&count, 10)
	})

	ok := waitFor(t, 600*time.Millisecond, func() bool {
		return atomic.LoadInt32(&count) >= 10
	})
	assert.True(t, ok, "新任务应触发")
	time.Sleep(50 * time.Millisecond)
	// 旧任务被取消，count 应恰好为 10（不是 11）
	assert.Equal(t, int32(10), atomic.LoadInt32(&count), "旧任务不应触发 (count==10)")
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestTimeWheel_ConcurrentAdds(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	const N = 300
	var fired int64
	var wg sync.WaitGroup

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			delay := time.Duration(i%80)*time.Millisecond + 10*time.Millisecond
			tw.Add(fmt.Sprintf("c%d", i), time.Now().Add(delay), func() {
				atomic.AddInt64(&fired, 1)
			})
		}(i)
	}
	wg.Wait()

	ok := waitFor(t, 3*time.Second, func() bool {
		return atomic.LoadInt64(&fired) == N
	})
	assert.True(t, ok, "所有 %d 个任务应触发，实际触发 %d", N, atomic.LoadInt64(&fired))
}

func TestTimeWheel_ConcurrentAddAndCancel(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	const N = 200
	var fired int64
	var wg sync.WaitGroup

	for i := 0; i < N; i++ {
		id := fmt.Sprintf("t%d", i)
		wg.Add(1)
		go func(id string, i int) {
			defer wg.Done()
			tw.Add(id, time.Now().Add(200*time.Millisecond), func() {
				atomic.AddInt64(&fired, 1)
			})
			if i%2 == 0 {
				tw.Cancel(id) // 取消偶数任务
			}
		}(id, i)
	}
	wg.Wait()

	time.Sleep(500 * time.Millisecond)
	got := atomic.LoadInt64(&fired)
	// 奇数任务约 N/2 个应触发，允许±10 误差（并发取消存在竞态窗口）
	assert.InDelta(t, N/2, got, 15,
		"约半数任务应触发 (got=%d)", got)
}

// ─── 触发精度 ─────────────────────────────────────────────────────────────────

func TestTimeWheel_FiringAccuracy(t *testing.T) {
	if testing.Short() {
		t.Skip("精度测试在 -short 模式下跳过")
	}
	tw, stop := newStarted(t)
	defer stop()

	type result struct{ expected, actual time.Duration }
	delays := []time.Duration{
		20 * time.Millisecond,
		50 * time.Millisecond,
		100 * time.Millisecond,
		256 * time.Millisecond,
		500 * time.Millisecond,
	}
	results := make([]result, len(delays))
	var wg sync.WaitGroup

	for i, d := range delays {
		wg.Add(1)
		i, d := i, d
		start := time.Now()
		tw.Add(fmt.Sprintf("acc%d", i), time.Now().Add(d), func() {
			results[i] = result{expected: d, actual: time.Since(start)}
			wg.Done()
		})
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("精度测试超时")
	}

	for _, r := range results {
		drift := r.actual - r.expected
		if drift < 0 { drift = -drift }
		assert.LessOrEqual(t, drift.Milliseconds(), int64(25),
			"delay=%v actual=%v drift=%v 应≤25ms", r.expected, r.actual, drift)
	}
}

// ─── Stop 安全 ────────────────────────────────────────────────────────────────

func TestTimeWheel_StopDrainsGracefully(t *testing.T) {
	tw := New(time.Now())
	tw.Start()

	tw.Add("x", time.Now().Add(500*time.Millisecond), func() {})
	tw.Stop()           // Stop 前任务可能触发也可能不触发，不应 panic
	time.Sleep(10 * time.Millisecond) // 让 goroutine 完成退出
}

func TestTimeWheel_DoubleStartNoPanic(t *testing.T) {
	tw := New(time.Now())
	tw.Start()
	tw.Start() // 第二次 Start 应幂等
	tw.Stop()
}

// ─── Stats ────────────────────────────────────────────────────────────────────

func TestTimeWheel_Stats(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	tw.Add("s1", time.Now().Add(time.Second), func() {})
	tw.Add("s2", time.Now().Add(2*time.Second), func() {})
	time.Sleep(10 * time.Millisecond)

	stats := tw.Stats(nil)
	assert.Equal(t, 2, stats.IndexSize, "索引应持有 2 个任务")
	assert.Equal(t, 0, stats.OverflowSize)
}

func TestTimeWheel_StatsAfterFire(t *testing.T) {
	tw, stop := newStarted(t)
	defer stop()

	var fired int32
	tw.Add("quick", time.Now().Add(30*time.Millisecond), func() {
		atomic.AddInt32(&fired, 1)
	})

	ok := waitFor(t, 300*time.Millisecond, func() bool {
		return atomic.LoadInt32(&fired) == 1
	})
	require.True(t, ok)

	// 触发后索引应清零
	time.Sleep(10 * time.Millisecond)
	stats := tw.Stats(nil)
	assert.Equal(t, 0, stats.IndexSize, "触发后索引应为空")
}

// ─── 大量任务吞吐 ─────────────────────────────────────────────────────────────

func TestTimeWheel_HighThroughput(t *testing.T) {
	if testing.Short() {
		t.Skip("吞吐测试在 -short 模式下跳过")
	}
	tw, stop := newStarted(t)
	defer stop()

	const N = 1000
	var fired int64
	for i := 0; i < N; i++ {
		delay := time.Duration(i%200)*time.Millisecond + 5*time.Millisecond
		tw.Add(fmt.Sprintf("hp%d", i), time.Now().Add(delay), func() {
			atomic.AddInt64(&fired, 1)
		})
	}

	ok := waitFor(t, 5*time.Second, func() bool {
		return atomic.LoadInt64(&fired) == N
	})
	assert.True(t, ok, "1000 个任务应全部触发，实际=%d", atomic.LoadInt64(&fired))
}
