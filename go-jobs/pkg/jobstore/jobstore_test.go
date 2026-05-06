package jobstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

type nopLogger struct{}

func (n *nopLogger) Info(msg string, fields ...zap.Field)  {}
func (n *nopLogger) Warn(msg string, fields ...zap.Field)  {}
func (n *nopLogger) Error(msg string, fields ...zap.Field) {}

func newStore() *Store { return New(&nopLogger{}) }

func makeJob(id int64, status model.JobStatus, next time.Time) *model.JobInfo {
	return &model.JobInfo{
		ID:              id,
		JobName:         fmt.Sprintf("job-%d", id),
		Status:          status,
		NextTriggerTime: &next,
	}
}

func makeJobNoTime(id int64, status model.JobStatus) *model.JobInfo {
	return &model.JobInfo{ID: id, JobName: fmt.Sprintf("job-%d", id), Status: status}
}

// mockFlusher 用于 flush loop 测试
type mockFlusher struct {
	mu    sync.Mutex
	calls []*model.JobInfo
	errOn int // 前 errOn 次调用返回错误
	fn    func(*model.JobInfo) error
}

func (m *mockFlusher) FlushJob(_ context.Context, job *model.JobInfo) error {
	if m.fn != nil {
		return m.fn(job)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, job)
	return nil
}

func (m *mockFlusher) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.calls)
}

// ─── Add / Get / Remove ───────────────────────────────────────────────────────

func TestStore_AddAndGet(t *testing.T) {
	s := newStore()
	next := time.Now().Add(time.Minute)
	s.Add(makeJob(1, model.JobRun, next))

	got, ok := s.Get(1)
	require.True(t, ok)
	assert.Equal(t, int64(1), got.ID)
	assert.Equal(t, model.JobRun, got.Status)
	assert.Equal(t, int64(1), s.Len())
}

func TestStore_GetMissing(t *testing.T) {
	s := newStore()
	_, ok := s.Get(9999)
	assert.False(t, ok)
}

func TestStore_Remove(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(time.Minute)))
	s.Remove(1)

	_, ok := s.Get(1)
	assert.False(t, ok)
	assert.Equal(t, int64(0), s.Len())
}

func TestStore_RemoveNonExistentIsNoop(t *testing.T) {
	s := newStore()
	require.NotPanics(t, func() { s.Remove(42) })
}

func TestStore_AddUpsert(t *testing.T) {
	s := newStore()
	next := time.Now().Add(time.Minute)
	s.Add(makeJob(1, model.JobRun, next))
	s.Add(makeJob(1, model.JobStop, next)) // 用相同 ID 覆盖

	got, ok := s.Get(1)
	require.True(t, ok)
	assert.Equal(t, model.JobStop, got.Status)
	assert.Equal(t, int64(1), s.Len(), "upsert 不应增加计数")
}

func TestStore_GetReturnsCopy(t *testing.T) {
	s := newStore()
	next := time.Now().Add(time.Minute)
	s.Add(makeJob(1, model.JobRun, next))

	got, _ := s.Get(1)
	got.JobName = "modified"

	// 再次 Get，内部存储不应受到修改
	got2, _ := s.Get(1)
	assert.Equal(t, "job-1", got2.JobName)
}

// ─── Heap / PopDue ────────────────────────────────────────────────────────────

func TestStore_PopDue_Empty(t *testing.T) {
	s := newStore()
	result := s.PopDue(time.Now(), 10)
	assert.Empty(t, result)
}

func TestStore_PopDue_SingleDue(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(-time.Second)))

	due := s.PopDue(time.Now(), 100)
	assert.Len(t, due, 1)
	assert.Equal(t, int64(1), due[0].ID)
	assert.Equal(t, 0, s.HeapLen(), "pop 后堆应为空")
}

func TestStore_PopDue_OnlyDueJobs(t *testing.T) {
	s := newStore()
	past := time.Now().Add(-time.Second)
	future := time.Now().Add(10 * time.Minute)

	s.Add(makeJob(1, model.JobRun, past))
	s.Add(makeJob(2, model.JobRun, future))
	s.Add(makeJob(3, model.JobRun, past))

	due := s.PopDue(time.Now(), 100)
	ids := make(map[int64]bool)
	for _, j := range due {
		ids[j.ID] = true
	}
	assert.True(t, ids[1])
	assert.True(t, ids[3])
	assert.False(t, ids[2], "未到期任务不应弹出")
	assert.Equal(t, 1, s.HeapLen(), "未到期任务应留在堆中")
}

func TestStore_PopDue_Limit(t *testing.T) {
	s := newStore()
	past := time.Now().Add(-time.Second)
	for i := 1; i <= 10; i++ {
		s.Add(makeJob(int64(i), model.JobRun, past))
	}
	due := s.PopDue(time.Now(), 3)
	assert.Len(t, due, 3)
}

func TestStore_PopDue_SkipsStoppedJobs(t *testing.T) {
	s := newStore()
	past := time.Now().Add(-time.Second)
	s.Add(makeJob(1, model.JobStop, past)) // 已停止，不应弹出
	s.Add(makeJob(2, model.JobRun, past))

	due := s.PopDue(time.Now(), 100)
	assert.Len(t, due, 1)
	assert.Equal(t, int64(2), due[0].ID)
}

// ─── HeapOrdering ─────────────────────────────────────────────────────────────

func TestStore_HeapOrdering(t *testing.T) {
	s := newStore()
	now := time.Now()
	// 倒序插入
	s.Add(makeJob(3, model.JobRun, now.Add(3*time.Minute)))
	s.Add(makeJob(1, model.JobRun, now.Add(1*time.Minute)))
	s.Add(makeJob(2, model.JobRun, now.Add(2*time.Minute)))

	due := s.PopDue(now.Add(10*time.Minute), 10)
	require.Len(t, due, 3)
	assert.Equal(t, int64(1), due[0].ID, "最早触发的任务应排在第一")
	assert.Equal(t, int64(2), due[1].ID)
	assert.Equal(t, int64(3), due[2].ID)
}

// ─── UpdateNextTrigger ────────────────────────────────────────────────────────

func TestStore_UpdateNextTrigger(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(-time.Second)))
	s.PopDue(time.Now(), 10) // 模拟调度出队

	next := time.Now().Add(5 * time.Minute)
	err := s.UpdateNextTrigger(1, next, time.Now())
	require.NoError(t, err)

	assert.Equal(t, 1, s.HeapLen(), "重新调度后应回到堆中")

	got, ok := s.Get(1)
	require.True(t, ok)
	assert.Equal(t, next.Unix(), got.NextTriggerTime.Unix())
}

func TestStore_UpdateNextTrigger_MissingJob(t *testing.T) {
	s := newStore()
	err := s.UpdateNextTrigger(999, time.Now(), time.Now())
	assert.Error(t, err)
}

func TestStore_UpdateNextTrigger_RepositionsInHeap(t *testing.T) {
	s := newStore()
	now := time.Now()
	s.Add(makeJob(1, model.JobRun, now.Add(5*time.Minute)))
	s.Add(makeJob(2, model.JobRun, now.Add(10*time.Minute)))

	// 把 job2 移到 job1 之前
	err := s.UpdateNextTrigger(2, now.Add(1*time.Minute), now)
	require.NoError(t, err)

	due := s.PopDue(now.Add(20*time.Minute), 10)
	require.Len(t, due, 2)
	assert.Equal(t, int64(2), due[0].ID, "调整后 job2 应排在前面")
}

// ─── UpdateStatus ─────────────────────────────────────────────────────────────

func TestStore_UpdateStatus_StopRemovesFromHeap(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(time.Minute)))
	assert.Equal(t, 1, s.HeapLen())

	err := s.UpdateStatus(1, model.JobStop)
	require.NoError(t, err)
	assert.Equal(t, 0, s.HeapLen(), "停止后应离开堆")
}

func TestStore_UpdateStatus_StartAddsToHeap(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobStop, time.Now().Add(time.Minute)))
	assert.Equal(t, 0, s.HeapLen(), "停止状态不应在堆中")

	err := s.UpdateStatus(1, model.JobRun)
	require.NoError(t, err)
	assert.Equal(t, 1, s.HeapLen(), "启动后应进入堆")
}

func TestStore_UpdateStatus_MissingJob(t *testing.T) {
	s := newStore()
	err := s.UpdateStatus(999, model.JobRun)
	assert.Error(t, err)
}

func TestStore_UpdateStatus_NoTimeNotInHeap(t *testing.T) {
	// 无 NextTriggerTime 的任务启动后不应进入堆
	s := newStore()
	s.Add(makeJobNoTime(1, model.JobStop))

	err := s.UpdateStatus(1, model.JobRun)
	require.NoError(t, err)
	assert.Equal(t, 0, s.HeapLen(), "无触发时间的任务不应入堆")
}

// ─── LoadAll 引导加载 ─────────────────────────────────────────────────────────

func TestStore_LoadAll(t *testing.T) {
	s := newStore()
	jobs := []*model.JobInfo{
		makeJob(1, model.JobRun, time.Now().Add(time.Minute)),
		makeJob(2, model.JobStop, time.Now().Add(time.Minute)),
		makeJob(3, model.JobRun, time.Now().Add(2*time.Minute)),
	}
	s.LoadAll(jobs)

	assert.Equal(t, int64(3), s.Len())
	assert.Equal(t, 2, s.HeapLen(), "只有运行中的任务入堆")
}

func TestStore_LoadAll_NoDirtyFlag(t *testing.T) {
	// bootstrap 加载的任务不应标记为 dirty（已存在于 MySQL）
	s := newStore()
	s.LoadAll([]*model.JobInfo{makeJob(1, model.JobRun, time.Now().Add(time.Minute))})

	dirty := s.DrainDirty()
	assert.Empty(t, dirty, "bootstrap 加载的任务不应为 dirty")
}

// ─── Dirty 标记与刷新 ─────────────────────────────────────────────────────────

func TestStore_DrainDirty_AfterAdd(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(time.Minute)))

	dirty := s.DrainDirty()
	assert.Len(t, dirty, 1)

	// 第二次 DrainDirty 应为空（标志已清除）
	assert.Empty(t, s.DrainDirty())
}

func TestStore_MarkDirty(t *testing.T) {
	s := newStore()
	s.LoadAll([]*model.JobInfo{makeJob(1, model.JobRun, time.Now().Add(time.Minute))})
	s.DrainDirty() // 确认为空

	s.MarkDirty(1)
	assert.Len(t, s.DrainDirty(), 1)
}

func TestStore_MarkDirty_NonExistentNoop(t *testing.T) {
	s := newStore()
	require.NotPanics(t, func() { s.MarkDirty(9999) })
}

// ─── Range ────────────────────────────────────────────────────────────────────

func TestStore_Range(t *testing.T) {
	s := newStore()
	for i := 1; i <= 5; i++ {
		s.Add(makeJob(int64(i), model.JobRun, time.Now().Add(time.Minute)))
	}

	var seen []int64
	s.Range(func(j *model.JobInfo) bool {
		seen = append(seen, j.ID)
		return true
	})
	assert.Len(t, seen, 5)
}

func TestStore_Range_EarlyStop(t *testing.T) {
	s := newStore()
	for i := 1; i <= 10; i++ {
		s.Add(makeJob(int64(i), model.JobRun, time.Now().Add(time.Minute)))
	}

	count := 0
	s.Range(func(_ *model.JobInfo) bool {
		count++
		return count < 3
	})
	assert.Equal(t, 3, count)
}

// ─── Flush Loop ───────────────────────────────────────────────────────────────

func TestStore_FlushLoop_CallsFlusher(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(time.Minute)))
	s.Add(makeJob(2, model.JobRun, time.Now().Add(2*time.Minute)))

	var flushed int32
	flusher := &mockFlusher{fn: func(j *model.JobInfo) error {
		atomic.AddInt32(&flushed, 1)
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	s.StartFlushLoop(ctx, 20*time.Millisecond, flusher)
	time.Sleep(120*time.Millisecond)
	cancel()
	time.Sleep(30*time.Millisecond)

	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&flushed)), 2, "两个 dirty 任务应被刷写")
}

func TestStore_FlushLoop_RetryOnError(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(time.Minute)))

	var calls int32
	flusher := &mockFlusher{fn: func(j *model.JobInfo) error {
		n := atomic.AddInt32(&calls, 1)
		if n < 3 {
			return fmt.Errorf("transient error")
		}
		return nil
	}}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.StartFlushLoop(ctx, 20*time.Millisecond, flusher)
	time.Sleep(200*time.Millisecond)

	assert.GreaterOrEqual(t, int(atomic.LoadInt32(&calls)), 3, "失败后应重试直到成功")
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestStore_ConcurrentAddGetRemove(t *testing.T) {
	s := newStore()
	const N = 500
	var wg sync.WaitGroup

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Add(makeJob(int64(i), model.JobRun, time.Now().Add(time.Minute)))
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int64(N), s.Len())

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _ = s.Get(int64(i))
			if i%2 == 0 {
				s.Remove(int64(i))
			}
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int64(N/2), s.Len())
}

func TestStore_ConcurrentUpdateNextTrigger(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(time.Minute)))

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			next := time.Now().Add(time.Duration(i+1) * time.Minute)
			_ = s.UpdateNextTrigger(1, next, time.Now())
		}(i)
	}
	wg.Wait()

	_, ok := s.Get(1)
	assert.True(t, ok, "并发更新后任务应仍存在")
	assert.Equal(t, 1, s.HeapLen(), "并发更新后堆中应有且仅有一个条目")
}

func TestStore_ConcurrentUpdateStatus(t *testing.T) {
	s := newStore()
	for i := 1; i <= 20; i++ {
		s.Add(makeJob(int64(i), model.JobRun, time.Now().Add(time.Minute)))
	}

	var wg sync.WaitGroup
	for i := 1; i <= 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			status := model.JobStop
			if i%2 == 0 {
				status = model.JobRun
			}
			_ = s.UpdateStatus(int64(i), status)
		}(i)
	}
	wg.Wait()
	// 不崩溃即通过
}

// ─── PeekDue ──────────────────────────────────────────────────────────────────

func TestStore_PeekDue_DoesNotRemove(t *testing.T) {
	s := newStore()
	s.Add(makeJob(1, model.JobRun, time.Now().Add(-time.Second)))

	peeked := s.PeekDue(time.Now(), 10)
	assert.Len(t, peeked, 1)
	// PeekDue 之后堆中任务应仍在（重新插入）
	assert.Equal(t, 1, s.HeapLen(), "PeekDue 后任务应重新入堆")
}

func TestStore_PeekDue_vs_PopDue(t *testing.T) {
	s := newStore()
	for i := 1; i <= 5; i++ {
		s.Add(makeJob(int64(i), model.JobRun, time.Now().Add(-time.Second)))
	}

	peeked := s.PeekDue(time.Now(), 100)
	assert.Len(t, peeked, 5)

	popped := s.PopDue(time.Now(), 100)
	assert.Len(t, popped, 5)
	assert.Equal(t, 0, s.HeapLen(), "PopDue 后堆应为空")
}
