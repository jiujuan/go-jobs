package logbuffer

// logbuffer_test.go
//
// 覆盖 Buffer 的全部核心行为：
//
// 基础功能
//   1.  Create() 回填真实 ID（非负数）
//   2.  Create() 在 batch 满时立即 flush（不等 ticker）
//   3.  Create() 在 ticker 到期时 flush（batch 未满）
//   4.  Stop() 等待 flush goroutine 退出，in-flight 记录不丢失
//   5.  Stop() 后 Create() 走同步写（fallback to inner.Create）
//
// ID 映射
//   6.  UpdateResult 用 tmpID 正确路由到 realID
//   7.  FindByID 用 tmpID 正确路由到 realID
//   8.  ID 已解析后 idMap 中的 tmpID 条目被 GC
//
// 并发安全
//   9.  N 个 goroutine 并发 Create()，所有 ID 唯一且正数
//  10.  并发 Create() + UpdateResult() 无竞态（-race 验证）
//
// BatchCreator 优化路径
//  11.  inner 实现 BatchCreator 时走 CreateBatch
//  12.  inner 不实现 BatchCreator 时走逐条 fallback
//
// 背压 / 满缓冲
//  13.  ring 满时 Create() 不永久阻塞（ctx 取消后返回 error）
//
// 转发方法
//  14.  ListByJob / ListRunning / CreateDetail / FindDetail 直接转发
//
// Stats
//  15.  Stats().Pending 反映 ring 中积压的条目数
//
// Options
//  16.  WithBatchSize / WithFlushInterval / WithRingCap 生效

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

// spyJobLogDAO 是一个线程安全的 JobLogDAO mock，记录所有调用。
type spyJobLogDAO struct {
	mu           sync.Mutex
	logs         map[int64]*model.JobLog
	seq          int64
	createCalls  int
	updateCalls  int
	createBatchN int // 若干条的 batch 写入总次数
}

func newSpyDAO() *spyJobLogDAO {
	return &spyJobLogDAO{logs: make(map[int64]*model.JobLog)}
}

func (s *spyJobLogDAO) Create(_ context.Context, l *model.JobLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	l.ID = s.seq
	cp := *l
	s.logs[l.ID] = &cp
	s.createCalls++
	return nil
}

func (s *spyJobLogDAO) CreateBatch(_ context.Context, logs []*model.JobLog) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.createBatchN++
	for _, l := range logs {
		s.seq++
		l.ID = s.seq
		cp := *l
		s.logs[l.ID] = &cp
	}
	return nil
}

func (s *spyJobLogDAO) UpdateResult(_ context.Context, id int64, status model.LogStatus, _ string, _, _ time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updateCalls++
	if l, ok := s.logs[id]; ok {
		l.Status = status
	}
	return nil
}

func (s *spyJobLogDAO) FindByID(_ context.Context, id int64) (*model.JobLog, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, ok := s.logs[id]; ok {
		cp := *l
		return &cp, nil
	}
	return nil, fmt.Errorf("not found: %d", id)
}

func (s *spyJobLogDAO) ListByJob(_ context.Context, _ int64, _, _ int) ([]*model.JobLog, int64, error) {
	return nil, 0, nil
}
func (s *spyJobLogDAO) ListRunning(_ context.Context) ([]*model.JobLog, error) { return nil, nil }
func (s *spyJobLogDAO) ListRetryable(_ context.Context, _ int) ([]*model.JobLog, error) {
	return nil, nil
}
func (s *spyJobLogDAO) CountRetries(_ context.Context, _ int64, _ time.Time) (int64, error) {
	return 0, nil
}
func (s *spyJobLogDAO) CreateDetail(_ context.Context, _ *model.JobLogDetail) error { return nil }
func (s *spyJobLogDAO) FindDetail(_ context.Context, _ int64) (*model.JobLogDetail, error) {
	return nil, nil
}

// countLogs returns the number of stored log entries.
func (s *spyJobLogDAO) countLogs() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.logs)
}

// spyJobLogDAONoBatch wraps spyJobLogDAO but does NOT expose CreateBatch.
// It is used to verify the sequential-insert fallback path in writeBatch.
// We intentionally do NOT embed spyJobLogDAO so that the BatchCreator interface
// is never satisfied — only the JobLogDAO methods are delegated.
type spyJobLogDAONoBatch struct {
	inner *spyJobLogDAO
}

func (s *spyJobLogDAONoBatch) Create(ctx context.Context, l *model.JobLog) error {
	return s.inner.Create(ctx, l)
}
func (s *spyJobLogDAONoBatch) UpdateResult(ctx context.Context, id int64, status model.LogStatus, em string, st, en time.Time) error {
	return s.inner.UpdateResult(ctx, id, status, em, st, en)
}
func (s *spyJobLogDAONoBatch) FindByID(ctx context.Context, id int64) (*model.JobLog, error) {
	return s.inner.FindByID(ctx, id)
}
func (s *spyJobLogDAONoBatch) ListByJob(ctx context.Context, jobID int64, page, pageSize int) ([]*model.JobLog, int64, error) {
	return s.inner.ListByJob(ctx, jobID, page, pageSize)
}
func (s *spyJobLogDAONoBatch) ListRunning(ctx context.Context) ([]*model.JobLog, error) {
	return s.inner.ListRunning(ctx)
}
func (s *spyJobLogDAONoBatch) ListRetryable(ctx context.Context, limit int) ([]*model.JobLog, error) {
	return s.inner.ListRetryable(ctx, limit)
}
func (s *spyJobLogDAONoBatch) CountRetries(ctx context.Context, jobID int64, t time.Time) (int64, error) {
	return s.inner.CountRetries(ctx, jobID, t)
}
func (s *spyJobLogDAONoBatch) CreateDetail(ctx context.Context, d *model.JobLogDetail) error {
	return s.inner.CreateDetail(ctx, d)
}
func (s *spyJobLogDAONoBatch) FindDetail(ctx context.Context, logID int64) (*model.JobLogDetail, error) {
	return s.inner.FindDetail(ctx, logID)
}

// makeLog returns a minimal JobLog for testing.
func makeLog(jobID int64) *model.JobLog {
	return &model.JobLog{
		JobID:       jobID,
		Status:      model.LogRunning,
		TriggerTime: time.Now(),
		TriggerType: model.TriggerCron,
	}
}

// newFastBuffer creates a Buffer with short ticker for tests.
func newFastBuffer(inner dao.JobLogDAO, batchSize int) *Buffer {
	return New(inner,
		WithBatchSize(batchSize),
		WithFlushInterval(50*time.Millisecond),
		WithRingCap(batchSize*4),
	)
}

// ─── 1. Create() 回填真实 ID ──────────────────────────────────────────────────

func TestCreate_BackfillsRealID(t *testing.T) {
	spy := newSpyDAO()
	b := newFastBuffer(spy, 100)
	b.Start()
	defer b.Stop()

	l := makeLog(1)
	require.NoError(t, b.Create(context.Background(), l))
	assert.Greater(t, l.ID, int64(0), "Create() 后 l.ID 应为正数（真实 DB ID）")
}

func TestCreate_IDIsUniquePerCall(t *testing.T) {
	spy := newSpyDAO()
	b := newFastBuffer(spy, 100)
	b.Start()
	defer b.Stop()

	l1, l2 := makeLog(1), makeLog(2)
	require.NoError(t, b.Create(context.Background(), l1))
	require.NoError(t, b.Create(context.Background(), l2))
	assert.NotEqual(t, l1.ID, l2.ID, "两次 Create() 的 ID 应不同")
}

// ─── 2. 批量满时立即 flush ────────────────────────────────────────────────────

func TestCreate_FlushesOnBatchFull(t *testing.T) {
	const batchSize = 5
	spy := newSpyDAO()
	// 设置很长的 ticker 确保只有 batch 满才触发 flush
	b := New(spy,
		WithBatchSize(batchSize),
		WithFlushInterval(10*time.Second),
		WithRingCap(batchSize*4),
	)
	b.Start()
	defer b.Stop()

	var wg sync.WaitGroup
	for i := 0; i < batchSize; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, b.Create(context.Background(), makeLog(int64(i+1))))
		}(i)
	}
	wg.Wait()

	// 全部 Create 已返回 → 数据一定在 DB 中
	assert.Equal(t, batchSize, spy.countLogs(), "batch 满时应批量写入 DB")
}

// ─── 3. ticker 到期时 flush ───────────────────────────────────────────────────

func TestCreate_FlushesOnTickerExpiry(t *testing.T) {
	spy := newSpyDAO()
	b := New(spy,
		WithBatchSize(1000),       // 不会被 batch 大小触发
		WithFlushInterval(30*time.Millisecond),
		WithRingCap(4000),
	)
	b.Start()
	defer b.Stop()

	l := makeLog(1)
	require.NoError(t, b.Create(context.Background(), l))
	assert.Greater(t, l.ID, int64(0))
	assert.Equal(t, 1, spy.countLogs())
}

// ─── 4. Stop() 不丢数据 ───────────────────────────────────────────────────────

func TestStop_DrainsPendingRecords(t *testing.T) {
	spy := newSpyDAO()
	b := New(spy,
		WithBatchSize(1000),
		WithFlushInterval(10*time.Second), // ticker 不会触发
		WithRingCap(4000),
	)
	b.Start()

	// 写 10 条（batch 不满，ticker 也不会到期）
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, b.Create(context.Background(), makeLog(int64(i+1))))
		}(i)
	}
	wg.Wait()

	b.Stop() // Stop 应 drain 所有记录
	assert.Equal(t, 10, spy.countLogs(), "Stop() 后 ring 中所有记录应已写入 DB")
}

// ─── 5. Stop() 后 Create() 走 fallback ────────────────────────────────────────

func TestCreate_AfterStop_FallbackToInner(t *testing.T) {
	spy := newSpyDAO()
	b := newFastBuffer(spy, 100)
	b.Start()
	b.Stop() // 立即停止

	l := makeLog(1)
	err := b.Create(context.Background(), l)
	// Stop 后走 inner.Create，不应报错
	require.NoError(t, err)
	assert.Greater(t, l.ID, int64(0))
}

// ─── 6. UpdateResult 透明路由 tmpID→realID ────────────────────────────────────

func TestUpdateResult_ResolvesTemporaryID(t *testing.T) {
	spy := newSpyDAO()
	b := newFastBuffer(spy, 100)
	b.Start()
	defer b.Stop()

	l := makeLog(1)
	require.NoError(t, b.Create(context.Background(), l))
	realID := l.ID
	require.Greater(t, realID, int64(0))

	// UpdateResult 应透明路由到 realID
	err := b.UpdateResult(context.Background(), realID,
		model.LogSuccess, "", time.Now(), time.Now())
	require.NoError(t, err)
	assert.Equal(t, 1, spy.updateCalls, "UpdateResult 应被调用一次")
}

// ─── 7. FindByID 透明路由 ─────────────────────────────────────────────────────

func TestFindByID_ResolvesID(t *testing.T) {
	spy := newSpyDAO()
	b := newFastBuffer(spy, 100)
	b.Start()
	defer b.Stop()

	l := makeLog(2)
	require.NoError(t, b.Create(context.Background(), l))

	found, err := b.FindByID(context.Background(), l.ID)
	require.NoError(t, err)
	assert.Equal(t, int64(2), found.JobID)
}

// ─── 8. GC：UpdateResult 后清理 idMap ────────────────────────────────────────

func TestUpdateResult_GCsIDMapEntry(t *testing.T) {
	spy := newSpyDAO()
	b := newFastBuffer(spy, 100)
	b.Start()
	defer b.Stop()

	l := makeLog(1)
	require.NoError(t, b.Create(context.Background(), l))

	// 初始 idMap 条目存在（如果是 tmpID）
	_ = b.UpdateResult(context.Background(), l.ID, model.LogSuccess, "", time.Now(), time.Now())

	// idMap 应该不再无限增长
	stats := b.Stats()
	assert.Equal(t, 0, stats.IDMapSize, "UpdateResult 后 tmpID→realID 映射应被清理")
}

// ─── 9. 并发 Create()，ID 全部唯一且正数 ─────────────────────────────────────

func TestCreate_Concurrent_UniquePositiveIDs(t *testing.T) {
	const N = 200
	spy := newSpyDAO()
	b := New(spy,
		WithBatchSize(50),
		WithFlushInterval(20*time.Millisecond),
		WithRingCap(N*2),
	)
	b.Start()
	defer b.Stop()

	logs := make([]*model.JobLog, N)
	for i := range logs {
		logs[i] = makeLog(int64(i + 1))
	}

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, b.Create(context.Background(), logs[i]))
		}(i)
	}
	wg.Wait()

	// 检查所有 ID 为正数
	for i, l := range logs {
		assert.Greater(t, l.ID, int64(0), "log[%d].ID 应为正数", i)
	}

	// 检查所有 ID 唯一
	seen := make(map[int64]bool, N)
	for _, l := range logs {
		assert.False(t, seen[l.ID], "ID %d 重复", l.ID)
		seen[l.ID] = true
	}

	assert.Equal(t, N, spy.countLogs(), "应有 %d 条日志写入 DB", N)
}

// ─── 10. 并发 Create() + UpdateResult() 无竞态 ────────────────────────────────

func TestCreate_Concurrent_WithUpdateResult_NoRace(t *testing.T) {
	const N = 50
	spy := newSpyDAO()
	b := New(spy,
		WithBatchSize(10),
		WithFlushInterval(20*time.Millisecond),
		WithRingCap(N*2),
	)
	b.Start()
	defer b.Stop()

	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := makeLog(int64(i + 1))
			if err := b.Create(context.Background(), l); err != nil {
				return
			}
			_ = b.UpdateResult(context.Background(), l.ID,
				model.LogSuccess, "", time.Now(), time.Now())
		}(i)
	}
	wg.Wait()
	// 无 panic / 无竞态（-race 检测）即为通过
}

// ─── 11. inner 实现 BatchCreator 时走 CreateBatch ────────────────────────────

func TestWriteBatch_UsesBatchCreatorWhenAvailable(t *testing.T) {
	spy := newSpyDAO()
	b := New(spy,
		WithBatchSize(3),
		WithFlushInterval(10*time.Second),
		WithRingCap(12),
	)
	b.Start()
	defer b.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, b.Create(context.Background(), makeLog(int64(i+1))))
		}(i)
	}
	wg.Wait()

	spy.mu.Lock()
	batchN := spy.createBatchN
	singleN := spy.createCalls
	spy.mu.Unlock()

	assert.Greater(t, batchN, 0, "应调用 CreateBatch")
	assert.Equal(t, 0, singleN, "实现了 BatchCreator 时不应调用单条 Create")
}

// ─── 12. 无 BatchCreator 时走 fallback ────────────────────────────────────────

func TestWriteBatch_FallsBackToSequentialInserts(t *testing.T) {
	// spyJobLogDAONoBatch 不嵌入 spyJobLogDAO，因此不暴露 CreateBatch 方法，
	// writeBatch 的类型断言 b.inner.(BatchCreator) 会返回 false，走 fallback。
	inner := &spyJobLogDAONoBatch{inner: newSpyDAO()}

	b := New(inner,
		WithBatchSize(3),
		WithFlushInterval(10*time.Second),
		WithRingCap(12),
	)
	b.Start()
	defer b.Stop()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			require.NoError(t, b.Create(context.Background(), makeLog(int64(i+1))))
		}(i)
	}
	wg.Wait()

	// spyJobLogDAONoBatch.Create 委托给 inner.Create，验证走了单条 fallback
	inner.inner.mu.Lock()
	singleN := inner.inner.createCalls
	inner.inner.mu.Unlock()

	assert.Equal(t, 3, singleN, "无 BatchCreator 时应走逐条 Create fallback")
	assert.Equal(t, 0, inner.inner.createBatchN, "不应调用 CreateBatch")
}

// ─── 13. ring 满时 ctx 取消不阻塞 ────────────────────────────────────────────

func TestCreate_RingFull_CtxCancelReturnsError(t *testing.T) {
	spy := newSpyDAO()
	// ring cap=1，batch size=10000（不会被 batch 大小触发），flush interval=1h
	b := New(spy,
		WithBatchSize(10000),
		WithFlushInterval(time.Hour),
		WithRingCap(1),
	)
	b.Start()
	defer b.Stop()

	// 占满 ring（直接放入一个 entry，不让 flush goroutine 消费）
	blocker := &pendingEntry{log: makeLog(999), done: make(chan error, 1)}
	b.ring <- blocker

	// 第二次 Create 应在 ctx 取消后立即返回错误
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	l := makeLog(2)
	err := b.Create(ctx, l)
	assert.Error(t, err, "ring 满且 ctx 取消时应返回 error")
	assert.Contains(t, err.Error(), "cancelled")

	// 解除 blocker，让 Stop 能正常退出
	blocker.done <- nil
	close(blocker.done)
}

// ─── 14. 转发方法 ─────────────────────────────────────────────────────────────

func TestForwardMethods_DelegateToInner(t *testing.T) {
	spy := newSpyDAO()
	b := newFastBuffer(spy, 100)
	b.Start()
	defer b.Stop()

	ctx := context.Background()

	_, _, err := b.ListByJob(ctx, 1, 1, 10)
	assert.NoError(t, err)

	_, err = b.ListRunning(ctx)
	assert.NoError(t, err)

	err = b.CreateDetail(ctx, &model.JobLogDetail{LogID: 1, JobID: 1, LogContent: "ok"})
	assert.NoError(t, err)

	_, err = b.FindDetail(ctx, 1)
	assert.NoError(t, err)
}

// ─── 15. Stats().Pending 反映积压 ────────────────────────────────────────────

func TestStats_PendingReflectsRingLen(t *testing.T) {
	spy := newSpyDAO()
	// 超大 batch + 超长 interval，确保 ring 积压
	b := New(spy,
		WithBatchSize(10000),
		WithFlushInterval(time.Hour),
		WithRingCap(10000),
	)
	// 不 Start，直接往 ring 里放
	entry := &pendingEntry{log: makeLog(1), done: make(chan error, 1)}
	b.ring <- entry

	stats := b.Stats()
	assert.Equal(t, 1, stats.Pending)
	assert.Equal(t, 10000, stats.RingCap)

	entry.done <- nil
	close(entry.done)
}

// ─── 16. Options ─────────────────────────────────────────────────────────────

func TestOptions_AllApplied(t *testing.T) {
	spy := newSpyDAO()
	b := New(spy,
		WithBatchSize(250),
		WithFlushInterval(500*time.Millisecond),
		WithRingCap(2000),
	)
	assert.Equal(t, 250, b.batchSize)
	assert.Equal(t, 500*time.Millisecond, b.flushInterval)
	assert.Equal(t, 2000, b.ringCap)
	assert.Equal(t, 2000, cap(b.ring))
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkCreate_Sequential(b *testing.B) {
	spy := newSpyDAO()
	buf := New(spy,
		WithBatchSize(500),
		WithFlushInterval(50*time.Millisecond),
		WithRingCap(2000),
	)
	buf.Start()
	defer buf.Stop()

	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buf.Create(ctx, makeLog(int64(i+1)))
	}
}

func BenchmarkCreate_Parallel(b *testing.B) {
	spy := newSpyDAO()
	buf := New(spy,
		WithBatchSize(500),
		WithFlushInterval(50*time.Millisecond),
		WithRingCap(4000),
	)
	buf.Start()
	defer buf.Stop()

	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var i int64
		for pb.Next() {
			_ = buf.Create(ctx, makeLog(atomic.AddInt64(&i, 1)))
		}
	})
}

func BenchmarkCreate_DirectDAO(b *testing.B) {
	// 对比基准：直接调用 inner.Create（模拟原始行为）
	spy := newSpyDAO()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = spy.Create(ctx, makeLog(int64(i+1)))
	}
}
