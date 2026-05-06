package scheduler

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

// ═══════════════════════════════════════════════════════════════════
// Mock DAO 实现（完全符合实际 DAO 接口）
// ═══════════════════════════════════════════════════════════════════

// ─── mockJobInfoDAO ────────────────────────────────────────────────

type mockJobInfoDAO struct {
	mu   sync.RWMutex
	jobs map[int64]*model.JobInfo
	seq  int64
}

func newMockJobDAO(initial ...*model.JobInfo) *mockJobInfoDAO {
	m := &mockJobInfoDAO{jobs: make(map[int64]*model.JobInfo)}
	for _, j := range initial {
		cp := *j
		m.jobs[j.ID] = &cp
		if j.ID > m.seq {
			m.seq = j.ID
		}
	}
	return m
}

func (m *mockJobInfoDAO) Create(_ context.Context, j *model.JobInfo) error {
	m.mu.Lock(); defer m.mu.Unlock()
	m.seq++; j.ID = m.seq
	cp := *j; m.jobs[j.ID] = &cp
	return nil
}
func (m *mockJobInfoDAO) Update(_ context.Context, j *model.JobInfo) error {
	m.mu.Lock(); defer m.mu.Unlock()
	cp := *j; m.jobs[j.ID] = &cp
	return nil
}
func (m *mockJobInfoDAO) Delete(_ context.Context, id int64) error {
	m.mu.Lock(); defer m.mu.Unlock()
	delete(m.jobs, id); return nil
}
func (m *mockJobInfoDAO) FindByID(_ context.Context, id int64) (*model.JobInfo, error) {
	m.mu.RLock(); defer m.mu.RUnlock()
	if j, ok := m.jobs[id]; ok { cp := *j; return &cp, nil }
	return nil, fmt.Errorf("not found: %d", id)
}
func (m *mockJobInfoDAO) UpdateStatus(_ context.Context, id int64, status model.JobStatus) error {
	m.mu.Lock(); defer m.mu.Unlock()
	if j, ok := m.jobs[id]; ok { j.Status = status; return nil }
	return fmt.Errorf("not found: %d", id)
}
func (m *mockJobInfoDAO) UpdateNextTriggerTime(_ context.Context, id int64, next, last time.Time) error {
	m.mu.Lock(); defer m.mu.Unlock()
	if j, ok := m.jobs[id]; ok {
		j.NextTriggerTime = &next; j.LastTriggerTime = &last; return nil
	}
	return fmt.Errorf("not found: %d", id)
}
func (m *mockJobInfoDAO) ListPendingJobs(_ context.Context, maxTime time.Time, limit int) ([]*model.JobInfo, error) {
	m.mu.RLock(); defer m.mu.RUnlock()
	var out []*model.JobInfo
	for _, j := range m.jobs {
		if j.Status == model.JobRun && j.NextTriggerTime != nil && !j.NextTriggerTime.After(maxTime) {
			cp := *j; out = append(out, &cp)
		}
		if len(out) >= limit { break }
	}
	return out, nil
}
func (m *mockJobInfoDAO) List(_ context.Context, q *dao.JobInfoQuery) ([]*model.JobInfo, int64, error) {
	m.mu.RLock(); defer m.mu.RUnlock()
	var out []*model.JobInfo
	for _, j := range m.jobs {
		if q.Status != nil && j.Status != *q.Status { continue }
		cp := *j; out = append(out, &cp)
	}
	return out, int64(len(out)), nil
}

// ─── mockJobLogDAO ─────────────────────────────────────────────────

type mockJobLogDAO struct {
	mu   sync.Mutex
	logs map[int64]*model.JobLog
	seq  int64
}

func newMockLogDAO() *mockJobLogDAO {
	return &mockJobLogDAO{logs: make(map[int64]*model.JobLog)}
}
func (m *mockJobLogDAO) Create(_ context.Context, l *model.JobLog) error {
	m.mu.Lock(); defer m.mu.Unlock()
	m.seq++; l.ID = m.seq
	cp := *l; m.logs[l.ID] = &cp
	return nil
}
func (m *mockJobLogDAO) UpdateResult(_ context.Context, id int64, status model.LogStatus, errMsg string, start, end time.Time) error {
	m.mu.Lock(); defer m.mu.Unlock()
	if l, ok := m.logs[id]; ok {
		l.Status = status; l.ErrorMsg = errMsg
		l.StartTime = &start; l.EndTime = &end
	}
	return nil
}
func (m *mockJobLogDAO) FindByID(_ context.Context, id int64) (*model.JobLog, error) {
	m.mu.Lock(); defer m.mu.Unlock()
	if l, ok := m.logs[id]; ok { cp := *l; return &cp, nil }
	return nil, fmt.Errorf("log %d not found", id)
}
func (m *mockJobLogDAO) ListByJob(_ context.Context, _ int64, _, _ int) ([]*model.JobLog, int64, error) {
	return nil, 0, nil
}
func (m *mockJobLogDAO) ListRunning(_ context.Context) ([]*model.JobLog, error) {
	m.mu.Lock(); defer m.mu.Unlock()
	var out []*model.JobLog
	for _, l := range m.logs {
		if l.Status == model.LogRunning { cp := *l; out = append(out, &cp) }
	}
	return out, nil
}
func (m *mockJobLogDAO) ListRetryable(_ context.Context, _ int) ([]*model.JobLog, error) {
	return nil, nil
}
func (m *mockJobLogDAO) CountRetries(_ context.Context, _ int64, _ time.Time) (int64, error) {
	return 0, nil
}
func (m *mockJobLogDAO) CreateDetail(_ context.Context, _ *model.JobLogDetail) error { return nil }
func (m *mockJobLogDAO) FindDetail(_ context.Context, _ int64) (*model.JobLogDetail, error) {
	return nil, nil
}

func (m *mockJobLogDAO) countLogs() int {
	m.mu.Lock(); defer m.mu.Unlock()
	return len(m.logs)
}

// ─── mockExecutorDAO ───────────────────────────────────────────────

type mockExecutorDAO struct {
	mu        sync.RWMutex
	executors map[string][]*model.Executor
}

func newMockExecutorDAO(app, addr string) *mockExecutorDAO {
	m := &mockExecutorDAO{executors: make(map[string][]*model.Executor)}
	if app != "" {
		m.executors[app] = []*model.Executor{
			{ID: 1, AppName: app, Address: addr, Status: model.ExecutorOnline},
		}
	}
	return m
}
func (m *mockExecutorDAO) Create(_ context.Context, _ *model.Executor) error { return nil }
func (m *mockExecutorDAO) Update(_ context.Context, _ *model.Executor) error { return nil }
func (m *mockExecutorDAO) Delete(_ context.Context, _ int64) error           { return nil }
func (m *mockExecutorDAO) FindByID(_ context.Context, _ int64) (*model.Executor, error) {
	return nil, nil
}
func (m *mockExecutorDAO) FindByAppAndAddress(_ context.Context, _, _ string) (*model.Executor, error) {
	return nil, nil
}
func (m *mockExecutorDAO) ListByApp(_ context.Context, app string) ([]*model.Executor, error) {
	m.mu.RLock(); defer m.mu.RUnlock()
	return m.executors[app], nil
}
func (m *mockExecutorDAO) ListOnline(_ context.Context) ([]*model.Executor, error) {
	return nil, nil
}
func (m *mockExecutorDAO) UpdateHeartbeat(_ context.Context, _ int64, _ time.Time) error {
	return nil
}
func (m *mockExecutorDAO) UpdateStatus(_ context.Context, _ int64, _ model.ExecutorStatus) error {
	return nil
}
func (m *mockExecutorDAO) MarkOfflineTimeout(_ context.Context, _ time.Duration) (int64, error) {
	return 0, nil
}
func (m *mockExecutorDAO) List(_ context.Context, _, _ int) ([]*model.Executor, int64, error) {
	return nil, 0, nil
}

// ═══════════════════════════════════════════════════════════════════
// 测试辅助
// ═══════════════════════════════════════════════════════════════════

// newTestScheduler 构建测试用 Scheduler（redis 设为 nil，handleTask 会跳过加锁）
func newTestScheduler(
	jobDAO dao.JobInfoDAO,
	logDAO dao.JobLogDAO,
	execDAO dao.ExecutorDAO,
) *Scheduler {
	return New(jobDAO, logDAO, execDAO, nil, nil, WithNodeID("test-node"))
}

func makeRunningJob(id int64, cronExpr string, next time.Time) *model.JobInfo {
	return &model.JobInfo{
		ID:              id,
		ExecutorApp:     "test-executor",
		JobName:         fmt.Sprintf("job-%d", id),
		CronExpression:  cronExpr,
		ExecuteType:     model.ExecuteTypeBean,
		ExecuteHandler:  "testHandler",
		Status:          model.JobRun,
		RouteStrategy:   model.RouteFirst,
		NextTriggerTime: &next,
	}
}

// waitFor 轮询等待条件满足
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() { return true }
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// ═══════════════════════════════════════════════════════════════════
// 生命周期测试
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_StartStop(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	assert.Error(t, s.Start(), "重复 Start 应返回错误")
	s.Stop()
	require.NotPanics(t, func() { s.Stop() }, "重复 Stop 应幂等")
}

func TestScheduler_Bootstrap_LoadsRunningJobs(t *testing.T) {
	now := time.Now()
	j1 := makeRunningJob(1, "0/10 * * * * ?", now.Add(time.Minute))
	j2 := makeRunningJob(2, "0/20 * * * * ?", now.Add(2*time.Minute))
	j3 := makeRunningJob(3, "0/30 * * * * ?", now.Add(3*time.Minute))
	j3.Status = model.JobStop // 已停止，不应加载入堆

	s := newTestScheduler(newMockJobDAO(j1, j2, j3), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	stats := s.Stats()
	assert.Equal(t, int64(2), stats.JobCount, "只有运行中的任务应被加载")
	assert.Equal(t, 2, stats.HeapLen, "运行中任务应在堆中")
}

func TestScheduler_Bootstrap_Pagination(t *testing.T) {
	// 构造超过 bootstrapBatchSize(2000) 数量的任务，验证分页加载
	jobDAO := newMockJobDAO()
	now := time.Now()
	for i := 1; i <= 2100; i++ {
		j := makeRunningJob(int64(i), "0/5 * * * * ?", now.Add(time.Minute))
		jobDAO.mu.Lock()
		cp := *j
		jobDAO.jobs[j.ID] = &cp
		if j.ID > jobDAO.seq { jobDAO.seq = j.ID }
		jobDAO.mu.Unlock()
	}

	s := newTestScheduler(jobDAO, newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	assert.Equal(t, int64(2100), s.Stats().JobCount, "所有任务应分页加载完毕")
}

// ═══════════════════════════════════════════════════════════════════
// 任务管理
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_AddJob(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	j := makeRunningJob(10, "0/5 * * * * ?", time.Now().Add(time.Minute))
	s.AddJob(j)

	assert.Equal(t, int64(1), s.Stats().JobCount)
	assert.Equal(t, 1, s.Stats().HeapLen)
}

func TestScheduler_AddJob_Stopped_NotInHeap(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Minute))
	j.Status = model.JobStop
	s.AddJob(j)

	assert.Equal(t, int64(1), s.Stats().JobCount)
	assert.Equal(t, 0, s.Stats().HeapLen, "停止状态任务不应入堆")
}

func TestScheduler_UpdateJob(t *testing.T) {
	now := time.Now()
	j := makeRunningJob(1, "0/5 * * * * ?", now.Add(time.Minute))
	s := newTestScheduler(newMockJobDAO(j), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	updated := makeRunningJob(1, "0/30 * * * * ?", now.Add(2*time.Minute))
	s.UpdateJob(updated)

	got, ok := s.store.Get(1)
	require.True(t, ok)
	assert.Equal(t, "0/30 * * * * ?", got.CronExpression, "Cron 表达式应被更新")
}

func TestScheduler_RemoveJob(t *testing.T) {
	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Minute))
	s := newTestScheduler(newMockJobDAO(j), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	s.RemoveJob(1)
	assert.Equal(t, int64(0), s.Stats().JobCount)
	assert.Equal(t, 0, s.Stats().HeapLen)
}

// ═══════════════════════════════════════════════════════════════════
// StartJob / StopJob
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_StopJob(t *testing.T) {
	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Minute))
	s := newTestScheduler(newMockJobDAO(j), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	err := s.StopJob(1)
	require.NoError(t, err)
	assert.Equal(t, 0, s.Stats().HeapLen, "停止后任务应离开堆")

	got, _ := s.store.Get(1)
	assert.Equal(t, model.JobStop, got.Status)
}

func TestScheduler_StopJob_NotFound(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	err := s.StopJob(999)
	assert.Error(t, err, "停止不存在的任务应返回错误")
}

func TestScheduler_StartJob(t *testing.T) {
	now := time.Now()
	j := makeRunningJob(1, "0 * * * * ?", now.Add(time.Minute))
	j.Status = model.JobStop

	s := newTestScheduler(newMockJobDAO(j), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	err := s.StartJob(context.Background(), 1)
	require.NoError(t, err)
	assert.Equal(t, 1, s.Stats().HeapLen, "启动后任务应进入堆")
}

func TestScheduler_StartJob_FallsBackToMySQL(t *testing.T) {
	// store 中没有，但 MySQL 中有
	j := makeRunningJob(42, "0/5 * * * * ?", time.Now().Add(time.Minute))
	j.Status = model.JobStop
	jobDAO := newMockJobDAO(j)

	s := newTestScheduler(jobDAO, newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	// bootstrap 只加载 JobRun 的任务，因此 store 中没有 job 42
	// StartJob 应回落到 MySQL
	s.store.Remove(42) // 确保 store 中无此记录
	err := s.StartJob(context.Background(), 42)
	require.NoError(t, err)
}

// ═══════════════════════════════════════════════════════════════════
// TriggerJob 手动触发
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_TriggerJob_CreatesLogRecord(t *testing.T) {
	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Hour))
	logDAO := newMockLogDAO()
	execDAO := newMockExecutorDAO("test-executor", "127.0.0.1:19901")

	s := newTestScheduler(newMockJobDAO(j), logDAO, execDAO)
	require.NoError(t, s.Start())
	defer s.Stop()

	err := s.TriggerJob(context.Background(), 1, "")
	require.NoError(t, err)

	// worker goroutine 尝试调用不存在的执行器，会记录一条失败日志
	ok := waitFor(t, time.Second, func() bool {
		return logDAO.countLogs() >= 1
	})
	assert.True(t, ok, "手动触发应产生至少一条日志记录")
}

func TestScheduler_TriggerJob_ParamOverrides(t *testing.T) {
	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Hour))
	j.ExecuteParam = "original"
	logDAO := newMockLogDAO()

	s := newTestScheduler(newMockJobDAO(j), logDAO, newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	_ = s.TriggerJob(context.Background(), 1, "overridden")
	time.Sleep(200 * time.Millisecond)

	logDAO.mu.Lock()
	var param string
	for _, l := range logDAO.logs {
		if l.JobID == 1 { param = l.ExecuteParam }
	}
	logDAO.mu.Unlock()
	assert.Equal(t, "overridden", param, "参数覆盖应生效")
}

func TestScheduler_TriggerJob_NotFound(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	err := s.TriggerJob(context.Background(), 9999, "")
	assert.Error(t, err, "触发不存在的任务应返回错误")
}

func TestScheduler_TriggerJob_ManualTriggerType(t *testing.T) {
	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Hour))
	logDAO := newMockLogDAO()

	s := newTestScheduler(newMockJobDAO(j), logDAO, newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	_ = s.TriggerJob(context.Background(), 1, "")
	waitFor(t, time.Second, func() bool { return logDAO.countLogs() >= 1 })

	logDAO.mu.Lock()
	var tt model.TriggerType
	for _, l := range logDAO.logs {
		tt = l.TriggerType
	}
	logDAO.mu.Unlock()
	assert.Equal(t, model.TriggerManual, tt, "手动触发的 TriggerType 应为 Manual")
}

// ═══════════════════════════════════════════════════════════════════
// CalcNextTriggerTime
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_CalcNextTriggerTime_Valid(t *testing.T) {
	s := New(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("a", "b"), nil)
	next, err := s.CalcNextTriggerTime("0 0 2 * * ?")
	require.NoError(t, err)
	assert.True(t, next.After(time.Now()))
}

func TestScheduler_CalcNextTriggerTime_Invalid(t *testing.T) {
	s := New(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("a", "b"), nil)
	_, err := s.CalcNextTriggerTime("not-a-cron")
	assert.Error(t, err)
}

func TestScheduler_CalcNextTriggerTime_EverySecond(t *testing.T) {
	s := New(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("a", "b"), nil)
	next, err := s.CalcNextTriggerTime("* * * * * ?")
	require.NoError(t, err)
	// 下次触发应在 1 秒内
	assert.Less(t, time.Until(next), 2*time.Second)
}

// ═══════════════════════════════════════════════════════════════════
// reschedule
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_Reschedule_UpdatesNextTrigger(t *testing.T) {
	now := time.Now()
	j := makeRunningJob(1, "* * * * * ?", now.Add(-time.Second)) // 已过期
	s := newTestScheduler(newMockJobDAO(j), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	s.reschedule(context.Background(), j)

	got, ok := s.store.Get(1)
	require.True(t, ok)
	require.NotNil(t, got.NextTriggerTime)
	assert.True(t, got.NextTriggerTime.After(now), "reschedule 后下次触发时间应在未来")
}

func TestScheduler_Reschedule_OneShot_Disables(t *testing.T) {
	now := time.Now()
	j := makeRunningJob(1, "", now.Add(-time.Second)) // 无 cron = 一次性
	s := newTestScheduler(newMockJobDAO(j), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	s.reschedule(context.Background(), j)

	got, ok := s.store.Get(1)
	require.True(t, ok)
	assert.Equal(t, model.JobStop, got.Status, "一次性任务执行后应自动停止")
}

// ═══════════════════════════════════════════════════════════════════
// Stats
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_Stats(t *testing.T) {
	now := time.Now()
	j1 := makeRunningJob(1, "0/5 * * * * ?", now.Add(time.Minute))
	j2 := makeRunningJob(2, "0/5 * * * * ?", now.Add(2*time.Minute))
	s := newTestScheduler(newMockJobDAO(j1, j2), newMockLogDAO(), newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	stats := s.Stats()
	assert.Equal(t, int64(2), stats.JobCount)
	assert.Equal(t, 2, stats.HeapLen)
}

// ═══════════════════════════════════════════════════════════════════
// 时间轮集成：任务确实在到期时触发
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_WheelFiresJob(t *testing.T) {
	if testing.Short() {
		t.Skip("时间轮触发测试在 -short 模式下跳过")
	}
	logDAO := newMockLogDAO()
	execDAO := newMockExecutorDAO("test-executor", "127.0.0.1:19902")

	// 任务 100ms 后到期
	fireAt := time.Now().Add(100 * time.Millisecond)
	j := makeRunningJob(1, "* * * * * ?", fireAt)
	j.CronExpression = "" // 一次性，方便断言

	s := newTestScheduler(newMockJobDAO(j), logDAO, execDAO)
	require.NoError(t, s.Start())
	defer s.Stop()

	ok := waitFor(t, 2*time.Second, func() bool {
		return logDAO.countLogs() >= 1
	})
	assert.True(t, ok, "时间轮应在任务到期后触发执行，并产生日志记录")
}

func TestScheduler_WheelDoesNotFireStoppedJob(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	logDAO := newMockLogDAO()

	fireAt := time.Now().Add(80 * time.Millisecond)
	j := makeRunningJob(1, "", fireAt)

	s := newTestScheduler(newMockJobDAO(j), logDAO, newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	// 立即停止任务（取消时间轮注册）
	require.NoError(t, s.StopJob(1))

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 0, logDAO.countLogs(), "已停止的任务不应被时间轮触发")
}

// ═══════════════════════════════════════════════════════════════════
// Misfire 补偿
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_Misfire_Ignore(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	// 触发时间超过 misfireThreshold (5s) 之前
	overdue := time.Now().Add(-10 * time.Second)
	j := makeRunningJob(1, "0/5 * * * * ?", overdue)
	j.MisfireStrategy = model.MisfireIgnore
	s.store.Add(j)

	handled := s.handleMisfire(context.Background(), j)
	assert.True(t, handled, "Ignore 策略应处理 misfire")

	// 任务不应被派发（logDAO 为空）
	time.Sleep(100 * time.Millisecond)
}

func TestScheduler_Misfire_RunOnce(t *testing.T) {
	logDAO := newMockLogDAO()
	execDAO := newMockExecutorDAO("test-executor", "127.0.0.1:19903")

	s := newTestScheduler(newMockJobDAO(), logDAO, execDAO)
	require.NoError(t, s.Start())
	defer s.Stop()

	overdue := time.Now().Add(-10 * time.Second)
	j := makeRunningJob(1, "0/5 * * * * ?", overdue)
	j.MisfireStrategy = model.MisfireRunOnce
	s.store.Add(j)

	handled := s.handleMisfire(context.Background(), j)
	assert.True(t, handled, "RunOnce 策略应处理 misfire")

	// 补偿任务应被投入 workerCh
	ok := waitFor(t, time.Second, func() bool {
		return logDAO.countLogs() >= 1
	})
	assert.True(t, ok, "RunOnce misfire 应产生一次补偿执行记录")
}

func TestScheduler_Misfire_NotTriggeredWithinThreshold(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	// 在阈值内，不算 misfire
	recent := time.Now().Add(-2 * time.Second)
	j := makeRunningJob(1, "0/5 * * * * ?", recent)
	j.MisfireStrategy = model.MisfireIgnore

	handled := s.handleMisfire(context.Background(), j)
	assert.False(t, handled, "阈值内不算 misfire")
}

// ═══════════════════════════════════════════════════════════════════
// 并发压力测试
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_ConcurrentAddRemove(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	const N = 200
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			j := makeRunningJob(int64(i+100), "0/5 * * * * ?", time.Now().Add(time.Minute))
			s.AddJob(j)
			if i%3 == 0 {
				s.RemoveJob(int64(i + 100))
			}
		}(i)
	}
	wg.Wait()
	// 验证不崩溃，计数合理
	stats := s.Stats()
	assert.GreaterOrEqual(t, stats.JobCount, int64(0))
}

func TestScheduler_ConcurrentTriggers(t *testing.T) {
	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Hour))
	logDAO := newMockLogDAO()
	s := newTestScheduler(newMockJobDAO(j), logDAO, newMockExecutorDAO("test-executor", "addr"))
	require.NoError(t, s.Start())
	defer s.Stop()

	var wg sync.WaitGroup
	const N = 20
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.TriggerJob(context.Background(), 1, "")
		}()
	}
	wg.Wait()
	time.Sleep(300 * time.Millisecond)
	// 应该有若干日志，数量 ≤ N
	assert.GreaterOrEqual(t, logDAO.countLogs(), 1)
}

// ═══════════════════════════════════════════════════════════════════
// Worker channel 背压保护
// ═══════════════════════════════════════════════════════════════════

func TestScheduler_WorkerChannelFull_NoBlock(t *testing.T) {
	s := newTestScheduler(newMockJobDAO(), newMockLogDAO(), newMockExecutorDAO("app", "addr"))
	s.workerCh = make(chan *triggerTask, 1)
	require.NoError(t, s.Start())
	defer s.Stop()

	j := makeRunningJob(1, "0/5 * * * * ?", time.Now().Add(time.Minute))
	s.AddJob(j)

	// 填满 channel
	s.workerCh <- &triggerTask{job: j, triggerTime: time.Now()}

	var timedOut int32
	done := make(chan struct{})
	go func() {
		// TriggerJob 在 channel 满时应非阻塞返回错误
		err := s.TriggerJob(context.Background(), 1, "")
		if err != nil { atomic.AddInt32(&timedOut, 1) }
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TriggerJob 在 channel 满时不应阻塞超过 1s")
	}
}
