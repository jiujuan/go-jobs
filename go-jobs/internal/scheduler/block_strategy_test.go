package scheduler

// block_strategy_test.go
//
// 覆盖 block_strategy.go 的全部逻辑：
//
// applyBlockStrategy
//   BlockSerial（default）
//     1.  没有同名运行中任务 → 返回 true（直接放行）
//     2.  有同名运行中任务   → 返回 true（串行，不干涉）
//
//   BlockDiscard
//     3.  logDAO.ListRunning 无同名任务 → 返回 true（允许触发）
//     4.  logDAO.ListRunning 有同名任务 → 返回 false（丢弃）
//     5.  logDAO.ListRunning 有其他任务（不同 jobID）→ 返回 true
//     6.  logDAO.ListRunning 返回 error → 返回 true（保守放行）
//     7.  有多条运行日志，第一条命中 jobID → 返回 false
//
//   BlockOverride
//     8.  logDAO.ListRunning 无同名任务 → 返回 true，不发 Kill
//     9.  logDAO.ListRunning 有同名任务且 ExecutorAddress 非空
//         → 向 executor 发 Kill 请求，返回 true
//    10.  logDAO.ListRunning 有同名任务但 ExecutorAddress 为空
//         → 不发 Kill，返回 true
//    11.  logDAO.ListRunning 返回 error → 返回 true（保守放行）
//    12.  Kill 请求失败（executor 不可达）→ 仍返回 true
//
// BlockStrategyKey
//    13.  BlockSerial  → "SERIAL"
//    14.  BlockDiscard → "DISCARD"
//    15.  BlockOverride → "OVERRIDE"
//    16.  未知值       → "SERIAL"（default 分支）

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

// bsScheduler 构建一个仅含 logDAO 的最小 Scheduler，用于 block strategy 测试。
// redis=nil 跳过分布式锁，executorStore=nil 跳过内存注册表查询。
// 接受 dao.JobLogDAO 接口，兼容 *mockJobLogDAO 和 *errJobLogDAO。
func bsScheduler(logDAO dao.JobLogDAO) *Scheduler {
	return New(
		newMockJobDAO(),
		logDAO,
		newMockExecutorDAO("", ""),
		nil, // redis nil → 跳过分布式锁
		nil, // executorStore nil
		WithNodeID("bs-test-node"),
	)
}

// makeLog 创建一条状态为 LogRunning 的运行日志。
func makeLog(id, jobID int64, executorAddr string) *model.JobLog {
	return &model.JobLog{
		ID:              id,
		JobID:           jobID,
		ExecutorAddress: executorAddr,
		Status:          model.LogRunning,
		TriggerTime:     time.Now(),
	}
}

// injectLog 直接向 mockJobLogDAO 插入一条日志（绕过 Create 的自增 ID）。
func injectLog(d *mockJobLogDAO, l *model.JobLog) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.logs[l.ID] = l
}

// killServer 启动一个记录 Kill 请求次数的 httptest.Server，
// 返回 (server, hitCount指针)，调用方负责 defer server.Close()。
func killServer(t *testing.T, statusCode int) (*httptest.Server, *int64) {
	t.Helper()
	var hits int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		io.ReadAll(r.Body) //nolint:errcheck
		w.WriteHeader(statusCode)
	}))
	return srv, &hits
}

// makeJobWithStrategy 构建带有指定阻塞策略的 JobInfo。
func makeJobWithStrategy(id int64, bs model.BlockStrategy) *model.JobInfo {
	return &model.JobInfo{
		ID:            id,
		ExecutorApp:   "test-app",
		JobName:       "test-job",
		BlockStrategy: bs,
		Status:        model.JobRun,
	}
}

// ─── 1-2. BlockSerial（default 分支） ────────────────────────────────────────

func TestApplyBlockStrategy_Serial_NoRunningJob_ReturnsTrue(t *testing.T) {
	logDAO := newMockLogDAO()
	s := bsScheduler(logDAO)

	job := makeJobWithStrategy(1, model.BlockSerial)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.True(t, result, "BlockSerial：无运行中任务应返回 true")
}

func TestApplyBlockStrategy_Serial_WithRunningJob_ReturnsTrue(t *testing.T) {
	logDAO := newMockLogDAO()
	injectLog(logDAO, makeLog(100, 1, "10.0.0.1:9000"))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(1, model.BlockSerial)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.True(t, result, "BlockSerial：有运行中任务仍应返回 true（串行排队）")
}

// ─── 3-7. BlockDiscard ───────────────────────────────────────────────────────

func TestApplyBlockStrategy_Discard_NoRunningJob_ReturnsTrue(t *testing.T) {
	logDAO := newMockLogDAO()
	s := bsScheduler(logDAO)

	job := makeJobWithStrategy(1, model.BlockDiscard)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.True(t, result, "BlockDiscard：无运行中任务应允许触发")
}

func TestApplyBlockStrategy_Discard_SameJobRunning_ReturnsFalse(t *testing.T) {
	logDAO := newMockLogDAO()
	injectLog(logDAO, makeLog(101, 42, "10.0.0.1:9000"))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(42, model.BlockDiscard)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.False(t, result, "BlockDiscard：同名任务运行中应丢弃本次触发")
}

func TestApplyBlockStrategy_Discard_DifferentJobRunning_ReturnsTrue(t *testing.T) {
	logDAO := newMockLogDAO()
	injectLog(logDAO, makeLog(102, 99, "10.0.0.1:9000")) // jobID=99，不同于目标 jobID=7

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(7, model.BlockDiscard)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.True(t, result, "BlockDiscard：不同 jobID 运行中不影响本任务触发")
}

func TestApplyBlockStrategy_Discard_ListRunningError_ReturnsTrue(t *testing.T) {
	// 使用会返回错误的 logDAO
	logDAO := &errJobLogDAO{}
	s := bsScheduler(logDAO)

	job := makeJobWithStrategy(1, model.BlockDiscard)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.True(t, result, "BlockDiscard：ListRunning 报错应保守放行（返回 true）")
}

func TestApplyBlockStrategy_Discard_MultipleLogsFirstHit_ReturnsFalse(t *testing.T) {
	logDAO := newMockLogDAO()
	// 多条日志，第一条命中目标 jobID
	injectLog(logDAO, makeLog(201, 5, "addr1"))
	injectLog(logDAO, makeLog(202, 9, "addr2"))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(5, model.BlockDiscard)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.False(t, result, "BlockDiscard：多条日志中命中同名任务应丢弃")
}

// ─── 8-12. BlockOverride ─────────────────────────────────────────────────────

func TestApplyBlockStrategy_Override_NoRunningJob_ReturnsTrue(t *testing.T) {
	logDAO := newMockLogDAO()
	s := bsScheduler(logDAO)

	job := makeJobWithStrategy(1, model.BlockOverride)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.True(t, result, "BlockOverride：无运行中任务应返回 true")
}

func TestApplyBlockStrategy_Override_SameJobRunning_SendsKillAndReturnsTrue(t *testing.T) {
	srv, hits := killServer(t, http.StatusOK)
	defer srv.Close()

	// srv.Listener.Addr().String() → "127.0.0.1:PORT"
	executorAddr := srv.Listener.Addr().String()

	logDAO := newMockLogDAO()
	// 注入同名任务的运行日志，携带真实 executor 地址
	injectLog(logDAO, makeLog(301, 10, executorAddr))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(10, model.BlockOverride)
	result := s.applyBlockStrategy(context.Background(), job)

	assert.True(t, result, "BlockOverride：有运行中任务仍应返回 true（允许新触发）")

	// 等待异步 Kill 请求到达（applyBlockStrategy 内部同步发送，无需等待）
	assert.Equal(t, int64(1), atomic.LoadInt64(hits), "BlockOverride 应向 executor 发送 Kill 请求")
}

func TestApplyBlockStrategy_Override_EmptyExecutorAddress_NoKill(t *testing.T) {
	logDAO := newMockLogDAO()
	// ExecutorAddress 为空：不应发 Kill
	injectLog(logDAO, makeLog(302, 20, "")) // address=""

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(20, model.BlockOverride)
	result := s.applyBlockStrategy(context.Background(), job)

	assert.True(t, result, "BlockOverride：ExecutorAddress 为空时应跳过 Kill，返回 true")
}

func TestApplyBlockStrategy_Override_ListRunningError_ReturnsTrue(t *testing.T) {
	logDAO := &errJobLogDAO{}
	s := bsScheduler(logDAO)

	job := makeJobWithStrategy(1, model.BlockOverride)
	result := s.applyBlockStrategy(context.Background(), job)
	assert.True(t, result, "BlockOverride：ListRunning 报错应保守放行（返回 true）")
}

func TestApplyBlockStrategy_Override_KillFails_StillReturnsTrue(t *testing.T) {
	srv, _ := killServer(t, http.StatusInternalServerError) // executor 返回 500
	defer srv.Close()

	executorAddr := srv.Listener.Addr().String()
	logDAO := newMockLogDAO()
	injectLog(logDAO, makeLog(303, 30, executorAddr))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(30, model.BlockOverride)
	result := s.applyBlockStrategy(context.Background(), job)

	assert.True(t, result, "BlockOverride：Kill 失败后仍应返回 true")
}

func TestApplyBlockStrategy_Override_DifferentJobRunning_NoKill(t *testing.T) {
	srv, hits := killServer(t, http.StatusOK)
	defer srv.Close()

	logDAO := newMockLogDAO()
	// 运行的是 jobID=99，目标是 jobID=55
	injectLog(logDAO, makeLog(304, 99, srv.Listener.Addr().String()))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(55, model.BlockOverride)
	result := s.applyBlockStrategy(context.Background(), job)

	assert.True(t, result)
	assert.Equal(t, int64(0), atomic.LoadInt64(hits), "不同 jobID 不应触发 Kill")
}

// ─── 13-16. BlockStrategyKey ─────────────────────────────────────────────────

func TestBlockStrategyKey_Serial(t *testing.T) {
	assert.Equal(t, "SERIAL", BlockStrategyKey(model.BlockSerial))
}

func TestBlockStrategyKey_Discard(t *testing.T) {
	assert.Equal(t, "DISCARD", BlockStrategyKey(model.BlockDiscard))
}

func TestBlockStrategyKey_Override(t *testing.T) {
	assert.Equal(t, "OVERRIDE", BlockStrategyKey(model.BlockOverride))
}

func TestBlockStrategyKey_Unknown_FallsBackToSerial(t *testing.T) {
	assert.Equal(t, "SERIAL", BlockStrategyKey(model.BlockStrategy(99)))
}

// ─── Kill 请求 payload 验证 ───────────────────────────────────────────────────

func TestApplyBlockStrategy_Override_KillPayload_CorrectFields(t *testing.T) {
	var capturedBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedBody, _ = io.ReadAll(r.Body)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const logID int64 = 555
	const jobID int64 = 42

	logDAO := newMockLogDAO()
	injectLog(logDAO, makeLog(logID, jobID, srv.Listener.Addr().String()))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(jobID, model.BlockOverride)
	s.applyBlockStrategy(context.Background(), job)

	require.NotEmpty(t, capturedBody, "Kill 请求应有 body")

	var kr KillRequest
	require.NoError(t, json.Unmarshal(capturedBody, &kr))
	assert.Equal(t, logID, kr.LogID, "Kill payload 的 LogID 应匹配运行日志 ID")
	assert.Equal(t, jobID, kr.JobID, "Kill payload 的 JobID 应匹配目标任务 ID")
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestApplyBlockStrategy_Discard_ConcurrentCalls_NoRace(t *testing.T) {
	logDAO := newMockLogDAO()
	injectLog(logDAO, makeLog(401, 7, ""))

	s := bsScheduler(logDAO)
	job := makeJobWithStrategy(7, model.BlockDiscard)

	done := make(chan struct{})
	for i := 0; i < 50; i++ {
		go func() {
			s.applyBlockStrategy(context.Background(), job)
			done <- struct{}{}
		}()
	}
	for i := 0; i < 50; i++ {
		<-done
	}
}

// ─── errJobLogDAO：ListRunning 总是返回 error ─────────────────────────────────

// errJobLogDAO 是一个始终让 ListRunning 报错的 mockJobLogDAO 变体，
// 用于测试 applyBlockStrategy 对 DAO 错误的保守放行行为。
type errJobLogDAO struct {
	mockJobLogDAO
}

func (e *errJobLogDAO) ListRunning(_ context.Context) ([]*model.JobLog, error) {
	return nil, errDAOFail
}

// errDAOFail 是测试用的哨兵错误。
var errDAOFail = &daoError{"simulated DAO failure"}

type daoError struct{ msg string }

func (e *daoError) Error() string { return e.msg }
