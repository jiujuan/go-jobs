// Package executor implements the executor-side of go-jobs.
// An executor registers itself with the scheduler, receives trigger requests,
// runs job handlers and reports results.
package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/pkg/logger"
)

// Handler is the function signature for a BEAN-mode job handler.
// ctx carries a deadline, param is the JSON string from job_info.execute_param.
// The handler should return an error if execution fails.
type Handler func(ctx context.Context, param string) error

// ShardingHandler is a handler that is aware of its shard index and total.
type ShardingHandler func(ctx context.Context, param string, shardIndex, shardTotal int) error

// JobContext carries per-execution metadata injected into the handler context.
type JobContext struct {
	LogID         int64
	JobID         int64
	ShardingIndex int
	ShardingTotal int
}

type jobContextKey struct{}

// WithJobContext injects a JobContext into a context.Context.
func WithJobContext(ctx context.Context, jc JobContext) context.Context {
	return context.WithValue(ctx, jobContextKey{}, jc)
}

// GetJobContext extracts the JobContext from a context.Context.
func GetJobContext(ctx context.Context) (JobContext, bool) {
	jc, ok := ctx.Value(jobContextKey{}).(JobContext)
	return jc, ok
}

// ─── Registry ─────────────────────────────────────────────────────────────────

// Registry holds all registered job handlers for this executor.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]Handler
}

// NewRegistry creates an empty handler registry.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register binds a handler name to a function.
// Panics if name is already registered (fail-fast at startup).
func (r *Registry) Register(name string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handlers[name]; ok {
		panic(fmt.Sprintf("executor: handler %q already registered", name))
	}
	r.handlers[name] = h
	logger.Info("executor: handler registered", zap.String("name", name))
}

// Get returns the handler for name, and whether it was found.
func (r *Registry) Get(name string) (Handler, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handlers[name]
	return h, ok
}

// ─── Running jobs tracker ─────────────────────────────────────────────────────

// runningJob tracks an in-flight execution so it can be cancelled.
type runningJob struct {
	cancel context.CancelFunc
	logID  int64
	jobID  int64
	start  time.Time
}

// RunningJobSet is a thread-safe set of currently-executing jobs.
//
// # 竞态安全设计
//
// 核心不变式：add() 和 close() 在同一把互斥锁（mu）内原子执行。
// 这消除了 Runner.Run() 通过 stopped 检查后、调用 add() 前，
// Stop() 恰好完成 waitDone() 的竞态窗口：
//
//   - Runner.Run()：持有 mu 才能 add，add 完成后 wg 计数已增
//   - Runner.Stop()：持有 mu 才能 close（标记 drained），
//     此后任何 add() 调用立即返回 false（不会向 WG add 新计数）
//   - waitDone() 在 close() 之后调用，wg.Wait() 只需等当前已 add 的 goroutine
//
// 结论：wg.Add(1) 与 drained 标志的翻转互斥，不存在漏计窗口。
type RunningJobSet struct {
	mu      sync.Mutex
	jobs    map[int64]*runningJob // keyed by logID
	wg      sync.WaitGroup
	drained bool // true = close() 已调用，不再接受新 add
}

func newRunningJobSet() *RunningJobSet {
	return &RunningJobSet{jobs: make(map[int64]*runningJob)}
}

// add 注册一个新的在途任务，并将 WaitGroup 计数加一。
// 如果 close() 已被调用（drained=true），add 返回 false，
// 调用方不应启动新 goroutine。
func (s *RunningJobSet) add(logID, jobID int64, cancel context.CancelFunc) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.drained {
		return false
	}
	s.wg.Add(1)
	s.jobs[logID] = &runningJob{
		cancel: cancel,
		logID:  logID,
		jobID:  jobID,
		start:  time.Now(),
	}
	return true
}

// remove 删除任务记录并将 WaitGroup 计数减一。
// 必须与每次成功的 add() 成对调用（通常通过 defer）。
func (s *RunningJobSet) remove(logID int64) {
	s.mu.Lock()
	delete(s.jobs, logID)
	s.mu.Unlock()
	s.wg.Done()
}

// close 原子地将 drained 标志置为 true，此后所有 add() 调用均返回 false。
// 返回关闭时的当前在途任务数。
// close 与 add 互斥，消除竞态窗口。
func (s *RunningJobSet) close() int {
	s.mu.Lock()
	s.drained = true
	n := len(s.jobs)
	s.mu.Unlock()
	return n
}

// count 返回当前在途任务数。
func (s *RunningJobSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.jobs)
}

// killAll 取消所有在途任务的 context。
// 用于 gracefulTimeout 超时后的强制关闭。
func (s *RunningJobSet) killAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		j.cancel()
	}
}

// waitDone 阻塞直到所有在途任务完成或超时。
// 返回 true 表示全部干净退出，false 表示超时。
// 必须在 close() 之后调用，否则 wg.Wait() 可能永不返回。
func (s *RunningJobSet) waitDone(timeout time.Duration) bool {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(timeout):
		return false
	}
}

func (s *RunningJobSet) kill(logID int64) bool {
	s.mu.Lock()
	j, ok := s.jobs[logID]
	s.mu.Unlock()
	if !ok {
		return false
	}
	j.cancel()
	return true
}

func (s *RunningJobSet) isIdle(jobID int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range s.jobs {
		if j.jobID == jobID {
			return false
		}
	}
	return true
}
