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
type RunningJobSet struct {
	mu   sync.RWMutex
	jobs map[int64]*runningJob // keyed by logID
}

func newRunningJobSet() *RunningJobSet {
	return &RunningJobSet{jobs: make(map[int64]*runningJob)}
}

func (s *RunningJobSet) add(logID, jobID int64, cancel context.CancelFunc) {
	s.mu.Lock()
	s.jobs[logID] = &runningJob{cancel: cancel, logID: logID, jobID: jobID, start: time.Now()}
	s.mu.Unlock()
}

func (s *RunningJobSet) remove(logID int64) {
	s.mu.Lock()
	delete(s.jobs, logID)
	s.mu.Unlock()
}

func (s *RunningJobSet) kill(logID int64) bool {
	s.mu.RLock()
	j, ok := s.jobs[logID]
	s.mu.RUnlock()
	if !ok {
		return false
	}
	j.cancel()
	return true
}

func (s *RunningJobSet) isIdle(jobID int64) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, j := range s.jobs {
		if j.jobID == jobID {
			return false
		}
	}
	return true
}
