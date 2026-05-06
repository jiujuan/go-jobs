package executor

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/pkg/idempotency"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// defaultGracefulTimeout is the maximum time Stop() waits for in-flight jobs
// to finish naturally before forcefully cancelling them.
const defaultGracefulTimeout = 30 * time.Second

// ErrRunnerStopped is returned by Run() after Stop() has been called.
// The scheduler should treat this as a signal that the executor is draining
// and should not dispatch further triggers to this instance.
var ErrRunnerStopped = fmt.Errorf("runner: executor is shutting down, no new jobs accepted")

// RunRequest is the incoming trigger payload from the scheduler.
type RunRequest struct {
	LogID           int64  `json:"log_id"`
	JobID           int64  `json:"job_id"`
	ExecutorHandler string `json:"executor_handler"`
	ExecuteType     string `json:"execute_type"`
	ExecuteParam    string `json:"execute_param"`
	ShardingIndex   int    `json:"sharding_index"`
	ShardingTotal   int    `json:"sharding_total"`
	Timeout         int    `json:"timeout"` // seconds; 0 = no limit
}

// KillReq is the incoming kill request from the scheduler.
type KillReq struct {
	LogID int64 `json:"log_id"`
	JobID int64 `json:"job_id"`
}

// Runner handles the execution of jobs on the executor side.
//
// # 生命周期
//
//	NewRunner → Run (可并发调用) → Stop (两阶段优雅关闭)
//
// # 优雅关闭（两阶段）
//
//  1. 阶段一：停止接受新触发
//     Stop() 被调用时立即将内部 stopped 标志置为 1，
//     此后所有 Run() 调用立即返回 ErrRunnerStopped，不再启动新 goroutine。
//
//  2. 阶段二：等待存量任务自然结束
//     Stop() 等待所有正在执行的 goroutine 通过 WaitGroup 自然退出，
//     上限为 GracefulTimeout（默认 30s，可通过 WithGracefulTimeout 覆盖）。
//     若超时仍有未完成任务，调用 killAll() 强制 cancel 其 context，
//     再等待 goroutine 实际退出后返回。
//     最后释放幂等表 GC goroutine。
//
// # 执行类型路由（开放/封闭原则）
//
//	BEAN   → Go 注册的 Handler 函数
//	其他   → ScriptEngineRegistry 中的对应引擎（SHELL/PYTHON/CMD/GO/JAVA/...）
type Runner struct {
	registry      *Registry
	scriptEngines *ScriptEngineRegistry
	running       *RunningJobSet
	idempotency   *idempotency.Table
	logCh         chan *LogLine

	// stopped 是原子标志：0=运行中，1=已停止（Stop 已调用）。
	// 用 atomic 保证无锁读写，避免在高频 Run() 路径引入锁竞争。
	stopped         int32
	gracefulTimeout time.Duration
}

// LogLine is a single log line to be reported to the scheduler.
type LogLine struct {
	LogID   int64
	JobID   int64
	Content string
}

// NewRunner creates a Runner backed by the given registry.
func NewRunner(registry *Registry, opts ...RunnerOption) *Runner {
	o := defaultRunnerOptions()
	for _, opt := range opts {
		opt(o)
	}
	if o.scriptEngines == nil {
		o.scriptEngines = NewScriptEngineRegistry()
	}
	return &Runner{
		registry:        registry,
		scriptEngines:   o.scriptEngines,
		running:         newRunningJobSet(),
		idempotency:     idempotency.New(o.idempotencyOpts...),
		logCh:           make(chan *LogLine, 4096),
		gracefulTimeout: o.gracefulTimeout,
	}
}

// ─── RunnerOption ─────────────────────────────────────────────────────────────

// RunnerOption 是 Runner 的函数式选项。
type RunnerOption func(*runnerOptions)

type runnerOptions struct {
	idempotencyOpts []idempotency.Option
	scriptEngines   *ScriptEngineRegistry
	gracefulTimeout time.Duration
}

func defaultRunnerOptions() *runnerOptions {
	return &runnerOptions{
		gracefulTimeout: defaultGracefulTimeout,
	}
}

// WithIdempotencyTTL 设置幂等记录保留时长。
func WithIdempotencyTTL(d time.Duration) RunnerOption {
	return func(o *runnerOptions) {
		o.idempotencyOpts = append(o.idempotencyOpts, idempotency.WithTTL(d))
	}
}

// WithIdempotencyGCInterval 设置幂等表 GC 间隔。
func WithIdempotencyGCInterval(d time.Duration) RunnerOption {
	return func(o *runnerOptions) {
		o.idempotencyOpts = append(o.idempotencyOpts, idempotency.WithGCInterval(d))
	}
}

// WithScriptEngineRegistry 注入自定义脚本引擎注册表。
func WithScriptEngineRegistry(r *ScriptEngineRegistry) RunnerOption {
	return func(o *runnerOptions) {
		o.scriptEngines = r
	}
}

// WithGracefulTimeout 设置 Stop() 等待存量任务自然结束的最大时长（默认 30s）。
// 超出此时长后，所有仍在运行的任务会被强制 cancel。
// 传入 0 表示不等待，直接强制关闭所有任务。
func WithGracefulTimeout(d time.Duration) RunnerOption {
	return func(o *runnerOptions) {
		o.gracefulTimeout = d
	}
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

// Stop 两阶段优雅关闭 Runner。
//
// 阶段一（原子）：调用 running.close() 同时完成两件事：
//   - 将内部 drained 标志置 true（此后 add() 返回 false，Run() 不再启动新 goroutine）
//   - 原子地读取当前在途任务数（close 与 add 互斥，无竞态窗口）
//
// 阶段二（等待）：等待所有存量任务自然退出，超出 GracefulTimeout 则强制 cancel。
//
// Stop 是幂等的，多次调用安全（CAS 保证只执行一次）。
// Stop 会阻塞直到所有 goroutine 完全退出后返回。
func (r *Runner) Stop() {
	// 原子 CAS：只有第一次调用真正执行关闭逻辑
	if !atomic.CompareAndSwapInt32(&r.stopped, 0, 1) {
		return // 已停止，幂等返回
	}

	// ── 阶段一：原子关闭，同时获取在途任务数 ────────────────────────────
	// close() 与 add() 互斥：此后任何 Run() 调用的 add() 都返回 false，
	// 已经在 close() 之前完成 add() 的 goroutine 已被 WG 计数，waitDone 能正确等待。
	n := r.running.close()
	if n > 0 {
		logger.Info("runner: stopping — waiting for in-flight jobs",
			zap.Int("count", n),
			zap.Duration("gracefulTimeout", r.gracefulTimeout),
		)
	} else {
		logger.Info("runner: stopping — no in-flight jobs")
	}

	// ── 阶段二：等待存量任务自然结束 ────────────────────────────────────
	if r.gracefulTimeout > 0 {
		if clean := r.running.waitDone(r.gracefulTimeout); clean {
			logger.Info("runner: all in-flight jobs finished cleanly")
		} else {
			remaining := r.running.count()
			logger.Warn("runner: graceful timeout exceeded, force-cancelling remaining jobs",
				zap.Int("remaining", remaining),
				zap.Duration("gracefulTimeout", r.gracefulTimeout),
			)
			r.running.killAll()
			// 强制 cancel 后仍需等待 goroutine 实际退出（通常极快）
			r.running.waitDone(5 * time.Second)
		}
	} else {
		// gracefulTimeout == 0：直接强制关闭
		r.running.killAll()
		r.running.waitDone(5 * time.Second)
	}

	// ── 阶段三：释放后台资源 ─────────────────────────────────────────────
	r.idempotency.Stop()
	logger.Info("runner: stopped")
}

// IsStopped 返回 Runner 是否已进入停止状态（Stop 已被调用）。
func (r *Runner) IsStopped() bool {
	return atomic.LoadInt32(&r.stopped) == 1
}

// ScriptEngines 返回当前使用的脚本引擎注册表（诊断/测试用）。
func (r *Runner) ScriptEngines() *ScriptEngineRegistry {
	return r.scriptEngines
}

// ─── Run ─────────────────────────────────────────────────────────────────────

// Run executes a job trigger request. Non-blocking; handler runs in a goroutine.
//
// 若 Runner 已停止（Stop 被调用），Run 立即返回 ErrRunnerStopped，不启动任何 goroutine。
// 竞态安全：stopped 标志检查（快速路径）+ running.add() 的 drained 检查（精确屏障）
// 双重保护，彻底消除 stopped 检查与 add() 之间的竞态窗口。
func (r *Runner) Run(ctx context.Context, req *RunRequest) error {
	// ── 快速路径：stopped 标志（原子读，无锁，高频路径性能优先） ──────────
	// 正常运行期间此检查始终通过，开销极低。
	// 注意：这里通过后仍可能被 running.add() 的 drained 检查拦截（精确屏障）。
	if atomic.LoadInt32(&r.stopped) == 1 {
		logger.Warn("runner: rejected — executor is shutting down",
			zap.Int64("logID", req.LogID),
			zap.Int64("jobID", req.JobID),
		)
		return ErrRunnerStopped
	}

	// ── 幂等检查 ────────────────────────────────────────────────────────────
	rec, acquired := r.idempotency.TryAcquire(req.LogID)
	if !acquired {
		switch rec.State {
		case idempotency.StateRunning:
			logger.Warn("runner: duplicate request rejected (already running)",
				zap.Int64("logID", req.LogID), zap.Int64("jobID", req.JobID))
			return idempotency.ErrAlreadyRunning
		case idempotency.StateSuccess:
			logger.Info("runner: duplicate request, already succeeded (idempotent ok)",
				zap.Int64("logID", req.LogID), zap.Int64("jobID", req.JobID))
			return nil
		case idempotency.StateFailed:
			logger.Warn("runner: duplicate request, already failed",
				zap.Int64("logID", req.LogID), zap.Int64("jobID", req.JobID), zap.Error(rec.Err))
			return rec.Err
		}
	}

	// ── 构建执行上下文 ───────────────────────────────────────────────────────
	var execCtx context.Context
	var cancel context.CancelFunc
	if req.Timeout > 0 {
		execCtx, cancel = context.WithTimeout(context.Background(), time.Duration(req.Timeout)*time.Second)
	} else {
		execCtx, cancel = context.WithCancel(context.Background())
	}
	execCtx = WithJobContext(execCtx, JobContext{
		LogID:         req.LogID,
		JobID:         req.JobID,
		ShardingIndex: req.ShardingIndex,
		ShardingTotal: req.ShardingTotal,
	})

	// ── 精确屏障：add() 与 close() 互斥 ────────────────────────────────────
	// 若 Stop() 在上面 stopped 检查之后、此处 add() 之前完成了 close()，
	// add() 会返回 false，我们安全地回滚并返回 ErrRunnerStopped。
	// 这彻底消除了竞态窗口：不会有 goroutine 在 waitDone 返回后才启动。
	if !r.running.add(req.LogID, req.JobID, cancel) {
		cancel()
		// 回滚幂等表：将刚 TryAcquire 的 Running 记录标记为 Failed
		r.idempotency.Complete(req.LogID, ErrRunnerStopped)
		logger.Warn("runner: rejected at add() — executor is draining",
			zap.Int64("logID", req.LogID),
			zap.Int64("jobID", req.JobID),
		)
		return ErrRunnerStopped
	}

	go func() {
		defer cancel()
		defer r.running.remove(req.LogID) // 同时递减 WaitGroup

		var err error
		switch strings.ToUpper(req.ExecuteType) {
		case "BEAN":
			err = r.runBean(execCtx, req)
		default:
			err = r.runScript(execCtx, req)
		}

		r.idempotency.Complete(req.LogID, err)

		if err != nil {
			logger.Warn("runner: job failed",
				zap.Int64("logID", req.LogID),
				zap.Int64("jobID", req.JobID),
				zap.String("executeType", req.ExecuteType),
				zap.Error(err))
		} else {
			logger.Info("runner: job succeeded",
				zap.Int64("logID", req.LogID),
				zap.Int64("jobID", req.JobID),
				zap.String("executeType", req.ExecuteType))
		}
	}()

	return nil
}

// Kill terminates a running job by logID.
func (r *Runner) Kill(logID int64) bool {
	return r.running.kill(logID)
}

// IsIdle returns true if no job with the given jobID is currently executing.
func (r *Runner) IsIdle(jobID int64) bool {
	return r.running.isIdle(jobID)
}

// ─── BEAN handler execution ───────────────────────────────────────────────────

func (r *Runner) runBean(ctx context.Context, req *RunRequest) error {
	h, ok := r.registry.Get(req.ExecutorHandler)
	if !ok {
		return fmt.Errorf("runner: handler %q not registered", req.ExecutorHandler)
	}
	return h(ctx, req.ExecuteParam)
}

// ─── Script execution（策略模式委托）─────────────────────────────────────────

func (r *Runner) runScript(ctx context.Context, req *RunRequest) error {
	engine, ok := r.scriptEngines.Get(req.ExecuteType)
	if !ok {
		return fmt.Errorf(
			"runner: unsupported execute_type %q (registered engines: %s)",
			req.ExecuteType,
			strings.Join(r.scriptEngines.Types(), ", "),
		)
	}

	result, err := engine.Execute(ctx, req)

	if result != nil && len(result.Combined()) > 0 {
		select {
		case r.logCh <- &LogLine{LogID: req.LogID, JobID: req.JobID, Content: result.Combined()}:
		default:
			logger.Warn("runner: logCh full, dropping script output",
				zap.Int64("logID", req.LogID))
		}
	}

	return err
}
