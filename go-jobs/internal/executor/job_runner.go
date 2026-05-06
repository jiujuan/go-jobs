package executor

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/pkg/logger"
)

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
type Runner struct {
	registry *Registry
	running  *RunningJobSet
	// logCh is used to async-report log lines back to the scheduler.
	logCh chan *LogLine
}

// LogLine is a single log line to be reported to the scheduler.
type LogLine struct {
	LogID   int64
	JobID   int64
	Content string
}

// NewRunner creates a Runner backed by the given registry.
func NewRunner(registry *Registry) *Runner {
	return &Runner{
		registry: registry,
		running:  newRunningJobSet(),
		logCh:    make(chan *LogLine, 4096),
	}
}

// Run executes a job trigger request.  It is non-blocking; the actual handler
// runs in a goroutine and results are reported asynchronously.
func (r *Runner) Run(ctx context.Context, req *RunRequest) error {
	// Build execution context with optional timeout.
	execCtx := ctx
	var cancel context.CancelFunc
	if req.Timeout > 0 {
		execCtx, cancel = context.WithTimeout(context.Background(), time.Duration(req.Timeout)*time.Second)
	} else {
		execCtx, cancel = context.WithCancel(context.Background())
	}

	// Inject job metadata into context.
	execCtx = WithJobContext(execCtx, JobContext{
		LogID:         req.LogID,
		JobID:         req.JobID,
		ShardingIndex: req.ShardingIndex,
		ShardingTotal: req.ShardingTotal,
	})

	r.running.add(req.LogID, req.JobID, cancel)

	go func() {
		defer cancel()
		defer r.running.remove(req.LogID)

		var err error
		switch strings.ToUpper(req.ExecuteType) {
		case "BEAN":
			err = r.runBean(execCtx, req)
		case "SHELL":
			err = r.runShell(execCtx, req)
		case "PYTHON":
			err = r.runPython(execCtx, req)
		case "CMD":
			err = r.runCmd(execCtx, req)
		default:
			err = fmt.Errorf("unsupported execute type: %s", req.ExecuteType)
		}

		if err != nil {
			logger.Warn("runner: job failed",
				zap.Int64("logID", req.LogID),
				zap.Int64("jobID", req.JobID),
				zap.Error(err))
		} else {
			logger.Info("runner: job succeeded",
				zap.Int64("logID", req.LogID),
				zap.Int64("jobID", req.JobID))
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

// ─── Script execution helpers ─────────────────────────────────────────────────

func (r *Runner) runShell(ctx context.Context, req *RunRequest) error {
	return r.runScript(ctx, "bash", []string{"-c", req.ExecuteParam})
}

func (r *Runner) runPython(ctx context.Context, req *RunRequest) error {
	return r.runScript(ctx, "python3", []string{"-c", req.ExecuteParam})
}

func (r *Runner) runCmd(ctx context.Context, req *RunRequest) error {
	parts := strings.Fields(req.ExecuteParam)
	if len(parts) == 0 {
		return fmt.Errorf("runner: empty command")
	}
	return r.runScript(ctx, parts[0], parts[1:])
}

func (r *Runner) runScript(ctx context.Context, name string, args []string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		logger.Info("runner: script output", zap.String("output", string(out)))
	}
	if err != nil {
		return fmt.Errorf("runner: script %q: %w (output: %s)", name, err, string(out))
	}
	return nil
}
