// Package executor_handler contains the HTTP handlers exposed by each executor instance.
// These endpoints are called by the scheduler (admin node), not by humans.
package executor_handler

import (
	"errors"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/jiujuan/go-jobs/api/response"
	"github.com/jiujuan/go-jobs/internal/executor"
	"github.com/jiujuan/go-jobs/pkg/idempotency"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

// ExecutorHTTPHandler exposes executor endpoints: /run, /kill, /beat, /idleBeat.
type ExecutorHTTPHandler struct {
	runner *executor.Runner
}

// NewExecutorHTTPHandler creates an ExecutorHTTPHandler.
func NewExecutorHTTPHandler(runner *executor.Runner) *ExecutorHTTPHandler {
	return &ExecutorHTTPHandler{runner: runner}
}

// Run handles POST /executor/run (trigger a job).
//
// 幂等语义：
//   - 200 OK：首次执行成功，或幂等重复（已成功完成）。
//   - 409 Conflict (ErrAlreadyRunning)：相同 LogID 正在执行，调度器不应重试同一 LogID。
//   - 503 Service Unavailable (ErrRunnerStopped)：executor 正在优雅关闭，调度器应路由到其他实例。
//   - 5xx：执行失败（含已失败完成后的幂等重复请求，返回原始错误）。
func (h *ExecutorHTTPHandler) Run(c *gin.Context) {
	var req executor.RunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := h.runner.Run(c.Request.Context(), &req); err != nil {
		switch {
		case errors.Is(err, idempotency.ErrAlreadyRunning):
			// 幂等冲突：相同 LogID 已在运行，返回 409
			response.Fail(c, xerror.New(xerror.CodeDuplicateExecution))
		case errors.Is(err, executor.ErrRunnerStopped):
			// executor 正在优雅关闭，返回 503 让调度器路由到其他实例
			response.Fail(c, xerror.New(xerror.CodeExecutorDraining))
		default:
			response.Fail(c, err)
		}
		return
	}
	response.OK(c, nil)
}

// Kill handles POST /executor/kill (terminate a running job).
func (h *ExecutorHTTPHandler) Kill(c *gin.Context) {
	var req executor.KillReq
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if !h.runner.Kill(req.LogID) {
		response.BadRequest(c, "job not running")
		return
	}
	response.OK(c, nil)
}

// Beat handles POST /executor/beat (health check from scheduler).
func (h *ExecutorHTTPHandler) Beat(c *gin.Context) {
	response.OK(c, map[string]interface{}{
		"time": time.Now().Format(time.RFC3339),
	})
}

// IdleBeat handles POST /executor/idleBeat (check if executor is idle for a job).
func (h *ExecutorHTTPHandler) IdleBeat(c *gin.Context) {
	var req struct {
		JobID int64 `json:"job_id"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if !h.runner.IsIdle(req.JobID) {
		response.BadRequest(c, "executor is busy")
		return
	}
	response.OK(c, nil)
}
