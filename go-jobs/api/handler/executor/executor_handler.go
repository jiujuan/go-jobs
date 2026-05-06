// Package executor_handler contains the HTTP handlers exposed by each executor instance.
// These endpoints are called by the scheduler (admin node), not by humans.
package executor_handler

import (
	"time"

	"github.com/gin-gonic/gin"

	"github.com/jiujuan/go-jobs/api/response"
	"github.com/jiujuan/go-jobs/internal/executor"
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
func (h *ExecutorHTTPHandler) Run(c *gin.Context) {
	var req executor.RunRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := h.runner.Run(c.Request.Context(), &req); err != nil {
		response.Fail(c, err)
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
