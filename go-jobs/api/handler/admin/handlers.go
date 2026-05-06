// Package admin contains the HTTP handlers for the go-jobs admin web API.
package admin

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/jiujuan/go-jobs/api/middleware"
	"github.com/jiujuan/go-jobs/api/response"
	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/internal/service"
)

// ─── User / Auth Handler ──────────────────────────────────────────────────────

// UserHandler handles authentication and user-related requests.
type UserHandler struct {
	userSvc *service.UserService
}

// NewUserHandler creates a UserHandler.
func NewUserHandler(userSvc *service.UserService) *UserHandler {
	return &UserHandler{userSvc: userSvc}
}

// Login godoc
// @Summary      Login
// @Tags         auth
// @Accept       json
// @Produce      json
// @Param        body body service.LoginRequest true "credentials"
// @Success      200  {object} response.R{data=service.LoginResponse}
// @Router       /api/login [post]
func (h *UserHandler) Login(c *gin.Context) {
	var req service.LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}

	resp, err := h.userSvc.Login(c.Request.Context(), &req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, resp)
}

// GetCurrentUser returns info about the logged-in user.
func (h *UserHandler) GetCurrentUser(c *gin.Context) {
	userID := middleware.CurrentUserID(c)
	user, err := h.userSvc.GetUserByID(c.Request.Context(), userID)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, user)
}

// ─── Job Handler ──────────────────────────────────────────────────────────────

// JobHandler handles job CRUD, start/stop, and manual trigger requests.
type JobHandler struct {
	jobSvc *service.JobService
}

// NewJobHandler creates a JobHandler.
func NewJobHandler(jobSvc *service.JobService) *JobHandler {
	return &JobHandler{jobSvc: jobSvc}
}

// CreateJob handles POST /api/jobs
func (h *JobHandler) CreateJob(c *gin.Context) {
	var req service.CreateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	req.CreateUser = middleware.CurrentUsername(c)

	job, err := h.jobSvc.CreateJob(c.Request.Context(), &req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, job)
}

// UpdateJob handles PUT /api/jobs/:id
func (h *JobHandler) UpdateJob(c *gin.Context) {
	var req service.UpdateJobRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	id, err := pathInt64(c, "id")
	if err != nil {
		response.BadRequest(c, "invalid id")
		return
	}
	req.ID = id

	job, err := h.jobSvc.UpdateJob(c.Request.Context(), &req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, job)
}

// DeleteJob handles DELETE /api/jobs/:id
func (h *JobHandler) DeleteJob(c *gin.Context) {
	id, err := pathInt64(c, "id")
	if err != nil {
		response.BadRequest(c, "invalid id")
		return
	}
	if err := h.jobSvc.DeleteJob(c.Request.Context(), id); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// GetJob handles GET /api/jobs/:id
func (h *JobHandler) GetJob(c *gin.Context) {
	id, err := pathInt64(c, "id")
	if err != nil {
		response.BadRequest(c, "invalid id")
		return
	}
	job, err := h.jobSvc.GetJob(c.Request.Context(), id)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, job)
}

// ListJobs handles GET /api/jobs
func (h *JobHandler) ListJobs(c *gin.Context) {
	page := queryInt(c, "page", 1)
	pageSize := queryInt(c, "page_size", 20)
	if pageSize > 100 {
		pageSize = 100
	}
	var statusPtr *model.JobStatus
	if s := c.Query("status"); s != "" {
		n, _ := strconv.Atoi(s)
		st := model.JobStatus(n)
		statusPtr = &st
	}
	q := &dao.JobInfoQuery{
		ExecutorApp: c.Query("executor_app"),
		JobName:     c.Query("job_name"),
		Status:      statusPtr,
		Page:        page,
		PageSize:    pageSize,
	}
	list, total, err := h.jobSvc.ListJobs(c.Request.Context(), q)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OKWithPage(c, list, total, page, pageSize)
}

// StartJob handles POST /api/jobs/:id/start
func (h *JobHandler) StartJob(c *gin.Context) {
	id, err := pathInt64(c, "id")
	if err != nil {
		response.BadRequest(c, "invalid id")
		return
	}
	if err := h.jobSvc.StartJob(c.Request.Context(), id); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// StopJob handles POST /api/jobs/:id/stop
func (h *JobHandler) StopJob(c *gin.Context) {
	id, err := pathInt64(c, "id")
	if err != nil {
		response.BadRequest(c, "invalid id")
		return
	}
	if err := h.jobSvc.StopJob(c.Request.Context(), id); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// TriggerJob handles POST /api/jobs/:id/trigger
func (h *JobHandler) TriggerJob(c *gin.Context) {
	id, err := pathInt64(c, "id")
	if err != nil {
		response.BadRequest(c, "invalid id")
		return
	}
	var body struct {
		Param string `json:"param"`
	}
	_ = c.ShouldBindJSON(&body)

	if err := h.jobSvc.TriggerJob(c.Request.Context(), id, body.Param); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// KillJob handles POST /api/logs/:logID/kill
func (h *JobHandler) KillJob(c *gin.Context) {
	logID, err := pathInt64(c, "logID")
	if err != nil {
		response.BadRequest(c, "invalid logID")
		return
	}
	if err := h.jobSvc.KillJob(c.Request.Context(), logID); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// ─── Log Handler ──────────────────────────────────────────────────────────────

// LogHandler handles job execution log queries.
type LogHandler struct {
	jobSvc *service.JobService
}

// NewLogHandler creates a LogHandler.
func NewLogHandler(jobSvc *service.JobService) *LogHandler {
	return &LogHandler{jobSvc: jobSvc}
}

// ListJobLogs handles GET /api/jobs/:id/logs
func (h *LogHandler) ListJobLogs(c *gin.Context) {
	jobID, err := pathInt64(c, "id")
	if err != nil {
		response.BadRequest(c, "invalid job id")
		return
	}
	page := queryInt(c, "page", 1)
	pageSize := queryInt(c, "page_size", 20)

	list, total, err := h.jobSvc.ListJobLogs(c.Request.Context(), jobID, page, pageSize)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OKWithPage(c, list, total, page, pageSize)
}

// GetLogDetail handles GET /api/logs/:logID/detail
func (h *LogHandler) GetLogDetail(c *gin.Context) {
	logID, err := pathInt64(c, "logID")
	if err != nil {
		response.BadRequest(c, "invalid log id")
		return
	}
	detail, err := h.jobSvc.GetJobLogDetail(c.Request.Context(), logID)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, detail)
}

// ─── Executor Handler ─────────────────────────────────────────────────────────

// ExecutorHandler handles executor management requests.
type ExecutorHandler struct {
	executorSvc *service.ExecutorService
}

// NewExecutorHandler creates an ExecutorHandler.
func NewExecutorHandler(executorSvc *service.ExecutorService) *ExecutorHandler {
	return &ExecutorHandler{executorSvc: executorSvc}
}

// ListExecutors handles GET /api/executors
func (h *ExecutorHandler) ListExecutors(c *gin.Context) {
	page := queryInt(c, "page", 1)
	pageSize := queryInt(c, "page_size", 20)

	list, total, err := h.executorSvc.ListExecutors(c.Request.Context(), page, pageSize)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OKWithPage(c, list, total, page, pageSize)
}

// ─── Internal API (called by executors) ──────────────────────────────────────

// InternalExecutorHandler handles internal requests from executor nodes.
type InternalExecutorHandler struct {
	executorSvc *service.ExecutorService
	jobSvc      *service.JobService
}

// NewInternalExecutorHandler creates an InternalExecutorHandler.
func NewInternalExecutorHandler(executorSvc *service.ExecutorService, jobSvc *service.JobService) *InternalExecutorHandler {
	return &InternalExecutorHandler{executorSvc: executorSvc, jobSvc: jobSvc}
}

// Register handles POST /api/executor/register
func (h *InternalExecutorHandler) Register(c *gin.Context) {
	var req service.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := h.executorSvc.Register(c.Request.Context(), &req); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// Heartbeat handles POST /api/executor/heartbeat
func (h *InternalExecutorHandler) Heartbeat(c *gin.Context) {
	var req service.RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if err := h.executorSvc.Heartbeat(c.Request.Context(), &req); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// Deregister handles POST /api/executor/deregister
func (h *InternalExecutorHandler) Deregister(c *gin.Context) {
	var req service.RegisterRequest
	_ = c.ShouldBindJSON(&req)
	_ = h.executorSvc.Deregister(c.Request.Context(), &req)
	response.OK(c, nil)
}

// ─── path / query helpers ─────────────────────────────────────────────────────

func pathInt64(c *gin.Context, key string) (int64, error) {
	return strconv.ParseInt(c.Param(key), 10, 64)
}

func queryInt(c *gin.Context, key string, defaultVal int) int {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return defaultVal
	}
	return n
}
