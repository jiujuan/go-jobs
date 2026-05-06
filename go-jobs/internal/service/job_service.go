// Package service contains the business logic layer for go-jobs.
// Services orchestrate DAOs and other packages; they must not import api/handler packages.
package service

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/internal/scheduler"
	"github.com/jiujuan/go-jobs/pkg/logger"
	"github.com/jiujuan/go-jobs/pkg/xerror"
	"github.com/jiujuan/go-jobs/pkg/utils"
)

// JobService handles all job-related business logic.
type JobService struct {
	jobDAO  dao.JobInfoDAO
	logDAO  dao.JobLogDAO
	execDAO dao.ExecutorDAO
	sched   *scheduler.Scheduler
}

// NewJobService creates a new JobService.
func NewJobService(
	jobDAO dao.JobInfoDAO,
	logDAO dao.JobLogDAO,
	execDAO dao.ExecutorDAO,
	sched *scheduler.Scheduler,
) *JobService {
	return &JobService{
		jobDAO:  jobDAO,
		logDAO:  logDAO,
		execDAO: execDAO,
		sched:   sched,
	}
}

// ─── CRUD ──────────────────────────────────────────────────────────────────────

// CreateJobRequest is the input for creating a new job.
type CreateJobRequest struct {
	ExecutorID      int64              `json:"executor_id"       binding:"required"`
	ExecutorApp     string             `json:"executor_app"      binding:"required"`
	JobName         string             `json:"job_name"          binding:"required,max=128"`
	JobDesc         string             `json:"job_desc"`
	JobType         model.JobType      `json:"job_type"`
	CronExpression  string             `json:"cron_expression"`
	ExecuteType     model.ExecuteType  `json:"execute_type"`
	ExecuteParam    string             `json:"execute_param"`
	ExecuteHandler  string             `json:"execute_handler"   binding:"required"`
	RouteStrategy   model.RouteStrategy`json:"route_strategy"`
	BlockStrategy   model.BlockStrategy`json:"block_strategy"`
	MisfireStrategy model.MisfireStrategy `json:"misfire_strategy"`
	Timeout         int                `json:"timeout"`
	RetryCount      int                `json:"retry_count"`
	RetryInterval   int                `json:"retry_interval"`
	ShardingNum     int                `json:"sharding_num"`
	AlarmEmail      string             `json:"alarm_email"`
	AlarmWebhook    string             `json:"alarm_webhook"`
	CreateUser      string             `json:"create_user"`
}

// CreateJob creates a new job.
func (s *JobService) CreateJob(ctx context.Context, req *CreateJobRequest) (*model.JobInfo, error) {
	// Validate cron expression if provided.
	var nextTrigger *time.Time
	if req.CronExpression != "" {
		t, err := s.sched.CalcNextTriggerTime(req.CronExpression)
		if err != nil {
			return nil, xerror.Wrap(xerror.CodeCronExprInvalid, err)
		}
		nextTrigger = &t
	}

	job := &model.JobInfo{
		ExecutorID:      req.ExecutorID,
		ExecutorApp:     req.ExecutorApp,
		JobName:         req.JobName,
		JobDesc:         req.JobDesc,
		JobType:         req.JobType,
		CronExpression:  req.CronExpression,
		ExecuteType:     req.ExecuteType,
		ExecuteParam:    req.ExecuteParam,
		ExecuteHandler:  req.ExecuteHandler,
		RouteStrategy:   req.RouteStrategy,
		BlockStrategy:   req.BlockStrategy,
		MisfireStrategy: req.MisfireStrategy,
		Timeout:         req.Timeout,
		RetryCount:      req.RetryCount,
		RetryInterval:   req.RetryInterval,
		ShardingNum:     max(1, req.ShardingNum),
		AlarmEmail:      req.AlarmEmail,
		AlarmWebhook:    req.AlarmWebhook,
		Status:          model.JobStop, // created but not started yet
		NextTriggerTime: nextTrigger,
		CreateUser:      req.CreateUser,
	}

	// Apply sensible defaults.
	if job.ExecuteType == "" {
		job.ExecuteType = model.ExecuteTypeBean
	}
	if job.RouteStrategy == "" {
		job.RouteStrategy = model.RouteRoundRobin
	}
	if job.BlockStrategy == 0 {
		job.BlockStrategy = model.BlockSerial
	}
	if job.MisfireStrategy == 0 {
		job.MisfireStrategy = model.MisfireIgnore
	}
	if job.JobType == 0 {
		job.JobType = model.JobTypeCron
	}

	if err := s.jobDAO.Create(ctx, job); err != nil {
		return nil, xerror.Wrap(xerror.CodeInternalServer, err, "create job failed")
	}
	logger.Info("job created", zap.Int64("jobID", job.ID), zap.String("name", job.JobName))
	return job, nil
}

// UpdateJobRequest carries fields allowed to be updated.
type UpdateJobRequest struct {
	ID              int64               `json:"id"                binding:"required"`
	JobName         string              `json:"job_name"`
	JobDesc         string              `json:"job_desc"`
	CronExpression  string              `json:"cron_expression"`
	ExecuteType     model.ExecuteType   `json:"execute_type"`
	ExecuteParam    string              `json:"execute_param"`
	ExecuteHandler  string              `json:"execute_handler"`
	RouteStrategy   model.RouteStrategy `json:"route_strategy"`
	BlockStrategy   model.BlockStrategy `json:"block_strategy"`
	MisfireStrategy model.MisfireStrategy `json:"misfire_strategy"`
	Timeout         int                 `json:"timeout"`
	RetryCount      int                 `json:"retry_count"`
	RetryInterval   int                 `json:"retry_interval"`
	AlarmEmail      string              `json:"alarm_email"`
	AlarmWebhook    string              `json:"alarm_webhook"`
}

// UpdateJob updates a job's configuration.
func (s *JobService) UpdateJob(ctx context.Context, req *UpdateJobRequest) (*model.JobInfo, error) {
	job, err := s.jobDAO.FindByID(ctx, req.ID)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, xerror.New(xerror.CodeJobNotFound)
		}
		return nil, xerror.Wrap(xerror.CodeInternalServer, err)
	}

	if req.JobName != "" {
		job.JobName = req.JobName
	}
	job.JobDesc = req.JobDesc
	if req.CronExpression != "" {
		next, err := s.sched.CalcNextTriggerTime(req.CronExpression)
		if err != nil {
			return nil, xerror.Wrap(xerror.CodeCronExprInvalid, err)
		}
		job.CronExpression = req.CronExpression
		job.NextTriggerTime = &next
	}
	if req.ExecuteType != "" {
		job.ExecuteType = req.ExecuteType
	}
	if req.ExecuteHandler != "" {
		job.ExecuteHandler = req.ExecuteHandler
	}
	job.ExecuteParam = req.ExecuteParam
	if req.RouteStrategy != "" {
		job.RouteStrategy = req.RouteStrategy
	}
	if req.BlockStrategy != 0 {
		job.BlockStrategy = req.BlockStrategy
	}
	if req.MisfireStrategy != 0 {
		job.MisfireStrategy = req.MisfireStrategy
	}
	job.Timeout = req.Timeout
	job.RetryCount = req.RetryCount
	job.RetryInterval = req.RetryInterval
	job.AlarmEmail = req.AlarmEmail
	job.AlarmWebhook = req.AlarmWebhook

	if err := s.jobDAO.Update(ctx, job); err != nil {
		return nil, xerror.Wrap(xerror.CodeInternalServer, err, "update job failed")
	}
	return job, nil
}

// DeleteJob removes a job (soft delete via status change is recommended for auditing,
// but here we hard-delete since xxl-job does so too).
func (s *JobService) DeleteJob(ctx context.Context, id int64) error {
	_, err := s.jobDAO.FindByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return xerror.New(xerror.CodeJobNotFound)
		}
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}
	if err := s.jobDAO.Delete(ctx, id); err != nil {
		return xerror.Wrap(xerror.CodeInternalServer, err, "delete job failed")
	}
	return nil
}

// GetJob retrieves a single job.
func (s *JobService) GetJob(ctx context.Context, id int64) (*model.JobInfo, error) {
	job, err := s.jobDAO.FindByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, xerror.New(xerror.CodeJobNotFound)
		}
		return nil, xerror.Wrap(xerror.CodeInternalServer, err)
	}
	return job, nil
}

// ListJobs returns a paginated list of jobs.
func (s *JobService) ListJobs(ctx context.Context, q *dao.JobInfoQuery) ([]*model.JobInfo, int64, error) {
	return s.jobDAO.List(ctx, q)
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

// StartJob enables a job (sets status=running and computes next trigger time).
func (s *JobService) StartJob(ctx context.Context, id int64) error {
	job, err := s.jobDAO.FindByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return xerror.New(xerror.CodeJobNotFound)
		}
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}
	if job.IsRunning() {
		return xerror.New(xerror.CodeJobAlreadyStart)
	}

	// Compute next trigger time.
	if job.CronExpression != "" {
		next, err := s.sched.CalcNextTriggerTime(job.CronExpression)
		if err != nil {
			return xerror.Wrap(xerror.CodeCronExprInvalid, err)
		}
		job.NextTriggerTime = &next
		if err := s.jobDAO.Update(ctx, job); err != nil {
			return xerror.Wrap(xerror.CodeInternalServer, err)
		}
	}

	return s.jobDAO.UpdateStatus(ctx, id, model.JobRun)
}

// StopJob disables a job.
func (s *JobService) StopJob(ctx context.Context, id int64) error {
	_, err := s.jobDAO.FindByID(ctx, id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return xerror.New(xerror.CodeJobNotFound)
		}
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}
	return s.jobDAO.UpdateStatus(ctx, id, model.JobStop)
}

// TriggerJob manually fires a job once.
func (s *JobService) TriggerJob(ctx context.Context, id int64, param string) error {
	if err := s.sched.TriggerJob(ctx, id, param); err != nil {
		return xerror.Wrap(xerror.CodeJobTriggerFail, err)
	}
	logger.Info("job manually triggered", zap.Int64("jobID", id))
	return nil
}

// KillJob attempts to terminate a running job execution.
func (s *JobService) KillJob(ctx context.Context, logID int64) error {
	log, err := s.logDAO.FindByID(ctx, logID)
	if err != nil {
		return xerror.Wrap(xerror.CodeNotFound, err, "log not found")
	}
	if log.Status != model.LogRunning {
		return xerror.New(xerror.CodeInvalidParam, "job is not running")
	}

	client := NewExecutorKillClient(log.ExecutorAddress)
	if err := client.Kill(ctx, logID, log.JobID); err != nil {
		return xerror.Wrap(xerror.CodeJobTriggerFail, err, "kill failed")
	}

	now := time.Now()
	return s.logDAO.UpdateResult(ctx, logID, model.LogKilled, "manually killed", now, now)
}

// ─── Helpers ──────────────────────────────────────────────────────────────────

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// executorKillClient is a minimal client just for kill calls; avoids circular imports.
type executorKillClient struct{ address string }

func NewExecutorKillClient(address string) *executorKillClient {
	return &executorKillClient{address: address}
}

func (c *executorKillClient) Kill(ctx context.Context, logID, jobID int64) error {
	sc := &scheduler.ExecutorClient{}
	_ = sc
	// Delegate to scheduler package client.
	client := scheduler.NewExecutorClient(c.address)
	return client.Kill(ctx, &scheduler.KillRequest{LogID: logID, JobID: jobID})
}

// ─── Log Service helpers (small enough to be here for v1) ────────────────────

// ListJobLogs returns paginated logs for a job.
func (s *JobService) ListJobLogs(ctx context.Context, jobID int64, page, pageSize int) ([]*model.JobLog, int64, error) {
	return s.logDAO.ListByJob(ctx, jobID, page, pageSize)
}

// GetJobLogDetail returns the detailed log content for a log record.
func (s *JobService) GetJobLogDetail(ctx context.Context, logID int64) (*model.JobLogDetail, error) {
	return s.logDAO.FindDetail(ctx, logID)
}

// ─── Report log from executor callback ───────────────────────────────────────

// ReportResult is called by the executor callback handler to persist execution results.
func (s *JobService) ReportResult(ctx context.Context, logID int64, status model.LogStatus, errMsg string, start, end time.Time, logContent string) error {
	if err := s.logDAO.UpdateResult(ctx, logID, status, errMsg, start, end); err != nil {
		return xerror.Wrap(xerror.CodeInternalServer, err, fmt.Sprintf("update log %d result failed", logID))
	}
	if logContent != "" {
		detail := &model.JobLogDetail{
			LogID:      logID,
			LogContent: logContent,
		}
		// Get job_id from log.
		if l, err := s.logDAO.FindByID(ctx, logID); err == nil {
			detail.JobID = l.JobID
		}
		if err := s.logDAO.CreateDetail(ctx, detail); err != nil {
			logger.Warn("failed to save log detail", zap.Int64("logID", logID), zap.Error(err))
		}
	}

	// v3: 如果任务成功，触发子任务
	if status == model.LogSuccess {
		if log, err := s.logDAO.FindByID(ctx, logID); err == nil {
			if job, err := s.jobDAO.FindByID(ctx, log.JobID); err == nil && job.ChildJobIDs != "" {
				for _, childID := range utils.StringToInt64Slice(job.ChildJobIDs) {
					if trigErr := s.sched.TriggerJob(ctx, childID, log.ExecuteParam); trigErr != nil {
						logger.Warn("child job trigger failed",
							zap.Int64("parentJobID", job.ID),
							zap.Int64("childJobID", childID),
							zap.Error(trigErr))
					} else {
						logger.Info("child job triggered",
							zap.Int64("parentJobID", job.ID),
							zap.Int64("childJobID", childID))
					}
				}
			}
		}
	}

	return nil
}

// ─── v3: 子任务触发 (由 ReportResult 调用) ──────────────────────────────────

// triggerChildJobs 在父任务成功时，触发 child_job_ids 中配置的所有子任务。
// 供 ReportResult 调用，不应直接调用。
func (s *JobService) triggerChildJobs(ctx context.Context, parentJobID int64, childJobIDsStr, param string) {
	if childJobIDsStr == "" {
		return
