// Package service - retry_service.go
// 失败重试服务：扫描失败日志，按配置的重试次数和间隔重新触发任务。
package service

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/scheduler"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// RetryService 扫描失败的执行日志，重新调度符合条件的任务。
type RetryService struct {
	jobDAO dao.JobInfoDAO
	logDAO dao.JobLogDAO
	sched  *scheduler.Scheduler
}

// NewRetryService 创建 RetryService。
func NewRetryService(jobDAO dao.JobInfoDAO, logDAO dao.JobLogDAO, sched *scheduler.Scheduler) *RetryService {
	return &RetryService{jobDAO: jobDAO, logDAO: logDAO, sched: sched}
}

// Run 应在定时器中周期调用（如每 30s 一次）。
// 逻辑：找出过去 5 分钟内首次失败、任务配置了 retry_count > 0
// 且尚未超过重试上限的日志，按 retry_interval 延迟触发。
func (s *RetryService) Run(ctx context.Context) {
	logs, err := s.logDAO.ListRetryable(ctx, 100)
	if err != nil {
		logger.Error("retry: list retryable logs", zap.Error(err))
		return
	}

	for _, l := range logs {
		l := l // capture
		job, err := s.jobDAO.FindByID(ctx, l.JobID)
		if err != nil || job.RetryCount <= 0 {
			continue
		}

		// 已重试次数
		retried, _ := s.logDAO.CountRetries(ctx, l.JobID, l.TriggerTime)
		if int(retried) >= job.RetryCount {
			continue
		}

		delay := time.Duration(job.RetryInterval) * time.Second
		if delay < time.Second {
			delay = time.Second
		}

		time.AfterFunc(delay, func() {
			if err := s.sched.TriggerJob(context.Background(), job.ID, l.ExecuteParam); err != nil {
				logger.Warn("retry: trigger failed",
					zap.Int64("jobID", job.ID),
					zap.Int64("logID", l.ID),
					zap.Error(err))
			} else {
				logger.Info("retry: triggered",
					zap.Int64("jobID", job.ID),
					zap.Int64("logID", l.ID),
					zap.Duration("delay", delay))
			}
		})
	}
}
