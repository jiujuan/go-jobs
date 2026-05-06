// Package scheduler - misfire.go
// Misfire 补偿执行：当调度器重启或短暂故障后，
// 检查 next_trigger_time 已过期的任务并按策略决定是否补偿触发。
package scheduler

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// misfireThreshold: 超过此阈值认为发生了 misfire（任务错过正常调度窗口）。
const misfireThreshold = 5 * time.Second

// handleMisfire 检查任务是否错过了调度窗口，并按策略处理。
// 返回 true 表示已由 misfire 逻辑处理（调用者跳过正常调度）。
// 在时间轮架构下，misfire 仅可能发生在进程重启后的首次 bootstrap 扫描中。
func (s *Scheduler) handleMisfire(ctx context.Context, job *model.JobInfo) bool {
	if job.NextTriggerTime == nil {
		return false
	}

	overdue := time.Since(*job.NextTriggerTime)
	if overdue <= misfireThreshold {
		return false // 正常范围内，不算 misfire
	}

	logger.Warn("scheduler: misfire detected",
		zap.Int64("jobID", job.ID),
		zap.String("jobName", job.JobName),
		zap.Duration("overdue", overdue),
		zap.Int8("strategy", int8(job.MisfireStrategy)))

	switch job.MisfireStrategy {
	case model.MisfireIgnore:
		// 忽略：只推进下次触发时间，不补偿执行。
		logger.Info("scheduler: misfire ignored, advancing next trigger",
			zap.Int64("jobID", job.ID))
		s.reschedule(ctx, job)
		return true

	case model.MisfireRunOnce:
		// 立即执行一次补偿，然后重新计算下次触发时间。
		logger.Info("scheduler: misfire run-once compensation",
			zap.Int64("jobID", job.ID))
		// 直接入 workerCh，非阻塞投递
		select {
		case s.workerCh <- &triggerTask{
			job:         job,
			triggerTime: time.Now(),
			triggerType: model.TriggerCron,
		}:
		default:
			logger.Warn("scheduler: worker channel full during misfire compensation",
				zap.Int64("jobID", job.ID))
		}
		s.reschedule(ctx, job)
		return true

	default:
		return false
	}
}
