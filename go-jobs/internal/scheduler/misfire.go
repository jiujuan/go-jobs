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

// misfireThreshold 超过此阈值认为发生了 misfire（调度延迟）。
const misfireThreshold = 5 * time.Second

// handleMisfire 检查任务是否错过了调度窗口，并按策略处理。
// 调用时机：任务在 schedule() 中被取出时，判断是否已超出正常延迟。
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
		// 忽略：只更新下次触发时间，不补偿执行。
		logger.Info("scheduler: misfire ignored", zap.Int64("jobID", job.ID))
		_ = s.advanceNextTrigger(ctx, job)
		return true // 告知调用者已处理，跳过正常调度

	case model.MisfireRunOnce:
		// 立即执行一次：先触发一次补偿，再更新下次触发时间。
		logger.Info("scheduler: misfire run-once compensation", zap.Int64("jobID", job.ID))
		s.enqueue(job, time.Now(), model.TriggerCron)
		_ = s.advanceNextTrigger(ctx, job)
		return true

	default:
		return false
	}
}
