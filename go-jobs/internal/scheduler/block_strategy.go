// Package scheduler - block_strategy.go
// 阻塞处理策略实现：当任务触发时已有同一任务在执行，
// 根据 job.BlockStrategy 决定行为。
package scheduler

import (
	"context"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// applyBlockStrategy 根据阻塞策略决定是否继续本次触发。
// 返回 true 表示继续调度，false 表示跳过本次触发。
func (s *Scheduler) applyBlockStrategy(ctx context.Context, job *model.JobInfo) bool {
	switch job.BlockStrategy {
	case model.BlockDiscard:
		// 丢弃策略：如果同一任务有正在运行的日志，丢弃本次触发。
		running, err := s.logDAO.ListRunning(ctx)
		if err != nil {
			logger.Warn("block: list running failed", zap.Int64("jobID", job.ID), zap.Error(err))
			return true
		}
		for _, l := range running {
			if l.JobID == job.ID {
				logger.Info("block: discard trigger (job still running)",
					zap.Int64("jobID", job.ID),
					zap.Int64("runningLogID", l.ID))
				return false
			}
		}
		return true

	case model.BlockOverride:
		// 覆盖策略：终止旧的执行，启动新的。
		running, err := s.logDAO.ListRunning(ctx)
		if err != nil {
			logger.Warn("block: list running failed", zap.Int64("jobID", job.ID), zap.Error(err))
			return true
		}
		for _, l := range running {
			if l.JobID == job.ID && l.ExecutorAddress != "" {
				client := NewExecutorClient(l.ExecutorAddress)
				killCtx, cancel := context.WithTimeout(ctx, 5000*1000*1000) // 5s
				defer cancel()
				if err := client.Kill(killCtx, &KillRequest{LogID: l.ID, JobID: job.ID}); err != nil {
					logger.Warn("block: kill old execution failed",
						zap.Int64("logID", l.ID),
						zap.Error(err))
				} else {
					logger.Info("block: killed old execution for override",
						zap.Int64("jobID", job.ID),
						zap.Int64("killedLogID", l.ID))
				}
			}
		}
		return true

	default: // BlockSerial: 串行执行，队列排队
		return true
	}
}

// BlockStrategyKey returns a descriptive name for the block strategy (for logging).
func BlockStrategyKey(bs model.BlockStrategy) string {
	switch bs {
	case model.BlockDiscard:
		return "DISCARD"
	case model.BlockOverride:
		return "OVERRIDE"
	default:
		return "SERIAL"
	}
}

// Ensure Scheduler has access to logDAO for block strategy (already has it).
var _ dao.JobLogDAO = (dao.JobLogDAO)(nil)
