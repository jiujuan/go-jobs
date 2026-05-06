// Package service - child_job_service.go
// 子任务依赖服务（v3）：父任务执行成功后，自动触发其配置的子任务列表。
package service

import (
	"context"
	"strings"
	"strconv"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/scheduler"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// ChildJobService 处理子任务依赖的触发逻辑。
type ChildJobService struct {
	jobDAO dao.JobInfoDAO
	sched  *scheduler.Scheduler
}

// NewChildJobService 创建 ChildJobService。
func NewChildJobService(jobDAO dao.JobInfoDAO, sched *scheduler.Scheduler) *ChildJobService {
	return &ChildJobService{jobDAO: jobDAO, sched: sched}
}

// TriggerChildren 当父任务成功时触发所有子任务。
// childJobIDsStr 为逗号分隔的任务 ID 字符串（来自 job_info.child_job_ids）。
func (s *ChildJobService) TriggerChildren(ctx context.Context, parentJobID int64, childJobIDsStr, param string) {
	if childJobIDsStr == "" {
		return
	}

	ids := parseIDList(childJobIDsStr)
	for _, childID := range ids {
		if err := s.sched.TriggerJob(ctx, childID, param); err != nil {
			logger.Warn("child job: trigger failed",
				zap.Int64("parentJobID", parentJobID),
				zap.Int64("childJobID", childID),
				zap.Error(err))
		} else {
			logger.Info("child job: triggered",
				zap.Int64("parentJobID", parentJobID),
				zap.Int64("childJobID", childID))
		}
	}
}

// GetChildJobs 返回子任务 ID 列表对应的任务信息。
func (s *ChildJobService) GetChildJobs(ctx context.Context, childJobIDsStr string) []*ChildJobInfo {
	ids := parseIDList(childJobIDsStr)
	result := make([]*ChildJobInfo, 0, len(ids))
	for _, id := range ids {
		job, err := s.jobDAO.FindByID(ctx, id)
		if err != nil {
			continue
		}
		result = append(result, &ChildJobInfo{
			ID:      job.ID,
			JobName: job.JobName,
			Status:  int8(job.Status),
		})
	}
	return result
}

// ChildJobInfo 子任务摘要信息。
type ChildJobInfo struct {
	ID      int64  `json:"id"`
	JobName string `json:"job_name"`
	Status  int8   `json:"status"`
}

func parseIDList(s string) []int64 {
	parts := strings.Split(s, ",")
	ids := make([]int64, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		id, err := strconv.ParseInt(p, 10, 64)
		if err == nil && id > 0 {
			ids = append(ids, id)
		}
	}
	return ids
}
