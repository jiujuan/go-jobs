// Package service - job_group_service.go
// 任务分组管理服务（v3）。
package service

import (
	"context"

	"gorm.io/gorm"

	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

// JobGroupService 管理任务分组。
type JobGroupService struct {
	db *gorm.DB
}

// NewJobGroupService 创建 JobGroupService。
func NewJobGroupService(db *gorm.DB) *JobGroupService {
	return &JobGroupService{db: db}
}

// CreateGroup 创建新分组。
func (s *JobGroupService) CreateGroup(ctx context.Context, name, description string) (*model.JobGroup, error) {
	g := &model.JobGroup{Name: name, Description: description}
	if err := s.db.WithContext(ctx).Create(g).Error; err != nil {
		return nil, xerror.Wrap(xerror.CodeInternalServer, err, "create group failed")
	}
	return g, nil
}

// ListGroups 返回所有分组。
func (s *JobGroupService) ListGroups(ctx context.Context) ([]*model.JobGroup, error) {
	var groups []*model.JobGroup
	err := s.db.WithContext(ctx).Order("create_time DESC").Find(&groups).Error
	return groups, err
}

// DeleteGroup 删除分组（不会删除组内任务，仅解除关联）。
func (s *JobGroupService) DeleteGroup(ctx context.Context, id int64) error {
	// 将该分组下任务的 group_id 重置为 0
	if err := s.db.WithContext(ctx).Exec(
		"UPDATE job_info SET group_id = 0 WHERE group_id = ?", id,
	).Error; err != nil {
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}
	return s.db.WithContext(ctx).Delete(&model.JobGroup{}, id).Error
}
