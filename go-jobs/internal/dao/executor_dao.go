// Package dao provides the data-access layer for go-jobs.
// Each DAO exposes an interface so that services can be unit-tested
// against mock implementations without a real database.
package dao

import (
	"context"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
	"gorm.io/gorm"
)

// ExecutorDAO defines the persistence contract for executor nodes.
type ExecutorDAO interface {
	Create(ctx context.Context, e *model.Executor) error
	Update(ctx context.Context, e *model.Executor) error
	Delete(ctx context.Context, id int64) error
	FindByID(ctx context.Context, id int64) (*model.Executor, error)
	FindByAppAndAddress(ctx context.Context, appName, address string) (*model.Executor, error)
	ListByApp(ctx context.Context, appName string) ([]*model.Executor, error)
	ListOnline(ctx context.Context) ([]*model.Executor, error)
	UpdateHeartbeat(ctx context.Context, id int64, t time.Time) error
	UpdateStatus(ctx context.Context, id int64, status model.ExecutorStatus) error
	// MarkOfflineTimeout marks executors offline whose heartbeat is older than timeout.
	MarkOfflineTimeout(ctx context.Context, timeout time.Duration) (int64, error)
	List(ctx context.Context, page, pageSize int) ([]*model.Executor, int64, error)
}

type executorDAO struct{ db *gorm.DB }

// NewExecutorDAO returns a GORM-backed ExecutorDAO.
func NewExecutorDAO(db *gorm.DB) ExecutorDAO { return &executorDAO{db: db} }

func (d *executorDAO) Create(ctx context.Context, e *model.Executor) error {
	return d.db.WithContext(ctx).Create(e).Error
}

func (d *executorDAO) Update(ctx context.Context, e *model.Executor) error {
	return d.db.WithContext(ctx).Save(e).Error
}

func (d *executorDAO) Delete(ctx context.Context, id int64) error {
	return d.db.WithContext(ctx).Delete(&model.Executor{}, id).Error
}

func (d *executorDAO) FindByID(ctx context.Context, id int64) (*model.Executor, error) {
	var e model.Executor
	if err := d.db.WithContext(ctx).First(&e, id).Error; err != nil {
		return nil, err
	}
	return &e, nil
}

func (d *executorDAO) FindByAppAndAddress(ctx context.Context, appName, address string) (*model.Executor, error) {
	var e model.Executor
	err := d.db.WithContext(ctx).
		Where("app_name = ? AND address = ?", appName, address).
		First(&e).Error
	if err != nil {
		return nil, err
	}
	return &e, nil
}

func (d *executorDAO) ListByApp(ctx context.Context, appName string) ([]*model.Executor, error) {
	var list []*model.Executor
	err := d.db.WithContext(ctx).
		Where("app_name = ? AND status = ?", appName, model.ExecutorOnline).
		Order("id ASC").
		Find(&list).Error
	return list, err
}

func (d *executorDAO) ListOnline(ctx context.Context) ([]*model.Executor, error) {
	var list []*model.Executor
	err := d.db.WithContext(ctx).
		Where("status = ?", model.ExecutorOnline).
		Find(&list).Error
	return list, err
}

func (d *executorDAO) UpdateHeartbeat(ctx context.Context, id int64, t time.Time) error {
	return d.db.WithContext(ctx).Model(&model.Executor{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"heartbeat_time": t,
			"status":         model.ExecutorOnline,
		}).Error
}

func (d *executorDAO) UpdateStatus(ctx context.Context, id int64, status model.ExecutorStatus) error {
	return d.db.WithContext(ctx).Model(&model.Executor{}).
		Where("id = ?", id).
		Update("status", status).Error
}

func (d *executorDAO) MarkOfflineTimeout(ctx context.Context, timeout time.Duration) (int64, error) {
	deadline := time.Now().Add(-timeout)
	result := d.db.WithContext(ctx).Model(&model.Executor{}).
		Where("status = ? AND heartbeat_time < ?", model.ExecutorOnline, deadline).
		Update("status", model.ExecutorOffline)
	return result.RowsAffected, result.Error
}

func (d *executorDAO) List(ctx context.Context, page, pageSize int) ([]*model.Executor, int64, error) {
	var (
		list  []*model.Executor
		total int64
	)
	offset := (page - 1) * pageSize
	db := d.db.WithContext(ctx).Model(&model.Executor{})
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	err := db.Offset(offset).Limit(pageSize).Order("id DESC").Find(&list).Error
	return list, total, err
}
