// Package dao provides the data-access layer for go-jobs.
// Each DAO exposes an interface so that services can be tested
// against mock implementations.
package dao

import (
	"context"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
	"gorm.io/gorm"
)

// ─── Executor DAO ─────────────────────────────────────────────────────────────

// ExecutorDAO defines the contract for executor persistence.
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
	err := d.db.WithContext(ctx).First(&e, id).Error
	if err != nil {
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
		Order("id ASC").Find(&list).Error
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
	var list []*model.Executor
	var total int64
	offset := (page - 1) * pageSize
	db := d.db.WithContext(ctx).Model(&model.Executor{})
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	err := db.Offset(offset).Limit(pageSize).Order("id DESC").Find(&list).Error
	return list, total, err
}

// ─── JobInfo DAO ──────────────────────────────────────────────────────────────

// JobInfoDAO defines the contract for job configuration persistence.
type JobInfoDAO interface {
	Create(ctx context.Context, j *model.JobInfo) error
	Update(ctx context.Context, j *model.JobInfo) error
	Delete(ctx context.Context, id int64) error
	FindByID(ctx context.Context, id int64) (*model.JobInfo, error)
	UpdateStatus(ctx context.Context, id int64, status model.JobStatus) error
	UpdateNextTriggerTime(ctx context.Context, id int64, next, last time.Time) error
	// ListPendingJobs returns all running jobs whose next_trigger_time <= maxTime.
	ListPendingJobs(ctx context.Context, maxTime time.Time, limit int) ([]*model.JobInfo, error)
	List(ctx context.Context, query *JobInfoQuery) ([]*model.JobInfo, int64, error)
}

// JobInfoQuery is used for paginated job list searches.
type JobInfoQuery struct {
	ExecutorApp string
	JobName     string
	Status      *model.JobStatus
	Page        int
	PageSize    int
}

type jobInfoDAO struct{ db *gorm.DB }

// NewJobInfoDAO returns a GORM-backed JobInfoDAO.
func NewJobInfoDAO(db *gorm.DB) JobInfoDAO { return &jobInfoDAO{db: db} }

func (d *jobInfoDAO) Create(ctx context.Context, j *model.JobInfo) error {
	return d.db.WithContext(ctx).Create(j).Error
}

func (d *jobInfoDAO) Update(ctx context.Context, j *model.JobInfo) error {
	return d.db.WithContext(ctx).Save(j).Error
}

func (d *jobInfoDAO) Delete(ctx context.Context, id int64) error {
	return d.db.WithContext(ctx).Delete(&model.JobInfo{}, id).Error
}

func (d *jobInfoDAO) FindByID(ctx context.Context, id int64) (*model.JobInfo, error) {
	var j model.JobInfo
	err := d.db.WithContext(ctx).First(&j, id).Error
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func (d *jobInfoDAO) UpdateStatus(ctx context.Context, id int64, status model.JobStatus) error {
	return d.db.WithContext(ctx).Model(&model.JobInfo{}).
		Where("id = ?", id).
		Update("status", status).Error
}

func (d *jobInfoDAO) UpdateNextTriggerTime(ctx context.Context, id int64, next, last time.Time) error {
	return d.db.WithContext(ctx).Model(&model.JobInfo{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"next_trigger_time": next,
			"last_trigger_time": last,
		}).Error
}

func (d *jobInfoDAO) ListPendingJobs(ctx context.Context, maxTime time.Time, limit int) ([]*model.JobInfo, error) {
	var list []*model.JobInfo
	err := d.db.WithContext(ctx).
		Where("status = ? AND next_trigger_time <= ?", model.JobRun, maxTime).
		Order("next_trigger_time ASC").
		Limit(limit).
		Find(&list).Error
	return list, err
}

func (d *jobInfoDAO) List(ctx context.Context, q *JobInfoQuery) ([]*model.JobInfo, int64, error) {
	var list []*model.JobInfo
	var total int64
	db := d.db.WithContext(ctx).Model(&model.JobInfo{})
	if q.ExecutorApp != "" {
		db = db.Where("executor_app = ?", q.ExecutorApp)
	}
	if q.JobName != "" {
		db = db.Where("job_name LIKE ?", "%"+q.JobName+"%")
	}
	if q.Status != nil {
		db = db.Where("status = ?", *q.Status)
	}
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	offset := (q.Page - 1) * q.PageSize
	err := db.Offset(offset).Limit(q.PageSize).Order("id DESC").Find(&list).Error
	return list, total, err
}

// ─── SysUser DAO ──────────────────────────────────────────────────────────────

// UserDAO defines the contract for user persistence.
type UserDAO interface {
	FindByUsername(ctx context.Context, username string) (*model.SysUser, error)
	FindByID(ctx context.Context, id int64) (*model.SysUser, error)
	Create(ctx context.Context, u *model.SysUser) error
	Update(ctx context.Context, u *model.SysUser) error
	UpdateLastLogin(ctx context.Context, id int64, t time.Time) error
}

type userDAO struct{ db *gorm.DB }

// NewUserDAO returns a GORM-backed UserDAO.
func NewUserDAO(db *gorm.DB) UserDAO { return &userDAO{db: db} }

func (d *userDAO) FindByUsername(ctx context.Context, username string) (*model.SysUser, error) {
	var u model.SysUser
	err := d.db.WithContext(ctx).Where("username = ?", username).First(&u).Error
	return &u, err
}

func (d *userDAO) FindByID(ctx context.Context, id int64) (*model.SysUser, error) {
	var u model.SysUser
	err := d.db.WithContext(ctx).First(&u, id).Error
	return &u, err
}

func (d *userDAO) Create(ctx context.Context, u *model.SysUser) error {
	return d.db.WithContext(ctx).Create(u).Error
}

func (d *userDAO) Update(ctx context.Context, u *model.SysUser) error {
	return d.db.WithContext(ctx).Save(u).Error
}

func (d *userDAO) UpdateLastLogin(ctx context.Context, id int64, t time.Time) error {
	return d.db.WithContext(ctx).Model(&model.SysUser{}).
		Where("id = ?", id).
		Update("last_login", t).Error
}
