package dao

import (
	"context"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
	"gorm.io/gorm"
)

// JobInfoQuery carries filter + pagination parameters for job list queries.
type JobInfoQuery struct {
	ExecutorApp string
	JobName     string
	Status      *model.JobStatus
	Page        int
	PageSize    int
}

// JobInfoDAO defines the persistence contract for job configuration.
type JobInfoDAO interface {
	Create(ctx context.Context, j *model.JobInfo) error
	Update(ctx context.Context, j *model.JobInfo) error
	Delete(ctx context.Context, id int64) error
	FindByID(ctx context.Context, id int64) (*model.JobInfo, error)
	UpdateStatus(ctx context.Context, id int64, status model.JobStatus) error
	UpdateNextTriggerTime(ctx context.Context, id int64, next, last time.Time) error
	// ListPendingJobs returns running jobs whose next_trigger_time <= maxTime.
	ListPendingJobs(ctx context.Context, maxTime time.Time, limit int) ([]*model.JobInfo, error)
	List(ctx context.Context, query *JobInfoQuery) ([]*model.JobInfo, int64, error)
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
	if err := d.db.WithContext(ctx).First(&j, id).Error; err != nil {
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
	var (
		list  []*model.JobInfo
		total int64
	)
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
