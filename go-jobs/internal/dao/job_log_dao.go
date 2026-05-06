package dao

import (
	"context"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
	"gorm.io/gorm"
)

// JobLogDAO defines the persistence contract for scheduling records.
type JobLogDAO interface {
	Create(ctx context.Context, l *model.JobLog) error
	UpdateResult(ctx context.Context, id int64, status model.LogStatus, errMsg string, start, end time.Time) error
	FindByID(ctx context.Context, id int64) (*model.JobLog, error)
	ListByJob(ctx context.Context, jobID int64, page, pageSize int) ([]*model.JobLog, int64, error)
	ListRunning(ctx context.Context) ([]*model.JobLog, error)
	// CreateDetail stores the detailed log text reported by the executor.
	CreateDetail(ctx context.Context, d *model.JobLogDetail) error
	FindDetail(ctx context.Context, logID int64) (*model.JobLogDetail, error)
}

type jobLogDAO struct{ db *gorm.DB }

// NewJobLogDAO returns a GORM-backed JobLogDAO.
func NewJobLogDAO(db *gorm.DB) JobLogDAO { return &jobLogDAO{db: db} }

func (d *jobLogDAO) Create(ctx context.Context, l *model.JobLog) error {
	return d.db.WithContext(ctx).Create(l).Error
}

func (d *jobLogDAO) UpdateResult(
	ctx context.Context,
	id int64,
	status model.LogStatus,
	errMsg string,
	start, end time.Time,
) error {
	durMs := end.Sub(start).Milliseconds()
	return d.db.WithContext(ctx).Model(&model.JobLog{}).
		Where("id = ?", id).
		Updates(map[string]interface{}{
			"status":      status,
			"error_msg":   errMsg,
			"start_time":  start,
			"end_time":    end,
			"duration_ms": durMs,
		}).Error
}

func (d *jobLogDAO) FindByID(ctx context.Context, id int64) (*model.JobLog, error) {
	var l model.JobLog
	err := d.db.WithContext(ctx).First(&l, id).Error
	return &l, err
}

func (d *jobLogDAO) ListByJob(ctx context.Context, jobID int64, page, pageSize int) ([]*model.JobLog, int64, error) {
	var (
		list  []*model.JobLog
		total int64
	)
	db := d.db.WithContext(ctx).Model(&model.JobLog{}).Where("job_id = ?", jobID)
	if err := db.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	offset := (page - 1) * pageSize
	err := db.Offset(offset).Limit(pageSize).Order("trigger_time DESC").Find(&list).Error
	return list, total, err
}

func (d *jobLogDAO) ListRunning(ctx context.Context) ([]*model.JobLog, error) {
	var list []*model.JobLog
	err := d.db.WithContext(ctx).
		Where("status = ?", model.LogRunning).
		Find(&list).Error
	return list, err
}

func (d *jobLogDAO) CreateDetail(ctx context.Context, det *model.JobLogDetail) error {
	return d.db.WithContext(ctx).Create(det).Error
}

func (d *jobLogDAO) FindDetail(ctx context.Context, logID int64) (*model.JobLogDetail, error) {
	var det model.JobLogDetail
	err := d.db.WithContext(ctx).Where("log_id = ?", logID).First(&det).Error
	return &det, err
}
