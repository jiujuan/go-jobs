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
	// ListRetryable returns recently failed logs eligible for retry.
	ListRetryable(ctx context.Context, limit int) ([]*model.JobLog, error)
	// CountRetries returns how many retry logs exist for a job since triggerTime.
	CountRetries(ctx context.Context, jobID int64, triggerTime time.Time) (int64, error)
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

// ─── v2.0 新增方法 ────────────────────────────────────────────────────────────

// ListRetryable 返回过去 5 分钟内、首次失败、任务配置了重试的日志。
// 排除 trigger_type=TriggerRetry 的记录（防止重复统计重试日志本身）。
func (d *jobLogDAO) ListRetryable(ctx context.Context, limit int) ([]*model.JobLog, error) {
	var list []*model.JobLog
	cutoff := time.Now().Add(-5 * time.Minute)
	err := d.db.WithContext(ctx).
		Joins("JOIN job_info ON job_info.id = job_log.job_id").
		Where(
			"job_log.status = ? AND job_log.trigger_type != ? AND job_info.retry_count > 0 AND job_log.trigger_time > ?",
			model.LogFail, model.TriggerRetry, cutoff,
		).
		Order("job_log.id DESC").
		Limit(limit).
		Find(&list).Error
	return list, err
}

// CountRetries 统计某任务从 triggerTime 起已产生的重试次数。
func (d *jobLogDAO) CountRetries(ctx context.Context, jobID int64, triggerTime time.Time) (int64, error) {
	var count int64
	err := d.db.WithContext(ctx).Model(&model.JobLog{}).
		Where("job_id = ? AND trigger_type = ? AND trigger_time >= ?",
			jobID, model.TriggerRetry, triggerTime).
		Count(&count).Error
	return count, err
}
