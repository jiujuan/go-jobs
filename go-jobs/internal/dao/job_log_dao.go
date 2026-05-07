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

// BatchJobLogDAO extends JobLogDAO with a bulk-insert method.
// logbuffer.Buffer detects this interface and uses CreateBatch instead of
// calling Create N times, reducing DB round-trips to a single multi-row INSERT.
type BatchJobLogDAO interface {
	JobLogDAO
	// CreateBatch inserts multiple JobLog records in one DB round-trip.
	// The ID field of every record in logs is set to the real auto-increment
	// value after a successful insert.
	CreateBatch(ctx context.Context, logs []*model.JobLog) error
}

type jobLogDAO struct{ db *gorm.DB }

// NewJobLogDAO returns a GORM-backed JobLogDAO.
// The returned value also satisfies BatchJobLogDAO so that logbuffer.Buffer
// can use the optimised CreateBatch path automatically.
func NewJobLogDAO(db *gorm.DB) BatchJobLogDAO { return &jobLogDAO{db: db} }

func (d *jobLogDAO) Create(ctx context.Context, l *model.JobLog) error {
	return d.db.WithContext(ctx).Create(l).Error
}

// CreateBatch inserts multiple JobLog records in a single multi-row INSERT.
//
// GORM's Create(slice) issues one INSERT … VALUES (…),(…),… statement and
// back-fills the auto-increment ID on each element using the first inserted ID
// returned by LastInsertId plus a monotonically-increasing offset.  This is
// correct for MySQL because MySQL guarantees that a multi-row INSERT allocates
// contiguous IDs in the order of the rows.
//
// If the batch is empty the call is a no-op.
func (d *jobLogDAO) CreateBatch(ctx context.Context, logs []*model.JobLog) error {
	if len(logs) == 0 {
		return nil
	}
	// GORM v2 handles pointer-slice batch insert correctly:
	// it dereferences each element, issues one INSERT, and sets the ID fields.
	return d.db.WithContext(ctx).Create(logs).Error
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
