// Package service - stat_service.go
// 调度统计服务：每小时将 job_log 数据聚合到 schedule_stat 表。
package service

import (
	"context"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// ScheduleStat 对应数据库 schedule_stat 表。
type ScheduleStat struct {
	ID           int64     `gorm:"primaryKey;autoIncrement" json:"id"`
	StatHour     time.Time `gorm:"not null;uniqueIndex"     json:"stat_hour"`
	TotalCount   int       `gorm:"not null;default:0"       json:"total_count"`
	SuccessCount int       `gorm:"not null;default:0"       json:"success_count"`
	FailCount    int       `gorm:"not null;default:0"       json:"fail_count"`
	TimeoutCount int       `gorm:"not null;default:0"       json:"timeout_count"`
	CreateTime   time.Time `gorm:"autoCreateTime"           json:"create_time"`
}

func (ScheduleStat) TableName() string { return "schedule_stat" }

// StatService 聚合调度统计数据。
type StatService struct {
	db *gorm.DB
}

// NewStatService 创建 StatService。
func NewStatService(db *gorm.DB) *StatService {
	return &StatService{db: db}
}

// Aggregate 聚合指定小时的调度统计，写入 schedule_stat 表。
// 通常在每小时末调用：StatService.Aggregate(ctx, time.Now().Truncate(time.Hour))
func (s *StatService) Aggregate(ctx context.Context, hour time.Time) error {
	hour = hour.Truncate(time.Hour)
	nextHour := hour.Add(time.Hour)

	type result struct {
		Total   int
		Success int
		Fail    int
		Timeout int
	}

	var r result
	err := s.db.WithContext(ctx).
		Model(&model.JobLog{}).
		Select(`
			COUNT(*) AS total,
			SUM(CASE WHEN status = 1 THEN 1 ELSE 0 END) AS success,
			SUM(CASE WHEN status = 2 THEN 1 ELSE 0 END) AS fail,
			SUM(CASE WHEN status = 4 THEN 1 ELSE 0 END) AS timeout
		`).
		Where("trigger_time >= ? AND trigger_time < ?", hour, nextHour).
		Scan(&r).Error
	if err != nil {
		return err
	}

	stat := ScheduleStat{
		StatHour:     hour,
		TotalCount:   r.Total,
		SuccessCount: r.Success,
		FailCount:    r.Fail,
		TimeoutCount: r.Timeout,
	}

	// Upsert：已存在则更新，否则插入。
	result2 := s.db.WithContext(ctx).
		Where(ScheduleStat{StatHour: hour}).
		Assign(ScheduleStat{
			TotalCount:   stat.TotalCount,
			SuccessCount: stat.SuccessCount,
			FailCount:    stat.FailCount,
			TimeoutCount: stat.TimeoutCount,
		}).
		FirstOrCreate(&stat)
	if result2.Error != nil {
		return result2.Error
	}

	logger.Info("stat aggregated",
		zap.Time("hour", hour),
		zap.Int("total", r.Total),
		zap.Int("success", r.Success),
		zap.Int("fail", r.Fail))
	return nil
}

// GetRecentStats 返回最近 N 小时的统计数据。
func (s *StatService) GetRecentStats(ctx context.Context, hours int) ([]ScheduleStat, error) {
	since := time.Now().Add(-time.Duration(hours) * time.Hour).Truncate(time.Hour)
	var stats []ScheduleStat
	err := s.db.WithContext(ctx).
		Where("stat_hour >= ?", since).
		Order("stat_hour ASC").
		Find(&stats).Error
	return stats, err
}

// GetDashboardStats 返回最近 24 小时汇总。
func (s *StatService) GetDashboardStats(ctx context.Context) (map[string]interface{}, error) {
	stats, err := s.GetRecentStats(ctx, 24)
	if err != nil {
		return nil, err
	}

	var total, success, fail, timeout int
	for _, st := range stats {
		total += st.TotalCount
		success += st.SuccessCount
		fail += st.FailCount
		timeout += st.TimeoutCount
	}

	successRate := float64(0)
	if total > 0 {
		successRate = float64(success) / float64(total) * 100
	}

	return map[string]interface{}{
		"total_24h":    total,
		"success_24h":  success,
		"fail_24h":     fail,
		"timeout_24h":  timeout,
		"success_rate": successRate,
		"hourly":       stats,
	}, nil
}
