// Package service - es_log_service.go
// ES 日志服务（v3）：将执行日志异步写入 ElasticSearch，支持全文检索。
package service

import (
	"context"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/model"
	esClient "github.com/jiujuan/go-jobs/pkg/es"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// ESLogService 将执行日志写入 ElasticSearch 并提供搜索能力。
type ESLogService struct {
	es      *esClient.Client
	enabled bool
}

// NewESLogService 创建 ESLogService。如果 enabled=false 则所有操作为 no-op。
func NewESLogService(es *esClient.Client, enabled bool) *ESLogService {
	return &ESLogService{es: es, enabled: enabled}
}

// IndexLog 异步将日志写入 ES（不阻塞调用方）。
func (s *ESLogService) IndexLog(log *model.JobLog, jobName, logContent string) {
	if !s.enabled || s.es == nil {
		return
	}
	doc := &esClient.LogDocument{
		LogID:           log.ID,
		JobID:           log.JobID,
		JobName:         jobName,
		ExecutorAddress: log.ExecutorAddress,
		Status:          int(log.Status),
		LogContent:      logContent,
		TriggerTime:     log.TriggerTime,
		DurationMs:      log.DurationMs,
		ErrorMsg:        log.ErrorMsg,
		ShardingIndex:   log.ShardingIndex,
		ShardingTotal:   log.ShardingTotal,
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10_000_000_000) // 10s
		defer cancel()
		if err := s.es.IndexLog(ctx, doc); err != nil {
			logger.Warn("es: index log failed",
				zap.Int64("logID", log.ID),
				zap.Error(err))
		}
	}()
}

// SearchLogs 在 ES 中搜索日志。
func (s *ESLogService) SearchLogs(ctx context.Context, req *esClient.SearchLogsRequest) ([]*esClient.LogDocument, int64, error) {
	if !s.enabled || s.es == nil {
		return nil, 0, nil
	}
	return s.es.SearchLogs(ctx, req)
}

// Enabled 返回 ES 功能是否启用。
func (s *ESLogService) Enabled() bool { return s.enabled && s.es != nil }
