package service

import (
	"context"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/logger"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

// ExecutorService handles executor registration, heartbeat and health management.
type ExecutorService struct {
	executorDAO dao.ExecutorDAO
	// heartbeatTimeout is how long without a heartbeat before marking offline.
	heartbeatTimeout time.Duration
}

// NewExecutorService creates an ExecutorService.
func NewExecutorService(executorDAO dao.ExecutorDAO, heartbeatTimeout time.Duration) *ExecutorService {
	return &ExecutorService{
		executorDAO:      executorDAO,
		heartbeatTimeout: heartbeatTimeout,
	}
}

// RegisterRequest is sent by an executor on startup.
type RegisterRequest struct {
	AppName string `json:"app_name" binding:"required"`
	Title   string `json:"title"`
	Address string `json:"address"  binding:"required"`
	Version string `json:"version"`
}

// Register upserts an executor record and marks it online.
func (s *ExecutorService) Register(ctx context.Context, req *RegisterRequest) error {
	existing, err := s.executorDAO.FindByAppAndAddress(ctx, req.AppName, req.Address)
	if err != nil && err != gorm.ErrRecordNotFound {
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}

	now := time.Now()
	if err == gorm.ErrRecordNotFound || existing == nil {
		// New executor - create record.
		e := &model.Executor{
			AppName:       req.AppName,
			Title:         req.Title,
			Address:       req.Address,
			RegisterType:  model.RegisterAuto,
			Status:        model.ExecutorOnline,
			HeartbeatTime: &now,
			Version:       req.Version,
		}
		if e.Title == "" {
			e.Title = req.AppName
		}
		if err := s.executorDAO.Create(ctx, e); err != nil {
			return xerror.Wrap(xerror.CodeExecutorRegisterFail, err)
		}
		logger.Info("executor registered", zap.String("app", req.AppName), zap.String("addr", req.Address))
	} else {
		// Existing executor - update heartbeat.
		if err := s.executorDAO.UpdateHeartbeat(ctx, existing.ID, now); err != nil {
			return xerror.Wrap(xerror.CodeInternalServer, err)
		}
	}
	return nil
}

// Heartbeat refreshes an executor's last-seen time.
func (s *ExecutorService) Heartbeat(ctx context.Context, req *RegisterRequest) error {
	existing, err := s.executorDAO.FindByAppAndAddress(ctx, req.AppName, req.Address)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// Re-register if the record was removed.
			return s.Register(ctx, req)
		}
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}
	return s.executorDAO.UpdateHeartbeat(ctx, existing.ID, time.Now())
}

// Deregister marks an executor offline (called on graceful shutdown).
func (s *ExecutorService) Deregister(ctx context.Context, req *RegisterRequest) error {
	existing, err := s.executorDAO.FindByAppAndAddress(ctx, req.AppName, req.Address)
	if err != nil {
		return nil // ignore if not found
	}
	return s.executorDAO.UpdateStatus(ctx, existing.ID, model.ExecutorOffline)
}

// ListExecutors returns a paginated list of executors.
func (s *ExecutorService) ListExecutors(ctx context.Context, page, pageSize int) ([]*model.Executor, int64, error) {
	return s.executorDAO.List(ctx, page, pageSize)
}

// GetOnlineAddresses returns all online executor addresses for a given appName.
func (s *ExecutorService) GetOnlineAddresses(ctx context.Context, appName string) ([]string, error) {
	executors, err := s.executorDAO.ListByApp(ctx, appName)
	if err != nil {
		return nil, xerror.Wrap(xerror.CodeInternalServer, err)
	}
	if len(executors) == 0 {
		return nil, xerror.New(xerror.CodeExecutorOffline)
	}
	addrs := make([]string, len(executors))
	for i, e := range executors {
		addrs[i] = e.Address
	}
	return addrs, nil
}

// SweepOfflineExecutors is called on a schedule to mark stale executors offline.
func (s *ExecutorService) SweepOfflineExecutors(ctx context.Context) {
	affected, err := s.executorDAO.MarkOfflineTimeout(ctx, s.heartbeatTimeout)
	if err != nil {
		logger.Error("sweep offline executors failed", zap.Error(err))
		return
	}
	if affected > 0 {
		logger.Info("swept offline executors", zap.Int64("count", affected))
	}
}
