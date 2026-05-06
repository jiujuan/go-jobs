package service

import (
	"context"
	"time"

	"go.uber.org/zap"
	"gorm.io/gorm"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/executorstore"
	"github.com/jiujuan/go-jobs/pkg/logger"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

// ExecutorService handles executor registration, heartbeat and health management.
type ExecutorService struct {
	executorDAO      dao.ExecutorDAO
	executorStore    *executorstore.Store // 内存注册表（可为 nil，兼容无注册表场景）
	heartbeatTimeout time.Duration
}

// NewExecutorService creates an ExecutorService.
// executorStore 可以传 nil（此时只写 DB，不维护内存状态）。
func NewExecutorService(
	executorDAO dao.ExecutorDAO,
	heartbeatTimeout time.Duration,
	executorStore *executorstore.Store,
) *ExecutorService {
	return &ExecutorService{
		executorDAO:      executorDAO,
		executorStore:    executorStore,
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
// 同时将执行器写入内存注册表，调度器路由热路径无需再查 DB。
func (s *ExecutorService) Register(ctx context.Context, req *RegisterRequest) error {
	existing, err := s.executorDAO.FindByAppAndAddress(ctx, req.AppName, req.Address)
	if err != nil && err != gorm.ErrRecordNotFound {
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}

	now := time.Now()
	var record *model.Executor
	if err == gorm.ErrRecordNotFound || existing == nil {
		// 新执行器：写入 DB
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
		record = e
		logger.Info("executor registered", zap.String("app", req.AppName), zap.String("addr", req.Address))
	} else {
		// 已有执行器：刷新 DB 心跳
		if err := s.executorDAO.UpdateHeartbeat(ctx, existing.ID, now); err != nil {
			return xerror.Wrap(xerror.CodeInternalServer, err)
		}
		record = existing
		record.Version = req.Version
	}

	// 同步写入内存注册表（Register 本身幂等，重复调用安全）
	if s.executorStore != nil && record != nil {
		s.executorStore.Register(record)
	}
	return nil
}

// Heartbeat refreshes an executor's last-seen time.
// 内存 store 直接更新，DB 异步写入（executorDAO.UpdateHeartbeat 仍保留用于持久化）。
func (s *ExecutorService) Heartbeat(ctx context.Context, req *RegisterRequest) error {
	existing, err := s.executorDAO.FindByAppAndAddress(ctx, req.AppName, req.Address)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			// 记录不存在时重新注册（含写内存 store）
			return s.Register(ctx, req)
		}
		return xerror.Wrap(xerror.CodeInternalServer, err)
	}
	if err := s.executorDAO.UpdateHeartbeat(ctx, existing.ID, time.Now()); err != nil {
		return err
	}
	// 内存注册表心跳：O(1) 原子更新，无额外 DB 查询
	if s.executorStore != nil {
		s.executorStore.Heartbeat(req.AppName, req.Address, existing.CPU, existing.Memory)
	}
	return nil
}

// Deregister marks an executor offline (called on graceful shutdown).
// 同步从内存注册表移除，调度器立即感知执行器下线。
func (s *ExecutorService) Deregister(ctx context.Context, req *RegisterRequest) error {
	// 先从内存移除，路由立即生效
	if s.executorStore != nil {
		s.executorStore.Deregister(req.AppName, req.Address)
	}
	// 再更新 DB（异步持久化，失败不影响内存状态）
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
