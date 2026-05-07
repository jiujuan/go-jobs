// Package scheduler contains the core scheduling engine for go-jobs v3.
//
// # Architecture (v3 rewrite)
//
// The old design polled MySQL every second looking for due jobs.  This caused
// N database round-trips per second regardless of how many jobs were actually
// due.
//
// The v3 design eliminates the polling loop:
//
//  1. At startup the store is bootstrapped from MySQL in a single query.
//  2. A hierarchical time-wheel (pkg/timewheel) fires a callback at the exact
//     moment each job is due — no busy-waiting, no polling.
//  3. An in-memory job store (pkg/jobstore) holds the canonical schedule; MySQL
//     is updated asynchronously by a background flush goroutine.  If the
//     process restarts, the store is re-bootstrapped from MySQL in O(n) time.
//
// # Concurrency guarantee
//
// Each trigger callback acquires a short-lived Redis lock before dispatching.
// This prevents duplicate execution when multiple scheduler nodes run
// concurrently (e.g. during Etcd leader-handover overlap).
package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/executorstore"
	"github.com/jiujuan/go-jobs/pkg/jobstore"
	"github.com/jiujuan/go-jobs/pkg/logger"
	"github.com/jiujuan/go-jobs/pkg/paramtpl"
	"github.com/jiujuan/go-jobs/pkg/ratelimit"
	redispkg "github.com/jiujuan/go-jobs/pkg/redis"
	"github.com/jiujuan/go-jobs/pkg/timewheel"
)

const (
	defaultWorkerNum   = 64
	lockTTL            = 30 * time.Second
	storeFlushInterval = 5 * time.Second
	bootstrapBatchSize = 2000
)

// ─── Scheduler ────────────────────────────────────────────────────────────────

// Scheduler is the central scheduling engine.
type Scheduler struct {
	jobDAO      dao.JobInfoDAO
	logDAO      dao.JobLogDAO
	executorDAO dao.ExecutorDAO

	redis  *redispkg.Client
	nodeID string

	store         *jobstore.Store
	executorStore *executorstore.Store
	wheel         *timewheel.TimeWheel

	workerCh   chan *triggerTask
	cronParser cron.Parser
	flushCtx   context.Context
	flushStop  context.CancelFunc

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup

	// 功能扩展
	rateLimiter *ratelimit.Registry // 任务限流与配额（可为 nil，不限流）
	paramEngine *paramtpl.Engine    // 参数模板渲染引擎（可为 nil，不渲染）
}

// Options configures the Scheduler.
type Options struct {
	PreloadWindow time.Duration
	WorkerNum     int
	NodeID        string
	RateLimiter   *ratelimit.Registry // 限流注册表，nil 表示不限流
	ParamEngine   *paramtpl.Engine    // 参数模板引擎，nil 表示不渲染
}

// Option is a functional option for Scheduler.
type Option func(*Options)

func defaultOpts() *Options {
	return &Options{
		PreloadWindow: 5 * time.Second,
		WorkerNum:     defaultWorkerNum,
	}
}

// WithPreloadWindow is kept for API compatibility (no-op in v3).
func WithPreloadWindow(d time.Duration) Option {
	return func(o *Options) { o.PreloadWindow = d }
}

// WithWorkerNum sets the trigger dispatch worker pool size.
func WithWorkerNum(n int) Option { return func(o *Options) { o.WorkerNum = n } }

// WithNodeID sets an explicit node identifier.
func WithNodeID(id string) Option { return func(o *Options) { o.NodeID = id } }

// WithRateLimiter 注入限流注册表。nil 表示不启用限流（默认）。
func WithRateLimiter(r *ratelimit.Registry) Option {
	return func(o *Options) { o.RateLimiter = r }
}

// WithParamEngine 注入参数模板引擎。nil 表示不启用渲染（默认）。
func WithParamEngine(e *paramtpl.Engine) Option {
	return func(o *Options) { o.ParamEngine = e }
}

// ─── storeFlusher adapts JobInfoDAO to jobstore.Flusher ──────────────────────

type storeFlusher struct{ dao dao.JobInfoDAO }

func (f *storeFlusher) FlushJob(ctx context.Context, job *model.JobInfo) error {
	return f.dao.Update(ctx, job)
}

// ─── Constructor ──────────────────────────────────────────────────────────────

// New creates a new Scheduler.
func New(
	jobDAO dao.JobInfoDAO,
	logDAO dao.JobLogDAO,
	executorDAO dao.ExecutorDAO,
	redis *redispkg.Client,
	executorStore *executorstore.Store,
	opts ...Option,
) *Scheduler {
	o := defaultOpts()
	for _, opt := range opts {
		opt(o)
	}
	if o.NodeID == "" {
		o.NodeID = uuid.New().String()
	}
	if o.WorkerNum <= 0 {
		o.WorkerNum = defaultWorkerNum
	}

	flushCtx, flushStop := context.WithCancel(context.Background())

	return &Scheduler{
		jobDAO:        jobDAO,
		logDAO:        logDAO,
		executorDAO:   executorDAO,
		redis:         redis,
		nodeID:        o.NodeID,
		store:         jobstore.New(&zapLogger{}),
		executorStore: executorStore,
		wheel:         timewheel.New(time.Now()),
		workerCh:      make(chan *triggerTask, o.WorkerNum*8),
		cronParser:    cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
		flushCtx:      flushCtx,
		flushStop:     flushStop,
		stopCh:        make(chan struct{}),
		rateLimiter:   o.RateLimiter,
		paramEngine:   o.ParamEngine,
	}
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

// Start bootstraps the store from MySQL, registers all jobs in the time wheel,
// and starts the worker pool. Safe to call only once.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return fmt.Errorf("scheduler: already running")
	}

	if err := s.bootstrap(); err != nil {
		return fmt.Errorf("scheduler: bootstrap: %w", err)
	}

	s.store.Range(func(job *model.JobInfo) bool {
		if job.Status == model.JobRun && job.NextTriggerTime != nil {
			s.scheduleWheel(job)
		}
		return true
	})

	s.wheel.Start()

	for i := 0; i < defaultWorkerNum; i++ {
		s.wg.Add(1)
		go s.worker()
	}

	s.store.StartFlushLoop(s.flushCtx, storeFlushInterval, &storeFlusher{dao: s.jobDAO})

	s.running = true
	logger.Info("scheduler started (time-wheel + in-memory store)",
		zap.String("nodeID", s.nodeID),
		zap.Int64("jobsLoaded", s.store.Len()))
	return nil
}

// Stop gracefully shuts down the scheduler.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return
	}
	s.running = false
	s.mu.Unlock()

	s.wheel.Stop()
	s.flushStop()
	close(s.stopCh)
	s.wg.Wait()
	logger.Info("scheduler stopped")
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

func (s *Scheduler) bootstrap() error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// ── 1. 加载任务到内存 jobstore ──────────────────────────────────────
	status := model.JobRun
	query := &dao.JobInfoQuery{Status: &status, Page: 1, PageSize: bootstrapBatchSize}
	for {
		jobs, _, err := s.jobDAO.List(ctx, query)
		if err != nil {
			return err
		}
		s.store.LoadAll(jobs)
		if len(jobs) < bootstrapBatchSize {
			break
		}
		query.Page++
	}

	// ── 2. 加载执行器到内存 executorstore（单次 DB 查询，后续无 DB）
	if s.executorStore != nil {
		executors, err := s.executorDAO.ListOnline(ctx)
		if err != nil {
			logger.Warn("scheduler: bootstrap executor store failed, fallback to DB routing",
				zap.Error(err))
		} else {
			s.executorStore.Bootstrap(executors)
		}
	}

	return nil
}

// ─── Time-wheel registration ──────────────────────────────────────────────────

func (s *Scheduler) scheduleWheel(job *model.JobInfo) {
	jobID := job.ID
	fireAt := *job.NextTriggerTime

	taskID := fmt.Sprintf("job:%d", jobID)
	s.wheel.Add(taskID, fireAt, func() {
		latest, ok := s.store.Get(jobID)
		if !ok || latest.Status != model.JobRun {
			return
		}
		select {
		case s.workerCh <- &triggerTask{
			job:         latest,
			triggerTime: fireAt,
			triggerType: model.TriggerCron,
		}:
		default:
			logger.Warn("scheduler: worker channel full, dropping trigger",
				zap.Int64("jobID", jobID))
		}
	})
}

// ─── Job management ───────────────────────────────────────────────────────────

// AddJob inserts a job into the in-memory store and schedules it.
func (s *Scheduler) AddJob(job *model.JobInfo) {
	s.store.Add(job)
	if job.Status == model.JobRun && job.NextTriggerTime != nil {
		s.scheduleWheel(job)
	}
}

// UpdateJob replaces a job's configuration and re-registers it in the wheel.
func (s *Scheduler) UpdateJob(job *model.JobInfo) {
	s.store.Add(job)
	s.wheel.Cancel(fmt.Sprintf("job:%d", job.ID))
	if job.Status == model.JobRun && job.NextTriggerTime != nil {
		s.scheduleWheel(job)
	}
}

// RemoveJob removes a job from the store and cancels its wheel entry.
func (s *Scheduler) RemoveJob(id int64) {
	s.store.Remove(id)
	s.wheel.Cancel(fmt.Sprintf("job:%d", id))
}

// StartJob enables a job and registers it in the wheel.
func (s *Scheduler) StartJob(ctx context.Context, id int64) error {
	job, ok := s.store.Get(id)
	if !ok {
		var err error
		job, err = s.jobDAO.FindByID(ctx, id)
		if err != nil {
			return fmt.Errorf("scheduler: job %d not found: %w", id, err)
		}
		s.store.Add(job)
	}
	if job.NextTriggerTime == nil && job.CronExpression != "" {
		next, err := s.CalcNextTriggerTime(job.CronExpression)
		if err != nil {
			return err
		}
		job.NextTriggerTime = &next
		_ = s.store.UpdateNextTrigger(id, next, time.Now())
	}
	if err := s.store.UpdateStatus(id, model.JobRun); err != nil {
		return err
	}
	if job, ok = s.store.Get(id); ok && job.NextTriggerTime != nil {
		s.scheduleWheel(job)
	}
	return nil
}

// StopJob disables a job and cancels its wheel entry.
func (s *Scheduler) StopJob(id int64) error {
	if err := s.store.UpdateStatus(id, model.JobStop); err != nil {
		return err
	}
	s.wheel.Cancel(fmt.Sprintf("job:%d", id))
	return nil
}

// TriggerJob manually fires a job once.
func (s *Scheduler) TriggerJob(ctx context.Context, jobID int64, param string) error {
	job, ok := s.store.Get(jobID)
	if !ok {
		var err error
		job, err = s.jobDAO.FindByID(ctx, jobID)
		if err != nil {
			return fmt.Errorf("scheduler: job %d not found: %w", jobID, err)
		}
	}
	if param != "" {
		job.ExecuteParam = param
	}
	select {
	case s.workerCh <- &triggerTask{job: job, triggerTime: time.Now(), triggerType: model.TriggerManual}:
	default:
		return fmt.Errorf("scheduler: worker channel full")
	}
	return nil
}

// ─── Worker pool ──────────────────────────────────────────────────────────────

type triggerTask struct {
	job         *model.JobInfo
	triggerTime time.Time
	triggerType model.TriggerType
}

func (s *Scheduler) worker() {
	defer s.wg.Done()
	for {
		select {
		case <-s.stopCh:
			return
		case task := <-s.workerCh:
			s.handleTask(task)
		}
	}
}

func (s *Scheduler) handleTask(task *triggerTask) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	job := task.job
	lockKey := fmt.Sprintf("go-jobs:lock:job:%d", job.ID)
	lockVal := s.nodeID + ":" + uuid.New().String()

	// redis 为 nil 时跳过分布式锁（仅用于单元测试）
	if s.redis != nil {
		locked, err := s.redis.TryLock(ctx, lockKey, lockVal, lockTTL)
		if err != nil {
			logger.Error("scheduler: acquire lock error", zap.Int64("jobID", job.ID), zap.Error(err))
			return
		}
		if !locked {
			logger.Debug("scheduler: job already locked, skipping", zap.Int64("jobID", job.ID))
			return
		}
		defer func() { _ = s.redis.ReleaseLock(ctx, lockKey, lockVal) }()
	}

	// ── 限流检查（令牌桶 + 窗口配额）────────────────────────────────────
	if s.rateLimiter != nil {
		if err := s.rateLimiter.CheckJob(job.ExecutorApp, job.ID); err != nil {
			logger.Warn("scheduler: job rate limited, skipping trigger",
				zap.Int64("jobID", job.ID),
				zap.String("app", job.ExecutorApp),
				zap.Error(err))
			s.reschedule(ctx, job)
			return
		}
	}

	if !s.applyBlockStrategy(ctx, job) {
		s.reschedule(ctx, job)
		return
	}

	// ── 从内存注册表获取执行器地址（零 DB 查询）────────────────────────────
	// Failover 路由需要健康地址列表；其他策略使用在线地址列表。
	var addresses []string
	if job.RouteStrategy == model.RouteFailover {
		if s.executorStore != nil {
			addresses = s.executorStore.ListHealthyAddresses(job.ExecutorApp)
		}
	} else {
		if s.executorStore != nil {
			addresses = s.executorStore.ListOnlineAddresses(job.ExecutorApp)
		}
	}

	// 内存注册表未初始化或地址为空时，回落到 DB 查询（兼容旧部署）
	if len(addresses) == 0 && s.executorStore == nil {
		executors, err := s.executorDAO.ListByApp(ctx, job.ExecutorApp)
		if err == nil {
			addresses = make([]string, 0, len(executors))
			for _, e := range executors {
				addresses = append(addresses, e.Address)
			}
		}
	}

	if len(addresses) == 0 {
		logger.Warn("scheduler: no online executor",
			zap.String("app", job.ExecutorApp), zap.Int64("jobID", job.ID))
		s.recordTriggerFail(ctx, job, task, "no online executor")
		s.reschedule(ctx, job)
		return
	}

	if job.RouteStrategy == model.RouteShardingBroadcast {
		for idx, addr := range addresses {
			s.sendTrigger(ctx, job, addr, task, idx, len(addresses))
		}
	} else {
		router := NewRouter(job.RouteStrategy)
		addr, routeErr := router.Route(addresses, job.ID, job.ExecuteParam)
		if routeErr != nil {
			logger.Warn("scheduler: route failed", zap.Int64("jobID", job.ID), zap.Error(routeErr))
			s.recordTriggerFail(ctx, job, task, routeErr.Error())
			s.reschedule(ctx, job)
			return
		}
		s.sendTrigger(ctx, job, addr, task, 0, 1)
	}

	s.reschedule(ctx, job)
}

// reschedule computes the next trigger time and schedules the next wheel event.
func (s *Scheduler) reschedule(ctx context.Context, job *model.JobInfo) {
	if job.CronExpression == "" {
		_ = s.store.UpdateStatus(job.ID, model.JobStop)
		return
	}

	sched, err := s.cronParser.Parse(job.CronExpression)
	if err != nil {
		logger.Error("scheduler: invalid cron", zap.Int64("jobID", job.ID), zap.Error(err))
		return
	}
	now := time.Now()
	next := sched.Next(now)
	last := now
	if job.NextTriggerTime != nil {
		last = *job.NextTriggerTime
	}

	_ = s.store.UpdateNextTrigger(job.ID, next, last)

	if fresh, ok := s.store.Get(job.ID); ok && fresh.Status == model.JobRun {
		s.scheduleWheel(fresh)
	}
}

// ─── HTTP dispatch (unchanged logic, same as v1) ─────────────────────────────

func (s *Scheduler) sendTrigger(
	ctx context.Context,
	job *model.JobInfo,
	address string,
	task *triggerTask,
	shardIdx, shardTotal int,
) {
	// ── 参数模板渲染：将 {{.Date}} 等占位符替换为触发时刻的真实值 ────────
	executeParam := job.ExecuteParam
	if s.paramEngine != nil {
		vars := s.paramEngine.BuildVars(paramtpl.TriggerContext{
			JobID:       job.ID,
			ShardIndex:  shardIdx,
			ShardTotal:  shardTotal,
			TriggerType: string(task.triggerType),
		})
		if rendered, err := s.paramEngine.Render(job.ExecuteParam, vars); err == nil {
			executeParam = rendered
		} else {
			logger.Warn("scheduler: param template render failed, using raw param",
				zap.Int64("jobID", job.ID), zap.Error(err))
		}
	}

	logRec := &model.JobLog{
		JobID:           job.ID,
		ExecutorID:      job.ExecutorID,
		ExecutorAddress: address,
		ExecuteParam:    executeParam,
		Status:          model.LogRunning,
		ShardingIndex:   shardIdx,
		ShardingTotal:   shardTotal,
		TriggerTime:     task.triggerTime,
		TriggerType:     task.triggerType,
	}
	if err := s.logDAO.Create(ctx, logRec); err != nil {
		logger.Error("scheduler: create job log", zap.Int64("jobID", job.ID), zap.Error(err))
		return
	}
	go s.callExecutor(job, logRec, address)
}

func (s *Scheduler) callExecutor(job *model.JobInfo, logRec *model.JobLog, address string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	trigger := &ExecutorTrigger{
		LogID:           logRec.ID,
		JobID:           job.ID,
		ExecutorHandler: job.ExecuteHandler,
		ExecuteType:     string(job.ExecuteType),
		ExecuteParam:    logRec.ExecuteParam, // 使用已渲染的参数（含模板变量替换）
		ShardingIndex:   logRec.ShardingIndex,
		ShardingTotal:   logRec.ShardingTotal,
		Timeout:         job.Timeout,
	}

	client := NewExecutorClient(address)
	start := time.Now()
	err := client.Run(ctx, trigger)
	end := time.Now()

	status := model.LogSuccess
	errMsg := ""
	if err != nil {
		status = model.LogFail
		errMsg = err.Error()
		logger.Warn("scheduler: executor call failed",
			zap.Int64("jobID", job.ID),
			zap.String("address", address),
			zap.Error(err))
	}

	updCtx, updCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer updCancel()
	_ = s.logDAO.UpdateResult(updCtx, logRec.ID, status, errMsg, start, end)
}

func (s *Scheduler) recordTriggerFail(ctx context.Context, job *model.JobInfo, task *triggerTask, reason string) {
	now := time.Now()
	failLog := &model.JobLog{
		JobID:       job.ID,
		ExecutorID:  job.ExecutorID,
		Status:      model.LogFail,
		ErrorMsg:    reason,
		TriggerTime: task.triggerTime,
		TriggerType: task.triggerType,
		StartTime:   &now,
		EndTime:     &now,
	}
	if err := s.logDAO.Create(ctx, failLog); err != nil {
		logger.Error("scheduler: record trigger fail", zap.Error(err))
	}
}

// ─── Cron helpers ─────────────────────────────────────────────────────────────

// CalcNextTriggerTime computes the next fire time for a cron expression.
func (s *Scheduler) CalcNextTriggerTime(cronExpr string) (time.Time, error) {
	sched, err := s.cronParser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}
	return sched.Next(time.Now()), nil
}

// ─── Observability ────────────────────────────────────────────────────────────

// StoreStats holds diagnostic counters from the in-memory store and wheel.
type StoreStats struct {
	JobCount      int64
	HeapLen       int
	WheelIndex    int
	WheelOverflow int
}

// Stats returns a snapshot for health-check endpoints.
func (s *Scheduler) Stats() StoreStats {
	ws := s.wheel.Stats(context.Background())
	return StoreStats{
		JobCount:      s.store.Len(),
		HeapLen:       s.store.HeapLen(),
		WheelIndex:    ws.IndexSize,
		WheelOverflow: ws.OverflowSize,
	}
}

// ─── zapLogger bridges pkg/logger to jobstore.Logger ─────────────────────────

type zapLogger struct{}

func (z *zapLogger) Info(msg string, fields ...zap.Field)  { logger.Info(msg, fields...) }
func (z *zapLogger) Warn(msg string, fields ...zap.Field)  { logger.Warn(msg, fields...) }
func (z *zapLogger) Error(msg string, fields ...zap.Field) { logger.Error(msg, fields...) }
