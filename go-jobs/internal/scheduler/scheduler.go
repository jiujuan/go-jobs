// Package scheduler contains the core scheduling engine.
// It pre-fetches jobs that are due within a short lookahead window,
// then fires them in a dedicated goroutine pool while holding a
// distributed lock (Redis or DB fallback) to prevent duplicate execution.
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
	"github.com/jiujuan/go-jobs/pkg/logger"
	redispkg "github.com/jiujuan/go-jobs/pkg/redis"
)

const (
	defaultPreloadWindow = 5 * time.Second
	defaultWorkerNum     = 64
	tickInterval         = time.Second
	lockTTL              = 30 * time.Second
)

// Scheduler is the central scheduling engine.
// It periodically scans the database for due jobs and dispatches triggers.
type Scheduler struct {
	jobDAO      dao.JobInfoDAO
	logDAO      dao.JobLogDAO
	executorDAO dao.ExecutorDAO

	redis  *redispkg.Client
	nodeID string // ip:port of this scheduler instance

	preloadWindow time.Duration
	workerCh      chan *triggerTask
	stopCh        chan struct{}
	wg            sync.WaitGroup

	// cronParser is used to compute next fire time.
	cronParser cron.Parser

	mu      sync.Mutex
	running bool
}

// Options configures the Scheduler.
type Options struct {
	PreloadWindow time.Duration
	WorkerNum     int
	NodeID        string
}

// Option is a functional option for Scheduler.
type Option func(*Options)

func defaultOpts() *Options {
	return &Options{
		PreloadWindow: defaultPreloadWindow,
		WorkerNum:     defaultWorkerNum,
	}
}

// WithPreloadWindow overrides how far ahead triggers are pre-loaded.
func WithPreloadWindow(d time.Duration) Option {
	return func(o *Options) { o.PreloadWindow = d }
}

// WithWorkerNum sets the trigger dispatch worker pool size.
func WithWorkerNum(n int) Option { return func(o *Options) { o.WorkerNum = n } }

// WithNodeID sets an explicit node identifier (default: ip:port).
func WithNodeID(id string) Option { return func(o *Options) { o.NodeID = id } }

// New creates a new Scheduler.
func New(
	jobDAO dao.JobInfoDAO,
	logDAO dao.JobLogDAO,
	executorDAO dao.ExecutorDAO,
	redis *redispkg.Client,
	opts ...Option,
) *Scheduler {
	o := defaultOpts()
	for _, opt := range opts {
		opt(o)
	}
	if o.NodeID == "" {
		o.NodeID = uuid.New().String()
	}
	return &Scheduler{
		jobDAO:        jobDAO,
		logDAO:        logDAO,
		executorDAO:   executorDAO,
		redis:         redis,
		nodeID:        o.NodeID,
		preloadWindow: o.PreloadWindow,
		workerCh:      make(chan *triggerTask, o.WorkerNum*4),
		stopCh:        make(chan struct{}),
		cronParser:    cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow),
	}
}

// Start launches the scheduling loop.  Safe to call only once.
func (s *Scheduler) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		return fmt.Errorf("scheduler: already running")
	}
	s.running = true

	// Launch worker pool.
	for i := 0; i < defaultWorkerNum; i++ {
		s.wg.Add(1)
		go s.worker()
	}

	// Launch main scheduling loop.
	s.wg.Add(1)
	go s.loop()

	logger.Info("scheduler started", zap.String("nodeID", s.nodeID))
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

	close(s.stopCh)
	s.wg.Wait()
	logger.Info("scheduler stopped")
}

// ─── Main loop ────────────────────────────────────────────────────────────────

func (s *Scheduler) loop() {
	defer s.wg.Done()
	ticker := time.NewTicker(tickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.schedule()
		}
	}
}

func (s *Scheduler) schedule() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	maxTime := time.Now().Add(s.preloadWindow)
	jobs, err := s.jobDAO.ListPendingJobs(ctx, maxTime, 1000)
	if err != nil {
		logger.Error("scheduler: list pending jobs", zap.Error(err))
		return
	}

	for _, job := range jobs {
		s.dispatchJob(ctx, job)
	}
}

func (s *Scheduler) dispatchJob(ctx context.Context, job *model.JobInfo) {
	// Compute the scheduled fire time: it might be in the near future.
	// Handle misfire before dispatching.
	if s.handleMisfire(ctx, job) {
		return
	}

	fireTime := time.Now()
	if job.NextTriggerTime != nil && job.NextTriggerTime.After(fireTime) {
		// Sleep until fire time (short duration within preload window).
		delay := time.Until(*job.NextTriggerTime)
		if delay > 0 {
			time.AfterFunc(delay, func() {
				s.enqueue(job, *job.NextTriggerTime, model.TriggerCron)
			})
		} else {
			s.enqueue(job, *job.NextTriggerTime, model.TriggerCron)
		}
	} else {
		s.enqueue(job, fireTime, model.TriggerCron)
	}

	// Immediately advance next_trigger_time so the job is not picked up again.
	if err := s.advanceNextTrigger(ctx, job); err != nil {
		logger.Error("scheduler: advance next trigger", zap.Int64("jobID", job.ID), zap.Error(err))
	}
}

// enqueue pushes a trigger task into the worker channel.
func (s *Scheduler) enqueue(job *model.JobInfo, triggerTime time.Time, triggerType model.TriggerType) {
	select {
	case s.workerCh <- &triggerTask{job: job, triggerTime: triggerTime, triggerType: triggerType}:
	default:
		logger.Warn("scheduler: worker channel full, dropping trigger",
			zap.Int64("jobID", job.ID))
	}
}

// TriggerJob manually fires a job (called from the admin API).
func (s *Scheduler) TriggerJob(ctx context.Context, jobID int64, param string) error {
	job, err := s.jobDAO.FindByID(ctx, jobID)
	if err != nil {
		return fmt.Errorf("scheduler: job %d not found: %w", jobID, err)
	}
	if param != "" {
		job.ExecuteParam = param
	}
	s.enqueue(job, time.Now(), model.TriggerManual)
	return nil
}

// ─── Worker ───────────────────────────────────────────────────────────────────

type triggerTask struct {
	job         *model.JobInfo
	triggerTime time.Time
	triggerType model.TriggerType
	retryCount  int
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

	// Acquire distributed lock to prevent duplicate execution across nodes.
	locked, err := s.redis.TryLock(ctx, lockKey, lockVal, lockTTL)
	if err != nil {
		logger.Error("scheduler: acquire lock error", zap.Int64("jobID", job.ID), zap.Error(err))
		return
	}
	if !locked {
		logger.Debug("scheduler: job already locked by another node, skipping",
			zap.Int64("jobID", job.ID))
		return
	}
	defer func() { _ = s.redis.ReleaseLock(ctx, lockKey, lockVal) }()

	// Apply block strategy before dispatching.
	if !s.applyBlockStrategy(ctx, job) {
		return
	}

	// Select executor(s) for the job.
	executors, err := s.executorDAO.ListByApp(ctx, job.ExecutorApp)
	if err != nil || len(executors) == 0 {
		logger.Warn("scheduler: no online executor",
			zap.String("app", job.ExecutorApp),
			zap.Int64("jobID", job.ID))
		s.recordTriggerFail(ctx, job, task, "no online executor")
		return
	}

	addresses := make([]string, 0, len(executors))
	for _, e := range executors {
		addresses = append(addresses, e.Address)
	}

	// Sharding broadcast: send to all executors.
	if job.RouteStrategy == model.RouteShardingBroadcast {
		for idx, addr := range addresses {
			s.sendTrigger(ctx, job, addr, task, idx, len(addresses))
		}
		return
	}

	// Normal route: pick one executor.
	router := NewRouter(job.RouteStrategy)
	addr, err := router.Route(addresses, job.ID, job.ExecuteParam)
	if err != nil {
		logger.Warn("scheduler: route failed", zap.Int64("jobID", job.ID), zap.Error(err))
		s.recordTriggerFail(ctx, job, task, err.Error())
		return
	}
	s.sendTrigger(ctx, job, addr, task, 0, 1)
}

// sendTrigger creates a log record and dispatches to the executor via HTTP.
func (s *Scheduler) sendTrigger(
	ctx context.Context,
	job *model.JobInfo,
	address string,
	task *triggerTask,
	shardIdx, shardTotal int,
) {
	// 1. Persist the trigger log record (status=running).
	log := &model.JobLog{
		JobID:           job.ID,
		ExecutorID:      job.ExecutorID,
		ExecutorAddress: address,
		ExecuteParam:    job.ExecuteParam,
		Status:          model.LogRunning,
		ShardingIndex:   shardIdx,
		ShardingTotal:   shardTotal,
		TriggerTime:     task.triggerTime,
		TriggerType:     task.triggerType,
	}
	if err := s.logDAO.Create(ctx, log); err != nil {
		logger.Error("scheduler: create job log", zap.Int64("jobID", job.ID), zap.Error(err))
		return
	}

	// 2. Call executor HTTP API asynchronously.
	go s.callExecutor(job, log, address)
}

// callExecutor sends the trigger request to the executor over HTTP.
// Retry logic is also handled here.
func (s *Scheduler) callExecutor(job *model.JobInfo, log *model.JobLog, address string) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	trigger := &ExecutorTrigger{
		LogID:         log.ID,
		JobID:         job.ID,
		ExecutorHandler: job.ExecuteHandler,
		ExecuteType:   string(job.ExecuteType),
		ExecuteParam:  job.ExecuteParam,
		ShardingIndex: log.ShardingIndex,
		ShardingTotal: log.ShardingTotal,
		Timeout:       job.Timeout,
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

	// 3. Update log result.
	updCtx, updCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer updCancel()
	_ = s.logDAO.UpdateResult(updCtx, log.ID, status, errMsg, start, end)
}

// recordTriggerFail persists a failed trigger record when we can't even reach an executor.
func (s *Scheduler) recordTriggerFail(ctx context.Context, job *model.JobInfo, task *triggerTask, reason string) {
	now := time.Now()
	log := &model.JobLog{
		JobID:       job.ID,
		ExecutorID:  job.ExecutorID,
		Status:      model.LogFail,
		ErrorMsg:    reason,
		TriggerTime: task.triggerTime,
		TriggerType: task.triggerType,
		StartTime:   &now,
		EndTime:     &now,
	}
	if err := s.logDAO.Create(ctx, log); err != nil {
		logger.Error("scheduler: record trigger fail", zap.Error(err))
	}
}

// ─── Cron helpers ─────────────────────────────────────────────────────────────

// advanceNextTrigger computes and persists the next fire time of the job.
func (s *Scheduler) advanceNextTrigger(ctx context.Context, job *model.JobInfo) error {
	if job.CronExpression == "" {
		// One-shot or delay jobs: disable after first run.
		return s.jobDAO.UpdateStatus(ctx, job.ID, model.JobStop)
	}

	sched, err := s.cronParser.Parse(job.CronExpression)
	if err != nil {
		return fmt.Errorf("parse cron %q: %w", job.CronExpression, err)
	}
	now := time.Now()
	next := sched.Next(now)
	last := now
	if job.NextTriggerTime != nil {
		last = *job.NextTriggerTime
	}
	return s.jobDAO.UpdateNextTriggerTime(ctx, job.ID, next, last)
}

// CalcNextTriggerTime computes the next fire time for a cron expression (exported for use by services).
func (s *Scheduler) CalcNextTriggerTime(cronExpr string) (time.Time, error) {
	sched, err := s.cronParser.Parse(cronExpr)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid cron expression %q: %w", cronExpr, err)
	}
	return sched.Next(time.Now()), nil
}
