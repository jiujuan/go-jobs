package metrics

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// ─── SchedulerObserver（Decorator Pattern + Observer Pattern）────────────────
//
// SchedulerObserver 为调度器提供非侵入式指标埋点。
//
// 设计原则（开闭原则 OCP）：
//   - 不修改 internal/scheduler/Scheduler 的任何代码
//   - 通过 Notify* 回调在调度器关键路径上注入埋点
//   - Notify* 方法全部非阻塞（channel 满时丢弃），绝不拖慢调度主路径
//
// 两类后台任务：
//  1. collectLoop：定时调用 statsFunc 采集 job 数量、时间轮大小等 Gauge 指标
//  2. eventLoop：消费 triggerCh / rateLimitedCh，将事件转换为 Counter/Histogram 调用
//
// 接入方式（main.go）：
//
//	obs := metrics.NewSchedulerObserver(
//	    func() metrics.SchedulerStats {
//	        s := sched.Stats()
//	        return metrics.SchedulerStats{JobCount: s.JobCount, WheelSize: s.WheelIndex}
//	    },
//	    m,
//	)
//	obs.Start(ctx)
//	defer obs.Stop()
//
//	// 在 scheduler.handleTask 关键路径上回调（建议通过 Option 注入）：
//	obs.NotifyTrigger(metrics.TriggerEvent{App: job.ExecutorApp, ...})

// SchedulerStats 是调度器上报给 SchedulerObserver 的瞬时统计快照。
// 对应 scheduler.Scheduler.Stats() 的返回值。
type SchedulerStats struct {
	JobCount      int64 // 当前运行中 job 总数
	WheelSize     int   // 时间轮当前事件数（WheelIndex + WheelOverflow）
	WheelOverflow int   // 时间轮溢出队列大小
}

// TriggerEvent 描述一次任务触发事件。
// 调度器在每次 handleTask 完成后，通过 SchedulerObserver.NotifyTrigger 上报。
type TriggerEvent struct {
	App         string        // executor app name
	JobID       int64         // 任务 ID（保留，可用于高基数标签场景）
	TriggerType string        // "cron" | "manual" | "retry" | "child"
	Status      string        // "success" | "fail" | "rate_limited" | "no_executor"
	Duration    time.Duration // 从触发开始到 executor 接收的耗时
}

type rateLimitEvt struct {
	App    string
	Reason string
}

// SchedulerObserverOption 是 SchedulerObserver 的函数式选项。
type SchedulerObserverOption func(*SchedulerObserver)

// WithCollectInterval 设置定时采集 Gauge 的间隔（默认 10s）。
func WithCollectInterval(d time.Duration) SchedulerObserverOption {
	return func(o *SchedulerObserver) { o.collectInterval = d }
}

// WithTriggerChanSize 设置触发事件 channel 容量（默认 2048）。
func WithTriggerChanSize(n int) SchedulerObserverOption {
	return func(o *SchedulerObserver) { o.triggerChanSize = n }
}

// SchedulerObserver 装饰调度器，无侵入地收集指标。
type SchedulerObserver struct {
	metrics         *Metrics
	statsFunc       func() SchedulerStats
	collectInterval time.Duration
	triggerChanSize int

	triggerCh     chan TriggerEvent
	rateLimitedCh chan rateLimitEvt

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewSchedulerObserver 创建调度器指标观察者。
//
//   - statsFunc: 每次定时采集时调用，返回调度器实时统计。
//   - m:         Metrics 门面。
func NewSchedulerObserver(statsFunc func() SchedulerStats, m *Metrics, opts ...SchedulerObserverOption) *SchedulerObserver {
	obs := &SchedulerObserver{
		metrics:         m,
		statsFunc:       statsFunc,
		collectInterval: 10 * time.Second,
		triggerChanSize: 2048,
	}
	for _, opt := range opts {
		opt(obs)
	}
	obs.triggerCh = make(chan TriggerEvent, obs.triggerChanSize)
	obs.rateLimitedCh = make(chan rateLimitEvt, 512)
	return obs
}

// Start 启动后台采集和事件处理 goroutine。幂等：重复调用安全。
func (obs *SchedulerObserver) Start(_ context.Context) {
	obs.mu.Lock()
	defer obs.mu.Unlock()
	if obs.running {
		return
	}
	obs.running = true
	obs.stopCh = make(chan struct{})
	obs.wg.Add(2)
	go obs.collectLoop()
	go obs.eventLoop()
}

// Stop 停止后台 goroutine，drain 剩余事件后返回。
func (obs *SchedulerObserver) Stop() {
	obs.mu.Lock()
	if !obs.running {
		obs.mu.Unlock()
		return
	}
	close(obs.stopCh)
	obs.running = false
	obs.mu.Unlock()
	obs.wg.Wait()
}

// NotifyTrigger 上报一次触发事件（非阻塞，channel 满时丢弃）。
// 由调度器在 handleTask 完成后调用。
func (obs *SchedulerObserver) NotifyTrigger(ev TriggerEvent) {
	select {
	case obs.triggerCh <- ev:
	default: // 丢弃，绝不阻塞调度器主路径
	}
}

// NotifyRateLimited 上报一次限流拒绝（非阻塞）。
func (obs *SchedulerObserver) NotifyRateLimited(app, reason string) {
	select {
	case obs.rateLimitedCh <- rateLimitEvt{App: app, Reason: reason}:
	default:
	}
}

// collectLoop 定时采集调度器统计 Gauge（jobActive、wheelSize）。
func (obs *SchedulerObserver) collectLoop() {
	defer obs.wg.Done()
	ticker := time.NewTicker(obs.collectInterval)
	defer ticker.Stop()
	for {
		select {
		case <-obs.stopCh:
			return
		case <-ticker.C:
			stats := obs.statsFunc()
			obs.metrics.SetActiveJobs(stats.JobCount)
			obs.metrics.SetWheelSize(stats.WheelSize + stats.WheelOverflow)
		}
	}
}

// eventLoop 消费触发和限流事件，转换为 Metrics 调用。
// Stop 时 drain 已缓冲的事件，保证数据不丢失。
func (obs *SchedulerObserver) eventLoop() {
	defer obs.wg.Done()
	for {
		select {
		case <-obs.stopCh:
			obs.drainAll()
			return
		case ev := <-obs.triggerCh:
			obs.metrics.RecordTrigger(ev.App, ev.TriggerType, ev.Status, ev.Duration)
		case ev := <-obs.rateLimitedCh:
			obs.metrics.IncRateLimited(ev.App, ev.Reason)
		}
	}
}

// drainAll 在退出前处理 channel 中剩余事件，保证数据完整性。
func (obs *SchedulerObserver) drainAll() {
	for {
		select {
		case ev := <-obs.triggerCh:
			obs.metrics.RecordTrigger(ev.App, ev.TriggerType, ev.Status, ev.Duration)
		case ev := <-obs.rateLimitedCh:
			obs.metrics.IncRateLimited(ev.App, ev.Reason)
		default:
			return
		}
	}
}

// ─── ExecutorObserver（Decorator Pattern）────────────────────────────────────
//
// ExecutorObserver 为执行器提供非侵入式指标埋点。
//
// 接入方式：
//
//	eo := metrics.NewExecutorObserver(m)
//	eo.Start()
//	defer eo.Stop()
//
//	// 在 Runner.Run 前后调用：
//	eo.NotifyRunStart()
//	defer eo.NotifyRunFinish(metrics.RunEvent{Handler: req.ExecutorHandler, Status: "success", Duration: elapsed})

// RunEvent 描述一次任务执行完成事件。
type RunEvent struct {
	Handler  string        // executor handler name
	Status   string        // "success" | "fail" | "timeout" | "killed"
	Duration time.Duration // 执行耗时
}

// ExecutorObserver 装饰执行器，无侵入地收集执行器侧指标。
type ExecutorObserver struct {
	metrics *Metrics
	eventCh chan RunEvent

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
	wg      sync.WaitGroup
}

// NewExecutorObserver 创建执行器指标观察者。
func NewExecutorObserver(m *Metrics) *ExecutorObserver {
	return &ExecutorObserver{
		metrics: m,
		eventCh: make(chan RunEvent, 4096),
		stopCh:  make(chan struct{}),
	}
}

// Start 启动后台事件处理 goroutine。幂等。
func (eo *ExecutorObserver) Start() {
	eo.mu.Lock()
	defer eo.mu.Unlock()
	if eo.running {
		return
	}
	eo.running = true
	eo.stopCh = make(chan struct{})
	eo.wg.Add(1)
	go eo.eventLoop()
}

// Stop 停止后台 goroutine，drain 剩余事件。
func (eo *ExecutorObserver) Stop() {
	eo.mu.Lock()
	if !eo.running {
		eo.mu.Unlock()
		return
	}
	close(eo.stopCh)
	eo.running = false
	eo.mu.Unlock()
	eo.wg.Wait()
}

// NotifyRunStart 通知 job 开始执行（同步递增 runningJobs，立即反映）。
func (eo *ExecutorObserver) NotifyRunStart() {
	eo.metrics.IncRunningJobs()
}

// NotifyRunFinish 通知 job 执行完成（同步递减 runningJobs，异步记录 run_total / duration）。
func (eo *ExecutorObserver) NotifyRunFinish(ev RunEvent) {
	eo.metrics.DecRunningJobs()
	select {
	case eo.eventCh <- ev:
	default: // 丢弃，不阻塞执行器主路径
	}
}

// NotifyIdempotentRejected 通知幂等拒绝（同步，低频调用）。
func (eo *ExecutorObserver) NotifyIdempotentRejected(handler string) {
	eo.metrics.IncIdempotentRejected(handler)
}

func (eo *ExecutorObserver) eventLoop() {
	defer eo.wg.Done()
	for {
		select {
		case <-eo.stopCh:
			for {
				select {
				case ev := <-eo.eventCh:
					eo.metrics.RecordRun(ev.Handler, ev.Status, ev.Duration)
				default:
					return
				}
			}
		case ev := <-eo.eventCh:
			eo.metrics.RecordRun(ev.Handler, ev.Status, ev.Duration)
		}
	}
}

// ─── 工具函数 ─────────────────────────────────────────────────────────────────

// FormatJobID 将 int64 job ID 格式化为标签字符串。
// 统一调用点，防止各处散落 strconv.FormatInt。
func FormatJobID(id int64) string { return fmt.Sprintf("%d", id) }
