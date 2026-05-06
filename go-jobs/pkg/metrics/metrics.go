package metrics

import (
	"net/http"
	"time"
)

// ─── Metrics（Facade Pattern）─────────────────────────────────────────────────
//
// Metrics 是指标收集的统一门面，业务代码的唯一入口。
//
// 职责：
//  1. 持有 Provider，转发所有埋点调用（完全不感知底层实现）
//  2. 提供语义化方法（RecordTrigger / RecordRun / …），
//     比直接操作 Counter/Gauge 更安全、更易读
//  3. 统一管理标签约定，防止各模块标签不一致
//  4. 代理 Handler() / Stop() 到底层 Provider
//
// 热路径设计：
//   RecordTrigger / RecordRun 等方法每次调用都通过 Provider.Counter() 查找
//   指标实例。Provider 内部使用 RWMutex + map 缓存，RLock 路径开销约 10-50ns，
//   在任务触发频率 < 10k QPS 时可忽略不计。
//   若需要极低延迟，可在业务侧缓存 Counter/Histogram 实例。
type Metrics struct {
	provider Provider

	// ── 预构建的核心指标（仅用于不带动态标签的 gauge/histogram）────────

	// jobActive：当前调度器维护的运行中 job 数（gauge，无标签）
	jobActive Gauge
	// wheelSize：时间轮事件数（gauge，无标签）
	wheelSize Gauge
	// triggerDuration：触发耗时分布（histogram，无动态标签）
	triggerDuration Histogram
	// runDuration：执行耗时分布（histogram，无动态标签）
	runDuration Histogram
	// runningJobs：执行器当前运行中 job 数（gauge，无标签）
	runningJobs Gauge
}

// New 创建 Metrics 门面并预构建无动态标签的核心指标。
// provider 不能为 nil；禁用 metrics 请传入 NewProvider(TypeNoop) 的结果。
func New(provider Provider) *Metrics {
	m := &Metrics{provider: provider}
	// 预构建无动态标签的指标，避免每次访问都走 map 查找
	m.jobActive = provider.Gauge(MetricJobActive)
	m.wheelSize = provider.Gauge(MetricWheelSize)
	m.triggerDuration = provider.Histogram(MetricTriggerDurationMs, nil)
	m.runDuration = provider.Histogram(MetricRunDurationMs, nil)
	m.runningJobs = provider.Gauge(MetricRunningJobs)
	return m
}

// ─── 调度器埋点 ───────────────────────────────────────────────────────────────

// RecordTrigger 记录一次任务触发。
//
//   - app:         executor app name（如 "email-sender"）
//   - triggerType: "cron" | "manual" | "retry" | "child"
//   - status:      "success" | "fail" | "rate_limited" | "no_executor"
//   - duration:    从开始触发到 executor 接收的耗时
func (m *Metrics) RecordTrigger(app, triggerType, status string, duration time.Duration) {
	m.provider.Counter(MetricTriggerTotal,
		"app", app,
		"trigger_type", triggerType,
		"status", status,
	).Inc()
	m.triggerDuration.Observe(float64(duration.Milliseconds()))
}

// SetActiveJobs 设置调度器维护的活跃 job 总数。
func (m *Metrics) SetActiveJobs(n int64) { m.jobActive.Set(float64(n)) }

// IncActiveJobs 递增活跃 job 数（job 状态变为 Running 时调用）。
func (m *Metrics) IncActiveJobs() { m.jobActive.Inc() }

// DecActiveJobs 递减活跃 job 数（job 状态变为 Stopped 时调用）。
func (m *Metrics) DecActiveJobs() { m.jobActive.Dec() }

// SetWheelSize 设置时间轮当前事件数（由 SchedulerObserver 定时采集）。
func (m *Metrics) SetWheelSize(n int) { m.wheelSize.Set(float64(n)) }

// IncRateLimited 记录一次限流拒绝。
//
//   - app:    executor app name
//   - reason: "rate_exceeded" | "quota_exceeded"
func (m *Metrics) IncRateLimited(app, reason string) {
	m.provider.Counter(MetricRateLimitedTotal, "app", app, "reason", reason).Inc()
}

// IncNoExecutor 记录一次「无可用执行器」事件。
func (m *Metrics) IncNoExecutor(app string) {
	m.provider.Counter(MetricNoExecutorTotal, "app", app).Inc()
}

// ─── 执行器埋点 ───────────────────────────────────────────────────────────────

// RecordRun 记录一次任务执行结果。
//
//   - handler:  executor handler name（如 "processOrders"）
//   - status:   "success" | "fail" | "timeout" | "killed"
//   - duration: 从开始执行到结束的耗时
func (m *Metrics) RecordRun(handler, status string, duration time.Duration) {
	m.provider.Counter(MetricRunTotal, "handler", handler, "status", status).Inc()
	m.runDuration.Observe(float64(duration.Milliseconds()))
}

// SetRunningJobs 设置执行器当前正在运行的 job 数。
func (m *Metrics) SetRunningJobs(n int) { m.runningJobs.Set(float64(n)) }

// IncRunningJobs 递增运行中 job 数（job 开始执行时调用）。
func (m *Metrics) IncRunningJobs() { m.runningJobs.Inc() }

// DecRunningJobs 递减运行中 job 数（job 完成/超时/被杀时调用）。
func (m *Metrics) DecRunningJobs() { m.runningJobs.Dec() }

// IncIdempotentRejected 记录一次幂等拒绝（相同 LogID 重复执行被拦截）。
func (m *Metrics) IncIdempotentRejected(handler string) {
	m.provider.Counter(MetricIdempotentReject, "handler", handler).Inc()
}

// ─── 限流埋点 ─────────────────────────────────────────────────────────────────

// IncRateLimitAllowed 记录一次限流器放行。
//
//   - key: 限流器 key（如 "app:email-sender" 或 "job:42"）
func (m *Metrics) IncRateLimitAllowed(key string) {
	m.provider.Counter(MetricRateLimitAllowed, "key", key).Inc()
}

// IncRateLimitRejected 记录一次限流器拒绝。
//
//   - key:    限流器 key
//   - reason: "rate_exceeded" | "quota_exceeded"
func (m *Metrics) IncRateLimitRejected(key, reason string) {
	m.provider.Counter(MetricRateLimitRejected, "key", key, "reason", reason).Inc()
}

// ─── 代理方法 ─────────────────────────────────────────────────────────────────

// Handler 返回 /metrics HTTP handler，注册到路由即可暴露指标。
//
//	r.GET("/metrics", gin.WrapH(m.Handler()))
func (m *Metrics) Handler() http.Handler { return m.provider.Handler() }

// Stop 优雅停止底层 Provider（VictoriaMetrics push 模式需要 flush）。
func (m *Metrics) Stop() { m.provider.Stop() }

// Provider 返回底层 Provider（高级场景：需要直接操作 Provider）。
func (m *Metrics) Provider() Provider { return m.provider }

// ─── 全局默认实例（Null Object Singleton）────────────────────────────────────

// globalInstance 是进程级全局 Metrics 实例，默认为 Noop（零开销）。
// 组件默认使用 Global()，无需强依赖注入即可工作。
// main 初始化阶段调用 SetGlobal(metrics.New(provider)) 替换为真实实例。
var globalInstance = New(newNoopProvider())

// Global 返回全局 Metrics 实例（默认为 Noop）。
func Global() *Metrics { return globalInstance }

// SetGlobal 替换全局实例。应在 main 初始化阶段调用，非线程安全。
func SetGlobal(m *Metrics) { globalInstance = m }
