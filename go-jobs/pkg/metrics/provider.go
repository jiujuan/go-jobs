// Package metrics 为 go-jobs 提供可插拔的指标收集层。
//
// # 架构与设计模式
//
//	┌─────────────────────────────────────────────────────────────────┐
//	│ 业务层 scheduler / executor / service / ratelimit               │
//	│   只调用 *Metrics 门面的语义化方法，不感知任何 backend          │
//	└────────────────────────┬────────────────────────────────────────┘
//	                         │ uses  (Facade Pattern)
//	┌────────────────────────▼────────────────────────────────────────┐
//	│  *Metrics  (Facade)                                             │
//	│  持有 Provider，提供 RecordTrigger / RecordRun 等语义方法       │
//	│  预注册所有核心指标对象（初始化期 map 查找，热路径零分配）      │
//	└────────────────────────┬────────────────────────────────────────┘
//	                         │ delegates to  (Strategy Pattern)
//	┌────────────────────────▼────────────────────────────────────────┐
//	│  Provider  (Strategy Interface)                                 │
//	│  ┌──────────────────┐  ┌──────────────────────┐               │
//	│  │PrometheusProvider│  │VictoriaMetricsProvider│               │
//	│  │  (default, pull) │  │ (pull / push 双模式) │               │
//	│  └──────────────────┘  └──────────────────────┘               │
//	│  ┌──────────────────┐                                          │
//	│  │  NoopProvider    │  (Null Object Pattern, testing/disabled) │
//	│  └──────────────────┘                                          │
//	└─────────────────────────────────────────────────────────────────┘
//
// # 快速开始
//
//	// 1. 创建 Provider（默认 Prometheus pull 模式）
//	p, err := metrics.NewProvider(metrics.TypePrometheus,
//	    metrics.WithNamespace("gojobs"),
//	    metrics.WithConstLabels(map[string]string{"env": "prod", "node": nodeID}),
//	)
//
//	// 2. 构建 Metrics 门面
//	m := metrics.New(p)
//
//	// 3. 注册 /metrics 端点
//	router.GET("/metrics", gin.WrapH(m.Handler()))
//
//	// 4. 注入组件
//	obs := metrics.NewSchedulerObserver(schedStatsFunc, m)
//	obs.Start(ctx)
//	defer obs.Stop()
//
// # 切换到 VictoriaMetrics（push 模式）
//
//	p, _ = metrics.NewProvider(metrics.TypeVictoriaMetrics,
//	    metrics.WithNamespace("gojobs"),
//	    metrics.WithPushURL("http://vm:8428/api/v1/import/prometheus"),
//	    metrics.WithPushInterval(15*time.Second),
//	)
//	// 其余业务代码完全不变
//
// # 核心指标（均以 {namespace}_ 为前缀）
//
// 调度器侧：
//
//	scheduler_trigger_total{app,trigger_type,status}    触发次数
//	scheduler_trigger_duration_ms                       触发耗时直方图（ms）
//	scheduler_job_active                                当前运行中 job 数
//	scheduler_rate_limited_total{app,reason}            被限流跳过次数
//	scheduler_no_executor_total{app}                    无执行器可用次数
//	scheduler_wheel_size                                时间轮事件数
//
// 执行器侧：
//
//	executor_run_total{handler,status}                  执行次数
//	executor_run_duration_ms                            执行耗时直方图（ms）
//	executor_running_jobs                               正在执行的 job 数
//	executor_idempotent_rejected_total{handler}         幂等拒绝次数
//
// 限流侧：
//
//	ratelimit_allowed_total{key}                        限流通过次数
//	ratelimit_rejected_total{key,reason}                限流拒绝次数
package metrics

import (
	"net/http"
	"time"
)

// ─── ProviderType ─────────────────────────────────────────────────────────────

// ProviderType 标识具体的 metrics backend。
type ProviderType string

const (
	// TypePrometheus 使用内置轻量 Prometheus text/exposition 格式（默认）。
	// 兼容所有 Prometheus scraper，无外部依赖。
	TypePrometheus ProviderType = "prometheus"

	// TypeVictoriaMetrics 与 Prometheus 使用相同 wire format，
	// 额外支持 Push 模式（后台定时推送到 VM 节点）。
	TypeVictoriaMetrics ProviderType = "victoriametrics"

	// TypeNoop 空实现，所有操作为 no-op，零开销（Null Object Pattern）。
	// 适用于测试环境和不需要 metrics 的部署。
	TypeNoop ProviderType = "noop"
)

// ─── Instrument 接口 ──────────────────────────────────────────────────────────

// Counter 单调递增计数器。线程安全。
type Counter interface {
	Inc()
	Add(delta float64)
}

// Gauge 可任意升降的瞬时值。线程安全。
type Gauge interface {
	Set(value float64)
	Inc()
	Dec()
	Add(delta float64)
}

// Histogram 记录观测值分布（桶式直方图）。线程安全。
type Histogram interface {
	// Observe 记录一个观测值。
	Observe(value float64)
	// ObserveDuration 记录 since 到 now 的耗时（毫秒）。便捷方法。
	ObserveDuration(since time.Time)
}

// ─── Provider 接口（Strategy Pattern）────────────────────────────────────────

// Provider 是 metrics backend 的策略接口。
//
// 标签约定：labels 为 key-value 对的有序切片，长度必须为偶数。
// 示例：Counter("hits", "method", "GET", "status", "200")
//
// 同一 name+labels 组合调用多次将返回同一个 instrument 实例（注册表语义）。
type Provider interface {
	// Counter 返回（或创建）带标签的计数器。
	Counter(name string, labels ...string) Counter

	// Gauge 返回（或创建）带标签的计量器。
	Gauge(name string, labels ...string) Gauge

	// Histogram 返回（或创建）带标签的直方图。
	// buckets 为直方图桶边界（毫秒），nil 使用 Provider 配置的默认值。
	Histogram(name string, buckets []float64, labels ...string) Histogram

	// Handler 返回暴露指标的 HTTP handler（用于 /metrics 端点）。
	Handler() http.Handler

	// Type 返回 Provider 的类型标识。
	Type() ProviderType

	// Stop 优雅停止（VictoriaMetrics push 模式需要 flush 最后一批数据）。
	Stop()
}

// ─── ProviderOptions ──────────────────────────────────────────────────────────

// ProviderOptions 持有创建 Provider 的全部配置。
type ProviderOptions struct {
	// Namespace 指标名称全局前缀，如 "gojobs"。
	// 最终指标名为 {Namespace}_{name}。
	Namespace string

	// Subsystem 二级前缀，如 "scheduler"。
	// 最终指标名为 {Namespace}_{Subsystem}_{name}。
	Subsystem string

	// ConstLabels 所有指标都携带的固定标签，如 {"env":"prod","node":"n1"}。
	ConstLabels map[string]string

	// DefaultBuckets 直方图默认桶边界（毫秒）。nil 使用 DefaultDurationBuckets。
	DefaultBuckets []float64

	// ── VictoriaMetrics Push 专用 ────────────────────────────────────────

	// PushURL VM 写入端点，非空时启用 Push 模式。
	// 例：http://vm-single:8428/api/v1/import/prometheus
	PushURL string

	// PushInterval Push 时间间隔，默认 15s。
	PushInterval time.Duration

	// PushJobName Push 请求携带的 job 标签值，默认 "go-jobs"。
	PushJobName string
}

// ProviderOption 是 ProviderOptions 的函数式选项。
type ProviderOption func(*ProviderOptions)

// WithNamespace 设置指标名称全局前缀。
func WithNamespace(ns string) ProviderOption {
	return func(o *ProviderOptions) { o.Namespace = ns }
}

// WithSubsystem 设置指标名称二级前缀。
func WithSubsystem(sub string) ProviderOption {
	return func(o *ProviderOptions) { o.Subsystem = sub }
}

// WithConstLabels 设置所有指标的固定标签（常量标签）。
func WithConstLabels(labels map[string]string) ProviderOption {
	return func(o *ProviderOptions) { o.ConstLabels = labels }
}

// WithDefaultBuckets 设置直方图默认桶边界（毫秒）。
func WithDefaultBuckets(buckets []float64) ProviderOption {
	return func(o *ProviderOptions) { o.DefaultBuckets = buckets }
}

// WithPushURL 设置 VictoriaMetrics Push 端点。
func WithPushURL(url string) ProviderOption {
	return func(o *ProviderOptions) { o.PushURL = url }
}

// WithPushInterval 设置 VictoriaMetrics Push 间隔。
func WithPushInterval(d time.Duration) ProviderOption {
	return func(o *ProviderOptions) { o.PushInterval = d }
}

// WithPushJobName 设置 Push 请求中的 job 标签值。
func WithPushJobName(name string) ProviderOption {
	return func(o *ProviderOptions) { o.PushJobName = name }
}

func defaultProviderOptions() *ProviderOptions {
	return &ProviderOptions{
		Namespace:      "gojobs",
		DefaultBuckets: DefaultDurationBuckets,
		PushInterval:   15 * time.Second,
		PushJobName:    "go-jobs",
	}
}

// ─── Factory（Factory Method Pattern）────────────────────────────────────────

// NewProvider 根据 typ 创建对应的 Provider 实例（Factory Method）。
//
// 示例：
//
//	// Prometheus（pull 模式，默认）
//	p, err := metrics.NewProvider(metrics.TypePrometheus,
//	    metrics.WithNamespace("gojobs"),
//	    metrics.WithConstLabels(map[string]string{"env": "prod"}),
//	)
//
//	// VictoriaMetrics（push 模式）
//	p, err := metrics.NewProvider(metrics.TypeVictoriaMetrics,
//	    metrics.WithPushURL("http://vm:8428/api/v1/import/prometheus"),
//	    metrics.WithPushInterval(10*time.Second),
//	)
//
//	// 测试用 noop
//	p, err := metrics.NewProvider(metrics.TypeNoop)
func NewProvider(typ ProviderType, opts ...ProviderOption) (Provider, error) {
	o := defaultProviderOptions()
	for _, opt := range opts {
		opt(o)
	}
	switch typ {
	case TypePrometheus:
		return newPrometheusProvider(o)
	case TypeVictoriaMetrics:
		return newVictoriaMetricsProvider(o)
	case TypeNoop, "":
		return newNoopProvider(), nil
	default:
		return nil, &UnknownProviderTypeError{Type: typ}
	}
}

// MustNewProvider 同 NewProvider，失败时 panic（适合 main 启动阶段）。
func MustNewProvider(typ ProviderType, opts ...ProviderOption) Provider {
	p, err := NewProvider(typ, opts...)
	if err != nil {
		panic("metrics: NewProvider: " + err.Error())
	}
	return p
}

// ─── 常量与指标名 ─────────────────────────────────────────────────────────────

// DefaultDurationBuckets 是耗时直方图的默认桶边界（毫秒）。
// 覆盖 10ms（快速任务）到 300s（慢速 ETL）的分布。
var DefaultDurationBuckets = []float64{
	10, 50, 100, 250, 500,
	1_000, 2_500, 5_000, 10_000, 30_000,
	60_000, 120_000, 300_000,
}

// 调度器侧指标名（不含 namespace）。
const (
	MetricTriggerTotal      = "scheduler_trigger_total"
	MetricTriggerDurationMs = "scheduler_trigger_duration_ms"
	MetricJobActive         = "scheduler_job_active"
	MetricRateLimitedTotal  = "scheduler_rate_limited_total"
	MetricWheelSize         = "scheduler_wheel_size"
	MetricNoExecutorTotal   = "scheduler_no_executor_total"
)

// 执行器侧指标名。
const (
	MetricRunTotal         = "executor_run_total"
	MetricRunDurationMs    = "executor_run_duration_ms"
	MetricRunningJobs      = "executor_running_jobs"
	MetricIdempotentReject = "executor_idempotent_rejected_total"
)

// 限流侧指标名。
const (
	MetricRateLimitAllowed  = "ratelimit_allowed_total"
	MetricRateLimitRejected = "ratelimit_rejected_total"
)

// ─── 错误类型 ─────────────────────────────────────────────────────────────────

// UnknownProviderTypeError 由 NewProvider 收到未知类型时返回。
type UnknownProviderTypeError struct {
	Type ProviderType
}

func (e *UnknownProviderTypeError) Error() string {
	return "metrics: unknown provider type: " + string(e.Type)
}
