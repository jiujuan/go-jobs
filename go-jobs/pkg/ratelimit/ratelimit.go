// Package ratelimit 为 go-jobs 提供两层任务限流与配额能力。
//
// # 设计目标
//
// 防止高频任务或异常重试打爆执行器集群，支持按 App / Job 两个维度独立配置。
//
// # 两层架构
//
//  1. TokenBucket（令牌桶）：控制单位时间内的触发速率（个/秒），
//     懒惰补充策略——每次 Allow() 时按经过时间补充，无后台 goroutine。
//     适合：高频 Cron 平滑限速。
//
//  2. FixedWindowQuota（固定窗口配额）：限制某时间窗口内的触发总次数。
//     窗口按挂钟取整对齐，自动重置。
//     适合：按天/小时的调用量配额、资费控制、SLA 保护。
//
// # Limiter（组合限流器）
//
// Limiter 同时持有 TokenBucket 和 FixedWindowQuota，两者均为可选。
// Allow() 依次检查：令牌桶 → 窗口配额，任一不通过立即拒绝。
//
// # Registry（限流注册表）
//
// Registry 按 key 管理 Limiter，调度器在 handleTask 触发前调用
// registry.CheckJob(appName, jobID)，不通过则记录日志并跳过本轮触发。
//
// Key 命名约定：
//   - App 级别：AppKey("email-sender")  → "app:email-sender"
//   - Job 级别：JobKey(42)              → "job:42"
//
// # 线程安全
//
// 全部公开方法均为 goroutine-safe。
package ratelimit

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ─── 哨兵错误 ─────────────────────────────────────────────────────────────────

// ErrRateLimitExceeded 令牌桶耗尽，触发速率超过上限。
var ErrRateLimitExceeded = errors.New("ratelimit: rate limit exceeded")

// ErrQuotaExceeded 固定窗口配额耗尽，本窗口内触发次数达到上限。
var ErrQuotaExceeded = errors.New("ratelimit: quota exceeded")

// ─── TokenBucket ──────────────────────────────────────────────────────────────

// TokenBucket 是线程安全的令牌桶实现。
//
// 采用懒惰补充（Lazy Refill）策略：不依赖后台 goroutine，
// 每次 Allow() 时根据距上次调用的时间差按速率补充令牌，
// 极低的内存和 CPU 开销，适合大量 job 并发调度场景。
type TokenBucket struct {
	rate     float64 // 令牌填充速率（个/秒）
	capacity float64 // 桶容量（最大令牌数 = 最大突发量）

	mu         sync.Mutex
	tokens     float64   // 当前令牌数
	lastRefill time.Time // 上次补充时间
}

// NewTokenBucket 创建令牌桶，初始状态为满桶。
//   - rate:     每秒填充令牌数（稳态吞吐上限）
//   - capacity: 桶容量（突发上限），须 >= rate；若传 0 则默认等于 rate
func NewTokenBucket(rate, capacity float64) *TokenBucket {
	if capacity <= 0 {
		capacity = rate
	}
	return &TokenBucket{
		rate:       rate,
		capacity:   capacity,
		tokens:     capacity, // 初始满桶
		lastRefill: time.Now(),
	}
}

// Allow 尝试消耗 1 个令牌。
// 成功返回 nil；令牌不足返回 ErrRateLimitExceeded。
func (tb *TokenBucket) Allow() error {
	return tb.AllowN(1)
}

// AllowN 尝试消耗 n 个令牌（批量触发场景）。
func (tb *TokenBucket) AllowN(n float64) error {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = minF(tb.capacity, tb.tokens+elapsed*tb.rate)
	tb.lastRefill = now

	if tb.tokens < n {
		return ErrRateLimitExceeded
	}
	tb.tokens -= n
	return nil
}

// Available 返回当前可用令牌数（不消耗，近似值）。
func (tb *TokenBucket) Available() float64 {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	return minF(tb.capacity, tb.tokens+elapsed*tb.rate)
}

// Reset 将令牌桶重置为满桶状态（用于测试或紧急恢复）。
func (tb *TokenBucket) Reset() {
	tb.mu.Lock()
	tb.tokens = tb.capacity
	tb.lastRefill = time.Now()
	tb.mu.Unlock()
}

// Rate 返回配置的填充速率（个/秒）。
func (tb *TokenBucket) Rate() float64 { return tb.rate }

// Capacity 返回配置的桶容量。
func (tb *TokenBucket) Capacity() float64 { return tb.capacity }

// ─── FixedWindowQuota ─────────────────────────────────────────────────────────

// FixedWindowQuota 在固定时间窗口内限制触发总次数。
//
// 窗口以挂钟时间取整对齐，例如 window=1h 时，
// 窗口为 0:00~1:00、1:00~2:00……进入新窗口时计数器自动重置。
// 实现完全无锁（依赖单个 sync.Mutex），无后台 goroutine。
type FixedWindowQuota struct {
	windowNs int64 // 窗口大小（纳秒）
	limit    int64 // 窗口内最大触发次数

	mu     sync.Mutex
	count  int64 // 当前窗口已消耗次数
	winKey int64 // 当前窗口编号（now.UnixNano() / windowNs）
}

// NewFixedWindowQuota 创建固定窗口配额。
//   - window: 时间窗口大小（如 time.Hour、24*time.Hour）
//   - limit:  窗口内允许的最大触发次数
func NewFixedWindowQuota(window time.Duration, limit int64) *FixedWindowQuota {
	return &FixedWindowQuota{
		windowNs: int64(window),
		limit:    limit,
		winKey:   time.Now().UnixNano() / int64(window),
	}
}

// Allow 尝试消耗 1 次配额。
// 成功返回 nil；配额耗尽返回 ErrQuotaExceeded。
func (q *FixedWindowQuota) Allow() error {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := time.Now().UnixNano() / q.windowNs
	if key != q.winKey {
		q.winKey = key
		q.count = 0
	}
	if q.count >= q.limit {
		return ErrQuotaExceeded
	}
	q.count++
	return nil
}

// Count 返回当前窗口已使用次数（近似值，不消耗配额）。
func (q *FixedWindowQuota) Count() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	if time.Now().UnixNano()/q.windowNs != q.winKey {
		return 0
	}
	return q.count
}

// Remaining 返回当前窗口剩余可用配额。
func (q *FixedWindowQuota) Remaining() int64 {
	used := q.Count()
	if r := q.limit - used; r > 0 {
		return r
	}
	return 0
}

// Limit 返回配置的窗口配额上限。
func (q *FixedWindowQuota) Limit() int64 { return q.limit }

// Window 返回配置的窗口大小。
func (q *FixedWindowQuota) Window() time.Duration { return time.Duration(q.windowNs) }

// ─── LimiterConfig ────────────────────────────────────────────────────────────

// LimiterConfig 配置单个组合限流器。
// Rate/Burst 和 QuotaWindow/QuotaLimit 均为可选，设为 0 表示不启用对应层。
type LimiterConfig struct {
	// ── 令牌桶 ──────────────────────────────────────────────────────────────
	// Rate > 0 时启用令牌桶。
	Rate  float64 // 每秒令牌填充速率（个/秒）
	Burst float64 // 桶容量（突发量）；0 表示等于 Rate

	// ── 固定窗口配额 ──────────────────────────────────────────────────────
	// QuotaWindow > 0 且 QuotaLimit > 0 时启用固定窗口配额。
	QuotaWindow time.Duration // 配额窗口大小（如 time.Hour、24*time.Hour）
	QuotaLimit  int64         // 窗口内最大触发次数
}

// ─── Limiter ──────────────────────────────────────────────────────────────────

// Limiter 组合令牌桶和固定窗口配额为单一限流单元。
//
// Allow() 的检查顺序：令牌桶（速率）→ 固定窗口（配额），
// 任一不通过立即返回对应错误，调用方不需要额外判断。
type Limiter struct {
	key    string
	bucket *TokenBucket      // 可为 nil（不限速率）
	quota  *FixedWindowQuota // 可为 nil（不限配额）

	allowed  int64 // 累计通过次数（原子）
	rejected int64 // 累计拒绝次数（原子）
}

// NewLimiter 根据配置创建 Limiter。
func NewLimiter(key string, cfg LimiterConfig) *Limiter {
	l := &Limiter{key: key}
	if cfg.Rate > 0 {
		l.bucket = NewTokenBucket(cfg.Rate, cfg.Burst)
	}
	if cfg.QuotaWindow > 0 && cfg.QuotaLimit > 0 {
		l.quota = NewFixedWindowQuota(cfg.QuotaWindow, cfg.QuotaLimit)
	}
	return l
}

// Allow 检查并消耗本次触发名额。
// nil 表示允许；ErrRateLimitExceeded 或 ErrQuotaExceeded 表示拒绝。
func (l *Limiter) Allow() error {
	if l.bucket != nil {
		if err := l.bucket.Allow(); err != nil {
			atomic.AddInt64(&l.rejected, 1)
			return err
		}
	}
	if l.quota != nil {
		if err := l.quota.Allow(); err != nil {
			atomic.AddInt64(&l.rejected, 1)
			return err
		}
	}
	atomic.AddInt64(&l.allowed, 1)
	return nil
}

// Key 返回限流器标识键。
func (l *Limiter) Key() string { return l.key }

// Stats 返回当前统计快照（线程安全）。
func (l *Limiter) Stats() LimiterStats {
	s := LimiterStats{
		Key:           l.key,
		TotalAllowed:  atomic.LoadInt64(&l.allowed),
		TotalRejected: atomic.LoadInt64(&l.rejected),
	}
	if l.bucket != nil {
		s.HasBucket       = true
		s.BucketAvailable = l.bucket.Available()
		s.BucketRate      = l.bucket.Rate()
		s.BucketCapacity  = l.bucket.Capacity()
	}
	if l.quota != nil {
		s.HasQuota       = true
		s.QuotaRemaining = l.quota.Remaining()
		s.QuotaLimit     = l.quota.Limit()
		s.QuotaWindow    = l.quota.Window()
	}
	return s
}

// LimiterStats 是 Limiter 的统计快照，用于监控和调试。
type LimiterStats struct {
	Key           string
	TotalAllowed  int64
	TotalRejected int64

	HasBucket       bool
	BucketAvailable float64
	BucketRate      float64
	BucketCapacity  float64

	HasQuota       bool
	QuotaRemaining int64
	QuotaLimit     int64
	QuotaWindow    time.Duration
}

// ─── Registry ─────────────────────────────────────────────────────────────────

// Registry 是全局限流器注册表，按 key 索引，线程安全。
//
// 典型用法：
//
//	reg := ratelimit.NewRegistry()
//	// App 级别：email-sender 每秒最多触发 10 次
//	reg.RegisterApp("email-sender", ratelimit.LimiterConfig{Rate: 10, Burst: 20})
//	// Job 级别：job 42 每天最多触发 1000 次
//	reg.RegisterJob(42, ratelimit.LimiterConfig{QuotaWindow: 24*time.Hour, QuotaLimit: 1000})
//
//	// 在 handleTask 中检查：
//	if err := reg.CheckJob("email-sender", 42); err != nil {
//	    // 被限流，跳过本次触发
//	}
type Registry struct {
	mu       sync.RWMutex
	limiters map[string]*Limiter
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{limiters: make(map[string]*Limiter, 16)}
}

// Register 注册或替换一个限流器。若 cfg 中两层均未配置则为空限流器（恒通过）。
func (r *Registry) Register(key string, cfg LimiterConfig) *Limiter {
	l := NewLimiter(key, cfg)
	r.mu.Lock()
	r.limiters[key] = l
	r.mu.Unlock()
	return l
}

// RegisterApp 注册 App 级别限流器（对该 App 下所有 Job 生效）。
func (r *Registry) RegisterApp(appName string, cfg LimiterConfig) *Limiter {
	return r.Register(AppKey(appName), cfg)
}

// RegisterJob 注册 Job 级别限流器（仅对该 JobID 生效）。
func (r *Registry) RegisterJob(jobID int64, cfg LimiterConfig) *Limiter {
	return r.Register(JobKey(jobID), cfg)
}

// Deregister 删除指定 key 的限流器。
func (r *Registry) Deregister(key string) {
	r.mu.Lock()
	delete(r.limiters, key)
	r.mu.Unlock()
}

// Get 返回指定 key 的 Limiter，不存在返回 nil, false。
func (r *Registry) Get(key string) (*Limiter, bool) {
	r.mu.RLock()
	l, ok := r.limiters[key]
	r.mu.RUnlock()
	return l, ok
}

// CheckJob 检查指定任务是否允许本次触发。
//
// 先检查 Job 级别，再检查 App 级别：
//   - Job 级别不通过 → 直接返回，App 配额不消耗
//   - 两者均通过 → 返回 nil
//   - 对应 key 未注册 → 视为不限流（nil）
func (r *Registry) CheckJob(appName string, jobID int64) error {
	if l, ok := r.Get(JobKey(jobID)); ok {
		if err := l.Allow(); err != nil {
			return fmt.Errorf("job %d rate limited: %w", jobID, err)
		}
	}
	if l, ok := r.Get(AppKey(appName)); ok {
		if err := l.Allow(); err != nil {
			return fmt.Errorf("app %q rate limited: %w", appName, err)
		}
	}
	return nil
}

// AllStats 返回所有已注册限流器的统计快照（顺序不保证）。
func (r *Registry) AllStats() []LimiterStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]LimiterStats, 0, len(r.limiters))
	for _, l := range r.limiters {
		out = append(out, l.Stats())
	}
	return out
}

// Len 返回已注册的限流器数量。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.limiters)
}

// ─── Key 辅助函数 ─────────────────────────────────────────────────────────────

// AppKey 生成 App 级别的限流器 key。
func AppKey(appName string) string { return "app:" + appName }

// JobKey 生成 Job 级别的限流器 key。
func JobKey(jobID int64) string { return fmt.Sprintf("job:%d", jobID) }

// ─── 内部工具 ─────────────────────────────────────────────────────────────────

func minF(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
