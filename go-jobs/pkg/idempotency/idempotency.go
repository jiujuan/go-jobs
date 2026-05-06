// Package idempotency 为执行器提供基于 LogID 的幂等执行保证。
//
// # 问题背景
//
// go-jobs 的触发→执行链路中存在若干重复执行的风险窗口：
//
//  1. 网络重试：调度器 callExecutor() HTTP 超时后自动重试，
//     但执行器已收到第一个请求并开始执行。
//  2. 多节点竞争：Redis 锁在网络抖动时可能出现短暂双主，
//     两个调度节点都通过了加锁检查。
//  3. 手动触发与 Cron 重叠：同一 JobID 在极短时间内收到两次触发。
//  4. 执行器重启回放：进程重启后，客户端重试的请求携带旧 LogID 重新到达。
//
// # 解决方案
//
// 以 LogID 为幂等键，在内存中维护一张执行状态表（Table）。
// 每次收到 /executor/run 请求时：
//
//	result, acquired := table.TryAcquire(logID)
//	if !acquired {
//	    // 重复请求：直接返回已有状态，不重新执行
//	    return result, nil
//	}
//	// 首次请求：执行，完成后调用 table.Complete(logID, err)
//
// # 状态机
//
//	         TryAcquire()          Complete(nil)
//	(空) ──────────────► Running ──────────────► Success
//	                         │
//	                         │ Complete(err)
//	                         ▼
//	                       Failed
//
// Running 状态的重复请求返回 ErrAlreadyRunning；
// Success/Failed 状态的重复请求返回已记录的结果（含耗时）。
//
// # 过期清理
//
// TTL 过期后条目会被 GC 扫描删除，防止无限增长。
// 默认 TTL = 24h，满足绝大多数场景（任务通常在几秒到几分钟内完成）。
package idempotency

import (
	"errors"
	"sync"
	"time"
)

// ─── 错误 ─────────────────────────────────────────────────────────────────────

// ErrAlreadyRunning 表示相同 LogID 的任务正在执行中，本次为重复请求。
var ErrAlreadyRunning = errors.New("idempotency: job is already running")

// ErrExpired 表示 LogID 对应的幂等记录已过期被清理。
// 调用方应当视情况重新执行或忽略。
var ErrExpired = errors.New("idempotency: record expired")

// ─── 状态 ─────────────────────────────────────────────────────────────────────

// State 是幂等记录的执行状态。
type State int8

const (
	// StateRunning 表示首次请求已开始执行，尚未完成。
	StateRunning State = iota
	// StateSuccess 表示执行成功完成。
	StateSuccess
	// StateFailed 表示执行失败。
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateRunning:
		return "running"
	case StateSuccess:
		return "success"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ─── 记录 ─────────────────────────────────────────────────────────────────────

// Record 保存单个 LogID 的幂等执行状态。
type Record struct {
	LogID      int64
	State      State
	Err        error     // 执行失败时的错误信息（State == StateFailed）
	StartTime  time.Time // TryAcquire 时间
	EndTime    time.Time // Complete 时间（State != StateRunning 时有效）
	DurationMs int64     // 执行耗时（毫秒）
	expireAt   time.Time // GC 截止时间
}

// Succeeded 返回执行是否成功完成。
func (r *Record) Succeeded() bool { return r.State == StateSuccess }

// Failed 返回执行是否失败。
func (r *Record) Failed() bool { return r.State == StateFailed }

// Running 返回是否正在执行中。
func (r *Record) Running() bool { return r.State == StateRunning }

// ─── Options ─────────────────────────────────────────────────────────────────

const (
	defaultTTL          = 24 * time.Hour
	defaultGCInterval   = 10 * time.Minute
	defaultInitialCap   = 1024
)

// Options 配置 Table 行为。
type Options struct {
	// TTL 是幂等记录在完成后的保留时间。
	// Running 状态的记录不受 TTL 影响，直到 Complete 被调用。
	// 默认 24h。
	TTL time.Duration

	// GCInterval 是后台 GC 扫描间隔。默认 10min。
	GCInterval time.Duration

	// InitialCapacity 是初始 map 容量。默认 1024。
	InitialCapacity int
}

// Option 是 Table 的函数式选项。
type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		TTL:             defaultTTL,
		GCInterval:      defaultGCInterval,
		InitialCapacity: defaultInitialCap,
	}
}

// WithTTL 设置记录保留时间（完成后开始计算）。
func WithTTL(d time.Duration) Option {
	return func(o *Options) { o.TTL = d }
}

// WithGCInterval 设置 GC 扫描间隔。
func WithGCInterval(d time.Duration) Option {
	return func(o *Options) { o.GCInterval = d }
}

// WithInitialCapacity 设置初始 map 容量，减少 rehash。
func WithInitialCapacity(n int) Option {
	return func(o *Options) { o.InitialCapacity = n }
}

// ─── Table ────────────────────────────────────────────────────────────────────

// Table 是幂等执行状态表，线程安全。
//
// 推荐每个 Runner 实例持有一个 Table，生命周期与 Runner 相同。
type Table struct {
	mu      sync.RWMutex
	records map[int64]*Record // logID → *Record
	opts    *Options

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New 创建并启动一个 Table。
// 调用方在关闭时应调用 Stop() 停止后台 GC goroutine。
func New(opts ...Option) *Table {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	t := &Table{
		records: make(map[int64]*Record, o.InitialCapacity),
		opts:    o,
		stopCh:  make(chan struct{}),
	}
	t.wg.Add(1)
	go t.gcLoop()
	return t
}

// Stop 停止后台 GC goroutine。
func (t *Table) Stop() {
	close(t.stopCh)
	t.wg.Wait()
}

// ─── 核心接口 ─────────────────────────────────────────────────────────────────

// TryAcquire 尝试为 logID 注册一次执行权。
//
// 返回值：
//   - *Record, true  ：首次请求，调用方获得执行权，应执行任务后调用 Complete。
//   - *Record, false ：重复请求，*Record 包含已有状态，调用方应直接返回。
//
// 当 *Record.State == StateRunning 时，重复请求应返回 ErrAlreadyRunning。
// 当 *Record.State == StateSuccess/StateFailed 时，可直接使用已有结果。
func (t *Table) TryAcquire(logID int64) (*Record, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if rec, ok := t.records[logID]; ok {
		// 已有记录：重复请求
		return rec, false
	}

	// 首次请求：创建 Running 记录
	rec := &Record{
		LogID:     logID,
		State:     StateRunning,
		StartTime: time.Now(),
		// expireAt 在 Running 时设为零值，Complete 时才设置
	}
	t.records[logID] = rec
	return rec, true
}

// Complete 标记 logID 对应的执行已完成。
//
// err == nil 表示成功，err != nil 表示失败。
// 完成后记录进入 TTL 倒计时，GC 会在 TTL 后清理。
//
// 若 logID 不存在（从未调用 TryAcquire），为幂等操作，静默忽略。
func (t *Table) Complete(logID int64, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	rec, ok := t.records[logID]
	if !ok {
		return
	}
	if rec.State != StateRunning {
		// 已经完成，防止重复调用
		return
	}

	end := time.Now()
	rec.EndTime = end
	rec.DurationMs = end.Sub(rec.StartTime).Milliseconds()
	rec.expireAt = end.Add(t.opts.TTL)

	if err != nil {
		rec.State = StateFailed
		rec.Err = err
	} else {
		rec.State = StateSuccess
	}
}

// Get 查询 logID 的幂等记录。
// 返回 nil 表示记录不存在（未执行过或已 GC）。
func (t *Table) Get(logID int64) *Record {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.records[logID]
}

// ForceDelete 强制删除记录（用于测试或异常恢复）。
func (t *Table) ForceDelete(logID int64) {
	t.mu.Lock()
	delete(t.records, logID)
	t.mu.Unlock()
}

// Len 返回当前记录数量（含 Running 状态）。
func (t *Table) Len() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.records)
}

// Stats 返回各状态的计数统计。
func (t *Table) Stats() TableStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var s TableStats
	for _, r := range t.records {
		switch r.State {
		case StateRunning:
			s.Running++
		case StateSuccess:
			s.Success++
		case StateFailed:
			s.Failed++
		}
	}
	s.Total = s.Running + s.Success + s.Failed
	return s
}

// TableStats 汇总各状态计数。
type TableStats struct {
	Total   int
	Running int
	Success int
	Failed  int
}

// ─── GC ───────────────────────────────────────────────────────────────────────

// gcLoop 定期扫描并删除已过期的已完成记录。
// Running 状态的记录不会被 GC（expireAt 为零值）。
func (t *Table) gcLoop() {
	defer t.wg.Done()
	ticker := time.NewTicker(t.opts.GCInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.gc()
		}
	}
}

func (t *Table) gc() {
	now := time.Now()
	t.mu.Lock()
	for logID, rec := range t.records {
		// expireAt 零值表示 Running，不删除
		if !rec.expireAt.IsZero() && now.After(rec.expireAt) {
			delete(t.records, logID)
		}
	}
	t.mu.Unlock()
}

// GetTTL 返回配置的 TTL（用于测试验证）。
func (t *Table) GetTTL() time.Duration { return t.opts.TTL }

// GetGCInterval 返回配置的 GC 间隔（用于测试验证）。
func (t *Table) GetGCInterval() time.Duration { return t.opts.GCInterval }
