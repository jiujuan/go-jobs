// Package executorstore 提供执行器内存注册表，彻底消除调度热路径上的 MySQL 查询。
//
// # 设计目标
//
// 原架构中，每次任务触发时 handleTask 都调用 executorDAO.ListByApp()，
// 这导致每次触发都有一次同步 DB 查询。高频任务场景下，这是主要瓶颈。
//
// # 解决方案
//
//  1. Store（内存注册表）：以 sync.Map 为底层存储，按 appName 分组维护
//     全部在线执行器，增删改均为 O(1)，读为无锁并发安全。
//
//  2. HealthProber（异步健康探测器）：后台 goroutine 定期对每个执行器
//     发送 Beat 请求，结果写入执行器的 healthy 字段（原子操作）。
//     Failover 路由直接读 healthy 字段，无需同步 HTTP 探测。
//
// # 生命周期
//
//	store := executorstore.New(client, logger)
//	store.Start(ctx)          // 启动健康探测后台循环
//	defer store.Stop()
//
//	// 执行器注册/心跳/注销（由 ExecutorService 调用）
//	store.Register(executor)
//	store.Heartbeat(appName, address)
//	store.Deregister(appName, address)
//
//	// 调度器路由选址（无 DB 查询）
//	addrs := store.ListOnlineAddresses(appName)
//	addrs = store.ListHealthyAddresses(appName)  // Failover 专用
package executorstore

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 常量 ─────────────────────────────────────────────────────────────────────

const (
	// defaultProbeInterval 健康探测间隔（每个执行器每隔此时间探一次）
	defaultProbeInterval = 10 * time.Second

	// defaultProbeTimeout 单次 Beat 请求超时
	defaultProbeTimeout = 2 * time.Second

	// defaultHeartbeatTTL 超过此时长无心跳则视为离线
	defaultHeartbeatTTL = 90 * time.Second

	// defaultProbeConcurrency 并发探测的最大 goroutine 数
	defaultProbeConcurrency = 32
)

// ─── 接口 ─────────────────────────────────────────────────────────────────────

// Logger 是注入到 Store 的最小日志接口，避免循环依赖。
type Logger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
}

// BeatClient 对执行器发送心跳探测。由 scheduler.ExecutorClient 实现，
// 此处定义接口以便测试时 mock。
type BeatClient interface {
	Beat(ctx context.Context) error
}

// BeatClientFactory 根据地址创建 BeatClient。
type BeatClientFactory func(address string) BeatClient

// ─── 执行器条目 ───────────────────────────────────────────────────────────────

// Entry 记录单个执行器的运行时状态。
type Entry struct {
	// 基础信息（来自 model.Executor，注册时写入，只读）
	ID       int64
	AppName  string
	Address  string
	Weight   int
	Version  string

	// 状态字段（并发读写，用原子操作或 mutex 保护）
	healthy       int32     // 1=健康, 0=不健康（探测结果）
	lastHeartbeat int64     // Unix nano，最近一次心跳时间
	mu            sync.RWMutex
	cpu           float64
	memory        float64
}

// IsHealthy 返回最近一次探测结果。
func (e *Entry) IsHealthy() bool { return atomic.LoadInt32(&e.healthy) == 1 }

// SetHealthy 设置健康状态（由健康探测器写入）。
func (e *Entry) SetHealthy(ok bool) {
	v := int32(0)
	if ok {
		v = 1
	}
	atomic.StoreInt32(&e.healthy, v)
}

// LastHeartbeat 返回最近心跳时间。
func (e *Entry) LastHeartbeat() time.Time {
	return time.Unix(0, atomic.LoadInt64(&e.lastHeartbeat))
}

// touch 更新心跳时间。
func (e *Entry) touch() {
	atomic.StoreInt64(&e.lastHeartbeat, time.Now().UnixNano())
}

// UpdateResource 更新 CPU/内存指标（由心跳上报携带）。
func (e *Entry) UpdateResource(cpu, memory float64) {
	e.mu.Lock()
	e.cpu = cpu
	e.memory = memory
	e.mu.Unlock()
}

// Resource 返回 (cpu, memory) 快照。
func (e *Entry) Resource() (cpu, memory float64) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.cpu, e.memory
}

// snapshot 返回只读副本（供外部查看）。
func (e *Entry) snapshot() EntrySnapshot {
	cpu, mem := e.Resource()
	return EntrySnapshot{
		ID:            e.ID,
		AppName:       e.AppName,
		Address:       e.Address,
		Weight:        e.Weight,
		Version:       e.Version,
		Healthy:       e.IsHealthy(),
		LastHeartbeat: e.LastHeartbeat(),
		CPU:           cpu,
		Memory:        mem,
	}
}

// EntrySnapshot 是 Entry 的只读快照，供外部安全使用。
type EntrySnapshot struct {
	ID            int64
	AppName       string
	Address       string
	Weight        int
	Version       string
	Healthy       bool
	LastHeartbeat time.Time
	CPU           float64
	Memory        float64
}

// ─── appGroup 按 app 分组 ─────────────────────────────────────────────────────

// appGroup 维护同一 AppName 下的执行器集合。
type appGroup struct {
	mu      sync.RWMutex
	entries map[string]*Entry // key = address
}

func newAppGroup() *appGroup {
	return &appGroup{entries: make(map[string]*Entry)}
}

// add 插入或替换执行器。
func (g *appGroup) add(e *Entry) {
	g.mu.Lock()
	g.entries[e.Address] = e
	g.mu.Unlock()
}

// remove 删除执行器，返回是否存在。
func (g *appGroup) remove(address string) bool {
	g.mu.Lock()
	_, ok := g.entries[address]
	delete(g.entries, address)
	g.mu.Unlock()
	return ok
}

// get 返回执行器，若不存在返回 nil。
func (g *appGroup) get(address string) *Entry {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.entries[address]
}

// listAll 返回全部执行器的引用切片（不过滤状态）。
func (g *appGroup) listAll() []*Entry {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]*Entry, 0, len(g.entries))
	for _, e := range g.entries {
		out = append(out, e)
	}
	return out
}

// ─── Options ─────────────────────────────────────────────────────────────────

// Options 配置 Store 行为。
type Options struct {
	ProbeInterval    time.Duration
	ProbeTimeout     time.Duration
	HeartbeatTTL     time.Duration
	ProbeConcurrency int
}

// Option 是 Store 的函数式选项。
type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		ProbeInterval:    defaultProbeInterval,
		ProbeTimeout:     defaultProbeTimeout,
		HeartbeatTTL:     defaultHeartbeatTTL,
		ProbeConcurrency: defaultProbeConcurrency,
	}
}

// WithProbeInterval 设置健康探测间隔。
func WithProbeInterval(d time.Duration) Option {
	return func(o *Options) { o.ProbeInterval = d }
}

// WithProbeTimeout 设置单次探测超时。
func WithProbeTimeout(d time.Duration) Option {
	return func(o *Options) { o.ProbeTimeout = d }
}

// WithHeartbeatTTL 设置心跳超时（超过此时长无心跳则视为离线）。
func WithHeartbeatTTL(d time.Duration) Option {
	return func(o *Options) { o.HeartbeatTTL = d }
}

// WithProbeConcurrency 设置健康探测的最大并发数。
func WithProbeConcurrency(n int) Option {
	return func(o *Options) { o.ProbeConcurrency = n }
}

// ─── Store ────────────────────────────────────────────────────────────────────

// Store 是执行器内存注册表。
// 它是调度器查询"哪些执行器可用"的唯一数据源，消除所有运行时 DB 查询。
type Store struct {
	// groups: appName → *appGroup
	groups sync.Map

	factory BeatClientFactory
	opts    *Options
	log     Logger

	stopCh chan struct{}
	wg     sync.WaitGroup
}

// New 创建一个 Store。
//
//   - factory: 根据地址创建 BeatClient（生产环境传 NewExecutorClientFactory()）
//   - log: 日志接口
//   - opts: 可选配置
func New(factory BeatClientFactory, log Logger, opts ...Option) *Store {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	return &Store{
		factory: factory,
		opts:    o,
		log:     log,
		stopCh:  make(chan struct{}),
	}
}

// Start 启动后台健康探测循环和心跳 TTL 扫描。
func (s *Store) Start(ctx context.Context) {
	s.wg.Add(2)
	go s.probeLoop(ctx)
	go s.ttlSweepLoop(ctx)
}

// Stop 停止后台 goroutine，等待退出。
func (s *Store) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

// ─── 写操作（由 ExecutorService 调用）───────────────────────────────────────

// Register 注册或更新一个执行器。
// 新执行器初始健康状态为 true（乐观初始，探测器会在下个周期修正）。
func (s *Store) Register(e *model.Executor) {
	entry := &Entry{
		ID:      e.ID,
		AppName: e.AppName,
		Address: e.Address,
		Weight:  e.Weight,
		Version: e.Version,
	}
	// 乐观初始化：新注册的执行器默认健康
	atomic.StoreInt32(&entry.healthy, 1)
	entry.touch()

	grp := s.getOrCreateGroup(e.AppName)
	grp.add(entry)

	s.log.Info("executorstore: registered",
		zap.String("app", e.AppName),
		zap.String("addr", e.Address),
		zap.Int64("id", e.ID),
	)
}

// Heartbeat 更新执行器的最近心跳时间，并可更新资源指标。
// 如果执行器尚未注册（例如 store 重启后），直接忽略（等待下次 Register）。
func (s *Store) Heartbeat(appName, address string, cpu, memory float64) {
	grp := s.getGroup(appName)
	if grp == nil {
		return
	}
	e := grp.get(address)
	if e == nil {
		return
	}
	e.touch()
	e.UpdateResource(cpu, memory)
	// 心跳到达说明执行器存活，将健康状态恢复为 true
	e.SetHealthy(true)
}

// Deregister 注销执行器（执行器优雅下线时调用）。
func (s *Store) Deregister(appName, address string) {
	grp := s.getGroup(appName)
	if grp == nil {
		return
	}
	if grp.remove(address) {
		s.log.Info("executorstore: deregistered",
			zap.String("app", appName),
			zap.String("addr", address),
		)
	}
}

// Bootstrap 从数据库查询结果批量加载（进程启动时调用一次）。
func (s *Store) Bootstrap(executors []*model.Executor) {
	for _, e := range executors {
		if e.Status == model.ExecutorOnline {
			s.Register(e)
		}
	}
	s.log.Info("executorstore: bootstrapped", zap.Int("count", len(executors)))
}

// ─── 读操作（由调度器路由热路径调用，无锁/无 DB）────────────────────────────

// ListOnlineAddresses 返回指定 app 的全部在线执行器地址。
// "在线" = 最近心跳在 HeartbeatTTL 内（不要求 Beat 探测结果）。
// 调用频率极高，路径上只有 RLock，无 DB 查询。
func (s *Store) ListOnlineAddresses(appName string) []string {
	grp := s.getGroup(appName)
	if grp == nil {
		return nil
	}
	entries := grp.listAll()
	ttl := s.opts.HeartbeatTTL
	now := time.Now()
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if now.Sub(e.LastHeartbeat()) < ttl {
			out = append(out, e.Address)
		}
	}
	return out
}

// ListHealthyAddresses 返回指定 app 的健康执行器地址（Beat 探测结果为 true）。
// 供 Failover 路由使用；普通路由用 ListOnlineAddresses 即可。
func (s *Store) ListHealthyAddresses(appName string) []string {
	grp := s.getGroup(appName)
	if grp == nil {
		return nil
	}
	entries := grp.listAll()
	ttl := s.opts.HeartbeatTTL
	now := time.Now()
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if now.Sub(e.LastHeartbeat()) < ttl && e.IsHealthy() {
			out = append(out, e.Address)
		}
	}
	return out
}

// ListEntries 返回指定 app 所有执行器的只读快照（供管理 API 展示）。
func (s *Store) ListEntries(appName string) []EntrySnapshot {
	grp := s.getGroup(appName)
	if grp == nil {
		return nil
	}
	entries := grp.listAll()
	out := make([]EntrySnapshot, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.snapshot())
	}
	return out
}

// TotalCount 返回全部执行器数量（含离线）。
func (s *Store) TotalCount() int {
	total := 0
	s.groups.Range(func(_, v interface{}) bool {
		grp := v.(*appGroup)
		grp.mu.RLock()
		total += len(grp.entries)
		grp.mu.RUnlock()
		return true
	})
	return total
}

// ─── 后台健康探测循环 ─────────────────────────────────────────────────────────

// probeLoop 定期对所有执行器发送 Beat 请求，结果写入 Entry.healthy。
func (s *Store) probeLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.opts.ProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.probeAll()
		}
	}
}

// probeAll 并发探测所有执行器，受 ProbeConcurrency 限制。
func (s *Store) probeAll() {
	// 收集所有待探测执行器
	var targets []*Entry
	s.groups.Range(func(_, v interface{}) bool {
		grp := v.(*appGroup)
		targets = append(targets, grp.listAll()...)
		return true
	})
	if len(targets) == 0 {
		return
	}

	// 信号量控制并发
	sem := make(chan struct{}, s.opts.ProbeConcurrency)
	var wg sync.WaitGroup

	for _, e := range targets {
		e := e // 捕获
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			s.probeOne(e)
		}()
	}
	wg.Wait()
}

// probeOne 对单个执行器发送 Beat，更新 healthy 字段。
func (s *Store) probeOne(e *Entry) {
	ctx, cancel := context.WithTimeout(context.Background(), s.opts.ProbeTimeout)
	defer cancel()

	client := s.factory(e.Address)
	err := client.Beat(ctx)
	was := e.IsHealthy()
	e.SetHealthy(err == nil)

	if err != nil && was {
		// 由健康变为不健康，打警告日志
		s.log.Warn("executorstore: executor unhealthy",
			zap.String("app", e.AppName),
			zap.String("addr", e.Address),
			zap.Error(err),
		)
	} else if err == nil && !was {
		// 由不健康恢复，打 info 日志
		s.log.Info("executorstore: executor recovered",
			zap.String("app", e.AppName),
			zap.String("addr", e.Address),
		)
	}
}

// ─── 心跳 TTL 扫描循环 ────────────────────────────────────────────────────────

// ttlSweepLoop 定期扫描心跳超时的执行器并将其标记为不健康（不删除，等待主动注销）。
func (s *Store) ttlSweepLoop(ctx context.Context) {
	defer s.wg.Done()
	// 扫描频率 = TTL / 3，保证及时性
	interval := s.opts.HeartbeatTTL / 3
	if interval < 5*time.Second {
		interval = 5 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweepTTL()
		}
	}
}

// sweepTTL 将心跳超时的执行器标记为不健康。
func (s *Store) sweepTTL() {
	ttl := s.opts.HeartbeatTTL
	now := time.Now()
	s.groups.Range(func(_, v interface{}) bool {
		grp := v.(*appGroup)
		for _, e := range grp.listAll() {
			if now.Sub(e.LastHeartbeat()) >= ttl && e.IsHealthy() {
				e.SetHealthy(false)
				s.log.Warn("executorstore: heartbeat timeout, marking unhealthy",
					zap.String("app", e.AppName),
					zap.String("addr", e.Address),
					zap.Duration("since", now.Sub(e.LastHeartbeat())),
				)
			}
		}
		return true
	})
}

// ─── 内部辅助 ─────────────────────────────────────────────────────────────────

func (s *Store) getOrCreateGroup(appName string) *appGroup {
	v, _ := s.groups.LoadOrStore(appName, newAppGroup())
	return v.(*appGroup)
}

func (s *Store) getGroup(appName string) *appGroup {
	v, ok := s.groups.Load(appName)
	if !ok {
		return nil
	}
	return v.(*appGroup)
}

// ─── 便利工具（供 main.go 使用）─────────────────────────────────────────────

// zapLoggerAdapter 将 *zap.Logger 适配为 executorstore.Logger 接口。
type zapLoggerAdapter struct{ l *zap.Logger }

func (a *zapLoggerAdapter) Info(msg string, fields ...zap.Field)  { a.l.Info(msg, fields...) }
func (a *zapLoggerAdapter) Warn(msg string, fields ...zap.Field)  { a.l.Warn(msg, fields...) }
func (a *zapLoggerAdapter) Error(msg string, fields ...zap.Field) { a.l.Error(msg, fields...) }

// ZapLoggerAdapter 将标准 *zap.Logger 适配为 executorstore 所需的 Logger 接口。
func ZapLoggerAdapter(l *zap.Logger) Logger {
	return &zapLoggerAdapter{l: l}
}
