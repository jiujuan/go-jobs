// Package paramtpl 为 go-jobs 提供任务参数模板化能力。
//
// # 设计目标
//
// 避免因运行日期变化而反复修改 job_info.execute_param。
// 支持在参数字符串中嵌入动态占位符，由调度器在触发时渲染为最终值，
// 渲染结果仅作用于本次触发，不修改数据库中的参数定义。
//
// # 模板语法
//
// 使用 Go 标准库 text/template，无外部依赖：
//
//	{"date":"{{.Yesterday}}","env":"{{.Env}}"}
//	{"partition":"dt={{.DateHour}}","shard":{{.ShardIndex}}}
//	{"range":"{{.StartOfDay}} ~ {{.EndOfDay}}"}
//	{"tag":"{{default "latest" .Tag}}"}
//
// # 内置变量（TriggerVars）
//
//	.Now         当前时间（time.Time）
//	.Date        当前日期 YYYY-MM-DD
//	.DateTime    当前日期时间 YYYY-MM-DD HH:mm:ss
//	.DateHour    当前年月日时 YYYY-MM-DD HH
//	.Yesterday   昨天日期 YYYY-MM-DD
//	.Tomorrow    明天日期 YYYY-MM-DD
//	.StartOfDay  今天 00:00:00
//	.EndOfDay    今天 23:59:59
//	.Unix        当前 Unix 时间戳（秒）
//	.UnixMilli   当前 Unix 毫秒时间戳
//	.Weekday     星期 Monday…Sunday
//	.Month       月份 January…December
//	.Year        年份整数
//	.ShardIndex  当前分片下标
//	.ShardTotal  分片总数
//	.JobID       任务 ID
//	.TriggerType 触发类型（cron/manual/retry/child）
//	.NodeID      调度节点 ID
//	.Env         运行环境（dev/prod/staging）
//
// Extra 中的 key 可覆盖同名内置变量，优先级最高。
//
// # 内置函数
//
//	daysAgo N       N 天前的日期字符串
//	daysAfter N     N 天后的日期字符串
//	formatDate T L  按 layout L 格式化 time.Time T
//	add A B         整数加法
//	mod A B         整数取模
//	default D V     若 V 为空字符串则返回 D
//	now             当前 Unix 时间戳（秒）
//
// # 快速路径
//
// 若参数字符串不含 {{ 则直接原样返回，零分配，对非模板参数无任何开销。
//
// # 缓存
//
// 已编译模板按原始文本缓存，重复模板仅编译一次。
// 缓存容量默认 1024 条，超限后整体清空（生产场景模板数量有限）。
package paramtpl

import (
	"bytes"
	"fmt"
	"sync"
	"text/template"
	"time"
)

// ─── TriggerContext ────────────────────────────────────────────────────────────

// TriggerContext 携带调度触发时的上下文，用于构建模板变量集合。
type TriggerContext struct {
	JobID       int64
	ShardIndex  int
	ShardTotal  int
	TriggerType string         // "cron" | "manual" | "retry" | "child"
	Extra       map[string]any // 用户自定义变量，可覆盖同名内置变量
}

// ─── TriggerVars ──────────────────────────────────────────────────────────────

// TriggerVars 是渲染模板时注入的完整变量集合。
// 模板中通过 {{.FieldName}} 引用内置字段，Extra 键值通过合并传入。
type TriggerVars struct {
	// 时间
	Now        time.Time
	Date       string // YYYY-MM-DD
	DateTime   string // YYYY-MM-DD HH:mm:ss
	DateHour   string // YYYY-MM-DD HH
	Yesterday  string // YYYY-MM-DD
	Tomorrow   string // YYYY-MM-DD
	StartOfDay string // YYYY-MM-DD 00:00:00
	EndOfDay   string // YYYY-MM-DD 23:59:59
	Unix       int64
	UnixMilli  int64
	Weekday    string // Monday…Sunday
	Month      string // January…December
	Year       int

	// 调度上下文
	ShardIndex  int
	ShardTotal  int
	JobID       int64
	TriggerType string

	// 环境
	NodeID string
	Env    string

	// 用户扩展（不直接暴露为字段，通过 mergeData 合并到 map 后传给模板）
	extra map[string]any
}

// ─── Engine ───────────────────────────────────────────────────────────────────

// Option 是 Engine 的函数式选项。
type Option func(*engineConfig)

type engineConfig struct {
	env       string
	nodeID    string
	cacheSize int
	funcMap   template.FuncMap
}

func defaultConfig() *engineConfig {
	return &engineConfig{
		env:       "dev",
		nodeID:    "default",
		cacheSize: 1024,
	}
}

// WithEnv 设置运行环境标识（dev/prod/staging）。
func WithEnv(env string) Option { return func(c *engineConfig) { c.env = env } }

// WithNodeID 设置调度节点 ID。
func WithNodeID(id string) Option { return func(c *engineConfig) { c.nodeID = id } }

// WithCacheSize 设置模板编译缓存上限（0 = 禁用缓存）。
func WithCacheSize(n int) Option { return func(c *engineConfig) { c.cacheSize = n } }

// WithFuncMap 追加自定义模板函数（可覆盖同名内置函数）。
func WithFuncMap(fm template.FuncMap) Option {
	return func(c *engineConfig) {
		if c.funcMap == nil {
			c.funcMap = make(template.FuncMap)
		}
		for k, v := range fm {
			c.funcMap[k] = v
		}
	}
}

// Engine 是参数模板引擎，所有公开方法均为 goroutine-safe。
type Engine struct {
	cfg     *engineConfig
	funcMap template.FuncMap

	mu    sync.RWMutex
	cache map[string]*template.Template
}

// New 创建并返回一个模板引擎实例。
func New(opts ...Option) *Engine {
	cfg := defaultConfig()
	for _, o := range opts {
		o(cfg)
	}
	fm := builtinFuncMap()
	for k, v := range cfg.funcMap {
		fm[k] = v
	}
	return &Engine{
		cfg:     cfg,
		funcMap: fm,
		cache:   make(map[string]*template.Template, 64),
	}
}

// BuildVars 根据触发上下文构建模板变量集合。
// 此方法纯 CPU，无 IO，可高频调用。
func (e *Engine) BuildVars(ctx TriggerContext) *TriggerVars {
	now := time.Now()
	date := now.Format("2006-01-02")
	return &TriggerVars{
		Now:        now,
		Date:       date,
		DateTime:   now.Format("2006-01-02 15:04:05"),
		DateHour:   now.Format("2006-01-02 15"),
		Yesterday:  now.AddDate(0, 0, -1).Format("2006-01-02"),
		Tomorrow:   now.AddDate(0, 0, 1).Format("2006-01-02"),
		StartOfDay: date + " 00:00:00",
		EndOfDay:   date + " 23:59:59",
		Unix:       now.Unix(),
		UnixMilli:  now.UnixMilli(),
		Weekday:    now.Weekday().String(),
		Month:      now.Month().String(),
		Year:       now.Year(),

		ShardIndex:  ctx.ShardIndex,
		ShardTotal:  ctx.ShardTotal,
		JobID:       ctx.JobID,
		TriggerType: ctx.TriggerType,

		NodeID: e.cfg.nodeID,
		Env:    e.cfg.env,

		extra: ctx.Extra,
	}
}

// Render 渲染模板字符串，返回渲染结果。
//
//   - 若 tpl 不含 {{ 则直接返回原文（快速路径，零分配）。
//   - vars 为 nil 时自动调用 BuildVars(TriggerContext{}) 生成默认变量。
//   - 渲染错误（如引用未定义变量）返回原始 tpl + error，调用方按需降级。
func (e *Engine) Render(tpl string, vars *TriggerVars) (string, error) {
	if !hasTemplate(tpl) {
		return tpl, nil
	}
	if vars == nil {
		vars = e.BuildVars(TriggerContext{})
	}
	t, err := e.compile(tpl)
	if err != nil {
		return tpl, fmt.Errorf("paramtpl: compile %q: %w", tpl, err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, e.mergeData(vars)); err != nil {
		return tpl, fmt.Errorf("paramtpl: execute: %w", err)
	}
	return buf.String(), nil
}

// Validate 仅做语法检查，不渲染。语法合法返回 nil。
func (e *Engine) Validate(tpl string) error {
	if !hasTemplate(tpl) {
		return nil
	}
	if _, err := e.compile(tpl); err != nil {
		return fmt.Errorf("paramtpl: invalid template: %w", err)
	}
	return nil
}

// MustRender 与 Render 相同，但渲染失败时 panic。适合启动时的配置校验。
func (e *Engine) MustRender(tpl string, vars *TriggerVars) string {
	result, err := e.Render(tpl, vars)
	if err != nil {
		panic(err)
	}
	return result
}

// CacheLen 返回已缓存模板数量（供测试与监控使用）。
func (e *Engine) CacheLen() int {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return len(e.cache)
}

// ClearCache 清空编译缓存（供测试使用）。
func (e *Engine) ClearCache() {
	e.mu.Lock()
	e.cache = make(map[string]*template.Template, 64)
	e.mu.Unlock()
}

// ─── 内部方法 ─────────────────────────────────────────────────────────────────

func (e *Engine) compile(tpl string) (*template.Template, error) {
	e.mu.RLock()
	if t, ok := e.cache[tpl]; ok {
		e.mu.RUnlock()
		return t, nil
	}
	e.mu.RUnlock()

	t, err := template.New("p").
		Option("missingkey=error"). // 引用未定义 key 时报错，而非静默输出 <no value>
		Funcs(e.funcMap).
		Parse(tpl)
	if err != nil {
		return nil, err
	}

	e.mu.Lock()
	if e.cfg.cacheSize > 0 && len(e.cache) >= e.cfg.cacheSize {
		e.cache = make(map[string]*template.Template, 64) // 简单 LRU：满则清空
	}
	if e.cfg.cacheSize > 0 {
		e.cache[tpl] = t
	}
	e.mu.Unlock()
	return t, nil
}

// mergeData 将 TriggerVars 展开为 map[string]any，Extra 优先级最高。
func (e *Engine) mergeData(v *TriggerVars) map[string]any {
	data := map[string]any{
		"Now": v.Now, "Date": v.Date, "DateTime": v.DateTime,
		"DateHour": v.DateHour, "Yesterday": v.Yesterday, "Tomorrow": v.Tomorrow,
		"StartOfDay": v.StartOfDay, "EndOfDay": v.EndOfDay,
		"Unix": v.Unix, "UnixMilli": v.UnixMilli,
		"Weekday": v.Weekday, "Month": v.Month, "Year": v.Year,
		"ShardIndex": v.ShardIndex, "ShardTotal": v.ShardTotal,
		"JobID": v.JobID, "TriggerType": v.TriggerType,
		"NodeID": v.NodeID, "Env": v.Env,
	}
	for k, val := range v.extra {
		data[k] = val
	}
	return data
}

// hasTemplate 快速判断字符串是否含 {{ 占位符（避免不必要的编译）。
func hasTemplate(s string) bool {
	for i := 0; i+1 < len(s); i++ {
		if s[i] == '{' && s[i+1] == '{' {
			return true
		}
	}
	return false
}

// ─── 内置 FuncMap ─────────────────────────────────────────────────────────────

func builtinFuncMap() template.FuncMap {
	return template.FuncMap{
		"daysAgo":    func(n int) string { return time.Now().AddDate(0, 0, -n).Format("2006-01-02") },
		"daysAfter":  func(n int) string { return time.Now().AddDate(0, 0, n).Format("2006-01-02") },
		"formatDate": func(t time.Time, layout string) string { return t.Format(layout) },
		"add":        func(a, b int) int { return a + b },
		"mod":        func(a, b int) int { return a % b },
		"default": func(def, val string) string {
			if val == "" {
				return def
			}
			return val
		},
		"now": func() int64 { return time.Now().Unix() },
	}
}
