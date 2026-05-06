// Package jobtpl 提供任务模板化能力：将一组任务配置抽象为可复用的"模板"，
// 支持从模板批量实例化 JobInfo，并允许在实例化时覆盖部分字段。
//
// # 设计目标
//
// 在以下场景中消除配置重复：
//  1. 同一批数据处理任务：相同执行类型、相同路由策略，仅 handler 和参数不同。
//  2. 多环境部署：dev/staging/prod 使用相同模板，覆盖 ExecutorApp 即可。
//  3. 批量创建分片任务：同一模板生成 N 个任务，ShardingNum 和 Param 不同。
//
// # 概念
//
//   JobTemplate  —— 模板定义，包含 JobInfo 的基础字段（均可被 Override 覆盖）
//   Override     —— 实例化时的字段覆盖，只有非零值才会覆盖模板对应字段
//   Instantiate  —— 从模板 + Override 生成一个完整的 model.JobInfo（不写 DB）
//
// # 存储
//
// Registry 提供纯内存存储，适合小规模（数百条）模板管理。
// 若需持久化，将 Registry.Export() 序列化为 JSON/YAML 存储即可。
// 生产场景可在 Service 层封装 DB 持久化，Registry 作为 L1 缓存。
//
// # 线程安全
//
// 全部公开方法均为 goroutine-safe。
package jobtpl

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 错误 ─────────────────────────────────────────────────────────────────────

var (
	// ErrTemplateNotFound 模板不存在。
	ErrTemplateNotFound = errors.New("jobtpl: template not found")
	// ErrTemplateExists 模板已存在（创建时重名）。
	ErrTemplateExists = errors.New("jobtpl: template already exists")
	// ErrInvalidTemplate 模板定义不合法（如缺少必填字段）。
	ErrInvalidTemplate = errors.New("jobtpl: invalid template")
)

// ─── JobTemplate ──────────────────────────────────────────────────────────────

// JobTemplate 是任务模板定义。
//
// 字段含义与 model.JobInfo 保持一致；模板中不设置的字段将在实例化时
// 使用 model.JobInfo 的零值（调用方可通过 Override 提供）。
type JobTemplate struct {
	// 标识
	ID          int64     `json:"id"`
	Name        string    `json:"name"`         // 模板名称，唯一
	Description string    `json:"description"`  // 描述

	// ── 任务属性（与 model.JobInfo 对齐）────────────────────────────────
	ExecutorApp     string               `json:"executor_app"`
	ExecuteType     model.ExecuteType    `json:"execute_type"`
	ExecuteHandler  string               `json:"execute_handler"`
	ExecuteParam    string               `json:"execute_param"`   // 支持 paramtpl 语法
	RouteStrategy   model.RouteStrategy  `json:"route_strategy"`
	BlockStrategy   model.BlockStrategy  `json:"block_strategy"`
	MisfireStrategy model.MisfireStrategy `json:"misfire_strategy"`
	JobType         model.JobType        `json:"job_type"`
	CronExpression  string               `json:"cron_expression"`
	Timeout         int                  `json:"timeout"`
	RetryCount      int                  `json:"retry_count"`
	RetryInterval   int                  `json:"retry_interval"`
	ShardingNum     int                  `json:"sharding_num"`
	AlarmEmail      string               `json:"alarm_email"`
	AlarmWebhook    string               `json:"alarm_webhook"`

	// 元信息
	Labels     map[string]string `json:"labels"`      // 自定义标签，如 team/env/tier
	CreateUser string            `json:"create_user"`
	CreateTime time.Time         `json:"create_time"`
	UpdateTime time.Time         `json:"update_time"`
}

// Validate 检查模板的必填字段。
func (t *JobTemplate) Validate() error {
	if t.Name == "" {
		return fmt.Errorf("%w: name is required", ErrInvalidTemplate)
	}
	if t.ExecutorApp == "" {
		return fmt.Errorf("%w: executor_app is required", ErrInvalidTemplate)
	}
	if t.ExecuteHandler == "" {
		return fmt.Errorf("%w: execute_handler is required", ErrInvalidTemplate)
	}
	return nil
}

// Clone 返回模板的深拷贝（Labels map 也深拷贝）。
func (t *JobTemplate) Clone() *JobTemplate {
	cp := *t
	if t.Labels != nil {
		cp.Labels = make(map[string]string, len(t.Labels))
		for k, v := range t.Labels {
			cp.Labels[k] = v
		}
	}
	return &cp
}

// ─── Override ─────────────────────────────────────────────────────────────────

// Override 是实例化时对模板字段的覆盖声明。
//
// 只有显式设置（非零值）的字段才会覆盖模板对应字段；
// 未设置的字段继承模板值。这允许每次实例化只声明"与模板不同的部分"。
//
// 特殊规则：
//   - JobName 为必填（每个实例必须有唯一名称）
//   - ExecutorID 为必填（实例化时需要绑定具体执行器记录）
type Override struct {
	// 必填
	JobName    string `json:"job_name"`
	ExecutorID int64  `json:"executor_id"`

	// 可选覆盖（零值 = 继承模板）
	ExecutorApp     string               `json:"executor_app,omitempty"`
	ExecuteType     model.ExecuteType    `json:"execute_type,omitempty"`
	ExecuteHandler  string               `json:"execute_handler,omitempty"`
	ExecuteParam    string               `json:"execute_param,omitempty"`
	RouteStrategy   model.RouteStrategy  `json:"route_strategy,omitempty"`
	BlockStrategy   model.BlockStrategy  `json:"block_strategy,omitempty"`
	MisfireStrategy model.MisfireStrategy `json:"misfire_strategy,omitempty"`
	JobType         model.JobType        `json:"job_type,omitempty"`
	CronExpression  string               `json:"cron_expression,omitempty"`
	Timeout         int                  `json:"timeout,omitempty"`
	RetryCount      int                  `json:"retry_count,omitempty"`
	RetryInterval   int                  `json:"retry_interval,omitempty"`
	ShardingNum     int                  `json:"sharding_num,omitempty"`
	AlarmEmail      string               `json:"alarm_email,omitempty"`
	AlarmWebhook    string               `json:"alarm_webhook,omitempty"`
	JobDesc         string               `json:"job_desc,omitempty"`
	CreateUser      string               `json:"create_user,omitempty"`
	GroupID         int64                `json:"group_id,omitempty"`
}

// Validate 检查 Override 的必填字段。
func (o *Override) Validate() error {
	if o.JobName == "" {
		return fmt.Errorf("%w: override.job_name is required", ErrInvalidTemplate)
	}
	if o.ExecutorID <= 0 {
		return fmt.Errorf("%w: override.executor_id must be > 0", ErrInvalidTemplate)
	}
	return nil
}

// ─── Instantiate ──────────────────────────────────────────────────────────────

// Instantiate 从模板和覆盖声明生成一个 model.JobInfo。
//
// 仅做内存合并，不写 DB，调用方负责持久化（如通过 JobService.CreateJob）。
// 覆盖规则：Override 中非零值字段优先，零值字段继承模板。
func Instantiate(tpl *JobTemplate, ov *Override) (*model.JobInfo, error) {
	if tpl == nil {
		return nil, fmt.Errorf("%w: template is nil", ErrInvalidTemplate)
	}
	if err := ov.Validate(); err != nil {
		return nil, err
	}

	job := &model.JobInfo{
		// 来自 Override（必填）
		JobName:    ov.JobName,
		ExecutorID: ov.ExecutorID,

		// 继承模板，按需覆盖
		ExecutorApp:     coalesceStr(ov.ExecutorApp, tpl.ExecutorApp),
		ExecuteType:     coalesceExecType(ov.ExecuteType, tpl.ExecuteType),
		ExecuteHandler:  coalesceStr(ov.ExecuteHandler, tpl.ExecuteHandler),
		ExecuteParam:    coalesceStr(ov.ExecuteParam, tpl.ExecuteParam),
		RouteStrategy:   coalesceRoute(ov.RouteStrategy, tpl.RouteStrategy),
		BlockStrategy:   coalesceBlock(ov.BlockStrategy, tpl.BlockStrategy),
		MisfireStrategy: coalesceMisfire(ov.MisfireStrategy, tpl.MisfireStrategy),
		JobType:         coalesceJobType(ov.JobType, tpl.JobType),
		CronExpression:  coalesceStr(ov.CronExpression, tpl.CronExpression),
		Timeout:         coalesceInt(ov.Timeout, tpl.Timeout),
		RetryCount:      coalesceInt(ov.RetryCount, tpl.RetryCount),
		RetryInterval:   coalesceInt(ov.RetryInterval, tpl.RetryInterval),
		ShardingNum:     coalesceInt(ov.ShardingNum, tpl.ShardingNum),
		AlarmEmail:      coalesceStr(ov.AlarmEmail, tpl.AlarmEmail),
		AlarmWebhook:    coalesceStr(ov.AlarmWebhook, tpl.AlarmWebhook),
		JobDesc:         coalesceStr(ov.JobDesc, tpl.Description),
		CreateUser:      coalesceStr(ov.CreateUser, tpl.CreateUser),
		GroupID:         coalesceInt64(ov.GroupID, 0),

		Status: model.JobStop, // 新实例默认停止，由调用方决定是否启动
	}

	// ShardingNum 最小为 1
	if job.ShardingNum < 1 {
		job.ShardingNum = 1
	}

	return job, nil
}

// ─── Registry ─────────────────────────────────────────────────────────────────

// Registry 是内存模板注册表，提供 CRUD 和批量操作，线程安全。
type Registry struct {
	mu        sync.RWMutex
	templates map[string]*JobTemplate // key = template.Name
	nameToID  map[int64]string        // id → name 索引，用于 GetByID
	nextID    int64                   // 原子自增 ID
}

// NewRegistry 创建空注册表。
func NewRegistry() *Registry {
	return &Registry{
		templates: make(map[string]*JobTemplate, 16),
		nameToID:  make(map[int64]string, 16),
	}
}

// Create 创建新模板。name 必须唯一。
func (r *Registry) Create(tpl *JobTemplate) (*JobTemplate, error) {
	if err := tpl.Validate(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.templates[tpl.Name]; ok {
		return nil, fmt.Errorf("%w: %q", ErrTemplateExists, tpl.Name)
	}

	cp := tpl.Clone()
	cp.ID = atomic.AddInt64(&r.nextID, 1)
	cp.CreateTime = time.Now()
	cp.UpdateTime = cp.CreateTime

	r.templates[cp.Name] = cp
	r.nameToID[cp.ID] = cp.Name
	return cp.Clone(), nil // 返回副本，外部不能修改注册表内部状态
}

// Update 更新已有模板（名称不可更改）。
func (r *Registry) Update(tpl *JobTemplate) (*JobTemplate, error) {
	if err := tpl.Validate(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	existing, ok := r.templates[tpl.Name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTemplateNotFound, tpl.Name)
	}

	cp := tpl.Clone()
	cp.ID = existing.ID
	cp.CreateTime = existing.CreateTime
	cp.UpdateTime = time.Now()

	r.templates[cp.Name] = cp
	return cp.Clone(), nil
}

// Delete 删除模板。
func (r *Registry) Delete(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	tpl, ok := r.templates[name]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTemplateNotFound, name)
	}
	delete(r.templates, name)
	delete(r.nameToID, tpl.ID)
	return nil
}

// Get 按名称查询模板，返回深拷贝。
func (r *Registry) Get(name string) (*JobTemplate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tpl, ok := r.templates[name]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrTemplateNotFound, name)
	}
	return tpl.Clone(), nil
}

// GetByID 按 ID 查询模板。
func (r *Registry) GetByID(id int64) (*JobTemplate, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	name, ok := r.nameToID[id]
	if !ok {
		return nil, fmt.Errorf("%w: id=%d", ErrTemplateNotFound, id)
	}
	return r.templates[name].Clone(), nil
}

// List 返回所有模板的快照（深拷贝列表）。
// filter 为可选过滤函数，nil 表示返回全部。
func (r *Registry) List(filter func(*JobTemplate) bool) []*JobTemplate {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*JobTemplate, 0, len(r.templates))
	for _, tpl := range r.templates {
		if filter == nil || filter(tpl) {
			out = append(out, tpl.Clone())
		}
	}
	return out
}

// Len 返回已注册模板数量。
func (r *Registry) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.templates)
}

// Instantiate 从指定名称的模板实例化一个 JobInfo。
func (r *Registry) Instantiate(name string, ov *Override) (*model.JobInfo, error) {
	tpl, err := r.Get(name)
	if err != nil {
		return nil, err
	}
	return Instantiate(tpl, ov)
}

// InstantiateByID 从模板 ID 实例化。
func (r *Registry) InstantiateByID(id int64, ov *Override) (*model.JobInfo, error) {
	tpl, err := r.GetByID(id)
	if err != nil {
		return nil, err
	}
	return Instantiate(tpl, ov)
}

// BatchInstantiate 从同一模板批量实例化多个 JobInfo。
// 任意一个 Override 不合法时返回错误，已成功的结果一同返回（部分成功语义）。
func (r *Registry) BatchInstantiate(name string, overrides []*Override) ([]*model.JobInfo, error) {
	tpl, err := r.Get(name)
	if err != nil {
		return nil, err
	}

	jobs := make([]*model.JobInfo, 0, len(overrides))
	var errs []error
	for i, ov := range overrides {
		job, instErr := Instantiate(tpl, ov)
		if instErr != nil {
			errs = append(errs, fmt.Errorf("override[%d]: %w", i, instErr))
			continue
		}
		jobs = append(jobs, job)
	}

	if len(errs) > 0 {
		return jobs, fmt.Errorf("batch instantiate partial failure (%d errors): %v", len(errs), errs[0])
	}
	return jobs, nil
}

// Export 导出所有模板（用于序列化/备份）。
func (r *Registry) Export() []*JobTemplate {
	return r.List(nil)
}

// Import 批量导入模板（幂等：已存在则更新，不存在则创建）。
func (r *Registry) Import(templates []*JobTemplate) error {
	for _, tpl := range templates {
		if _, err := r.Get(tpl.Name); err != nil {
			if _, err2 := r.Create(tpl); err2 != nil {
				return fmt.Errorf("import %q: %w", tpl.Name, err2)
			}
		} else {
			if _, err2 := r.Update(tpl); err2 != nil {
				return fmt.Errorf("import update %q: %w", tpl.Name, err2)
			}
		}
	}
	return nil
}

// ─── coalesce 辅助函数 ────────────────────────────────────────────────────────

func coalesceStr(override, fallback string) string {
	if override != "" {
		return override
	}
	return fallback
}

func coalesceInt(override, fallback int) int {
	if override != 0 {
		return override
	}
	return fallback
}

func coalesceInt64(override, fallback int64) int64 {
	if override != 0 {
		return override
	}
	return fallback
}

func coalesceExecType(override, fallback model.ExecuteType) model.ExecuteType {
	if override != "" {
		return override
	}
	return fallback
}

func coalesceRoute(override, fallback model.RouteStrategy) model.RouteStrategy {
	if override != "" {
		return override
	}
	return fallback
}

func coalesceBlock(override, fallback model.BlockStrategy) model.BlockStrategy {
	if override != 0 {
		return override
	}
	return fallback
}

func coalesceMisfire(override, fallback model.MisfireStrategy) model.MisfireStrategy {
	if override != 0 {
		return override
	}
	return fallback
}

func coalesceJobType(override, fallback model.JobType) model.JobType {
	if override != 0 {
		return override
	}
	return fallback
}
