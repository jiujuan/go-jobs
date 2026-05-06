// Package service — JobTemplateService 将任务模板能力暴露为业务服务。
package service

import (
	"context"
	"fmt"

	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/jobtpl"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

// ─── JobTemplateService ───────────────────────────────────────────────────────

// JobTemplateService 提供任务模板的 CRUD 和实例化操作。
//
// 存储：使用内存 Registry（jobtpl.Registry）作为主存储。
// 若需持久化，可在此 Service 中添加 DAO 层；当前版本聚焦业务逻辑。
type JobTemplateService struct {
	registry *jobtpl.Registry
	jobSvc   *JobService // 实例化后创建 job 时使用
}

// NewJobTemplateService 创建 JobTemplateService。
func NewJobTemplateService(registry *jobtpl.Registry, jobSvc *JobService) *JobTemplateService {
	return &JobTemplateService{
		registry: registry,
		jobSvc:   jobSvc,
	}
}

// ─── 模板 CRUD ────────────────────────────────────────────────────────────────

// CreateTemplate 创建新任务模板。
func (s *JobTemplateService) CreateTemplate(ctx context.Context, req *CreateTemplateRequest) (*jobtpl.JobTemplate, error) {
	tpl := &jobtpl.JobTemplate{
		Name:            req.Name,
		Description:     req.Description,
		ExecutorApp:     req.ExecutorApp,
		ExecuteType:     req.ExecuteType,
		ExecuteHandler:  req.ExecuteHandler,
		ExecuteParam:    req.ExecuteParam,
		RouteStrategy:   req.RouteStrategy,
		BlockStrategy:   req.BlockStrategy,
		MisfireStrategy: req.MisfireStrategy,
		JobType:         req.JobType,
		CronExpression:  req.CronExpression,
		Timeout:         req.Timeout,
		RetryCount:      req.RetryCount,
		RetryInterval:   req.RetryInterval,
		ShardingNum:     req.ShardingNum,
		AlarmEmail:      req.AlarmEmail,
		AlarmWebhook:    req.AlarmWebhook,
		Labels:          req.Labels,
		CreateUser:      req.CreateUser,
	}

	created, err := s.registry.Create(tpl)
	if err != nil {
		if isTemplateExists(err) {
			return nil, xerror.New(xerror.CodeAlreadyExists, fmt.Sprintf("template %q already exists", req.Name))
		}
		return nil, xerror.Wrap(xerror.CodeInvalidParam, err)
	}
	return created, nil
}

// UpdateTemplate 更新已有模板。
func (s *JobTemplateService) UpdateTemplate(ctx context.Context, req *UpdateTemplateRequest) (*jobtpl.JobTemplate, error) {
	existing, err := s.registry.Get(req.Name)
	if err != nil {
		return nil, xerror.New(xerror.CodeNotFound, fmt.Sprintf("template %q not found", req.Name))
	}

	// 合并更新：只覆盖请求中明确设置的字段
	if req.Description != "" {
		existing.Description = req.Description
	}
	if req.ExecutorApp != "" {
		existing.ExecutorApp = req.ExecutorApp
	}
	if req.ExecuteHandler != "" {
		existing.ExecuteHandler = req.ExecuteHandler
	}
	if req.ExecuteParam != "" {
		existing.ExecuteParam = req.ExecuteParam
	}
	if req.ExecuteType != "" {
		existing.ExecuteType = req.ExecuteType
	}
	if req.RouteStrategy != "" {
		existing.RouteStrategy = req.RouteStrategy
	}
	if req.BlockStrategy != 0 {
		existing.BlockStrategy = req.BlockStrategy
	}
	if req.MisfireStrategy != 0 {
		existing.MisfireStrategy = req.MisfireStrategy
	}
	if req.JobType != 0 {
		existing.JobType = req.JobType
	}
	if req.CronExpression != "" {
		existing.CronExpression = req.CronExpression
	}
	if req.Timeout != 0 {
		existing.Timeout = req.Timeout
	}
	if req.RetryCount != 0 {
		existing.RetryCount = req.RetryCount
	}
	if req.RetryInterval != 0 {
		existing.RetryInterval = req.RetryInterval
	}
	if req.ShardingNum != 0 {
		existing.ShardingNum = req.ShardingNum
	}
	if req.AlarmEmail != "" {
		existing.AlarmEmail = req.AlarmEmail
	}
	if req.AlarmWebhook != "" {
		existing.AlarmWebhook = req.AlarmWebhook
	}
	if req.Labels != nil {
		existing.Labels = req.Labels
	}

	updated, err := s.registry.Update(existing)
	if err != nil {
		return nil, xerror.Wrap(xerror.CodeInternalServer, err)
	}
	return updated, nil
}

// DeleteTemplate 删除模板。
func (s *JobTemplateService) DeleteTemplate(ctx context.Context, name string) error {
	if err := s.registry.Delete(name); err != nil {
		return xerror.New(xerror.CodeNotFound, fmt.Sprintf("template %q not found", name))
	}
	return nil
}

// GetTemplate 按名称查询模板。
func (s *JobTemplateService) GetTemplate(ctx context.Context, name string) (*jobtpl.JobTemplate, error) {
	tpl, err := s.registry.Get(name)
	if err != nil {
		return nil, xerror.New(xerror.CodeNotFound, fmt.Sprintf("template %q not found", name))
	}
	return tpl, nil
}

// GetTemplateByID 按 ID 查询模板。
func (s *JobTemplateService) GetTemplateByID(ctx context.Context, id int64) (*jobtpl.JobTemplate, error) {
	tpl, err := s.registry.GetByID(id)
	if err != nil {
		return nil, xerror.New(xerror.CodeNotFound, fmt.Sprintf("template id=%d not found", id))
	}
	return tpl, nil
}

// ListTemplates 查询模板列表，支持标签过滤。
func (s *JobTemplateService) ListTemplates(ctx context.Context, req *ListTemplatesRequest) []*jobtpl.JobTemplate {
	var filter func(*jobtpl.JobTemplate) bool
	if req != nil && req.LabelKey != "" && req.LabelValue != "" {
		filter = func(t *jobtpl.JobTemplate) bool {
			return t.Labels[req.LabelKey] == req.LabelValue
		}
	}
	return s.registry.List(filter)
}

// ─── 实例化 ────────────────────────────────────────────────────────────────────

// InstantiateResult 是单次实例化的结果。
type InstantiateResult struct {
	Job   *model.JobInfo `json:"job"`
	Error string         `json:"error,omitempty"`
}

// InstantiateTemplate 从模板实例化并创建一个任务（写入 DB）。
func (s *JobTemplateService) InstantiateTemplate(ctx context.Context, req *InstantiateTemplateRequest) (*model.JobInfo, error) {
	ov := &jobtpl.Override{
		JobName:         req.JobName,
		ExecutorID:      req.ExecutorID,
		ExecutorApp:     req.ExecutorApp,
		ExecuteType:     req.ExecuteType,
		ExecuteHandler:  req.ExecuteHandler,
		ExecuteParam:    req.ExecuteParam,
		RouteStrategy:   req.RouteStrategy,
		BlockStrategy:   req.BlockStrategy,
		MisfireStrategy: req.MisfireStrategy,
		JobType:         req.JobType,
		CronExpression:  req.CronExpression,
		Timeout:         req.Timeout,
		RetryCount:      req.RetryCount,
		RetryInterval:   req.RetryInterval,
		ShardingNum:     req.ShardingNum,
		AlarmEmail:      req.AlarmEmail,
		AlarmWebhook:    req.AlarmWebhook,
		JobDesc:         req.JobDesc,
		CreateUser:      req.CreateUser,
		GroupID:         req.GroupID,
	}

	jobInfo, err := s.registry.Instantiate(req.TemplateName, ov)
	if err != nil {
		if isTemplateNotFound(err) {
			return nil, xerror.New(xerror.CodeNotFound, fmt.Sprintf("template %q not found", req.TemplateName))
		}
		return nil, xerror.Wrap(xerror.CodeInvalidParam, err)
	}

	// 通过 JobService 持久化
	createReq := jobInfoToCreateRequest(jobInfo)
	return s.jobSvc.CreateJob(ctx, createReq)
}

// BatchInstantiateTemplate 从同一模板批量实例化多个任务。
// 部分失败时仍返回成功列表，通过 results[i].Error 判断各项状态。
func (s *JobTemplateService) BatchInstantiateTemplate(ctx context.Context, req *BatchInstantiateRequest) []*InstantiateResult {
	results := make([]*InstantiateResult, len(req.Overrides))

	for i, ov := range req.Overrides {
		instReq := &InstantiateTemplateRequest{
			TemplateName:    req.TemplateName,
			JobName:         ov.JobName,
			ExecutorID:      ov.ExecutorID,
			ExecutorApp:     ov.ExecutorApp,
			ExecuteType:     ov.ExecuteType,
			ExecuteHandler:  ov.ExecuteHandler,
			ExecuteParam:    ov.ExecuteParam,
			RouteStrategy:   ov.RouteStrategy,
			BlockStrategy:   ov.BlockStrategy,
			MisfireStrategy: ov.MisfireStrategy,
			JobType:         ov.JobType,
			CronExpression:  ov.CronExpression,
			Timeout:         ov.Timeout,
			RetryCount:      ov.RetryCount,
			RetryInterval:   ov.RetryInterval,
			ShardingNum:     ov.ShardingNum,
			AlarmEmail:      ov.AlarmEmail,
			AlarmWebhook:    ov.AlarmWebhook,
			JobDesc:         ov.JobDesc,
			CreateUser:      ov.CreateUser,
			GroupID:         ov.GroupID,
		}

		job, err := s.InstantiateTemplate(ctx, instReq)
		if err != nil {
			results[i] = &InstantiateResult{Error: err.Error()}
		} else {
			results[i] = &InstantiateResult{Job: job}
		}
	}
	return results
}

// ─── Request / Response DTOs ──────────────────────────────────────────────────

// CreateTemplateRequest 是创建模板的请求 DTO。
type CreateTemplateRequest struct {
	Name            string                `json:"name"             binding:"required"`
	Description     string                `json:"description"`
	ExecutorApp     string                `json:"executor_app"     binding:"required"`
	ExecuteType     model.ExecuteType     `json:"execute_type"`
	ExecuteHandler  string                `json:"execute_handler"  binding:"required"`
	ExecuteParam    string                `json:"execute_param"`
	RouteStrategy   model.RouteStrategy   `json:"route_strategy"`
	BlockStrategy   model.BlockStrategy   `json:"block_strategy"`
	MisfireStrategy model.MisfireStrategy `json:"misfire_strategy"`
	JobType         model.JobType         `json:"job_type"`
	CronExpression  string                `json:"cron_expression"`
	Timeout         int                   `json:"timeout"`
	RetryCount      int                   `json:"retry_count"`
	RetryInterval   int                   `json:"retry_interval"`
	ShardingNum     int                   `json:"sharding_num"`
	AlarmEmail      string                `json:"alarm_email"`
	AlarmWebhook    string                `json:"alarm_webhook"`
	Labels          map[string]string     `json:"labels"`
	CreateUser      string                `json:"create_user"`
}

// UpdateTemplateRequest 是更新模板的请求 DTO（仅覆盖非零字段）。
type UpdateTemplateRequest struct {
	Name            string                `json:"name"             binding:"required"`
	Description     string                `json:"description"`
	ExecutorApp     string                `json:"executor_app"`
	ExecuteType     model.ExecuteType     `json:"execute_type"`
	ExecuteHandler  string                `json:"execute_handler"`
	ExecuteParam    string                `json:"execute_param"`
	RouteStrategy   model.RouteStrategy   `json:"route_strategy"`
	BlockStrategy   model.BlockStrategy   `json:"block_strategy"`
	MisfireStrategy model.MisfireStrategy `json:"misfire_strategy"`
	JobType         model.JobType         `json:"job_type"`
	CronExpression  string                `json:"cron_expression"`
	Timeout         int                   `json:"timeout"`
	RetryCount      int                   `json:"retry_count"`
	RetryInterval   int                   `json:"retry_interval"`
	ShardingNum     int                   `json:"sharding_num"`
	AlarmEmail      string                `json:"alarm_email"`
	AlarmWebhook    string                `json:"alarm_webhook"`
	Labels          map[string]string     `json:"labels"`
}

// ListTemplatesRequest 是查询模板列表的请求 DTO。
type ListTemplatesRequest struct {
	LabelKey   string `form:"label_key"`
	LabelValue string `form:"label_value"`
}

// InstantiateTemplateRequest 是实例化模板的请求 DTO（可覆盖任意模板字段）。
type InstantiateTemplateRequest struct {
	TemplateName    string                `json:"template_name"    binding:"required"`
	JobName         string                `json:"job_name"         binding:"required"`
	ExecutorID      int64                 `json:"executor_id"      binding:"required"`
	ExecutorApp     string                `json:"executor_app"`
	ExecuteType     model.ExecuteType     `json:"execute_type"`
	ExecuteHandler  string                `json:"execute_handler"`
	ExecuteParam    string                `json:"execute_param"`
	RouteStrategy   model.RouteStrategy   `json:"route_strategy"`
	BlockStrategy   model.BlockStrategy   `json:"block_strategy"`
	MisfireStrategy model.MisfireStrategy `json:"misfire_strategy"`
	JobType         model.JobType         `json:"job_type"`
	CronExpression  string                `json:"cron_expression"`
	Timeout         int                   `json:"timeout"`
	RetryCount      int                   `json:"retry_count"`
	RetryInterval   int                   `json:"retry_interval"`
	ShardingNum     int                   `json:"sharding_num"`
	AlarmEmail      string                `json:"alarm_email"`
	AlarmWebhook    string                `json:"alarm_webhook"`
	JobDesc         string                `json:"job_desc"`
	CreateUser      string                `json:"create_user"`
	GroupID         int64                 `json:"group_id"`
}

// BatchInstantiateRequest 是批量实例化的请求 DTO。
type BatchInstantiateRequest struct {
	TemplateName string                        `json:"template_name" binding:"required"`
	Overrides    []*InstantiateTemplateRequest `json:"overrides"     binding:"required"`
}

// ─── 工具函数 ─────────────────────────────────────────────────────────────────

// jobInfoToCreateRequest 将 model.JobInfo 转换为 CreateJobRequest。
func jobInfoToCreateRequest(j *model.JobInfo) *CreateJobRequest {
	return &CreateJobRequest{
		ExecutorID:      j.ExecutorID,
		ExecutorApp:     j.ExecutorApp,
		JobName:         j.JobName,
		JobDesc:         j.JobDesc,
		JobType:         j.JobType,
		CronExpression:  j.CronExpression,
		ExecuteType:     j.ExecuteType,
		ExecuteParam:    j.ExecuteParam,
		ExecuteHandler:  j.ExecuteHandler,
		RouteStrategy:   j.RouteStrategy,
		BlockStrategy:   j.BlockStrategy,
		MisfireStrategy: j.MisfireStrategy,
		Timeout:         j.Timeout,
		RetryCount:      j.RetryCount,
		RetryInterval:   j.RetryInterval,
		ShardingNum:     j.ShardingNum,
		AlarmEmail:      j.AlarmEmail,
		AlarmWebhook:    j.AlarmWebhook,
		CreateUser:      j.CreateUser,
	}
}

func isTemplateNotFound(err error) bool {
	return err != nil && (contains(err.Error(), "not found"))
}

func isTemplateExists(err error) bool {
	return err != nil && (contains(err.Error(), "already exists"))
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(s) > 0 && findStr(s, sub))
}

func findStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
