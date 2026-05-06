package admin

import (
	"strconv"

	"github.com/gin-gonic/gin"

	"github.com/jiujuan/go-jobs/api/response"
	"github.com/jiujuan/go-jobs/internal/service"
)

// JobTemplateHandler 处理任务模板相关的 HTTP 请求。
type JobTemplateHandler struct {
	svc *service.JobTemplateService
}

// NewJobTemplateHandler 创建 JobTemplateHandler。
func NewJobTemplateHandler(svc *service.JobTemplateService) *JobTemplateHandler {
	return &JobTemplateHandler{svc: svc}
}

// CreateTemplate POST /api/job-templates
func (h *JobTemplateHandler) CreateTemplate(c *gin.Context) {
	var req service.CreateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	if req.CreateUser == "" {
		req.CreateUser = currentUsername(c)
	}
	tpl, err := h.svc.CreateTemplate(c.Request.Context(), &req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, tpl)
}

// UpdateTemplate PUT /api/job-templates/:name
func (h *JobTemplateHandler) UpdateTemplate(c *gin.Context) {
	var req service.UpdateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	req.Name = c.Param("name")
	tpl, err := h.svc.UpdateTemplate(c.Request.Context(), &req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, tpl)
}

// DeleteTemplate DELETE /api/job-templates/:name
func (h *JobTemplateHandler) DeleteTemplate(c *gin.Context) {
	name := c.Param("name")
	if err := h.svc.DeleteTemplate(c.Request.Context(), name); err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, nil)
}

// GetTemplate GET /api/job-templates/:name
func (h *JobTemplateHandler) GetTemplate(c *gin.Context) {
	name := c.Param("name")
	tpl, err := h.svc.GetTemplate(c.Request.Context(), name)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, tpl)
}

// GetTemplateByID GET /api/job-templates/id/:id
func (h *JobTemplateHandler) GetTemplateByID(c *gin.Context) {
	id, err := strconv.ParseInt(c.Param("id"), 10, 64)
	if err != nil {
		response.BadRequest(c, "invalid id")
		return
	}
	tpl, err := h.svc.GetTemplateByID(c.Request.Context(), id)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, tpl)
}

// ListTemplates GET /api/job-templates
func (h *JobTemplateHandler) ListTemplates(c *gin.Context) {
	req := &service.ListTemplatesRequest{
		LabelKey:   c.Query("label_key"),
		LabelValue: c.Query("label_value"),
	}
	list := h.svc.ListTemplates(c.Request.Context(), req)
	response.OK(c, gin.H{"list": list, "total": len(list)})
}

// InstantiateTemplate POST /api/job-templates/:name/instantiate
func (h *JobTemplateHandler) InstantiateTemplate(c *gin.Context) {
	var req service.InstantiateTemplateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	req.TemplateName = c.Param("name")
	if req.CreateUser == "" {
		req.CreateUser = currentUsername(c)
	}
	job, err := h.svc.InstantiateTemplate(c.Request.Context(), &req)
	if err != nil {
		response.Fail(c, err)
		return
	}
	response.OK(c, job)
}

// BatchInstantiateTemplate POST /api/job-templates/:name/batch-instantiate
func (h *JobTemplateHandler) BatchInstantiateTemplate(c *gin.Context) {
	var req service.BatchInstantiateRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		response.BadRequest(c, err.Error())
		return
	}
	req.TemplateName = c.Param("name")
	results := h.svc.BatchInstantiateTemplate(c.Request.Context(), &req)
	response.OK(c, gin.H{"results": results, "total": len(results)})
}

// currentUsername 从 gin context 读取当前用户名（复用已有 middleware）。
func currentUsername(c *gin.Context) string {
	if u, ok := c.Get("username"); ok {
		if s, ok := u.(string); ok {
			return s
		}
	}
	return "system"
}
