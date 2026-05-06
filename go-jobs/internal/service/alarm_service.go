// Package service - alarm_service.go
// 告警服务：支持邮件、钉钉、企业微信、通用 Webhook 四种通道。
package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/smtp"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// ─── 告警级别 ──────────────────────────────────────────────────────────────────

// AlarmLevel 告警严重程度。
type AlarmLevel string

const (
	AlarmLevelWarn  AlarmLevel = "WARN"
	AlarmLevelError AlarmLevel = "ERROR"
)

// ─── 告警事件 ──────────────────────────────────────────────────────────────────

// AlarmEvent 包含一次告警所需的全部信息。
type AlarmEvent struct {
	JobID   int64      `json:"job_id"`
	JobName string     `json:"job_name"`
	LogID   int64      `json:"log_id"`
	Level   AlarmLevel `json:"level"`
	Message string     `json:"message"`
	Time    time.Time  `json:"time"`
}

// ─── 告警接口 ──────────────────────────────────────────────────────────────────

// Alarmer 每种告警通道都必须实现该接口。
type Alarmer interface {
	Send(ctx context.Context, event AlarmEvent) error
}

// ─── 邮件告警 ──────────────────────────────────────────────────────────────────

// EmailAlarmer 通过 SMTP 发送邮件告警。
type EmailAlarmer struct {
	Host     string
	Port     int
	Username string
	Password string
	From     string
	To       []string
}

func (a *EmailAlarmer) Send(ctx context.Context, ev AlarmEvent) error {
	auth := smtp.PlainAuth("", a.Username, a.Password, a.Host)
	subject := fmt.Sprintf("[go-jobs][%s] Job #%d - %s", ev.Level, ev.JobID, ev.JobName)
	body := fmt.Sprintf(
		"任务: %s (ID: %d)\n日志ID: %d\n级别: %s\n信息: %s\n时间: %s",
		ev.JobName, ev.JobID, ev.LogID, ev.Level, ev.Message,
		ev.Time.Format("2006-01-02 15:04:05"),
	)
	msg := []byte("Subject: " + subject + "\r\nContent-Type: text/plain; charset=utf-8\r\n\r\n" + body)
	addr := fmt.Sprintf("%s:%d", a.Host, a.Port)
	return smtp.SendMail(addr, auth, a.From, a.To, msg)
}

// ─── 钉钉告警 ──────────────────────────────────────────────────────────────────

// DingtalkAlarmer 发送钉钉机器人消息（Markdown 卡片）。
type DingtalkAlarmer struct {
	WebhookURL string
	Secret     string // 可选：钉钉安全签名密钥
}

func (a *DingtalkAlarmer) Send(ctx context.Context, ev AlarmEvent) error {
	levelEmoji := "⚠️"
	if ev.Level == AlarmLevelError {
		levelEmoji = "🔴"
	}
	text := fmt.Sprintf(
		"## %s [go-jobs] 任务告警\n\n"+
			"- **任务名**: %s (ID: %d)\n"+
			"- **日志ID**: %d\n"+
			"- **级别**: %s %s\n"+
			"- **信息**: %s\n"+
			"- **时间**: %s",
		levelEmoji,
		ev.JobName, ev.JobID, ev.LogID,
		ev.Level, levelEmoji,
		ev.Message,
		ev.Time.Format("2006-01-02 15:04:05"),
	)
	payload := map[string]interface{}{
		"msgtype": "markdown",
		"markdown": map[string]string{
			"title": fmt.Sprintf("[go-jobs] %s 告警 - %s", ev.Level, ev.JobName),
			"text":  text,
		},
	}
	return a.postJSON(ctx, a.WebhookURL, payload)
}

// ─── 企业微信告警 ──────────────────────────────────────────────────────────────

// WeComAlarmer 发送企业微信群机器人消息。
type WeComAlarmer struct {
	WebhookURL string
}

func (a *WeComAlarmer) Send(ctx context.Context, ev AlarmEvent) error {
	content := fmt.Sprintf(
		"[go-jobs] 任务告警\n"+
			"任务: %s (ID: %d)\n"+
			"级别: %s\n"+
			"信息: %s\n"+
			"时间: %s",
		ev.JobName, ev.JobID,
		ev.Level,
		ev.Message,
		ev.Time.Format("2006-01-02 15:04:05"),
	)
	payload := map[string]interface{}{
		"msgtype": "text",
		"text": map[string]string{
			"content": content,
		},
	}
	return a.postJSON(ctx, a.WebhookURL, payload)
}

func (a *WeComAlarmer) postJSON(ctx context.Context, url string, v interface{}) error {
	return postJSON(ctx, url, v)
}

// ─── Webhook 告警 ──────────────────────────────────────────────────────────────

// WebhookAlarmer 向任意 HTTP endpoint POST 告警事件 JSON。
type WebhookAlarmer struct {
	URL     string
	Headers map[string]string // 可选：附加请求头（如 Authorization）
}

func (a *WebhookAlarmer) Send(ctx context.Context, ev AlarmEvent) error {
	data, _ := json.Marshal(ev)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.URL, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("webhook: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range a.Headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: post: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook: server returned %d", resp.StatusCode)
	}
	return nil
}

// ─── 告警服务 ──────────────────────────────────────────────────────────────────

// AlarmService 将告警事件分发到所有已注册的通道。
type AlarmService struct {
	alarmers []Alarmer
}

// NewAlarmService 创建 AlarmService。
func NewAlarmService(alarmers ...Alarmer) *AlarmService {
	return &AlarmService{alarmers: alarmers}
}

// Notify 将告警事件发送到全部通道，失败仅记录日志，不阻塞。
func (s *AlarmService) Notify(ctx context.Context, ev AlarmEvent) {
	for _, a := range s.alarmers {
		if err := a.Send(ctx, ev); err != nil {
			logger.Warn("alarm: send failed",
				zap.String("level", string(ev.Level)),
				zap.Int64("jobID", ev.JobID),
				zap.Error(err))
		}
	}
}

// NotifyJobFailed 封装任务失败告警。
func (s *AlarmService) NotifyJobFailed(ctx context.Context, job *model.JobInfo, log *model.JobLog) {
	s.Notify(ctx, AlarmEvent{
		JobID:   job.ID,
		JobName: job.JobName,
		LogID:   log.ID,
		Level:   AlarmLevelError,
		Message: fmt.Sprintf("任务执行失败: %s", log.ErrorMsg),
		Time:    time.Now(),
	})
}

// NotifyJobTimeout 封装任务超时告警。
func (s *AlarmService) NotifyJobTimeout(ctx context.Context, job *model.JobInfo, log *model.JobLog) {
	s.Notify(ctx, AlarmEvent{
		JobID:   job.ID,
		JobName: job.JobName,
		LogID:   log.ID,
		Level:   AlarmLevelWarn,
		Message: fmt.Sprintf("任务执行超时 (timeout=%ds)", job.Timeout),
		Time:    time.Now(),
	})
}

// ─── 公共 HTTP 辅助 ───────────────────────────────────────────────────────────

func postJSON(ctx context.Context, url string, v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("postJSON: marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("postJSON: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("postJSON: do: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("postJSON: server returned %d", resp.StatusCode)
	}
	return nil
}

