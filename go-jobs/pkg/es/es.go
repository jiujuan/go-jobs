// Package es provides an ElasticSearch client for go-jobs v3 log storage.
// When ES is enabled, execution logs are indexed for full-text search.
package es

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

// Options configures the ES client.
type Options struct {
	Addresses []string
	Username  string
	Password  string
	Index     string
}

// Option is a functional option.
type Option func(*Options)

// WithAddresses sets the ES node addresses.
func WithAddresses(addrs []string) Option { return func(o *Options) { o.Addresses = addrs } }

// WithCredentials sets the username and password.
func WithCredentials(u, p string) Option { return func(o *Options) { o.Username = u; o.Password = p } }

// WithIndex sets the default index name.
func WithIndex(idx string) Option { return func(o *Options) { o.Index = idx } }

// Client wraps the ES client with app-specific helpers.
type Client struct {
	*elasticsearch.Client
	Index string
}

// New creates a new ES Client.
func New(opts ...Option) (*Client, error) {
	o := &Options{Index: "go-jobs-logs"}
	for _, opt := range opts {
		opt(o)
	}
	cli, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: o.Addresses,
		Username:  o.Username,
		Password:  o.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("es: new client: %w", err)
	}
	return &Client{Client: cli, Index: o.Index}, nil
}

// ─── 日志文档 ──────────────────────────────────────────────────────────────────

// LogDocument 表示 ES 中的一条执行日志。
type LogDocument struct {
	LogID           int64     `json:"log_id"`
	JobID           int64     `json:"job_id"`
	JobName         string    `json:"job_name"`
	ExecutorAddress string    `json:"executor_address"`
	Status          int       `json:"status"`
	LogContent      string    `json:"log_content"`
	TriggerTime     time.Time `json:"trigger_time"`
	DurationMs      int64     `json:"duration_ms"`
	ErrorMsg        string    `json:"error_msg"`
	ShardingIndex   int       `json:"sharding_index"`
	ShardingTotal   int       `json:"sharding_total"`
}

// IndexLog 将一条日志文档写入 ES。
func (c *Client) IndexLog(ctx context.Context, doc *LogDocument) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("es: marshal log: %w", err)
	}
	docID := fmt.Sprintf("%d", doc.LogID)
	req := esapi.IndexRequest{
		Index:      c.Index,
		DocumentID: docID,
		Body:       bytes.NewReader(data),
		Refresh:    "false",
	}
	resp, err := req.Do(ctx, c.Client)
	if err != nil {
		return fmt.Errorf("es: index log %d: %w", doc.LogID, err)
	}
	defer resp.Body.Close()
	if resp.IsError() {
		return fmt.Errorf("es: index log %d: status=%s", doc.LogID, resp.Status())
	}
	return nil
}

// SearchLogsRequest 日志搜索请求参数。
type SearchLogsRequest struct {
	Keyword   string    // 全文搜索关键词（搜索 log_content）
	JobID     int64     // 过滤指定任务（0 表示全部）
	Status    int       // 过滤状态（0 表示全部）
	StartTime time.Time // 时间范围起始
	EndTime   time.Time // 时间范围结束
	Page      int
	PageSize  int
}

// SearchLogs 对 ES 中的日志执行全文检索。
func (c *Client) SearchLogs(ctx context.Context, req *SearchLogsRequest) ([]*LogDocument, int64, error) {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 {
		req.PageSize = 20
	}

	must := []map[string]interface{}{}

	if req.Keyword != "" {
		must = append(must, map[string]interface{}{
			"match": map[string]interface{}{
				"log_content": map[string]interface{}{
					"query":    req.Keyword,
					"operator": "and",
				},
			},
		})
	}
	if req.JobID > 0 {
		must = append(must, map[string]interface{}{
			"term": map[string]interface{}{"job_id": req.JobID},
		})
	}
	if req.Status > 0 {
		must = append(must, map[string]interface{}{
			"term": map[string]interface{}{"status": req.Status},
		})
	}
	if !req.StartTime.IsZero() || !req.EndTime.IsZero() {
		rangeFilter := map[string]interface{}{}
		if !req.StartTime.IsZero() {
			rangeFilter["gte"] = req.StartTime.Format(time.RFC3339)
		}
		if !req.EndTime.IsZero() {
			rangeFilter["lte"] = req.EndTime.Format(time.RFC3339)
		}
		must = append(must, map[string]interface{}{
			"range": map[string]interface{}{"trigger_time": rangeFilter},
		})
	}

	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{"must": must},
		},
		"from": (req.Page - 1) * req.PageSize,
		"size": req.PageSize,
		"sort": []map[string]interface{}{{"trigger_time": "desc"}},
	}

	data, _ := json.Marshal(query)
	resp, err := c.Client.Search(
		c.Client.Search.WithContext(ctx),
		c.Client.Search.WithIndex(c.Index),
		c.Client.Search.WithBody(bytes.NewReader(data)),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("es: search: %w", err)
	}
	defer resp.Body.Close()

	if resp.IsError() {
		return nil, 0, fmt.Errorf("es: search returned %s", resp.Status())
	}

	var result struct {
		Hits struct {
			Total struct {
				Value int64 `json:"value"`
			} `json:"total"`
			Hits []struct {
				Source LogDocument `json:"_source"`
			} `json:"hits"`
		} `json:"hits"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, 0, fmt.Errorf("es: decode response: %w", err)
	}

	docs := make([]*LogDocument, len(result.Hits.Hits))
	for i, h := range result.Hits.Hits {
		d := h.Source
		docs[i] = &d
	}
	return docs, result.Hits.Total.Value, nil
}

// EnsureIndex 确保索引和 mapping 已创建（首次启动调用）。
func (c *Client) EnsureIndex(ctx context.Context) error {
	existsResp, err := c.Client.Indices.Exists([]string{c.Index},
		c.Client.Indices.Exists.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("es: check index exists: %w", err)
	}
	defer existsResp.Body.Close()
	if existsResp.StatusCode == 200 {
		return nil // 已存在
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": map[string]interface{}{
				"log_id":           map[string]interface{}{"type": "long"},
				"job_id":           map[string]interface{}{"type": "long"},
				"job_name":         map[string]interface{}{"type": "keyword"},
				"executor_address": map[string]interface{}{"type": "keyword"},
				"status":           map[string]interface{}{"type": "integer"},
				"log_content":      map[string]interface{}{"type": "text", "analyzer": "standard"},
				"trigger_time":     map[string]interface{}{"type": "date"},
				"duration_ms":      map[string]interface{}{"type": "long"},
				"error_msg":        map[string]interface{}{"type": "text"},
				"sharding_index":   map[string]interface{}{"type": "integer"},
				"sharding_total":   map[string]interface{}{"type": "integer"},
			},
		},
	}
	body, _ := json.Marshal(mapping)
	createResp, err := c.Client.Indices.Create(c.Index,
		c.Client.Indices.Create.WithContext(ctx),
		c.Client.Indices.Create.WithBody(bytes.NewReader(body)))
	if err != nil {
		return fmt.Errorf("es: create index: %w", err)
	}
	defer createResp.Body.Close()
	if createResp.IsError() {
		return fmt.Errorf("es: create index returned %s", createResp.Status())
	}
	return nil
}
