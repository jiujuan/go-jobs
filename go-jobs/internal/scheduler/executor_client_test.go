package scheduler

// executor_client_test.go
//
// 覆盖 executor_client.go 的全部公开方法和 post 内部逻辑，
// 使用 net/http/httptest.NewServer 模拟 executor，无真实网络依赖。
//
// NewExecutorClient
//   1.  address 被正确存储
//   2.  httpClient 超时为 60s
//   3.  Transport 配置 MaxIdleConnsPerHost=10
//
// Run（/executor/run）
//   4.  200 OK → nil error
//   5.  请求 Path 为 /executor/run
//   6.  请求方法为 POST
//   7.  Content-Type 为 application/json
//   8.  X-Go-Jobs-Token 为 "internal"
//   9.  Body 正确序列化 ExecutorTrigger 所有字段
//  10.  非 200 → 返回含状态码的 error
//  11.  非 200 + JSON error body → error 含 message 字段
//  12.  ctx 超时 → 返回 error
//  13.  executor 不可达 → 返回 error
//
// Kill（/executor/kill）
//  14.  200 OK → nil error
//  15.  Path 为 /executor/kill
//  16.  Body 含 log_id 和 job_id
//  17.  非 200 → error
//
// IdleBeat（/executor/idleBeat）
//  18.  200 OK → nil error
//  19.  Path 为 /executor/idleBeat
//  20.  Body 含 job_id
//  21.  非 200 → error
//
// Beat（/executor/beat）
//  22.  200 OK → nil error
//  23.  Path 为 /executor/beat
//  24.  body 为空（nil body → 空 bytes.Buffer）
//  25.  非 200 → error
//
// ExecutorTrigger / KillRequest JSON 字段名
//  26.  ExecutorTrigger 所有字段为 snake_case
//  27.  KillRequest 字段为 snake_case

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

// ecServer 构建一个捕获请求的 httptest.Server。
// 每次收到请求都将解析结果追加到 reqs 中。
type capturedReq struct {
	method      string
	path        string
	contentType string
	token       string
	rawBody     []byte
}

type ecServer struct {
	*httptest.Server
	statusCode int
	reqs       []capturedReq
	errBody    string // 非空时作为 JSON {"message":"..."} 响应体返回
}

func newECServer(statusCode int) *ecServer {
	s := &ecServer{statusCode: statusCode}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		s.reqs = append(s.reqs, capturedReq{
			method:      r.Method,
			path:        r.URL.Path,
			contentType: r.Header.Get("Content-Type"),
			token:       r.Header.Get("X-Go-Jobs-Token"),
			rawBody:     body,
		})
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(s.statusCode)
		if s.errBody != "" {
			w.Write([]byte(`{"message":"` + s.errBody + `"}`)) //nolint:errcheck
		}
	}))
	return s
}

// addr returns the server address in "host:port" form (no scheme).
func (s *ecServer) addr() string {
	return strings.TrimPrefix(s.URL, "http://")
}

// last returns the most-recently captured request.
func (s *ecServer) last() capturedReq {
	return s.reqs[len(s.reqs)-1]
}

// sampleTrigger builds a fully-populated ExecutorTrigger.
func sampleTrigger() *ExecutorTrigger {
	return &ExecutorTrigger{
		LogID:           101,
		JobID:           7,
		ExecutorHandler: "myHandler",
		ExecuteType:     "BEAN",
		ExecuteParam:    `{"key":"val"}`,
		ShardingIndex:   2,
		ShardingTotal:   5,
		Timeout:         30,
	}
}

// sampleKill builds a fully-populated KillRequest.
func sampleKill() *KillRequest {
	return &KillRequest{LogID: 202, JobID: 99}
}

// ─── 1-3. NewExecutorClient ──────────────────────────────────────────────────

func TestNewExecutorClient_StoresAddress(t *testing.T) {
	c := NewExecutorClient("10.0.0.1:9000")
	assert.Equal(t, "10.0.0.1:9000", c.address)
}

func TestNewExecutorClient_HTTPClientTimeout_60s(t *testing.T) {
	c := NewExecutorClient("addr")
	assert.Equal(t, 60*time.Second, c.httpClient.Timeout)
}

func TestNewExecutorClient_TransportMaxIdleConns(t *testing.T) {
	c := NewExecutorClient("addr")
	tr, ok := c.httpClient.Transport.(*http.Transport)
	require.True(t, ok, "Transport 应为 *http.Transport")
	assert.Equal(t, 10, tr.MaxIdleConnsPerHost)
}

// ─── 4-13. Run ───────────────────────────────────────────────────────────────

func TestExecutorClient_Run_200_NoError(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.Run(context.Background(), sampleTrigger())
	require.NoError(t, err)
}

func TestExecutorClient_Run_PathIsExecutorRun(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.Run(context.Background(), sampleTrigger())
	assert.Equal(t, "/executor/run", srv.last().path)
}

func TestExecutorClient_Run_MethodIsPOST(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.Run(context.Background(), sampleTrigger())
	assert.Equal(t, http.MethodPost, srv.last().method)
}

func TestExecutorClient_Run_ContentTypeIsJSON(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.Run(context.Background(), sampleTrigger())
	assert.Equal(t, "application/json", srv.last().contentType)
}

func TestExecutorClient_Run_AuthTokenIsInternal(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.Run(context.Background(), sampleTrigger())
	assert.Equal(t, "internal", srv.last().token)
}

func TestExecutorClient_Run_BodyContainsAllTriggerFields(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	trigger := sampleTrigger()
	c := NewExecutorClient(srv.addr())
	_ = c.Run(context.Background(), trigger)

	var got ExecutorTrigger
	require.NoError(t, json.Unmarshal(srv.last().rawBody, &got))
	assert.Equal(t, trigger.LogID, got.LogID)
	assert.Equal(t, trigger.JobID, got.JobID)
	assert.Equal(t, trigger.ExecutorHandler, got.ExecutorHandler)
	assert.Equal(t, trigger.ExecuteType, got.ExecuteType)
	assert.Equal(t, trigger.ExecuteParam, got.ExecuteParam)
	assert.Equal(t, trigger.ShardingIndex, got.ShardingIndex)
	assert.Equal(t, trigger.ShardingTotal, got.ShardingTotal)
	assert.Equal(t, trigger.Timeout, got.Timeout)
}

func TestExecutorClient_Run_Non200_ReturnsError(t *testing.T) {
	srv := newECServer(http.StatusBadGateway)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.Run(context.Background(), sampleTrigger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "502")
}

func TestExecutorClient_Run_Non200_WithMessageBody_ErrorContainsMessage(t *testing.T) {
	srv := newECServer(http.StatusInternalServerError)
	srv.errBody = "executor is overloaded"
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.Run(context.Background(), sampleTrigger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "executor is overloaded")
}

func TestExecutorClient_Run_CtxTimeout_ReturnsError(t *testing.T) {
	// 服务端故意延迟，ctx 先超时
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(500 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer slow.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	c := NewExecutorClient(strings.TrimPrefix(slow.URL, "http://"))
	err := c.Run(ctx, sampleTrigger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "executor client: do request")
}

func TestExecutorClient_Run_UnreachableAddress_ReturnsError(t *testing.T) {
	c := NewExecutorClient("127.0.0.1:19991")
	c.httpClient.Timeout = 300 * time.Millisecond

	err := c.Run(context.Background(), sampleTrigger())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "executor client: do request")
}

// ─── 14-17. Kill ─────────────────────────────────────────────────────────────

func TestExecutorClient_Kill_200_NoError(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.Kill(context.Background(), sampleKill())
	require.NoError(t, err)
}

func TestExecutorClient_Kill_PathIsExecutorKill(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.Kill(context.Background(), sampleKill())
	assert.Equal(t, "/executor/kill", srv.last().path)
}

func TestExecutorClient_Kill_BodyContainsLogIDAndJobID(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	kr := sampleKill()
	c := NewExecutorClient(srv.addr())
	_ = c.Kill(context.Background(), kr)

	var got KillRequest
	require.NoError(t, json.Unmarshal(srv.last().rawBody, &got))
	assert.Equal(t, kr.LogID, got.LogID)
	assert.Equal(t, kr.JobID, got.JobID)
}

func TestExecutorClient_Kill_Non200_ReturnsError(t *testing.T) {
	srv := newECServer(http.StatusForbidden)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.Kill(context.Background(), sampleKill())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

// ─── 18-21. IdleBeat ─────────────────────────────────────────────────────────

func TestExecutorClient_IdleBeat_200_NoError(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.IdleBeat(context.Background(), 7)
	require.NoError(t, err)
}

func TestExecutorClient_IdleBeat_PathIsExecutorIdleBeat(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.IdleBeat(context.Background(), 7)
	assert.Equal(t, "/executor/idleBeat", srv.last().path)
}

func TestExecutorClient_IdleBeat_BodyContainsJobID(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.IdleBeat(context.Background(), 42)

	var payload map[string]int64
	require.NoError(t, json.Unmarshal(srv.last().rawBody, &payload))
	assert.Equal(t, int64(42), payload["job_id"])
}

func TestExecutorClient_IdleBeat_Non200_ReturnsError(t *testing.T) {
	srv := newECServer(http.StatusServiceUnavailable)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.IdleBeat(context.Background(), 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "503")
}

// ─── 22-25. Beat ─────────────────────────────────────────────────────────────

func TestExecutorClient_Beat_200_NoError(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.Beat(context.Background())
	require.NoError(t, err)
}

func TestExecutorClient_Beat_PathIsExecutorBeat(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.Beat(context.Background())
	assert.Equal(t, "/executor/beat", srv.last().path)
}

func TestExecutorClient_Beat_BodyIsEmpty(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	_ = c.Beat(context.Background())
	// Beat 传 nil body，bytes.Buffer 应为空
	assert.Empty(t, srv.last().rawBody, "Beat 请求体应为空")
}

func TestExecutorClient_Beat_Non200_ReturnsError(t *testing.T) {
	srv := newECServer(http.StatusBadRequest)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	err := c.Beat(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "400")
}

// ─── 26-27. JSON 字段名（snake_case 验证） ───────────────────────────────────

func TestExecutorTrigger_JSONFieldNames_SnakeCase(t *testing.T) {
	tr := sampleTrigger()
	data, err := json.Marshal(tr)
	require.NoError(t, err)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &m))

	for _, key := range []string{
		"log_id", "job_id", "executor_handler",
		"execute_type", "execute_param",
		"sharding_index", "sharding_total", "timeout",
	} {
		_, ok := m[key]
		assert.True(t, ok, "ExecutorTrigger JSON 应包含字段 %q", key)
	}
}

func TestKillRequest_JSONFieldNames_SnakeCase(t *testing.T) {
	kr := sampleKill()
	data, err := json.Marshal(kr)
	require.NoError(t, err)

	var m map[string]interface{}
	require.NoError(t, json.Unmarshal(data, &m))

	_, hasLogID := m["log_id"]
	_, hasJobID := m["job_id"]
	assert.True(t, hasLogID, "KillRequest JSON 应包含 log_id")
	assert.True(t, hasJobID, "KillRequest JSON 应包含 job_id")
}

// ─── 多方法共享 header 检查 ───────────────────────────────────────────────────

func TestExecutorClient_AllMethods_ShareAuthToken(t *testing.T) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())

	_ = c.Run(context.Background(), sampleTrigger())
	_ = c.Kill(context.Background(), sampleKill())
	_ = c.IdleBeat(context.Background(), 1)
	_ = c.Beat(context.Background())

	for i, req := range srv.reqs {
		assert.Equal(t, "internal", req.token,
			"请求 #%d 的 X-Go-Jobs-Token 应为 'internal'", i)
	}
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkExecutorClient_Run(b *testing.B) {
	srv := newECServer(http.StatusOK)
	defer srv.Close()

	c := NewExecutorClient(srv.addr())
	trigger := sampleTrigger()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.Run(context.Background(), trigger)
	}
}
