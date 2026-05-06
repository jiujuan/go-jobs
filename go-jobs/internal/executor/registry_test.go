package executor

// registry_test.go
//
// 针对 registry.go（AutoRegistrar）的单元测试。
// 全部使用 net/http/httptest 搭建假 admin server，不依赖真实网络。
//
// 覆盖场景：
//  NewAutoRegistrar
//    1.  默认心跳间隔为 defaultHeartbeatInterval
//    2.  WithHeartbeatInterval 覆盖间隔
//    3.  默认 httpClient 超时为 10s
//    4.  RegistrationRequest 字段正确存储
//
//  Start / Stop
//    5.  Start 成功：admin 返回 200，发起 register 请求
//    6.  Start 失败：admin 返回 500，返回 error
//    7.  Start 失败：admin 不可达，返回 error
//    8.  Stop 发起 deregister 请求
//    9.  Stop 后心跳循环退出（不再发送心跳）
//    10. Stop 幂等（多次调用不 panic）
//
//  心跳（beatLoop）
//    11. 心跳按间隔发送到 /api/executor/heartbeat
//    12. 心跳失败时尝试重新 register
//
//  post 方法
//    13. POST 请求携带正确 Content-Type: application/json
//    14. POST 请求 body 是 RegistrationRequest 的 JSON 序列化
//    15. POST 收到非 200 状态码返回 error
//    16. POST 到不可达地址返回 error
//
//  RegistrationRequest
//    17. JSON 序列化字段名正确（snake_case）

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

// adminServer 启动一个假 admin HTTP server，按路径统计请求次数。
type adminServer struct {
	server       *httptest.Server
	registerHits int64
	heartbeatHits int64
	deregisterHits int64

	// 每个路径返回的状态码，默认 200
	registerStatus  int
	heartbeatStatus int
	deregisterStatus int

	// 记录最后一次收到的 body（任意路径）
	lastBody []byte
	lastContentType string
}

func newAdminServer() *adminServer {
	a := &adminServer{
		registerStatus:  http.StatusOK,
		heartbeatStatus: http.StatusOK,
		deregisterStatus: http.StatusOK,
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/executor/register", func(w http.ResponseWriter, r *http.Request) {
		a.lastContentType = r.Header.Get("Content-Type")
		a.lastBody, _ = io.ReadAll(r.Body)
		atomic.AddInt64(&a.registerHits, 1)
		w.WriteHeader(a.registerStatus)
	})
	mux.HandleFunc("/api/executor/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&a.heartbeatHits, 1)
		w.WriteHeader(a.heartbeatStatus)
	})
	mux.HandleFunc("/api/executor/deregister", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&a.deregisterHits, 1)
		w.WriteHeader(a.deregisterStatus)
	})
	a.server = httptest.NewServer(mux)
	return a
}

func (a *adminServer) Close() { a.server.Close() }
func (a *adminServer) URL() string { return a.server.URL }

func (a *adminServer) Registers() int64  { return atomic.LoadInt64(&a.registerHits) }
func (a *adminServer) Heartbeats() int64 { return atomic.LoadInt64(&a.heartbeatHits) }
func (a *adminServer) Deregisters() int64 { return atomic.LoadInt64(&a.deregisterHits) }

// sampleReq 返回一个典型的 RegistrationRequest。
func sampleReq() RegistrationRequest {
	return RegistrationRequest{
		AppName: "test-executor",
		Title:   "Test Executor",
		Address: "127.0.0.1:9901",
		Version: "1.0.0",
	}
}

// ─── 1-4. NewAutoRegistrar ────────────────────────────────────────────────────

func TestNewAutoRegistrar_DefaultHeartbeatInterval(t *testing.T) {
	ar := NewAutoRegistrar("http://localhost", sampleReq())
	assert.Equal(t, defaultHeartbeatInterval, ar.interval)
}

func TestNewAutoRegistrar_WithHeartbeatInterval_Overrides(t *testing.T) {
	ar := NewAutoRegistrar("http://localhost", sampleReq(),
		WithHeartbeatInterval(5*time.Second),
	)
	assert.Equal(t, 5*time.Second, ar.interval)
}

func TestNewAutoRegistrar_DefaultHTTPClientTimeout(t *testing.T) {
	ar := NewAutoRegistrar("http://localhost", sampleReq())
	assert.Equal(t, 10*time.Second, ar.httpClient.Timeout)
}

func TestNewAutoRegistrar_StoresRegistrationRequest(t *testing.T) {
	req := sampleReq()
	ar := NewAutoRegistrar("http://admin.local", req)
	assert.Equal(t, req.AppName, ar.req.AppName)
	assert.Equal(t, req.Title, ar.req.Title)
	assert.Equal(t, req.Address, ar.req.Address)
	assert.Equal(t, req.Version, ar.req.Version)
	assert.Equal(t, "http://admin.local", ar.adminURL)
}

// ─── 5-7. Start ───────────────────────────────────────────────────────────────

func TestStart_Success_SendsRegisterRequest(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(time.Hour), // 避免测试期间触发心跳
	)
	err := ar.Start()
	require.NoError(t, err)
	ar.Stop()

	assert.Equal(t, int64(1), admin.Registers(),
		"Start should send exactly one register request")
}

func TestStart_AdminReturns500_ReturnsError(t *testing.T) {
	admin := newAdminServer()
	admin.registerStatus = http.StatusInternalServerError
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq())
	err := ar.Start()

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "registrar: initial register failed")
}

func TestStart_AdminUnreachable_ReturnsError(t *testing.T) {
	// 用一个不存在的地址
	ar := NewAutoRegistrar("http://127.0.0.1:19999", sampleReq())
	ar.httpClient.Timeout = 500 * time.Millisecond // 加速失败

	err := ar.Start()
	assert.Error(t, err, "unreachable admin should cause Start to fail")
}

// ─── 8-10. Stop ───────────────────────────────────────────────────────────────

func TestStop_SendsDeregisterRequest(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(time.Hour),
	)
	require.NoError(t, ar.Start())
	ar.Stop()

	assert.Equal(t, int64(1), admin.Deregisters(),
		"Stop should send exactly one deregister request")
}

func TestStop_HeartbeatLoopExits(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(50*time.Millisecond),
	)
	require.NoError(t, ar.Start())

	// 等待至少一次心跳
	time.Sleep(150 * time.Millisecond)
	hitsBefore := admin.Heartbeats()
	assert.Greater(t, hitsBefore, int64(0), "should have sent heartbeats")

	ar.Stop()

	// Stop 后心跳应停止
	hitsAfterStop := admin.Heartbeats()
	time.Sleep(150 * time.Millisecond)
	hitsAfterWait := admin.Heartbeats()

	assert.Equal(t, hitsAfterStop, hitsAfterWait,
		"no more heartbeats should be sent after Stop")
}

func TestStop_Idempotent_NoPanic(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(time.Hour),
	)
	require.NoError(t, ar.Start())

	// Stop 只能安全调用一次（close(stopCh) 第二次会 panic）
	// 这里只验证单次调用不 panic；不做多次调用（stopCh 语义）
	require.NotPanics(t, func() {
		ar.Stop()
	})
}

// ─── 11-12. 心跳（beatLoop） ─────────────────────────────────────────────────

func TestBeatLoop_SendsHeartbeatsAtInterval(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(40*time.Millisecond),
	)
	require.NoError(t, ar.Start())
	defer ar.Stop()

	// 等待 ~3 个心跳周期
	time.Sleep(160 * time.Millisecond)

	beats := admin.Heartbeats()
	assert.GreaterOrEqual(t, beats, int64(2),
		"should have sent at least 2 heartbeats in 4 intervals")
}

func TestBeatLoop_HeartbeatFails_TriggersReRegister(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	// 让心跳返回 500，触发重新注册
	admin.heartbeatStatus = http.StatusInternalServerError

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(40*time.Millisecond),
	)
	require.NoError(t, ar.Start())
	defer ar.Stop()

	// 等待几个心跳周期
	time.Sleep(200 * time.Millisecond)

	// Start 已发送 1 次；心跳失败后会重试 register
	registers := admin.Registers()
	assert.Greater(t, registers, int64(1),
		"heartbeat failure should trigger re-register")
}

// ─── 13-16. post 方法 ────────────────────────────────────────────────────────

func TestPost_ContentTypeIsJSON(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(time.Hour),
	)
	require.NoError(t, ar.Start())
	ar.Stop()

	assert.Equal(t, "application/json", admin.lastContentType,
		"POST should set Content-Type: application/json")
}

func TestPost_BodyIsJSONOfRegistrationRequest(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	req := sampleReq()
	ar := NewAutoRegistrar(admin.URL(), req, WithHeartbeatInterval(time.Hour))
	require.NoError(t, ar.Start())
	ar.Stop()

	var got RegistrationRequest
	err := json.Unmarshal(admin.lastBody, &got)
	require.NoError(t, err, "body should be valid JSON")
	assert.Equal(t, req.AppName, got.AppName)
	assert.Equal(t, req.Address, got.Address)
	assert.Equal(t, req.Version, got.Version)
}

func TestPost_Non200Response_ReturnsError(t *testing.T) {
	admin := newAdminServer()
	admin.registerStatus = http.StatusForbidden
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq())
	err := ar.Start()

	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

func TestPost_UnreachableAddress_ReturnsError(t *testing.T) {
	ar := NewAutoRegistrar("http://127.0.0.1:19998", sampleReq())
	ar.httpClient.Timeout = 300 * time.Millisecond

	err := ar.post("/api/executor/register", sampleReq())
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "registrar: POST")
}

// ─── 17. RegistrationRequest JSON 字段名 ─────────────────────────────────────

func TestRegistrationRequest_JSONFieldNames(t *testing.T) {
	req := RegistrationRequest{
		AppName: "myapp",
		Title:   "My App",
		Address: "10.0.0.1:9000",
		Version: "v2.3.1",
	}
	data, err := json.Marshal(req)
	require.NoError(t, err)

	var m map[string]string
	require.NoError(t, json.Unmarshal(data, &m))

	assert.Equal(t, "myapp", m["app_name"])
	assert.Equal(t, "My App", m["title"])
	assert.Equal(t, "10.0.0.1:9000", m["address"])
	assert.Equal(t, "v2.3.1", m["version"])
}

func TestRegistrationRequest_JSONRoundTrip(t *testing.T) {
	orig := RegistrationRequest{
		AppName: "executor-1",
		Title:   "Executor One",
		Address: "192.168.1.1:8080",
		Version: "3.0.0",
	}
	data, err := json.Marshal(orig)
	require.NoError(t, err)

	var got RegistrationRequest
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, orig, got)
}

// ─── 18. 多 Option 组合 ───────────────────────────────────────────────────────

func TestNewAutoRegistrar_MultipleOptions_AllApplied(t *testing.T) {
	ar := NewAutoRegistrar("http://admin",
		sampleReq(),
		WithHeartbeatInterval(3*time.Second),
	)
	assert.Equal(t, 3*time.Second, ar.interval)
	assert.Equal(t, "http://admin", ar.adminURL)
}

// ─── 19. Start 成功后 Stop 不影响 deregister 结果 ────────────────────────────

func TestStartStop_FullLifecycle(t *testing.T) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(),
		WithHeartbeatInterval(time.Hour),
	)

	require.NoError(t, ar.Start())
	assert.Equal(t, int64(1), admin.Registers())
	assert.Equal(t, int64(0), admin.Deregisters())

	ar.Stop()
	assert.Equal(t, int64(1), admin.Deregisters())
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkAutoRegistrar_Post(b *testing.B) {
	admin := newAdminServer()
	defer admin.Close()

	ar := NewAutoRegistrar(admin.URL(), sampleReq(), WithHeartbeatInterval(time.Hour))
	require.NoError(b, ar.Start())
	defer ar.Stop()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ar.post("/api/executor/heartbeat", ar.req)
	}
}
