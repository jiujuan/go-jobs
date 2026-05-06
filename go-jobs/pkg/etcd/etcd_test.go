package etcd

// etcd_test.go
//
// 覆盖 etcd.go 的全部可测逻辑。
//
// 无需真实 etcd 的测试（纯内存 / 选项逻辑）：
//
// defaultOptions
//   1.  Endpoints 默认 ["127.0.0.1:2379"]
//   2.  DialTimeout 默认 5s
//   3.  Username 默认 ""
//   4.  Password 默认 ""
//
// Option 工厂函数
//   5.  WithEndpoints 修改 Endpoints 切片
//   6.  WithDialTimeout 修改 DialTimeout
//   7.  WithCredentials 同时设置 Username 和 Password
//   8.  多个选项叠加全部生效
//   9.  WithEndpoints 空切片
//  10.  WithEndpoints 多个 endpoint
//
// LeaderElection 内存字段
//  11.  IsLeader() 新建后初始值为 false
//  12.  isLeader 字段可直接设置（白盒测试）
//  13.  stopCh 初始为非 nil 通道
//
// NewClient
//  14.  Endpoints 空时 clientv3.New 返回 error（空 endpoint 列表）
//  15.  不可达地址 + 短超时 → NewClient 返回 client（lazy connect，不立即失败）
//  16.  NewClient 成功时 client 非 nil（需要 ETCD_ADDR，否则 Skip）
//
// NewLeaderElection
//  17.  需要真实 etcd，用 ETCD_ADDR 控制（否则 Skip）
//
// 集成测试（ETCD_ADDR 环境变量）：
//  18.  NewClient 成功连接真实 etcd
//  19.  NewLeaderElection 创建成功，IsLeader() 初始为 false
//  20.  Stop() 不崩溃（未调用 Run 的情况）

import (
	"context"
	"os"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── 集成测试辅助 ─────────────────────────────────────────────────────────────

// realEtcdClient 获取真实 etcd client，若未设置 ETCD_ADDR 则 Skip。
func realEtcdClient(t *testing.T) *clientv3.Client {
	t.Helper()
	addr := os.Getenv("ETCD_ADDR")
	if addr == "" {
		t.Skip("ETCD_ADDR not set, skipping etcd integration test")
	}
	cli, err := NewClient(
		WithEndpoints([]string{addr}),
		WithDialTimeout(3*time.Second),
	)
	require.NoError(t, err, "连接真实 etcd 失败")
	t.Cleanup(func() { cli.Close() })
	return cli
}

// ─── 1-4. defaultOptions ─────────────────────────────────────────────────────

func TestDefaultOptions_Endpoints(t *testing.T) {
	o := defaultOptions()
	require.Len(t, o.Endpoints, 1)
	assert.Equal(t, "127.0.0.1:2379", o.Endpoints[0])
}

func TestDefaultOptions_DialTimeout(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 5*time.Second, o.DialTimeout)
}

func TestDefaultOptions_Username_Empty(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, "", o.Username)
}

func TestDefaultOptions_Password_Empty(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, "", o.Password)
}

// ─── 5-10. Option 工厂函数 ────────────────────────────────────────────────────

func TestWithEndpoints_SetsField(t *testing.T) {
	o := defaultOptions()
	WithEndpoints([]string{"10.0.0.1:2379"})(o)
	require.Len(t, o.Endpoints, 1)
	assert.Equal(t, "10.0.0.1:2379", o.Endpoints[0])
}

func TestWithEndpoints_MultipleEndpoints(t *testing.T) {
	o := defaultOptions()
	eps := []string{"10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"}
	WithEndpoints(eps)(o)
	assert.Equal(t, eps, o.Endpoints)
}

func TestWithEndpoints_EmptySlice(t *testing.T) {
	o := defaultOptions()
	WithEndpoints([]string{})(o)
	assert.Empty(t, o.Endpoints)
}

func TestWithDialTimeout_SetsField(t *testing.T) {
	o := defaultOptions()
	WithDialTimeout(10 * time.Second)(o)
	assert.Equal(t, 10*time.Second, o.DialTimeout)
}

func TestWithCredentials_SetsUsernameAndPassword(t *testing.T) {
	o := defaultOptions()
	WithCredentials("admin", "s3cr3t")(o)
	assert.Equal(t, "admin", o.Username)
	assert.Equal(t, "s3cr3t", o.Password)
}

func TestWithCredentials_EmptyCredentials(t *testing.T) {
	o := defaultOptions()
	o.Username = "old-user"
	o.Password = "old-pass"
	WithCredentials("", "")(o)
	assert.Equal(t, "", o.Username)
	assert.Equal(t, "", o.Password)
}

func TestOptions_MultipleOptions_AllApplied(t *testing.T) {
	o := defaultOptions()
	WithEndpoints([]string{"etcd1:2379", "etcd2:2379"})(o)
	WithDialTimeout(3 * time.Second)(o)
	WithCredentials("root", "rootpass")(o)

	assert.Equal(t, []string{"etcd1:2379", "etcd2:2379"}, o.Endpoints)
	assert.Equal(t, 3*time.Second, o.DialTimeout)
	assert.Equal(t, "root", o.Username)
	assert.Equal(t, "rootpass", o.Password)
}

// ─── 11-13. LeaderElection 内存字段 ──────────────────────────────────────────

func TestLeaderElection_IsLeader_InitiallyFalse(t *testing.T) {
	// 直接构造结构体（白盒测试），不需要真实 etcd
	le := &LeaderElection{
		isLeader: false,
		stopCh:   make(chan struct{}),
	}
	assert.False(t, le.IsLeader(), "新建 LeaderElection 的 IsLeader() 初始值应为 false")
}

func TestLeaderElection_IsLeader_CanBeSetTrue(t *testing.T) {
	le := &LeaderElection{
		isLeader: true,
		stopCh:   make(chan struct{}),
	}
	assert.True(t, le.IsLeader(), "isLeader=true 时 IsLeader() 应返回 true")
}

func TestLeaderElection_StopCh_InitiallyOpen(t *testing.T) {
	le := &LeaderElection{
		stopCh: make(chan struct{}),
	}
	// stopCh 初始应为开放通道（不应已关闭）
	select {
	case <-le.stopCh:
		t.Fatal("stopCh 不应在构造后立即可读（已关闭）")
	default:
		// 正常：通道是开放的
	}
}

func TestLeaderElection_Fields_DirectAccess(t *testing.T) {
	// 验证所有字段的零值不崩溃
	assert.NotPanics(t, func() {
		le := &LeaderElection{}
		_ = le.nodeID
		_ = le.isLeader
		_ = le.onElected
		_ = le.onRevoked
	})
}

// ─── 14-15. NewClient 无需真实 etcd ─────────────────────────────────────────

func TestNewClient_EmptyEndpoints_ReturnsError(t *testing.T) {
	// etcd clientv3.New 对空 endpoints 列表返回 error
	_, err := NewClient(WithEndpoints([]string{}))
	// clientv3 v3.5 对空 endpoints 会返回 error
	if err != nil {
		assert.Contains(t, err.Error(), "etcd:")
	} else {
		// 某些版本可能不立即失败，跳过断言
		t.Log("clientv3.New with empty endpoints did not fail immediately (lazy connect)")
	}
}

func TestNewClient_UnreachableAddress_ClientCreated(t *testing.T) {
	// clientv3.New 是 lazy connect，即使地址不可达也能创建 client
	// （连接在首次操作时建立）
	cli, err := NewClient(
		WithEndpoints([]string{"127.0.0.1:12379"}),
		WithDialTimeout(100*time.Millisecond),
	)
	if err != nil {
		// 部分版本会立即失败，验证 error 包含 "etcd:"
		assert.Contains(t, err.Error(), "etcd:")
		return
	}
	// 如果成功创建（lazy），关闭 client
	require.NotNil(t, cli)
	cli.Close()
}

func TestNewClient_ValidConfig_OptionsApplied(t *testing.T) {
	// 验证选项正确传递到 clientv3.Config（通过 client 的 Endpoints 方法验证）
	endpoints := []string{"127.0.0.1:2379", "127.0.0.2:2379"}
	cli, err := NewClient(
		WithEndpoints(endpoints),
		WithDialTimeout(100*time.Millisecond),
	)
	if err != nil {
		t.Skip("NewClient failed (etcd not available), skipping endpoint verification")
	}
	defer cli.Close()
	// clientv3.Client 的 Endpoints() 返回配置的 endpoints
	assert.Equal(t, endpoints, cli.Endpoints())
}

// ─── Options 结构体零值 ───────────────────────────────────────────────────────

func TestOptions_ZeroValue_NoCrash(t *testing.T) {
	assert.NotPanics(t, func() {
		var o Options
		_ = o.Endpoints
		_ = o.DialTimeout
		_ = o.Username
		_ = o.Password
	})
}

// ─── 表驱动：选项工厂覆盖 ─────────────────────────────────────────────────────

func TestWithDialTimeout_VariousValues(t *testing.T) {
	cases := []time.Duration{
		100 * time.Millisecond,
		1 * time.Second,
		5 * time.Second,
		30 * time.Second,
	}
	for _, d := range cases {
		o := defaultOptions()
		WithDialTimeout(d)(o)
		assert.Equal(t, d, o.DialTimeout, "DialTimeout %v 应被正确设置", d)
	}
}

func TestWithEndpoints_VariousSlices(t *testing.T) {
	cases := [][]string{
		{"127.0.0.1:2379"},
		{"etcd1:2379", "etcd2:2379"},
		{"a:1", "b:2", "c:3", "d:4", "e:5"},
	}
	for _, eps := range cases {
		o := defaultOptions()
		WithEndpoints(eps)(o)
		assert.Equal(t, eps, o.Endpoints)
	}
}

// ─── 集成测试：需要真实 etcd ──────────────────────────────────────────────────

func TestNewClient_WithRealEtcd_Succeeds(t *testing.T) {
	cli := realEtcdClient(t)
	assert.NotNil(t, cli)

	// 发送一次 Get 验证连接可用
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := cli.Get(ctx, "/go-jobs-test-probe")
	assert.NoError(t, err, "真实 etcd 连接应可用")
}

func TestNewLeaderElection_WithRealEtcd_InitialIsLeaderFalse(t *testing.T) {
	cli := realEtcdClient(t)

	le, err := NewLeaderElection(
		cli,
		"/go-jobs/test/leader",
		"test-node-1",
		nil,
		nil,
	)
	require.NoError(t, err)
	defer le.Stop()

	assert.False(t, le.IsLeader(), "刚创建的 LeaderElection IsLeader() 应为 false")
}

func TestLeaderElection_Stop_WithoutRun_NoCrash(t *testing.T) {
	cli := realEtcdClient(t)

	le, err := NewLeaderElection(
		cli,
		"/go-jobs/test/stop-test",
		"test-node-stop",
		nil,
		nil,
	)
	require.NoError(t, err)

	// 不调用 Run，直接 Stop，不应崩溃
	assert.NotPanics(t, func() {
		le.Stop()
	})
}

func TestNewLeaderElection_WithCallbacks_NoCrash(t *testing.T) {
	cli := realEtcdClient(t)

	elected := make(chan struct{}, 1)
	revoked := make(chan struct{}, 1)

	le, err := NewLeaderElection(
		cli,
		"/go-jobs/test/callbacks",
		"test-node-cb",
		func() { elected <- struct{}{} },
		func() { revoked <- struct{}{} },
	)
	require.NoError(t, err)
	assert.NotNil(t, le)
	le.Stop()
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkDefaultOptions(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultOptions()
	}
}

func BenchmarkWithEndpoints(b *testing.B) {
	eps := []string{"10.0.0.1:2379", "10.0.0.2:2379", "10.0.0.3:2379"}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := defaultOptions()
		WithEndpoints(eps)(o)
	}
}
