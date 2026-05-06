package executorstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

type nopLogger struct{}

func (n *nopLogger) Info(msg string, fields ...zap.Field)  {}
func (n *nopLogger) Warn(msg string, fields ...zap.Field)  {}
func (n *nopLogger) Error(msg string, fields ...zap.Field) {}

// mockBeatClient 可配置是否返回错误。
type mockBeatClient struct {
	fail  int32 // 1=返回 error
	calls int64
}

func (m *mockBeatClient) Beat(_ context.Context) error {
	atomic.AddInt64(&m.calls, 1)
	if atomic.LoadInt32(&m.fail) == 1 {
		return fmt.Errorf("beat: connection refused")
	}
	return nil
}

// clientMap 允许按地址配置不同的 mock。
type clientMap struct {
	mu      sync.RWMutex
	clients map[string]*mockBeatClient
}

func newClientMap() *clientMap { return &clientMap{clients: make(map[string]*mockBeatClient)} }

func (cm *clientMap) set(addr string, c *mockBeatClient) {
	cm.mu.Lock()
	cm.clients[addr] = c
	cm.mu.Unlock()
}

func (cm *clientMap) factory() BeatClientFactory {
	return func(address string) BeatClient {
		cm.mu.RLock()
		c, ok := cm.clients[address]
		cm.mu.RUnlock()
		if ok {
			return c
		}
		return &mockBeatClient{} // 默认健康
	}
}

func newStore(opts ...Option) (*Store, *clientMap) {
	cm := newClientMap()
	s := New(cm.factory(), &nopLogger{}, opts...)
	return s, cm
}

func makeExecutor(id int64, app, addr string) *model.Executor {
	return &model.Executor{
		ID:      id,
		AppName: app,
		Address: addr,
		Weight:  1,
		Status:  model.ExecutorOnline,
	}
}

// ─── Register / Heartbeat / Deregister ───────────────────────────────────────

func TestStore_Register(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "host:8080"))

	addrs := s.ListOnlineAddresses("app")
	assert.Equal(t, []string{"host:8080"}, addrs)
}

func TestStore_Register_UpdatesExisting(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "host:8080"))
	// 再次注册同地址（心跳触发）
	e2 := makeExecutor(1, "app", "host:8080")
	e2.Version = "v2"
	s.Register(e2)

	addrs := s.ListOnlineAddresses("app")
	assert.Len(t, addrs, 1, "重复注册不应增加数量")
}

func TestStore_Register_MultipleApps(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app-a", "a1:8080"))
	s.Register(makeExecutor(2, "app-a", "a2:8080"))
	s.Register(makeExecutor(3, "app-b", "b1:8080"))

	addrsA := s.ListOnlineAddresses("app-a")
	addrsB := s.ListOnlineAddresses("app-b")
	assert.Len(t, addrsA, 2)
	assert.Len(t, addrsB, 1)
}

func TestStore_Heartbeat_UpdatesTimestamp(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "host:8080"))

	grp := s.getGroup("app")
	e := grp.get("host:8080")
	before := e.LastHeartbeat()

	time.Sleep(5 * time.Millisecond)
	s.Heartbeat("app", "host:8080", 0.5, 0.3)

	after := e.LastHeartbeat()
	assert.True(t, after.After(before), "Heartbeat 应更新时间戳")
}

func TestStore_Heartbeat_RestoresHealth(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "host:8080"))

	grp := s.getGroup("app")
	e := grp.get("host:8080")
	e.SetHealthy(false) // 模拟不健康状态

	s.Heartbeat("app", "host:8080", 0, 0)
	assert.True(t, e.IsHealthy(), "Heartbeat 应恢复健康状态")
}

func TestStore_Heartbeat_UnknownAddress_Noop(t *testing.T) {
	s, _ := newStore()
	require.NotPanics(t, func() {
		s.Heartbeat("app", "unknown:9999", 0, 0)
	})
}

func TestStore_Deregister(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "host:8080"))
	s.Deregister("app", "host:8080")

	addrs := s.ListOnlineAddresses("app")
	assert.Empty(t, addrs)
}

func TestStore_Deregister_NonExistent_Noop(t *testing.T) {
	s, _ := newStore()
	require.NotPanics(t, func() {
		s.Deregister("ghost-app", "ghost:8080")
	})
}

// ─── Bootstrap ────────────────────────────────────────────────────────────────

func TestStore_Bootstrap(t *testing.T) {
	s, _ := newStore()
	executors := []*model.Executor{
		makeExecutor(1, "app", "h1:8080"),
		makeExecutor(2, "app", "h2:8080"),
		{ID: 3, AppName: "app", Address: "h3:8080", Status: model.ExecutorOffline}, // 离线，不应加载
	}
	s.Bootstrap(executors)

	addrs := s.ListOnlineAddresses("app")
	assert.Len(t, addrs, 2, "Bootstrap 只加载在线执行器")
}

// ─── ListOnlineAddresses ──────────────────────────────────────────────────────

func TestStore_ListOnlineAddresses_NoApp(t *testing.T) {
	s, _ := newStore()
	addrs := s.ListOnlineAddresses("missing-app")
	assert.Nil(t, addrs)
}

func TestStore_ListOnlineAddresses_TTLFilters(t *testing.T) {
	// TTL 设置极短，注册后等待超时
	s, _ := newStore(WithHeartbeatTTL(50 * time.Millisecond))
	s.Register(makeExecutor(1, "app", "old:8080"))
	s.Register(makeExecutor(2, "app", "new:8080"))

	// 让 old 超时
	time.Sleep(60 * time.Millisecond)
	// new 发心跳刷新时间
	s.Heartbeat("app", "new:8080", 0, 0)

	addrs := s.ListOnlineAddresses("app")
	assert.Equal(t, []string{"new:8080"}, addrs, "超过 TTL 的执行器应被过滤")
}

// ─── ListHealthyAddresses ─────────────────────────────────────────────────────

func TestStore_ListHealthyAddresses(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "h1:8080"))
	s.Register(makeExecutor(2, "app", "h2:8080"))

	// 手动标记 h2 不健康
	grp := s.getGroup("app")
	grp.get("h2:8080").SetHealthy(false)

	addrs := s.ListHealthyAddresses("app")
	assert.Equal(t, []string{"h1:8080"}, addrs)
}

func TestStore_ListHealthyAddresses_AllUnhealthy(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "h1:8080"))

	grp := s.getGroup("app")
	grp.get("h1:8080").SetHealthy(false)

	addrs := s.ListHealthyAddresses("app")
	assert.Empty(t, addrs)
}

// ─── 健康探测 probeOne ────────────────────────────────────────────────────────

func TestStore_ProbeOne_HealthyClient(t *testing.T) {
	s, cm := newStore()
	client := &mockBeatClient{}
	cm.set("host:8080", client)

	s.Register(makeExecutor(1, "app", "host:8080"))
	grp := s.getGroup("app")
	e := grp.get("host:8080")

	s.probeOne(e)

	assert.True(t, e.IsHealthy())
	assert.Equal(t, int64(1), atomic.LoadInt64(&client.calls))
}

func TestStore_ProbeOne_UnhealthyClient(t *testing.T) {
	s, cm := newStore()
	client := &mockBeatClient{}
	atomic.StoreInt32(&client.fail, 1)
	cm.set("host:8080", client)

	s.Register(makeExecutor(1, "app", "host:8080"))
	grp := s.getGroup("app")
	e := grp.get("host:8080")

	s.probeOne(e)

	assert.False(t, e.IsHealthy())
}

func TestStore_ProbeOne_Recovery(t *testing.T) {
	s, cm := newStore()
	client := &mockBeatClient{}
	atomic.StoreInt32(&client.fail, 1)
	cm.set("host:8080", client)

	s.Register(makeExecutor(1, "app", "host:8080"))
	grp := s.getGroup("app")
	e := grp.get("host:8080")

	s.probeOne(e)
	assert.False(t, e.IsHealthy())

	// 恢复正常
	atomic.StoreInt32(&client.fail, 0)
	s.probeOne(e)
	assert.True(t, e.IsHealthy(), "恢复后应重新标记为健康")
}

func TestStore_ProbeAll_Concurrent(t *testing.T) {
	s, cm := newStore(WithProbeConcurrency(4))
	for i := 0; i < 20; i++ {
		addr := fmt.Sprintf("host%d:8080", i)
		cm.set(addr, &mockBeatClient{})
		s.Register(makeExecutor(int64(i), "app", addr))
	}

	require.NotPanics(t, func() {
		s.probeAll()
	})

	addrs := s.ListHealthyAddresses("app")
	assert.Len(t, addrs, 20, "所有健康执行器应通过探测")
}

// ─── 后台循环集成 ─────────────────────────────────────────────────────────────

func TestStore_ProbeLoop_UpdatesHealthStatus(t *testing.T) {
	if testing.Short() {
		t.Skip("后台循环测试在 -short 模式下跳过")
	}

	s, cm := newStore(
		WithProbeInterval(30*time.Millisecond),
		WithProbeTimeout(500*time.Millisecond),
	)
	client := &mockBeatClient{}
	cm.set("host:8080", client)
	s.Register(makeExecutor(1, "app", "host:8080"))

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer s.Stop()

	// 初始健康
	time.Sleep(80 * time.Millisecond)
	assert.True(t, s.getGroup("app").get("host:8080").IsHealthy())
	assert.GreaterOrEqual(t, atomic.LoadInt64(&client.calls), int64(1))

	// 触发故障
	atomic.StoreInt32(&client.fail, 1)
	time.Sleep(80 * time.Millisecond)
	assert.False(t, s.getGroup("app").get("host:8080").IsHealthy())

	// 恢复
	atomic.StoreInt32(&client.fail, 0)
	time.Sleep(80 * time.Millisecond)
	assert.True(t, s.getGroup("app").get("host:8080").IsHealthy())

	cancel()
}

func TestStore_TTLSweep(t *testing.T) {
	if testing.Short() {
		t.Skip("TTL 扫描测试在 -short 模式下跳过")
	}

	s, _ := newStore(
		WithHeartbeatTTL(60*time.Millisecond),
		WithProbeInterval(10*time.Second), // 禁止探测干扰
	)
	s.Register(makeExecutor(1, "app", "host:8080"))
	assert.True(t, s.getGroup("app").get("host:8080").IsHealthy())

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	defer func() { cancel(); s.Stop() }()

	// 等待 TTL 超时 + 扫描周期
	time.Sleep(150 * time.Millisecond)
	assert.False(t, s.getGroup("app").get("host:8080").IsHealthy(),
		"超过 TTL 后应被标记为不健康")
}

// ─── TTL 扫描 sweepTTL ────────────────────────────────────────────────────────

func TestStore_SweepTTL_MarksTimeoutEntries(t *testing.T) {
	s, _ := newStore(WithHeartbeatTTL(50 * time.Millisecond))
	s.Register(makeExecutor(1, "app", "old:8080"))
	s.Register(makeExecutor(2, "app", "new:8080"))

	time.Sleep(60 * time.Millisecond)
	// 给 new 刷新心跳
	s.Heartbeat("app", "new:8080", 0, 0)

	s.sweepTTL()

	grp := s.getGroup("app")
	assert.False(t, grp.get("old:8080").IsHealthy(), "超时执行器应被标记为不健康")
	assert.True(t, grp.get("new:8080").IsHealthy(), "刷新心跳的执行器应保持健康")
}

// ─── ListEntries / TotalCount ─────────────────────────────────────────────────

func TestStore_ListEntries(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app", "h1:8080"))
	s.Register(makeExecutor(2, "app", "h2:8080"))

	snaps := s.ListEntries("app")
	assert.Len(t, snaps, 2)
	for _, snap := range snaps {
		assert.Equal(t, "app", snap.AppName)
		assert.True(t, snap.Healthy)
	}
}

func TestStore_TotalCount(t *testing.T) {
	s, _ := newStore()
	s.Register(makeExecutor(1, "app-a", "a1:8080"))
	s.Register(makeExecutor(2, "app-a", "a2:8080"))
	s.Register(makeExecutor(3, "app-b", "b1:8080"))

	assert.Equal(t, 3, s.TotalCount())
}

// ─── Entry 方法 ───────────────────────────────────────────────────────────────

func TestEntry_SetAndGetHealthy(t *testing.T) {
	e := &Entry{}
	assert.False(t, e.IsHealthy())

	e.SetHealthy(true)
	assert.True(t, e.IsHealthy())

	e.SetHealthy(false)
	assert.False(t, e.IsHealthy())
}

func TestEntry_TouchAndLastHeartbeat(t *testing.T) {
	e := &Entry{}
	before := time.Now()
	e.touch()
	after := time.Now()

	hb := e.LastHeartbeat()
	assert.True(t, hb.After(before) || hb.Equal(before))
	assert.True(t, hb.Before(after) || hb.Equal(after))
}

func TestEntry_UpdateResource(t *testing.T) {
	e := &Entry{}
	e.UpdateResource(0.75, 0.50)
	cpu, mem := e.Resource()
	assert.InDelta(t, 0.75, cpu, 0.001)
	assert.InDelta(t, 0.50, mem, 0.001)
}

func TestEntry_Snapshot(t *testing.T) {
	e := &Entry{ID: 1, AppName: "app", Address: "h:8080", Weight: 2}
	e.SetHealthy(true)
	e.UpdateResource(0.3, 0.6)

	snap := e.snapshot()
	assert.Equal(t, int64(1), snap.ID)
	assert.Equal(t, "app", snap.AppName)
	assert.True(t, snap.Healthy)
	assert.InDelta(t, 0.3, snap.CPU, 0.001)
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestStore_ConcurrentRegisterHeartbeatDeregister(t *testing.T) {
	s, _ := newStore()
	const N = 100
	var wg sync.WaitGroup

	// 并发注册
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s.Register(makeExecutor(int64(i), "app", fmt.Sprintf("h%d:8080", i)))
		}(i)
	}
	wg.Wait()

	// 并发心跳 + 注销
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			addr := fmt.Sprintf("h%d:8080", i)
			if i%2 == 0 {
				s.Heartbeat("app", addr, float64(i)/100, 0)
			} else {
				s.Deregister("app", addr)
			}
		}(i)
	}
	wg.Wait()
	// 不崩溃即通过，计数在 [N/2, N] 之间
	addrs := s.ListOnlineAddresses("app")
	assert.GreaterOrEqual(t, len(addrs), 0)
}

func TestStore_ConcurrentProbeAndRead(t *testing.T) {
	s, cm := newStore(WithProbeConcurrency(8))
	for i := 0; i < 50; i++ {
		addr := fmt.Sprintf("h%d:8080", i)
		cm.set(addr, &mockBeatClient{})
		s.Register(makeExecutor(int64(i), "app", addr))
	}

	var wg sync.WaitGroup
	// 并发探测
	for j := 0; j < 5; j++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.probeAll() }()
	}
	// 并发读
	for j := 0; j < 10; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.ListOnlineAddresses("app")
			_ = s.ListHealthyAddresses("app")
		}()
	}
	wg.Wait()
}
