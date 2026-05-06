package scheduler

// route_strategy_test.go
//
// 覆盖 route_strategy.go 中所有路由策略实现和工厂函数。
//
// 测试分组：
//   NewRouter 工厂
//     1.  每种 RouteStrategy 返回非 nil Router
//     2.  未知策略回退到 RoundRobin（可正常路由）
//
//   firstRouter
//     3.  单节点返回唯一地址
//     4.  多节点返回第一个
//     5.  空列表返回 error
//
//   lastRouter
//     6.  单节点返回唯一地址
//     7.  多节点返回最后一个
//     8.  空列表返回 error
//
//   roundRobinRouter
//     9.  轮询遍历所有节点
//    10.  轮询过一圈后从头开始
//    11.  空列表返回 error
//    12.  并发调用无数据竞争
//    13.  独立实例互不影响全局状态
//
//   randomRouter
//    14.  单节点始终返回该节点
//    15.  结果总在地址列表范围内
//    16.  空列表返回 error
//
//   consistentHashRouter
//    17.  相同 jobID+param 路由到相同节点（确定性）
//    18.  不同 jobID 可路由到不同节点
//    19.  不同 param 可路由到不同节点
//    20.  单节点始终路由到该节点
//    21.  空列表返回 error
//
//   lfuRouter
//    22.  初始状态选第一个节点（count 全 0 时取 addresses[0]）
//    23.  调用频率低的节点优先被选中
//    24.  count 递增后下次选最小 count 节点
//    25.  空列表返回 error
//    26.  并发调用无数据竞争
//
//   lruRouter
//    27.  初始状态选第一个节点（lastUsed 全零时取 addresses[0]）
//    28.  最久未被使用的节点优先被选中
//    29.  空列表返回 error
//    30.  并发调用无数据竞争
//
//   failoverRouter
//    31.  返回第一个地址（健康地址由 handleTask 预过滤）
//    32.  空列表返回含 "no healthy" 的 error
//
//   listHash
//    33.  相同地址列表产生相同 hash
//    34.  不同地址列表产生不同 hash
//    35.  空列表不 panic
//    36.  单元素列表不同地址产生不同 hash
//
//   AllStrategies 端到端
//    37.  所有策略均可正常路由（NewRouter + Route）

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── 测试用地址集合 ───────────────────────────────────────────────────────────

var (
	rsAddrs1 = []string{"10.0.0.1:9000"}
	rsAddrs3 = []string{"10.0.0.1:9000", "10.0.0.2:9000", "10.0.0.3:9000"}
	rsAddrs5 = []string{"a:1", "b:1", "c:1", "d:1", "e:1"}
)

// newLRU / newLFU / newRR 创建独立实例，避免污染全局状态
func newRR() *roundRobinRouter  { return &roundRobinRouter{counter: make(map[string]int64)} }
func newLFU() *lfuRouter        { return &lfuRouter{count: make(map[string]int64)} }
func newLRU() *lruRouter        { return &lruRouter{lastUsed: make(map[string]time.Time)} }

// ─── 1-2. NewRouter 工厂 ──────────────────────────────────────────────────────

func TestNewRouter_AllKnownStrategies_NotNil(t *testing.T) {
	strategies := []model.RouteStrategy{
		model.RouteFirst, model.RouteLast, model.RouteRoundRobin,
		model.RouteRandom, model.RouteConsistentHash, model.RouteFailover,
		model.RouteLFU, model.RouteLRU,
	}
	for _, s := range strategies {
		r := NewRouter(s)
		assert.NotNil(t, r, "NewRouter(%v) 不应为 nil", s)
	}
}

func TestNewRouter_UnknownStrategy_CanRoute(t *testing.T) {
	r := NewRouter("TOTALLY_UNKNOWN")
	require.NotNil(t, r)
	addr, err := r.Route(rsAddrs3, 1, "")
	require.NoError(t, err)
	assert.Contains(t, rsAddrs3, addr)
}

// ─── 3-5. firstRouter ────────────────────────────────────────────────────────

func TestFirstRouter_SingleNode(t *testing.T) {
	r := &firstRouter{}
	addr, err := r.Route(rsAddrs1, 0, "")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs1[0], addr)
}

func TestFirstRouter_MultipleNodes_ReturnsFirst(t *testing.T) {
	r := &firstRouter{}
	addr, err := r.Route(rsAddrs3, 99, "param")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs3[0], addr)
}

func TestFirstRouter_EmptyList_Error(t *testing.T) {
	r := &firstRouter{}
	_, err := r.Route(nil, 0, "")
	assert.Error(t, err)
}

// ─── 6-8. lastRouter ─────────────────────────────────────────────────────────

func TestLastRouter_SingleNode(t *testing.T) {
	r := &lastRouter{}
	addr, err := r.Route(rsAddrs1, 0, "")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs1[0], addr)
}

func TestLastRouter_MultipleNodes_ReturnsLast(t *testing.T) {
	r := &lastRouter{}
	addr, err := r.Route(rsAddrs3, 1, "")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs3[len(rsAddrs3)-1], addr)
}

func TestLastRouter_EmptyList_Error(t *testing.T) {
	r := &lastRouter{}
	_, err := r.Route([]string{}, 0, "")
	assert.Error(t, err)
}

// ─── 9-13. roundRobinRouter ──────────────────────────────────────────────────

func TestRoundRobinRouter_CyclesThroughAllNodes(t *testing.T) {
	r := newRR()
	seen := make(map[string]int)
	for i := 0; i < len(rsAddrs3)*3; i++ {
		addr, err := r.Route(rsAddrs3, 0, "")
		require.NoError(t, err)
		seen[addr]++
	}
	for _, a := range rsAddrs3 {
		assert.Equal(t, 3, seen[a], "节点 %s 应被轮询恰好 3 次", a)
	}
}

func TestRoundRobinRouter_WrapsAround(t *testing.T) {
	r := newRR()
	first, _ := r.Route(rsAddrs3, 0, "")
	for i := 0; i < len(rsAddrs3); i++ {
		r.Route(rsAddrs3, 0, "")
	}
	again, _ := r.Route(rsAddrs3, 0, "")
	assert.Equal(t, first, again, "轮询一圈后应回到起点")
}

func TestRoundRobinRouter_EmptyList_Error(t *testing.T) {
	r := newRR()
	_, err := r.Route(nil, 0, "")
	assert.Error(t, err)
}

func TestRoundRobinRouter_ConcurrentCalls_NoRace(t *testing.T) {
	r := newRR()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := r.Route(rsAddrs5, 0, "")
			require.NoError(t, err)
			assert.Contains(t, rsAddrs5, addr)
		}()
	}
	wg.Wait()
}

func TestRoundRobinRouter_IndependentInstances_NoInterference(t *testing.T) {
	r1, r2 := newRR(), newRR()
	a1, _ := r1.Route(rsAddrs3, 0, "")
	a2, _ := r2.Route(rsAddrs3, 0, "")
	assert.Equal(t, a1, a2, "独立实例应从相同位置开始，互不影响")
}

// ─── 14-16. randomRouter ─────────────────────────────────────────────────────

func TestRandomRouter_SingleNode_AlwaysReturnsIt(t *testing.T) {
	r := &randomRouter{}
	for i := 0; i < 20; i++ {
		addr, err := r.Route(rsAddrs1, int64(i), "")
		require.NoError(t, err)
		assert.Equal(t, rsAddrs1[0], addr)
	}
}

func TestRandomRouter_ResultAlwaysInList(t *testing.T) {
	r := &randomRouter{}
	for i := 0; i < 100; i++ {
		addr, err := r.Route(rsAddrs5, int64(i), "")
		require.NoError(t, err)
		assert.Contains(t, rsAddrs5, addr)
	}
}

func TestRandomRouter_EmptyList_Error(t *testing.T) {
	r := &randomRouter{}
	_, err := r.Route([]string{}, 0, "")
	assert.Error(t, err)
}

// ─── 17-21. consistentHashRouter ─────────────────────────────────────────────

func TestConsistentHashRouter_SameInput_SameResult(t *testing.T) {
	r := &consistentHashRouter{}
	expected, err := r.Route(rsAddrs5, 42, "my-param")
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		addr, err := r.Route(rsAddrs5, 42, "my-param")
		require.NoError(t, err)
		assert.Equal(t, expected, addr, "相同 jobID+param 应始终路由到相同节点")
	}
}

func TestConsistentHashRouter_DifferentJobIDs_Distributed(t *testing.T) {
	r := &consistentHashRouter{}
	seen := make(map[string]bool)
	for id := int64(0); id < 50; id++ {
		addr, err := r.Route(rsAddrs5, id, "")
		require.NoError(t, err)
		seen[addr] = true
	}
	assert.Greater(t, len(seen), 1, "不同 jobID 应分散到多个节点")
}

func TestConsistentHashRouter_DifferentParams_Distributed(t *testing.T) {
	r := &consistentHashRouter{}
	seen := make(map[string]bool)
	params := []string{"alpha", "beta", "gamma", "delta", "epsilon",
		"zeta", "eta", "theta", "iota", "kappa"}
	for _, p := range params {
		addr, err := r.Route(rsAddrs5, 1, p)
		require.NoError(t, err)
		seen[addr] = true
	}
	assert.Greater(t, len(seen), 1, "不同 param 应分散到多个节点")
}

func TestConsistentHashRouter_SingleNode_AlwaysRoutesTheSame(t *testing.T) {
	r := &consistentHashRouter{}
	for id := int64(0); id < 10; id++ {
		addr, err := r.Route(rsAddrs1, id, "param")
		require.NoError(t, err)
		assert.Equal(t, rsAddrs1[0], addr)
	}
}

func TestConsistentHashRouter_EmptyList_Error(t *testing.T) {
	r := &consistentHashRouter{}
	_, err := r.Route(nil, 1, "p")
	assert.Error(t, err)
}

// ─── 22-26. lfuRouter ────────────────────────────────────────────────────────

func TestLFURouter_InitialState_PicksFirstNode(t *testing.T) {
	r := newLFU()
	addr, err := r.Route(rsAddrs3, 0, "")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs3[0], addr, "count 全 0 时应选 addresses[0]")
}

func TestLFURouter_PrefersLessFrequentNode(t *testing.T) {
	r := newLFU()
	r.count[rsAddrs3[0]] = 10
	r.count[rsAddrs3[1]] = 5
	// rsAddrs3[2] count=0，应被选中
	addr, err := r.Route(rsAddrs3, 0, "")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs3[2], addr)
}

func TestLFURouter_CountIncrements(t *testing.T) {
	r := newLFU()
	addrs := []string{"x:1", "y:1"}
	// count 全 0：选 x（addrs[0]），x.count → 1
	addr1, _ := r.Route(addrs, 0, "")
	assert.Equal(t, "x:1", addr1)
	// x=1, y=0：选 y，y.count → 1
	addr2, _ := r.Route(addrs, 0, "")
	assert.Equal(t, "y:1", addr2)
	// x=1, y=1：选 x（addrs[0]）
	addr3, _ := r.Route(addrs, 0, "")
	assert.Equal(t, "x:1", addr3)
}

func TestLFURouter_EmptyList_Error(t *testing.T) {
	r := newLFU()
	_, err := r.Route(nil, 0, "")
	assert.Error(t, err)
}

func TestLFURouter_ConcurrentCalls_NoRace(t *testing.T) {
	r := newLFU()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := r.Route(rsAddrs5, 0, "")
			require.NoError(t, err)
			assert.Contains(t, rsAddrs5, addr)
		}()
	}
	wg.Wait()
}

// ─── 27-30. lruRouter ────────────────────────────────────────────────────────

func TestLRURouter_InitialState_PicksFirstNode(t *testing.T) {
	r := newLRU()
	addr, err := r.Route(rsAddrs3, 0, "")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs3[0], addr, "lastUsed 全零时应选 addresses[0]")
}

func TestLRURouter_PrefersLeastRecentlyUsedNode(t *testing.T) {
	r := newLRU()
	addrs := []string{"p:1", "q:1", "r:1"}
	// 第一次选 p（全零，选 addrs[0]），p.lastUsed 更新
	addr1, _ := r.Route(addrs, 0, "")
	assert.Equal(t, "p:1", addr1)
	// 第二次：q、r lastUsed=0（最久未用），选 q（addrs[1]）
	addr2, _ := r.Route(addrs, 0, "")
	assert.Equal(t, "q:1", addr2)
	// 第三次：r lastUsed=0（最久未用），选 r
	addr3, _ := r.Route(addrs, 0, "")
	assert.Equal(t, "r:1", addr3)
}

func TestLRURouter_EmptyList_Error(t *testing.T) {
	r := newLRU()
	_, err := r.Route(nil, 0, "")
	assert.Error(t, err)
}

func TestLRURouter_ConcurrentCalls_NoRace(t *testing.T) {
	r := newLRU()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			addr, err := r.Route(rsAddrs5, 0, "")
			require.NoError(t, err)
			assert.Contains(t, rsAddrs5, addr)
		}()
	}
	wg.Wait()
}

// ─── 31-32. failoverRouter ───────────────────────────────────────────────────

func TestFailoverRouter_ReturnsFirstAddress(t *testing.T) {
	r := &failoverRouter{}
	addr, err := r.Route(rsAddrs3, 0, "")
	require.NoError(t, err)
	assert.Equal(t, rsAddrs3[0], addr)
}

func TestFailoverRouter_EmptyList_ContainsNoHealthy(t *testing.T) {
	r := &failoverRouter{}
	_, err := r.Route(nil, 0, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no healthy executor")
}

// ─── 33-36. listHash ─────────────────────────────────────────────────────────

func TestListHash_SameList_SameHash(t *testing.T) {
	list := []string{"a:1", "b:2", "c:3"}
	assert.Equal(t, listHash(list), listHash(list))
}

func TestListHash_DifferentLists_DifferentHash(t *testing.T) {
	h1 := listHash([]string{"a:1", "b:2"})
	h2 := listHash([]string{"a:1", "c:3"})
	assert.NotEqual(t, h1, h2)
}

func TestListHash_EmptyList_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() {
		h := listHash(nil)
		assert.NotEmpty(t, h)
	})
}

func TestListHash_SingleElement_DifferentAddresses(t *testing.T) {
	h1 := listHash([]string{"10.0.0.1:9000"})
	h2 := listHash([]string{"10.0.0.1:9001"})
	assert.NotEqual(t, h1, h2)
}

// ─── 37. AllStrategies 端到端 ─────────────────────────────────────────────────

func TestNewRouter_AllStrategies_CanRoute(t *testing.T) {
	strategies := []model.RouteStrategy{
		model.RouteFirst, model.RouteLast, model.RouteRoundRobin,
		model.RouteRandom, model.RouteConsistentHash,
		model.RouteFailover, model.RouteLFU, model.RouteLRU,
	}
	for _, s := range strategies {
		r := NewRouter(s)
		addr, err := r.Route(rsAddrs3, 1, "param")
		require.NoError(t, err, "策略 %v 不应返回 error", s)
		assert.Contains(t, rsAddrs3, addr, "策略 %v 结果应在地址列表内", s)
	}
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkFirstRouter_Route(b *testing.B) {
	r := &firstRouter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Route(rsAddrs5, 1, "")
		}
	})
}

func BenchmarkRoundRobinRouter_Route(b *testing.B) {
	r := newRR()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Route(rsAddrs5, 1, "")
		}
	})
}

func BenchmarkConsistentHashRouter_Route(b *testing.B) {
	r := &consistentHashRouter{}
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r.Route(rsAddrs5, 42, "my-param")
		}
	})
}
