// Package scheduler implements the core scheduling engine for go-jobs.
package scheduler

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
)

// ─── Router 接口 ──────────────────────────────────────────────────────────────

// Router 根据路由策略从候选地址列表中选择一个执行器地址。
// addresses 由 Scheduler.handleTask 通过 executorstore 获取，
// 已按路由类型区分在线地址（ListOnlineAddresses）或健康地址（ListHealthyAddresses）。
type Router interface {
	Route(addresses []string, jobID int64, param string) (string, error)
}

// NewRouter 返回对应路由策略的 Router 实现。
func NewRouter(strategy model.RouteStrategy) Router {
	switch strategy {
	case model.RouteFirst:
		return &firstRouter{}
	case model.RouteLast:
		return &lastRouter{}
	case model.RouteRoundRobin:
		return globalRoundRobin
	case model.RouteRandom:
		return &randomRouter{}
	case model.RouteConsistentHash:
		return &consistentHashRouter{}
	case model.RouteFailover:
		// Failover 路由直接使用传入的健康地址列表（已由 executorstore 异步预探）
		// 不再发起同步 Beat，退化为 First 策略选第一个健康节点
		return &failoverRouter{}
	case model.RouteLFU:
		return globalLFU
	case model.RouteLRU:
		return globalLRU
	default:
		return globalRoundRobin
	}
}

// ─── First ────────────────────────────────────────────────────────────────────

type firstRouter struct{}

func (r *firstRouter) Route(addresses []string, _ int64, _ string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("route: no executor available")
	}
	return addresses[0], nil
}

// ─── Last ─────────────────────────────────────────────────────────────────────

type lastRouter struct{}

func (r *lastRouter) Route(addresses []string, _ int64, _ string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("route: no executor available")
	}
	return addresses[len(addresses)-1], nil
}

// ─── Round-Robin ──────────────────────────────────────────────────────────────

type roundRobinRouter struct {
	mu      sync.Mutex
	counter map[string]int64 // key = address list hash
}

var globalRoundRobin = &roundRobinRouter{counter: make(map[string]int64)}

func (r *roundRobinRouter) Route(addresses []string, _ int64, _ string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("route: no executor available")
	}
	key := listHash(addresses)
	r.mu.Lock()
	idx := r.counter[key] % int64(len(addresses))
	r.counter[key]++
	r.mu.Unlock()
	return addresses[idx], nil
}

// ─── Random ───────────────────────────────────────────────────────────────────

type randomRouter struct{}

func (r *randomRouter) Route(addresses []string, _ int64, _ string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("route: no executor available")
	}
	//nolint:gosec
	return addresses[rand.Intn(len(addresses))], nil
}

// ─── Consistent Hash ──────────────────────────────────────────────────────────

type consistentHashRouter struct{}

func (r *consistentHashRouter) Route(addresses []string, jobID int64, param string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("route: no executor available")
	}
	h := fnv.New32a()
	_, _ = fmt.Fprintf(h, "%d:%s", jobID, param)
	idx := int(h.Sum32()) % len(addresses)
	return addresses[idx], nil
}

// ─── LFU (Least Frequently Used) ─────────────────────────────────────────────

type lfuRouter struct {
	mu    sync.Mutex
	count map[string]int64
}

var globalLFU = &lfuRouter{count: make(map[string]int64)}

func (r *lfuRouter) Route(addresses []string, _ int64, _ string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("route: no executor available")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	minAddr := addresses[0]
	minCount := r.count[minAddr]
	for _, addr := range addresses[1:] {
		if c := r.count[addr]; c < minCount {
			minCount = c
			minAddr = addr
		}
	}
	r.count[minAddr]++
	return minAddr, nil
}

// ─── LRU (Least Recently Used) ────────────────────────────────────────────────

type lruRouter struct {
	mu       sync.Mutex
	lastUsed map[string]time.Time
}

var globalLRU = &lruRouter{lastUsed: make(map[string]time.Time)}

func (r *lruRouter) Route(addresses []string, _ int64, _ string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("route: no executor available")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	oldest := addresses[0]
	oldestTime := r.lastUsed[oldest]
	for _, addr := range addresses[1:] {
		if t := r.lastUsed[addr]; t.Before(oldestTime) {
			oldestTime = t
			oldest = addr
		}
	}
	r.lastUsed[oldest] = time.Now()
	return oldest, nil
}

// ─── Failover ─────────────────────────────────────────────────────────────────

// failoverRouter 使用 executorstore 的异步预探健康结果。
//
// # 原架构（已废弃）
//
//	触发时同步发送 Beat → 最坏情况 N × 3s 阻塞 → 任务堆积
//
// # 新架构
//
//	executorstore.HealthProber 后台每 10s 探测一次，结果存入 Entry.healthy（原子位）。
//	路由时调用方（handleTask）已传入 ListHealthyAddresses() 的结果，
//	failoverRouter 只需从中选第一个即可，延迟降为 O(1) 内存读。
//
// # 降级策略
//
//	若健康地址列表为空（全部探测失败），handleTask 检测到 len==0，
//	记录失败日志，等待下次 reschedule。
type failoverRouter struct{}

// Route 从健康地址列表中选第一个（由 handleTask 传入已经过健康过滤的 addresses）。
func (r *failoverRouter) Route(addresses []string, _ int64, _ string) (string, error) {
	if len(addresses) == 0 {
		return "", fmt.Errorf("failover: no healthy executor available")
	}
	return addresses[0], nil
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func listHash(addrs []string) string {
	h := fnv.New64a()
	for _, a := range addrs {
		_, _ = fmt.Fprint(h, a)
	}
	return fmt.Sprintf("%x", h.Sum64())
}
