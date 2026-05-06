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

// Router selects one executor address from a list according to a strategy.
type Router interface {
	Route(addresses []string, jobID int64, param string) (string, error)
}

// NewRouter returns the Router implementation for the given strategy.
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
	counter map[string]int64 // key = sorted address list hash
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

// ─── helpers ──────────────────────────────────────────────────────────────────

func listHash(addrs []string) string {
	h := fnv.New64a()
	for _, a := range addrs {
		_, _ = fmt.Fprint(h, a)
	}
	return fmt.Sprintf("%x", h.Sum64())
}
