package metrics

import (
	"fmt"
	"net/http"
	"sync"
	"time"
)

// ─── prometheusProvider（Strategy Pattern 实现 #1）────────────────────────────
//
// 输出标准 Prometheus text/exposition format（version 0.0.4），
// 无需引入 prometheus/client_golang 外部依赖。
//
// 实现决策：内置轻量实现 vs prometheus/client_golang
//
//   - 输出格式与官方 SDK 完全相同，Prometheus / Grafana / VM 均可直接使用
//   - 无传递依赖，减少 go.sum 膨胀（约 30+ 个间接依赖）
//   - 若未来需要 exemplar、native histogram 等高级特性，
//     只需将本文件替换为 prometheus/client_golang 实现，
//     调用方（Metrics 门面 + 业务代码）无需任何改动
//
// Histogram 实现说明：
//
//   Prometheus histogram 使用"散射写入+累积读取"模型：
//   - Observe(v) 时，仅在 v <= upperBound 的最小桶（以及 +Inf）各自 +1
//     （非累积：每个桶仅计入该区间的观测值）
//   - writeTo() 时，按从小到大顺序累加输出（cumulative count），
//     符合 PromQL histogram_quantile 的前提
type prometheusProvider struct {
	opts *ProviderOptions

	mu         sync.RWMutex
	counters   map[string]*promCounter
	gauges     map[string]*promGauge
	histograms map[string]*promHistogram
}

func newPrometheusProvider(opts *ProviderOptions) (Provider, error) {
	return &prometheusProvider{
		opts:       opts,
		counters:   make(map[string]*promCounter),
		gauges:     make(map[string]*promGauge),
		histograms: make(map[string]*promHistogram),
	}, nil
}

func (p *prometheusProvider) Type() ProviderType { return TypePrometheus }
func (p *prometheusProvider) Stop()              {}

// ── Counter ───────────────────────────────────────────────────────────────────

func (p *prometheusProvider) Counter(name string, labels ...string) Counter {
	fullName := p.buildName(name)
	key := buildKey(fullName, labels)

	p.mu.RLock()
	if c, ok := p.counters[key]; ok {
		p.mu.RUnlock()
		return c
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if c, ok := p.counters[key]; ok { // double-check
		return c
	}
	c := &promCounter{
		name:   fullName,
		labels: mergeConst(labels, p.opts.ConstLabels),
	}
	p.counters[key] = c
	return c
}

// ── Gauge ─────────────────────────────────────────────────────────────────────

func (p *prometheusProvider) Gauge(name string, labels ...string) Gauge {
	fullName := p.buildName(name)
	key := buildKey(fullName, labels)

	p.mu.RLock()
	if g, ok := p.gauges[key]; ok {
		p.mu.RUnlock()
		return g
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if g, ok := p.gauges[key]; ok {
		return g
	}
	g := &promGauge{
		name:   fullName,
		labels: mergeConst(labels, p.opts.ConstLabels),
	}
	p.gauges[key] = g
	return g
}

// ── Histogram ────────────────────────────────────────────────────────────────

func (p *prometheusProvider) Histogram(name string, buckets []float64, labels ...string) Histogram {
	fullName := p.buildName(name)
	key := buildKey(fullName, labels)

	p.mu.RLock()
	if h, ok := p.histograms[key]; ok {
		p.mu.RUnlock()
		return h
	}
	p.mu.RUnlock()

	p.mu.Lock()
	defer p.mu.Unlock()
	if h, ok := p.histograms[key]; ok {
		return h
	}
	if len(buckets) == 0 {
		buckets = p.opts.DefaultBuckets
	}
	h := newPromHistogram(fullName, mergeConst(labels, p.opts.ConstLabels), buckets)
	p.histograms[key] = h
	return h
}

// ── Handler（/metrics 端点）──────────────────────────────────────────────────

func (p *prometheusProvider) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)

		p.mu.RLock()
		defer p.mu.RUnlock()

		for _, c := range p.counters {
			c.writeTo(w)
		}
		for _, g := range p.gauges {
			g.writeTo(w)
		}
		for _, h := range p.histograms {
			h.writeTo(w)
		}
	})
}

// ── buildName ─────────────────────────────────────────────────────────────────

func (p *prometheusProvider) buildName(name string) string {
	switch {
	case p.opts.Namespace == "":
		return name
	case p.opts.Subsystem == "":
		return p.opts.Namespace + "_" + name
	default:
		return p.opts.Namespace + "_" + p.opts.Subsystem + "_" + name
	}
}

// ─── promCounter ──────────────────────────────────────────────────────────────

type promCounter struct {
	name   string
	labels []string

	mu    sync.Mutex
	value float64
}

func (c *promCounter) Inc()          { c.Add(1) }
func (c *promCounter) Add(d float64) { c.mu.Lock(); c.value += d; c.mu.Unlock() }

func (c *promCounter) writeTo(w http.ResponseWriter) {
	c.mu.Lock()
	val := c.value
	c.mu.Unlock()
	fmt.Fprintf(w, "# TYPE %s counter\n%s%s %.17g\n",
		c.name, c.name, fmtLabels(c.labels), val)
}

// ─── promGauge ───────────────────────────────────────────────────────────────

type promGauge struct {
	name   string
	labels []string

	mu    sync.Mutex
	value float64
}

func (g *promGauge) Set(v float64) { g.mu.Lock(); g.value = v; g.mu.Unlock() }
func (g *promGauge) Inc()          { g.Add(1) }
func (g *promGauge) Dec()          { g.Add(-1) }
func (g *promGauge) Add(d float64) { g.mu.Lock(); g.value += d; g.mu.Unlock() }

func (g *promGauge) writeTo(w http.ResponseWriter) {
	g.mu.Lock()
	val := g.value
	g.mu.Unlock()
	fmt.Fprintf(w, "# TYPE %s gauge\n%s%s %.17g\n",
		g.name, g.name, fmtLabels(g.labels), val)
}

// ─── promHistogram ────────────────────────────────────────────────────────────
//
// 采用"散射写入 + 累积读取"模型：
//   - counts[i] 仅计入落在 (bounds[i-1], bounds[i]] 区间的观测值（非累积）
//   - writeTo() 按从小到大顺序累加，输出 Prometheus cumulative count

type promHistogram struct {
	name        string
	labels      []string
	upperBounds []float64 // 已排序去重，末尾为 maxFloat64 代表 +Inf

	mu     sync.Mutex
	counts []uint64 // len == len(upperBounds)，每桶独立计数（非累积）
	sum    float64
	total  uint64
}

// maxFloat64 作为 +Inf 桶的上边界哨兵值。
const maxFloat64 = float64(^uint64(0) >> 1) // 约 9.22e18，渲染时输出 "+Inf"

func newPromHistogram(name string, labels []string, buckets []float64) *promHistogram {
	// 深拷贝 + 排序 + 去重
	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	for i := 1; i < len(sorted); i++ { // 插入排序，桶数 < 20 时足够
		for j := i; j > 0 && sorted[j] < sorted[j-1]; j-- {
			sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
		}
	}
	deduped := sorted[:0]
	for i, v := range sorted {
		if i == 0 || v != sorted[i-1] {
			deduped = append(deduped, v)
		}
	}
	bounds := append(deduped, maxFloat64) // +Inf 哨兵

	return &promHistogram{
		name:        name,
		labels:      labels,
		upperBounds: bounds,
		counts:      make([]uint64, len(bounds)),
	}
}

// Observe 将 val 计入最小满足 val <= upperBound 的桶（散射写入）。
func (h *promHistogram) Observe(val float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sum += val
	h.total++
	// 线性扫描（桶数 < 20），找到第一个 val <= bound 的桶
	for i, ub := range h.upperBounds {
		if val <= ub {
			h.counts[i]++
			return
		}
	}
	// 安全兜底：+Inf 桶（val > 最大有限桶，但由于末尾有 maxFloat64 哨兵，正常不会走到这里）
	h.counts[len(h.counts)-1]++
}

func (h *promHistogram) ObserveDuration(since time.Time) {
	h.Observe(float64(time.Since(since).Milliseconds()))
}

// writeTo 输出 Prometheus cumulative histogram 格式。
func (h *promHistogram) writeTo(w http.ResponseWriter) {
	h.mu.Lock()
	counts := make([]uint64, len(h.counts))
	copy(counts, h.counts)
	sum := h.sum
	total := h.total
	bounds := h.upperBounds
	h.mu.Unlock()

	baseLabels := h.labels
	fmt.Fprintf(w, "# TYPE %s histogram\n", h.name)

	// 累积输出（从最小桶到 +Inf 依次累加）
	var cumCount uint64
	for i, ub := range bounds {
		cumCount += counts[i]
		var leVal string
		if ub >= maxFloat64 {
			leVal = "+Inf"
		} else {
			leVal = fmt.Sprintf("%g", ub)
		}
		leLabels := appendKV(baseLabels, "le", leVal)
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, fmtLabels(leLabels), cumCount)
	}
	fmt.Fprintf(w, "%s_sum%s %.17g\n", h.name, fmtLabels(baseLabels), sum)
	fmt.Fprintf(w, "%s_count%s %d\n", h.name, fmtLabels(baseLabels), total)
}

// ─── 工具函数 ─────────────────────────────────────────────────────────────────

// buildKey 根据 name + labels kv 切片生成注册表唯一键。
func buildKey(name string, labels []string) string {
	if len(labels) == 0 {
		return name
	}
	key := name
	for _, l := range labels {
		key += "\x00" + l
	}
	return key
}

// mergeConst 将 constLabels（固定标签，map）追加到 labels（动态标签，kv 切片）末尾。
// 若动态标签中已有同名 key，则跳过 constLabels 中的对应项（动态标签优先）。
func mergeConst(labels []string, constLabels map[string]string) []string {
	if len(constLabels) == 0 {
		return labels
	}
	existing := make(map[string]bool, len(labels)/2)
	for i := 0; i+1 < len(labels); i += 2 {
		existing[labels[i]] = true
	}
	result := make([]string, len(labels), len(labels)+len(constLabels)*2)
	copy(result, labels)
	for k, v := range constLabels {
		if !existing[k] {
			result = append(result, k, v)
		}
	}
	return result
}

// appendKV 返回在 labels 末尾追加 k/v 的新切片（不修改原切片）。
func appendKV(labels []string, k, v string) []string {
	out := make([]string, len(labels)+2)
	copy(out, labels)
	out[len(labels)] = k
	out[len(labels)+1] = v
	return out
}

// fmtLabels 将 kv 切片格式化为 Prometheus 标签字符串 {k="v",...}。
// 若 labels 为空，返回空字符串（无标签指标）。
func fmtLabels(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	s := "{"
	for i := 0; i+1 < len(labels); i += 2 {
		if i > 0 {
			s += ","
		}
		s += labels[i] + `="` + escapeLabel(labels[i+1]) + `"`
	}
	return s + "}"
}

// escapeLabel 转义标签值中的特殊字符（符合 Prometheus text format 规范）。
func escapeLabel(s string) string {
	var out []byte
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			out = append(out, '\\', '\\')
		case '"':
			out = append(out, '\\', '"')
		case '\n':
			out = append(out, '\\', 'n')
		default:
			out = append(out, s[i])
		}
	}
	if out == nil {
		return s // 无特殊字符，零分配
	}
	return string(out)
}
