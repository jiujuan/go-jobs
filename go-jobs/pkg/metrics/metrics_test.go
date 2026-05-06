package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ═══════════════════════════════════════════════════════════════════════════════
// 辅助函数
// ═══════════════════════════════════════════════════════════════════════════════

// scrape 模拟 Prometheus 拉取，返回 /metrics 响应体。
func scrape(t *testing.T, p Provider) string {
	t.Helper()
	w := httptest.NewRecorder()
	p.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	require.Equal(t, http.StatusOK, w.Code)
	return strings.TrimSpace(w.Body.String())
}

func newProm(t *testing.T, opts ...ProviderOption) Provider {
	t.Helper()
	p, err := NewProvider(TypePrometheus, append([]ProviderOption{WithNamespace("t")}, opts...)...)
	require.NoError(t, err)
	return p
}

// ═══════════════════════════════════════════════════════════════════════════════
// UnknownProviderTypeError
// ═══════════════════════════════════════════════════════════════════════════════

func TestUnknownProviderTypeError_Message(t *testing.T) {
	err := &UnknownProviderTypeError{Type: "bad"}
	assert.Contains(t, err.Error(), "bad")
	assert.Contains(t, err.Error(), "unknown provider type")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Factory — NewProvider / MustNewProvider
// ═══════════════════════════════════════════════════════════════════════════════

func TestNewProvider_Noop(t *testing.T) {
	p, err := NewProvider(TypeNoop)
	require.NoError(t, err)
	assert.Equal(t, TypeNoop, p.Type())
}

func TestNewProvider_EmptyString_DefaultsToNoop(t *testing.T) {
	p, err := NewProvider("")
	require.NoError(t, err)
	assert.Equal(t, TypeNoop, p.Type())
}

func TestNewProvider_Prometheus(t *testing.T) {
	p, err := NewProvider(TypePrometheus, WithNamespace("x"))
	require.NoError(t, err)
	assert.Equal(t, TypePrometheus, p.Type())
}

func TestNewProvider_VictoriaMetrics_PullMode(t *testing.T) {
	p, err := NewProvider(TypeVictoriaMetrics, WithNamespace("x"))
	require.NoError(t, err)
	assert.Equal(t, TypeVictoriaMetrics, p.Type())
	p.Stop()
}

func TestNewProvider_UnknownType(t *testing.T) {
	_, err := NewProvider("unknown_backend_xyz")
	require.Error(t, err)
	var e *UnknownProviderTypeError
	require.ErrorAs(t, err, &e)
	assert.Equal(t, ProviderType("unknown_backend_xyz"), e.Type)
}

func TestMustNewProvider_Valid(t *testing.T) {
	assert.NotPanics(t, func() { MustNewProvider(TypeNoop) })
}

func TestMustNewProvider_Invalid_Panics(t *testing.T) {
	assert.Panics(t, func() { MustNewProvider("bad") })
}

// ═══════════════════════════════════════════════════════════════════════════════
// ProviderOptions
// ═══════════════════════════════════════════════════════════════════════════════

func TestProviderOptions_Defaults(t *testing.T) {
	o := defaultProviderOptions()
	assert.Equal(t, "gojobs", o.Namespace)
	assert.Equal(t, DefaultDurationBuckets, o.DefaultBuckets)
	assert.Equal(t, 15*time.Second, o.PushInterval)
	assert.Equal(t, "go-jobs", o.PushJobName)
}

func TestWithNamespace(t *testing.T) {
	o := defaultProviderOptions()
	WithNamespace("myapp")(o)
	assert.Equal(t, "myapp", o.Namespace)
}

func TestWithSubsystem(t *testing.T) {
	o := defaultProviderOptions()
	WithSubsystem("sched")(o)
	assert.Equal(t, "sched", o.Subsystem)
}

func TestWithConstLabels(t *testing.T) {
	o := defaultProviderOptions()
	WithConstLabels(map[string]string{"env": "prod"})(o)
	assert.Equal(t, "prod", o.ConstLabels["env"])
}

func TestWithDefaultBuckets(t *testing.T) {
	custom := []float64{1, 5, 10}
	o := defaultProviderOptions()
	WithDefaultBuckets(custom)(o)
	assert.Equal(t, custom, o.DefaultBuckets)
}

func TestWithPushURL(t *testing.T) {
	o := defaultProviderOptions()
	WithPushURL("http://vm:8428")(o)
	assert.Equal(t, "http://vm:8428", o.PushURL)
}

func TestWithPushInterval(t *testing.T) {
	o := defaultProviderOptions()
	WithPushInterval(30 * time.Second)(o)
	assert.Equal(t, 30*time.Second, o.PushInterval)
}

func TestWithPushJobName(t *testing.T) {
	o := defaultProviderOptions()
	WithPushJobName("my-job")(o)
	assert.Equal(t, "my-job", o.PushJobName)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Noop Provider
// ═══════════════════════════════════════════════════════════════════════════════

func TestNoop_Type(t *testing.T) {
	assert.Equal(t, TypeNoop, newNoopProvider().Type())
}

func TestNoop_Counter_SameInstance(t *testing.T) {
	p := newNoopProvider()
	// noop 返回全局单例，所有调用指向同一对象
	assert.Same(t, p.Counter("a"), p.Counter("b"))
}

func TestNoop_Gauge_SameInstance(t *testing.T) {
	p := newNoopProvider()
	assert.Same(t, p.Gauge("a"), p.Gauge("b"))
}

func TestNoop_Histogram_SameInstance(t *testing.T) {
	p := newNoopProvider()
	assert.Same(t, p.Histogram("a", nil), p.Histogram("b", nil))
}

func TestNoop_Instruments_NoPanic(t *testing.T) {
	p := newNoopProvider()
	assert.NotPanics(t, func() {
		c := p.Counter("c")
		c.Inc(); c.Add(100)
		g := p.Gauge("g")
		g.Set(1); g.Inc(); g.Dec(); g.Add(-1)
		h := p.Histogram("h", nil)
		h.Observe(100); h.ObserveDuration(time.Now().Add(-time.Second))
	})
}

func TestNoop_Handler_NoPanic(t *testing.T) {
	p := newNoopProvider()
	w := httptest.NewRecorder()
	assert.NotPanics(t, func() {
		p.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	})
}

func TestNoop_Stop_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() { newNoopProvider().Stop() })
}

// ═══════════════════════════════════════════════════════════════════════════════
// Prometheus — buildName / namespace / subsystem
// ═══════════════════════════════════════════════════════════════════════════════

func TestPrometheus_NamespacePrefix(t *testing.T) {
	p := newProm(t, WithNamespace("gojobs"))
	p.Counter("hits").Inc()
	assert.Contains(t, scrape(t, p), "gojobs_hits")
}

func TestPrometheus_EmptyNamespace_NoPrefix(t *testing.T) {
	p, _ := NewProvider(TypePrometheus) // namespace="" (default "gojobs" 被重置)
	pp := p.(*prometheusProvider)
	pp.opts.Namespace = "" // 强制清空 namespace
	p.Counter("raw").Inc()
	body := scrape(t, p)
	assert.Contains(t, body, "raw")
	assert.NotContains(t, body, "_raw")
}

func TestPrometheus_WithSubsystem(t *testing.T) {
	p, _ := NewProvider(TypePrometheus,
		WithNamespace("gojobs"),
		WithSubsystem("scheduler"),
	)
	p.Counter("triggers").Inc()
	assert.Contains(t, scrape(t, p), "gojobs_scheduler_triggers")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Prometheus — Counter
// ═══════════════════════════════════════════════════════════════════════════════

func TestPrometheus_Counter_Inc(t *testing.T) {
	p := newProm(t)
	p.Counter("hits").Inc()
	p.Counter("hits").Inc()
	assert.Contains(t, scrape(t, p), "t_hits 2")
}

func TestPrometheus_Counter_Add(t *testing.T) {
	p := newProm(t)
	p.Counter("bytes").Add(1024)
	assert.Contains(t, scrape(t, p), "t_bytes 1024")
}

func TestPrometheus_Counter_WithLabels(t *testing.T) {
	p := newProm(t)
	p.Counter("req", "method", "GET").Inc()
	p.Counter("req", "method", "POST").Inc()
	p.Counter("req", "method", "POST").Inc()
	body := scrape(t, p)
	assert.Contains(t, body, `method="GET"} 1`)
	assert.Contains(t, body, `method="POST"} 2`)
}

func TestPrometheus_Counter_SameLabels_SameInstance(t *testing.T) {
	p := newProm(t)
	c1 := p.Counter("hits", "k", "v")
	c2 := p.Counter("hits", "k", "v")
	assert.Same(t, c1, c2)
}

func TestPrometheus_Counter_DifferentLabels_DifferentInstances(t *testing.T) {
	p := newProm(t)
	c1 := p.Counter("hits", "k", "a")
	c2 := p.Counter("hits", "k", "b")
	assert.NotSame(t, c1, c2)
}

func TestPrometheus_Counter_ConstLabels_Appended(t *testing.T) {
	p := newProm(t, WithConstLabels(map[string]string{"env": "prod"}))
	p.Counter("req").Inc()
	assert.Contains(t, scrape(t, p), `env="prod"`)
}

func TestPrometheus_Counter_DynamicOverridesConst(t *testing.T) {
	// 动态标签 env=dev 优先于 const env=prod
	p := newProm(t, WithConstLabels(map[string]string{"env": "prod"}))
	p.Counter("req", "env", "dev").Inc()
	body := scrape(t, p)
	assert.Contains(t, body, `env="dev"`)
	assert.NotContains(t, body, `env="prod"`)
}

func TestPrometheus_Counter_TypeHeader(t *testing.T) {
	p := newProm(t)
	p.Counter("clicks").Inc()
	assert.Contains(t, scrape(t, p), "# TYPE t_clicks counter")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Prometheus — Gauge
// ═══════════════════════════════════════════════════════════════════════════════

func TestPrometheus_Gauge_Set(t *testing.T) {
	p := newProm(t)
	p.Gauge("temp").Set(42.5)
	assert.Contains(t, scrape(t, p), "t_temp 42.5")
}

func TestPrometheus_Gauge_IncDec(t *testing.T) {
	p := newProm(t)
	g := p.Gauge("cnt")
	g.Inc(); g.Inc(); g.Inc(); g.Dec()
	assert.Contains(t, scrape(t, p), "t_cnt 2")
}

func TestPrometheus_Gauge_AddNegative(t *testing.T) {
	p := newProm(t)
	g := p.Gauge("level")
	g.Set(10); g.Add(-3)
	assert.Contains(t, scrape(t, p), "t_level 7")
}

func TestPrometheus_Gauge_TypeHeader(t *testing.T) {
	p := newProm(t)
	p.Gauge("mem").Set(0)
	assert.Contains(t, scrape(t, p), "# TYPE t_mem gauge")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Prometheus — Histogram（核心：散射写入 + 累积读取）
// ═══════════════════════════════════════════════════════════════════════════════

func TestPrometheus_Histogram_SumCount(t *testing.T) {
	p := newProm(t)
	h := p.Histogram("dur", []float64{100, 500, 1000})
	h.Observe(50); h.Observe(200); h.Observe(800)
	body := scrape(t, p)
	assert.Contains(t, body, "t_dur_sum 1050")
	assert.Contains(t, body, "t_dur_count 3")
}

func TestPrometheus_Histogram_CumulativeBuckets(t *testing.T) {
	// 散射写入：每个观测值只计入一个桶
	// 累积读取：le=100 应含 ≤100 的所有观测值
	p := newProm(t)
	h := p.Histogram("lat", []float64{10, 100, 1000})
	h.Observe(5)   // → 桶 le=10
	h.Observe(50)  // → 桶 le=100
	h.Observe(500) // → 桶 le=1000
	h.Observe(2000) // → 桶 le=+Inf
	body := scrape(t, p)

	// 累积：le=10 → 1（仅 5ms）
	assert.Contains(t, body, `le="10"} 1`)
	// 累积：le=100 → 2（5 + 50ms）
	assert.Contains(t, body, `le="100"} 2`)
	// 累积：le=1000 → 3（5 + 50 + 500ms）
	assert.Contains(t, body, `le="1000"} 3`)
	// 累积：le=+Inf → 4（全部）
	assert.Contains(t, body, `le="+Inf"} 4`)
}

func TestPrometheus_Histogram_BucketExactlyOnBoundary(t *testing.T) {
	// val == upperBound 时应落在该桶（≤ 语义）
	p := newProm(t)
	h := p.Histogram("x", []float64{100})
	h.Observe(100) // 恰好等于边界
	body := scrape(t, p)
	assert.Contains(t, body, `le="100"} 1`)
	assert.Contains(t, body, `le="+Inf"} 1`)
}

func TestPrometheus_Histogram_ValueAboveAllBuckets(t *testing.T) {
	// val > 最大有限桶，应落入 +Inf 桶
	p := newProm(t)
	h := p.Histogram("x", []float64{10, 100})
	h.Observe(9999)
	body := scrape(t, p)
	assert.Contains(t, body, `le="10"} 0`)
	assert.Contains(t, body, `le="100"} 0`)
	assert.Contains(t, body, `le="+Inf"} 1`)
}

func TestPrometheus_Histogram_UnsortedBuckets_AutoSorted(t *testing.T) {
	// 乱序桶应自动排序
	p := newProm(t)
	h := p.Histogram("x", []float64{1000, 10, 100}) // 乱序
	h.Observe(50)
	body := scrape(t, p)
	// le=10 应在 le=100 之前（排序正确）
	pos10 := strings.Index(body, `le="10"`)
	pos100 := strings.Index(body, `le="100"`)
	assert.Less(t, pos10, pos100, "桶应按升序排列")
}

func TestPrometheus_Histogram_DuplicateBuckets_Deduped(t *testing.T) {
	// 重复桶应去重
	p := newProm(t)
	h := p.Histogram("x", []float64{100, 100, 100})
	h.Observe(50)
	body := scrape(t, p)
	// le=100 只出现一次
	count := strings.Count(body, `le="100"`)
	assert.Equal(t, 1, count, "重复桶应去重")
}

func TestPrometheus_Histogram_DefaultBuckets_WhenNil(t *testing.T) {
	custom := []float64{100, 500}
	p := newProm(t, WithDefaultBuckets(custom))
	p.Histogram("x", nil).Observe(200)
	body := scrape(t, p)
	assert.Contains(t, body, `le="100"`)
	assert.Contains(t, body, `le="500"`)
}

func TestPrometheus_Histogram_ObserveDuration_CountOne(t *testing.T) {
	p := newProm(t)
	h := p.Histogram("dur", nil)
	h.ObserveDuration(time.Now().Add(-50 * time.Millisecond))
	assert.Contains(t, scrape(t, p), "t_dur_count 1")
}

func TestPrometheus_Histogram_TypeHeader(t *testing.T) {
	p := newProm(t)
	p.Histogram("rtt", nil).Observe(1)
	assert.Contains(t, scrape(t, p), "# TYPE t_rtt histogram")
}

func TestPrometheus_Histogram_WithLabels(t *testing.T) {
	p := newProm(t)
	p.Histogram("dur", nil, "app", "email").Observe(100)
	body := scrape(t, p)
	assert.Contains(t, body, `app="email"`)
}

// ═══════════════════════════════════════════════════════════════════════════════
// Prometheus — Handler
// ═══════════════════════════════════════════════════════════════════════════════

func TestPrometheus_Handler_ContentType(t *testing.T) {
	p := newProm(t)
	w := httptest.NewRecorder()
	p.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	ct := w.Header().Get("Content-Type")
	assert.Contains(t, ct, "text/plain")
	assert.Contains(t, ct, "0.0.4")
}

func TestPrometheus_Handler_EmptyBody_WhenNoMetrics(t *testing.T) {
	p := newProm(t)
	assert.Empty(t, scrape(t, p))
}

// ═══════════════════════════════════════════════════════════════════════════════
// Prometheus — 并发安全（配合 -race 运行）
// ═══════════════════════════════════════════════════════════════════════════════

func TestPrometheus_Counter_ConcurrentInc(t *testing.T) {
	p := newProm(t)
	c := p.Counter("parallel")
	var wg sync.WaitGroup
	const n = 1000
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); c.Inc() }()
	}
	wg.Wait()
	assert.Contains(t, scrape(t, p), fmt.Sprintf("t_parallel %d", n))
}

func TestPrometheus_Gauge_ConcurrentAddSub(t *testing.T) {
	p := newProm(t)
	g := p.Gauge("cg")
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); g.Inc() }()
		go func() { defer wg.Done(); g.Dec() }()
	}
	wg.Wait()
	// 只验证无 data race，不验证最终值
}

func TestPrometheus_Histogram_ConcurrentObserve(t *testing.T) {
	p := newProm(t)
	h := p.Histogram("ch", nil)
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(v float64) { defer wg.Done(); h.Observe(v) }(float64(i))
	}
	wg.Wait()
	assert.Contains(t, scrape(t, p), "t_ch_count 500")
}

func TestPrometheus_ConcurrentRegistration(t *testing.T) {
	p := newProm(t)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 10 个不同 key，产生竞争注册
			p.Counter(fmt.Sprintf("m_%d", i%10), "i", fmt.Sprintf("%d", i)).Inc()
		}(i)
	}
	wg.Wait()
}

// ═══════════════════════════════════════════════════════════════════════════════
// 工具函数
// ═══════════════════════════════════════════════════════════════════════════════

func TestBuildKey_NoLabels(t *testing.T) {
	assert.Equal(t, "name", buildKey("name", nil))
	assert.Equal(t, "name", buildKey("name", []string{}))
}

func TestBuildKey_WithLabels(t *testing.T) {
	k := buildKey("name", []string{"a", "1", "b", "2"})
	assert.Contains(t, k, "name")
	assert.Contains(t, k, "a")
}

func TestMergeConst_Empty(t *testing.T) {
	result := mergeConst([]string{"a", "1"}, nil)
	assert.Equal(t, []string{"a", "1"}, result)
}

func TestMergeConst_AppendsConst(t *testing.T) {
	result := mergeConst([]string{"a", "1"}, map[string]string{"env": "prod"})
	m := sliceToMap(result)
	assert.Equal(t, "1", m["a"])
	assert.Equal(t, "prod", m["env"])
}

func TestMergeConst_DynamicOverrides(t *testing.T) {
	// 动态标签 env=dev 优先，const env=prod 被跳过
	result := mergeConst([]string{"env", "dev"}, map[string]string{"env": "prod"})
	m := sliceToMap(result)
	assert.Equal(t, "dev", m["env"])
	// env 只出现一次
	count := 0
	for _, v := range result {
		if v == "env" {
			count++
		}
	}
	assert.Equal(t, 1, count)
}

func TestFmtLabels_Empty(t *testing.T) {
	assert.Equal(t, "", fmtLabels(nil))
	assert.Equal(t, "", fmtLabels([]string{}))
}

func TestFmtLabels_Single(t *testing.T) {
	assert.Equal(t, `{k="v"}`, fmtLabels([]string{"k", "v"}))
}

func TestFmtLabels_Multiple(t *testing.T) {
	result := fmtLabels([]string{"a", "1", "b", "2"})
	assert.Equal(t, `{a="1",b="2"}`, result)
}

func TestEscapeLabel_NoSpecial(t *testing.T) {
	assert.Equal(t, "hello", escapeLabel("hello"))
}

func TestEscapeLabel_SpecialChars(t *testing.T) {
	result := escapeLabel(`say "hi"\n`)
	assert.Contains(t, result, `\"`)
	assert.Contains(t, result, `\\`)
	assert.Contains(t, result, `\n`)
}

func TestAppendKV(t *testing.T) {
	orig := []string{"a", "1"}
	result := appendKV(orig, "b", "2")
	assert.Equal(t, []string{"a", "1", "b", "2"}, result)
	// 不修改原始切片
	assert.Equal(t, []string{"a", "1"}, orig)
}

// sliceToMap 将 kv 切片转换为 map（测试辅助）。
func sliceToMap(kv []string) map[string]string {
	m := make(map[string]string)
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return m
}

// ═══════════════════════════════════════════════════════════════════════════════
// VictoriaMetrics Provider
// ═══════════════════════════════════════════════════════════════════════════════

func TestVictoriaMetrics_Type(t *testing.T) {
	p, err := NewProvider(TypeVictoriaMetrics, WithNamespace("vm"))
	require.NoError(t, err)
	assert.Equal(t, TypeVictoriaMetrics, p.Type())
	p.Stop()
}

func TestVictoriaMetrics_Counter_Inc(t *testing.T) {
	p, _ := NewProvider(TypeVictoriaMetrics, WithNamespace("vm"))
	p.Counter("events").Inc()
	p.Counter("events").Add(4)
	assert.Contains(t, scrape(t, p), "vm_events 5")
	p.Stop()
}

func TestVictoriaMetrics_Gauge_Set(t *testing.T) {
	p, _ := NewProvider(TypeVictoriaMetrics, WithNamespace("vm"))
	p.Gauge("load").Set(0.75)
	assert.Contains(t, scrape(t, p), "vm_load 0.75")
	p.Stop()
}

func TestVictoriaMetrics_PullMode_HandlerOK(t *testing.T) {
	p, _ := NewProvider(TypeVictoriaMetrics, WithNamespace("vm"))
	p.Counter("req").Inc()
	w := httptest.NewRecorder()
	p.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "vm_req")
	p.Stop()
}

func TestVictoriaMetrics_PushMode_SendsToServer(t *testing.T) {
	var received int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			atomic.AddInt64(&received, 1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, err := NewProvider(TypeVictoriaMetrics,
		WithNamespace("vm"),
		WithPushURL(srv.URL+"/api/v1/import/prometheus"),
		WithPushInterval(25*time.Millisecond),
	)
	require.NoError(t, err)
	p.Counter("ev").Inc()

	time.Sleep(100 * time.Millisecond) // 等待 ≥2 次 push
	p.Stop()

	assert.Greater(t, atomic.LoadInt64(&received), int64(0), "应至少推送一次")
}

func TestVictoriaMetrics_PushMode_StopFlushes(t *testing.T) {
	var received int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if len(body) > 0 {
			atomic.AddInt64(&received, 1)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, _ := NewProvider(TypeVictoriaMetrics,
		WithNamespace("vm"),
		WithPushURL(srv.URL),
		WithPushInterval(10*time.Minute), // 很长，不会自动触发
	)
	p.Counter("x").Add(99)
	p.Stop() // Stop 时应触发最后一次 flush

	assert.GreaterOrEqual(t, atomic.LoadInt64(&received), int64(1), "Stop 时应 flush")
}

func TestVictoriaMetrics_PushMode_Stats(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, _ := NewProvider(TypeVictoriaMetrics,
		WithNamespace("vm"),
		WithPushURL(srv.URL),
		WithPushInterval(20*time.Millisecond),
	)
	vp := p.(*victoriaMetricsProvider)
	p.Counter("y").Inc()
	time.Sleep(80 * time.Millisecond)
	p.Stop()

	success, _ := vp.PushStats()
	assert.Greater(t, success, int64(0))
}

func TestVictoriaMetrics_Stop_IdempotentWithoutPush(t *testing.T) {
	p, _ := NewProvider(TypeVictoriaMetrics, WithNamespace("vm"))
	assert.NotPanics(t, func() { p.Stop(); p.Stop() })
}

func TestVictoriaMetrics_PushJobName_InURL(t *testing.T) {
	var gotURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotURL = r.URL.String()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	p, _ := NewProvider(TypeVictoriaMetrics,
		WithPushURL(srv.URL),
		WithPushInterval(20*time.Millisecond),
		WithPushJobName("testjob"),
	)
	p.Counter("c").Inc()
	time.Sleep(50 * time.Millisecond)
	p.Stop()

	assert.Contains(t, gotURL, "testjob", "URL 应包含 job 标签")
}

// ═══════════════════════════════════════════════════════════════════════════════
// Metrics Facade
// ═══════════════════════════════════════════════════════════════════════════════

func newMetrics(t *testing.T) (*Metrics, Provider) {
	t.Helper()
	p := newProm(t, WithNamespace("gojobs"))
	return New(p), p
}

func TestMetrics_New_NoPanic(t *testing.T) {
	assert.NotPanics(t, func() { New(newNoopProvider()) })
}

func TestMetrics_RecordTrigger_CounterAndHistogram(t *testing.T) {
	m, p := newMetrics(t)
	m.RecordTrigger("email-app", "cron", "success", 100*time.Millisecond)
	m.RecordTrigger("email-app", "cron", "success", 200*time.Millisecond)
	m.RecordTrigger("sms-app", "manual", "fail", 50*time.Millisecond)
	body := scrape(t, p)
	// counter 有 app/trigger_type/status 标签
	assert.Contains(t, body, `gojobs_`+MetricTriggerTotal)
	assert.Contains(t, body, `app="email-app"`)
	assert.Contains(t, body, `status="success"`)
	// histogram 有 count
	assert.Contains(t, body, `gojobs_`+MetricTriggerDurationMs+`_count 3`)
}

func TestMetrics_RecordTrigger_DifferentLabels_SeparateSeries(t *testing.T) {
	m, p := newMetrics(t)
	m.RecordTrigger("app-a", "cron", "success", time.Second)
	m.RecordTrigger("app-b", "cron", "fail", time.Second)
	body := scrape(t, p)
	assert.Contains(t, body, `app="app-a"`)
	assert.Contains(t, body, `app="app-b"`)
}

func TestMetrics_SetActiveJobs(t *testing.T) {
	m, p := newMetrics(t)
	m.SetActiveJobs(42)
	assert.Contains(t, scrape(t, p), "gojobs_"+MetricJobActive+" 42")
}

func TestMetrics_IncDecActiveJobs(t *testing.T) {
	m, p := newMetrics(t)
	m.IncActiveJobs(); m.IncActiveJobs(); m.IncActiveJobs(); m.DecActiveJobs()
	assert.Contains(t, scrape(t, p), "gojobs_"+MetricJobActive+" 2")
}

func TestMetrics_SetWheelSize(t *testing.T) {
	m, p := newMetrics(t)
	m.SetWheelSize(256)
	assert.Contains(t, scrape(t, p), "gojobs_"+MetricWheelSize+" 256")
}

func TestMetrics_IncRateLimited(t *testing.T) {
	m, p := newMetrics(t)
	m.IncRateLimited("fast-app", "rate_exceeded")
	m.IncRateLimited("fast-app", "quota_exceeded")
	body := scrape(t, p)
	assert.Contains(t, body, "gojobs_"+MetricRateLimitedTotal)
	assert.Contains(t, body, `reason="rate_exceeded"`)
}

func TestMetrics_IncNoExecutor(t *testing.T) {
	m, p := newMetrics(t)
	m.IncNoExecutor("offline-app")
	assert.Contains(t, scrape(t, p), "gojobs_"+MetricNoExecutorTotal)
}

func TestMetrics_RecordRun(t *testing.T) {
	m, p := newMetrics(t)
	m.RecordRun("ordersJob", "success", 2*time.Second)
	m.RecordRun("ordersJob", "fail", 100*time.Millisecond)
	body := scrape(t, p)
	assert.Contains(t, body, "gojobs_"+MetricRunTotal)
	assert.Contains(t, body, "gojobs_"+MetricRunDurationMs)
	assert.Contains(t, body, `gojobs_`+MetricRunDurationMs+`_count 2`)
}

func TestMetrics_SetRunningJobs(t *testing.T) {
	m, p := newMetrics(t)
	m.SetRunningJobs(7)
	assert.Contains(t, scrape(t, p), "gojobs_"+MetricRunningJobs+" 7")
}

func TestMetrics_IncDecRunningJobs(t *testing.T) {
	m, p := newMetrics(t)
	m.IncRunningJobs(); m.IncRunningJobs(); m.DecRunningJobs()
	assert.Contains(t, scrape(t, p), "gojobs_"+MetricRunningJobs+" 1")
}

func TestMetrics_IncIdempotentRejected(t *testing.T) {
	m, p := newMetrics(t)
	m.IncIdempotentRejected("myHandler")
	m.IncIdempotentRejected("myHandler")
	body := scrape(t, p)
	assert.Contains(t, body, "gojobs_"+MetricIdempotentReject)
	assert.Contains(t, body, `handler="myHandler"`)
}

func TestMetrics_RateLimitAllowedRejected(t *testing.T) {
	m, p := newMetrics(t)
	m.IncRateLimitAllowed("app:email")
	m.IncRateLimitRejected("app:email", "rate_exceeded")
	body := scrape(t, p)
	assert.Contains(t, body, "gojobs_"+MetricRateLimitAllowed)
	assert.Contains(t, body, "gojobs_"+MetricRateLimitRejected)
}

func TestMetrics_Handler_DelegatesProvider(t *testing.T) {
	p := newProm(t)
	m := New(p)
	// Handler 应委托给 provider（不直接比较，验证内容一致）
	w1, w2 := httptest.NewRecorder(), httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	m.Handler().ServeHTTP(w1, req)
	p.Handler().ServeHTTP(w2, req)
	assert.Equal(t, w1.Body.String(), w2.Body.String())
}

func TestMetrics_Provider_ReturnsUnderlying(t *testing.T) {
	p, _ := NewProvider(TypeNoop)
	m := New(p)
	assert.Equal(t, p, m.Provider())
}

func TestMetrics_Stop_DelegatesProvider(t *testing.T) {
	p := newNoopProvider()
	m := New(p)
	assert.NotPanics(t, func() { m.Stop() })
}

func TestMetrics_Noop_AllMethodsNoPanic(t *testing.T) {
	m := New(newNoopProvider())
	assert.NotPanics(t, func() {
		m.RecordTrigger("a", "cron", "success", time.Second)
		m.SetActiveJobs(1); m.IncActiveJobs(); m.DecActiveJobs()
		m.SetWheelSize(10)
		m.IncRateLimited("a", "r"); m.IncNoExecutor("a")
		m.RecordRun("h", "success", time.Second)
		m.SetRunningJobs(1); m.IncRunningJobs(); m.DecRunningJobs()
		m.IncIdempotentRejected("h")
		m.IncRateLimitAllowed("k"); m.IncRateLimitRejected("k", "r")
		m.Stop()
	})
}

// ═══════════════════════════════════════════════════════════════════════════════
// Global Instance
// ═══════════════════════════════════════════════════════════════════════════════

func TestGlobal_DefaultIsNoop(t *testing.T) {
	g := Global()
	require.NotNil(t, g)
	assert.Equal(t, TypeNoop, g.Provider().Type())
}

func TestSetGlobal_ReplacesInstance(t *testing.T) {
	original := globalInstance
	defer func() { globalInstance = original }()

	p, _ := NewProvider(TypePrometheus, WithNamespace("new"))
	m := New(p)
	SetGlobal(m)
	assert.Same(t, m, Global())
}

// ═══════════════════════════════════════════════════════════════════════════════
// SchedulerObserver
// ═══════════════════════════════════════════════════════════════════════════════

func newObsMetrics(t *testing.T) (*Metrics, Provider) {
	t.Helper()
	p := newProm(t, WithNamespace("gojobs"))
	return New(p), p
}

func TestSchedulerObserver_StartStop_Idempotent(t *testing.T) {
	obs := NewSchedulerObserver(
		func() SchedulerStats { return SchedulerStats{} },
		New(newNoopProvider()),
	)
	obs.Start(context.Background())
	obs.Start(context.Background()) // 重复 Start 安全
	obs.Stop()
	obs.Stop() // 重复 Stop 安全
}

func TestSchedulerObserver_CollectsStats(t *testing.T) {
	m, p := newObsMetrics(t)
	var callCount int64

	obs := NewSchedulerObserver(
		func() SchedulerStats {
			atomic.AddInt64(&callCount, 1)
			return SchedulerStats{JobCount: 10, WheelSize: 50}
		},
		m,
		WithCollectInterval(20*time.Millisecond),
	)
	obs.Start(context.Background())
	time.Sleep(70 * time.Millisecond)
	obs.Stop()

	assert.Greater(t, atomic.LoadInt64(&callCount), int64(1), "应多次采集统计")
	body := scrape(t, p)
	assert.Contains(t, body, MetricJobActive)
	assert.Contains(t, body, MetricWheelSize)
}

func TestSchedulerObserver_CollectsWheelOverflow(t *testing.T) {
	m, p := newObsMetrics(t)
	obs := NewSchedulerObserver(
		func() SchedulerStats {
			return SchedulerStats{JobCount: 5, WheelSize: 30, WheelOverflow: 10}
		},
		m,
		WithCollectInterval(20*time.Millisecond),
	)
	obs.Start(context.Background())
	time.Sleep(50 * time.Millisecond)
	obs.Stop()

	body := scrape(t, p)
	// WheelSize = 30 + 10 = 40
	assert.Contains(t, body, MetricWheelSize+" 40")
}

func TestSchedulerObserver_NotifyTrigger_Recorded(t *testing.T) {
	m, p := newObsMetrics(t)
	obs := NewSchedulerObserver(
		func() SchedulerStats { return SchedulerStats{} },
		m,
		WithCollectInterval(time.Hour), // 禁止定时采集干扰
	)
	obs.Start(context.Background())
	obs.NotifyTrigger(TriggerEvent{
		App: "test-app", TriggerType: "cron", Status: "success",
		Duration: 100 * time.Millisecond,
	})
	time.Sleep(30 * time.Millisecond)
	obs.Stop()

	body := scrape(t, p)
	assert.Contains(t, body, MetricTriggerTotal)
	assert.Contains(t, body, `app="test-app"`)
}

func TestSchedulerObserver_NotifyRateLimited_Recorded(t *testing.T) {
	m, p := newObsMetrics(t)
	obs := NewSchedulerObserver(
		func() SchedulerStats { return SchedulerStats{} },
		m,
		WithCollectInterval(time.Hour),
	)
	obs.Start(context.Background())
	obs.NotifyRateLimited("slowapp", "quota_exceeded")
	time.Sleep(30 * time.Millisecond)
	obs.Stop()

	body := scrape(t, p)
	assert.Contains(t, body, MetricRateLimitedTotal)
	assert.Contains(t, body, `reason="quota_exceeded"`)
}

func TestSchedulerObserver_NotifyTrigger_NonBlocking_ChannelFull(t *testing.T) {
	// channel 满时 NotifyTrigger 不应阻塞
	obs := NewSchedulerObserver(
		func() SchedulerStats { return SchedulerStats{} },
		New(newNoopProvider()),
		WithTriggerChanSize(10),
	)
	// 不调用 Start()，eventLoop 不消费，channel 会快速满

	done := make(chan struct{})
	go func() {
		for i := 0; i < 2000; i++ {
			obs.NotifyTrigger(TriggerEvent{App: "x", TriggerType: "cron", Status: "success"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("NotifyTrigger blocked when channel is full")
	}
}

func TestSchedulerObserver_DrainOnStop(t *testing.T) {
	// Stop 时 drain 已缓冲事件（不丢失）
	m, p := newObsMetrics(t)
	obs := NewSchedulerObserver(
		func() SchedulerStats { return SchedulerStats{} },
		m,
		WithCollectInterval(time.Hour),
	)
	obs.Start(context.Background())
	for i := 0; i < 50; i++ {
		obs.NotifyTrigger(TriggerEvent{App: "app", TriggerType: "cron", Status: "success"})
	}
	obs.Stop()

	body := scrape(t, p)
	assert.Contains(t, body, MetricTriggerTotal)
}

func TestSchedulerObserver_WithTriggerChanSize(t *testing.T) {
	obs := NewSchedulerObserver(
		func() SchedulerStats { return SchedulerStats{} },
		New(newNoopProvider()),
		WithTriggerChanSize(512),
	)
	assert.Equal(t, 512, cap(obs.triggerCh))
}

// ═══════════════════════════════════════════════════════════════════════════════
// ExecutorObserver
// ═══════════════════════════════════════════════════════════════════════════════

func TestExecutorObserver_StartStop_Idempotent(t *testing.T) {
	eo := NewExecutorObserver(New(newNoopProvider()))
	eo.Start(); eo.Start()
	eo.Stop(); eo.Stop()
}

func TestExecutorObserver_NotifyRunStart_IncRunning(t *testing.T) {
	m, p := newObsMetrics(t)
	eo := NewExecutorObserver(m)
	eo.Start()
	eo.NotifyRunStart()
	eo.NotifyRunStart()
	assert.Contains(t, scrape(t, p), "gojobs_"+MetricRunningJobs+" 2")
	eo.Stop()
}

func TestExecutorObserver_NotifyRunFinish_DecrAndRecord(t *testing.T) {
	m, p := newObsMetrics(t)
	eo := NewExecutorObserver(m)
	eo.Start()
	eo.NotifyRunStart()
	eo.NotifyRunFinish(RunEvent{
		Handler: "myHandler", Status: "success", Duration: 500 * time.Millisecond,
	})
	time.Sleep(30 * time.Millisecond)
	body := scrape(t, p)
	assert.Contains(t, body, "gojobs_"+MetricRunningJobs+" 0")
	assert.Contains(t, body, "gojobs_"+MetricRunTotal)
	assert.Contains(t, body, `handler="myHandler"`)
	eo.Stop()
}

func TestExecutorObserver_NotifyIdempotentRejected(t *testing.T) {
	m, p := newObsMetrics(t)
	eo := NewExecutorObserver(m)
	eo.Start()
	eo.NotifyIdempotentRejected("dupHandler")
	time.Sleep(10 * time.Millisecond)
	body := scrape(t, p)
	assert.Contains(t, body, "gojobs_"+MetricIdempotentReject)
	assert.Contains(t, body, `handler="dupHandler"`)
	eo.Stop()
}

func TestExecutorObserver_NotifyRunFinish_NonBlocking(t *testing.T) {
	eo := NewExecutorObserver(New(newNoopProvider()))
	// 不 Start()，channel 满时不阻塞
	done := make(chan struct{})
	go func() {
		for i := 0; i < 5000; i++ {
			eo.NotifyRunFinish(RunEvent{Handler: "h", Status: "success"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("NotifyRunFinish blocked")
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// FormatJobID
// ═══════════════════════════════════════════════════════════════════════════════

func TestFormatJobID(t *testing.T) {
	assert.Equal(t, "0", FormatJobID(0))
	assert.Equal(t, "42", FormatJobID(42))
	assert.Equal(t, "-1", FormatJobID(-1))
	assert.Equal(t, "9223372036854775807", FormatJobID(1<<63-1))
}

// ═══════════════════════════════════════════════════════════════════════════════
// DefaultDurationBuckets
// ═══════════════════════════════════════════════════════════════════════════════

func TestDefaultDurationBuckets_Sorted(t *testing.T) {
	for i := 1; i < len(DefaultDurationBuckets); i++ {
		assert.Less(t, DefaultDurationBuckets[i-1], DefaultDurationBuckets[i],
			"DefaultDurationBuckets 应严格升序")
	}
}

func TestDefaultDurationBuckets_Range(t *testing.T) {
	assert.LessOrEqual(t, DefaultDurationBuckets[0], float64(50), "最小桶应 ≤ 50ms")
	assert.GreaterOrEqual(t, DefaultDurationBuckets[len(DefaultDurationBuckets)-1],
		float64(60_000), "最大桶应 ≥ 60s")
}

// ═══════════════════════════════════════════════════════════════════════════════
// VictoriaMetrics serialize（内部方法）
// ═══════════════════════════════════════════════════════════════════════════════

func TestVictoriaMetrics_Serialize_ContainsCounters(t *testing.T) {
	p, _ := NewProvider(TypeVictoriaMetrics, WithNamespace("vm"))
	p.Counter("hits").Inc()
	p.Gauge("load").Set(0.5)
	vp := p.(*victoriaMetricsProvider)
	body := string(vp.serialize())
	assert.Contains(t, body, "vm_hits")
	assert.Contains(t, body, "vm_load")
	p.Stop()
}

// ═══════════════════════════════════════════════════════════════════════════════
// Benchmark
// ═══════════════════════════════════════════════════════════════════════════════

func BenchmarkPrometheus_Counter_Inc_NoLabel(b *testing.B) {
	p, _ := NewProvider(TypePrometheus, WithNamespace("bench"))
	c := p.Counter("req")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkPrometheus_Counter_Inc_WithLabel(b *testing.B) {
	p, _ := NewProvider(TypePrometheus, WithNamespace("bench"))
	// 预注册（避免 benchmark 里混入注册开销）
	c := p.Counter("req", "status", "200")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			c.Inc()
		}
	})
}

func BenchmarkPrometheus_Histogram_Observe(b *testing.B) {
	p, _ := NewProvider(TypePrometheus, WithNamespace("bench"))
	h := p.Histogram("lat", nil)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Observe(100)
		}
	})
}

func BenchmarkMetrics_RecordTrigger(b *testing.B) {
	p, _ := NewProvider(TypePrometheus, WithNamespace("bench"))
	m := New(p)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.RecordTrigger("app", "cron", "success", 100*time.Millisecond)
		}
	})
}

func BenchmarkNoop_RecordTrigger(b *testing.B) {
	m := New(newNoopProvider())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.RecordTrigger("app", "cron", "success", 100*time.Millisecond)
		}
	})
}

// io 在测试辅助函数中用于 io.ReadAll。
var _ = io.Discard
