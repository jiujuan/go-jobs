package metrics

import (
	"net/http"
	"time"
)

// ─── noopProvider（Null Object Pattern）──────────────────────────────────────
//
// noopProvider 是 Provider 的空实现：所有操作均为 no-op，
// 不产生任何内存分配（返回全局单例 instrument），CPU 开销趋近于零。
//
// 使用场景：
//  1. 单元测试：不产生 metrics 副作用，测试专注业务逻辑。
//  2. 禁用 metrics：部署时只需切换 ProviderType，业务代码无需改动。
type noopProvider struct{}

func newNoopProvider() Provider { return &noopProvider{} }

func (p *noopProvider) Counter(_ string, _ ...string) Counter          { return _noopCounter }
func (p *noopProvider) Gauge(_ string, _ ...string) Gauge              { return _noopGauge }
func (p *noopProvider) Histogram(_ string, _ []float64, _ ...string) Histogram { return _noopHistogram }
func (p *noopProvider) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {})
}
func (p *noopProvider) Type() ProviderType { return TypeNoop }
func (p *noopProvider) Stop()              {}

// 全局单例 instrument 实例，所有 noopProvider 方法返回这同一组对象。
var (
	_noopCounter  Counter   = &noopCounter{}
	_noopGauge    Gauge     = &noopGauge{}
	_noopHistogram Histogram = &noopHistogram{}
)

type noopCounter struct{}

func (c *noopCounter) Inc()          {}
func (c *noopCounter) Add(_ float64) {}

type noopGauge struct{}

func (g *noopGauge) Set(_ float64) {}
func (g *noopGauge) Inc()          {}
func (g *noopGauge) Dec()          {}
func (g *noopGauge) Add(_ float64) {}

type noopHistogram struct{}

func (h *noopHistogram) Observe(_ float64)           {}
func (h *noopHistogram) ObserveDuration(_ time.Time) {}
