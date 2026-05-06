package metrics

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

// ─── victoriaMetricsProvider（Strategy Pattern 实现 #2）──────────────────────
//
// VictoriaMetrics 完全兼容 Prometheus text/exposition 格式，
// 因此本实现通过 Go 结构体组合（组合复用）内嵌 prometheusProvider，
// 复用全部 Counter / Gauge / Histogram 逻辑，
// 仅在 Type() / Handler() / Stop() 三点做差异化：
//
//   Pull 模式（PushURL == ""）：
//     完全等同于 PrometheusProvider，暴露 /metrics 供 scraper 拉取。
//     VictoriaMetrics 的 vmagent / vmselect 均支持 Prometheus scrape 协议。
//
//   Push 模式（PushURL != ""）：
//     后台 goroutine 定时将所有指标序列化为 Prometheus text format，
//     HTTP POST 到 VictoriaMetrics /api/v1/import/prometheus 端点。
//     Push 模式下 Handler() 仍可用（双模式），便于本地调试。
//     Stop() 会取消后台 goroutine 并等待最后一次 push 完成（数据不丢失）。
//
// 代码复用策略：
//   victoriaMetricsProvider 内嵌 *prometheusProvider（Go 组合）。
//   Counter / Gauge / Histogram 方法由嵌入字段自动提升（promotion），
//   本结构体无需重新实现。
type victoriaMetricsProvider struct {
	*prometheusProvider // 组合复用：Counter / Gauge / Histogram 全部提升

	pushURL      string
	pushInterval time.Duration
	pushJobName  string
	httpClient   *http.Client

	pushCtx    context.Context
	pushCancel context.CancelFunc
	pushWg     sync.WaitGroup

	// 原子计数器，供健康检查和测试断言使用
	pushSuccessTotal int64
	pushFailTotal    int64
}

func newVictoriaMetricsProvider(opts *ProviderOptions) (Provider, error) {
	inner, err := newPrometheusProvider(opts)
	if err != nil {
		return nil, err
	}
	pushCtx, pushCancel := context.WithCancel(context.Background())

	interval := opts.PushInterval
	if interval <= 0 {
		interval = 15 * time.Second
	}
	jobName := opts.PushJobName
	if jobName == "" {
		jobName = "go-jobs"
	}

	p := &victoriaMetricsProvider{
		prometheusProvider: inner.(*prometheusProvider),
		pushURL:            opts.PushURL,
		pushInterval:       interval,
		pushJobName:        jobName,
		httpClient:         &http.Client{Timeout: 10 * time.Second},
		pushCtx:            pushCtx,
		pushCancel:         pushCancel,
	}

	// 仅 Push 模式才启动后台 goroutine
	if p.pushURL != "" {
		p.pushWg.Add(1)
		go p.pushLoop()
	}

	return p, nil
}

func (p *victoriaMetricsProvider) Type() ProviderType { return TypeVictoriaMetrics }

// Handler 在 Push 模式下仍暴露 /metrics，便于调试。
// 复用内嵌 prometheusProvider 的 Handler（行为完全相同）。
func (p *victoriaMetricsProvider) Handler() http.Handler {
	return p.prometheusProvider.Handler()
}

// Stop 取消 Push 后台 goroutine，并等待最后一次 flush 完成。
// Pull 模式下无 goroutine，Stop 为 no-op（但调用安全）。
func (p *victoriaMetricsProvider) Stop() {
	p.pushCancel()
	p.pushWg.Wait()
}

// PushStats 返回累计推送成功/失败次数（原子读取，线程安全）。
// 主要用于健康检查和集成测试断言。
func (p *victoriaMetricsProvider) PushStats() (success, fail int64) {
	return atomic.LoadInt64(&p.pushSuccessTotal), atomic.LoadInt64(&p.pushFailTotal)
}

// ── Push 后台循环 ─────────────────────────────────────────────────────────────

func (p *victoriaMetricsProvider) pushLoop() {
	defer p.pushWg.Done()
	ticker := time.NewTicker(p.pushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.pushCtx.Done():
			p.doPush() // Stop 时做最后一次 flush，保证数据完整性
			return
		case <-ticker.C:
			p.doPush()
		}
	}
}

func (p *victoriaMetricsProvider) doPush() {
	body := p.serialize()
	if len(body) == 0 {
		return
	}

	// 构造目标 URL，附加 job 标签（VictoriaMetrics extra_label 语法）
	url := p.pushURL
	if p.pushJobName != "" {
		url = fmt.Sprintf("%s?extra_label=job=%s", p.pushURL, p.pushJobName)
	}

	req, err := http.NewRequestWithContext(p.pushCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		atomic.AddInt64(&p.pushFailTotal, 1)
		return
	}
	req.Header.Set("Content-Type", "text/plain; version=0.0.4")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		atomic.AddInt64(&p.pushFailTotal, 1)
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode >= 400 {
		atomic.AddInt64(&p.pushFailTotal, 1)
	} else {
		atomic.AddInt64(&p.pushSuccessTotal, 1)
	}
}

// serialize 将所有已注册指标序列化为 Prometheus text format 字节流。
// 复用 promCounter / promGauge / promHistogram 的 writeTo 方法。
func (p *victoriaMetricsProvider) serialize() []byte {
	var buf bytes.Buffer
	bw := &vmBufWriter{&buf}

	p.prometheusProvider.mu.RLock()
	defer p.prometheusProvider.mu.RUnlock()

	for _, c := range p.prometheusProvider.counters {
		c.writeTo(bw)
	}
	for _, g := range p.prometheusProvider.gauges {
		g.writeTo(bw)
	}
	for _, h := range p.prometheusProvider.histograms {
		h.writeTo(bw)
	}
	return buf.Bytes()
}

// vmBufWriter 将 bytes.Buffer 适配为 http.ResponseWriter（仅 Write 有效）。
// 用于 serialize() 中复用 writeTo(http.ResponseWriter) 方法。
type vmBufWriter struct{ *bytes.Buffer }

func (b *vmBufWriter) Header() http.Header         { return http.Header{} }
func (b *vmBufWriter) WriteHeader(_ int)            {}
func (b *vmBufWriter) Write(p []byte) (int, error) { return b.Buffer.Write(p) }
