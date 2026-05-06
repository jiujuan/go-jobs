package paramtpl

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"text/template"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── helpers ──────────────────────────────────────────────────────────────────

func eng(opts ...Option) *Engine {
	return New(append([]Option{WithEnv("test"), WithNodeID("node-1")}, opts...)...)
}

func ctx0() TriggerContext {
	return TriggerContext{JobID: 1, ShardIndex: 0, ShardTotal: 1, TriggerType: "cron"}
}

// ─── hasTemplate ─────────────────────────────────────────────────────────────

func TestHasTemplate_WithBraces(t *testing.T)    { assert.True(t, hasTemplate(`{{.Date}}`)) }
func TestHasTemplate_WithoutBraces(t *testing.T) { assert.False(t, hasTemplate(`plain`)) }
func TestHasTemplate_Empty(t *testing.T)          { assert.False(t, hasTemplate("")) }
func TestHasTemplate_SingleBrace(t *testing.T)    { assert.False(t, hasTemplate("{x}")) }

// ─── Render — 快速路径 ────────────────────────────────────────────────────────

func TestRender_NoTemplate_ReturnsRaw(t *testing.T) {
	e := eng()
	result, err := e.Render(`{"date":"2025-01-01"}`, nil)
	require.NoError(t, err)
	assert.Equal(t, `{"date":"2025-01-01"}`, result)
}

func TestRender_Empty_ReturnsEmpty(t *testing.T) {
	e := eng()
	result, err := e.Render("", nil)
	require.NoError(t, err)
	assert.Equal(t, "", result)
}

// ─── 内置时间变量 ─────────────────────────────────────────────────────────────

func TestRender_Date(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{.Date}}`, v)
	require.NoError(t, err)
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, result)
}

func TestRender_DateTime(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{.DateTime}}`, v)
	require.NoError(t, err)
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`, result)
}

func TestRender_DateHour(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{.DateHour}}`, v)
	require.NoError(t, err)
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2} \d{2}$`, result)
}

func TestRender_Yesterday(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	today, _ := time.Parse("2006-01-02", v.Date)
	result, err := e.Render(`{{.Yesterday}}`, v)
	require.NoError(t, err)
	assert.Equal(t, today.AddDate(0, 0, -1).Format("2006-01-02"), result)
}

func TestRender_Tomorrow(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	today, _ := time.Parse("2006-01-02", v.Date)
	result, err := e.Render(`{{.Tomorrow}}`, v)
	require.NoError(t, err)
	assert.Equal(t, today.AddDate(0, 0, 1).Format("2006-01-02"), result)
}

func TestRender_StartAndEndOfDay(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	sod, _ := e.Render(`{{.StartOfDay}}`, v)
	eod, _ := e.Render(`{{.EndOfDay}}`, v)
	assert.True(t, strings.HasSuffix(sod, "00:00:00"))
	assert.True(t, strings.HasSuffix(eod, "23:59:59"))
}

func TestRender_Unix(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{.Unix}}`, v)
	require.NoError(t, err)
	assert.Regexp(t, `^\d+$`, result)
}

func TestRender_Year(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{.Year}}`, v)
	require.NoError(t, err)
	assert.Regexp(t, `^\d{4}$`, result)
}

func TestRender_WeekdayAndMonth(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	wd, _ := e.Render(`{{.Weekday}}`, v)
	mo, _ := e.Render(`{{.Month}}`, v)
	assert.NotEmpty(t, wd)
	assert.NotEmpty(t, mo)
}

// ─── 环境变量 ─────────────────────────────────────────────────────────────────

func TestRender_Env(t *testing.T) {
	e := eng(WithEnv("production"))
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{.Env}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "production", result)
}

func TestRender_NodeID(t *testing.T) {
	e := eng(WithNodeID("scheduler-007"))
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{.NodeID}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "scheduler-007", result)
}

// ─── 调度上下文变量 ───────────────────────────────────────────────────────────

func TestRender_ShardIndexAndTotal(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{ShardIndex: 3, ShardTotal: 8})
	result, err := e.Render(`idx={{.ShardIndex}},total={{.ShardTotal}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "idx=3,total=8", result)
}

func TestRender_JobID(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{JobID: 999})
	result, err := e.Render(`job-{{.JobID}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "job-999", result)
}

func TestRender_TriggerType(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{TriggerType: "manual"})
	result, err := e.Render(`{{.TriggerType}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "manual", result)
}

// ─── Extra 自定义变量 ─────────────────────────────────────────────────────────

func TestRender_ExtraVariable(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{
		Extra: map[string]any{"Region": "cn-east", "Table": "orders"},
	})
	result, err := e.Render(`region={{.Region}},table={{.Table}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "region=cn-east,table=orders", result)
}

func TestRender_Extra_OverridesBuiltin(t *testing.T) {
	e := eng(WithEnv("dev"))
	v := e.BuildVars(TriggerContext{Extra: map[string]any{"Env": "OVERRIDE"}})
	result, err := e.Render(`{{.Env}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "OVERRIDE", result)
}

// ─── 内置函数 ─────────────────────────────────────────────────────────────────

func TestRender_FuncDaysAgo(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{daysAgo 1}}`, v)
	require.NoError(t, err)
	assert.Equal(t, time.Now().AddDate(0, 0, -1).Format("2006-01-02"), result)
}

func TestRender_FuncDaysAfter(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{daysAfter 7}}`, v)
	require.NoError(t, err)
	assert.Equal(t, time.Now().AddDate(0, 0, 7).Format("2006-01-02"), result)
}

func TestRender_FuncAdd(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{ShardIndex: 3})
	result, err := e.Render(`{{add .ShardIndex 1}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "4", result)
}

func TestRender_FuncMod(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{ShardIndex: 7})
	result, err := e.Render(`{{mod .ShardIndex 4}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "3", result)
}

func TestRender_FuncDefault_UsesDefault(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{Extra: map[string]any{"Tag": ""}})
	result, err := e.Render(`{{default "latest" .Tag}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "latest", result)
}

func TestRender_FuncDefault_UsesValue(t *testing.T) {
	e := eng()
	v := e.BuildVars(TriggerContext{Extra: map[string]any{"Tag": "v1.2.3"}})
	result, err := e.Render(`{{default "latest" .Tag}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "v1.2.3", result)
}

func TestRender_FuncFormatDate(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	result, err := e.Render(`{{formatDate .Now "2006/01/02"}}`, v)
	require.NoError(t, err)
	assert.Regexp(t, `^\d{4}/\d{2}/\d{2}$`, result)
}

// ─── 自定义 FuncMap ───────────────────────────────────────────────────────────

func TestRender_CustomFuncMap(t *testing.T) {
	e := eng(WithFuncMap(template.FuncMap{
		"upper": strings.ToUpper,
	}))
	v := e.BuildVars(TriggerContext{Extra: map[string]any{"Name": "hello"}})
	result, err := e.Render(`{{upper .Name}}`, v)
	require.NoError(t, err)
	assert.Equal(t, "HELLO", result)
}

// ─── 复杂 JSON 模板 ───────────────────────────────────────────────────────────

func TestRender_ComplexJSON(t *testing.T) {
	e := eng(WithEnv("prod"))
	v := e.BuildVars(TriggerContext{
		JobID: 42, ShardIndex: 1, ShardTotal: 4,
		Extra: map[string]any{"Table": "orders"},
	})
	tpl := `{"date":"{{.Yesterday}}","table":"{{.Table}}","shard":{{.ShardIndex}},"total":{{.ShardTotal}},"env":"{{.Env}}"}`
	result, err := e.Render(tpl, v)
	require.NoError(t, err)
	assert.Contains(t, result, `"table":"orders"`)
	assert.Contains(t, result, `"shard":1`)
	assert.Contains(t, result, `"env":"prod"`)
}

// ─── 错误处理 ─────────────────────────────────────────────────────────────────

func TestRender_InvalidSyntax_ReturnsError(t *testing.T) {
	e := eng()
	_, err := e.Render(`{{.Broken`, nil)
	assert.Error(t, err)
}

func TestRender_UndefinedKey_ReturnsError(t *testing.T) {
	// missingkey=error：引用未定义 key 时报错
	e := eng()
	v := e.BuildVars(ctx0()) // extra 为空
	_, err := e.Render(`{{.UndefinedKey}}`, v)
	assert.Error(t, err, "引用未定义 key 应返回错误")
}

func TestRender_NilVars_UsesDefaults(t *testing.T) {
	e := eng(WithEnv("staging"))
	result, err := e.Render(`{{.Env}}`, nil)
	require.NoError(t, err)
	assert.Equal(t, "staging", result)
}

// ─── Validate ─────────────────────────────────────────────────────────────────

func TestValidate_Valid(t *testing.T) {
	assert.NoError(t, eng().Validate(`{{.Date}}`))
}

func TestValidate_Invalid(t *testing.T) {
	assert.Error(t, eng().Validate(`{{.Broken`))
}

func TestValidate_NoTemplate(t *testing.T) {
	assert.NoError(t, eng().Validate(`plain`))
}

// ─── MustRender ───────────────────────────────────────────────────────────────

func TestMustRender_Valid(t *testing.T) {
	e := eng()
	v := e.BuildVars(ctx0())
	assert.Equal(t, "test", e.MustRender(`{{.Env}}`, v))
}

func TestMustRender_Invalid_Panics(t *testing.T) {
	assert.Panics(t, func() { eng().MustRender(`{{.Broken`, nil) })
}

// ─── 缓存 ─────────────────────────────────────────────────────────────────────

func TestCache_HitAfterFirst(t *testing.T) {
	e := eng(WithCacheSize(100))
	v := e.BuildVars(ctx0())
	e.Render(`{{.Date}}`, v)
	assert.Equal(t, 1, e.CacheLen())
	e.Render(`{{.Date}}`, v)
	assert.Equal(t, 1, e.CacheLen())
}

func TestCache_DifferentTemplates(t *testing.T) {
	e := eng(WithCacheSize(100))
	v := e.BuildVars(ctx0())
	e.Render(`{{.Date}}`, v)
	e.Render(`{{.Env}}`, v)
	assert.Equal(t, 2, e.CacheLen())
}

func TestCache_ClearCache(t *testing.T) {
	e := eng(WithCacheSize(100))
	e.Render(`{{.Date}}`, nil)
	e.ClearCache()
	assert.Equal(t, 0, e.CacheLen())
}

func TestCache_Eviction(t *testing.T) {
	e := eng(WithCacheSize(3))
	v := e.BuildVars(ctx0())
	for i := 0; i < 10; i++ {
		e.Render(fmt.Sprintf(`{{.Date}}-suffix-%d`, i), v)
	}
	assert.LessOrEqual(t, e.CacheLen(), 3)
}

func TestCache_Disabled(t *testing.T) {
	e := eng(WithCacheSize(0))
	v := e.BuildVars(ctx0())
	e.Render(`{{.Date}}`, v)
	assert.Equal(t, 0, e.CacheLen(), "缓存禁用时不应缓存")
}

// ─── BuildVars ────────────────────────────────────────────────────────────────

func TestBuildVars_AllBuiltinFields(t *testing.T) {
	e := eng(WithEnv("staging"), WithNodeID("n-99"))
	v := e.BuildVars(TriggerContext{
		JobID: 100, ShardIndex: 2, ShardTotal: 8, TriggerType: "retry",
	})
	assert.Equal(t, int64(100), v.JobID)
	assert.Equal(t, 2, v.ShardIndex)
	assert.Equal(t, 8, v.ShardTotal)
	assert.Equal(t, "retry", v.TriggerType)
	assert.Equal(t, "staging", v.Env)
	assert.Equal(t, "n-99", v.NodeID)
	assert.NotEmpty(t, v.Date)
	assert.NotEmpty(t, v.Yesterday)
	assert.NotEmpty(t, v.Tomorrow)
	assert.NotZero(t, v.Unix)
	assert.NotZero(t, v.Year)
}

func TestBuildVars_NilExtra_NoPanic(t *testing.T) {
	require.NotPanics(t, func() {
		eng().BuildVars(TriggerContext{})
	})
}

// ─── 并发安全 ─────────────────────────────────────────────────────────────────

func TestEngine_ConcurrentRender(t *testing.T) {
	e := eng(WithCacheSize(100))
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := e.BuildVars(TriggerContext{JobID: int64(i)})
			_, err := e.Render(`{"job":{{.JobID}},"date":"{{.Date}}","env":"{{.Env}}"}`, v)
			assert.NoError(t, err)
		}(i)
	}
	wg.Wait()
}

func TestEngine_ConcurrentClearAndRender(t *testing.T) {
	e := eng(WithCacheSize(50))
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			e.Render(`{{.Date}}`, nil)
		}()
		go func() {
			defer wg.Done()
			e.ClearCache()
		}()
	}
	wg.Wait()
}

// ─── Benchmarks ───────────────────────────────────────────────────────────────

func BenchmarkRender_Cached(b *testing.B) {
	e := eng()
	v := e.BuildVars(ctx0())
	tpl := `{"date":"{{.Yesterday}}","env":"{{.Env}}","job":{{.JobID}}}`
	e.Render(tpl, v) // warm
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			e.Render(tpl, v)
		}
	})
}

func BenchmarkRender_NoTemplate(b *testing.B) {
	e := eng()
	plain := `{"date":"2025-01-01","env":"prod"}`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.Render(plain, nil)
	}
}

func BenchmarkBuildVars(b *testing.B) {
	e := eng()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e.BuildVars(TriggerContext{JobID: 1, ShardIndex: 0, ShardTotal: 4})
	}
}
