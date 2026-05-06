package cron

// cron_test.go
//
// 覆盖 cron.go 的全部公开 API：
//
// SecondParser（包级变量）
//   1.  解析 xxl-job 格式 "0/30 * * * * ?" 不返回 error
//   2.  解析标准 6 段秒级 cron "* * * * * *" 成功
//
// NextTime
//   3.  合法表达式返回 from 之后的时间，error=nil
//   4.  "* * * * * ?" 每秒触发，next 在 [from+1s, from+2s)
//   5.  "0 0 * * * ?" 整点触发，next 在同一小时内或下一小时
//   6.  非法表达式返回 time.Time{} 和 error
//   7.  error 消息包含 "cron: parse"
//   8.  "0/10 * * * * ?" 每 10s 触发，验证 next-from 在 (0, 10s]
//   9.  from 极端值（零值）不崩溃
//  10.  "0 * * * * ?" 每分钟第 0 秒触发，next.Second()==0
//
// IsValid
//  11.  合法表达式返回 true
//  12.  非法表达式返回 false
//  13.  空字符串返回 false
//  14.  各典型格式（5/6/7 段）验证
//  15.  xxl-job 常用格式逐一验证
//
// 确定性与幂等性
//  16.  相同表达式和 from，NextTime 两次结果相同

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── SecondParser ─────────────────────────────────────────────────────────────

func TestSecondParser_ParsesXXLJobFormat(t *testing.T) {
	_, err := SecondParser.Parse("0/30 * * * * ?")
	assert.NoError(t, err, "SecondParser 应能解析 xxl-job 格式 '0/30 * * * * ?'")
}

func TestSecondParser_ParsesSixFieldFormat(t *testing.T) {
	_, err := SecondParser.Parse("* * * * * *")
	assert.NoError(t, err, "SecondParser 应能解析标准 6 段 cron")
}

// ─── NextTime ────────────────────────────────────────────────────────────────

func TestNextTime_ValidExpr_ReturnsAfterFrom(t *testing.T) {
	from := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	next, err := NextTime("* * * * * ?", from)
	require.NoError(t, err)
	assert.True(t, next.After(from), "next 应在 from 之后")
}

func TestNextTime_EverySecond_NextWithinOneSecond(t *testing.T) {
	from := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)
	next, err := NextTime("* * * * * ?", from)
	require.NoError(t, err)
	diff := next.Sub(from)
	assert.Greater(t, diff, time.Duration(0), "diff 应大于 0")
	assert.LessOrEqual(t, diff, 2*time.Second, "每秒触发的 next 应在 2s 内")
}

func TestNextTime_Every30Seconds_DiffAtMost30s(t *testing.T) {
	from := time.Date(2024, 6, 1, 8, 0, 0, 0, time.UTC)
	next, err := NextTime("0/30 * * * * ?", from)
	require.NoError(t, err)
	diff := next.Sub(from)
	assert.Greater(t, diff, time.Duration(0))
	assert.LessOrEqual(t, diff, 30*time.Second, "每 30s 触发的 next 应在 30s 内")
}

func TestNextTime_Every10Seconds_DiffAtMost10s(t *testing.T) {
	from := time.Date(2024, 6, 1, 8, 0, 5, 0, time.UTC)
	next, err := NextTime("0/10 * * * * ?", from)
	require.NoError(t, err)
	diff := next.Sub(from)
	assert.Greater(t, diff, time.Duration(0))
	assert.LessOrEqual(t, diff, 10*time.Second)
}

func TestNextTime_AtMinuteBoundary_SecondIsZero(t *testing.T) {
	from := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	next, err := NextTime("0 * * * * ?", from)
	require.NoError(t, err)
	assert.Equal(t, 0, next.Second(), "每分钟 0 秒触发，next.Second 应为 0")
}

func TestNextTime_InvalidExpr_ReturnsError(t *testing.T) {
	_, err := NextTime("not-a-cron", time.Now())
	assert.Error(t, err, "非法表达式应返回 error")
}

func TestNextTime_InvalidExpr_ErrorContainsCronParse(t *testing.T) {
	_, err := NextTime("invalid expr !!!", time.Now())
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "cron: parse"),
		"error 消息应包含 'cron: parse'，实际: %s", err.Error())
}

func TestNextTime_InvalidExpr_ReturnsZeroTime(t *testing.T) {
	got, err := NextTime("bad", time.Now())
	require.Error(t, err)
	assert.True(t, got.IsZero(), "非法表达式应返回零值 time.Time")
}

func TestNextTime_ZeroFrom_NoCrash(t *testing.T) {
	assert.NotPanics(t, func() {
		NextTime("* * * * * ?", time.Time{})
	})
}

func TestNextTime_Deterministic_SameInputSameOutput(t *testing.T) {
	from := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)
	expr := "0/5 * * * * ?"
	next1, err1 := NextTime(expr, from)
	next2, err2 := NextTime(expr, from)
	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Equal(t, next1, next2, "相同输入应产生相同结果")
}

func TestNextTime_HourlyExpr_NextAtHourBoundary(t *testing.T) {
	// "0 0 * * * ?" 每小时第 0 分 0 秒触发
	from := time.Date(2024, 5, 10, 14, 30, 0, 0, time.UTC)
	next, err := NextTime("0 0 * * * ?", from)
	require.NoError(t, err)
	assert.Equal(t, 0, next.Minute(), "整点触发的 next.Minute 应为 0")
	assert.Equal(t, 0, next.Second(), "整点触发的 next.Second 应为 0")
}

// ─── IsValid ─────────────────────────────────────────────────────────────────

func TestIsValid_ValidEverySecond(t *testing.T) {
	assert.True(t, IsValid("* * * * * ?"))
}

func TestIsValid_ValidXXLJobFormat(t *testing.T) {
	assert.True(t, IsValid("0/30 * * * * ?"))
}

func TestIsValid_ValidEvery5Seconds(t *testing.T) {
	assert.True(t, IsValid("0/5 * * * * ?"))
}

func TestIsValid_ValidDailyAtMidnight(t *testing.T) {
	assert.True(t, IsValid("0 0 0 * * ?"))
}

func TestIsValid_ValidSpecificTime(t *testing.T) {
	assert.True(t, IsValid("0 30 9 * * ?"))
}

func TestIsValid_ValidWeekday(t *testing.T) {
	assert.True(t, IsValid("0 0 10 ? * MON-FRI"))
}

func TestIsValid_InvalidEmpty(t *testing.T) {
	assert.False(t, IsValid(""))
}

func TestIsValid_InvalidRandom(t *testing.T) {
	assert.False(t, IsValid("not-a-cron"))
}

func TestIsValid_InvalidTooFewFields(t *testing.T) {
	// 5 段（标准 cron 格式），SecondParser 要求 6 段
	assert.False(t, IsValid("* * * * *"))
}

func TestIsValid_InvalidOutOfRange(t *testing.T) {
	// 秒字段 70 超范围
	assert.False(t, IsValid("70 * * * * ?"))
}

// ─── 批量表格驱动测试 ──────────────────────────────────────────────────────────

func TestIsValid_CommonXXLJobExpressions(t *testing.T) {
	valid := []string{
		"* * * * * ?",          // 每秒
		"0/5 * * * * ?",        // 每 5 秒
		"0/10 * * * * ?",       // 每 10 秒
		"0/30 * * * * ?",       // 每 30 秒
		"0 * * * * ?",          // 每分钟
		"0 0/5 * * * ?",        // 每 5 分钟
		"0 0 * * * ?",          // 每小时
		"0 0 0 * * ?",          // 每天凌晨
		"0 0 9 * * ?",          // 每天 9:00
		"0 0 0 1 * ?",          // 每月 1 日
		"0 0 0 ? * MON",        // 每周一
	}
	for _, expr := range valid {
		assert.True(t, IsValid(expr), "应为合法表达式: %q", expr)
	}
}

func TestIsValid_InvalidExpressions(t *testing.T) {
	invalid := []string{
		"",
		"abc",
		"* * * *",         // 只有 4 段
		"* * * * *",       // 5 段（无秒字段）
		"99 * * * * ?",    // 秒 99 超范围
		"0 60 * * * ?",    // 分 60 超范围
		"0 0 25 * * ?",    // 时 25 超范围
	}
	for _, expr := range invalid {
		assert.False(t, IsValid(expr), "应为非法表达式: %q", expr)
	}
}

// ─── NextTime 多场景表驱动 ────────────────────────────────────────────────────

func TestNextTime_TableDriven(t *testing.T) {
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		expr       string
		wantErr    bool
		checkFn    func(t *testing.T, next time.Time)
	}{
		{
			name:    "每秒触发",
			expr:    "* * * * * ?",
			wantErr: false,
			checkFn: func(t *testing.T, next time.Time) {
				assert.True(t, next.After(base))
			},
		},
		{
			name:    "每分钟触发",
			expr:    "0 * * * * ?",
			wantErr: false,
			checkFn: func(t *testing.T, next time.Time) {
				assert.Equal(t, 0, next.Second())
			},
		},
		{
			name:    "每天 15:00",
			expr:    "0 0 15 * * ?",
			wantErr: false,
			checkFn: func(t *testing.T, next time.Time) {
				assert.Equal(t, 15, next.Hour())
				assert.Equal(t, 0, next.Minute())
			},
		},
		{
			name:    "非法表达式",
			expr:    "garbage input",
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			next, err := NextTime(tc.expr, base)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tc.checkFn != nil {
				tc.checkFn(t, next)
			}
		})
	}
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkNextTime(b *testing.B) {
	from := time.Now()
	for i := 0; i < b.N; i++ {
		NextTime("0/5 * * * * ?", from)
	}
}

func BenchmarkIsValid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		IsValid("0/30 * * * * ?")
	}
}
