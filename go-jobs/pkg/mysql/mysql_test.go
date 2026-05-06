package mysql

// mysql_test.go
//
// 覆盖 mysql.go 的全部可测逻辑。
//
// 无需真实 MySQL 实例的测试（纯内存 / 立即失败路径）：
//
// defaultOptions
//   1.  MaxOpenConns  默认 100
//   2.  MaxIdleConns  默认 10
//   3.  ConnMaxLifetime 默认 1h
//   4.  ConnMaxIdleTime 默认 30m
//   5.  SlowThreshold  默认 200ms
//   6.  LogLevel 默认 logger.Warn
//   7.  DSN 默认 ""（空字符串）
//
// Option 工厂函数
//   8.  WithDSN 修改 DSN 字段
//   9.  WithMaxOpenConns 修改 MaxOpenConns
//  10.  WithMaxIdleConns 修改 MaxIdleConns
//  11.  WithConnMaxLifetime 修改 ConnMaxLifetime
//  12.  WithConnMaxIdleTime 修改 ConnMaxIdleTime
//  13.  WithSlowThreshold 修改 SlowThreshold
//  14.  WithLogLevel 修改 LogLevel
//  15.  多个选项叠加全部生效
//
// New —— 不依赖真实 MySQL 的失败路径
//  16.  DSN="" 时立即返回 error（校验前置检查）
//  17.  error 消息包含 "mysql: DSN is required"
//  18.  DSN 格式非法时返回 error（gorm/mysql driver 解析失败）
//  19.  error 消息包含 "mysql:"
//  20.  成功时返回非 nil *gorm.DB（需要 MYSQL_DSN 环境变量，否则 Skip）
//
// MustNew
//  21.  DSN="" 时 panic（内部调用 New，返回 error → panic）
//  22.  panic 消息来自 New 的 error
//  23.  成功时返回非 nil（需要 MYSQL_DSN，否则 Skip）
//
// 选项组合 smoke test
//  24.  所有 Option 叠加后再调用 New(DSN="") 仍返回 error（选项不影响前置校验）

import (
	"os"
	"testing"
	"time"

	gormlogger "gorm.io/gorm/logger"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─── 1-7. defaultOptions ─────────────────────────────────────────────────────

func TestDefaultOptions_MaxOpenConns(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 100, o.MaxOpenConns)
}

func TestDefaultOptions_MaxIdleConns(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 10, o.MaxIdleConns)
}

func TestDefaultOptions_ConnMaxLifetime(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, time.Hour, o.ConnMaxLifetime)
}

func TestDefaultOptions_ConnMaxIdleTime(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 30*time.Minute, o.ConnMaxIdleTime)
}

func TestDefaultOptions_SlowThreshold(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, 200*time.Millisecond, o.SlowThreshold)
}

func TestDefaultOptions_LogLevel(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, gormlogger.Warn, o.LogLevel)
}

func TestDefaultOptions_DSN_Empty(t *testing.T) {
	o := defaultOptions()
	assert.Equal(t, "", o.DSN)
}

// ─── 8-15. Option 工厂函数 ────────────────────────────────────────────────────

func TestWithDSN_SetsField(t *testing.T) {
	o := defaultOptions()
	WithDSN("user:pass@tcp(host:3306)/db")(o)
	assert.Equal(t, "user:pass@tcp(host:3306)/db", o.DSN)
}

func TestWithMaxOpenConns_SetsField(t *testing.T) {
	o := defaultOptions()
	WithMaxOpenConns(50)(o)
	assert.Equal(t, 50, o.MaxOpenConns)
}

func TestWithMaxIdleConns_SetsField(t *testing.T) {
	o := defaultOptions()
	WithMaxIdleConns(5)(o)
	assert.Equal(t, 5, o.MaxIdleConns)
}

func TestWithConnMaxLifetime_SetsField(t *testing.T) {
	o := defaultOptions()
	WithConnMaxLifetime(30 * time.Minute)(o)
	assert.Equal(t, 30*time.Minute, o.ConnMaxLifetime)
}

func TestWithConnMaxIdleTime_SetsField(t *testing.T) {
	o := defaultOptions()
	WithConnMaxIdleTime(10 * time.Minute)(o)
	assert.Equal(t, 10*time.Minute, o.ConnMaxIdleTime)
}

func TestWithSlowThreshold_SetsField(t *testing.T) {
	o := defaultOptions()
	WithSlowThreshold(500 * time.Millisecond)(o)
	assert.Equal(t, 500*time.Millisecond, o.SlowThreshold)
}

func TestWithLogLevel_SetsField_Silent(t *testing.T) {
	o := defaultOptions()
	WithLogLevel(gormlogger.Silent)(o)
	assert.Equal(t, gormlogger.Silent, o.LogLevel)
}

func TestWithLogLevel_SetsField_Info(t *testing.T) {
	o := defaultOptions()
	WithLogLevel(gormlogger.Info)(o)
	assert.Equal(t, gormlogger.Info, o.LogLevel)
}

func TestWithLogLevel_SetsField_Error(t *testing.T) {
	o := defaultOptions()
	WithLogLevel(gormlogger.Error)(o)
	assert.Equal(t, gormlogger.Error, o.LogLevel)
}

func TestOptions_MultipleOptions_AllApplied(t *testing.T) {
	o := defaultOptions()
	WithDSN("root:pass@tcp(db:3306)/app")(o)
	WithMaxOpenConns(200)(o)
	WithMaxIdleConns(20)(o)
	WithConnMaxLifetime(2 * time.Hour)(o)
	WithConnMaxIdleTime(15 * time.Minute)(o)
	WithSlowThreshold(1 * time.Second)(o)
	WithLogLevel(gormlogger.Info)(o)

	assert.Equal(t, "root:pass@tcp(db:3306)/app", o.DSN)
	assert.Equal(t, 200, o.MaxOpenConns)
	assert.Equal(t, 20, o.MaxIdleConns)
	assert.Equal(t, 2*time.Hour, o.ConnMaxLifetime)
	assert.Equal(t, 15*time.Minute, o.ConnMaxIdleTime)
	assert.Equal(t, 1*time.Second, o.SlowThreshold)
	assert.Equal(t, gormlogger.Info, o.LogLevel)
}

// ─── 16-19. New —— 失败路径（不需要真实 MySQL）──────────────────────────────

func TestNew_EmptyDSN_ReturnsError(t *testing.T) {
	_, err := New()
	require.Error(t, err)
}

func TestNew_EmptyDSN_ErrorContainsDSNRequired(t *testing.T) {
	_, err := New()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mysql: DSN is required")
}

func TestNew_EmptyDSN_ReturnsNilDB(t *testing.T) {
	db, _ := New()
	assert.Nil(t, db)
}

func TestNew_InvalidDSN_ReturnsError(t *testing.T) {
	// 格式错误的 DSN（完全无效格式），gorm/mysql 驱动会解析失败
	_, err := New(WithDSN("not-a-valid-dsn-at-all"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mysql:")
}

func TestNew_UnreachableDSN_ReturnsError(t *testing.T) {
	// 格式合法但服务器不可达，gorm.Open 会尝试连接然后失败
	// 使用极短超时的 DSN 参数，确保快速失败
	_, err := New(WithDSN("root:@tcp(127.0.0.1:13399)/testdb?charset=utf8mb4&parseTime=True&timeout=100ms"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mysql:")
}

// ─── 21-22. MustNew —— panic 路径 ────────────────────────────────────────────

func TestMustNew_EmptyDSN_Panics(t *testing.T) {
	assert.Panics(t, func() {
		MustNew()
	})
}

func TestMustNew_InvalidDSN_Panics(t *testing.T) {
	assert.Panics(t, func() {
		MustNew(WithDSN("totally-invalid-dsn"))
	})
}

func TestMustNew_PanicValue_IsError(t *testing.T) {
	defer func() {
		r := recover()
		require.NotNil(t, r, "MustNew 应当 panic")
		// panic 的值可以是 error 或字符串
		switch v := r.(type) {
		case error:
			assert.Contains(t, v.Error(), "mysql:")
		case string:
			assert.Contains(t, v, "mysql:")
		default:
			t.Fatalf("unexpected panic type: %T", r)
		}
	}()
	MustNew()
}

// ─── 24. 选项组合后仍做前置校验 ───────────────────────────────────────────────

func TestNew_AllOptionsButEmptyDSN_StillErrors(t *testing.T) {
	_, err := New(
		WithMaxOpenConns(50),
		WithMaxIdleConns(5),
		WithConnMaxLifetime(30*time.Minute),
		WithConnMaxIdleTime(10*time.Minute),
		WithSlowThreshold(300*time.Millisecond),
		WithLogLevel(gormlogger.Silent),
		// 故意不设 WithDSN
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "mysql: DSN is required")
}

// ─── 20, 23. 真实 MySQL 集成测试（需要 MYSQL_DSN 环境变量）──────────────────

func TestNew_WithRealMySQL_ReturnsDB(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("MYSQL_DSN not set, skipping real MySQL integration test")
	}
	db, err := New(
		WithDSN(dsn),
		WithMaxOpenConns(5),
		WithMaxIdleConns(2),
	)
	require.NoError(t, err)
	require.NotNil(t, db)

	// 验证连接池配置
	sqlDB, err := db.DB()
	require.NoError(t, err)
	stats := sqlDB.Stats()
	assert.GreaterOrEqual(t, stats.MaxOpenConnections, 5)
	sqlDB.Close()
}

func TestMustNew_WithRealMySQL_ReturnsDB(t *testing.T) {
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skip("MYSQL_DSN not set, skipping real MySQL integration test")
	}
	assert.NotPanics(t, func() {
		db := MustNew(WithDSN(dsn))
		require.NotNil(t, db)
		sqlDB, err := db.DB()
		require.NoError(t, err)
		sqlDB.Close()
	})
}

// ─── Options 结构体零值 ───────────────────────────────────────────────────────

func TestOptions_ZeroValue_NoCrash(t *testing.T) {
	assert.NotPanics(t, func() {
		var o Options
		_ = o.DSN
		_ = o.MaxOpenConns
		_ = o.LogLevel
	})
}

// ─── LogLevel 全枚举覆盖 ──────────────────────────────────────────────────────

func TestWithLogLevel_AllLevels(t *testing.T) {
	levels := []gormlogger.LogLevel{
		gormlogger.Silent,
		gormlogger.Error,
		gormlogger.Warn,
		gormlogger.Info,
	}
	for _, lvl := range levels {
		o := defaultOptions()
		WithLogLevel(lvl)(o)
		assert.Equal(t, lvl, o.LogLevel, "LogLevel %v 应被正确设置", lvl)
	}
}

// ─── Benchmark ────────────────────────────────────────────────────────────────

func BenchmarkDefaultOptions(b *testing.B) {
	for i := 0; i < b.N; i++ {
		defaultOptions()
	}
}

func BenchmarkOptions_Apply(b *testing.B) {
	opts := []Option{
		WithDSN("root:pass@tcp(localhost:3306)/db"),
		WithMaxOpenConns(50),
		WithMaxIdleConns(5),
		WithConnMaxLifetime(time.Hour),
		WithSlowThreshold(200 * time.Millisecond),
		WithLogLevel(gormlogger.Warn),
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := defaultOptions()
		for _, opt := range opts {
			opt(o)
		}
	}
}
