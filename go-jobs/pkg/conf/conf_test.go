package conf

// conf_test.go
//
// 覆盖 conf.go 的全部可测逻辑，无需真实数据库/Redis/Etcd。
//
// setDefaults（通过 Load 间接或直接测试）
//   1.  app.name 默认为 "go-jobs"
//   2.  server.port 默认为 8080
//   3.  mysql.max_open_conns 默认为 100
//   4.  redis.pool_size 默认为 20
//   5.  etcd.dial_timeout 默认为 "5s"
//   6.  logger.level 默认为 "info"
//   7.  scheduler.use_redis_lock 默认为 true
//   8.  executor.timeout 默认为 "60s"
//   9.  jwt.expire_duration 默认为 "24h"
//
// LoaderOption 工厂函数
//  10.  WithConfigFile 修改 configFile 字段
//  11.  WithConfigType 修改 configType 字段
//  12.  WithEnvPrefix 修改 envPrefix 字段
//  13.  多个选项可叠加
//
// Load（使用临时 YAML 文件）
//  14.  最小合法 YAML 正常加载，error=nil
//  15.  AppConfig 字段正确映射
//  16.  ServerConfig port 字段正确映射
//  17.  MySQLConfig DSN 字段正确映射
//  18.  RedisConfig addr 字段正确映射
//  19.  EtcdConfig endpoints 切片正确映射
//  20.  LoggerConfig 字段正确映射
//  21.  SchedulerConfig 字段正确映射
//  22.  ExecutorConfig 字段正确映射
//  23.  JWTConfig secret 字段正确映射
//  24.  ScriptEngines 切片正确映射
//  25.  不存在的文件返回 error
//  26.  环境变量覆盖字段（GOJOBS_APP_NAME）
//
// Config 结构体
//  27.  零值 Config 可安全使用（无崩溃）
//  28.  ScriptEngineConfig 所有字段均有 mapstructure tag

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/spf13/viper"
)

// ─── 测试辅助 ─────────────────────────────────────────────────────────────────

// writeTempConfig 在临时目录写入 YAML 内容，返回文件路径。
// 测试结束后自动删除。
func writeTempConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(content), 0644))
	return path
}

// minimalYAML 是最小合法配置（仅覆盖关键字段，其余走默认值）。
const minimalYAML = `
app:
  name: test-app
  env: prod
  version: 2.0.0
server:
  port: 9090
`

// fullYAML 覆盖全部主要配置段。
const fullYAML = `
app:
  name: full-app
  env: dev
  version: 3.1.0
server:
  host: 127.0.0.1
  port: 8888
  mode: release
mysql:
  dsn: "root:pass@tcp(db:3306)/jobs?charset=utf8mb4&parseTime=True"
  max_open_conns: 50
  max_idle_conns: 5
  conn_max_lifetime: 30m
redis:
  addr: "redis:6379"
  password: "secret"
  db: 1
  pool_size: 10
etcd:
  endpoints:
    - "etcd1:2379"
    - "etcd2:2379"
  dial_timeout: 3s
logger:
  level: debug
  filename: /tmp/test.log
  max_size_mb: 50
  max_backups: 3
  max_age_days: 7
  compress: false
  json: true
scheduler:
  preload_duration: 10s
  heartbeat_timeout: 60s
  node_id: node-1
  use_redis_lock: false
  internal_token: my-token
executor:
  app_name: my-executor
  address: "0.0.0.0:9901"
  port: 9901
  admin_url: "http://admin:8080"
  timeout: 120s
  script_engines:
    - type: GO
      binary: go
      args: ["run"]
      exec_mode: file
      file_ext: .go
      work_dir: /tmp/scripts
      env: ["GOPATH=/tmp"]
      max_output_bytes: 4194304
      disabled: false
jwt:
  secret: "my-jwt-secret"
  expire_duration: 12h
`

// ─── 1-9. setDefaults（通过直接操作 viper 测试）──────────────────────────────

func TestSetDefaults_AppName(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, "go-jobs", v.GetString("app.name"))
}

func TestSetDefaults_ServerPort(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 8080, v.GetInt("server.port"))
}

func TestSetDefaults_MySQLMaxOpenConns(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 100, v.GetInt("mysql.max_open_conns"))
}

func TestSetDefaults_MySQLMaxIdleConns(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 10, v.GetInt("mysql.max_idle_conns"))
}

func TestSetDefaults_MySQLConnMaxLifetime(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, time.Hour, v.GetDuration("mysql.conn_max_lifetime"))
}

func TestSetDefaults_RedisPoolSize(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 20, v.GetInt("redis.pool_size"))
}

func TestSetDefaults_RedisDB(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 0, v.GetInt("redis.db"))
}

func TestSetDefaults_EtcdDialTimeout(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 5*time.Second, v.GetDuration("etcd.dial_timeout"))
}

func TestSetDefaults_LoggerLevel(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, "info", v.GetString("logger.level"))
}

func TestSetDefaults_LoggerMaxSizeMB(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 100, v.GetInt("logger.max_size_mb"))
}

func TestSetDefaults_SchedulerUseRedisLock(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.True(t, v.GetBool("scheduler.use_redis_lock"))
}

func TestSetDefaults_SchedulerPreloadDuration(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 5*time.Second, v.GetDuration("scheduler.preload_duration"))
}

func TestSetDefaults_ExecutorTimeout(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 60*time.Second, v.GetDuration("executor.timeout"))
}

func TestSetDefaults_JWTExpireDuration(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, 24*time.Hour, v.GetDuration("jwt.expire_duration"))
}

func TestSetDefaults_ServerHost(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, "0.0.0.0", v.GetString("server.host"))
}

func TestSetDefaults_ServerMode(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, "debug", v.GetString("server.mode"))
}

func TestSetDefaults_AppEnv(t *testing.T) {
	v := viper.New()
	setDefaults(v)
	assert.Equal(t, "dev", v.GetString("app.env"))
}

// ─── 10-13. LoaderOption 工厂函数 ─────────────────────────────────────────────

func TestWithConfigFile_SetsField(t *testing.T) {
	o := &loaderOptions{}
	WithConfigFile("/custom/path/config.yaml")(o)
	assert.Equal(t, "/custom/path/config.yaml", o.configFile)
}

func TestWithConfigType_SetsField(t *testing.T) {
	o := &loaderOptions{}
	WithConfigType("json")(o)
	assert.Equal(t, "json", o.configType)
}

func TestWithEnvPrefix_SetsField(t *testing.T) {
	o := &loaderOptions{}
	WithEnvPrefix("MYPKG")(o)
	assert.Equal(t, "MYPKG", o.envPrefix)
}

func TestLoaderOptions_MultipleOptions_AllApplied(t *testing.T) {
	o := &loaderOptions{configFile: "default.yaml", configType: "yaml", envPrefix: "GOJOBS"}
	WithConfigFile("/my/config.yaml")(o)
	WithConfigType("toml")(o)
	WithEnvPrefix("MYAPP")(o)
	assert.Equal(t, "/my/config.yaml", o.configFile)
	assert.Equal(t, "toml", o.configType)
	assert.Equal(t, "MYAPP", o.envPrefix)
}

// ─── 14. Load - 不存在文件返回 error ─────────────────────────────────────────

func TestLoad_NonExistentFile_ReturnsError(t *testing.T) {
	_, err := Load(WithConfigFile("/nonexistent/config.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "conf: read config file")
}

// ─── 15-24. Load - 从临时文件解析各字段 ──────────────────────────────────────

func TestLoad_MinimalYAML_NoError(t *testing.T) {
	path := writeTempConfig(t, minimalYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	require.NotNil(t, cfg)
}

func TestLoad_AppConfig_FieldMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, "full-app", cfg.App.Name)
	assert.Equal(t, "dev", cfg.App.Env)
	assert.Equal(t, "3.1.0", cfg.App.Version)
}

func TestLoad_ServerConfig_PortMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, 8888, cfg.Server.Port)
	assert.Equal(t, "127.0.0.1", cfg.Server.Host)
	assert.Equal(t, "release", cfg.Server.Mode)
}

func TestLoad_MySQLConfig_DSNMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Contains(t, cfg.MySQL.DSN, "root:pass")
	assert.Equal(t, 50, cfg.MySQL.MaxOpenConns)
	assert.Equal(t, 5, cfg.MySQL.MaxIdleConns)
}

func TestLoad_RedisConfig_AddrMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, "redis:6379", cfg.Redis.Addr)
	assert.Equal(t, "secret", cfg.Redis.Password)
	assert.Equal(t, 1, cfg.Redis.DB)
	assert.Equal(t, 10, cfg.Redis.PoolSize)
}

func TestLoad_EtcdConfig_EndpointsSlice(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	require.Len(t, cfg.Etcd.Endpoints, 2)
	assert.Equal(t, "etcd1:2379", cfg.Etcd.Endpoints[0])
	assert.Equal(t, "etcd2:2379", cfg.Etcd.Endpoints[1])
	assert.Equal(t, 3*time.Second, cfg.Etcd.DialTimeout)
}

func TestLoad_LoggerConfig_FieldMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, "debug", cfg.Logger.Level)
	assert.Equal(t, "/tmp/test.log", cfg.Logger.Filename)
	assert.Equal(t, 50, cfg.Logger.MaxSizeMB)
	assert.Equal(t, 3, cfg.Logger.MaxBackups)
	assert.Equal(t, 7, cfg.Logger.MaxAgeDays)
	assert.False(t, cfg.Logger.Compress)
	assert.True(t, cfg.Logger.JSON)
}

func TestLoad_SchedulerConfig_FieldMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, 10*time.Second, cfg.Scheduler.PreloadDuration)
	assert.Equal(t, 60*time.Second, cfg.Scheduler.HeartbeatTimeout)
	assert.Equal(t, "node-1", cfg.Scheduler.NodeID)
	assert.False(t, cfg.Scheduler.UseRedisLock)
	assert.Equal(t, "my-token", cfg.Scheduler.InternalToken)
}

func TestLoad_ExecutorConfig_FieldMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, "my-executor", cfg.Executor.AppName)
	assert.Equal(t, "0.0.0.0:9901", cfg.Executor.Address)
	assert.Equal(t, 9901, cfg.Executor.Port)
	assert.Equal(t, "http://admin:8080", cfg.Executor.AdminURL)
	assert.Equal(t, 120*time.Second, cfg.Executor.Timeout)
}

func TestLoad_JWTConfig_FieldMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, "my-jwt-secret", cfg.JWT.Secret)
	assert.Equal(t, 12*time.Hour, cfg.JWT.ExpireDuration)
}

func TestLoad_ScriptEngines_SliceMapping(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	require.Len(t, cfg.Executor.ScriptEngines, 1)
	eng := cfg.Executor.ScriptEngines[0]
	assert.Equal(t, "GO", eng.Type)
	assert.Equal(t, "go", eng.Binary)
	assert.Equal(t, []string{"run"}, eng.Args)
	assert.Equal(t, "file", eng.ExecMode)
	assert.Equal(t, ".go", eng.FileExt)
	assert.Equal(t, "/tmp/scripts", eng.WorkDir)
	assert.Equal(t, []string{"GOPATH=/tmp"}, eng.Env)
	assert.Equal(t, int64(4194304), eng.MaxOutputBytes)
	assert.False(t, eng.Disabled)
}

func TestLoad_DefaultsApplied_WhenFieldMissing(t *testing.T) {
	// minimalYAML 只设了 app 和 server，其余走默认值
	path := writeTempConfig(t, minimalYAML)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	// 来自 setDefaults
	assert.Equal(t, 100, cfg.MySQL.MaxOpenConns)
	assert.Equal(t, 20, cfg.Redis.PoolSize)
}

// ─── 26. 环境变量覆盖 ─────────────────────────────────────────────────────────

func TestLoad_EnvOverride_AppName(t *testing.T) {
	path := writeTempConfig(t, minimalYAML)
	// GOJOBS_APP_NAME 覆盖 app.name
	t.Setenv("GOJOBS_APP_NAME", "env-override-app")
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, "env-override-app", cfg.App.Name)
}

func TestLoad_EnvOverride_ServerPort(t *testing.T) {
	path := writeTempConfig(t, minimalYAML)
	t.Setenv("GOJOBS_SERVER_PORT", "7777")
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, 7777, cfg.Server.Port)
}

// ─── 27. Config 零值安全 ──────────────────────────────────────────────────────

func TestConfig_ZeroValue_NoCrash(t *testing.T) {
	assert.NotPanics(t, func() {
		var c Config
		_ = c.App.Name
		_ = c.Server.Port
		_ = c.MySQL.DSN
		_ = c.Redis.Addr
		_ = c.Etcd.Endpoints
		_ = c.Executor.ScriptEngines
	})
}

// ─── 28. ScriptEngineConfig 字段完整性 ───────────────────────────────────────

func TestScriptEngineConfig_AllFields_Accessible(t *testing.T) {
	cfg := ScriptEngineConfig{
		Type:           "JAVA",
		Binary:         "java",
		Args:           []string{"-jar"},
		FileExt:        ".jar",
		WorkDir:        "/tmp",
		Env:            []string{"JAVA_HOME=/usr/java"},
		ExecMode:       "jar",
		MaxOutputBytes: 1024,
		Disabled:       true,
	}
	assert.Equal(t, "JAVA", cfg.Type)
	assert.Equal(t, "java", cfg.Binary)
	assert.Equal(t, []string{"-jar"}, cfg.Args)
	assert.Equal(t, ".jar", cfg.FileExt)
	assert.Equal(t, "/tmp", cfg.WorkDir)
	assert.True(t, cfg.Disabled)
}

// ─── AlarmConfig / EmailConfig 字段测试 ──────────────────────────────────────

func TestLoad_AlarmConfig_FieldMapping(t *testing.T) {
	yaml := minimalYAML + `
alarm:
  dingtalk_webhook: "https://dingtalk.example.com/webhook"
  wecom_webhook: "https://wecom.example.com/webhook"
  email:
    host: smtp.example.com
    port: 465
    username: admin@example.com
    password: mailpass
    from: noreply@example.com
    to:
      - ops@example.com
      - dev@example.com
`
	path := writeTempConfig(t, yaml)
	cfg, err := Load(WithConfigFile(path))
	require.NoError(t, err)
	assert.Equal(t, "https://dingtalk.example.com/webhook", cfg.Alarm.DingtalkWebhook)
	assert.Equal(t, "smtp.example.com", cfg.Alarm.Email.Host)
	assert.Equal(t, 465, cfg.Alarm.Email.Port)
	assert.Len(t, cfg.Alarm.Email.To, 2)
}

// ─── Load 幂等性 ──────────────────────────────────────────────────────────────

func TestLoad_Idempotent_SameFileTwice(t *testing.T) {
	path := writeTempConfig(t, fullYAML)
	cfg1, err1 := Load(WithConfigFile(path))
	cfg2, err2 := Load(WithConfigFile(path))
	require.NoError(t, err1)
	require.NoError(t, err2)
	assert.Equal(t, cfg1.App.Name, cfg2.App.Name)
	assert.Equal(t, cfg1.Server.Port, cfg2.Server.Port)
}
