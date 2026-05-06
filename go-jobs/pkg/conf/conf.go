// Package conf handles configuration loading for go-jobs using Viper.
// It supports YAML config files, environment variable overrides, and
// multiple environments (dev / prod).
package conf

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config is the root configuration structure.
type Config struct {
	Alarm    AlarmConfig    `mapstructure:"alarm"`
	App      AppConfig      `mapstructure:"app"`
	Server   ServerConfig   `mapstructure:"server"`
	MySQL    MySQLConfig    `mapstructure:"mysql"`
	Redis    RedisConfig    `mapstructure:"redis"`
	Etcd     EtcdConfig     `mapstructure:"etcd"`
	ES       ESConfig       `mapstructure:"es"`
	Logger   LoggerConfig   `mapstructure:"logger"`
	Scheduler SchedulerConfig `mapstructure:"scheduler"`
	Executor ExecutorConfig  `mapstructure:"executor"`
	JWT      JWTConfig      `mapstructure:"jwt"`
}

type AppConfig struct {
	Name    string `mapstructure:"name"`
	Env     string `mapstructure:"env"`   // dev | prod
	Version string `mapstructure:"version"`
}

type ServerConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
	Mode         string        `mapstructure:"mode"` // debug | release
}

type MySQLConfig struct {
	DSN             string        `mapstructure:"dsn"`
	MaxOpenConns    int           `mapstructure:"max_open_conns"`
	MaxIdleConns    int           `mapstructure:"max_idle_conns"`
	ConnMaxLifetime time.Duration `mapstructure:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `mapstructure:"conn_max_idle_time"`
	SlowThreshold   time.Duration `mapstructure:"slow_threshold"`
	LogLevel        string        `mapstructure:"log_level"`
}

type RedisConfig struct {
	Addr         string        `mapstructure:"addr"`
	Password     string        `mapstructure:"password"`
	DB           int           `mapstructure:"db"`
	PoolSize     int           `mapstructure:"pool_size"`
	MinIdleConns int           `mapstructure:"min_idle_conns"`
	DialTimeout  time.Duration `mapstructure:"dial_timeout"`
	ReadTimeout  time.Duration `mapstructure:"read_timeout"`
	WriteTimeout time.Duration `mapstructure:"write_timeout"`
}

type EtcdConfig struct {
	Endpoints   []string      `mapstructure:"endpoints"`
	DialTimeout time.Duration `mapstructure:"dial_timeout"`
	Username    string        `mapstructure:"username"`
	Password    string        `mapstructure:"password"`
}

type ESConfig struct {
	Addresses []string `mapstructure:"addresses"`
	Username  string   `mapstructure:"username"`
	Password  string   `mapstructure:"password"`
	Index     string   `mapstructure:"index"`
	Enabled   bool     `mapstructure:"enabled"`
}

type LoggerConfig struct {
	Level      string `mapstructure:"level"`
	Filename   string `mapstructure:"filename"`
	MaxSizeMB  int    `mapstructure:"max_size_mb"`
	MaxBackups int    `mapstructure:"max_backups"`
	MaxAgeDays int    `mapstructure:"max_age_days"`
	Compress   bool   `mapstructure:"compress"`
	JSON       bool   `mapstructure:"json"`
}

type SchedulerConfig struct {
	// How far ahead to pre-load triggers (default: 5s)
	PreloadDuration time.Duration `mapstructure:"preload_duration"`
	// Heartbeat interval for executor health check
	HeartbeatTimeout time.Duration `mapstructure:"heartbeat_timeout"`
	// Node identifier for distributed lock (auto-set to ip:port if empty)
	NodeID string `mapstructure:"node_id"`
	// Whether to enable Redis-based distributed lock (vs DB lock)
	UseRedisLock bool `mapstructure:"use_redis_lock"`
	// InternalToken is the shared secret for executor → scheduler API calls.
	// Defaults to "go-jobs-internal" if empty.
	InternalToken string `mapstructure:"internal_token"`
}

type ExecutorConfig struct {
	AppName  string        `mapstructure:"app_name"`
	Address  string        `mapstructure:"address"` // ip:port this executor listens on
	Port     int           `mapstructure:"port"`
	AdminURL string        `mapstructure:"admin_url"` // scheduler admin base URL
	Timeout  time.Duration `mapstructure:"timeout"`
}

type JWTConfig struct {
	Secret     string        `mapstructure:"secret"`
	ExpireDuration time.Duration `mapstructure:"expire_duration"`
}

// loaderOptions holds loader configuration.
type loaderOptions struct {
	configFile string
	configType string
	envPrefix  string
}

// LoaderOption is a functional option for the config loader.
type LoaderOption func(*loaderOptions)

// WithConfigFile sets an explicit config file path.
func WithConfigFile(path string) LoaderOption {
	return func(o *loaderOptions) { o.configFile = path }
}

// WithConfigType overrides the config file type (yaml, json, toml).
func WithConfigType(t string) LoaderOption {
	return func(o *loaderOptions) { o.configType = t }
}

// WithEnvPrefix sets the env-variable prefix (default "GOJOBS").
func WithEnvPrefix(prefix string) LoaderOption {
	return func(o *loaderOptions) { o.envPrefix = prefix }
}

// Load reads configuration from file + env variables.
func Load(opts ...LoaderOption) (*Config, error) {
	o := &loaderOptions{
		configFile: "config/config.yaml",
		configType: "yaml",
		envPrefix:  "GOJOBS",
	}
	for _, opt := range opts {
		opt(o)
	}

	v := viper.New()
	v.SetConfigFile(o.configFile)
	v.SetConfigType(o.configType)
	v.SetEnvPrefix(o.envPrefix)
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	setDefaults(v)

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("conf: read config file %q: %w", o.configFile, err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("conf: unmarshal config: %w", err)
	}

	return &cfg, nil
}

// setDefaults registers sensible default values.
func setDefaults(v *viper.Viper) {
	v.SetDefault("app.name", "go-jobs")
	v.SetDefault("app.env", "dev")
	v.SetDefault("app.version", "1.0.0")

	v.SetDefault("server.host", "0.0.0.0")
	v.SetDefault("server.port", 8080)
	v.SetDefault("server.read_timeout", "30s")
	v.SetDefault("server.write_timeout", "30s")
	v.SetDefault("server.mode", "debug")

	v.SetDefault("mysql.max_open_conns", 100)
	v.SetDefault("mysql.max_idle_conns", 10)
	v.SetDefault("mysql.conn_max_lifetime", "1h")
	v.SetDefault("mysql.conn_max_idle_time", "30m")
	v.SetDefault("mysql.slow_threshold", "200ms")
	v.SetDefault("mysql.log_level", "warn")

	v.SetDefault("redis.db", 0)
	v.SetDefault("redis.pool_size", 20)
	v.SetDefault("redis.dial_timeout", "5s")
	v.SetDefault("redis.read_timeout", "3s")
	v.SetDefault("redis.write_timeout", "3s")

	v.SetDefault("etcd.dial_timeout", "5s")

	v.SetDefault("logger.level", "info")
	v.SetDefault("logger.max_size_mb", 100)
	v.SetDefault("logger.max_backups", 7)
	v.SetDefault("logger.max_age_days", 30)
	v.SetDefault("logger.compress", true)

	v.SetDefault("scheduler.preload_duration", "5s")
	v.SetDefault("scheduler.heartbeat_timeout", "30s")
	v.SetDefault("scheduler.use_redis_lock", true)

	v.SetDefault("executor.timeout", "60s")

	v.SetDefault("jwt.expire_duration", "24h")
}

// AlarmConfig holds alarm channel configuration for v2.
type AlarmConfig struct {
	DingtalkWebhook string      `mapstructure:"dingtalk_webhook"`
	WeComWebhook    string      `mapstructure:"wecom_webhook"`
	WebhookURL      string      `mapstructure:"webhook_url"`
	Email           EmailConfig `mapstructure:"email"`
}

type EmailConfig struct {
	Host     string   `mapstructure:"host"`
	Port     int      `mapstructure:"port"`
	Username string   `mapstructure:"username"`
	Password string   `mapstructure:"password"`
	From     string   `mapstructure:"from"`
	To       []string `mapstructure:"to"`
}
