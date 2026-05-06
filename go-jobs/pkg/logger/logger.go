// Package logger provides a production-ready structured logger based on zap
// with log rotation via lumberjack. It uses the functional options pattern
// so callers can customise behaviour without breaking existing code.
package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinish/lumberjack.v2"
)

// global logger instance (package-level for convenience helpers).
var global *zap.Logger

// Options holds configuration for the logger.
type Options struct {
	Level      string // debug | info | warn | error
	Filename   string // log file path, empty = stdout only
	MaxSizeMB  int    // max size per file in MB
	MaxBackups int    // max rotated files to keep
	MaxAgeDays int    // max age in days for old logs
	Compress   bool   // compress rotated files
	Caller     bool   // include caller info
	JSON       bool   // json format (default: console in dev)
}

// Option is a functional option for Options.
type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		Level:      "info",
		MaxSizeMB:  100,
		MaxBackups: 7,
		MaxAgeDays: 30,
		Compress:   true,
		Caller:     true,
		JSON:       false,
	}
}

// WithLevel sets the minimum log level.
func WithLevel(level string) Option { return func(o *Options) { o.Level = level } }

// WithFilename sets the log file path.
func WithFilename(f string) Option { return func(o *Options) { o.Filename = f } }

// WithMaxSizeMB sets the maximum log file size in MB before rotation.
func WithMaxSizeMB(mb int) Option { return func(o *Options) { o.MaxSizeMB = mb } }

// WithMaxBackups sets how many rotated log files to retain.
func WithMaxBackups(n int) Option { return func(o *Options) { o.MaxBackups = n } }

// WithMaxAgeDays sets how many days to keep rotated log files.
func WithMaxAgeDays(d int) Option { return func(o *Options) { o.MaxAgeDays = d } }

// WithCompress enables/disables gzip compression of rotated files.
func WithCompress(c bool) Option { return func(o *Options) { o.Compress = c } }

// WithCaller enables/disables caller information in log entries.
func WithCaller(c bool) Option { return func(o *Options) { o.Caller = c } }

// WithJSON enables JSON log format (useful for production / log aggregation).
func WithJSON(j bool) Option { return func(o *Options) { o.JSON = j } }

// Init initialises the global logger.  Call this once at startup.
func Init(opts ...Option) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}
	global = build(o)
	zap.ReplaceGlobals(global)
}

// build constructs a *zap.Logger from the given options.
func build(o *Options) *zap.Logger {
	level := parseLevel(o.Level)
	encoderCfg := productionEncoderConfig()

	var enc zapcore.Encoder
	if o.JSON {
		enc = zapcore.NewJSONEncoder(encoderCfg)
	} else {
		enc = zapcore.NewConsoleEncoder(encoderCfg)
	}

	// Always write to stdout.
	writers := []zapcore.WriteSyncer{zapcore.AddSync(os.Stdout)}

	// Optionally write to a rotating file.
	if o.Filename != "" {
		rotator := &lumberjack.Logger{
			Filename:   o.Filename,
			MaxSize:    o.MaxSizeMB,
			MaxBackups: o.MaxBackups,
			MaxAge:     o.MaxAgeDays,
			Compress:   o.Compress,
		}
		writers = append(writers, zapcore.AddSync(rotator))
	}

	core := zapcore.NewCore(enc, zapcore.NewMultiWriteSyncer(writers...), zap.NewAtomicLevelAt(level))

	zapOpts := []zap.Option{zap.AddStacktrace(zapcore.ErrorLevel)}
	if o.Caller {
		zapOpts = append(zapOpts, zap.AddCaller(), zap.AddCallerSkip(1))
	}

	return zap.New(core, zapOpts...)
}

func productionEncoderConfig() zapcore.EncoderConfig {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	return cfg
}

func parseLevel(l string) zapcore.Level {
	switch l {
	case "debug":
		return zapcore.DebugLevel
	case "warn":
		return zapcore.WarnLevel
	case "error":
		return zapcore.ErrorLevel
	default:
		return zapcore.InfoLevel
	}
}

// ─── Package-level convenience helpers ────────────────────────────────────────

// Logger returns the global zap.Logger (initialised after Init is called).
func Logger() *zap.Logger {
	if global == nil {
		// Fallback to a sensible default so callers don't panic before Init.
		global, _ = zap.NewDevelopment()
	}
	return global
}

func Debug(msg string, fields ...zap.Field)  { Logger().Debug(msg, fields...) }
func Info(msg string, fields ...zap.Field)   { Logger().Info(msg, fields...) }
func Warn(msg string, fields ...zap.Field)   { Logger().Warn(msg, fields...) }
func Error(msg string, fields ...zap.Field)  { Logger().Error(msg, fields...) }
func Fatal(msg string, fields ...zap.Field)  { Logger().Fatal(msg, fields...) }

// With returns a child logger with the given fields pre-attached.
func With(fields ...zap.Field) *zap.Logger { return Logger().With(fields...) }

// Sync flushes any buffered log entries.  Call this before process exit.
func Sync() { _ = Logger().Sync() }
