// Package mysql provides a GORM-based MySQL client factory with
// connection-pool management and slow-query logging.
package mysql

import (
	"fmt"
	"log"
	"os"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Options configures the MySQL client.
type Options struct {
	DSN             string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
	SlowThreshold   time.Duration
	LogLevel        logger.LogLevel
}

// Option is a functional option for Options.
type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		MaxOpenConns:    100,
		MaxIdleConns:    10,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
		SlowThreshold:   200 * time.Millisecond,
		LogLevel:        logger.Warn,
	}
}

// WithDSN sets the MySQL DSN.
func WithDSN(dsn string) Option { return func(o *Options) { o.DSN = dsn } }

// WithMaxOpenConns sets the maximum open connections in the pool.
func WithMaxOpenConns(n int) Option { return func(o *Options) { o.MaxOpenConns = n } }

// WithMaxIdleConns sets the maximum idle connections in the pool.
func WithMaxIdleConns(n int) Option { return func(o *Options) { o.MaxIdleConns = n } }

// WithConnMaxLifetime sets how long a connection may be reused.
func WithConnMaxLifetime(d time.Duration) Option {
	return func(o *Options) { o.ConnMaxLifetime = d }
}

// WithConnMaxIdleTime sets the maximum time a connection may be idle.
func WithConnMaxIdleTime(d time.Duration) Option {
	return func(o *Options) { o.ConnMaxIdleTime = d }
}

// WithSlowThreshold configures the slow-query threshold for logging.
func WithSlowThreshold(d time.Duration) Option {
	return func(o *Options) { o.SlowThreshold = d }
}

// WithLogLevel sets the GORM log level (Silent|Error|Warn|Info).
func WithLogLevel(l logger.LogLevel) Option { return func(o *Options) { o.LogLevel = l } }

// New opens a GORM *DB with the given options applied.
// The returned *gorm.DB is safe for concurrent use.
func New(opts ...Option) (*gorm.DB, error) {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if o.DSN == "" {
		return nil, fmt.Errorf("mysql: DSN is required")
	}

	gormLogger := logger.New(
		log.New(os.Stdout, "\r\n", log.LstdFlags),
		logger.Config{
			SlowThreshold:             o.SlowThreshold,
			LogLevel:                  o.LogLevel,
			IgnoreRecordNotFoundError: true,
			Colorful:                  true,
		},
	)

	db, err := gorm.Open(mysql.Open(o.DSN), &gorm.Config{
		Logger:                                   gormLogger,
		PrepareStmt:                              true,
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		return nil, fmt.Errorf("mysql: open connection: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("mysql: get underlying sql.DB: %w", err)
	}
	sqlDB.SetMaxOpenConns(o.MaxOpenConns)
	sqlDB.SetMaxIdleConns(o.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(o.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(o.ConnMaxIdleTime)

	return db, nil
}

// MustNew is like New but panics on error.  Intended for use in main().
func MustNew(opts ...Option) *gorm.DB {
	db, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return db
}
