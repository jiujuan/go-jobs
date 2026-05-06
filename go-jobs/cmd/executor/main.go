// Command executor starts a go-jobs executor instance.
// You can add your own job handlers in the registerHandlers() function below.
//
// Usage:
//
//	go run ./cmd/executor -config config/executor.dev.yaml
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	executor_handler "github.com/jiujuan/go-jobs/api/handler/executor"
	"github.com/jiujuan/go-jobs/api/middleware"
	"github.com/jiujuan/go-jobs/internal/executor"
	"github.com/jiujuan/go-jobs/pkg/conf"
	"github.com/jiujuan/go-jobs/pkg/logger"
	"github.com/jiujuan/go-jobs/pkg/utils"
)

func main() {
	configFile := flag.String("config", "config/executor.dev.yaml", "config file path")
	flag.Parse()

	// ── Config ──────────────────────────────────────────────────────────────
	cfg, err := conf.Load(conf.WithConfigFile(*configFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "load config: %v\n", err)
		os.Exit(1)
	}

	// ── Logger ───────────────────────────────────────────────────────────────
	logger.Init(
		logger.WithLevel(cfg.Logger.Level),
		logger.WithFilename(cfg.Logger.Filename),
		logger.WithJSON(cfg.Logger.JSON),
	)
	defer logger.Sync()

	// ── Handler registry ─────────────────────────────────────────────────────
	registry := executor.NewRegistry()
	registerHandlers(registry)

	// ── Job runner ───────────────────────────────────────────────────────────
	runner := executor.NewRunner(registry)

	// ── Auto-registrar (heartbeat to admin) ──────────────────────────────────
	port := cfg.Executor.Port
	if port == 0 {
		port = 9901
	}
	address := cfg.Executor.Address
	if address == "" {
		address = fmt.Sprintf("%s:%d", utils.LocalIP(), port)
	}

	registrar := executor.NewAutoRegistrar(
		cfg.Executor.AdminURL,
		executor.RegistrationRequest{
			AppName: cfg.Executor.AppName,
			Title:   cfg.Executor.AppName,
			Address: address,
			Version: cfg.App.Version,
		},
	)

	// ── HTTP server ───────────────────────────────────────────────────────────
	gin.SetMode(cfg.Server.Mode)
	r := gin.New()
	r.Use(middleware.Recovery(), middleware.RequestLogger())

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"status": "ok", "address": address})
	})

	internalToken := "go-jobs-internal"
	execGroup := r.Group("/executor")
	execGroup.Use(middleware.InternalAuth(internalToken))
	{
		h := executor_handler.NewExecutorHTTPHandler(runner)
		execGroup.POST("/run", h.Run)
		execGroup.POST("/kill", h.Kill)
		execGroup.POST("/beat", h.Beat)
		execGroup.POST("/idleBeat", h.IdleBeat)
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf("0.0.0.0:%d", port),
		Handler:      r,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	// ── Start registrar ───────────────────────────────────────────────────────
	if err := registrar.Start(); err != nil {
		logger.Fatal("registrar start failed", zap.Error(err))
	}

	// ── Start HTTP server ─────────────────────────────────────────────────────
	go func() {
		logger.Info("executor listening", zap.String("addr", srv.Addr), zap.String("address", address))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("executor listen failed", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("executor shutting down...")
	registrar.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
	logger.Info("executor stopped")
}

// registerHandlers is where you add your application-specific job handlers.
// Each handler must be registered under the same name used in job_info.execute_handler.
func registerHandlers(r *executor.Registry) {
	// Example: a simple demo handler
	r.Register("demoJob", func(ctx context.Context, param string) error {
		logger.Info("demoJob executed", zap.String("param", param))
		// Simulate some work.
		select {
		case <-time.After(2 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	// Example: a sharding-aware handler
	r.Register("shardingDemoJob", func(ctx context.Context, param string) error {
		jc, ok := executor.GetJobContext(ctx)
		if !ok {
			return fmt.Errorf("no job context")
		}
		logger.Info("shardingDemoJob",
			zap.Int("shardIndex", jc.ShardingIndex),
			zap.Int("shardTotal", jc.ShardingTotal),
			zap.String("param", param))
		return nil
	})
}
