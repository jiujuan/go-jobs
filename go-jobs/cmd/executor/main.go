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

	// ── Handler registry（BEAN 模式） ─────────────────────────────────────────
	registry := executor.NewRegistry()
	registerHandlers(registry)

	// ── Script engine registry ────────────────────────────────────────────────
	engineRegistry := executor.NewScriptEngineRegistry()
	if len(cfg.Executor.ScriptEngines) > 0 {
		engineConfigs := confToEngineConfigs(cfg.Executor.ScriptEngines)
		if err := engineRegistry.LoadFromConfig(engineConfigs); err != nil {
			logger.Warn("executor: some script engines failed to register", zap.Error(err))
		}
	}

	// ── Job runner ───────────────────────────────────────────────────────────
	// WithGracefulTimeout 设置优雅关闭时最多等待存量任务完成的时间。
	// 超出后强制 cancel 所有剩余任务再退出。
	runner := executor.NewRunner(registry,
		executor.WithScriptEngineRegistry(engineRegistry),
		executor.WithGracefulTimeout(30*time.Second),
	)

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
		c.JSON(200, gin.H{
			"status":         "ok",
			"address":        address,
			"script_engines": engineRegistry.Types(),
			"draining":       runner.IsStopped(),
		})
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
		logger.Info("executor listening",
			zap.String("addr", srv.Addr),
			zap.String("address", address),
			zap.Strings("script_engines", engineRegistry.Types()),
		)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("executor listen failed", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	// 关闭顺序（严格有序）：
	//  1. 收到 SIGINT/SIGTERM
	//  2. 先从调度器注销：停止心跳，让调度器不再向本实例分配新任务
	//  3. 关闭 HTTP server：停止接受来自调度器的新 /run 请求
	//  4. runner.Stop()：两阶段等待存量任务结束（最多 30s）再强制 cancel
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("executor: received shutdown signal")

	// Step 1: 注销，让调度器停止向本实例分配新任务
	registrar.Stop()
	logger.Info("executor: deregistered from scheduler")

	// Step 2: 关闭 HTTP server，不再接受新的 /run 触发
	httpCtx, httpCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer httpCancel()
	if err := srv.Shutdown(httpCtx); err != nil {
		logger.Warn("executor: HTTP server shutdown error", zap.Error(err))
	}
	logger.Info("executor: HTTP server closed")

	// Step 3: 两阶段优雅关闭 Runner
	//   - 阶段一：标记 stopped，Run() 立即拒绝新请求
	//   - 阶段二：等待存量任务自然结束（上限 30s），超时则强制 cancel
	logger.Info("executor: draining in-flight jobs (max 30s)...")
	runner.Stop()

	logger.Info("executor: shutdown complete")
}

// confToEngineConfigs 将 pkg/conf 层的配置转换为 executor 层的 EngineConfig。
func confToEngineConfigs(cfgList []conf.ScriptEngineConfig) []executor.EngineConfig {
	result := make([]executor.EngineConfig, 0, len(cfgList))
	for _, c := range cfgList {
		result = append(result, executor.EngineConfig{
			Type:           c.Type,
			Binary:         c.Binary,
			Args:           c.Args,
			FileExt:        c.FileExt,
			WorkDir:        c.WorkDir,
			Env:            c.Env,
			ExecMode:       c.ExecMode,
			MaxOutputBytes: c.MaxOutputBytes,
			Disabled:       c.Disabled,
		})
	}
	return result
}

// registerHandlers 注册业务 BEAN Handler。
func registerHandlers(r *executor.Registry) {
	r.Register("demoJob", func(ctx context.Context, param string) error {
		logger.Info("demoJob executed", zap.String("param", param))
		select {
		case <-time.After(2 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

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
