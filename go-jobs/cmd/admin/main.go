// Command admin starts the go-jobs scheduler centre and admin HTTP API.
//
// Usage:
//
//	go run ./cmd/admin -config config/config.yaml
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

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	gormlogger "gorm.io/gorm/logger"

	adminhandler "github.com/jiujuan/go-jobs/api/handler/admin"
	"github.com/jiujuan/go-jobs/api/middleware"
	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/scheduler"
	"github.com/jiujuan/go-jobs/internal/service"
	"github.com/jiujuan/go-jobs/pkg/conf"
	applogger "github.com/jiujuan/go-jobs/pkg/logger"
	pkgmysql "github.com/jiujuan/go-jobs/pkg/mysql"
	pkgredis "github.com/jiujuan/go-jobs/pkg/redis"
	"github.com/jiujuan/go-jobs/pkg/utils"
)

func main() {
	configFile := flag.String("config", "config/config.yaml", "config file path")
	flag.Parse()

	// ── Load configuration ──────────────────────────────────────────────────
	cfg, err := conf.Load(conf.WithConfigFile(*configFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	// ── Initialise logger ───────────────────────────────────────────────────
	applogger.Init(
		applogger.WithLevel(cfg.Logger.Level),
		applogger.WithFilename(cfg.Logger.Filename),
		applogger.WithJSON(cfg.Logger.JSON),
		applogger.WithMaxSizeMB(cfg.Logger.MaxSizeMB),
		applogger.WithCompress(cfg.Logger.Compress),
	)
	defer applogger.Sync()

	applogger.Info("go-jobs admin starting",
		zap.String("version", cfg.App.Version),
		zap.String("env", cfg.App.Env))

	// ── MySQL ───────────────────────────────────────────────────────────────
	var gormLogLevel gormlogger.LogLevel
	switch cfg.MySQL.LogLevel {
	case "info":
		gormLogLevel = gormlogger.Info
	case "error":
		gormLogLevel = gormlogger.Error
	default:
		gormLogLevel = gormlogger.Warn
	}
	db := pkgmysql.MustNew(
		pkgmysql.WithDSN(cfg.MySQL.DSN),
		pkgmysql.WithMaxOpenConns(cfg.MySQL.MaxOpenConns),
		pkgmysql.WithMaxIdleConns(cfg.MySQL.MaxIdleConns),
		pkgmysql.WithConnMaxLifetime(cfg.MySQL.ConnMaxLifetime),
		pkgmysql.WithSlowThreshold(cfg.MySQL.SlowThreshold),
		pkgmysql.WithLogLevel(gormLogLevel),
	)
	applogger.Info("mysql connected")

	// ── Redis ────────────────────────────────────────────────────────────────
	rdb := pkgredis.MustNew(
		pkgredis.WithAddr(cfg.Redis.Addr),
		pkgredis.WithPassword(cfg.Redis.Password),
		pkgredis.WithDB(cfg.Redis.DB),
		pkgredis.WithPoolSize(cfg.Redis.PoolSize),
	)
	applogger.Info("redis connected")

	// ── DAOs ─────────────────────────────────────────────────────────────────
	jobDAO := dao.NewJobInfoDAO(db)
	logDAO := dao.NewJobLogDAO(db)
	executorDAO := dao.NewExecutorDAO(db)
	userDAO := dao.NewUserDAO(db)

	// ── Scheduler ─────────────────────────────────────────────────────────
	nodeID := cfg.Scheduler.NodeID
	if nodeID == "" {
		nodeID = utils.NodeID(cfg.Server.Port)
	}
	sched := scheduler.New(jobDAO, logDAO, executorDAO, rdb,
		scheduler.WithPreloadWindow(cfg.Scheduler.PreloadDuration),
		scheduler.WithNodeID(nodeID),
	)

	// ── Services ──────────────────────────────────────────────────────────
	jobSvc := service.NewJobService(jobDAO, logDAO, executorDAO, sched)
	userSvc := service.NewUserService(userDAO, cfg.JWT.Secret, cfg.JWT.ExpireDuration)
	execSvc := service.NewExecutorService(executorDAO, cfg.Scheduler.HeartbeatTimeout)

	// ── HTTP Server ────────────────────────────────────────────────────────
	gin.SetMode(cfg.Server.Mode)
	r := gin.New()
	r.Use(
		middleware.Recovery(),
		middleware.RequestLogger(),
		cors.New(cors.Config{
			AllowOrigins:     []string{"*"},
			AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowHeaders:     []string{"*"},
			AllowCredentials: true,
		}),
	)

	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok", "node": nodeID}) })

	// Public
	r.POST("/api/login", adminhandler.NewUserHandler(userSvc).Login)

	// Internal (executor → scheduler)
	internalToken := cfg.Scheduler.InternalToken
	if internalToken == "" {
		internalToken = "go-jobs-internal"
	}
	internal := r.Group("/api/executor")
	internal.Use(middleware.InternalAuth(internalToken))
	internalHandler := adminhandler.NewInternalExecutorHandler(execSvc, jobSvc)
	internal.POST("/register", internalHandler.Register)
	internal.POST("/heartbeat", internalHandler.Heartbeat)
	internal.POST("/deregister", internalHandler.Deregister)

	// Admin (JWT-protected)
	api := r.Group("/api")
	api.Use(middleware.JWTAuth(userSvc))
	{
		userH := adminhandler.NewUserHandler(userSvc)
		api.GET("/user/me", userH.GetCurrentUser)

		jobH := adminhandler.NewJobHandler(jobSvc)
		logH := adminhandler.NewLogHandler(jobSvc)
		execH := adminhandler.NewExecutorHandler(execSvc)

		jobs := api.Group("/jobs")
		jobs.POST("", jobH.CreateJob)
		jobs.GET("", jobH.ListJobs)
		jobs.GET("/:id", jobH.GetJob)
		jobs.PUT("/:id", jobH.UpdateJob)
		jobs.DELETE("/:id", jobH.DeleteJob)
		jobs.POST("/:id/start", jobH.StartJob)
		jobs.POST("/:id/stop", jobH.StopJob)
		jobs.POST("/:id/trigger", jobH.TriggerJob)
		jobs.GET("/:id/logs", logH.ListJobLogs)

		logs := api.Group("/logs")
		logs.GET("/:logID/detail", logH.GetLogDetail)
		logs.POST("/:logID/kill", jobH.KillJob)

		executors := api.Group("/executors")
		executors.GET("", execH.ListExecutors)
	}

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.Server.Host, cfg.Server.Port),
		Handler:      r,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
	}

	// ── Start scheduler ─────────────────────────────────────────────────────
	if err := sched.Start(); err != nil {
		applogger.Fatal("start scheduler failed", zap.Error(err))
	}

	// ── Start sweeper (mark stale executors offline) ────────────────────────
	sweepTicker := time.NewTicker(30 * time.Second)
	go func() {
		for range sweepTicker.C {
			execSvc.SweepOfflineExecutors(context.Background())
		}
	}()

	// ── Start HTTP server (non-blocking) ────────────────────────────────────
	go func() {
		applogger.Info("http server listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			applogger.Fatal("listen failed", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ───────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	applogger.Info("shutting down...")
	sweepTicker.Stop()
	sched.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		applogger.Error("server shutdown error", zap.Error(err))
	}
	applogger.Info("go-jobs admin stopped")
}
