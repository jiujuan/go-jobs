// Command admin starts the go-jobs v3 scheduler centre.
// v3 新增：Etcd选主、ES日志、子任务依赖、任务分组、FAILOVER路由、资源上报、调度统计。
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
	"github.com/jiujuan/go-jobs/pkg/executorstore"
	"github.com/jiujuan/go-jobs/pkg/jobtpl"
	"github.com/jiujuan/go-jobs/pkg/logbuffer"
	"github.com/jiujuan/go-jobs/pkg/paramtpl"
	"github.com/jiujuan/go-jobs/pkg/ratelimit"
	"github.com/jiujuan/go-jobs/pkg/conf"
	espkg "github.com/jiujuan/go-jobs/pkg/es"
	etcdpkg "github.com/jiujuan/go-jobs/pkg/etcd"
	applogger "github.com/jiujuan/go-jobs/pkg/logger"
	pkgmysql "github.com/jiujuan/go-jobs/pkg/mysql"
	pkgredis "github.com/jiujuan/go-jobs/pkg/redis"
	"github.com/jiujuan/go-jobs/pkg/utils"
)

func main() {
	configFile := flag.String("config", "config/config.yaml", "config file path")
	flag.Parse()

	cfg, err := conf.Load(conf.WithConfigFile(*configFile))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to load config: %v\n", err)
		os.Exit(1)
	}

	applogger.Init(
		applogger.WithLevel(cfg.Logger.Level),
		applogger.WithFilename(cfg.Logger.Filename),
		applogger.WithJSON(cfg.Logger.JSON),
		applogger.WithMaxSizeMB(cfg.Logger.MaxSizeMB),
		applogger.WithCompress(cfg.Logger.Compress),
	)
	defer applogger.Sync()

	applogger.Info("go-jobs admin v3 starting",
		zap.String("version", cfg.App.Version),
		zap.String("env", cfg.App.Env))

	// ── MySQL ────────────────────────────────────────────────────────────────
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

	// ── ElasticSearch (optional) ─────────────────────────────────────────────
	var esClient *espkg.Client
	if cfg.ES.Enabled && len(cfg.ES.Addresses) > 0 {
		esClient, err = espkg.New(
			espkg.WithAddresses(cfg.ES.Addresses),
			espkg.WithCredentials(cfg.ES.Username, cfg.ES.Password),
			espkg.WithIndex(cfg.ES.Index),
		)
		if err != nil {
			applogger.Warn("es init failed (disabled)", zap.Error(err))
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			if err := esClient.EnsureIndex(ctx); err != nil {
				applogger.Warn("es ensure index failed", zap.Error(err))
			}
			cancel()
			applogger.Info("elasticsearch connected", zap.String("index", cfg.ES.Index))
		}
	}
	esLogSvc := service.NewESLogService(esClient, cfg.ES.Enabled && esClient != nil)

	// ── Etcd (optional, for leader election) ────────────────────────────────
	var etcdClient interface{ Close() error }

	// ── DAOs ──────────────────────────────────────────────────────────────────
	jobDAO := dao.NewJobInfoDAO(db)
	rawLogDAO := dao.NewJobLogDAO(db) // GORM-backed; also satisfies BatchCreator
	executorDAO := dao.NewExecutorDAO(db)
	userDAO := dao.NewUserDAO(db)

	// ── Buffered log DAO（批量写入缓冲区，大幅减少 DB 连接占用）─────────────
	// logbuffer.Buffer 收集 JobLog 记录，累积到 batchSize 或超过 flushInterval
	// 时批量 INSERT，单次 DB 往返写入多行，调用方语义不变（Create 返回时 ID 已有效）。
	logBuf := logbuffer.New(rawLogDAO,
		logbuffer.WithBatchSize(500),
		logbuffer.WithFlushInterval(2*time.Second),
		logbuffer.WithRingCap(2000),
	)
	logBuf.Start()
	// logDAO 是 dao.JobLogDAO 接口类型，Buffer 满足该接口
	logDAO := dao.JobLogDAO(logBuf)

	// ── Scheduler ─────────────────────────────────────────────────────────────
	nodeID := cfg.Scheduler.NodeID
	if nodeID == "" {
		nodeID = utils.NodeID(cfg.Server.Port)
	}
	// ── ExecutorStore（内存注册表 + 异步健康探测）──────────────────────────
	execStore := executorstore.New(
		func(addr string) executorstore.BeatClient { return scheduler.NewExecutorClient(addr) },
		executorstore.ZapLoggerAdapter(applogger.Logger()),
		executorstore.WithProbeInterval(10*time.Second),
		executorstore.WithProbeTimeout(2*time.Second),
		executorstore.WithHeartbeatTTL(cfg.Scheduler.HeartbeatTimeout),
	)
	execStore.Start(context.Background())

	// ── 限流注册表（任务限流与配额）──────────────────────────────────────
	// 可通过 rateLimiterReg.RegisterApp / RegisterJob 动态配置限流规则。
	rateLimiterReg := ratelimit.NewRegistry()
	// 示例：为关键 App 配置默认限流（生产环境建议从配置文件读取）
	// rateLimiterReg.RegisterApp("critical-app", ratelimit.LimiterConfig{
	//     Rate: 10, Burst: 20,
	//     QuotaWindow: 24*time.Hour, QuotaLimit: 10000,
	// })

	// ── 参数模板引擎（任务参数模板化）──────────────────────────────────────
	paramEngine := paramtpl.New(
		paramtpl.WithEnv(cfg.App.Env),
		paramtpl.WithNodeID(nodeID),
		paramtpl.WithCacheSize(1024),
	)

	// ── 任务模板注册表（任务模板化）──────────────────────────────────────
	tplRegistry := jobtpl.NewRegistry()

	sched := scheduler.New(jobDAO, logDAO, executorDAO, rdb, execStore,
		scheduler.WithPreloadWindow(cfg.Scheduler.PreloadDuration),
		scheduler.WithNodeID(nodeID),
		scheduler.WithRateLimiter(rateLimiterReg),
		scheduler.WithParamEngine(paramEngine),
	)

	// ── Services ───────────────────────────────────────────────────────────────
	jobSvc := service.NewJobService(jobDAO, logDAO, executorDAO, sched)
	tplSvc := service.NewJobTemplateService(tplRegistry, jobSvc)
	userSvc := service.NewUserService(userDAO, cfg.JWT.Secret, cfg.JWT.ExpireDuration)
	execSvc := service.NewExecutorService(executorDAO, cfg.Scheduler.HeartbeatTimeout, execStore)
	retrySvc := service.NewRetryService(jobDAO, logDAO, sched)
	statSvc := service.NewStatService(db)
	groupSvc := service.NewJobGroupService(db)
	_ = esLogSvc

	// 告警通道
	var alarmers []service.Alarmer
	if cfg.Alarm.DingtalkWebhook != "" {
		alarmers = append(alarmers, &service.DingtalkAlarmer{WebhookURL: cfg.Alarm.DingtalkWebhook})
	}
	if cfg.Alarm.WeComWebhook != "" {
		alarmers = append(alarmers, &service.WeComAlarmer{WebhookURL: cfg.Alarm.WeComWebhook})
	}
	if cfg.Alarm.WebhookURL != "" {
		alarmers = append(alarmers, &service.WebhookAlarmer{URL: cfg.Alarm.WebhookURL})
	}
	alarmSvc := service.NewAlarmService(alarmers...)
	_ = alarmSvc

	// ── HTTP Server ────────────────────────────────────────────────────────────
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

	r.GET("/health", func(c *gin.Context) {
		c.JSON(200, gin.H{
			"status":    "ok",
			"version":   "3.0",
			"node":      nodeID,
			"es_enabled": esLogSvc.Enabled(),
		})
	})

	// 统计 API
	r.GET("/api/stats/dashboard", middleware.JWTAuth(userSvc), func(c *gin.Context) {
		data, err := statSvc.GetDashboardStats(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"code": 1500, "message": err.Error()})
			return
		}
		c.JSON(200, gin.H{"code": 0, "message": "success", "data": data})
	})

	// v3: ES 日志搜索 API
	r.GET("/api/logs/search", middleware.JWTAuth(userSvc), func(c *gin.Context) {
		if !esLogSvc.Enabled() {
			c.JSON(400, gin.H{"code": 1001, "message": "ES is not enabled"})
			return
		}
		req := &espkg.SearchLogsRequest{
			Keyword:  c.Query("keyword"),
			Page:     queryInt(c, "page", 1),
			PageSize: queryInt(c, "page_size", 20),
		}
		if jid := c.Query("job_id"); jid != "" {
			fmt.Sscanf(jid, "%d", &req.JobID)
		}
		docs, total, err := esLogSvc.SearchLogs(c.Request.Context(), req)
		if err != nil {
			c.JSON(500, gin.H{"code": 1500, "message": err.Error()})
			return
		}
		c.JSON(200, gin.H{"code": 0, "data": gin.H{
			"list": docs, "total": total,
			"page": req.Page, "page_size": req.PageSize,
		}})
	})

	// v3: 任务分组 API
	r.GET("/api/groups", middleware.JWTAuth(userSvc), func(c *gin.Context) {
		groups, err := groupSvc.ListGroups(c.Request.Context())
		if err != nil {
			c.JSON(500, gin.H{"code": 1500, "message": err.Error()})
			return
		}
		c.JSON(200, gin.H{"code": 0, "data": groups})
	})
	r.POST("/api/groups", middleware.JWTAuth(userSvc), func(c *gin.Context) {
		var body struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(400, gin.H{"code": 1001, "message": err.Error()})
			return
		}
		g, err := groupSvc.CreateGroup(c.Request.Context(), body.Name, body.Description)
		if err != nil {
			c.JSON(500, gin.H{"code": 1500, "message": err.Error()})
			return
		}
		c.JSON(200, gin.H{"code": 0, "data": g})
	})
	r.DELETE("/api/groups/:id", middleware.JWTAuth(userSvc), func(c *gin.Context) {
		var id int64
		fmt.Sscanf(c.Param("id"), "%d", &id)
		if err := groupSvc.DeleteGroup(c.Request.Context(), id); err != nil {
			c.JSON(500, gin.H{"code": 1500, "message": err.Error()})
			return
		}
		c.JSON(200, gin.H{"code": 0})
	})

	r.POST("/api/login", adminhandler.NewUserHandler(userSvc).Login)

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

	api := r.Group("/api")
	api.Use(middleware.JWTAuth(userSvc))
	{
		userH := adminhandler.NewUserHandler(userSvc)
		api.GET("/user/me", userH.GetCurrentUser)

		jobH := adminhandler.NewJobHandler(jobSvc)
		logH := adminhandler.NewLogHandler(jobSvc)
		execH := adminhandler.NewExecutorHandler(execSvc)

		// ── 任务模板 API ─────────────────────────────────────────────────
		tplH := adminhandler.NewJobTemplateHandler(tplSvc)
		templates := api.Group("/job-templates")
		templates.POST("", tplH.CreateTemplate)
		templates.GET("", tplH.ListTemplates)
		templates.GET("/id/:id", tplH.GetTemplateByID)
		templates.GET("/:name", tplH.GetTemplate)
		templates.PUT("/:name", tplH.UpdateTemplate)
		templates.DELETE("/:name", tplH.DeleteTemplate)
		templates.POST("/:name/instantiate", tplH.InstantiateTemplate)
		templates.POST("/:name/batch-instantiate", tplH.BatchInstantiateTemplate)

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

	// ── Etcd Leader Election ─────────────────────────────────────────────────
	var leaderElection *etcdpkg.LeaderElection
	if len(cfg.Etcd.Endpoints) > 0 && cfg.Etcd.Endpoints[0] != "" {
		etcdCli, err := etcdpkg.NewClient(
			etcdpkg.WithEndpoints(cfg.Etcd.Endpoints),
			etcdpkg.WithDialTimeout(cfg.Etcd.DialTimeout),
		)
		if err != nil {
			applogger.Warn("etcd connect failed, running as standalone", zap.Error(err))
		} else {
			etcdClient = etcdCli
			leaderElection, err = etcdpkg.NewLeaderElection(
				etcdCli,
				"/go-jobs/scheduler/leader",
				nodeID,
				func() {
					applogger.Info("etcd: became leader, starting scheduler")
					if startErr := sched.Start(); startErr != nil {
						applogger.Error("scheduler start (after election) failed", zap.Error(startErr))
					}
				},
				func() {
					applogger.Warn("etcd: lost leadership, stopping scheduler")
					sched.Stop()
				},
			)
			if err != nil {
				applogger.Warn("leader election init failed", zap.Error(err))
				leaderElection = nil
			} else {
				applogger.Info("etcd connected, starting leader election",
					zap.Strings("endpoints", cfg.Etcd.Endpoints))
				go leaderElection.Run(context.Background())
			}
		}
	}
	_ = etcdClient

	// 无 Etcd 时直接启动
	if leaderElection == nil {
		if err := sched.Start(); err != nil {
			applogger.Fatal("start scheduler failed", zap.Error(err))
		}
		applogger.Info("scheduler started (standalone mode)")
	}

	// ── Background tickers ────────────────────────────────────────────────────
	sweepTicker := time.NewTicker(30 * time.Second)
	retryTicker := time.NewTicker(30 * time.Second)
	statTicker := time.NewTicker(time.Hour)

	go func() {
		for range sweepTicker.C {
			execSvc.SweepOfflineExecutors(context.Background())
		}
	}()
	go func() {
		for range retryTicker.C {
			retrySvc.Run(context.Background())
		}
	}()
	go func() {
		for t := range statTicker.C {
			if err := statSvc.Aggregate(context.Background(), t.Add(-time.Hour)); err != nil {
				applogger.Warn("stat aggregate failed", zap.Error(err))
			}
		}
	}()

	go func() {
		applogger.Info("http server listening", zap.String("addr", srv.Addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			applogger.Fatal("listen failed", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	applogger.Info("shutting down...")
	sweepTicker.Stop()
	retryTicker.Stop()
	statTicker.Stop()

	if leaderElection != nil {
		leaderElection.Stop()
	} else {
		sched.Stop()
	}

	// Flush remaining log records before closing DB connections.
	// logBuf.Stop() drains the ring buffer and waits for the flush goroutine,
	// ensuring no job_log rows are silently dropped on shutdown.
	logBuf.Stop()
	applogger.Info("log buffer flushed")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		applogger.Error("server shutdown error", zap.Error(err))
	}
	applogger.Info("go-jobs admin v3 stopped")
}

func queryInt(c *gin.Context, key string, defaultVal int) int {
	s := c.Query(key)
	if s == "" {
		return defaultVal
	}
	var n int
	if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
		return defaultVal
	}
	return n
}
