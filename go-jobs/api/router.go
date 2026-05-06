// Package api wires together all HTTP routes for go-jobs.
package api

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	"github.com/jiujuan/go-jobs/api/handler/admin"
	executor_handler "github.com/jiujuan/go-jobs/api/handler/executor"
	"github.com/jiujuan/go-jobs/api/middleware"
	"github.com/jiujuan/go-jobs/internal/service"
)

// AdminRouterDeps bundles all handler dependencies for the admin server.
type AdminRouterDeps struct {
	UserSvc    *service.UserService
	JobSvc     *service.JobService
	ExecSvc    *service.ExecutorService
	InternalToken string // shared secret for executor<->admin communication
}

// SetupAdminRouter configures and returns the Gin engine for the admin (scheduler) server.
func SetupAdminRouter(deps *AdminRouterDeps) *gin.Engine {
	r := gin.New()
	r.Use(
		middleware.Recovery(),
		middleware.RequestLogger(),
		cors.New(cors.Config{
			AllowOrigins:     []string{"*"},
			AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
			AllowHeaders:     []string{"*"},
			ExposeHeaders:    []string{"X-Request-ID"},
			AllowCredentials: true,
		}),
	)

	// Health probe - unauthenticated.
	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })

	// Public routes.
	pubGroup := r.Group("/api")
	{
		userHandler := admin.NewUserHandler(deps.UserSvc)
		pubGroup.POST("/login", userHandler.Login)
	}

	// ── Internal routes (executor → scheduler) ────────────────────────────
	// Uses a shared internal token instead of JWT.
	internalGroup := r.Group("/api/executor")
	internalGroup.Use(middleware.InternalAuth(deps.InternalToken))
	{
		internalHandler := admin.NewInternalExecutorHandler(deps.ExecSvc, deps.JobSvc)
		internalGroup.POST("/register", internalHandler.Register)
		internalGroup.POST("/heartbeat", internalHandler.Heartbeat)
		internalGroup.POST("/deregister", internalHandler.Deregister)
	}

	// ── Admin API routes (require JWT) ────────────────────────────────────
	apiGroup := r.Group("/api")
	apiGroup.Use(middleware.JWTAuth(deps.UserSvc))
	{
		userHandler := admin.NewUserHandler(deps.UserSvc)
		apiGroup.GET("/user/me", userHandler.GetCurrentUser)

		jobHandler := admin.NewJobHandler(deps.JobSvc)
		jobs := apiGroup.Group("/jobs")
		{
			jobs.POST("", jobHandler.CreateJob)
			jobs.GET("", jobHandler.ListJobs)
			jobs.GET("/:id", jobHandler.GetJob)
			jobs.PUT("/:id", jobHandler.UpdateJob)
			jobs.DELETE("/:id", jobHandler.DeleteJob)
			jobs.POST("/:id/start", jobHandler.StartJob)
			jobs.POST("/:id/stop", jobHandler.StopJob)
			jobs.POST("/:id/trigger", jobHandler.TriggerJob)
			jobs.GET("/:id/logs", admin.NewLogHandler(deps.JobSvc).ListJobLogs)
		}

		logHandler := admin.NewLogHandler(deps.JobSvc)
		logs := apiGroup.Group("/logs")
		{
			logs.GET("/:logID/detail", logHandler.GetLogDetail)
			logs.POST("/:logID/kill", jobHandler.KillJob)
		}

		execHandler := admin.NewExecutorHandler(deps.ExecSvc)
		executors := apiGroup.Group("/executors")
		{
			executors.GET("", execHandler.ListExecutors)
		}
	}

	return r
}

// ExecutorRouterDeps bundles dependencies for the executor's HTTP server.
type ExecutorRouterDeps struct {
	Runner        *executor.Runner
	InternalToken string
}

// SetupExecutorRouter configures the Gin engine for the executor server.
// We import the runner via interface to avoid a heavy dependency.
func SetupExecutorRouter(runner interface{}, internalToken string) *gin.Engine {
	r := gin.New()
	r.Use(
		middleware.Recovery(),
		middleware.RequestLogger(),
	)

	r.GET("/health", func(c *gin.Context) { c.JSON(200, gin.H{"status": "ok"}) })

	// Executor endpoints are protected by a shared internal token.
	exec := r.Group("/executor")
	exec.Use(middleware.InternalAuth(internalToken))
	{
		// The handler is wired in cmd/executor/main.go to avoid import cycle.
		// These routes are documented here for reference:
		// POST /executor/run
		// POST /executor/kill
		// POST /executor/beat
		// POST /executor/idleBeat
		_ = exec
	}
	return r
}
