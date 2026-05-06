// Package middleware provides Gin middleware for go-jobs HTTP servers.
package middleware

import (
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/api/response"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/internal/service"
	"github.com/jiujuan/go-jobs/pkg/logger"
	"github.com/jiujuan/go-jobs/pkg/utils"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

const (
	// CtxUserID is the gin context key for the authenticated user's ID.
	CtxUserID   = "userID"
	// CtxUsername is the gin context key for the authenticated username.
	CtxUsername = "username"
	// CtxUserRole is the gin context key for the authenticated user role.
	CtxUserRole = "userRole"
)

// RequestLogger logs each request with method, path, status, latency and request ID.
func RequestLogger() gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		requestID := utils.NewRequestID()
		c.Set("requestID", requestID)
		c.Header("X-Request-ID", requestID)

		c.Next()

		latency := time.Since(start)
		logger.Info("http",
			zap.String("requestID", requestID),
			zap.String("method", c.Request.Method),
			zap.String("path", c.Request.URL.Path),
			zap.Int("status", c.Writer.Status()),
			zap.Duration("latency", latency),
			zap.String("clientIP", c.ClientIP()),
		)
	}
}

// Recovery catches panics and returns a 500 response.
func Recovery() gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				logger.Error("panic recovered",
					zap.Any("error", err),
					zap.String("path", c.Request.URL.Path))
				c.AbortWithStatusJSON(http.StatusInternalServerError, response.R{
					Code:    int(xerror.CodeInternalServer),
					Message: "internal server error",
				})
			}
		}()
		c.Next()
	}
}

// JWTAuth validates the Bearer token and injects user info into the context.
func JWTAuth(userSvc *service.UserService) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			response.Unauthorized(c, "missing Authorization header")
			c.Abort()
			return
		}

		parts := strings.SplitN(authHeader, " ", 2)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "bearer") {
			response.Unauthorized(c, "invalid Authorization header format")
			c.Abort()
			return
		}

		claims, err := userSvc.ParseToken(parts[1])
		if err != nil {
			response.Fail(c, err)
			c.Abort()
			return
		}

		c.Set(CtxUserID, claims.UserID)
		c.Set(CtxUsername, claims.Username)
		c.Set(CtxUserRole, claims.Role)
		c.Next()
	}
}

// AdminOnly only allows admin-role users through.
func AdminOnly() gin.HandlerFunc {
	return func(c *gin.Context) {
		role, _ := c.Get(CtxUserRole)
		if r, ok := role.(model.UserRole); !ok || r != model.RoleAdmin {
			c.AbortWithStatusJSON(http.StatusForbidden, response.R{
				Code:    int(xerror.CodeForbidden),
				Message: "admin access required",
			})
			return
		}
		c.Next()
	}
}

// InternalAuth validates the internal service token used by executors.
// This is a simple shared-secret check; in production use mTLS or a proper secret manager.
func InternalAuth(token string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.GetHeader("X-Go-Jobs-Token") != token {
			c.AbortWithStatusJSON(http.StatusUnauthorized, response.R{
				Code:    int(xerror.CodeUnauthorized),
				Message: "invalid internal token",
			})
			return
		}
		c.Next()
	}
}

// CurrentUserID extracts the logged-in user's ID from the gin context.
func CurrentUserID(c *gin.Context) int64 {
	id, _ := c.Get(CtxUserID)
	if uid, ok := id.(int64); ok {
		return uid
	}
	return 0
}

// CurrentUsername extracts the logged-in username from the gin context.
func CurrentUsername(c *gin.Context) string {
	u, _ := c.Get(CtxUsername)
	if s, ok := u.(string); ok {
		return s
	}
	return ""
}
