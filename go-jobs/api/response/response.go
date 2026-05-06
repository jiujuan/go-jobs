// Package response provides the standard API response envelope for go-jobs.
package response

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jiujuan/go-jobs/pkg/xerror"
)

// R is the unified JSON response envelope.
type R struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Page wraps paginated results.
type Page[T any] struct {
	List     []T   `json:"list"`
	Total    int64 `json:"total"`
	Page     int   `json:"page"`
	PageSize int   `json:"page_size"`
}

// OK sends a 200 response with data.
func OK(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, R{Code: 0, Message: "success", Data: data})
}

// OKWithPage sends a 200 response with paginated data.
func OKWithPage[T any](c *gin.Context, list []T, total int64, page, pageSize int) {
	OK(c, Page[T]{List: list, Total: total, Page: page, PageSize: pageSize})
}

// Fail sends an error response derived from an *xerror.Error.
func Fail(c *gin.Context, err error) {
	if e, ok := err.(*xerror.Error); ok {
		c.JSON(e.Code.HTTPStatus(), R{
			Code:    int(e.Code),
			Message: e.Message,
		})
		return
	}
	c.JSON(http.StatusInternalServerError, R{
		Code:    int(xerror.CodeInternalServer),
		Message: err.Error(),
	})
}

// BadRequest sends a 400 response.
func BadRequest(c *gin.Context, msg string) {
	c.JSON(http.StatusBadRequest, R{
		Code:    int(xerror.CodeInvalidParam),
		Message: msg,
	})
}

// Unauthorized sends a 401 response.
func Unauthorized(c *gin.Context, msg string) {
	if msg == "" {
		msg = xerror.CodeUnauthorized.Message()
	}
	c.JSON(http.StatusUnauthorized, R{
		Code:    int(xerror.CodeUnauthorized),
		Message: msg,
	})
}
