// Package xerror defines unified error codes and custom error types for go-jobs.
// All API responses and service errors use these codes to ensure consistency.
package xerror

import (
	"fmt"
	"net/http"
)

// Code represents a business error code.
type Code int

const (
	// Success
	CodeOK Code = 0

	// Common errors 10xx
	CodeInvalidParam   Code = 1001
	CodeUnauthorized   Code = 1002
	CodeForbidden      Code = 1003
	CodeNotFound       Code = 1004
	CodeAlreadyExists  Code = 1005
	CodeInternalServer Code = 1500

	// Executor errors 20xx
	CodeExecutorOffline     Code = 2001
	CodeExecutorNotFound    Code = 2002
	CodeExecutorRegisterFail Code = 2003

	// Job errors 30xx
	CodeJobNotFound     Code = 3001
	CodeJobAlreadyStop  Code = 3002
	CodeJobAlreadyStart Code = 3003
	CodeJobTriggerFail  Code = 3004
	CodeJobParamInvalid Code = 3005
	CodeCronExprInvalid Code = 3006

	// Lock errors 40xx
	CodeLockFail    Code = 4001
	CodeLockTimeout Code = 4002

	// CodeDuplicateExecution 表示相同 LogID 的任务正在执行中，本次为重复请求。
	// HTTP 层应返回 409 Conflict。
	CodeDuplicateExecution Code = 4003

	// CodeExecutorDraining 表示 executor 正在优雅关闭（draining），
	// 不再接受新任务触发。调度器收到此错误后应将任务路由到其他实例。
	// HTTP 层返回 503 Service Unavailable。
	CodeExecutorDraining Code = 4004

	// Auth errors 50xx
	CodeLoginFail       Code = 5001
	CodeTokenExpired    Code = 5002
	CodeTokenInvalid    Code = 5003
	CodeUserDisabled    Code = 5004
	CodePasswordWrong   Code = 5005
)

var codeMessages = map[Code]string{
	CodeOK:              "success",
	CodeInvalidParam:    "invalid parameter",
	CodeUnauthorized:    "unauthorized",
	CodeForbidden:       "forbidden",
	CodeNotFound:        "not found",
	CodeAlreadyExists:   "already exists",
	CodeInternalServer:  "internal server error",

	CodeExecutorOffline:     "executor is offline",
	CodeExecutorNotFound:    "executor not found",
	CodeExecutorRegisterFail:"executor register failed",

	CodeJobNotFound:     "job not found",
	CodeJobAlreadyStop:  "job is already stopped",
	CodeJobAlreadyStart: "job is already running",
	CodeJobTriggerFail:  "job trigger failed",
	CodeJobParamInvalid: "job parameter invalid",
	CodeCronExprInvalid: "cron expression invalid",

	CodeLockFail:    "acquire lock failed",
	CodeLockTimeout: "lock timeout",

	CodeDuplicateExecution: "duplicate execution: same log_id already running",
	CodeExecutorDraining:   "executor is draining, no new jobs accepted",

	CodeLoginFail:     "login failed",
	CodeTokenExpired:  "token expired",
	CodeTokenInvalid:  "token invalid",
	CodeUserDisabled:  "user is disabled",
	CodePasswordWrong: "username or password incorrect",
}

// Message returns the default message for a given code.
func (c Code) Message() string {
	if msg, ok := codeMessages[c]; ok {
		return msg
	}
	return "unknown error"
}

// HTTPStatus maps business code to HTTP status code.
func (c Code) HTTPStatus() int {
	switch c {
	case CodeOK:
		return http.StatusOK
	case CodeInvalidParam, CodeCronExprInvalid, CodeJobParamInvalid:
		return http.StatusBadRequest
	case CodeUnauthorized, CodeTokenExpired, CodeTokenInvalid, CodeLoginFail:
		return http.StatusUnauthorized
	case CodeForbidden:
		return http.StatusForbidden
	case CodeDuplicateExecution:
		return http.StatusConflict // 409
	case CodeExecutorDraining:
		return http.StatusServiceUnavailable // 503
	case CodeNotFound, CodeJobNotFound, CodeExecutorNotFound:
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

// Error is the unified error type used throughout go-jobs.
type Error struct {
	Code    Code   `json:"code"`
	Message string `json:"message"`
	Details string `json:"details,omitempty"`
	cause   error
}

// New creates a new Error with the given code and an optional custom message.
func New(code Code, msg ...string) *Error {
	e := &Error{Code: code, Message: code.Message()}
	if len(msg) > 0 && msg[0] != "" {
		e.Message = msg[0]
	}
	return e
}

// Wrap wraps an existing error with a business code.
func Wrap(code Code, cause error, msg ...string) *Error {
	e := New(code, msg...)
	e.cause = cause
	if cause != nil {
		e.Details = cause.Error()
	}
	return e
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("code=%d msg=%s cause=%v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("code=%d msg=%s", e.Code, e.Message)
}

// Unwrap allows errors.Is / errors.As traversal.
func (e *Error) Unwrap() error { return e.cause }

// IsCode checks whether an error carries the given code.
func IsCode(err error, code Code) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*Error); ok {
		return e.Code == code
	}
	return false
}
