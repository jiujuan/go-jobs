package jobsdk

import (
	"fmt"
	"time"
)

// ─── Desc — handler descriptor ────────────────────────────────────────────────

// Desc describes a job handler and its execution policy.
// All fields except Name are optional; zero values mean "use executor default".
type Desc struct {
	// Name is the handler name that must match job_info.execute_handler.
	// Required; panics on registration if empty.
	Name string

	// Timeout overrides the per-execution deadline for this handler.
	// 0 means "inherit the timeout from the trigger request".
	// When set, a context.WithTimeout wrapping is applied before the handler
	// is called (in addition to any timeout already on the RunRequest).
	Timeout time.Duration

	// Retry is purely declarative metadata: it is stored on the Desc and
	// accessible via SDK.DescOf(name), but retry scheduling is controlled
	// by the scheduler (job_info.retry_count). Set it here to document
	// intent and enable tooling/generators to read it.
	Retry int

	// PanicRecover controls whether panics in the handler are caught and
	// returned as errors. Default true (safe); set false only for debugging.
	// When not explicitly set (zero value), defaults to true.
	panicRecoverSet bool
	panicRecover    bool
}

// WithPanicRecover returns a copy of d with PanicRecover set to v.
func (d Desc) WithPanicRecover(v bool) Desc {
	d.panicRecoverSet = true
	d.panicRecover = v
	return d
}

// shouldRecover returns true if panic recovery is enabled (default: true).
func (d Desc) shouldRecover() bool {
	if d.panicRecoverSet {
		return d.panicRecover
	}
	return true // safe default
}

// ─── HandlerFunc — function-style handler ─────────────────────────────────────

// HandlerFunc is the function signature for SDK-style job handlers.
// ctx provides parameter binding, logging and job metadata.
type HandlerFunc func(ctx Context) error

// ─── JobHandler — struct-style handler ───────────────────────────────────────

// JobHandler is implemented by structs that want to register themselves as
// job handlers. The Desc() method supplies the handler name and policy;
// Execute() is the entry point called on each trigger.
//
// Example:
//
//	type EmailHandler struct { mailer Mailer }
//
//	func (h *EmailHandler) Desc() jobsdk.Desc {
//	    return jobsdk.Desc{Name: "sendEmail", Timeout: 30 * time.Second, Retry: 3}
//	}
//
//	func (h *EmailHandler) Execute(ctx jobsdk.Context) error {
//	    var p EmailParams
//	    if err := ctx.Bind(&p); err != nil { return err }
//	    ctx.Log("sending to %s", p.To)
//	    return h.mailer.Send(p)
//	}
type JobHandler interface {
	Desc() Desc
	Execute(ctx Context) error
}

// ─── panicError ───────────────────────────────────────────────────────────────

// panicError wraps a recovered panic value so callers can distinguish
// a panic-turned-error from a regular handler error.
type panicError struct{ v interface{} }

func (e *panicError) Error() string {
	return fmt.Sprintf("jobsdk: handler panic: %v", e.v)
}

// IsPanic returns true if err was produced by a recovered panic.
func IsPanic(err error) bool {
	_, ok := err.(*panicError)
	return ok
}
