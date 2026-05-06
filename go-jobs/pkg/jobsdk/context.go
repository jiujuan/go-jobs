// Package jobsdk provides a decorator-style API for registering job handlers
// on a go-jobs executor, replacing the low-level Handler func signature with
// a richer Context interface that supports parameter binding, structured
// logging, sharding metadata, and automatic panic recovery.
//
// # Quick start
//
//	sdk := jobsdk.New(executor.NewRegistry())
//
//	// Function-style registration
//	sdk.Register(jobsdk.Desc{
//	    Name:    "sendEmail",
//	    Timeout: 30 * time.Second,
//	    Retry:   3,
//	}, func(ctx jobsdk.Context) error {
//	    var p EmailParams
//	    if err := ctx.Bind(&p); err != nil {
//	        return err
//	    }
//	    ctx.Log("sending email to %s", p.To)
//	    return sendEmail(p)
//	})
//
//	// Struct-style registration (preferred for handlers with dependencies)
//	sdk.RegisterHandler(&EmailHandler{mailer: mailer})
//
//	// sdk.Registry() returns the *executor.Registry for use in Runner/main.go
package jobsdk

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/jiujuan/go-jobs/internal/executor"
)

// ─── Context ──────────────────────────────────────────────────────────────────

// Context is the rich execution context passed to every SDK handler.
// It wraps the standard context.Context and adds convenience methods
// for parameter binding, logging, and accessing job metadata.
type Context interface {
	context.Context

	// Bind deserializes the job's ExecuteParam JSON into v.
	// v must be a non-nil pointer to a struct, map, or primitive.
	// Returns an error if the param is not valid JSON for the target type.
	Bind(v interface{}) error

	// RawParam returns the raw ExecuteParam string unchanged.
	RawParam() string

	// Log appends a formatted message to this execution's in-memory log.
	// Collected lines are accessible via LogLines() for forwarding to scheduler.
	Log(format string, args ...interface{})

	// LogLines returns all Log() lines collected so far (snapshot).
	LogLines() []string

	// JobID returns the ID of the job definition being executed.
	JobID() int64

	// LogID returns the unique log/execution record ID for this trigger.
	LogID() int64

	// ShardingIndex returns this executor's shard index (0-based) when
	// RouteStrategy = SHARDING_BROADCAST, or 0 for non-sharded jobs.
	ShardingIndex() int

	// ShardingTotal returns the total number of shards in the broadcast,
	// or 1 for non-sharded jobs.
	ShardingTotal() int
}

// ─── jobContext implementation ─────────────────────────────────────────────────

type jobContext struct {
	context.Context
	param  string
	jc     executor.JobContext
	mu     sync.Mutex
	lines  []string
}

func newJobContext(ctx context.Context, param string, jc executor.JobContext) *jobContext {
	return &jobContext{
		Context: ctx,
		param:   param,
		jc:      jc,
	}
}

// Bind deserializes the JSON ExecuteParam into v.
// Empty param is treated as `{}` for struct targets so callers don't
// need to guard against empty param strings.
func (c *jobContext) Bind(v interface{}) error {
	s := strings.TrimSpace(c.param)
	if s == "" {
		s = "{}"
	}
	if err := json.Unmarshal([]byte(s), v); err != nil {
		return fmt.Errorf("jobsdk: Bind: %w (param=%q)", err, c.param)
	}
	return nil
}

func (c *jobContext) RawParam() string { return c.param }

func (c *jobContext) Log(format string, args ...interface{}) {
	line := fmt.Sprintf(format, args...)
	c.mu.Lock()
	c.lines = append(c.lines, line)
	c.mu.Unlock()
}

func (c *jobContext) LogLines() []string {
	c.mu.Lock()
	snap := make([]string, len(c.lines))
	copy(snap, c.lines)
	c.mu.Unlock()
	return snap
}

func (c *jobContext) JobID() int64        { return c.jc.JobID }
func (c *jobContext) LogID() int64        { return c.jc.LogID }
func (c *jobContext) ShardingIndex() int  { return c.jc.ShardingIndex }
func (c *jobContext) ShardingTotal() int  {
	if c.jc.ShardingTotal == 0 {
		return 1
	}
	return c.jc.ShardingTotal
}
