package jobsdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jiujuan/go-jobs/internal/executor"
)

// ─── Test helpers ──────────────────────────────────────────────────────────────

func newTestSDK() *SDK {
	return New(executor.NewRegistry())
}

// execHandler calls the raw executor.Handler wired into the registry for name.
// This lets tests exercise the full wrapHandler path without a Runner.
func execHandler(t *testing.T, sdk *SDK, name string, ctx context.Context, param string) error {
	t.Helper()
	h, ok := sdk.Registry().Get(name)
	require.True(t, ok, "handler %q not found in registry", name)
	return h(ctx, param)
}

// jcCtx returns a background context with a JobContext injected.
func jcCtx(jobID, logID int64, shardIdx, shardTotal int) context.Context {
	return executor.WithJobContext(context.Background(), executor.JobContext{
		JobID:         jobID,
		LogID:         logID,
		ShardingIndex: shardIdx,
		ShardingTotal: shardTotal,
	})
}

// ─── jobContext — Bind ─────────────────────────────────────────────────────────

func TestBind_ValidJSON_UnmarshalsIntoStruct(t *testing.T) {
	type Params struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}

	called := false
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "bindJob"}, func(ctx Context) error {
		var p Params
		require.NoError(t, ctx.Bind(&p))
		assert.Equal(t, "alice@example.com", p.To)
		assert.Equal(t, "Hello", p.Subject)
		called = true
		return nil
	})

	param := `{"to":"alice@example.com","subject":"Hello"}`
	err := execHandler(t, sdk, "bindJob", context.Background(), param)
	require.NoError(t, err)
	assert.True(t, called)
}

func TestBind_EmptyParam_TreatedAsEmptyObject(t *testing.T) {
	type Params struct{ Name string }

	sdk := newTestSDK()
	sdk.Register(Desc{Name: "emptyParamJob"}, func(ctx Context) error {
		var p Params
		// Empty param → treated as `{}` → Name stays zero value
		require.NoError(t, ctx.Bind(&p))
		assert.Equal(t, "", p.Name)
		return nil
	})

	err := execHandler(t, sdk, "emptyParamJob", context.Background(), "")
	require.NoError(t, err)
}

func TestBind_WhitespaceOnlyParam_TreatedAsEmptyObject(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "wsJob"}, func(ctx Context) error {
		var m map[string]interface{}
		require.NoError(t, ctx.Bind(&m))
		assert.Empty(t, m)
		return nil
	})
	err := execHandler(t, sdk, "wsJob", context.Background(), "   \t\n")
	require.NoError(t, err)
}

func TestBind_InvalidJSON_ReturnsError(t *testing.T) {
	sdk := newTestSDK()
	var bindErr error
	sdk.Register(Desc{Name: "badParamJob"}, func(ctx Context) error {
		var p struct{ X int }
		bindErr = ctx.Bind(&p)
		return bindErr
	})

	err := execHandler(t, sdk, "badParamJob", context.Background(), `not-json`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "jobsdk: Bind")
}

func TestBind_Map_Works(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "mapJob"}, func(ctx Context) error {
		var m map[string]interface{}
		require.NoError(t, ctx.Bind(&m))
		assert.Equal(t, float64(42), m["count"])
		return nil
	})
	err := execHandler(t, sdk, "mapJob", context.Background(), `{"count":42}`)
	require.NoError(t, err)
}

func TestBind_Primitives_WorkWithPointers(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "primitiveJob"}, func(ctx Context) error {
		var n int
		require.NoError(t, ctx.Bind(&n))
		assert.Equal(t, 7, n)
		return nil
	})
	err := execHandler(t, sdk, "primitiveJob", context.Background(), `7`)
	require.NoError(t, err)
}

// ─── jobContext — RawParam ─────────────────────────────────────────────────────

func TestRawParam_ReturnedUnchanged(t *testing.T) {
	raw := `{"key":"value with spaces"}`
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "rawJob"}, func(ctx Context) error {
		assert.Equal(t, raw, ctx.RawParam())
		return nil
	})
	err := execHandler(t, sdk, "rawJob", context.Background(), raw)
	require.NoError(t, err)
}

// ─── jobContext — Log ─────────────────────────────────────────────────────────

func TestLog_CollectsLines(t *testing.T) {
	var captured []string
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "logJob"}, func(ctx Context) error {
		ctx.Log("step %d", 1)
		ctx.Log("step %d", 2)
		ctx.Log("done")
		captured = ctx.LogLines()
		return nil
	})
	err := execHandler(t, sdk, "logJob", context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, []string{"step 1", "step 2", "done"}, captured)
}

func TestLog_ConcurrentSafe(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "concLogJob"}, func(ctx Context) error {
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				ctx.Log("msg %d", n)
			}(i)
		}
		wg.Wait()
		lines := ctx.LogLines()
		assert.Len(t, lines, 50)
		return nil
	})
	err := execHandler(t, sdk, "concLogJob", context.Background(), "")
	require.NoError(t, err)
}

func TestLogLines_SnapshotIsolated(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "snapJob"}, func(ctx Context) error {
		ctx.Log("first")
		snap1 := ctx.LogLines()
		ctx.Log("second")
		snap2 := ctx.LogLines()

		// snap1 must not be affected by later Log() calls
		assert.Len(t, snap1, 1)
		assert.Len(t, snap2, 2)
		return nil
	})
	err := execHandler(t, sdk, "snapJob", context.Background(), "")
	require.NoError(t, err)
}

// ─── jobContext — metadata accessors ──────────────────────────────────────────

func TestJobID_LogID_ShardingIndex_ShardingTotal(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "metaJob"}, func(ctx Context) error {
		assert.Equal(t, int64(42), ctx.JobID())
		assert.Equal(t, int64(999), ctx.LogID())
		assert.Equal(t, 2, ctx.ShardingIndex())
		assert.Equal(t, 5, ctx.ShardingTotal())
		return nil
	})

	ctx := jcCtx(42, 999, 2, 5)
	err := execHandler(t, sdk, "metaJob", ctx, "")
	require.NoError(t, err)
}

func TestShardingTotal_ZeroSharding_ReturnsOne(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "noShardJob"}, func(ctx Context) error {
		// ShardingTotal==0 from executor means non-sharded → SDK returns 1
		assert.Equal(t, 1, ctx.ShardingTotal())
		return nil
	})

	ctx := jcCtx(1, 1, 0, 0) // ShardingTotal=0
	err := execHandler(t, sdk, "noShardJob", ctx, "")
	require.NoError(t, err)
}

// ─── jobContext — context.Context delegation ──────────────────────────────────

func TestContext_DeadlineCancellationPropagated(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "ctxJob"}, func(ctx Context) error {
		select {
		case <-time.After(10 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := execHandler(t, sdk, "ctxJob", ctx, "")
	assert.ErrorIs(t, err, context.Canceled)
}

// ─── Desc ─────────────────────────────────────────────────────────────────────

func TestDesc_ShouldRecover_DefaultTrue(t *testing.T) {
	d := Desc{Name: "x"}
	assert.True(t, d.shouldRecover(), "default should be panic-recovery on")
}

func TestDesc_WithPanicRecover_False(t *testing.T) {
	d := Desc{Name: "x"}.WithPanicRecover(false)
	assert.False(t, d.shouldRecover())
}

func TestDesc_WithPanicRecover_True(t *testing.T) {
	d := Desc{Name: "x"}.WithPanicRecover(true)
	assert.True(t, d.shouldRecover())
}

// ─── IsPanic ──────────────────────────────────────────────────────────────────

func TestIsPanic_PanicError_ReturnsTrue(t *testing.T) {
	err := &panicError{v: "boom"}
	assert.True(t, IsPanic(err))
}

func TestIsPanic_RegularError_ReturnsFalse(t *testing.T) {
	assert.False(t, IsPanic(errors.New("normal")))
}

func TestIsPanic_Nil_ReturnsFalse(t *testing.T) {
	assert.False(t, IsPanic(nil))
}

// ─── Panic recovery ───────────────────────────────────────────────────────────

func TestPanicRecovery_DefaultEnabled_PanicBecomesPanicError(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "panicJob"}, func(ctx Context) error {
		panic("something exploded")
	})

	err := execHandler(t, sdk, "panicJob", context.Background(), "")
	require.Error(t, err)
	assert.True(t, IsPanic(err), "panic should be wrapped as panicError")
	assert.Contains(t, err.Error(), "something exploded")
}

func TestPanicRecovery_StringPanic_CapturedCorrectly(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "strPanicJob"}, func(ctx Context) error {
		panic("string panic")
	})

	err := execHandler(t, sdk, "strPanicJob", context.Background(), "")
	assert.Contains(t, err.Error(), "string panic")
}

func TestPanicRecovery_ErrorPanic_CapturedCorrectly(t *testing.T) {
	sdk := newTestSDK()
	innerErr := errors.New("inner error")
	sdk.Register(Desc{Name: "errPanicJob"}, func(ctx Context) error {
		panic(innerErr)
	})

	err := execHandler(t, sdk, "errPanicJob", context.Background(), "")
	require.True(t, IsPanic(err))
	assert.Contains(t, err.Error(), "inner error")
}

func TestPanicRecovery_Disabled_PanicPropagate(t *testing.T) {
	sdk := newTestSDK()
	d := Desc{Name: "noRecoverJob"}.WithPanicRecover(false)
	sdk.Register(d, func(ctx Context) error {
		panic("unhandled")
	})

	assert.Panics(t, func() {
		execHandler(t, sdk, "noRecoverJob", context.Background(), "")
	})
}

func TestPanicRecovery_NilPanic_CapturedCorrectly(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "nilPanicJob"}, func(ctx Context) error {
		panic(nil)
	})

	err := execHandler(t, sdk, "nilPanicJob", context.Background(), "")
	require.True(t, IsPanic(err))
}

// ─── Per-handler timeout tightening ──────────────────────────────────────────

func TestTimeout_DescTimeout_AppliedWhenNoDeadline(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "shortTimeoutJob", Timeout: 50 * time.Millisecond}, func(ctx Context) error {
		select {
		case <-time.After(10 * time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})

	start := time.Now()
	err := execHandler(t, sdk, "shortTimeoutJob", context.Background(), "")
	elapsed := time.Since(start)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	// Should fire around 50ms, certainly under 500ms
	assert.Less(t, elapsed, 500*time.Millisecond)
}

func TestTimeout_DescTimeout_NotAppliedWhenTighterDeadlineExists(t *testing.T) {
	// Incoming context has 20ms; Desc has 500ms → incoming deadline wins
	sdk := newTestSDK()
	var deadlineFromCtx time.Time
	sdk.Register(Desc{Name: "deadlineJob", Timeout: 500 * time.Millisecond}, func(ctx Context) error {
		if dl, ok := ctx.Deadline(); ok {
			deadlineFromCtx = dl
		}
		return nil
	})

	tightCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// Run handler immediately (it doesn't block, just captures deadline)
	_ = execHandler(t, sdk, "deadlineJob", tightCtx, "")

	// Deadline should be ~20ms from when the tight context was created, not 500ms
	if !deadlineFromCtx.IsZero() {
		fromNow := time.Until(deadlineFromCtx) + 20*time.Millisecond
		// 20ms context + some slack; definitely less than 500ms timeout
		assert.Less(t, fromNow, 400*time.Millisecond,
			"tight incoming deadline should prevail over Desc.Timeout")
	}
}

func TestTimeout_ZeroDescTimeout_NoExtraWrapping(t *testing.T) {
	sdk := newTestSDK()
	var hasDeadline bool
	sdk.Register(Desc{Name: "noTimeoutJob"}, func(ctx Context) error {
		_, hasDeadline = ctx.Deadline()
		return nil
	})

	err := execHandler(t, sdk, "noTimeoutJob", context.Background(), "")
	require.NoError(t, err)
	// Plain background ctx has no deadline, and Desc.Timeout==0 adds none
	assert.False(t, hasDeadline)
}

// ─── Register — panics on invalid input ──────────────────────────────────────

func TestRegister_EmptyName_Panics(t *testing.T) {
	sdk := newTestSDK()
	assert.Panics(t, func() {
		sdk.Register(Desc{Name: ""}, func(ctx Context) error { return nil })
	})
}

func TestRegister_DuplicateName_Panics(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "dupJob"}, func(ctx Context) error { return nil })
	assert.Panics(t, func() {
		sdk.Register(Desc{Name: "dupJob"}, func(ctx Context) error { return nil })
	})
}

// ─── RegisterHandler — struct-style ──────────────────────────────────────────

// fakeHandler is a minimal JobHandler for testing.
type fakeHandler struct {
	name    string
	timeout time.Duration
	execFn  func(ctx Context) error
}

func (h *fakeHandler) Desc() Desc {
	return Desc{Name: h.name, Timeout: h.timeout, Retry: 2}
}

func (h *fakeHandler) Execute(ctx Context) error {
	if h.execFn != nil {
		return h.execFn(ctx)
	}
	return nil
}

func TestRegisterHandler_StructHandler_ExecutedCorrectly(t *testing.T) {
	var called bool
	sdk := newTestSDK()
	sdk.RegisterHandler(&fakeHandler{
		name: "structJob",
		execFn: func(ctx Context) error {
			called = true
			return nil
		},
	})

	err := execHandler(t, sdk, "structJob", context.Background(), "")
	require.NoError(t, err)
	assert.True(t, called)
}

func TestRegisterHandler_EmptyName_Panics(t *testing.T) {
	sdk := newTestSDK()
	assert.Panics(t, func() {
		sdk.RegisterHandler(&fakeHandler{name: ""})
	})
}

func TestRegisterHandler_BindAndMetadata(t *testing.T) {
	type Params struct{ Value int `json:"value"` }
	sdk := newTestSDK()
	sdk.RegisterHandler(&fakeHandler{
		name: "structBindJob",
		execFn: func(ctx Context) error {
			var p Params
			require.NoError(t, ctx.Bind(&p))
			assert.Equal(t, 99, p.Value)
			ctx.Log("value=%d", p.Value)
			return nil
		},
	})

	err := execHandler(t, sdk, "structBindJob", jcCtx(1, 1, 0, 0), `{"value":99}`)
	require.NoError(t, err)
}

func TestRegisterHandler_PanicRecovery_WorksForStructHandlers(t *testing.T) {
	sdk := newTestSDK()
	sdk.RegisterHandler(&fakeHandler{
		name:   "panicStructJob",
		execFn: func(ctx Context) error { panic("struct handler panic") },
	})

	err := execHandler(t, sdk, "panicStructJob", context.Background(), "")
	require.True(t, IsPanic(err))
}

// ─── DescOf ───────────────────────────────────────────────────────────────────

func TestDescOf_RegisteredHandler_ReturnsDesc(t *testing.T) {
	sdk := newTestSDK()
	d := Desc{Name: "myJob", Timeout: 30 * time.Second, Retry: 3}
	sdk.Register(d, func(ctx Context) error { return nil })

	got, ok := sdk.DescOf("myJob")
	require.True(t, ok)
	assert.Equal(t, "myJob", got.Name)
	assert.Equal(t, 30*time.Second, got.Timeout)
	assert.Equal(t, 3, got.Retry)
}

func TestDescOf_UnknownHandler_ReturnsFalse(t *testing.T) {
	sdk := newTestSDK()
	_, ok := sdk.DescOf("doesNotExist")
	assert.False(t, ok)
}

// ─── Names ────────────────────────────────────────────────────────────────────

func TestNames_ReturnsAllRegistered(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "jobA"}, func(ctx Context) error { return nil })
	sdk.Register(Desc{Name: "jobB"}, func(ctx Context) error { return nil })
	sdk.RegisterHandler(&fakeHandler{name: "jobC"})

	names := sdk.Names()
	assert.ElementsMatch(t, []string{"jobA", "jobB", "jobC"}, names)
}

func TestNames_Empty_ReturnsEmptySlice(t *testing.T) {
	sdk := newTestSDK()
	assert.Empty(t, sdk.Names())
}

// ─── Registry passthrough ─────────────────────────────────────────────────────

func TestRegistry_ReturnsSameRegistry(t *testing.T) {
	reg := executor.NewRegistry()
	sdk := New(reg)
	assert.Same(t, reg, sdk.Registry())
}

// ─── Integration: full execution pipeline ─────────────────────────────────────

// orderHandler is a real JobHandler used in the full pipeline integration test.
type orderHandler struct {
	processedOrders []orderParams
	mu              sync.Mutex
}

type orderParams struct {
	OrderID   string  `json:"order_id"`
	Amount    float64 `json:"amount"`
	UserEmail string  `json:"user_email"`
}

func (h *orderHandler) Desc() Desc {
	return Desc{Name: "processOrder", Timeout: 60 * time.Second, Retry: 2}
}

func (h *orderHandler) Execute(ctx Context) error {
	var p orderParams
	if err := ctx.Bind(&p); err != nil {
		return err
	}
	ctx.Log("processing order %s for %s", p.OrderID, p.UserEmail)
	ctx.Log("amount: %.2f, shard %d/%d", p.Amount, ctx.ShardingIndex(), ctx.ShardingTotal())
	h.mu.Lock()
	h.processedOrders = append(h.processedOrders, p)
	h.mu.Unlock()
	return nil
}

// TestIntegration_FullPipeline exercises the SDK end-to-end with a real
// JobHandler struct: Bind + Log + sharding metadata + Desc introspection.
func TestIntegration_FullPipeline(t *testing.T) {
	handler := &orderHandler{}
	sdk := newTestSDK()
	sdk.RegisterHandler(handler)

	param := `{"order_id":"ORD-001","amount":99.50,"user_email":"bob@example.com"}`
	ctx := jcCtx(10, 100, 1, 3)

	err := execHandler(t, sdk, "processOrder", ctx, param)
	require.NoError(t, err)

	handler.mu.Lock()
	defer handler.mu.Unlock()
	require.Len(t, handler.processedOrders, 1)
	assert.Equal(t, "ORD-001", handler.processedOrders[0].OrderID)
	assert.InDelta(t, 99.50, handler.processedOrders[0].Amount, 0.001)
	assert.Equal(t, "bob@example.com", handler.processedOrders[0].UserEmail)

	// Verify Desc metadata stored correctly
	d, ok := sdk.DescOf("processOrder")
	require.True(t, ok)
	assert.Equal(t, 60*time.Second, d.Timeout)
	assert.Equal(t, 2, d.Retry)
}

// TestIntegration_ConcurrentRegistrationsAndExecution verifies the SDK is
// safe under concurrent Register + handler execution.
func TestIntegration_ConcurrentRegistrationsAndExecution(t *testing.T) {
	const N = 20
	sdk := newTestSDK()
	var execCount int64

	// Register N handlers upfront (sequential, safe)
	for i := 0; i < N; i++ {
		name := fmt.Sprintf("concJob%d", i)
		sdk.Register(Desc{Name: name}, func(ctx Context) error {
			atomic.AddInt64(&execCount, 1)
			return nil
		})
	}

	// Execute all N handlers concurrently
	var wg sync.WaitGroup
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			name := fmt.Sprintf("concJob%d", idx)
			h, ok := sdk.Registry().Get(name)
			if !ok {
				return
			}
			h(context.Background(), "")
		}(i)
	}
	wg.Wait()

	assert.Equal(t, int64(N), atomic.LoadInt64(&execCount))
}

// TestIntegration_BindWithNestedJSON verifies complex nested structures.
func TestIntegration_BindWithNestedJSON(t *testing.T) {
	type Address struct {
		Street string `json:"street"`
		City   string `json:"city"`
	}
	type User struct {
		Name    string  `json:"name"`
		Age     int     `json:"age"`
		Address Address `json:"address"`
		Tags    []string `json:"tags"`
	}

	sdk := newTestSDK()
	sdk.Register(Desc{Name: "nestedJob"}, func(ctx Context) error {
		var u User
		require.NoError(t, ctx.Bind(&u))
		assert.Equal(t, "Alice", u.Name)
		assert.Equal(t, 30, u.Age)
		assert.Equal(t, "Baker St", u.Address.Street)
		assert.Equal(t, "London", u.Address.City)
		assert.Equal(t, []string{"admin", "user"}, u.Tags)
		return nil
	})

	param := `{
		"name":"Alice","age":30,
		"address":{"street":"Baker St","city":"London"},
		"tags":["admin","user"]
	}`
	err := execHandler(t, sdk, "nestedJob", context.Background(), param)
	require.NoError(t, err)
}

// TestIntegration_HandlerReturnsError verifies error propagation.
func TestIntegration_HandlerReturnsError(t *testing.T) {
	expectedErr := errors.New("downstream service unavailable")
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "errorJob"}, func(ctx Context) error {
		return expectedErr
	})

	err := execHandler(t, sdk, "errorJob", context.Background(), "")
	assert.Equal(t, expectedErr, err)
}

// TestIntegration_MultipleHandlersInOneSDK ensures different handlers on
// the same SDK don't interfere.
func TestIntegration_MultipleHandlersInOneSDK(t *testing.T) {
	sdk := newTestSDK()
	var countA, countB int64

	sdk.Register(Desc{Name: "handlerA"}, func(ctx Context) error {
		atomic.AddInt64(&countA, 1)
		return nil
	})
	sdk.Register(Desc{Name: "handlerB"}, func(ctx Context) error {
		atomic.AddInt64(&countB, 1)
		return nil
	})

	for i := 0; i < 5; i++ {
		execHandler(t, sdk, "handlerA", context.Background(), "")
	}
	for i := 0; i < 3; i++ {
		execHandler(t, sdk, "handlerB", context.Background(), "")
	}

	assert.Equal(t, int64(5), atomic.LoadInt64(&countA))
	assert.Equal(t, int64(3), atomic.LoadInt64(&countB))
}

// TestIntegration_JSONValidation verifies json.Valid behaviour boundaries.
func TestIntegration_Bind_RawMessagePassthrough(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "rawMsgJob"}, func(ctx Context) error {
		var rm json.RawMessage
		require.NoError(t, ctx.Bind(&rm))
		assert.True(t, json.Valid(rm))
		// Verify content
		var m map[string]interface{}
		require.NoError(t, json.Unmarshal(rm, &m))
		assert.Equal(t, "world", m["hello"])
		return nil
	})

	err := execHandler(t, sdk, "rawMsgJob", context.Background(), `{"hello":"world"}`)
	require.NoError(t, err)
}

// TestIntegration_ContextValuesPassThrough ensures values set on the outer
// context reach the handler (e.g., tracing spans).
func TestIntegration_ContextValuesPassThrough(t *testing.T) {
	type traceKey struct{}
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "traceJob"}, func(ctx Context) error {
		v := ctx.Value(traceKey{})
		require.NotNil(t, v)
		assert.Equal(t, "trace-id-123", v.(string))
		return nil
	})

	ctx := context.WithValue(context.Background(), traceKey{}, "trace-id-123")
	err := execHandler(t, sdk, "traceJob", ctx, "")
	require.NoError(t, err)
}

// TestIntegration_LogLinesAccessibleAfterPanic verifies that even when a
// handler panics, collected log lines are not lost (recovery path captures them).
func TestIntegration_LogLinesInContext_PreservedBeforePanic(t *testing.T) {
	// We can't access ctx.LogLines() after execution since the context is
	// internal to the handler, but we can verify that panic recovery works
	// alongside logging by checking the final error.
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "logThenPanicJob"}, func(ctx Context) error {
		ctx.Log("checkpoint 1")
		ctx.Log("checkpoint 2")
		panic("boom after logging")
	})

	err := execHandler(t, sdk, "logThenPanicJob", context.Background(), "")
	require.True(t, IsPanic(err))
	assert.Contains(t, err.Error(), "boom after logging")
}

// ─── Edge cases ───────────────────────────────────────────────────────────────

func TestBind_ArrayJSON_Works(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "arrayJob"}, func(ctx Context) error {
		var items []string
		require.NoError(t, ctx.Bind(&items))
		assert.Equal(t, []string{"a", "b", "c"}, items)
		return nil
	})
	err := execHandler(t, sdk, "arrayJob", context.Background(), `["a","b","c"]`)
	require.NoError(t, err)
}

func TestBind_NullJSON_HandledGracefully(t *testing.T) {
	sdk := newTestSDK()
	sdk.Register(Desc{Name: "nullJob"}, func(ctx Context) error {
		var m map[string]interface{}
		// json.Unmarshal of `null` into a map sets it to nil — that's fine
		err := ctx.Bind(&m)
		require.NoError(t, err)
		assert.Nil(t, m)
		return nil
	})
	err := execHandler(t, sdk, "nullJob", context.Background(), `null`)
	require.NoError(t, err)
}

func TestPanicError_Message_ContainsHandler(t *testing.T) {
	err := &panicError{v: fmt.Errorf("db connection lost")}
	assert.Contains(t, err.Error(), "jobsdk: handler panic")
	assert.Contains(t, err.Error(), "db connection lost")
}

func TestDescOf_AfterRegisterHandler_ContainsRetry(t *testing.T) {
	sdk := newTestSDK()
	sdk.RegisterHandler(&fakeHandler{name: "retryJob", timeout: 10 * time.Second})

	d, ok := sdk.DescOf("retryJob")
	require.True(t, ok)
	assert.Equal(t, 2, d.Retry) // fakeHandler hardcodes Retry: 2
	assert.Equal(t, 10*time.Second, d.Timeout)
}

// TestBind_DeepEqualRoundtrip verifies that marshalling and binding a struct
// preserves all fields exactly.
func TestBind_DeepEqualRoundtrip(t *testing.T) {
	type Deep struct {
		ID     int64             `json:"id"`
		Labels map[string]string `json:"labels"`
		Nested struct {
			Score float64 `json:"score"`
		} `json:"nested"`
	}

	original := Deep{
		ID:     12345,
		Labels: map[string]string{"env": "prod", "region": "us-east-1"},
	}
	original.Nested.Score = 9.87

	raw, err := json.Marshal(original)
	require.NoError(t, err)

	sdk := newTestSDK()
	sdk.Register(Desc{Name: "roundtripJob"}, func(ctx Context) error {
		var got Deep
		require.NoError(t, ctx.Bind(&got))
		assert.Equal(t, original.ID, got.ID)
		assert.Equal(t, original.Labels, got.Labels)
		assert.InDelta(t, original.Nested.Score, got.Nested.Score, 0.001)
		return nil
	})

	err = execHandler(t, sdk, "roundtripJob", context.Background(), string(raw))
	require.NoError(t, err)
}
