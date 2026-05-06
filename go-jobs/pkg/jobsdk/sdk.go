package jobsdk

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jiujuan/go-jobs/internal/executor"
)

// ─── SDK ──────────────────────────────────────────────────────────────────────

// SDK is the decorator-style handler registration facade.
// It owns a *executor.Registry and wraps every registered handler with:
//   - rich Context creation (Bind, Log, metadata)
//   - optional per-handler timeout tightening
//   - automatic panic recovery (default on)
//
// Usage:
//
//	sdk := jobsdk.New(executor.NewRegistry())
//	sdk.Register(jobsdk.Desc{Name: "myJob"}, myHandlerFunc)
//	sdk.RegisterHandler(&MyStructHandler{})
//	runner := executor.NewRunner(sdk.Registry())
type SDK struct {
	reg  *executor.Registry
	mu   sync.RWMutex
	desc map[string]Desc // name → descriptor
}

// New creates an SDK backed by the given registry.
// The registry is typically created with executor.NewRegistry() and later
// passed to executor.NewRunner().
func New(reg *executor.Registry) *SDK {
	return &SDK{
		reg:  reg,
		desc: make(map[string]Desc),
	}
}

// Registry returns the underlying *executor.Registry.
// Pass this to executor.NewRunner() after all handlers are registered.
func (s *SDK) Registry() *executor.Registry { return s.reg }

// ─── Register — function-style ─────────────────────────────────────────────────

// Register binds a HandlerFunc to the given Desc.
// Panics if d.Name is empty or already registered.
func (s *SDK) Register(d Desc, fn HandlerFunc) {
	if d.Name == "" {
		panic("jobsdk: Register called with empty Desc.Name")
	}
	s.storeDesc(d)
	s.reg.Register(d.Name, s.wrapHandler(d, fn))
}

// ─── RegisterHandler — struct-style ──────────────────────────────────────────

// RegisterHandler registers a JobHandler implementation.
// The handler's Desc().Name must be non-empty and not already registered.
func (s *SDK) RegisterHandler(h JobHandler) {
	d := h.Desc()
	if d.Name == "" {
		panic(fmt.Sprintf("jobsdk: RegisterHandler: %T returned empty Desc.Name", h))
	}
	s.storeDesc(d)
	s.reg.Register(d.Name, s.wrapHandler(d, h.Execute))
}

// ─── DescOf ────────────────────────────────────────────────────────────────────

// DescOf returns the Desc for the named handler, and whether it was found.
// Useful for tooling/codegen that wants to read retry/timeout metadata.
func (s *SDK) DescOf(name string) (Desc, bool) {
	s.mu.RLock()
	d, ok := s.desc[name]
	s.mu.RUnlock()
	return d, ok
}

// Names returns all registered handler names in insertion order.
func (s *SDK) Names() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	names := make([]string, 0, len(s.desc))
	for n := range s.desc {
		names = append(names, n)
	}
	return names
}

// ─── Internal ─────────────────────────────────────────────────────────────────

func (s *SDK) storeDesc(d Desc) {
	s.mu.Lock()
	s.desc[d.Name] = d
	s.mu.Unlock()
}

// wrapHandler converts a HandlerFunc into an executor.Handler, adding:
//  1. rich jobContext construction
//  2. optional Desc.Timeout tightening
//  3. optional panic recovery
func (s *SDK) wrapHandler(d Desc, fn HandlerFunc) executor.Handler {
	return func(ctx context.Context, param string) (retErr error) {
		// ── 1. Panic recovery ────────────────────────────────────────────────
		if d.shouldRecover() {
			defer func() {
				if r := recover(); r != nil {
					retErr = &panicError{v: r}
				}
			}()
		}

		// ── 2. Per-handler timeout tightening ────────────────────────────────
		// Only wrap when Desc.Timeout is set AND the incoming ctx has no
		// tighter deadline already (respects scheduler-set timeouts).
		execCtx := ctx
		if d.Timeout > 0 {
			if dl, ok := ctx.Deadline(); !ok || time.Until(dl) > d.Timeout {
				var cancel context.CancelFunc
				execCtx, cancel = context.WithTimeout(ctx, d.Timeout)
				defer cancel()
			}
		}

		// ── 3. Extract JobContext injected by the executor runner ─────────────
		jc, _ := executor.GetJobContext(execCtx)

		// ── 4. Build rich SDK Context ─────────────────────────────────────────
		sdkCtx := newJobContext(execCtx, param, jc)

		// ── 5. Call the user handler ──────────────────────────────────────────
		return fn(sdkCtx)
	}
}
