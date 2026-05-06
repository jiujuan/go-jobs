// Package jobstore provides an in-memory store for scheduled job configurations.
//
// # Design
//
//   - A sync.Map (jobByID) gives O(1) amortised reads for arbitrary ID lookups
//     and concurrent writes without a global write lock.
//   - A min-heap (nextHeap) ordered by NextTriggerTime enables the scheduler to
//     find all jobs due within any lookahead window in O(k log n) time, where k
//     is the number of due jobs and n is the total count.
//   - The heap is protected by a single mutex (heapMu). The common case
//     (reads via sync.Map) does not touch heapMu at all.
//   - MySQL is used only for bootstrap (LoadAll) and crash-recovery.  All
//     runtime mutations go to the store first; a background goroutine
//     periodically flushes dirty records to MySQL.
package jobstore

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jiujuan/go-jobs/internal/model"
	"go.uber.org/zap"
)

// ─── Logger interface (injected to avoid circular import) ────────────────────

// Logger is a minimal logging interface used by the store.
type Logger interface {
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
}

// ─── Entry ────────────────────────────────────────────────────────────────────

// Entry wraps a JobInfo with store-specific metadata.
type Entry struct {
	job     *model.JobInfo
	heapIdx int // position in the min-heap (-1 if not in heap)
	dirty   int32 // 1 = needs flush to MySQL
}

func (e *Entry) Job() *model.JobInfo { return e.job }

// ─── Min-heap of *Entry ordered by NextTriggerTime ───────────────────────────

type jobHeap []*Entry

func (h jobHeap) Len() int { return len(h) }
func (h jobHeap) Less(i, j int) bool {
	ti := h[i].job.NextTriggerTime
	tj := h[j].job.NextTriggerTime
	switch {
	case ti == nil && tj == nil:
		return false
	case ti == nil:
		return false
	case tj == nil:
		return true
	default:
		return ti.Before(*tj)
	}
}
func (h jobHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIdx = i
	h[j].heapIdx = j
}
func (h *jobHeap) Push(x interface{}) {
	e := x.(*Entry)
	e.heapIdx = len(*h)
	*h = append(*h, e)
}
func (h *jobHeap) Pop() interface{} {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.heapIdx = -1
	return e
}

// ─── Store ────────────────────────────────────────────────────────────────────

// Store is a thread-safe in-memory job registry.
type Store struct {
	// jobByID holds *Entry values; keys are int64 job IDs (stored as int64).
	jobByID sync.Map

	heapMu sync.Mutex
	heap   jobHeap

	// count tracks the number of active (non-deleted) entries.
	count int64

	log Logger
}

// New creates an empty Store with the given logger.
func New(log Logger) *Store {
	s := &Store{log: log}
	heap.Init(&s.heap)
	return s
}

// ─── CRUD ─────────────────────────────────────────────────────────────────────

// Add inserts or fully replaces a job entry. Safe to call concurrently.
func (s *Store) Add(job *model.JobInfo) {
	e := &Entry{job: copyJob(job), heapIdx: -1}
	atomic.StoreInt32(&e.dirty, 1)

	if old, loaded := s.jobByID.Swap(job.ID, e); loaded {
		s.removeFromHeap(old.(*Entry))
		// Do not decrement count – we're replacing not adding.
	} else {
		atomic.AddInt64(&s.count, 1)
	}

	if job.Status == model.JobRun && job.NextTriggerTime != nil {
		s.addToHeap(e)
	}
}

// Remove deletes a job from the store. No-op if id does not exist.
func (s *Store) Remove(id int64) {
	if v, ok := s.jobByID.LoadAndDelete(id); ok {
		s.removeFromHeap(v.(*Entry))
		atomic.AddInt64(&s.count, -1)
	}
}

// Get returns the job with the given id, or (nil, false) if not found.
func (s *Store) Get(id int64) (*model.JobInfo, bool) {
	if v, ok := s.jobByID.Load(id); ok {
		return copyJob(v.(*Entry).job), true
	}
	return nil, false
}

// UpdateNextTrigger sets the next and last trigger times for job id and
// re-positions the entry in the heap. Returns an error if id is not found.
func (s *Store) UpdateNextTrigger(id int64, next, last time.Time) error {
	v, ok := s.jobByID.Load(id)
	if !ok {
		return fmt.Errorf("jobstore: job %d not found", id)
	}
	e := v.(*Entry)

	s.heapMu.Lock()
	e.job.NextTriggerTime = &next
	e.job.LastTriggerTime = &last
	atomic.StoreInt32(&e.dirty, 1)

	if e.heapIdx >= 0 {
		heap.Fix(&s.heap, e.heapIdx)
	} else if e.job.Status == model.JobRun {
		heap.Push(&s.heap, e)
	}
	s.heapMu.Unlock()
	return nil
}

// UpdateStatus enables or disables a job. When disabled the entry is removed
// from the heap so it will never be selected as pending.
func (s *Store) UpdateStatus(id int64, status model.JobStatus) error {
	v, ok := s.jobByID.Load(id)
	if !ok {
		return fmt.Errorf("jobstore: job %d not found", id)
	}
	e := v.(*Entry)

	s.heapMu.Lock()
	e.job.Status = status
	atomic.StoreInt32(&e.dirty, 1)
	if status == model.JobStop && e.heapIdx >= 0 {
		heap.Remove(&s.heap, e.heapIdx)
		e.heapIdx = -1
	} else if status == model.JobRun && e.heapIdx < 0 && e.job.NextTriggerTime != nil {
		heap.Push(&s.heap, e)
	}
	s.heapMu.Unlock()
	return nil
}

// ─── Scheduler-facing queries ─────────────────────────────────────────────────

// PeekDue returns up to limit jobs whose NextTriggerTime ≤ deadline, without
// removing them from the heap. Jobs are returned in ascending trigger order.
// This is O(k log k) where k is the number of due jobs.
func (s *Store) PeekDue(deadline time.Time, limit int) []*model.JobInfo {
	s.heapMu.Lock()
	defer s.heapMu.Unlock()

	var result []*model.JobInfo
	for s.heap.Len() > 0 && len(result) < limit {
		top := s.heap[0]
		if top.job.NextTriggerTime == nil || top.job.NextTriggerTime.After(deadline) {
			break
		}
		if top.job.Status != model.JobRun {
			// Stale entry (status changed after enqueue); remove and skip.
			heap.Pop(&s.heap)
			continue
		}
		result = append(result, copyJob(top.job))
		// Pop and immediately re-push so we can scan the rest.
		// (We re-add them after the loop using PopDue.)
		heap.Pop(&s.heap)
	}
	// Re-insert the ones we popped (they still need to fire).
	// The caller should call PopDue when it actually fires the jobs.
	for _, j := range result {
		if v, ok := s.jobByID.Load(j.ID); ok {
			e := v.(*Entry)
			if e.heapIdx < 0 {
				heap.Push(&s.heap, e)
			}
		}
	}
	return result
}

// PopDue removes and returns all jobs whose NextTriggerTime ≤ deadline.
// Use this when the scheduler is about to fire the jobs and will call
// UpdateNextTrigger to re-schedule them.
func (s *Store) PopDue(deadline time.Time, limit int) []*model.JobInfo {
	s.heapMu.Lock()
	defer s.heapMu.Unlock()

	var result []*model.JobInfo
	for s.heap.Len() > 0 && len(result) < limit {
		top := s.heap[0]
		if top.job.NextTriggerTime == nil || top.job.NextTriggerTime.After(deadline) {
			break
		}
		heap.Pop(&s.heap) // sets heapIdx = -1
		if top.job.Status != model.JobRun {
			continue
		}
		result = append(result, copyJob(top.job))
	}
	return result
}

// Len returns the number of active jobs in the store.
func (s *Store) Len() int64 { return atomic.LoadInt64(&s.count) }

// HeapLen returns the number of jobs currently in the trigger heap.
func (s *Store) HeapLen() int {
	s.heapMu.Lock()
	defer s.heapMu.Unlock()
	return s.heap.Len()
}

// Range calls fn for every entry in the store. Iteration stops early if fn
// returns false. Order is not guaranteed.
func (s *Store) Range(fn func(job *model.JobInfo) bool) {
	s.jobByID.Range(func(_, v interface{}) bool {
		return fn(copyJob(v.(*Entry).job))
	})
}

// ─── Dirty-flush helpers ──────────────────────────────────────────────────────

// DrainDirty collects and returns all entries that need to be flushed to MySQL,
// clearing their dirty flag. Intended to be called by the background flush loop.
func (s *Store) DrainDirty() []*model.JobInfo {
	var out []*model.JobInfo
	s.jobByID.Range(func(_, v interface{}) bool {
		e := v.(*Entry)
		if atomic.CompareAndSwapInt32(&e.dirty, 1, 0) {
			out = append(out, copyJob(e.job))
		}
		return true
	})
	return out
}

// MarkDirty marks the job as needing a flush. Call this if you mutate the job
// outside of the store's own methods.
func (s *Store) MarkDirty(id int64) {
	if v, ok := s.jobByID.Load(id); ok {
		atomic.StoreInt32(&v.(*Entry).dirty, 1)
	}
}

// ─── Bootstrap ───────────────────────────────────────────────────────────────

// LoadAll bulk-loads jobs from a slice (typically read from MySQL at startup).
// Existing entries are replaced.
func (s *Store) LoadAll(jobs []*model.JobInfo) {
	for _, j := range jobs {
		// On bootstrap we don't mark dirty – they already exist in MySQL.
		e := &Entry{job: copyJob(j), heapIdx: -1, dirty: 0}
		s.jobByID.Store(j.ID, e)
		atomic.AddInt64(&s.count, 1)
		if j.Status == model.JobRun && j.NextTriggerTime != nil {
			s.addToHeap(e)
		}
	}
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

func (s *Store) addToHeap(e *Entry) {
	s.heapMu.Lock()
	if e.heapIdx < 0 {
		heap.Push(&s.heap, e)
	}
	s.heapMu.Unlock()
}

func (s *Store) removeFromHeap(e *Entry) {
	s.heapMu.Lock()
	if e.heapIdx >= 0 {
		heap.Remove(&s.heap, e.heapIdx)
		e.heapIdx = -1
	}
	s.heapMu.Unlock()
}

// copyJob returns a shallow copy of j so mutations by callers don't affect the
// store's canonical record.
func copyJob(j *model.JobInfo) *model.JobInfo {
	c := *j
	if j.NextTriggerTime != nil {
		t := *j.NextTriggerTime
		c.NextTriggerTime = &t
	}
	if j.LastTriggerTime != nil {
		t := *j.LastTriggerTime
		c.LastTriggerTime = &t
	}
	return &c
}

// ─── Background flush loop ────────────────────────────────────────────────────

// Flusher is the interface the store uses to persist dirty jobs back to MySQL.
type Flusher interface {
	// FlushJob updates or inserts a single job record in MySQL.
	FlushJob(ctx context.Context, job *model.JobInfo) error
}

// StartFlushLoop launches a goroutine that periodically flushes dirty entries
// to MySQL. Call Stop on the returned context-cancel to shut it down cleanly.
func (s *Store) StartFlushLoop(ctx context.Context, interval time.Duration, flusher Flusher) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Final flush before exit.
				s.flush(context.Background(), flusher)
				return
			case <-ticker.C:
				s.flush(ctx, flusher)
			}
		}
	}()
}

func (s *Store) flush(ctx context.Context, flusher Flusher) {
	dirty := s.DrainDirty()
	for _, j := range dirty {
		if err := flusher.FlushJob(ctx, j); err != nil {
			s.log.Warn("jobstore: flush failed",
				zap.Int64("jobID", j.ID),
				zap.Error(err))
			// Re-mark dirty so we retry next cycle.
			s.MarkDirty(j.ID)
		}
	}
}
