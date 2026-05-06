// Package timewheel implements a hierarchical timing wheel for efficient
// scheduled task dispatch.
//
// # Architecture
//
// A 4-level hierarchy covers spans from 1 ms to ~11.6 days without a heap:
//
//	Level 0 (ms-wheel)  : 256 slots × 1 ms  = 256 ms
//	Level 1 (sec-wheel) : 64  slots × 256 ms = ~16 s
//	Level 2 (min-wheel) : 64  slots × 16 s   = ~17 min
//	Level 3 (hour-wheel): 64  slots × 17 min = ~18 h
//
// Tasks beyond the top level are stored in a pending heap and re-inserted
// when they come within range (cascade). This keeps each level's slot count
// small while still supporting arbitrary far-future times.
//
// # Concurrency
//
// All public methods are goroutine-safe. The internal tick loop runs in a
// single goroutine to avoid lock contention on the slot arrays; external
// callers communicate via channels.
package timewheel

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ─── Constants ────────────────────────────────────────────────────────────────

const (
	// Level sizes and tick durations.
	lvl0Slots    = 256                        // ms wheel
	lvl0Tick     = time.Millisecond           // 1 ms per slot
	lvl1Slots    = 64                         // second wheel
	lvl1Tick     = lvl0Slots * lvl0Tick       // 256 ms
	lvl2Slots    = 64                         // minute wheel
	lvl2Tick     = lvl1Slots * lvl1Tick       // ~16 s
	lvl3Slots    = 64                         // hour wheel
	lvl3Tick     = lvl2Slots * lvl2Tick       // ~17 min
	maxWheelSpan = lvl3Slots * lvl3Tick       // ~18 h

	// Maximum tasks per slot list (hard cap to bound memory).
	maxSlotLen = 100_000
)

// ─── Task ─────────────────────────────────────────────────────────────────────

// Task represents a single scheduled invocation.
type Task struct {
	// ID is a caller-supplied unique identifier. Duplicate IDs overwrite the
	// previous entry (idempotent update).
	ID string

	// FireAt is the absolute wall-clock time at which fn should be called.
	FireAt time.Time

	// fn is called by the wheel goroutine when the task fires. It must not
	// block; use a goroutine or channel for heavy work.
	fn func()

	// cancelled is set atomically to 1 by Cancel.
	cancelled int32

	// level/slot record where the task lives (for O(1) removal from heap).
	level int
	slot  int
}

// Cancel marks the task as cancelled. A cancelled task's fn will not be
// called even if the tick loop encounters it.
func (t *Task) Cancel() { atomic.StoreInt32(&t.cancelled, 1) }

// isCancelled returns true if the task has been cancelled.
func (t *Task) isCancelled() bool { return atomic.LoadInt32(&t.cancelled) == 1 }

// ─── Slot list ────────────────────────────────────────────────────────────────

type slotList struct {
	mu    sync.Mutex
	tasks []*Task
}

func (sl *slotList) add(t *Task) error {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	if len(sl.tasks) >= maxSlotLen {
		return fmt.Errorf("timewheel: slot full (max %d)", maxSlotLen)
	}
	sl.tasks = append(sl.tasks, t)
	return nil
}

// drain removes and returns all tasks, resetting the list.
func (sl *slotList) drain() []*Task {
	sl.mu.Lock()
	defer sl.mu.Unlock()
	out := sl.tasks
	sl.tasks = nil
	return out
}

// ─── Overflow heap (tasks beyond maxWheelSpan) ────────────────────────────────

type heapItem struct {
	task  *Task
	index int // position in heap array (for heap.Fix)
}

type overflowHeap []*heapItem

func (h overflowHeap) Len() int            { return len(h) }
func (h overflowHeap) Less(i, j int) bool  { return h[i].task.FireAt.Before(h[j].task.FireAt) }
func (h overflowHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *overflowHeap) Push(x interface{}) {
	item := x.(*heapItem)
	item.index = len(*h)
	*h = append(*h, item)
}
func (h *overflowHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	item.index = -1
	return item
}

// ─── TimeWheel ────────────────────────────────────────────────────────────────

// TimeWheel is a hierarchical timing wheel that fires registered tasks at
// their scheduled wall-clock time.
type TimeWheel struct {
	// 4-level wheel grids: wheels[level][slot]
	wheels [4][]*slotList

	// Current slot index for each level.
	cursor [4]int

	// Wall-clock time of the current level-0 tick.
	now time.Time

	// Tasks that extend beyond maxWheelSpan.
	overflow     overflowHeap
	overflowMu   sync.Mutex

	// ID → task index for fast cancellation / update.
	index   map[string]*Task
	indexMu sync.RWMutex

	// addCh receives add requests from external callers.
	addCh chan *Task

	// cancelCh receives IDs to cancel.
	cancelCh chan string

	// stopCh is closed to signal shutdown.
	stopCh chan struct{}

	started int32 // atomic flag
	wg      sync.WaitGroup
}

// New creates a TimeWheel whose tick starts at startTime (usually time.Now()).
func New(startTime time.Time) *TimeWheel {
	tw := &TimeWheel{
		index:    make(map[string]*Task),
		addCh:    make(chan *Task, 4096),
		cancelCh: make(chan string, 1024),
		stopCh:   make(chan struct{}),
		now:      startTime.Truncate(lvl0Tick),
	}
	// Allocate slot lists for all levels.
	sizes := [4]int{lvl0Slots, lvl1Slots, lvl2Slots, lvl3Slots}
	for lvl := 0; lvl < 4; lvl++ {
		tw.wheels[lvl] = make([]*slotList, sizes[lvl])
		for s := 0; s < sizes[lvl]; s++ {
			tw.wheels[lvl][s] = &slotList{}
		}
	}
	heap.Init(&tw.overflow)
	return tw
}

// Start launches the tick goroutine. Call once.
func (tw *TimeWheel) Start() {
	if !atomic.CompareAndSwapInt32(&tw.started, 0, 1) {
		return
	}
	tw.wg.Add(1)
	go tw.run()
}

// Stop shuts down the wheel and waits for the tick goroutine to exit.
func (tw *TimeWheel) Stop() {
	if atomic.LoadInt32(&tw.started) == 0 {
		return
	}
	close(tw.stopCh)
	tw.wg.Wait()
}

// Add schedules fn to be called at fireAt. If id already exists, the old
// task is replaced (cancel + re-add). fn must not block.
// Returns the Task handle for manual cancellation.
func (tw *TimeWheel) Add(id string, fireAt time.Time, fn func()) *Task {
	t := &Task{ID: id, FireAt: fireAt, fn: fn}
	select {
	case tw.addCh <- t:
	case <-tw.stopCh:
	}
	return t
}

// Cancel cancels the task with the given ID if it has not yet fired.
func (tw *TimeWheel) Cancel(id string) {
	select {
	case tw.cancelCh <- id:
	case <-tw.stopCh:
	}
}

// ─── Internal tick loop ───────────────────────────────────────────────────────

func (tw *TimeWheel) run() {
	defer tw.wg.Done()

	ticker := time.NewTicker(lvl0Tick)
	defer ticker.Stop()

	for {
		select {
		case <-tw.stopCh:
			return

		case t := <-tw.addCh:
			tw.insert(t)

		case id := <-tw.cancelCh:
			tw.doCancel(id)

		case now := <-ticker.C:
			tw.tick(now)
		}
	}
}

// tick advances the wheel by one lvl0 slot and fires due tasks.
func (tw *TimeWheel) tick(wallNow time.Time) {
	// Drain any pending adds/cancels that arrived before this tick.
	for {
		select {
		case t := <-tw.addCh:
			tw.insert(t)
		case id := <-tw.cancelCh:
			tw.doCancel(id)
		default:
			goto done
		}
	}
done:

	tw.now = wallNow.Truncate(lvl0Tick)

	// Cascade overflow heap into wheel if tasks are now within range.
	tw.cascadeOverflow()

	// Advance level 0 and fire tasks in current slot.
	tw.cursor[0] = (tw.cursor[0] + 1) % lvl0Slots
	tw.fireTasks(tw.wheels[0][tw.cursor[0]])

	// Cascade higher levels on rollover.
	if tw.cursor[0] == 0 {
		tw.cursor[1] = (tw.cursor[1] + 1) % lvl1Slots
		tw.cascade(1, tw.wheels[1][tw.cursor[1]])
		if tw.cursor[1] == 0 {
			tw.cursor[2] = (tw.cursor[2] + 1) % lvl2Slots
			tw.cascade(2, tw.wheels[2][tw.cursor[2]])
			if tw.cursor[2] == 0 {
				tw.cursor[3] = (tw.cursor[3] + 1) % lvl3Slots
				tw.cascade(3, tw.wheels[3][tw.cursor[3]])
			}
		}
	}
}

// fireTasks calls fn for each non-cancelled task in the slot, then clears it.
func (tw *TimeWheel) fireTasks(sl *slotList) {
	tasks := sl.drain()
	for _, t := range tasks {
		if t.isCancelled() {
			continue
		}
		tw.indexMu.Lock()
		delete(tw.index, t.ID)
		tw.indexMu.Unlock()
		go t.fn() // call in goroutine to avoid blocking the tick loop
	}
}

// cascade drains a higher-level slot and re-inserts tasks into their proper
// lower-level position now that time has advanced.
func (tw *TimeWheel) cascade(level int, sl *slotList) {
	tasks := sl.drain()
	for _, t := range tasks {
		if !t.isCancelled() {
			tw.place(t)
		}
	}
}

// cascadeOverflow re-inserts overflow tasks that are now within maxWheelSpan.
func (tw *TimeWheel) cascadeOverflow() {
	tw.overflowMu.Lock()
	defer tw.overflowMu.Unlock()
	horizon := tw.now.Add(maxWheelSpan)
	for tw.overflow.Len() > 0 {
		top := tw.overflow[0]
		if top.task.FireAt.After(horizon) {
			break
		}
		heap.Pop(&tw.overflow)
		if !top.task.isCancelled() {
			tw.place(top.task)
		}
	}
}

// ─── Placement helpers ────────────────────────────────────────────────────────

// insert registers t in the index and places it in the appropriate slot.
func (tw *TimeWheel) insert(t *Task) {
	// Cancel any existing task with the same ID.
	tw.indexMu.Lock()
	if old, ok := tw.index[t.ID]; ok {
		old.Cancel()
	}
	tw.index[t.ID] = t
	tw.indexMu.Unlock()

	tw.place(t)
}

// place puts t in the correct wheel slot (or overflow heap).
func (tw *TimeWheel) place(t *Task) {
	delay := t.FireAt.Sub(tw.now)

	switch {
	case delay <= 0:
		// Already due: fire immediately.
		if !t.isCancelled() {
			tw.indexMu.Lock()
			delete(tw.index, t.ID)
			tw.indexMu.Unlock()
			go t.fn()
		}

	case delay < lvl1Tick:
		// Level 0: ms wheel
		ticks := int(delay/lvl0Tick) + 1
		slot := (tw.cursor[0] + ticks) % lvl0Slots
		t.level, t.slot = 0, slot
		_ = tw.wheels[0][slot].add(t)

	case delay < lvl2Tick:
		// Level 1: second wheel
		ticks := int(delay/lvl1Tick) + 1
		slot := (tw.cursor[1] + ticks) % lvl1Slots
		t.level, t.slot = 1, slot
		_ = tw.wheels[1][slot].add(t)

	case delay < lvl3Tick:
		// Level 2: minute wheel
		ticks := int(delay/lvl2Tick) + 1
		slot := (tw.cursor[2] + ticks) % lvl2Slots
		t.level, t.slot = 2, slot
		_ = tw.wheels[2][slot].add(t)

	case delay < maxWheelSpan:
		// Level 3: hour wheel
		ticks := int(delay/lvl3Tick) + 1
		slot := (tw.cursor[3] + ticks) % lvl3Slots
		t.level, t.slot = 3, slot
		_ = tw.wheels[3][slot].add(t)

	default:
		// Beyond top level: overflow heap.
		tw.overflowMu.Lock()
		heap.Push(&tw.overflow, &heapItem{task: t})
		tw.overflowMu.Unlock()
	}
}

// doCancel marks the task with the given ID as cancelled.
func (tw *TimeWheel) doCancel(id string) {
	tw.indexMu.Lock()
	if t, ok := tw.index[id]; ok {
		t.Cancel()
		delete(tw.index, id)
	}
	tw.indexMu.Unlock()
}

// ─── Stats (for observability) ────────────────────────────────────────────────

// Stats holds diagnostic counters.
type Stats struct {
	IndexSize    int
	OverflowSize int
}

// Stats returns a snapshot of the wheel's internal state.
func (tw *TimeWheel) Stats(ctx context.Context) Stats {
	tw.indexMu.RLock()
	idxLen := len(tw.index)
	tw.indexMu.RUnlock()
	tw.overflowMu.Lock()
	ovfLen := tw.overflow.Len()
	tw.overflowMu.Unlock()
	return Stats{IndexSize: idxLen, OverflowSize: ovfLen}
}
