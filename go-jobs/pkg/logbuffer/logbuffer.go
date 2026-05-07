// Package logbuffer provides a write-buffer for JobLog INSERT operations.
//
// # Problem
//
// The scheduler calls logDAO.Create() synchronously on every trigger.  At high
// frequency (e.g. 100 jobs firing per second) this generates 100 individual
// INSERT statements per second, each holding a DB connection for ~1 ms, which
// quickly exhausts the connection pool and adds scheduling latency.
//
// # Solution
//
// Buffer collects incoming JobLog records in an in-memory ring buffer and
// flushes them to the database in one bulk INSERT either when the buffer
// reaches batchSize entries or when the flush interval elapses — whichever
// comes first.
//
// # ID back-fill guarantee
//
// MySQL's INSERT...RETURNING / LastInsertId can only return the first auto-
// increment ID for a multi-row INSERT, so back-filling IDs for the caller is
// done by assigning a monotonically-increasing temporary negative ID (-1, -2,
// …) at enqueue time.  After the bulk INSERT succeeds, the real IDs assigned
// by the DB are read back and an ID mapping (tmpID → realID) is maintained so
// that callers who do UpdateResult(tmpID, …) transparently operate on the
// correct row.
//
// # Caller contract
//
// Create() enqueues the record and blocks until the flush goroutine has
// written it to the DB and back-filled l.ID with the real auto-increment
// value.  From the caller's perspective Create() behaves identically to a
// direct INSERT — it returns only after the row exists in the database and
// l.ID is valid.  The performance gain comes from the fact that many
// concurrent Create() calls are coalesced into one multi-row INSERT.
//
// UpdateResult() resolves any pending tmpID→realID mapping before delegating
// to the underlying DAO, so it always operates on the correct DB row.
//
// All other JobLogDAO methods are forwarded directly to the underlying DAO.
package logbuffer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/jiujuan/go-jobs/internal/dao"
	"github.com/jiujuan/go-jobs/internal/model"
	"github.com/jiujuan/go-jobs/pkg/logger"
)

// ─── tunable constants ────────────────────────────────────────────────────────

const (
	// DefaultBatchSize is the number of records that trigger an immediate flush.
	DefaultBatchSize = 500

	// DefaultFlushInterval is the maximum time a record can wait before being
	// flushed even if the batch is not full.
	DefaultFlushInterval = 2 * time.Second

	// DefaultRingCap is the capacity of the in-memory ring buffer channel.
	// At DefaultBatchSize=500 and peak 10 000 triggers/s the channel allows
	// 2 batches worth of headroom before back-pressure hits Create().
	DefaultRingCap = DefaultBatchSize * 4
)

// ─── pending record ───────────────────────────────────────────────────────────

// pendingEntry couples an in-flight JobLog with the channel the flush goroutine
// uses to signal completion (and deliver the real DB id back to the caller).
type pendingEntry struct {
	log  *model.JobLog
	done chan error // closed (or sent err) by flush goroutine after INSERT
}

// ─── Options ─────────────────────────────────────────────────────────────────

// Option is a functional option for Buffer.
type Option func(*Buffer)

// WithBatchSize overrides the default batch size.
func WithBatchSize(n int) Option { return func(b *Buffer) { b.batchSize = n } }

// WithFlushInterval overrides the default flush interval.
func WithFlushInterval(d time.Duration) Option { return func(b *Buffer) { b.flushInterval = d } }

// WithRingCap overrides the channel capacity.
func WithRingCap(n int) Option { return func(b *Buffer) { b.ringCap = n } }

// ─── Buffer ───────────────────────────────────────────────────────────────────

// Buffer is a buffered write layer that implements dao.JobLogDAO.
// It collects JobLog records and flushes them in batches.
type Buffer struct {
	inner dao.JobLogDAO

	batchSize     int
	flushInterval time.Duration
	ringCap       int

	// ring is the inbound channel for pending records.
	ring chan *pendingEntry

	// tmpSeq generates negative temporary IDs (−1, −2, …).
	tmpSeq atomic.Int64

	// idMap maps tmpID → realID once the batch has been written.
	idMap   map[int64]int64
	idMapMu sync.RWMutex

	stopCh chan struct{}
	stopWg sync.WaitGroup
}

// New creates a Buffer wrapping inner.  Call Start() before use and Stop()
// during graceful shutdown.
func New(inner dao.JobLogDAO, opts ...Option) *Buffer {
	b := &Buffer{
		inner:         inner,
		batchSize:     DefaultBatchSize,
		flushInterval: DefaultFlushInterval,
		ringCap:       DefaultRingCap,
		idMap:         make(map[int64]int64),
		stopCh:        make(chan struct{}),
	}
	for _, o := range opts {
		o(b)
	}
	b.ring = make(chan *pendingEntry, b.ringCap)
	return b
}

// ─── Lifecycle ────────────────────────────────────────────────────────────────

// Start launches the background flush goroutine.  Must be called once before
// Create() is used.
func (b *Buffer) Start() {
	b.stopWg.Add(1)
	go b.flushLoop()
}

// Stop drains the buffer and waits for the flush goroutine to exit.
// Any records still in the ring at shutdown time are flushed synchronously
// before Stop returns, so no data is lost.
func (b *Buffer) Stop() {
	close(b.stopCh)
	b.stopWg.Wait()
}

// ─── flushLoop ────────────────────────────────────────────────────────────────

func (b *Buffer) flushLoop() {
	defer b.stopWg.Done()

	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	batch := make([]*pendingEntry, 0, b.batchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		b.writeBatch(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-b.stopCh:
			// Drain any remaining records in the ring before exiting.
			draining := true
			for draining {
				select {
				case entry := <-b.ring:
					batch = append(batch, entry)
					if len(batch) >= b.batchSize {
						flush()
					}
				default:
					draining = false
				}
			}
			flush()
			return

		case entry := <-b.ring:
			batch = append(batch, entry)
			if len(batch) >= b.batchSize {
				flush()
			}

		case <-ticker.C:
			flush()
		}
	}
}

// ─── writeBatch ───────────────────────────────────────────────────────────────

// writeBatch performs a single bulk INSERT for all records in batch,
// back-fills their IDs, and signals each caller's done channel.
func (b *Buffer) writeBatch(batch []*pendingEntry) {
	if len(batch) == 0 {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// snapshot of tmpIDs before insert so we can build the mapping afterwards
	tmpIDs := make([]int64, len(batch))
	for i, e := range batch {
		tmpIDs[i] = e.log.ID // negative tmp IDs assigned in Create()
		// Reset to 0 so GORM treats it as unset and lets MySQL assign the real ID.
		e.log.ID = 0
	}

	logs := make([]*model.JobLog, len(batch))
	for i, e := range batch {
		logs[i] = e.log
	}

	// Bulk INSERT via the inner DAO's CreateBatch.
	// If the inner DAO does not support bulk, fall back to individual inserts.
	var insertErr error
	if batcher, ok := b.inner.(BatchCreator); ok {
		insertErr = batcher.CreateBatch(ctx, logs)
	} else {
		// Fallback: sequential inserts (still coalesced from the caller's
		// perspective since Create() was called concurrently but flushed here).
		for _, l := range logs {
			if err := b.inner.Create(ctx, l); err != nil {
				insertErr = err
				// On partial failure, mark all remaining as failed.
				break
			}
		}
	}

	if insertErr != nil {
		logger.Error("logbuffer: bulk insert failed",
			zap.Int("batch", len(batch)),
			zap.Error(insertErr))
	}

	// Back-fill tmpID → realID mapping and signal callers.
	// The mapping is pre-populated so that concurrent UpdateResult() calls
	// that arrive before the caller reads l.ID still resolve correctly.
	b.idMapMu.Lock()
	for i, entry := range batch {
		if insertErr == nil {
			realID := logs[i].ID // set by Create/CreateBatch
			tmpID := tmpIDs[i]
			entry.log.ID = realID // back-fill the caller's *model.JobLog
			if tmpID < 0 && realID > 0 {
				b.idMap[tmpID] = realID
			}
		}
		// Signal the caller regardless of success/failure.
		entry.done <- insertErr
		close(entry.done)
	}
	b.idMapMu.Unlock()

	if insertErr == nil {
		logger.Debug("logbuffer: flushed batch",
			zap.Int("count", len(batch)))
	}
}

// ─── resolveID ────────────────────────────────────────────────────────────────

// resolveID converts a temporary negative ID to a real DB ID.
// Returns the input id unchanged if it is already a real (positive) ID.
func (b *Buffer) resolveID(tmpID int64) int64 {
	if tmpID >= 0 {
		return tmpID
	}
	b.idMapMu.RLock()
	realID, ok := b.idMap[tmpID]
	b.idMapMu.RUnlock()
	if ok {
		return realID
	}
	return tmpID
}

// gcIDMap removes stale entries from the id map.
// Called opportunistically to prevent unbounded map growth.
// In practice the map stays small because entries are removed once used.
func (b *Buffer) gcIDMap(used int64) {
	b.idMapMu.Lock()
	delete(b.idMap, used)
	b.idMapMu.Unlock()
}

// ─── dao.JobLogDAO implementation ────────────────────────────────────────────

// Create enqueues the record and blocks until the flush goroutine has written
// it to the database and back-filled l.ID with the real auto-increment value.
//
// The caller cannot distinguish this from a direct INSERT: when Create returns
// without error l.ID is the real DB row id.
func (b *Buffer) Create(ctx context.Context, l *model.JobLog) error {
	// Assign a stable temporary negative ID so writeBatch can track this entry
	// in the idMap.  The real positive ID is written back by writeBatch.
	tmpID := -b.tmpSeq.Add(1) // -1, -2, -3, …
	l.ID = tmpID

	entry := &pendingEntry{
		log:  l,
		done: make(chan error, 1),
	}

	// Enqueue; respect context cancellation and stopCh.
	select {
	case b.ring <- entry:
	case <-ctx.Done():
		l.ID = 0 // reset; caller should treat as error
		return fmt.Errorf("logbuffer: enqueue cancelled: %w", ctx.Err())
	case <-b.stopCh:
		// Buffer is shutting down; fall back to synchronous insert.
		l.ID = 0
		return b.inner.Create(ctx, l)
	}

	// Wait for the flush goroutine to complete the INSERT.
	select {
	case err := <-entry.done:
		if err != nil {
			l.ID = 0
			return fmt.Errorf("logbuffer: flush failed: %w", err)
		}
		// l.ID has been updated to the real DB id by writeBatch.
		return nil
	case <-ctx.Done():
		// The context expired while waiting.  The record is still in the
		// buffer and will be written; we just can't wait for the ID.
		return fmt.Errorf("logbuffer: wait for id cancelled: %w", ctx.Err())
	}
}

// UpdateResult resolves any pending tmpID→realID mapping before delegating.
func (b *Buffer) UpdateResult(
	ctx context.Context,
	id int64,
	status model.LogStatus,
	errMsg string,
	start, end time.Time,
) error {
	realID := b.resolveID(id)
	if realID != id {
		b.gcIDMap(id)
	}
	return b.inner.UpdateResult(ctx, realID, status, errMsg, start, end)
}

// FindByID resolves tmpID before delegating.
func (b *Buffer) FindByID(ctx context.Context, id int64) (*model.JobLog, error) {
	return b.inner.FindByID(ctx, b.resolveID(id))
}

// ListByJob is forwarded directly to the underlying DAO.
func (b *Buffer) ListByJob(ctx context.Context, jobID int64, page, pageSize int) ([]*model.JobLog, int64, error) {
	return b.inner.ListByJob(ctx, jobID, page, pageSize)
}

// ListRunning is forwarded directly to the underlying DAO.
func (b *Buffer) ListRunning(ctx context.Context) ([]*model.JobLog, error) {
	return b.inner.ListRunning(ctx)
}

// ListRetryable is forwarded directly to the underlying DAO.
func (b *Buffer) ListRetryable(ctx context.Context, limit int) ([]*model.JobLog, error) {
	return b.inner.ListRetryable(ctx, limit)
}

// CountRetries is forwarded directly to the underlying DAO.
func (b *Buffer) CountRetries(ctx context.Context, jobID int64, triggerTime time.Time) (int64, error) {
	return b.inner.CountRetries(ctx, jobID, triggerTime)
}

// CreateDetail is forwarded directly to the underlying DAO.
func (b *Buffer) CreateDetail(ctx context.Context, d *model.JobLogDetail) error {
	return b.inner.CreateDetail(ctx, d)
}

// FindDetail is forwarded directly to the underlying DAO.
func (b *Buffer) FindDetail(ctx context.Context, logID int64) (*model.JobLogDetail, error) {
	return b.inner.FindDetail(ctx, logID)
}

// ─── Metrics ─────────────────────────────────────────────────────────────────

// Stats returns a snapshot of the current buffer state for monitoring.
type Stats struct {
	// Pending is the number of records currently waiting in the ring buffer.
	Pending int
	// RingCap is the total ring buffer capacity.
	RingCap int
	// IDMapSize is the number of entries in the tmpID→realID mapping table.
	IDMapSize int
}

// Stats returns a Stats snapshot.
func (b *Buffer) Stats() Stats {
	b.idMapMu.RLock()
	idMapSize := len(b.idMap)
	b.idMapMu.RUnlock()
	return Stats{
		Pending:   len(b.ring),
		RingCap:   b.ringCap,
		IDMapSize: idMapSize,
	}
}

// ─── BatchCreator ─────────────────────────────────────────────────────────────

// BatchCreator is an optional extension of dao.JobLogDAO that supports
// bulk inserts.  When the inner DAO implements this interface the buffer uses
// a single multi-row INSERT instead of N individual inserts.
type BatchCreator interface {
	// CreateBatch inserts multiple JobLog records in one DB round-trip.
	// Implementations must back-fill the ID field of each record.
	CreateBatch(ctx context.Context, logs []*model.JobLog) error
}
