/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"sync"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// maxPooledNotifyBufferIDs caps the capacity of a notify buffer returned to the pool. A run touching many channels
// grows its buffer, and pooling that buffer would pin its memory for as long as the pool keeps it, so it is dropped
// instead.
const maxPooledNotifyBufferIDs = 4096

// notifyBufferPool holds the buffers runs collect their changed channels in. A buffer goes from a run to the
// completion tracker, which keeps it until the watermark passes the run, and comes back to the pool once its channels
// have been merged into the notify accumulator.
var notifyBufferPool = sync.Pool{
	New: func() any {
		buf := make([]channels.ID, 0, 64)
		return &buf
	},
}

// getNotifyBuffer returns an empty buffer from the pool.
func getNotifyBuffer() *[]channels.ID {
	return notifyBufferPool.Get().(*[]channels.ID)
}

// putNotifyBuffer returns buf to the pool, or drops it if it has grown past maxPooledNotifyBufferIDs. The IDs are
// cleared before truncating, so the pooled buffer doesn't keep channel names alive.
func putNotifyBuffer(buf *[]channels.ID) {
	if buf == nil || cap(*buf) > maxPooledNotifyBufferIDs {
		return
	}
	clear(*buf)
	*buf = (*buf)[:0]
	notifyBufferPool.Put(buf)
}

// dispatchRun is up to runSize entries released by one lock hold, with everything needed to cache them and retire
// them in release order.
type dispatchRun struct {
	ticket            uint64      // Position in release order, from 1. Runs retire in ticket order whatever order they finish in.
	releasedThrough   uint64      // The watermark may move here once this run and every earlier run are cached.
	entries           []*LogEntry // The released entries, in release order.
	removeSkipped     []uint64    // Late sequences among this run's entries, to remove from skippedSeqs once the entries are cached.
	unusedFromSkipped bool        // The hold removed an unused range from skippedSeqs, so the run wakes waiters on the unused key even without an unused entry. Only on a hold's final run.
}

// _dispatchReleased dispatches the entries released during this lock hold, cut into runs by _cutRuns.
//
// The publish cursor is raised to the final releasedThrough before any run is dispatched, so a channel cache created
// concurrently either receives these entries or starts above them (see addChannelCache).
//
// The caller must have limited the release to the hold's budget, so that every run's ticket has room in the tracker.
// Callers must hold c.lock.
//
// TODO: CBG-6120 - hand the runs to consumer goroutines instead of dispatching them here. The runs will then keep
// sub-slices of c.released past the lock hold, so it must be replaced rather than reused.
func (c *changeCache) _dispatchReleased(ctx context.Context) {
	if len(c.released) == 0 && !c.releasedUnusedFromSkipped {
		return
	}

	c.channelCache.advancePublishCursor(c.nextSequence - 1)
	c._cutRuns(func(run dispatchRun) {
		c.dispatchRun(ctx, run)
	})

	// Nil the slots before truncating: truncation alone leaves the backing array pointing at these entries, and the GC
	// would keep them alive until a later hold of the same size overwrote the slots.
	clear(c.released)
	c.released = c.released[:0]
	c.releasedLate = c.releasedLate[:0]
	c.releasedUnusedFromSkipped = false
}

// _cutRuns cuts the entries released during this lock hold into runs of at most runSize, gives each the next ticket,
// and passes them to dispatch in ticket order. An inner run's releasedThrough is its last entry's sequence; the final
// run's is nextSequence-1, so the watermark also covers ranges skipped at the end of the release.
//
// Each late sequence goes on the run that holds it, which removes it from skippedSeqs after its own inserts. No other
// run would do: once runs are cached concurrently, a later run can finish first and remove the sequence before it is
// cached, and the stable sequence, which the watermark doesn't bound below a late sequence, would pass it. Late
// arrivals are rare, so building removeSkipped only for the runs that have one costs ordinary runs nothing.
//
// The final run carries the hold's skipped-range flag. A hold that released nothing but removed an unused range from
// skippedSeqs still gets one empty run, so the wake is sent through the same path as every other. Callers must hold
// c.lock.
func (c *changeCache) _cutRuns(dispatch func(dispatchRun)) {
	numRuns := max(1, (len(c.released)+c.runSize-1)/c.runSize)
	nextLate := 0
	for i := range numRuns {
		end := min((i+1)*c.runSize, len(c.released))
		run := dispatchRun{
			ticket:  c.nextTicket,
			entries: c.released[i*c.runSize : end],
		}
		c.nextTicket++
		for ; nextLate < len(c.releasedLate) && c.releasedLate[nextLate] < end; nextLate++ {
			run.removeSkipped = append(run.removeSkipped, c.released[c.releasedLate[nextLate]].Sequence)
		}
		if i == numRuns-1 {
			run.releasedThrough = c.nextSequence - 1
			run.unusedFromSkipped = c.releasedUnusedFromSkipped
		} else {
			last := run.entries[len(run.entries)-1]
			run.releasedThrough = max(last.Sequence, last.EndSequence)
		}
		dispatch(run)
	}
}

// dispatchRun caches run's entries, collecting the channels they change in a pooled buffer, then retires the run.
// Unused sequences and principals have no channels. An unused entry, or the final run of a hold that removed an
// unused range from skippedSeqs, adds unusedSeqChannelID once, so request_plus waiters are woken when an unused
// sequence becomes readable rather than when it arrives. Late sequences leave skippedSeqs only after the run's
// entries are cached, so the stable sequence never passes an entry before it can be read.
//
// TODO: CBG-6120 - this becomes the consumer's body, and runs without c.lock.
func (c *changeCache) dispatchRun(ctx context.Context, run dispatchRun) {
	buf := getNotifyBuffer()
	notifyUnused := run.unusedFromSkipped
	for _, change := range run.entries {
		if change.UnusedSequence {
			notifyUnused = true
			continue
		}
		if change.IsPrincipal {
			continue
		}
		start := len(*buf)
		*buf = c.channelCache.dispatchEntry(ctx, change, *buf)
		if base.LogDebugEnabled(ctx, base.KeyChanges) {
			base.DebugfCtx(ctx, base.KeyChanges, " #%d ==> channels %v", change.Sequence, base.UD((*buf)[start:]))
		}
		if change.TimeReceived != 0 {
			c.db.DbStats.Database().DCPCachingCount.Add(1)
			c.db.DbStats.Database().DCPCachingTime.Add(change.TimeReceived.Since())
		}
	}
	if notifyUnused {
		*buf = append(*buf, unusedSeqChannelID)
	}

	for _, sequence := range run.removeSkipped {
		if err := c.RemoveSkipped(sequence); err != nil {
			base.DebugfCtx(ctx, base.KeyCache, "Error removing skipped sequence: #%d from cache: %v", sequence, err)
		}
	}

	c.retire(ctx, run.ticket, run.releasedThrough, buf)
}

// retire records that run ticket is cached. If that lets the tracker retire one or more runs, the watermark moves to
// the newest one's releasedThrough first, and only then are their channels merged for notification, so a woken
// changes feed always finds the entries it was woken for. The buffers go back to the pool once merged.
//
// TODO: CBG-6120 - runs on consumer goroutines, each with its own scratch slice in place of c.retiredScratch.
func (c *changeCache) retire(ctx context.Context, ticket, releasedThrough uint64, buf *[]channels.ID) {
	retired, watermark := c.tracker.Complete(ctx, ticket, releasedThrough, buf, c.retiredScratch[:0])
	c.retiredScratch = retired
	if len(retired) == 0 {
		return
	}
	c.channelCache.updateHighCacheSequence(watermark)
	c.notifyAcc.MergeBuffers(retired)
	for _, b := range retired {
		putNotifyBuffer(b)
	}
	clear(retired)
}
