/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package notifyaccumulator collects the channels whose caches have gained entries, so that a single
// periodic drain can notify them all at once instead of every caching step notifying on its own. It lives
// in its own package so that the spare set, which only the drainer may touch, is unreachable from callers.
package notifyaccumulator

import (
	"sync"

	"github.com/couchbase/sync_gateway/channels"
)

// MaxRetainedKeys caps the size of a drained set that is kept for reuse. A Go map keeps all of its memory
// after clear(), about 80 B per key for a channels.ID key, so without a cap one burst of many distinct
// channels would pin that memory for the life of the database. A drained set larger than this is dropped,
// and a merge after the next swap allocates afresh.
//
// The cap has to sit above the number of distinct channels one broadcast tick collects under heavy load,
// or the sets are dropped on every drain and never reused: at ~130K sequences/s, 10 channels per doc over
// 20K channels, one 50 ms tick touches ~19K distinct channels. 64K keys is ~5 MB per set, so the two sets
// retain at most ~10 MB. A workload over the cap falls back to allocating one set per tick, still far
// fewer allocations than notifying per mutation.
const MaxRetainedKeys = 65536

// Accumulator is a double-buffered set of channel IDs. Merges add to the active set; a drain swaps in the
// spare, hands the full set to its callback, then clears it and keeps it as the next spare. In steady state
// the same two maps are reused for the life of the accumulator, so merging and draining don't allocate.
//
// The callback runs outside the merge lock, so merges are never blocked behind a drain's callback, only
// behind the swap. Lock order: drainMu → mu. A caller that holds its own lock across Drain (the change
// listener holds tapNotifier.L) must never take that lock inside a merge.
//
// The zero value is ready to use.
type Accumulator struct {
	mu     sync.Mutex   // guards active
	active channels.Set // receives merges; nil until the first merge after construction or a dropped set

	drainMu sync.Mutex   // serialises drains; guards spare
	spare   channels.Set // the cleared set the next drain swaps in; nil if none is held
}

// Merge adds ids to the active set. An empty slice takes no lock.
func (a *Accumulator) Merge(ids []channels.ID) {
	if len(ids) == 0 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a._add(ids)
}

// MergeBuffers adds every buffer's ids to the active set under one lock hold, so that a caller retiring
// several runs at once pays for one acquisition, not one per run. Nil buffers are skipped. The buffers are
// only read: returning them to a pool is the caller's job, after this returns.
func (a *Accumulator) MergeBuffers(bufs []*[]channels.ID) {
	if !anyIDs(bufs) {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, buf := range bufs {
		if buf != nil {
			a._add(*buf)
		}
	}
}

// _add adds ids to the active set, allocating it if needed. Requires a.mu.
//
// It doesn't use channels.Set.UpdateWithSlice, which replaces an empty set with a new map and would discard
// the cleared spare that Drain just swapped in.
func (a *Accumulator) _add(ids []channels.ID) {
	if a.active == nil {
		// No size hint: ids can repeat a channel many times, so its length can far exceed the distinct count, and a
		// map keeps its size after clear(). Grown from empty, each set reaches its peak distinct count once and keeps
		// that for reuse.
		a.active = make(channels.Set)
	}
	for _, id := range ids {
		a.active.Add(id)
	}
}

// Drain swaps out everything merged since the previous drain and, if it is non-empty, passes it to apply.
// apply must not keep the set, because it is cleared for reuse as soon as apply returns. apply must not
// call Drain (drains are serialised and the lock isn't re-entrant), but may call Merge: such a merge lands
// in the next drain.
//
// Returns whether apply was called.
func (a *Accumulator) Drain(apply func(channels.Set)) bool {
	a.drainMu.Lock()
	defer a.drainMu.Unlock()

	// After the swap, a.spare is nil and no field refers to drained, so this drain owns it outright: merges
	// go to the new active set, and drainMu keeps any other drain out until drained is settled below.
	drained := a.swap()
	delivered := len(drained) > 0 // taken now, because the clear below empties drained
	if delivered {
		apply(drained)
	}
	// Keep drained as the next spare unless it grew too large to be worth pinning. Not storing it leaves
	// a.spare nil, so the GC frees the map, and the next swap installs nil as the active set for a merge to
	// allocate afresh. drained is nil when nothing has been merged since construction or since a dropped
	// set, and an empty but allocated set is kept like any other.
	if drained != nil && len(drained) <= MaxRetainedKeys {
		clear(drained)
		a.spare = drained
	}
	return delivered
}

// swap installs the spare as the active set and returns the set it replaces. Requires a.drainMu.
//
// It sets a.spare to nil so that the spare is never the same map as the active set. Otherwise a drain that
// drops an oversized set, and so doesn't write a.spare back, would leave a.spare pointing at the live active
// set, and the next swap would hand apply a map that merges are still writing to.
func (a *Accumulator) swap() channels.Set {
	a.mu.Lock()
	defer a.mu.Unlock()
	drained := a.active
	a.active = a.spare
	a.spare = nil
	return drained
}

// anyIDs reports whether any buffer holds at least one id.
func anyIDs(bufs []*[]channels.ID) bool {
	for _, buf := range bufs {
		if buf != nil && len(*buf) > 0 {
			return true
		}
	}
	return false
}
