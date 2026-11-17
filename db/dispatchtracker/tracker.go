/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package dispatchtracker holds the completion tracker that turns out-of-order run completions into an
// in-order watermark. It lives in its own package so that the slot storage is unreachable from the change
// cache: the slice is sized once at construction and nothing outside this file can append to it, re-slice
// it or replace it.
package dispatchtracker

import (
	"context"
	"fmt"
	"math/bits"
	"sync"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

const (
	// RunSize is the maximum number of entries in one run. The capacity rules below assume it.
	RunSize = 32

	// MinCapacity is the smallest capacity allowed outside tests, and the default. It keeps the headroom
	// over the handoff queue and consumers that stops normal load from waiting for a free slot.
	MinCapacity = 1024

	// MaxCapacity bounds memory: 24 B of slots per run, and up to RunSize entries in flight per run.
	MaxCapacity = 1 << 16

	// MaxNumPending is the largest max_num_pending that MaxCapacity can hold: a full cascade of it is cut
	// into MaxCapacity-1 runs, one fewer than the capacity, as ValidateCapacity requires.
	MaxNumPending = (MaxCapacity - 1) * RunSize
)

// slot records a completed run until the head walk retires it. A slot is written only when its run
// completes, so slot.ticket == head means the head run is done. Whatever else a slot holds is an older,
// already retired ticket (or zero before the slice first wraps), never a newer one, because the issuing
// rule keeps tickets within capacity of head.
type slot struct {
	ticket          uint64         // 0 = never written; tickets start at 1 so a zeroed slot never matches head
	releasedThrough uint64         // the watermark may move here once this run and every earlier run are done
	changed         *[]channels.ID // the run's notify buffer; nil once retired
}

// Tracker is a fixed-capacity completion tracker. Tickets are issued by the caller in release order,
// starting at 1, and a ticket may only be issued while HasRoom reports true for it.
type Tracker struct {
	mu    sync.Mutex    // guards slots and the head walk
	slots []slot        // len == cap == capacity for the tracker's whole life
	mask  uint64        // capacity - 1; capacity is a power of two
	head  atomic.Uint64 // oldest unretired ticket; stored only under mu (one walker at a time), atomic so HasRoom can read it without mu
}

// NewTracker returns a tracker for production use. The capacity must pass ValidateCapacity's range rules.
func NewTracker(capacity uint64) (*Tracker, error) {
	if err := validateRange(capacity); err != nil {
		return nil, err
	}
	return newTracker(capacity), nil
}

// NewForTest returns a tracker of any power-of-two capacity, so tests can fill and wrap it with a handful of
// tickets.
func NewForTest(capacity uint64) (*Tracker, error) {
	if !isPowerOfTwo(capacity) {
		return nil, fmt.Errorf("capacity %d must be a power of two", capacity)
	}
	return newTracker(capacity), nil
}

func newTracker(capacity uint64) *Tracker {
	// Full slice expression so that an accidental append inside this package reallocates instead of
	// silently extending into spare backing capacity.
	slots := make([]slot, capacity)
	t := &Tracker{
		slots: slots[:capacity:capacity],
		mask:  capacity - 1,
	}
	t.head.Store(1)
	return t
}

// Capacity is the maximum number of tickets that can be outstanding at once.
func (t *Tracker) Capacity() uint64 {
	return t.mask + 1
}

// Head is the oldest ticket not yet retired.
func (t *Tracker) Head() uint64 {
	return t.head.Load()
}

// InFlight is the number of tickets issued but not yet retired, given that next is the next ticket to issue.
func (t *Tracker) InFlight(next uint64) uint64 {
	return next - t.head.Load()
}

// HasRoom reports whether ticket next may be issued. Issuing only while this holds is what guarantees the
// slot for next is free: its previous occupant, next - capacity, is below head and therefore retired. head
// only rises, so a true result stays true until the caller issues more tickets, and a stale read of head
// can only understate the room, never overstate it.
func (t *Tracker) HasRoom(next uint64) bool {
	return t.InFlight(next) < t.Capacity()
}

// Complete records that run ticket has finished, then retires every consecutive finished ticket from head.
// The retired runs' changed buffers are appended to passed (pass a reused scratch slice to avoid allocating)
// in ticket order, and releasedThrough is that of the newest retired ticket. If head did not move, passed
// comes back with nothing appended and releasedThrough is 0.
//
// Only bookkeeping happens under the mutex. Publishing the watermark and merging the buffers is the
// caller's job, after Complete returns.
func (t *Tracker) Complete(ctx context.Context, ticket, releasedThrough uint64, changed *[]channels.ID, passed []*[]channels.ID) ([]*[]channels.ID, uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	head := t.head.Load()
	s := &t.slots[ticket&t.mask]
	// A ticket outside [head, head+capacity) was issued without room or completed twice, and a slot already
	// holding this ticket means a double completion. Writing would overwrite a live record and freeze the
	// watermark, so leave the tracker untouched for the tickets that were issued correctly.
	if ticket < head || ticket-head >= t.Capacity() || s.ticket == ticket {
		base.AssertfCtx(ctx, "completion tracker: ticket %d completed outside the issuing rule (head %d, capacity %d, slot holds %d)", ticket, head, t.Capacity(), s.ticket)
		return passed, 0
	}
	s.ticket, s.releasedThrough, s.changed = ticket, releasedThrough, changed

	// Walk forward from head while each slot holds the ticket we expect next. A slot is written only when
	// its run completes, so a mismatch means ticket h is still in flight: the slot still holds its previous
	// occupant, h - capacity (or 0 before the first wrap). Stop there; the Complete call for h resumes the
	// walk. The loop always ends, because after capacity steps the slot holds h - capacity, never h.
	var watermark uint64
	for h := head; t.slots[h&t.mask].ticket == h; h++ {
		hs := &t.slots[h&t.mask]
		passed = append(passed, hs.changed)
		watermark = hs.releasedThrough
		// ticket and releasedThrough stay as harmless leftovers: they name an older ticket than any head
		// that can reach this slot again.
		hs.changed = nil
		t.head.Store(h + 1)
	}
	return passed, watermark
}

// ValidateCapacity checks a configured capacity: a power of two within [MinCapacity, MaxCapacity] that can
// hold one full pending-sequence cascade of maxNumPending entries.
func ValidateCapacity(capacity uint64, maxNumPending int) error {
	if err := validateRange(capacity); err != nil {
		return err
	}
	if need := cascadeRuns(maxNumPending); capacity <= need {
		// Name the smallest value that works rather than the bound itself: the bound is rarely a power of
		// two, so a value just above it would only be rejected again.
		smallest, err := CapacityFor(maxNumPending)
		if err != nil {
			return err
		}
		return fmt.Errorf("%d is too small for max_num_pending %d: use at least %d, or leave it unset to size it automatically", capacity, maxNumPending, smallest)
	}
	return nil
}

// CapacityFor returns the smallest valid capacity for maxNumPending, used when no capacity is configured.
func CapacityFor(maxNumPending int) (uint64, error) {
	capacity := uint64(MinCapacity)
	if need := cascadeRuns(maxNumPending) + 1; need > capacity {
		capacity = 1 << bits.Len64(need-1) // next power of two >= need
	}
	if capacity > MaxCapacity {
		return 0, fmt.Errorf("%d is above the maximum of %d", maxNumPending, MaxNumPending)
	}
	return capacity, nil
}

// cascadeRuns is the number of runs a full cascade of maxNumPending pending entries is cut into. A
// cascade can release a few entries beyond maxNumPending (the arriving entry, and the one that tips the
// heap over the limit), which is why the capacity must be strictly greater than this.
func cascadeRuns(maxNumPending int) uint64 {
	if maxNumPending <= 0 {
		return 0
	}
	return (uint64(maxNumPending) + RunSize - 1) / RunSize
}

func validateRange(capacity uint64) error {
	if !isPowerOfTwo(capacity) {
		return fmt.Errorf("capacity %d must be a power of two", capacity)
	}
	if capacity < MinCapacity || capacity > MaxCapacity {
		return fmt.Errorf("capacity %d must be between %d and %d", capacity, MinCapacity, MaxCapacity)
	}
	return nil
}

// isPowerOfTwo reports whether n has exactly one bit set. Subtracting 1 clears that bit and sets every bit
// below it (0b1000 - 1 = 0b0111), so n&(n-1) is zero only when no other bit was set. Zero is excluded
// explicitly, since 0&(0-1) is also zero.
func isPowerOfTwo(n uint64) bool {
	return n != 0 && n&(n-1) == 0
}
