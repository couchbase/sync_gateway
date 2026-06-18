//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"sync"
)

// hlcLogicalBits is the number of low-order bits of a Couchbase Server CAS reserved for the logical
// (counter) component of the Hybrid Logical Clock. The remaining high-order bits hold the physical
// (wall-clock nanosecond) component. Clearing these bits when reading the wall clock keeps generated
// values in the same numeric space as a CAS and leaves room for same-instant logical increments.
const hlcLogicalBits = 16

// hlcLogicalMask masks off the logical component, isolating the physical component of a timestamp.
const hlcLogicalMask = (1 << hlcLogicalBits) - 1

// HybridLogicalClock generates monotonically increasing timestamps in the same numeric space as a
// Couchbase Server CAS (a 48-bit nanosecond physical component plus a 16-bit logical counter). Sync
// Gateway uses it to assign HLV current-version values without relying on server-side CAS macro
// expansion, so the value is known before the write and can be written as a literal into every xattr
// field that would otherwise require a per-field subdoc macro-expansion path.
//
// Generated values are CAS-comparable: under synchronised clocks a value generated just before a write
// is below the CAS the server stamps at commit. This is an invariant goxdcr enforces (cv.ver <= cas) until MB-72252.
// Monotonicity per HLV source is the caller's responsibility, supplied via the floor argument to Now.
type HybridLogicalClock struct {
	clock       func() uint64 // wall-clock source, nanoseconds since the Unix epoch; overridable in tests
	highestTime uint64
	mutex       sync.Mutex
}

// NewHybridLogicalClock returns a HybridLogicalClock backed by the system wall clock.
func NewHybridLogicalClock() *HybridLogicalClock {
	return &HybridLogicalClock{clock: func() uint64 { return hlcWallClock() }}
}

// SetClockForTest overrides the wall-clock source and resets the clock's high-water mark, so the next
// value returned by Now is determined solely by getTime. Test-only: used to simulate clock skew between
// Sync Gateway and the server.
func (c *HybridLogicalClock) SetClockForTest(getTime func() uint64) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.clock = getTime
	c.highestTime = 0
}

// Now returns the next timestamp, guaranteed to be strictly greater than both the previous value
// returned by this clock (monotonic across calls) and the supplied floor, while tracking the wall clock
// where possible. The result is max(physical, highestTime+1, floor+1) where physical is the wall-clock
// time with its logical bits cleared.
//
// floor is the highest existing HLV version value for the caller's source; passing 0 (a brand-new
// document, or a source not yet present in the HLV) imposes no lower bound beyond monotonicity, so Now(0)
// is an ordinary clock tick.
func (c *HybridLogicalClock) Now(floor uint64) uint64 {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	next := c.clock() &^ hlcLogicalMask // physical component, tracks the wall clock
	if c.highestTime+1 > next {         // ensure strictly greater than the previous value
		next = c.highestTime + 1
	}
	if floor+1 > next { // ensure strictly greater than the caller's floor
		next = floor + 1
	}
	c.highestTime = next
	return next
}

// CASToPhysicalNanos returns the physical (wall-clock nanosecond) component of a CAS / HLC value, with the
// logical counter bits cleared. The difference between two such values approximates the elapsed wall-clock
// time between them, which is used to decide how long to wait for a lagging clock to catch up.
func CASToPhysicalNanos(cas uint64) uint64 {
	return cas &^ hlcLogicalMask
}
