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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// hourNanos is an offset used to place test floors a wall-clock hour either side of "now".
const hourNanos = uint64(time.Hour)

// TestHybridLogicalClockMonotonic asserts successive values from the system-backed clock strictly increase.
func TestHybridLogicalClockMonotonic(t *testing.T) {
	hlc := NewHybridLogicalClock()
	prev := hlc.Now(0)
	for i := 0; i < 1000; i++ {
		next := hlc.Now(0)
		require.Greater(t, next, prev, "value at iteration %d did not strictly increase", i)
		prev = next
	}
}

// TestHybridLogicalClockTracksWallClock asserts Now(0) reflects the wall clock (physical component) with
// its logical bits cleared, rather than drifting off into logical-counter space from a cold start.
func TestHybridLogicalClockTracksWallClock(t *testing.T) {
	hlc := NewHybridLogicalClock()
	before := hlcWallClock() &^ hlcLogicalMask
	got := hlc.Now(0)
	after := hlcWallClock()
	require.GreaterOrEqual(t, got, before)
	require.LessOrEqual(t, got, after)
}

// TestHybridLogicalClockSameInstant asserts that when the wall clock does not advance, successive values
// still strictly increase via the logical counter.
func TestHybridLogicalClockSameInstant(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	hlc := &HybridLogicalClock{clock: func() uint64 { return now }}

	first := hlc.Now(0)
	require.Equal(t, now&^hlcLogicalMask, first)
	require.Equal(t, first+1, hlc.Now(0))
	require.Equal(t, first+2, hlc.Now(0))
}

// TestHybridLogicalClockPhysicalMask asserts the logical bits of the wall clock are cleared so generated
// values stay in the CAS numeric space.
func TestHybridLogicalClockPhysicalMask(t *testing.T) {
	// OR in low bits so masking is observable regardless of the current nanosecond.
	now := uint64(time.Now().UnixNano()) | hlcLogicalMask
	hlc := &HybridLogicalClock{clock: func() uint64 { return now }}

	got := hlc.Now(0)
	// Isolates only bits 15–0 of the result. Requires them to be all zero — proving Now() stripped them via &^ hlcLogicalMask.
	require.Zero(t, got&hlcLogicalMask, "low %d (logical) bits should be cleared", hlcLogicalBits)
	// Both sides clear the low 16 bits of now
	require.Equal(t, now&^hlcLogicalMask, got)
}

// TestHybridLogicalClockFloor asserts Now never returns a value <= floor.
func TestHybridLogicalClockFloor(t *testing.T) {
	t.Run("floor below physical is ignored", func(t *testing.T) {
		now := uint64(time.Now().UnixNano())
		hlc := &HybridLogicalClock{clock: func() uint64 { return now }}
		got := hlc.Now(now - hourNanos) // a floor an hour in the past
		require.Equal(t, now&^hlcLogicalMask, got)
	})

	t.Run("floor above physical wins", func(t *testing.T) {
		now := uint64(time.Now().UnixNano())
		hlc := &HybridLogicalClock{clock: func() uint64 { return now }}
		floor := now + hourNanos // a floor an hour in the future
		got := hlc.Now(floor)
		require.Equal(t, floor+1, got)
		require.Greater(t, got, floor)
	})
}

// TestHybridLogicalClockNoFloor covers the brand-new-document / absent-source case where floor is 0: Now
// behaves as an ordinary tick rather than special-casing.
func TestHybridLogicalClockNoFloor(t *testing.T) {
	now := uint64(time.Now().UnixNano())
	hlc := &HybridLogicalClock{clock: func() uint64 { return now }}
	require.Equal(t, now&^hlcLogicalMask, hlc.Now(0))
}

// TestHybridLogicalClockConcurrent asserts thread-safety: concurrent callers never receive duplicate
// values (strict monotonicity implies uniqueness).
func TestHybridLogicalClockConcurrent(t *testing.T) {
	hlc := NewHybridLogicalClock()

	const goroutines = 16
	const perGoroutine = 500

	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			values := make([]uint64, perGoroutine)
			for i := range values {
				values[i] = hlc.Now(0)
			}
			results[g] = values
		}(g)
	}
	wg.Wait()

	seen := make(map[uint64]struct{}, goroutines*perGoroutine)
	for _, values := range results {
		for _, v := range values {
			_, dup := seen[v]
			assert.False(t, dup, "duplicate value %d returned to concurrent callers", v)
			seen[v] = struct{}{}
		}
	}
	require.Len(t, seen, goroutines*perGoroutine)
}
