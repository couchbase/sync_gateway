/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package notifyaccumulator

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func ids(names ...string) []channels.ID {
	out := make([]channels.ID, 0, len(names))
	for _, name := range names {
		out = append(out, channels.NewID(name, 0))
	}
	return out
}

// drainCopy drains a and returns a copy of what was delivered, or nil if apply wasn't called. It copies
// because Drain clears the set once apply returns.
func drainCopy(t *testing.T, a *Accumulator) channels.Set {
	var got channels.Set
	called := a.Drain(func(set channels.Set) {
		require.NotEmpty(t, set, "apply called with an empty set")
		got = make(channels.Set, len(set))
		for id := range set {
			got.Add(id)
		}
	})
	require.Equal(t, called, got != nil)
	return got
}

// mapIdentity returns the address of a map's header, so a test can tell whether two sets are the same map.
func mapIdentity(set channels.Set) uintptr {
	return reflect.ValueOf(set).Pointer()
}

func TestDrainEmpty(t *testing.T) {
	var a Accumulator
	require.Nil(t, drainCopy(t, &a), "zero value")

	a.Merge(nil)
	a.Merge([]channels.ID{})
	a.MergeBuffers(nil)
	a.MergeBuffers([]*[]channels.ID{nil, {}})
	require.Nil(t, drainCopy(t, &a), "after empty merges")
}

func TestMergeAndDrain(t *testing.T) {
	var a Accumulator
	a.Merge(ids("a", "b"))
	a.Merge(ids("b", "c"))
	require.Equal(t, channels.SetOfNoValidate(ids("a", "b", "c")...), drainCopy(t, &a))

	// Everything was delivered, so the next drain has nothing to deliver.
	require.Nil(t, drainCopy(t, &a))

	// The reused set starts empty: only what was merged after the drain is delivered.
	a.Merge(ids("d"))
	require.Equal(t, channels.SetOfNoValidate(ids("d")...), drainCopy(t, &a))
}

func TestMergeKeepsCollectionsApart(t *testing.T) {
	var a Accumulator
	a.Merge([]channels.ID{channels.NewID("a", 1), channels.NewID("a", 2)})
	require.Len(t, drainCopy(t, &a), 2)
}

func TestMergeBuffers(t *testing.T) {
	var a Accumulator
	a.Merge(ids("a"))
	run1 := ids("a", "b")
	run2 := ids("b", "c")
	empty := []channels.ID{}
	a.MergeBuffers([]*[]channels.ID{&run1, nil, &empty, &run2})
	require.Equal(t, channels.SetOfNoValidate(ids("a", "b", "c")...), drainCopy(t, &a))

	// The buffers are only read.
	require.Equal(t, ids("a", "b"), run1)
	require.Equal(t, ids("b", "c"), run2)
}

// TestMergeDuringApply checks that a merge made while a drain's callback is running neither blocks nor
// joins the set being delivered: it lands in the next drain.
func TestMergeDuringApply(t *testing.T) {
	var a Accumulator
	a.Merge(ids("a"))
	called := a.Drain(func(set channels.Set) {
		a.Merge(ids("b"))
		assert.Equal(t, channels.SetOfNoValidate(ids("a")...), set)
	})
	require.True(t, called)
	require.Equal(t, channels.SetOfNoValidate(ids("b")...), drainCopy(t, &a))
}

// TestDrainReusesSets checks that, once both sets exist, merges and drains alternate between the same two
// maps rather than allocating new ones.
func TestDrainReusesSets(t *testing.T) {
	var a Accumulator
	var seen []uintptr
	for i := range 6 {
		a.Merge(ids(fmt.Sprintf("ch%d", i)))
		require.True(t, a.Drain(func(set channels.Set) { seen = append(seen, mapIdentity(set)) }))
	}
	require.NotEqual(t, seen[0], seen[1], "the second drain should deliver the second set")
	for i := 2; i < len(seen); i++ {
		require.Equal(t, seen[i-2], seen[i], "drain %d should deliver the set drained two drains earlier", i)
	}
}

// TestEmptyDrainKeepsSets checks that a drain with nothing to deliver doesn't lose either allocated set.
func TestEmptyDrainKeepsSets(t *testing.T) {
	var a Accumulator
	var seen []uintptr
	record := func(set channels.Set) { seen = append(seen, mapIdentity(set)) }
	a.Merge(ids("a"))
	require.True(t, a.Drain(record))
	a.Merge(ids("b"))
	require.True(t, a.Drain(record))
	require.False(t, a.Drain(record))
	require.False(t, a.Drain(record))
	a.Merge(ids("c"))
	require.True(t, a.Drain(record))
	a.Merge(ids("d"))
	require.True(t, a.Drain(record))

	require.Len(t, seen, 4)
	require.NotEqual(t, seen[0], seen[1])
	require.ElementsMatch(t, seen[:2], seen[2:], "empty drains should leave the same two sets in use")
}

// TestDrainDropsOversizedSet checks that a set grown past MaxRetainedKeys isn't kept for reuse, while one at
// the limit is. 20,000 is roughly what one tick collects under the heavy load the cap is sized for, so it
// must be reused.
func TestDrainDropsOversizedSet(t *testing.T) {
	for _, size := range []int{20000, MaxRetainedKeys, MaxRetainedKeys + 1} {
		t.Run(fmt.Sprintf("keys=%d", size), func(t *testing.T) {
			var a Accumulator
			// Two warm-up drains so that both sets exist and the big one is the one being drained.
			a.Merge(ids("warm1"))
			require.True(t, a.Drain(func(channels.Set) {}))
			a.Merge(ids("warm2"))
			require.True(t, a.Drain(func(channels.Set) {}))

			big := make([]channels.ID, 0, size)
			for i := range size {
				big = append(big, channels.NewID(fmt.Sprintf("ch%d", i), 0))
			}
			a.Merge(big)
			// Hold the drained map so its address can't be reused by a later allocation.
			var bigSet channels.Set
			require.True(t, a.Drain(func(set channels.Set) {
				assert.Len(t, set, size)
				bigSet = set
			}))

			// The next drain delivers the other set and swaps in whatever the big drain left as spare, so
			// the drain after that delivers it: the big set itself if kept, or a fresh map if dropped.
			a.Merge(ids("x"))
			require.True(t, a.Drain(func(channels.Set) {}))
			var reused uintptr
			a.Merge(ids("y"))
			require.True(t, a.Drain(func(set channels.Set) { reused = mapIdentity(set) }))

			if size > MaxRetainedKeys {
				require.Len(t, bigSet, size, "an oversized set must not be cleared and reused")
				require.NotEqual(t, mapIdentity(bigSet), reused)
			} else {
				require.Empty(t, bigSet, "a set at or below the limit is cleared for reuse")
				require.Equal(t, mapIdentity(bigSet), reused)
			}
		})
	}
}

// TestNoLostMergeAfterOversizedDrain checks that dropping an oversized set doesn't leave the spare pointing
// at the active set. If it did, the next drain would deliver the active set itself: a merge made while
// apply runs would join the set being delivered and then be wiped by its clear, and never be notified.
func TestNoLostMergeAfterOversizedDrain(t *testing.T) {
	var a Accumulator
	// Two warm-up drains so that both sets exist.
	a.Merge(ids("warm1"))
	require.True(t, a.Drain(func(channels.Set) {}))
	a.Merge(ids("warm2"))
	require.True(t, a.Drain(func(channels.Set) {}))

	big := make([]channels.ID, 0, MaxRetainedKeys+1)
	for i := range MaxRetainedKeys + 1 {
		big = append(big, channels.NewID(fmt.Sprintf("ch%d", i), 0))
	}
	a.Merge(big)
	require.True(t, a.Drain(func(channels.Set) {}))

	a.Merge(ids("x"))
	require.True(t, a.Drain(func(set channels.Set) {
		a.Merge(ids("late"))
		assert.Equal(t, channels.SetOfNoValidate(ids("x")...), set)
	}))
	require.Equal(t, channels.SetOfNoValidate(ids("late")...), drainCopy(t, &a))
}

// TestConcurrentMergeDrain merges distinct ids from many goroutines, through both Merge and MergeBuffers,
// while drains run concurrently, and checks that every id is delivered exactly once. Run with -race.
func TestConcurrentMergeDrain(t *testing.T) {
	const (
		mergers         = 8
		mergesPerWorker = 2000
		drainers        = 2
	)
	var a Accumulator

	var deliveredMu sync.Mutex
	delivered := make(map[channels.ID]int)
	apply := func(set channels.Set) {
		deliveredMu.Lock()
		defer deliveredMu.Unlock()
		for id := range set {
			delivered[id]++
		}
	}

	stop := make(chan struct{})
	var drainWG sync.WaitGroup
	for range drainers {
		drainWG.Go(func() {
			for done := false; !done; {
				select {
				case <-stop:
					done = true
				default:
					a.Drain(apply)
				}
			}
		})
	}

	var mergeWG sync.WaitGroup
	for w := range mergers {
		mergeWG.Go(func() {
			for i := range mergesPerWorker {
				id := channels.NewID(fmt.Sprintf("w%d-%d", w, i), uint32(w))
				if i%2 == 0 {
					a.Merge([]channels.ID{id})
				} else {
					buf := []channels.ID{id}
					a.MergeBuffers([]*[]channels.ID{nil, &buf})
				}
			}
		})
	}
	mergeWG.Wait()
	close(stop)
	drainWG.Wait()
	a.Drain(apply)

	require.Len(t, delivered, mergers*mergesPerWorker)
	for id, count := range delivered {
		require.Equal(t, 1, count, "id %v delivered %d times", id, count)
	}
}
