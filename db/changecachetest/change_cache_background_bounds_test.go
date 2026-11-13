/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package changecachetest

import (
	"testing"
	"testing/synctest"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestChangeCacheBackgroundTaskBounds pins how long the cache's two housekeeping tasks take at worst. Each task wakes
// every half of its max wait and acts only once an entry is at least a full max wait old, so the first wake-up that
// finds it old enough comes no later than 1.5 times the max wait after it arrived. The fuzz harness only
// checks that the cache ends in a consistent state, so a task that ran at the wrong interval would otherwise pass.
func TestChangeCacheBackgroundTaskBounds(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelWarn, base.KeyAll)
	env := newFuzzEnv(t)

	t.Run("pending entry is released", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newFuzzHarness(t, env, defaultFuzzConfig())
			h.start()
			defer h.stop()

			// Sequence 2 arrives while sequence 1 is missing, so it waits in the pending buffer.
			h.write(0, chanMask("A"), false)
			h.write(1, chanMask("A"), false)
			h.withholdOldest() // sequence 1 never arrives
			h.deliverOne(1, 0)
			next, _, skipped := h.cache.SequenceStateForTest(t, h.env.ctx)
			require.Equal(t, uint64(1), next)
			require.Empty(t, skipped)

			time.Sleep(fuzzPendingSeqMaxWait*3/2 + time.Nanosecond)
			synctest.Wait()
			next, _, skipped = h.cache.SequenceStateForTest(t, h.env.ctx)
			assert.Equal(t, uint64(3), next, "sequence 2 should have been released")
			assert.Equal(t, []uint64{1}, skipped, "sequence 1 should have been skipped")
		})
	})

	t.Run("skipped sequence is abandoned", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newFuzzHarness(t, env, defaultFuzzConfig())
			h.start()
			defer h.stop()

			h.write(0, chanMask("A"), false)
			h.write(1, chanMask("A"), false)
			h.withholdOldest() // sequence 1 never arrives
			h.deliverOne(1, 0)
			time.Sleep(fuzzPendingSeqMaxWait*3/2 + time.Nanosecond)
			synctest.Wait()
			_, _, skipped := h.cache.SequenceStateForTest(t, h.env.ctx)
			require.Equal(t, []uint64{1}, skipped)

			time.Sleep(fuzzSkippedSeqMaxWait*3/2 + time.Nanosecond)
			synctest.Wait()
			_, _, skipped = h.cache.SequenceStateForTest(t, h.env.ctx)
			assert.Empty(t, skipped, "sequence 1 should have been abandoned")
		})
	})
}

// TestChangeCachePendingCapacityBoundary pins that the pending buffer is only flushed by an arrival once it holds MORE
// than CachePendingSeqMaxNum entries. Sequence 2 is already older than the pending max wait when sequences 3 and 4 arrive,
// but the housekeeping task, which wakes every half of that wait, has not run since it became old; so the arrivals that
// leave the buffer at or below its capacity must leave everything pending. The task flushes only if nothing was added to
// the buffer for a full max wait, so it is the first wake-up after that which releases it.
func TestChangeCachePendingCapacityBoundary(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelWarn, base.KeyAll)
	env := newFuzzEnv(t)
	synctest.Test(t, func(t *testing.T) {
		cfg := defaultFuzzConfig()
		cfg.PendingMaxNum = 3
		h := newFuzzHarness(t, env, cfg)
		h.start()
		defer h.stop()
		for doc := range 4 {
			h.write(doc, chanMask("A"), false)
		}
		h.withholdOldest() // sequence 1 never arrives
		assertPending := func(what string) {
			t.Helper()
			next, _, skipped := h.cache.SequenceStateForTest(t, h.env.ctx)
			assert.Equal(t, uint64(1), next, what)
			assert.Empty(t, skipped, what)
		}

		// The task wakes at multiples of half the max wait from the start. Sequence 2 arrives at 1s, so the wake-ups at 2.5s
		// and 5s find it younger than the max wait, and at 6.5s it is old but the wake-up at 7.5s has not happened yet.
		time.Sleep(time.Second)
		h.deliverOne(1, 0)
		time.Sleep(fuzzPendingSeqMaxWait + fuzzPendingSeqMaxWait/10)
		synctest.Wait()
		assertPending("after sequence 2 became old")
		h.deliverOne(1, 0)
		assertPending("with 2 of 3 pending")
		h.deliverOne(1, 0)
		assertPending("with 3 of 3 pending")

		time.Sleep(fuzzPendingSeqMaxWait*3/2 + time.Nanosecond)
		synctest.Wait()
		next, _, skipped := h.cache.SequenceStateForTest(t, h.env.ctx)
		assert.Equal(t, uint64(5), next, "the wake-up releases the old entries")
		assert.Equal(t, []uint64{1}, skipped)
	})
}
