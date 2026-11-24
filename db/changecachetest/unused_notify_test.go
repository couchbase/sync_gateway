//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package changecachetest

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestChangeCacheNotifiesUnusedSequences covers when request_plus waiters, which wait on the unused-sequence key, are
// woken. The key must be notified when an unused sequence becomes readable, whichever path makes it readable, and
// not when one merely arrives: a wake for a sequence that is still pending or already processed wakes every waiter
// for nothing, and a missing wake leaves them waiting for a heartbeat or their timeout.
func TestChangeCacheNotifiesUnusedSequences(t *testing.T) {
	const initialSequence = 100
	unusedKey := db.UnusedSeqChannelIDForTest()

	// changeCache is the part of the change cache these tests drive. Its type is unexported from db.
	type changeCache interface {
		DocChanged(event sgbucket.FeedEvent, docType db.DocumentType)
		ProcessEntryForTest(tb testing.TB, ctx context.Context, change *db.LogEntry)
		SkippedSeqsForTest(tb testing.TB) *db.SkippedSequenceSkiplist
		GetMaxStableCachedForTest(tb testing.TB, ctx context.Context) uint64
		Stop(ctx context.Context)
	}
	type testCache struct {
		ctx          context.Context
		cache        changeCache
		notified     *notifyRecorder
		metaKeys     *base.MetadataKeys
		collectionID uint32
	}
	// newDatabase sets up the database a cache runs against. It is built outside any synctest bubble.
	newDatabase := func(t *testing.T) (*db.Database, context.Context) {
		database, ctx := db.SetupTestDB(t)
		t.Cleanup(func() {
			database.Close(ctx)
		})
		return database, ctx
	}
	// startCache starts a standalone change cache on database, expecting initialSequence+1 next. The caller must
	// Stop it; inside a synctest bubble that has to happen before the bubble's function returns.
	startCache := func(t *testing.T, database *db.Database, ctx context.Context, setOptions func(*db.CacheOptions)) *testCache {
		cacheOptions := db.DefaultCacheOptions()
		cacheOptions.CachePendingSeqMaxWait = 2 * time.Minute
		if setOptions != nil {
			setOptions(&cacheOptions)
		}
		changeCache := db.NewChangeCacheForTest(t)
		require.NoError(t, changeCache.Init(ctx, database.DatabaseContext, database.DatabaseContext.ChannelCacheForTest(t),
			&cacheOptions, database.DatabaseContext.MetadataKeys))
		notified := newNotifyRecorder(t, changeCache)
		require.NoError(t, changeCache.Start(initialSequence))
		return &testCache{
			ctx:          ctx,
			cache:        changeCache,
			notified:     notified,
			metaKeys:     database.DatabaseContext.MetadataKeys,
			collectionID: db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionID(),
		}
	}
	newCache := func(t *testing.T, setOptions func(*db.CacheOptions)) *testCache {
		database, ctx := newDatabase(t)
		tc := startCache(t, database, ctx, setOptions)
		t.Cleanup(func() {
			tc.cache.Stop(ctx)
		})
		return tc
	}
	unused := func(tc *testCache, seq uint64) {
		tc.cache.DocChanged(sgbucket.FeedEvent{Synchronous: true, Key: []byte(tc.metaKeys.UnusedSeqKey(seq)), CollectionID: tc.collectionID}, db.DocTypeUnusedSeq)
	}
	unusedRange := func(tc *testCache, from, to uint64) {
		tc.cache.DocChanged(sgbucket.FeedEvent{Synchronous: true, Key: []byte(tc.metaKeys.UnusedSeqRangeKey(from, to)), CollectionID: tc.collectionID}, db.DocTypeUnusedSeqRange)
	}
	// requireUnusedWake flushes and requires exactly one notification, naming the unused key and stamped with
	// wantWatermark. Nothing else moves the watermark in these tests, so its value at the flush is exact.
	requireUnusedWake := func(t *testing.T, tc *testCache, wantWatermark uint64, msg string) {
		t.Helper()
		notifications := tc.notified.flush(t)
		require.Len(t, notifications, 1, msg)
		assert.True(t, notifications[0].channels.Contains(unusedKey), msg)
		assert.Equal(t, wantWatermark, notifications[0].watermark, "watermark when waiters are woken")
	}

	t.Run("a buffered unused sequence wakes waiters only once it is released", func(t *testing.T) {
		tc := newCache(t, nil)
		unused(tc, 102)
		require.Empty(t, tc.notified.flush(t), "102 is held behind 101, so nothing is readable yet")

		unused(tc, 101)
		requireUnusedWake(t, tc, 102, "releasing 101 releases 102 too, and waiters must be woken for both")
	})

	t.Run("a duplicate unused sequence does not wake waiters", func(t *testing.T) {
		tc := newCache(t, nil)
		unused(tc, 101)
		requireUnusedWake(t, tc, 101, "101 is released on arrival")

		unused(tc, 101)
		assert.Empty(t, tc.notified.flush(t), "101 was already processed, so nothing new is readable")
	})

	// Runs in a synctest bubble so that the background task can only release 102 when the test lets fake time pass:
	// on real time, a slow machine could reach the pending deadline before the first flush.
	t.Run("an unused sequence released by the background pending release wakes waiters", func(t *testing.T) {
		const pendingMaxWait = 100 * time.Millisecond
		database, ctx := newDatabase(t)
		synctest.Test(t, func(t *testing.T) {
			tc := startCache(t, database, ctx, func(o *db.CacheOptions) {
				o.CachePendingSeqMaxWait = pendingMaxWait
			})
			// A failed assertion ends this goroutine without returning; the cache's background tasks must still stop
			// inside the bubble, or the failure turns into a bubble deadlock.
			defer tc.cache.Stop(ctx)

			unused(tc, 102)
			require.Empty(t, tc.notified.flush(t), "102 is held behind 101, so nothing is readable yet")

			// Nothing else arrives: only the background task, skipping 101 once it has waited too long, releases 102.
			// Sleep just past the deadline so the task and the test never wake at the same fake instant, then wait for
			// the task to finish, including its merge after it releases the lock.
			time.Sleep(pendingMaxWait + time.Nanosecond)
			synctest.Wait()
			requireUnusedWake(t, tc, 102, "the background release made 102 readable")
		})
	})

	t.Run("a late unused sequence leaving the skipped list wakes waiters", func(t *testing.T) {
		// With no room in pending, 102 arriving out of order skips 101 at once.
		tc := newCache(t, func(o *db.CacheOptions) {
			o.CachePendingSeqMaxNum = 0
		})
		tc.cache.ProcessEntryForTest(t, tc.ctx, db.MakeTestLogEntry(102, "doc102", "1-a"))
		require.True(t, tc.cache.SkippedSeqsForTest(t).Contains(101))
		require.Equal(t, uint64(100), tc.cache.GetMaxStableCachedForTest(t, tc.ctx), "101 is skipped, so the stable sequence stops below it")
		_ = tc.notified.flush(t)

		unused(tc, 101)
		require.False(t, tc.cache.SkippedSeqsForTest(t).Contains(101))
		// The watermark was already 102, so the wake is sent as 101 is released from the skipped list, like any unused
		// sequence that becomes readable.
		requireUnusedWake(t, tc, 102, "101 left the skipped list")
		assert.Equal(t, uint64(102), tc.cache.GetMaxStableCachedForTest(t, tc.ctx), "stable sequence when waiters are woken")
	})

	t.Run("an unused range removed from the skipped list wakes waiters", func(t *testing.T) {
		tc := newCache(t, func(o *db.CacheOptions) {
			o.CachePendingSeqMaxNum = 0
		})
		tc.cache.ProcessEntryForTest(t, tc.ctx, db.MakeTestLogEntry(105, "doc105", "1-a"))
		require.True(t, tc.cache.SkippedSeqsForTest(t).Contains(101))
		require.Equal(t, uint64(100), tc.cache.GetMaxStableCachedForTest(t, tc.ctx), "101-104 are skipped, so the stable sequence stops below them")
		_ = tc.notified.flush(t)

		unusedRange(tc, 101, 104)
		require.False(t, tc.cache.SkippedSeqsForTest(t).Contains(101))
		// No entry is released for the range, so the wake comes from the hold's final run. The watermark was already
		// 105, since the hold that skipped 101-104 released 105.
		requireUnusedWake(t, tc, 105, "101-104 left the skipped list")
		assert.Equal(t, uint64(105), tc.cache.GetMaxStableCachedForTest(t, tc.ctx), "stable sequence when waiters are woken")

		unusedRange(tc, 101, 104)
		assert.Empty(t, tc.notified.flush(t), "101-104 are no longer skipped, so the repeated range makes nothing readable")
	})
}
