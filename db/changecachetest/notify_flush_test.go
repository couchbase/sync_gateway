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
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestManualNotifyDeliversOnFlush checks that manual-flush mode holds notifications back until the test flushes,
// and that a flush then delivers them on the calling goroutine.
func TestManualNotifyDeliversOnFlush(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	collection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)
	collection.ChannelMapper = channels.NewChannelMapper(ctx, channels.DocChannelsSyncFunction, database.Options.JavascriptTimeout)
	expected := channels.NewID("zero", collection.GetCollectionID())

	// The callback runs only inside FlushNotificationsForTest, and only one flush runs at a time (EventuallyWithT
	// runs its conditions one after another), so it needs no lock.
	var notified []channels.Set
	cache := database.ChangeCacheForTest(t)
	cache.SetManualNotifyChangeFuncForTest(t, func(_ context.Context, chans channels.Set) {
		if chans.Contains(expected) {
			notified = append(notified, chans)
		}
	})

	_, _, err := collection.Put(ctx, "manualNotifyDoc", db.Body{"channels": []string{"zero"}})
	require.NoError(t, err)

	// Nothing but a flush delivers in manual mode, so nothing has arrived yet, however far the caching feed has got.
	require.Empty(t, notified)

	// Put returns before the caching feed has processed the write, and the merge into the accumulator follows the
	// caching, so an early flush can find nothing to deliver: flush on every poll until the channel arrives.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		cache.FlushNotificationsForTest(t)
		assert.Len(c, notified, 1)
	}, 10*time.Second, 10*time.Millisecond)
}

// TestReplacedFlusherStopsDraining checks that installing a new notify callback stops the previous callback's flusher
// before it returns. A flusher still running would take notifications merged after the replacement, and the new
// callback would never see them.
func TestReplacedFlusherStopsDraining(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)
	cache := database.ChangeCacheForTest(t)

	var oldReceived atomic.Int64
	cache.SetNotifyChangeFuncForTest(t, func(_ context.Context, chans channels.Set) {
		oldReceived.Add(int64(len(chans)))
	})
	// Give the flusher time to park on its ticker, where a replacement it only checked for between ticks would go
	// unnoticed until the next tick had already drained.
	time.Sleep(10 * time.Millisecond)

	// The callback runs only inside FlushNotificationsForTest on this goroutine, so it needs no lock.
	var manualReceived int
	cache.SetManualNotifyChangeFuncForTest(t, func(_ context.Context, chans channels.Set) {
		manualReceived += len(chans)
	})

	cache.MergeNotificationsForTest(t, channels.NewID("afterReplacement", 0))
	// Long enough for the old flusher to tick several times, had it survived the replacement.
	time.Sleep(3 * db.DefaultBroadcastChangesTime)
	cache.FlushNotificationsForTest(t)

	assert.Zero(t, oldReceived.Load(), "the replaced flusher drained into the old callback")
	assert.Equal(t, 1, manualReceived, "the new callback must receive what was merged after it was installed")
}
