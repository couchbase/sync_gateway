//  Copyright 2026-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package changesfeedtest

import (
	"context"
	"errors"
	"runtime/pprof"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

// failingSecondPageCache returns one page of changes, then cancels the changes request and fails the next query.
type failingSecondPageCache struct {
	db.SingleChannelCache
	channelID     channels.ID
	cancelChanges context.CancelCauseFunc
	calls         int
}

func (c *failingSecondPageCache) ChannelID() channels.ID {
	return c.channelID
}

func (c *failingSecondPageCache) GetChanges(_ context.Context, _ db.ChangesOptions) ([]*db.LogEntry, error) {
	c.calls++
	if c.calls == 1 {
		return []*db.LogEntry{{DocID: "doc1", RevID: "1-a", Sequence: 1}}, nil
	}
	c.cancelChanges(errors.New("client disconnected"))
	return nil, errors.New("query failed")
}

// TestChangesFeedQueryErrorAfterChangesCancelled verifies that the channel feed goroutine exits when a query fails
// after the client stops reading.
func TestChangesFeedQueryErrorAfterChangesCancelled(t *testing.T) {
	const labelKey = "test"

	cacheOptions := db.DefaultCacheOptions()
	cacheOptions.ChannelQueryLimit = 1
	database, ctx := db.SetupTestDBWithCacheOptions(t, cacheOptions)
	defer database.Close(ctx)
	collection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)

	changesCtx, cancelChanges := context.WithCancelCause(ctx)
	defer cancelChanges(nil)
	cache := &failingSecondPageCache{
		channelID:     channels.NewID("ABC", collection.GetCollectionID()),
		cancelChanges: cancelChanges,
	}

	// The feed goroutine inherits the label, which separates it from goroutines started by other tests. Nothing reads
	// the feed until the goroutine exits, so the first page fills the feed's buffer.
	var feed <-chan *db.ChangeEntry
	pprof.Do(ctx, pprof.Labels(labelKey, t.Name()), func(ctx context.Context) {
		feed = collection.ChangesFeedForTest(t, ctx, cache, db.ChangesOptions{ChangesCtx: changesCtx}, "")
	})
	base.RequireChanClosed(t, changesCtx.Done(), "second query never ran")

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Zero(c, sgtest.CountLabeledGoroutines(c, labelKey, t.Name()))
	}, 10*time.Second, 10*time.Millisecond, "changesFeed goroutine did not exit after the changes request was cancelled")

	entry := base.RequireChanRecv(t, feed, "first page entry was not sent")
	require.Equal(t, "doc1", entry.ID)
	base.RequireChanClosed(t, feed, "feed was not closed")
}

// TestRequestPlusTargetInUnwatchedChannel checks that a one-shot request_plus feed completes once the cache
// reaches its request_plus sequence, even when nothing at that sequence is in a channel the feed is watching. The
// feed's waiter only wakes for its own keys, so if nothing else wakes it, the feed hangs past its target.
func TestRequestPlusTargetInUnwatchedChannel(t *testing.T) {
	testCases := []struct {
		name        string
		docChannels []string
		docSeq      uint64
		unusedSeqs  []uint64 // embedded in the document's sync data
	}{
		{name: "target in watched channel (control)", docChannels: []string{"ABC"}, docSeq: 2},
		{name: "target in unwatched channel", docChannels: []string{"XYZ"}, docSeq: 2},
		{name: "target in no channel", docChannels: nil, docSeq: 2},
		{name: "target is an embedded unused sequence", docChannels: []string{"XYZ"}, docSeq: 3, unusedSeqs: []uint64{2}},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCache)

			database, ctx := db.SetupTestDBWithCacheOptions(t, db.ShortWaitCache())
			defer database.Close(ctx)

			authenticator := database.Authenticator(ctx)
			user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(user))
			database.SetUser(user)

			dbCollection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)
			collection := dbCollection.DatabaseCollection

			// Sequence 1 is visible to the user and already cached when the request arrives.
			db.WriteDirect(t, collection, []string{"ABC"}, 1)
			database.WaitForSequence(t, 1)

			// One-shot request_plus feed whose target is sequence 2, not yet cached.
			changesCtx, changesCtxCancel := context.WithCancelCause(ctx)
			defer changesCtxCancel(errors.New("test teardown"))
			options := db.ChangesOptions{
				Since:          db.SequenceID{Seq: 0},
				ChangesCtx:     changesCtx,
				RequestPlusSeq: 2,
			}
			feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("ABC"), options)
			require.NoError(t, err)

			// Read until the feed parks (nil entry): it has sent sequence 1 and is waiting for the cache to
			// reach 2.
			var sent []string
			for parked := false; !parked; {
				select {
				case entry, ok := <-feed:
					require.True(t, ok, "feed closed before reaching its request_plus sequence")
					if entry == nil {
						parked = true
					} else {
						sent = append(sent, entry.Seq.String())
					}
				case <-time.After(10 * time.Second):
					require.FailNow(t, "feed never parked")
				}
			}
			require.Equal(t, []string{"1"}, sent)

			// The cache now passes the target, via a document the feed can't see.
			db.WriteDirectWithUnusedSequences(t, collection, tc.docChannels, tc.docSeq, tc.unusedSeqs)
			database.WaitForSequence(t, tc.docSeq)

			// The feed's exit condition (currentCachedSequence >= RequestPlusSeq) is now satisfied, so it
			// should close promptly. Five seconds is 100 broadcast ticks.
			deadline := time.After(5 * time.Second)
			for closed := false; !closed; {
				select {
				case _, ok := <-feed:
					closed = !ok
				case <-deadline:
					require.FailNow(t, "request_plus feed did not complete after the cache passed its target",
						"sequence %d is in channels %q; the feed watches ABC", tc.docSeq, tc.docChannels)
				}
			}
		})
	}
}

// TestRequestPlusPollingFeedCancelled checks that a request_plus feed parked below its target, and so polling the
// cache rather than waiting on its change waiter, exits promptly when its changes context is cancelled, and
// stops counting as caught up.
func TestRequestPlusPollingFeedCancelled(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCache)

	database, ctx := db.SetupTestDBWithCacheOptions(t, db.ShortWaitCache())
	defer database.Close(ctx)

	authenticator := database.Authenticator(ctx)
	user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))
	database.SetUser(user)

	dbCollection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)
	collection := dbCollection.DatabaseCollection

	db.WriteDirect(t, collection, []string{"ABC"}, 1)
	database.WaitForSequence(t, 1)

	caughtUp := database.DbStats.CBLReplicationPull().NumPullReplCaughtUp
	caughtUpStart := caughtUp.Value()

	// Sequence 2 is never written, so the feed stays below its target and polls until cancelled.
	changesCtx, changesCtxCancel := context.WithCancelCause(ctx)
	defer changesCtxCancel(errors.New("test teardown"))
	options := db.ChangesOptions{
		Since:          db.SequenceID{Seq: 0},
		ChangesCtx:     changesCtx,
		RequestPlusSeq: 2,
	}
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("ABC"), options)
	require.NoError(t, err)

	entry := <-feed
	require.NotNil(t, entry)
	require.Equal(t, "1", entry.Seq.String())
	require.Nil(t, <-feed, "expected the waiting marker once sequence 1 was sent")

	// The caught-up gauge only rises inside waitForFeedProgress, so once it does the feed is parked in the poll.
	base.RequireStatGreaterThan(t, caughtUp, caughtUpStart)

	changesCtxCancel(errors.New("client went away"))

	// Well inside waitForCacheUpdate's ~5s poll timeout, so the feed must have seen the cancellation rather than
	// timed out of the poll.
	base.RequireChanClosedWithTimeout(t, feed, time.Second, "request_plus feed did not exit after cancellation")
	base.RequireWaitForStat(t, caughtUp.Value, caughtUpStart)
}

// TestRequestPlusLongpollParksAfterTarget checks that a longpoll request_plus feed only polls the cache while it is
// below its target. Once the target is reached with nothing to send, it must park on its change waiter like any
// other longpoll feed: a cache move in a channel it doesn't watch must not wake it.
func TestRequestPlusLongpollParksAfterTarget(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCache)

	database, ctx := db.SetupTestDBWithCacheOptions(t, db.ShortWaitCache())
	defer database.Close(ctx)

	authenticator := database.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))
	database.SetUser(user)

	dbCollection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)
	collection := dbCollection.DatabaseCollection

	db.WriteDirect(t, collection, []string{"ABC"}, 1)
	database.WaitForSequence(t, 1)

	totalCaughtUp := database.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp

	changesCtx, changesCtxCancel := context.WithCancelCause(ctx)
	defer changesCtxCancel(errors.New("test teardown"))
	options := db.ChangesOptions{
		Since:          db.SequenceID{Seq: 0},
		ChangesCtx:     changesCtx,
		Wait:           true,
		RequestPlusSeq: 2,
	}
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("ABC"), options)
	require.NoError(t, err)

	entry := <-feed
	require.NotNil(t, entry)
	require.Equal(t, "1", entry.Seq.String())
	require.Nil(t, <-feed, "expected the waiting marker while below the request_plus sequence")

	// Reach the target via a document the feed can't see. The feed has nothing new to send, so it parks again. It
	// may wake once more straight away (the waiter's baseline count predates its channel keys), so drain waiting
	// markers until it has gone quiet before measuring.
	db.WriteDirect(t, collection, []string{"XYZ"}, 2)
	database.WaitForSequence(t, 2)
	require.Nil(t, <-feed, "expected the waiting marker after reaching the request_plus sequence")
	// The overall deadline catches a feed that never parks and keeps sending waiting markers, which would otherwise
	// restart the quiet timer forever.
	drainDeadline := time.After(5 * time.Second)
	for quiet := false; !quiet; {
		select {
		case entry, ok := <-feed:
			require.True(t, ok, "feed closed while parked")
			require.Nil(t, entry, "expected only waiting markers after reaching the request_plus sequence")
		case <-time.After(500 * time.Millisecond):
			quiet = true
		case <-drainDeadline:
			require.FailNow(t, "feed kept sending waiting markers instead of parking after reaching the request_plus sequence")
		}
	}
	parkedTotal := totalCaughtUp.Value()

	// Move the cache again in a channel the feed doesn't watch. A feed still polling would see this within one
	// 100ms poll, re-run its loop and send another waiting marker; a feed parked on its waiter doesn't see it.
	db.WriteDirect(t, collection, []string{"XYZ"}, 3)
	database.WaitForSequence(t, 3)
	select {
	case entry := <-feed:
		require.FailNow(t, "feed woke for a change it doesn't watch", "received %v", entry)
	case <-time.After(500 * time.Millisecond):
	}
	require.Equal(t, parkedTotal, totalCaughtUp.Value(), "feed woke for a change it doesn't watch")

	// A change it does watch wakes it, and as a longpoll that has now sent something, it completes.
	db.WriteDirect(t, collection, []string{"ABC"}, 4)
	entry = <-feed
	require.NotNil(t, entry)
	require.Equal(t, "4", entry.Seq.String())
	base.RequireChanClosed(t, feed, "longpoll feed did not complete after sending a change")
	require.Equal(t, parkedTotal, totalCaughtUp.Value())
}

func TestRequestPlusFeedExitsOnListenerStop(t *testing.T) {
	database, ctx := db.SetupTestDBWithCacheOptions(t, db.ShortWaitCache())
	defer database.Close(ctx)
	authenticator := database.Authenticator(ctx)
	user, err := authenticator.NewUser("alice", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))
	database.SetUser(user)
	dbCollection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)
	db.WriteDirect(t, dbCollection.DatabaseCollection, []string{"ABC"}, 1)
	database.WaitForSequence(t, 1)
	caughtUp := database.DbStats.CBLReplicationPull().NumPullReplCaughtUp
	caughtUpStart := caughtUp.Value()
	changesCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(errors.New("teardown"))
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("ABC"), db.ChangesOptions{
		Since: db.SequenceID{Seq: 0}, ChangesCtx: changesCtx, RequestPlusSeq: 2,
	})
	require.NoError(t, err)
	require.NotNil(t, <-feed)
	require.Nil(t, <-feed)
	base.RequireStatGreaterThan(t, caughtUp, caughtUpStart)

	database.GetMutationListener(t).Stop(ctx)
	deadline := time.After(15 * time.Second)
	for closed := false; !closed; {
		select {
		case _, ok := <-feed:
			closed = !ok
		case <-deadline:
			require.FailNow(t, "request_plus feed still running 15s after the change listener stopped")
		}
	}
}
