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

	// Well inside waitForCacheSequence's ~5s poll timeout, so the feed must have seen the cancellation rather than
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

	// Reach the target via a document the feed can't see. The feed has nothing new to send, so it parks again. Drain
	// any further waiting markers until it has gone quiet before measuring, so the check below only sees wakes
	// caused by the next write.
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

// TestRequestPlusFeedWakesAtTarget checks that a request_plus feed polling below its target doesn't re-run its
// changes loop each time the cache moves, only once the cache reaches the target. Every re-run walks all of the
// user's channels, so with a lagging cache and many request_plus feeds, re-running on every move costs read-side
// CPU just when caching is behind.
func TestRequestPlusFeedWakesAtTarget(t *testing.T) {
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
	totalCaughtUpStart := totalCaughtUp.Value()

	const unwatchedWrites = 8
	const requestPlusSeq = unwatchedWrites + 2
	changesCtx, changesCtxCancel := context.WithCancelCause(ctx)
	defer changesCtxCancel(errors.New("test teardown"))
	options := db.ChangesOptions{
		Since:          db.SequenceID{Seq: 0},
		ChangesCtx:     changesCtx,
		RequestPlusSeq: requestPlusSeq,
	}
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("ABC"), options)
	require.NoError(t, err)

	entry := <-feed
	require.NotNil(t, entry)
	require.Equal(t, "1", entry.Seq.String())
	require.Nil(t, <-feed, "expected the waiting marker while below the request_plus sequence")
	base.RequireWaitForStat(t, totalCaughtUp.Value, totalCaughtUpStart+1)

	// Move the cache towards the target through a channel the feed doesn't watch. Each write is given longer than
	// one 100ms poll to be noticed, so a feed waking on any cache movement would re-run once per write. The whole
	// sequence stays well inside the ~5s poll timeout, so any wake seen here is one the movement caused.
	for seq := uint64(2); seq < requestPlusSeq; seq++ {
		db.WriteDirect(t, collection, []string{"XYZ"}, seq)
		database.WaitForSequence(t, seq)
		select {
		case entry, ok := <-feed:
			require.FailNow(t, "feed re-ran below its request_plus sequence", "cached sequence %d, received %v (open: %t)", seq, entry, ok)
		case <-time.After(150 * time.Millisecond):
		}
	}
	require.Equal(t, totalCaughtUpStart+1, totalCaughtUp.Value(), "feed re-ran below its request_plus sequence")

	// Reaching the target wakes it, and as a one-shot feed with nothing more to send, it completes.
	db.WriteDirect(t, collection, []string{"XYZ"}, requestPlusSeq)
	base.RequireChanClosedWithTimeout(t, feed, 2*time.Second, "request_plus feed did not complete after the cache reached its target")
}

// TestRequestPlusFeedParksAfterPollWake checks that a continuous feed which polled up to its request_plus target
// parks on its change waiter straight after the pass that wakes it. A change it watches lands while it polls; that
// pass sends it, so the waiter must not also count it as new and wake the feed for an empty pass.
func TestRequestPlusFeedParksAfterPollWake(t *testing.T) {
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
	totalCaughtUpStart := totalCaughtUp.Value()

	changesCtx, changesCtxCancel := context.WithCancelCause(ctx)
	defer changesCtxCancel(errors.New("test teardown"))
	options := db.ChangesOptions{
		Since:          db.SequenceID{Seq: 0},
		ChangesCtx:     changesCtx,
		Continuous:     true,
		Wait:           true,
		RequestPlusSeq: 3,
	}
	feed, err := dbCollection.MultiChangesFeed(ctx, base.SetOf("ABC"), options)
	require.NoError(t, err)

	entry := <-feed
	require.NotNil(t, entry)
	require.Equal(t, "1", entry.Seq.String())
	require.Nil(t, <-feed, "expected the waiting marker while below the request_plus sequence")
	base.RequireWaitForStat(t, totalCaughtUp.Value, totalCaughtUpStart+1)

	// Sequence 2 is in a watched channel, so it moves the waiter's count, but it is below the target so the
	// polling feed doesn't wake for it. Sequence 3 reaches the target through a channel the feed doesn't watch.
	db.WriteDirect(t, collection, []string{"ABC"}, 2)
	db.WriteDirect(t, collection, []string{"XYZ"}, 3)

	entry = <-feed
	require.NotNil(t, entry)
	require.Equal(t, "2", entry.Seq.String())
	require.Nil(t, <-feed, "expected the waiting marker after reaching the request_plus sequence")

	// Wake only for the target: anything further, inside the ~5s poll timeout and well past the 50ms broadcast
	// interval, is the waiter waking for sequence 2 again.
	select {
	case entry, ok := <-feed:
		require.FailNow(t, "feed re-ran for a change it had already sent", "received %v (open: %t)", entry, ok)
	case <-time.After(500 * time.Millisecond):
	}
	require.Equal(t, totalCaughtUpStart+2, totalCaughtUp.Value(), "feed re-ran for a change it had already sent")
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

// TestPollingContinuousFeedSeesUserUpdate checks whether a continuous feed that is polling the cache, rather than
// parked on its change waiter, notices its user losing a channel before sending a doc from that channel.
func TestPollingContinuousFeedSeesUserUpdate(t *testing.T) {
	testCases := []struct {
		name           string
		requestPlusSeq uint64
	}{
		{name: "parked on waiter (control)", requestPlusSeq: 0},
		{name: "polling below request_plus target", requestPlusSeq: 1000}, // never reached
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyCache)

			database, ctx := db.SetupTestDBDefaultCollection(t)
			defer database.Close(ctx)

			authenticator := database.Authenticator(ctx)
			user, err := authenticator.NewUser("naomi", "letmein", channels.BaseSetOf(t, "ABC", "DEF"))
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(user))
			database.SetUser(user)

			userCollection, ctx := db.GetSingleDatabaseCollectionWithUser(ctx, t, database)
			adminCollection := &db.DatabaseCollectionWithUser{DatabaseCollection: userCollection.DatabaseCollection}

			_, _, err = adminCollection.Put(ctx, "abc-1", db.Body{"channels": []string{"ABC"}})
			require.NoError(t, err)
			database.WaitForPendingChanges(t)

			caughtUp := database.DbStats.CBLReplicationPull().NumPullReplCaughtUp
			caughtUpStart := caughtUp.Value()

			changesCtx, changesCtxCancel := context.WithCancelCause(ctx)
			defer changesCtxCancel(errors.New("test teardown"))
			options := db.ChangesOptions{
				Since:          db.SequenceID{Seq: 0},
				ChangesCtx:     changesCtx,
				Continuous:     true,
				Wait:           true,
				RequestPlusSeq: tc.requestPlusSeq,
			}
			feed, err := userCollection.MultiChangesFeed(ctx, base.SetOf("*"), options)
			require.NoError(t, err)

			// Read the initial changes up to the first waiting marker.
			for entry := range feed {
				if entry == nil {
					break
				}
			}
			base.RequireStatGreaterThan(t, caughtUp, caughtUpStart)

			// Revoke DEF, then write a doc to it.
			userInfo, err := database.GetPrincipalForTest(t, "naomi", true)
			require.NoError(t, err)
			userInfo.ExplicitChannels = base.SetOf("ABC")
			_, _, err = database.UpdatePrincipal(ctx, userInfo, true, true)
			require.NoError(t, err)
			database.WaitForPendingChanges(t)

			_, _, err = adminCollection.Put(ctx, "def-1", db.Body{"channels": []string{"DEF"}})
			require.NoError(t, err)
			database.WaitForPendingChanges(t)

			// Collect what the feed sends over the next couple of seconds: several 100ms polls, and well past the
			// 50ms broadcast tick that would wake a parked feed.
			var sent []string
			deadline := time.After(2 * time.Second)
			for done := false; !done; {
				select {
				case entry, ok := <-feed:
					require.True(t, ok, "feed closed unexpectedly")
					if entry != nil {
						sent = append(sent, entry.ID)
					}
				case <-deadline:
					done = true
				}
			}
			require.NotContains(t, sent, "def-1", "feed sent a doc from a channel the user lost access to")
		})
	}
}
