// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package changecachetest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// noTickInterval is long enough that the listener's own broadcast ticker never fires during a test, so the only
// channel stamps are the ones a test makes with BroadcastTickForTest.
const noTickInterval = time.Hour

// waitResult runs waiter.Wait on its own goroutine and returns a go channel that receives the result. The channel is
// buffered, so the goroutine never blocks on a test that has stopped listening.
func waitResult(ctx context.Context, waiter *db.ChangeWaiter) <-chan uint32 {
	result := make(chan uint32, 1)
	go func() {
		result <- waiter.Wait(ctx)
	}()
	return result
}

// requireWoken fails unless result reports changes within the caching-feed timeout.
func requireWoken(t *testing.T, result <-chan uint32, msg string) {
	t.Helper()
	timeout := 10 * time.Second * db.GetCachingFeedDelayFactor(t)
	select {
	case got := <-result:
		require.Equal(t, db.WaiterHasChanges, got, msg)
	case <-time.After(timeout):
		require.FailNow(t, "timed out waiting for wake", msg)
	}
}

// requireNotWoken fails if result arrives within a short window. A missing wake can't be proven by waiting, so this
// only catches an early wake that happens promptly; it can't fail a correct implementation.
func requireNotWoken(t *testing.T, result <-chan uint32, msg string) {
	t.Helper()
	select {
	case got := <-result:
		require.FailNow(t, "woken early", "%s (Wait returned %d)", msg, got)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestPrincipalChangeWakesFeedsWithoutTick checks that saving, updating and deleting a user wakes that user's parked
// feed straight away. No broadcast tick runs in this test, so a wake can only have come from notifyKey.
func TestPrincipalChangeWakesFeedsWithoutTick(t *testing.T) {
	database, ctx := db.SetupTestDBWithCacheOptions(t, pinnedCacheOptions(noTickInterval))
	defer database.Close(ctx)
	collectionID := db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionID()
	listener := database.GetMutationListener(t)

	const username = "bob"
	authenticator := database.Authenticator(ctx)
	user, err := authenticator.NewUser(username, "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	// Created before the save, so the save's notification can't land before the waiter takes its baseline.
	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(t, err)
	waiter := userDb.NewUserWaiter()

	// A channel notification merged before any principal change. It stays unstamped for the whole test, which shows
	// that no tick ran, and that a principal wake doesn't deliver channel notifications early.
	pendingChannel := channels.NewID("ABC", collectionID)
	database.ChangeCacheForTest(t).MergeNotificationsForTest(t, pendingChannel)

	steps := []struct {
		name   string
		change func(t *testing.T)
	}{
		{"save", func(t *testing.T) {
			require.NoError(t, authenticator.Save(user))
		}},
		{"update", func(t *testing.T) {
			name := username
			_, _, err := database.UpdatePrincipal(ctx, &auth.PrincipalConfig{
				Name:             &name,
				ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
			}, true, true)
			require.NoError(t, err)
		}},
		{"delete", func(t *testing.T) {
			current, err := authenticator.GetUser(username)
			require.NoError(t, err)
			require.NoError(t, authenticator.DeleteUser(current))
		}},
	}
	// Each step depends on the one before it, so they share the waiter and run in order.
	for _, step := range steps {
		t.Run(step.name, func(t *testing.T) {
			before := waiter.CurrentUserCount()
			woken := waitResult(ctx, waiter)
			step.change(t)
			requireWoken(t, woken, "a principal change must wake its feeds without waiting for a tick")
			require.Greater(t, waiter.CurrentUserCount(), before, "the wake must come with the user's count moved")
		})
	}

	keyCounts, principalCounts := listener.PrincipalCountsForTest(t)
	assert.Zero(t, keyCounts[pendingChannel], "the merged channel must still be waiting for a tick")
	_, isPrincipal := principalCounts[pendingChannel]
	assert.False(t, isPrincipal)
}

// TestPrincipalWakeAndChannelDrainInOneTick runs a principal wake and a channel drain within one tick period, and
// checks that each wakes only its own feed, at its own time, and that the drain doesn't count as a second change
// to the user.
func TestPrincipalWakeAndChannelDrainInOneTick(t *testing.T) {
	database, ctx := db.SetupTestDBWithCacheOptions(t, pinnedCacheOptions(noTickInterval))
	defer database.Close(ctx)
	collectionID := db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionID()
	listener := database.GetMutationListener(t)
	changeCache := database.ChangeCacheForTest(t)

	// The user is never saved, so the only notifications for it are the direct ones below, with no DCP event.
	user, err := database.Authenticator(ctx).NewUser("bob", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(t, err)
	userWaiter := userDb.NewUserWaiter()
	userKey := channels.NewID(database.MetadataKeys.UserKey(user.Name()), 0)

	channelKey := channels.NewID("ABC", collectionID)
	channelWaiter := listener.NewWaiter([]channels.ID{channelKey}, false)

	// The channel's entry has been cached and its notification merged, but no tick has drained it yet.
	changeCache.MergeNotificationsForTest(t, channelKey)
	userWoken := waitResult(ctx, userWaiter)
	channelWoken := waitResult(ctx, channelWaiter)

	listener.NotifyKeyForTest(t, ctx, userKey)
	requireWoken(t, userWoken, "the principal wake must not wait for the tick")
	// notifyKey's broadcast reached the channel waiter too, but its key hasn't moved, so it must go back to waiting.
	requireNotWoken(t, channelWoken, "the channel feed must not wake before a tick stamps its channel")

	keyCounts, principalCounts := listener.PrincipalCountsForTest(t)
	userCount := keyCounts[userKey]
	require.NotZero(t, userCount)
	require.Equal(t, userCount, principalCounts[userKey])
	require.Zero(t, keyCounts[channelKey], "notifyKey must not drain channel notifications")

	listener.BroadcastTickForTest(t, ctx)
	requireWoken(t, channelWoken, "the tick must wake the channel feed")

	keyCounts, principalCounts = listener.PrincipalCountsForTest(t)
	assert.Greater(t, keyCounts[channelKey], userCount, "the drain stamps the channel after the principal change")
	assert.Equal(t, userCount, keyCounts[userKey], "the drain must not re-stamp the user")
	assert.Equal(t, userCount, principalCounts[userKey], "the drain must not advance the user's count")
	_, isPrincipal := principalCounts[channelKey]
	assert.False(t, isPrincipal, "a drained channel must not get a principal counter")
	assert.False(t, userWaiter.RefreshUserCount(), "the drain must not look like a second change to the user")
}

// TestPrincipalWakesRaceChannelDrains runs principal wakes, channel merges and real broadcast ticks concurrently. It
// checks that no channel notification is lost, that every principal keeps its keyCounts and principalCounts entries
// equal, and that no channel gains a principal counter. A lock-order cycle between notifyKey (tapNotifier.L) and the
// tick's drain (tapNotifier.L, then the accumulator's locks) would deadlock, which the timeout reports. Run with -race.
func TestPrincipalWakesRaceChannelDrains(t *testing.T) {
	const (
		principals        = 4
		wakesPerPrincipal = 500
		mergers           = 4
		mergesPerMerger   = 500
	)
	database, ctx := db.SetupTestDBWithCacheOptions(t, pinnedCacheOptions(time.Millisecond))
	defer database.Close(ctx)
	collectionID := db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionID()
	listener := database.GetMutationListener(t)
	changeCache := database.ChangeCacheForTest(t)

	principalKeys := make([]channels.ID, 0, principals)
	for i := range principals {
		principalKeys = append(principalKeys, channels.NewID(database.MetadataKeys.UserKey(fmt.Sprintf("user%d", i)), 0))
	}
	channelKey := func(merger, i int) channels.ID {
		return channels.NewID(fmt.Sprintf("ch-%d-%d", merger, i), collectionID)
	}

	var wg sync.WaitGroup
	for _, key := range principalKeys {
		wg.Go(func() {
			for range wakesPerPrincipal {
				listener.NotifyKeyForTest(t, ctx, key)
			}
		})
	}
	for m := range mergers {
		wg.Go(func() {
			for i := range mergesPerMerger {
				changeCache.MergeNotificationsForTest(t, channelKey(m, i))
			}
		})
	}
	finished := make(chan struct{})
	go func() {
		wg.Wait()
		close(finished)
	}()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		require.FailNow(t, "principal wakes and channel merges did not finish: possible lock-order deadlock")
	}

	// The listener's own 1 ms tick drains what was merged last.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		keyCounts, _ := listener.PrincipalCountsForTest(t)
		for m := range mergers {
			for i := range mergesPerMerger {
				assert.NotZero(c, keyCounts[channelKey(m, i)], "channel %v was never stamped", channelKey(m, i))
			}
		}
	}, 10*time.Second, 10*time.Millisecond)

	keyCounts, principalCounts := listener.PrincipalCountsForTest(t)
	for _, key := range principalKeys {
		require.NotZero(t, principalCounts[key], "principal %v was never counted", key)
		require.Equal(t, keyCounts[key], principalCounts[key], "principal %v's counts diverged", key)
	}
	for m := range mergers {
		for i := range mergesPerMerger {
			_, isPrincipal := principalCounts[channelKey(m, i)]
			require.False(t, isPrincipal, "channel %v got a principal counter", channelKey(m, i))
		}
	}
}
