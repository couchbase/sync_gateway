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
	"github.com/couchbase/sync_gateway/testing/require"
)

// pinnedCacheOptions pins both broadcast intervals to the same duration, so BroadcastSlowMode
// flipping can't change the wake cadence mid-benchmark.
func pinnedCacheOptions(interval time.Duration) db.CacheOptions {
	opts := db.DefaultCacheOptions()
	opts.BroadcastChangesInterval = interval
	opts.SkippedSequenceBroadcastInterval = interval
	return opts
}

// populateKeyCounts seeds keyCounts with n channel entries in a single tapNotifier.L acquisition.
func populateKeyCounts(b *testing.B, ctx context.Context, database *db.Database, collectionID uint32, n int) {
	b.Helper()
	listener := database.GetMutationListener(b)
	keys := make([]channels.ID, 0, n)
	for i := range n {
		keys = append(keys, channels.NewID(fmt.Sprintf("bench-chan-%d", i), collectionID))
	}
	listener.Notify(ctx, channels.SetOfNoValidate(keys...))
}

// newBenchUser returns a saved user with `roles` resolved roles, giving a waiter the production
// shape of 1+roles tracked principal keys.  The save-then-GetUser round trip is required:
// SetExplicitRoles leaves RoleInvalSeq set, and RoleNames() reports nothing until that resolves.
func newBenchUser(b *testing.B, ctx context.Context, database *db.Database, name string, roles int) auth.User {
	b.Helper()
	a := database.Authenticator(ctx)
	roleNames := make([]string, 0, roles)
	for i := range roles {
		roleName := fmt.Sprintf("%s-role-%d", name, i)
		role, err := a.NewRole(roleName, channels.BaseSetOf(b, "ABC"))
		require.NoError(b, err)
		require.NoError(b, a.Save(role))
		roleNames = append(roleNames, roleName)
	}
	user, err := a.NewUser(name, "letmein", channels.BaseSetOf(b, "ABC"))
	require.NoError(b, err)
	if roles > 0 {
		user.SetExplicitRoles(channels.AtSequence(base.SetOf(roleNames...), 1), 1)
	}
	require.NoError(b, a.Save(user))
	user, err = a.GetUser(name) // resolves ExplicitRoles -> roles
	require.NoError(b, err)
	require.Len(b, user.RoleNames(), roles)
	return user
}

// newBenchWaiter builds a waiter for user and asserts it tracks 1+roles principal keys.
func newBenchWaiter(b *testing.B, database *db.Database, user auth.User, expectedKeys int) *db.ChangeWaiter {
	b.Helper()
	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(b, err)
	w := userDb.NewUserWaiter()
	require.Len(b, w.UserKeysCopyForTest(b), expectedKeys)
	return w
}

// startHerd parks `waiters` ChangeWaiters, each on its own channel key that is never individually
// satisfied, and drives background channel Notify calls so the broadcaster ticker always has
// something to broadcast.  Every tick wakes every waiter, which contends for tapNotifier.L, finds
// its own channel unaffected and re-parks: the wasted-wake load the benchmarks measure against.
// The waiters are load, not the measurement, so they are deliberately not barrier-synchronised.
//
// Teardown releases the herd itself rather than relying on changeListener.Stop, which broadcasts
// without holding tapNotifier.L - a goroutine midway through Cond.Wait's unlock-and-register step
// can miss that broadcast and park forever.  notifyKey holds the lock across write-and-broadcast,
// so teardown bumps every herd key via Notify and then issues one notifyKey to broadcast reliably.
// `stop` covers the window where a goroutine is between Wait() calls rather than parked.
//
// The returned teardown owns the whole shutdown, including database.Close.  Call it exactly once.
func startHerd(b *testing.B, ctx context.Context, database *db.Database, waiters int) (teardown func()) {
	b.Helper()
	listener := database.GetMutationListener(b)

	stop := make(chan struct{})
	var wg sync.WaitGroup

	for i := range waiters {
		chanKey := channels.NewID(fmt.Sprintf("herd-chan-%d", i), 0)
		w := listener.NewWaiterWithChannels(channels.SetOfNoValidate(chanKey), nil, false)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				w.Wait(ctx)
			}
		}()
	}

	driverKey := channels.NewID("herd-driver-channel", 0)
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				listener.Notify(ctx, channels.SetOfNoValidate(driverKey))
			}
		}
	}()

	return func() {
		close(stop)
		allHerdKeys := make([]channels.ID, 0, waiters)
		for i := range waiters {
			allHerdKeys = append(allHerdKeys, channels.NewID(fmt.Sprintf("herd-chan-%d", i), 0))
		}
		listener.Notify(ctx, channels.SetOfNoValidate(allHerdKeys...))
		listener.NotifyKeyForTest(b, ctx, channels.NewID("herd-teardown", 0))
		database.Close(ctx)
		wg.Wait()
	}
}

// shortModeGrid collapses a parameter grid to a single cheap arm under -short, the mode CI's
// test-benchmark-compile job runs on every push.
func shortModeGrid[T any](full []T, short []T) []T {
	if testing.Short() {
		return short
	}
	return full
}

// BenchmarkRefreshUserUnderHerd measures RefreshUserCount() - the check refreshUser performs on
// every inbound BLIP message - while W unrelated changes-feed waiters are parked and being
// broadcast to on tapNotifier.L.  ns/op should stay flat in `waiters`, since the check reads
// per-principal atomics and never touches that mutex.  The measured call is wrapped in the same
// exclusive lock refreshUser holds today (dbUserLock), modelled inline rather than by building a
// full BlipSyncContext.
func BenchmarkRefreshUserUnderHerd(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelError, base.KeyCache, base.KeyChanges)

	keyCountSizes := shortModeGrid([]int{1, 20000}, []int{1})
	roleCounts := shortModeGrid([]int{0, 5, 50}, []int{0})
	waiterCounts := shortModeGrid([]int{0, 100, 1000, 10000}, []int{10})

	for _, keyCountSize := range keyCountSizes {
		for _, roles := range roleCounts {
			for _, waiters := range waiterCounts {
				name := fmt.Sprintf("keys=%d/roles=%d/waiters=%d", keyCountSize, roles, waiters)
				b.Run(name, func(b *testing.B) {
					database, ctx := db.SetupTestDBWithCacheOptions(b, pinnedCacheOptions(50*time.Millisecond))
					collectionID := db.GetSingleDatabaseCollection(b, database.DatabaseContext).GetCollectionID()
					populateKeyCounts(b, ctx, database, collectionID, keyCountSize)

					user := newBenchUser(b, ctx, database, "bench-user", roles)
					measured := newBenchWaiter(b, database, user, roles+1)

					// startHerd takes ownership of database.Close - see its doc comment.
					teardown := startHerd(b, ctx, database, waiters)
					defer teardown()

					var dbUserLock sync.RWMutex // models BlipSyncContext.dbUserLock
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						dbUserLock.Lock()
						measured.RefreshUserCount()
						dbUserLock.Unlock()
					}
				})
			}
		}
	}
}

// BenchmarkChangeWaiterWakeCost measures ChangeWaiter.Wait for a production-shape waiter whose
// tracked principal key is bumped continuously, so the predicate is satisfied on essentially every
// call - the "already changed, return" cost rather than the park-and-wake cost.  This is the path
// where the user-count refresh no longer takes a second tapNotifier.L acquisition after
// listener.Wait has returned.
//
// Ignore allocs/op here: Go's allocation counters are process-wide, so the driver goroutine's own
// notifyKey loop (which logs, and so allocates) is attributed to whatever op is measured alongside
// it.  ns/op is unaffected and is the figure to read.
func BenchmarkChangeWaiterWakeCost(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelError, base.KeyCache, base.KeyChanges)

	keyCountSizes := shortModeGrid([]int{1, 20000}, []int{1})
	roleCounts := shortModeGrid([]int{0, 5}, []int{0})

	for _, keyCountSize := range keyCountSizes {
		for _, roles := range roleCounts {
			name := fmt.Sprintf("keys=%d/roles=%d", keyCountSize, roles)
			b.Run(name, func(b *testing.B) {
				database, ctx := db.SetupTestDBWithCacheOptions(b, pinnedCacheOptions(50*time.Millisecond))
				collectionID := db.GetSingleDatabaseCollection(b, database.DatabaseContext).GetCollectionID()
				populateKeyCounts(b, ctx, database, collectionID, keyCountSize)

				user := newBenchUser(b, ctx, database, "bench-user", roles)
				measured := newBenchWaiter(b, database, user, roles+1)
				userKey := channels.NewID(database.MetadataKeys.UserKey(user.Name()), 0)
				listener := database.GetMutationListener(b)

				driverStop := make(chan struct{})
				var driverWG sync.WaitGroup
				driverWG.Add(1)
				go func() {
					defer driverWG.Done()
					for {
						select {
						case <-driverStop:
							return
						default:
							listener.NotifyKeyForTest(b, ctx, userKey)
						}
					}
				}()

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					measured.Wait(ctx)
				}
				b.StopTimer()

				close(driverStop)
				driverWG.Wait()
				database.Close(ctx)
			})
		}
	}
}

// BenchmarkNotifyBroadcastWake measures notifyKey itself - the caching feed's write side - with W
// production-shape waiters parked on the same principal key and woken by every call.  This is a
// guard rail, not a win: the dual-write adds one atomic store inside notifyKey's existing
// tapNotifier.L critical section.  A regression at high waiter counts means the design needs
// revisiting.
func BenchmarkNotifyBroadcastWake(b *testing.B) {
	base.SetUpBenchmarkLogging(b, base.LevelError, base.KeyCache, base.KeyChanges)

	roleCounts := shortModeGrid([]int{0, 5}, []int{0})
	waiterCounts := shortModeGrid([]int{100, 1000, 10000}, []int{10})

	for _, roles := range roleCounts {
		for _, waiters := range waiterCounts {
			name := fmt.Sprintf("roles=%d/waiters=%d", roles, waiters)
			b.Run(name, func(b *testing.B) {
				database, ctx := db.SetupTestDBWithCacheOptions(b, pinnedCacheOptions(50*time.Millisecond))
				collectionID := db.GetSingleDatabaseCollection(b, database.DatabaseContext).GetCollectionID()
				populateKeyCounts(b, ctx, database, collectionID, 20000)

				user := newBenchUser(b, ctx, database, "bench-user", roles)
				userKey := channels.NewID(database.MetadataKeys.UserKey(user.Name()), 0)
				listener := database.GetMutationListener(b)

				// W waiters sharing one user - the production shape of many connections
				// replicating for the same account - parked in Wait on the exact key about to
				// be hammered.  Each checks `stop` before every Wait() for the reason given in
				// startHerd's teardown note.
				stop := make(chan struct{})
				var wg sync.WaitGroup
				for range waiters {
					w := newBenchWaiter(b, database, user, roles+1)
					wg.Add(1)
					go func() {
						defer wg.Done()
						for {
							select {
							case <-stop:
								return
							default:
							}
							w.Wait(ctx)
						}
					}()
				}

				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					listener.NotifyKeyForTest(b, ctx, userKey)
				}
				b.StopTimer()

				close(stop)
				// One more lock-held broadcast before Close - see startHerd for why Close's own
				// broadcast can't be trusted to release every parked waiter.
				listener.NotifyKeyForTest(b, ctx, userKey)
				database.Close(ctx)
				wg.Wait()
			})
		}
	}
}
