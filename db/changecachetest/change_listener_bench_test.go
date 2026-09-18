// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

// Benchmarks for the CBG-5693 refreshUser/tapNotifier decoupling. See
// cbg5693_refreshuser_decouple_plan.md (repo root) for the full methodology, including why a
// caching-throughput macro run cannot show this win, why absolute ns/op numbers from this harness
// do not survive across sessions (only within-session, interleaved before/after deltas do), and the
// run protocol for a true before/after comparison against a pre-PR1 checkout.
//
// CI note: .github/workflows/ci.yml's test-benchmark-compile job runs `-short -bench=. -benchtime=1x
// -run '!'` over ./... on every push, so every benchmark here collapses to a single, cheap arm under
// testing.Short() and reliably tears down every goroutine it starts before returning.

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
// flipping can't change the wake cadence mid-benchmark.  (These benchmarks never write real
// documents or allocate sequences, so slow mode never actually engages - this is insurance against
// that changing, not a response to anything currently observed.)
func pinnedCacheOptions(interval time.Duration) db.CacheOptions {
	opts := db.DefaultCacheOptions()
	opts.BroadcastChangesInterval = interval
	opts.SkippedSequenceBroadcastInterval = interval
	return opts
}

// populateKeyCounts seeds keyCounts with n channel entries via the already-exported Notify, in a
// single tapNotifier.L acquisition for the whole batch.  Production carries ~20,000 resident
// channels; a 1-entry map makes a map-lookup-shaped check look cheaper than it is - the earlier
// perf study measured 20.0ns at 0 roles rising to 763ns at 50 roles against a 20,000-entry map.
// Note this only matters for a true before/after comparison: the current (post-PR1) code no longer
// reads keyCounts for the user-count check at all, so this axis should show flat, identical numbers
// on its own - which is itself a result worth confirming.
func populateKeyCounts(b *testing.B, ctx context.Context, database *db.Database, collectionID uint32, n int) {
	b.Helper()
	listener := database.GetMutationListener(b)
	keys := make([]channels.ID, 0, n)
	for i := range n {
		keys = append(keys, channels.NewID(fmt.Sprintf("bench-chan-%d", i), collectionID))
	}
	listener.Notify(ctx, channels.SetOfNoValidate(keys...))
}

// newBenchUser returns a saved user with `roles` resolved roles, so a waiter built from it has the
// production shape: 1+roles tracked principal keys, each with its own resolved counter.
// SetExplicitRoles alone isn't enough - it leaves RoleInvalSeq non-zero, and RoleNames() reports
// nothing until a save-then-GetUser round trip resolves it (auth/user.go) - so this always does
// both, even for roles == 0, to keep every arm's setup shape uniform.
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

// newBenchWaiter builds a waiter for user and asserts it has the production shape: 1+roles tracked
// principal keys.  That assertion is load-bearing, not decorative - a nil-user waiter takes one
// tapNotifier.L acquisition per wake where a real one takes two (or, post-PR1, zero vs the
// lock-free path), and an earlier study's benchmarks were invalidated by silently using the
// half-traffic nil-user shape.  This assertion is what stops that regressing again.
func newBenchWaiter(b *testing.B, database *db.Database, user auth.User, expectedKeys int) *db.ChangeWaiter {
	b.Helper()
	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(b, err)
	w := userDb.NewUserWaiter()
	require.Len(b, w.UserKeysForTest(b), expectedKeys)
	return w
}

// startHerd parks `waiters` independent ChangeWaiters, each watching its own distinct, never-
// individually-satisfied channel key, and drives a background stream of ordinary channel-key
// Notify calls (mimicking the caching feed writing documents) so the pinned-cadence broadcaster
// ticker always has something to broadcast. Every tick, every parked waiter wakes, contends for
// tapNotifier.L in turn, finds its own channel unaffected, and re-parks - reproducing the "wasted
// wake" cost (measured in the earlier perf study's A19) that this file's money benchmark measures
// the impact of on an unrelated per-BLIP-message check sharing the same mutex.
//
// There is deliberately no per-round WaitGroup barrier synchronising the waiters: they are load,
// not the measurement, and a barrier that acks before re-blocking would understate park/unpark and
// prevent overlapping broadcasts, which is exactly the behaviour under test.
//
// Releasing every parked herd waiter reliably is the tricky part, and getting it wrong is a real,
// previously-hit hang (confirmed via a goroutine dump during development), not a theoretical one.
// changeListener.Stop's own teardown calls tapNotifier.Broadcast() WITHOUT holding tapNotifier.L
// (change_listener.go's Stop) - deliberately, per sync.Cond's contract, which allows it. But that
// means it can race with a goroutine that is concurrently past its own predicate check and about to
// park: Broadcast() only wakes goroutines already registered as parked *at the instant it runs*, and
// a goroutine mid-way through Cond.Wait's internal "unlock and register" step can miss it and then
// park forever, since nothing broadcasts again afterwards. This is a property of Stop() itself, not
// something this change introduced - these benchmarks are just the first thing to stress it hard
// enough (many goroutines continuously cycling through Wait() right up to teardown) to expose it.
//
// notifyKey does not have this problem: it holds tapNotifier.L for its entire write-then-broadcast,
// so by the time it broadcasts, every other goroutine touching that lock has either already returned
// or is fully, atomically parked - there is no "in between" for it to race with. So teardown releases
// the herd itself, reliably, instead of trusting Stop()'s broadcast to do it: bump every herd
// waiter's own key first (Notify - no broadcast needed for this, just the state change), then issue
// one notifyKey call to broadcast reliably, so every waiter wakes and finds its own key already
// satisfied. `stop` still guards the small window where a goroutine is between Wait() calls (not
// parked at all) rather than relying on it seeing a change that hasn't happened yet.
//
// The returned teardown function owns the *entire* shutdown sequence. Call it exactly once, and do
// not also register a separate database.Close for the same database.
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

// shortModeGrid collapses a benchmark's parameter grid to a single, cheap arm under -short - the
// mode ci.yml's test-benchmark-compile job always runs, on every push.
func shortModeGrid[T any](full []T, short []T) []T {
	if testing.Short() {
		return short
	}
	return full
}

// BenchmarkRefreshUserUnderHerd is the money benchmark: it measures ns per RefreshUserCount() call
// on a connection's own waiter - the check userBlipHandler's refreshUser performs on every single
// inbound BLIP message (db/blip_handler.go:128-135) - while W other, unrelated changes-feed waiters
// are parked and being broadcast to on the shared tapNotifier.L. The measured call is wrapped in the
// same exclusive lock/unlock pair refreshUser itself uses today (dbUserLock,
// db/blip_sync_context.go:106) - modelled inline here rather than by constructing a full
// BlipSyncContext, since only that lock/unlock pair affects the measurement, not the surrounding
// connection plumbing. That lock is NOT what PR1 removes (that's PR3's job); it's kept here
// specifically so this benchmark's ns/op is the real, current per-BLIP-message cost as it stands
// today (PR1 landed, PR3 not yet), not just the bare counter-check cost in isolation.
//
// Expected shape: before PR1, ns/op rises with `waiters` (the check queues behind the herd's
// contention on tapNotifier.L); after PR1, ns/op should stay flat in `waiters`, since the check
// never touches that mutex.  That shape - not any single absolute number - is the claim to publish;
// see the plan doc's benchmark run protocol for why.
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

					var dbUserLock sync.RWMutex // models db/blip_sync_context.go:106
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

// BenchmarkChangeWaiterWakeCost measures ChangeWaiter.Wait for a production-shape waiter whose own
// tracked principal key is being bumped continuously by a background goroutine, so the predicate is
// satisfied (or very close to it) on essentially every call - i.e. the "already changed, return"
// cost, not the separate cost of actually parking and being woken (which BenchmarkRefreshUserUnderHerd's
// herd already exercises, from the other side).
//
// Before PR1, this call went on to take a SECOND, separate tapNotifier.L acquisition via
// CurrentCount(waiter.userKeys) after listener.Wait had already returned; that second acquisition
// is gone from the current code (see ChangeWaiter.Wait in change_listener.go), so this benchmark's
// before/after delta - once run against a pre-PR1 checkout per the plan doc's protocol - is exactly
// that removed acquisition's cost.
//
// The driver races the measured loop rather than being synchronised with it, so parking is possible
// in principle if the measured goroutine ever gets ahead of it; the design accepts that rather than
// entangling the measurement with the driver's own notifyKey cost (which BenchmarkNotifyBroadcastWake
// measures separately).
//
// Ignore this benchmark's allocs/op: Go's allocation counters are process-wide, not per-goroutine,
// so the driver's own tight, unthrottled notifyKey loop (which logs via base.DebugfCtx, allocating
// on every call) gets attributed to whatever op happens to be measured at the same moment. The
// ns/op figure is unaffected by this and is what to read.
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

// BenchmarkNotifyBroadcastWake measures notifyKey's own cost - the caching feed's write side - with
// W production-shape waiters parked on the SAME principal key and woken by every call (notifyKey
// broadcasts immediately, unlike the bulk channel Notify path, which waits for the ticker). This is
// a GUARD RAIL, not a win: PR1's dual-write adds one atomic store inside notifyKey's existing
// tapNotifier.L critical section. A real regression here at high waiter counts means the design
// needs revisiting - see the plan doc's benchmark section.
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

				// W production-shape waiters sharing the same user - the production shape of
				// many connections replicating for the same account, all sharing one role or
				// user key - parked in Wait, watching the exact key about to be hammered. Each
				// checks `stop` before every Wait() call (not just relying on WaiterClosed) for
				// the same reason startHerd does: a goroutine caught between Wait() calls when
				// the teardown broadcast fires would otherwise park forever - see startHerd's
				// doc comment for the mechanism and why this was a real, previously-hit hang.
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
				// One more reliable, lock-held broadcast before Close() - see startHerd's doc
				// comment for why Close()'s own teardown broadcast can't be trusted alone to
				// release every parked waiter.
				listener.NotifyKeyForTest(b, ctx, userKey)
				database.Close(ctx)
				wg.Wait()
			})
		}
	}
}
