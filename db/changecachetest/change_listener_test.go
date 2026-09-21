/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package changecachetest

import (
	"log"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/db"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestUserWaiter(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	// Create user
	username := "bob"
	authenticator := database.Authenticator(ctx)
	require.NotNil(t, authenticator, "db.Authenticator(db.Ctx) returned nil")
	user, err := authenticator.NewUser(username, "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new user")

	// Create the user waiter (note: user hasn't been saved yet)
	log.Printf("Saved user")
	userDb := &db.Database{DatabaseContext: database.DatabaseContext}
	userDb.SetUserForTest(t, user)
	userWaiter := userDb.NewUserWaiter()
	assert.False(t, userWaiter.RefreshUserCount())

	// Save user
	err = authenticator.Save(user)
	require.NoError(t, err, "Error saving user")

	// Wait for notify from initial save
	db.WaitForUserWaiterChange(t, userWaiter)

	// Update the user to grant new channel
	updatedUser := auth.PrincipalConfig{
		Name:             &username,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, _, err = database.UpdatePrincipal(ctx, &updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notification from grant
	db.WaitForUserWaiterChange(t, userWaiter)
}

func TestUserWaiterForRoleChange(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	// Create role
	roleName := "good_egg"
	authenticator := database.Authenticator(ctx)
	require.NotNil(t, authenticator, "db.Authenticator(ctx) returned nil")
	role, err := authenticator.NewRole(roleName, channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new role")
	require.NoError(t, authenticator.Save(role))

	// Create user
	username := "bob"
	require.NotNil(t, authenticator, "db.Authenticator(ctx) returned nil")
	user, err := authenticator.NewUser(username, "letmein", nil)
	require.NoError(t, err, "Error creating new user")

	// Create the user waiter (note: user hasn't been saved yet)
	userDb := &db.Database{DatabaseContext: database.DatabaseContext}
	userDb.SetUserForTest(t, user)
	userWaiter := userDb.NewUserWaiter()
	isChanged := userWaiter.RefreshUserCount()
	assert.False(t, isChanged)

	// Save user
	err = authenticator.Save(user)
	require.NoError(t, err, "Error saving user")

	// Wait for notify from initial save
	db.WaitForUserWaiterChange(t, userWaiter)

	// Update the user to grant role
	updatedUser := auth.PrincipalConfig{
		Name:              &username,
		ExplicitRoleNames: base.SetOf(roleName),
	}
	_, _, err = database.UpdatePrincipal(ctx, &updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notify from updated user
	db.WaitForUserWaiterChange(t, userWaiter)

	// Retrieve the user.  This will trigger a user update to move ExplicitRoles->roles
	userRefresh, err := authenticator.GetUser(username)
	require.NoError(t, err, "Error retrieving user")

	// Wait for notify from retrieval
	db.WaitForUserWaiterChange(t, userWaiter)

	// Update the waiter with the current user (adds role to waiter.UserKeys)
	userWaiter.RefreshUserKeys(userRefresh, database.MetadataKeys)

	// Update the role to grant a new channel
	updatedRole := auth.PrincipalConfig{
		Name:             &roleName,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, _, err = database.UpdatePrincipal(ctx, &updatedRole, false, true)
	require.NoError(t, err, "Error updating role")

	// Wait for user notification of updated role
	db.WaitForUserWaiterChange(t, userWaiter)
}

// TestUserWaiterForUserDelete ensures that deleting a user notifies the change listener.  A deletion
// that isn't notified leaves running feeds serving a user that no longer exists.
func TestUserWaiterForUserDelete(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	const username = "bob"
	authenticator := database.Authenticator(ctx)
	user, err := authenticator.NewUser(username, "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new user")

	// Create the waiter before the save, so the save's notification can't land before the waiter
	// takes its baseline count
	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(t, err)
	userWaiter := userDb.NewUserWaiter()

	require.NoError(t, authenticator.Save(user), "Error saving user")

	// Wait for notify from the save, so the next wait can only be satisfied by the delete
	db.WaitForUserWaiterChange(t, userWaiter)

	require.NoError(t, authenticator.DeleteUser(user), "Error deleting user")
	db.WaitForUserWaiterChange(t, userWaiter)
}

// TestUserWaiterForRolePurge ensures that a purged role notifies the change listener.  A non-purge
// role delete writes a tombstone (a mutation), but purge is a true deletion.
func TestUserWaiterForRolePurge(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	const roleName = "good_egg"
	authenticator := database.Authenticator(ctx)
	role, err := authenticator.NewRole(roleName, channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new role")
	require.NoError(t, authenticator.Save(role))

	const username = "bob"
	user, err := authenticator.NewUser(username, "letmein", nil)
	require.NoError(t, err, "Error creating new user")
	user.SetExplicitRoles(channels.AtSequence(base.SetOf(roleName), 1), 1)

	// Create the waiter before the save, so the save's notification can't land before the waiter
	// takes its baseline count
	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(t, err)
	userWaiter := userDb.NewUserWaiter()

	require.NoError(t, authenticator.Save(user), "Error saving user")
	db.WaitForUserWaiterChange(t, userWaiter)

	// Retrieving the user moves ExplicitRoles->roles, which is another user write to wait out
	user, err = authenticator.GetUser(username)
	require.NoError(t, err, "Error retrieving user")
	require.True(t, user.RoleNames().Contains(roleName))
	db.WaitForUserWaiterChange(t, userWaiter)

	// Add the role to the waiter's keys.  RefreshUserKeys re-baselines the count, so the next wait
	// can only be satisfied by the purge.
	userWaiter.RefreshUserKeys(user, database.MetadataKeys)

	require.NoError(t, database.DeleteRole(ctx, roleName, true), "Error purging role")
	db.WaitForUserWaiterChange(t, userWaiter)
}

// TestChangeWaiterWakeObservesUserCount pins the happens-before edge between notifyKey's principal
// counter store and tapNotifier.Broadcast: a waiter returning WaiterHasChanges from a principal
// notify must already observe the new user count with no polling and no retry.  If the store were
// ever moved to after the Broadcast, a woken BLIP connection could reload against a stale count and
// keep serving the old channel set.
func TestChangeWaiterWakeObservesUserCount(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	const username = "bob"
	authenticator := database.Authenticator(ctx)
	user, err := authenticator.NewUser(username, "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	user.SetExplicitRoles(channels.AtSequence(base.SetOf("role1", "role2"), 1), 1)

	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(t, err)
	waiter := userDb.NewUserWaiter()

	listener := database.GetMutationListener(t)
	userKey := channels.NewID(database.MetadataKeys.UserKey(username), 0)

	for range 100 {
		before := waiter.CurrentUserCount()
		done := make(chan uint32, 1)
		go func() { done <- waiter.Wait(ctx) }()

		// Give the waiter a chance to park before notifying.  If it hasn't parked yet, Wait's
		// count check just returns immediately instead - also a correct outcome, since either
		// path goes through the same lastUserCount refresh below.
		time.Sleep(time.Millisecond)
		listener.NotifyKeyForTest(t, ctx, userKey)

		require.Equal(t, db.WaiterHasChanges, <-done)
		// No Eventually, no retry: the value must already be visible the instant Wait returns.
		require.Greater(t, waiter.CurrentUserCount(), before)
	}
}

// TestUserWaiterConcurrentRefreshRace races readers spinning RefreshUserCount and feed waiters
// parked in Wait against a stream of principal notifies, and asserts no reader ever observes the
// count moving backwards, and all readers converge on the final value.
//
// Each reader owns exactly one ChangeWaiter and publishes its latest observed count through a
// dedicated atomic slot - ChangeWaiter's fields are not synchronized by design,
// so only the goroutine that owns a waiter may call its methods, and the atomic slot is what lets
// this test's main goroutine observe convergence without a second, racing touch on the waiter.
func TestUserWaiterConcurrentRefreshRace(t *testing.T) {
	const (
		readers = 8
		parked  = 16
		writes  = 200
	)
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)
	listener := database.GetMutationListener(t)

	const username = "bob"
	authenticator := database.Authenticator(ctx)
	user, err := authenticator.NewUser(username, "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	user.SetExplicitRoles(channels.AtSequence(base.SetOf("role1", "role2", "role3"), 1), 1)

	userKey := channels.NewID(database.MetadataKeys.UserKey(username), 0)

	newWaiter := func() *db.ChangeWaiter {
		userDb, err := db.GetDatabase(database.DatabaseContext, user)
		require.NoError(t, err)
		return userDb.NewUserWaiter()
	}

	stop := make(chan struct{})

	observed := make([]atomic.Uint64, readers)
	var rg sync.WaitGroup
	for i := range readers {
		w := newWaiter()
		slot := &observed[i]
		rg.Add(1)
		go func() {
			defer rg.Done()
			var last uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				w.RefreshUserCount()
				cur := w.CurrentUserCount()
				assert.GreaterOrEqual(t, cur, last, "user count went backwards")
				last = cur
				slot.Store(cur)
			}
		}()
	}

	var pg sync.WaitGroup
	for range parked {
		w := newWaiter()
		pg.Add(1)
		go func() {
			defer pg.Done()
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

	for range writes {
		listener.NotifyKeyForTest(t, ctx, userKey)
	}
	_, principalCounts := listener.PrincipalCountsForTest(t)
	final := principalCounts[userKey]

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		for i := range observed {
			assert.Equal(c, final, observed[i].Load(), "reader %d", i)
		}
	}, 10*time.Second, 5*time.Millisecond)

	close(stop)
	rg.Wait()
	// notifyKey broadcasts inline, so this both unblocks any parked waiters and lets them observe
	// stop having been closed.
	listener.NotifyKeyForTest(t, ctx, userKey)
	pg.Wait()
}

// TestUserWaiterNotifyCounterInvariant asserts that after any principal notify, keyCounts and
// principalCounts agree for that key, and both equal the listener's global counter - the property
// that makes an unsynchronized multi-key max() safe (see the design notes).
func TestUserWaiterNotifyCounterInvariant(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)
	listener := database.GetMutationListener(t)

	userKey := channels.NewID(database.MetadataKeys.UserKey("bob"), 0)
	roleKey := channels.NewID(database.MetadataKeys.RoleKey("good_egg"), 0)

	for _, key := range []channels.ID{userKey, roleKey, userKey} {
		listener.NotifyKeyForTest(t, ctx, key)
		keyCounts, principalCounts := listener.PrincipalCountsForTest(t)
		global := listener.CounterForTest(t)
		require.Equal(t, global, keyCounts[key])
		require.Equal(t, global, principalCounts[key], "principal counter must store the global counter")
		for k, v := range principalCounts {
			require.Equal(t, keyCounts[k], v, "divergence for key %v", k)
		}
	}

	// A *channel* whose name happens to collide with a principal doc key in collection 0
	// (principalDocCollectionIDForChannelID == base.DefaultCollectionID == 0, and
	// channels.IsValidChannel only rejects empty/comma names) must go through Notify, never
	// notifyKey - so it must NOT create a principalCounts entry.  Documented as benign: the only
	// consequence is a spurious user-count check that finds nothing changed, never a missed one.
	collidingKey := channels.NewID(database.MetadataKeys.UserKey("collision-victim"), 0)
	listener.Notify(ctx, channels.SetOfNoValidate(collidingKey))
	keyCounts, principalCounts := listener.PrincipalCountsForTest(t)
	_, hasPrincipalCounter := principalCounts[collidingKey]
	require.False(t, hasPrincipalCounter, "Notify must not create a principalCounts entry")
	require.NotZero(t, keyCounts[collidingKey], "Notify should still have advanced keyCounts")
}

// TestUserWaiterBeforeFirstNotify asserts that two independent waiters registered against a
// principal key that has never been notified both observe the first notification identically.  A
// per-waiter private counter (rather than a shared, listener-owned one) would pass with a single
// waiter and fail here.
func TestUserWaiterBeforeFirstNotify(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)
	listener := database.GetMutationListener(t)

	// User object only - never saved, so its principal key has never been notified.
	user, err := database.Authenticator(ctx).NewUser("bob", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)

	newWaiter := func() *db.ChangeWaiter {
		userDb := &db.Database{DatabaseContext: database.DatabaseContext}
		userDb.SetUserForTest(t, user)
		return userDb.NewUserWaiter()
	}
	w1 := newWaiter()
	w2 := newWaiter()

	require.False(t, w1.RefreshUserCount())
	require.False(t, w2.RefreshUserCount())
	require.Zero(t, w1.CurrentUserCount())

	userKey := channels.NewID(database.MetadataKeys.UserKey("bob"), 0)
	listener.NotifyKeyForTest(t, ctx, userKey)

	require.True(t, w1.RefreshUserCount())
	require.True(t, w2.RefreshUserCount())
	require.Equal(t, w1.CurrentUserCount(), w2.CurrentUserCount())
}

// TestUserWaiterRoleRebaselineMonotonic asserts that RefreshUserKeys never moves the reported count
// backwards across a key-set change, for both a role ADD and a role REMOVAL.  The removal half is
// the case where a non-global (independent per-principal) counter space would break the
// unsynchronized max() over the tracked keys, and it is not exercised anywhere else in the suite.
func TestUserWaiterRoleRebaselineMonotonic(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	roleName := "bob_role"
	username := "bob"
	authenticator := database.Authenticator(ctx)
	role, err := authenticator.NewRole(roleName, channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(role))

	user, err := authenticator.NewUser(username, "letmein", nil)
	require.NoError(t, err)

	userDb, err := db.GetDatabase(database.DatabaseContext, user)
	require.NoError(t, err)
	userWaiter := userDb.NewUserWaiter()

	require.NoError(t, authenticator.Save(user), "Error saving user")
	db.WaitForUserWaiterChange(t, userWaiter)

	_, _, err = database.UpdatePrincipal(ctx, &auth.PrincipalConfig{
		Name:              &username,
		ExplicitRoleNames: base.SetOf(roleName),
	}, true, true)
	require.NoError(t, err)
	db.WaitForUserWaiterChange(t, userWaiter)

	// Retrieving the user moves ExplicitRoles -> roles
	userWithRole, err := authenticator.GetUser(username)
	require.NoError(t, err)
	db.WaitForUserWaiterChange(t, userWaiter)

	// GreaterOrEqual, not Equal: adding a key to a max-over-set can only raise or hold the max, so
	// this holds unconditionally - it doesn't rely on the role's counter (from its much earlier
	// creation) actually being lower than the user's, which is a DCP-ordering assumption, not a
	// property of RefreshUserKeys.
	beforeRoleAdd := userWaiter.CurrentUserCount()
	userWaiter.RefreshUserKeys(userWithRole, database.MetadataKeys)
	require.Len(t, userWaiter.UserKeysCopyForTest(t), 2, "user key + role key")
	require.GreaterOrEqual(t, userWaiter.CurrentUserCount(), beforeRoleAdd,
		"RefreshUserKeys re-baseline moved the count backwards on a role add")

	// A role doc update must be observed now that the role key is tracked
	_, _, err = database.UpdatePrincipal(ctx, &auth.PrincipalConfig{
		Name:             &roleName,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}, false, true)
	require.NoError(t, err)
	db.WaitForUserWaiterChange(t, userWaiter)

	// Remove the role - the tracked key set shrinks back down to just the user key.
	_, _, err = database.UpdatePrincipal(ctx, &auth.PrincipalConfig{
		Name:              &username,
		ExplicitRoleNames: base.Set{},
	}, true, true)
	require.NoError(t, err)
	db.WaitForUserWaiterChange(t, userWaiter)

	userWithoutRole, err := authenticator.GetUser(username)
	require.NoError(t, err)
	require.Empty(t, userWithoutRole.RoleNames())

	beforeRoleRemove := userWaiter.CurrentUserCount()
	userWaiter.RefreshUserKeys(userWithoutRole, database.MetadataKeys)
	require.Len(t, userWaiter.UserKeysCopyForTest(t), 1, "role key dropped")
	require.GreaterOrEqual(t, userWaiter.CurrentUserCount(), beforeRoleRemove,
		"RefreshUserKeys re-baseline moved the count backwards on a role removal")
}

// TestUserWaiterNilUser asserts that a nil-user (admin/no-principal) waiter never panics on the
// user-count paths, always reports a zero user count, and still wakes on an ordinary channel notify.
func TestUserWaiterNilUser(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)
	collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
	collectionID := collection.GetCollectionID()

	chans := channels.SetOfNoValidate(channels.NewID("ABC", collectionID))
	waiter := database.GetMutationListener(t).NewWaiterWithChannels(chans, nil, false)

	require.False(t, waiter.RefreshUserCount())
	require.Zero(t, waiter.CurrentUserCount())
	require.False(t, waiter.RefreshUserCount()) // idempotent

	successChan := make(chan uint32, 1)
	go func() { successChan <- waiter.Wait(ctx) }()

	db.WriteDirect(t, collection, []string{"ABC"}, 1)

	select {
	case result := <-successChan:
		require.Equal(t, db.WaiterHasChanges, result)
	case <-time.After(3 * time.Second):
		t.Fatal("No notification after 3 seconds")
	}

	require.False(t, waiter.RefreshUserCount())
	require.Zero(t, waiter.CurrentUserCount())
}

// TestUserWaiterSharedRoleFanout asserts that two users sharing one role both observe a role
// update - the production shape is one role shared by many users, and a per-connection cache keyed
// on anything but the role's own principal key would let one of them miss the update.
func TestUserWaiterSharedRoleFanout(t *testing.T) {
	database, ctx := db.SetupTestDB(t)
	defer database.Close(ctx)

	roleName := "shared-role"
	authenticator := database.Authenticator(ctx)
	role, err := authenticator.NewRole(roleName, channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(role))

	// setUpUser grants roleName to a new user and returns a waiter tracking it.  SetExplicitRoles
	// alone isn't enough for RoleNames() to report the role: it also sets RoleInvalSeq non-zero,
	// and RoleNames() returns nil while that's set (auth/user.go).  Only a save followed by a
	// GetUser round trip resolves ExplicitRoles into RolesSince_ and clears the invalidation, so
	// only after that does the role's key end up in the waiter's tracked set.
	setUpUser := func(username string) *db.ChangeWaiter {
		user, err := authenticator.NewUser(username, "letmein", nil)
		require.NoError(t, err)
		user.SetExplicitRoles(channels.AtSequence(base.SetOf(roleName), 1), 1)

		// Create the waiter before the save, so the save's notification can't land before the
		// waiter takes its baseline count.
		userDb, err := db.GetDatabase(database.DatabaseContext, user)
		require.NoError(t, err)
		waiter := userDb.NewUserWaiter()

		require.NoError(t, authenticator.Save(user))
		db.WaitForUserWaiterChange(t, waiter)

		// Retrieving the user moves ExplicitRoles -> roles, which is another user write to wait out
		user, err = authenticator.GetUser(username)
		require.NoError(t, err)
		require.True(t, user.RoleNames().Contains(roleName))
		db.WaitForUserWaiterChange(t, waiter)

		// Now that RoleNames() resolves, add the role to the waiter's tracked keys.
		waiter.RefreshUserKeys(user, database.MetadataKeys)
		return waiter
	}

	waiterA := setUpUser("alice")
	waiterB := setUpUser("bob")

	// Update the shared role - neither user's own document changes again from here.
	_, _, err = database.UpdatePrincipal(ctx, &auth.PrincipalConfig{
		Name:             &roleName,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}, false, true)
	require.NoError(t, err)

	// A per-waiter private counter would let one of these observe the update and the other miss it.
	db.WaitForUserWaiterChange(t, waiterA)
	db.WaitForUserWaiterChange(t, waiterB)
}
