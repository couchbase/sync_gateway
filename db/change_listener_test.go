/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"log"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestUserWaiter(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	// Create user
	username := "bob"
	authenticator := db.Authenticator(ctx)
	require.NotNil(t, authenticator, "db.Authenticator(db.Ctx) returned nil")
	user, err := authenticator.NewUser(username, "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new user")

	// Create the user waiter (note: user hasn't been saved yet)
	log.Printf("Saved user")
	userDb := &Database{
		user:            user,
		DatabaseContext: db.DatabaseContext,
	}
	userWaiter := userDb.NewUserWaiter()
	assert.False(t, userWaiter.RefreshUserCount())

	// Save user
	err = authenticator.Save(user)
	require.NoError(t, err, "Error saving user")

	// Wait for notify from initial save
	WaitForUserWaiterChange(t, userWaiter)

	// Update the user to grant new channel
	updatedUser := auth.PrincipalConfig{
		Name:             &username,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, _, err = db.UpdatePrincipal(ctx, &updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notification from grant
	WaitForUserWaiterChange(t, userWaiter)
}

func TestUserWaiterForRoleChange(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	// Create role
	roleName := "good_egg"
	authenticator := db.Authenticator(ctx)
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
	userDb := &Database{
		user:            user,
		DatabaseContext: db.DatabaseContext,
	}
	userWaiter := userDb.NewUserWaiter()
	isChanged := userWaiter.RefreshUserCount()
	assert.False(t, isChanged)

	// Save user
	err = authenticator.Save(user)
	require.NoError(t, err, "Error saving user")

	// Wait for notify from initial save
	WaitForUserWaiterChange(t, userWaiter)

	// Update the user to grant role
	updatedUser := auth.PrincipalConfig{
		Name:              &username,
		ExplicitRoleNames: base.SetOf(roleName),
	}
	_, _, err = db.UpdatePrincipal(ctx, &updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notify from updated user
	WaitForUserWaiterChange(t, userWaiter)

	// Retrieve the user.  This will trigger a user update to move ExplicitRoles->roles
	userRefresh, err := authenticator.GetUser(username)
	require.NoError(t, err, "Error retrieving user")

	// Wait for notify from retrieval
	WaitForUserWaiterChange(t, userWaiter)

	// Update the waiter with the current user (adds role to waiter.UserKeys)
	userWaiter.RefreshUserKeys(userRefresh, db.MetadataKeys)

	// Update the role to grant a new channel
	updatedRole := auth.PrincipalConfig{
		Name:             &roleName,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, _, err = db.UpdatePrincipal(ctx, &updatedRole, false, true)
	require.NoError(t, err, "Error updating role")

	// Wait for user notification of updated role
	WaitForUserWaiterChange(t, userWaiter)
}

// TestChangeWaiterWaitAfterListenerStop asserts that a ChangeWaiter.Wait() that begins after the
// changeListener has been stopped returns, rather than blocking forever.
//
// changeListener.Stop() issues a single tapNotifier.Broadcast() to release waiters that are already
// parked, and terminates the broadcaster goroutine.  A waiter that enters Wait() after that point has
// missed the broadcast: no further Notify/notifyKey/NotifyCheckForTermination will occur on a stopped
// listener, so nothing remains to wake it.  The listener.terminator check in changeListener.Wait() is
// evaluated only after tapNotifier.Wait() returns, so a closed terminator cannot release a goroutine
// that is already parked on the sync.Cond.
//
// This is the mechanism behind a permanently-parked GenerateChanges goroutine - a push replicator
// started against a DatabaseContext that is midway through Close() builds its changes feed on an
// already-stopped mutationListener.  Note that DatabaseContext._stopOnlineProcesses stops
// mutationListener before SGReplicateMgr, so any replication started during teardown hits exactly this.
func TestChangeWaiterWaitAfterListenerStop(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	waiter := db.mutationListener.NewWaiter([]channels.ID{channels.NewID("ABC", 0)}, false)

	// Stop the listener before waiting.  Close() will call this again during teardown, which is a no-op.
	db.mutationListener.Stop(ctx)

	// Run Wait() on a separate goroutine like a changes feed would be n the real world.
	// On failure the goroutine stays parked for the remainder of
	// the package run, which is the leak this test exists to catch.
	waitReturned := make(chan uint32, 1)
	go func() {
		waitReturned <- waiter.Wait(ctx)
	}()

	select {
	case response := <-waitReturned:
		assert.Equal(t, WaiterClosed, response, "Wait() after listener stop should report WaiterClosed")
	case <-time.After(10 * time.Second):
		assert.Fail(t, "ChangeWaiter.Wait() did not return after the changeListener was stopped - goroutine is parked permanently")
	}
}
