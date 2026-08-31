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

// TestUserWaiterForUserDelete ensures that deleting a user notifies the change listener.  A deletion
// that isn't notified leaves running feeds serving a user that no longer exists.
func TestUserWaiterForUserDelete(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	const username = "bob"
	authenticator := db.Authenticator(ctx)
	user, err := authenticator.NewUser(username, "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new user")
	require.NoError(t, authenticator.Save(user), "Error saving user")

	userDb, err := GetDatabase(db.DatabaseContext, user)
	require.NoError(t, err)
	userWaiter := userDb.NewUserWaiter()

	// Wait for notify from the initial save, so the next wait can only be satisfied by the delete
	WaitForUserWaiterChange(t, userWaiter)

	require.NoError(t, authenticator.DeleteUser(user), "Error deleting user")
	WaitForUserWaiterChange(t, userWaiter)
}

// TestUserWaiterForRolePurge ensures that a purged role notifies the change listener.  A non-purge
// role delete writes a tombstone (a mutation), but purge is a true deletion.
func TestUserWaiterForRolePurge(t *testing.T) {
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	const roleName = "good_egg"
	authenticator := db.Authenticator(ctx)
	role, err := authenticator.NewRole(roleName, channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new role")
	require.NoError(t, authenticator.Save(role))

	const username = "bob"
	user, err := authenticator.NewUser(username, "letmein", nil)
	require.NoError(t, err, "Error creating new user")
	user.SetExplicitRoles(channels.AtSequence(base.SetOf(roleName), 1), 1)
	require.NoError(t, authenticator.Save(user), "Error saving user")

	// Reload the user so the waiter's role keys are populated
	user, err = authenticator.GetUser(username)
	require.NoError(t, err, "Error retrieving user")
	require.True(t, user.RoleNames().Contains(roleName))

	userDb, err := GetDatabase(db.DatabaseContext, user)
	require.NoError(t, err)
	userWaiter := userDb.NewUserWaiter()

	// Wait for notify from the user save, so the next wait can only be satisfied by the purge
	WaitForUserWaiterChange(t, userWaiter)

	require.NoError(t, db.DeleteRole(ctx, roleName, true), "Error purging role")
	WaitForUserWaiterChange(t, userWaiter)
}
