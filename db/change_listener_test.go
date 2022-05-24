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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserWaiter(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	db := setupTestDB(t)
	defer db.Close()

	ctx := base.TestCtx(t)

	// Create user
	username := "bob"
	authenticator := db.Authenticator(ctx)
	require.NotNil(t, authenticator, "db.Authenticator(base.TestCtx(t)) returned nil")
	user, err := authenticator.NewUser(username, "letmein", channels.SetOf(t, "ABC"))
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
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the user to grant new channel
	updatedUser := PrincipalConfig{
		Name:             &username,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, err = db.UpdatePrincipal(ctx, updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notification from grant
	require.True(t, WaitForUserWaiterChange(userWaiter))

}

func TestUserWaiterForRoleChange(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache)

	db := setupTestDB(t)
	defer db.Close()

	ctx := base.TestCtx(t)

	// Create role
	roleName := "good_egg"
	authenticator := db.Authenticator(ctx)
	require.NotNil(t, authenticator, "db.Authenticator(base.TestCtx(t)) returned nil")
	role, err := authenticator.NewRole(roleName, channels.SetOf(t, "ABC"))
	require.NoError(t, err, "Error creating new role")
	require.NoError(t, authenticator.Save(role))

	// Create user
	username := "bob"
	require.NotNil(t, authenticator, "db.Authenticator(base.TestCtx(t)) returned nil")
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
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the user to grant role
	updatedUser := PrincipalConfig{
		Name:              &username,
		ExplicitRoleNames: []string{roleName},
	}
	_, err = db.UpdatePrincipal(ctx, updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notify from updated user
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Retrieve the user.  This will trigger a user update to move ExplicitRoles->roles
	userRefresh, err := authenticator.GetUser(username)
	require.NoError(t, err, "Error retrieving user")

	// Wait for notify from retrieval
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the waiter with the current user (adds role to waiter.UserKeys)
	userWaiter.RefreshUserKeys(userRefresh)

	// Update the role to grant a new channel
	updatedRole := PrincipalConfig{
		Name:             &roleName,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, err = db.UpdatePrincipal(ctx, updatedRole, false, true)
	require.NoError(t, err, "Error updating role")

	// Wait for user notification of updated role
	require.True(t, WaitForUserWaiterChange(userWaiter))
}
