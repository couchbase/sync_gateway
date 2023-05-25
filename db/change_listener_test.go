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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the user to grant new channel
	updatedUser := auth.PrincipalConfig{
		Name:             &username,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, err = db.UpdatePrincipal(ctx, &updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notification from grant
	require.True(t, WaitForUserWaiterChange(userWaiter))

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
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the user to grant role
	updatedUser := auth.PrincipalConfig{
		Name:              &username,
		ExplicitRoleNames: base.SetOf(roleName),
	}
	_, err = db.UpdatePrincipal(ctx, &updatedUser, true, true)
	require.NoError(t, err, "Error updating user")

	// Wait for notify from updated user
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Retrieve the user.  This will trigger a user update to move ExplicitRoles->roles
	userRefresh, err := authenticator.GetUser(username)
	require.NoError(t, err, "Error retrieving user")

	// Wait for notify from retrieval
	require.True(t, WaitForUserWaiterChange(userWaiter))

	// Update the waiter with the current user (adds role to waiter.UserKeys)
	userWaiter.RefreshUserKeys(userRefresh, db.MetadataKeys)

	// Update the role to grant a new channel
	updatedRole := auth.PrincipalConfig{
		Name:             &roleName,
		ExplicitChannels: base.SetFromArray([]string{"ABC", "DEF"}),
	}
	_, err = db.UpdatePrincipal(ctx, &updatedRole, false, true)
	require.NoError(t, err, "Error updating role")

	// Wait for user notification of updated role
	require.True(t, WaitForUserWaiterChange(userWaiter))
}

// TestMutationStartHighSeq sure docs written before sync gateway start do not get cached
func TestMutationStartHighSeq(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("test requires import feed, which requies DCP")
	}

	bucket := base.GetTestBucket(t)

	doc1 := "doc1"
	// this is the revID if the document were imported
	revID1 := "1-2a9efe8178aa817f4414ae976aa032d9"
	_, err := bucket.GetSingleDataStore().Add(doc1, 0, rawDocNoMeta())
	require.NoError(t, err)

	db, ctx := setupTestDBForBucket(t, bucket)

	defer db.Close(ctx)

	collection := GetSingleDatabaseCollectionWithUser(t, db)

	require.Equal(t, int64(0), db.DbStats.Database().DCPReceivedCount.Value())

	doc2 := "doc2"
	revID2, _, err := collection.Put(base.TestCtx(t), doc2, Body{"key": "value"})
	require.NoError(t, err)
	_, ok := base.WaitForStat(func() int64 {
		return db.DbStats.Database().DCPReceivedCount.Value()
	}, 1)
	require.True(t, ok)

	_, exists := collection.revisionCache.Peek(base.TestCtx(t), doc2, revID2)
	require.True(t, exists)

	_, exists = collection.revisionCache.Peek(base.TestCtx(t), doc1, revID1)
	require.False(t, exists)

}
