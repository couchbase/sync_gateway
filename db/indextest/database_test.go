/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package indextest

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestAllPrincipalIDs(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless bool
	}{
		{
			isServerless: false,
		},
		{
			isServerless: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestAllPrincipalIDs in Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			db, ctx := setupTestDBForBucketWithOptions(t, dbContextConfig)
			defer db.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(db.Bucket, testCase.isServerless)
			assert.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				assert.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

			db.Options.QueryPaginationLimit = 100
			db.ChannelMapper = channels.NewDefaultChannelMapper()
			authenticator := db.Authenticator(ctx)

			rolename1 := uuid.NewString()
			rolename2 := uuid.NewString()
			username := uuid.NewString()

			user1, err := authenticator.NewUser(username, "letmein", nil)
			assert.NoError(t, err)
			user1.SetExplicitRoles(channels.TimedSet{rolename1: channels.NewVbSimpleSequence(1), rolename2: channels.NewVbSimpleSequence(1)}, 1)
			assert.NoError(t, authenticator.Save(user1))

			role1, err := authenticator.NewRole(rolename1, nil)
			assert.NoError(t, err)
			assert.NoError(t, authenticator.Save(role1))

			role2, err := authenticator.NewRole(rolename2, nil)
			assert.NoError(t, err)
			assert.NoError(t, authenticator.Save(role2))

			err = db.DeleteRole(ctx, role2.Name(), false)
			assert.NoError(t, err)
			roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
			assert.NoError(t, err)
			assert.True(t, roleGet.IsDeleted())

			t.Log("user1:", user1.Name())
			t.Log("role1:", role1.Name())
			t.Log("role2:", role2.Name())

			// assert allprincipals still returns users and deleted roles
			users, roles, err := db.AllPrincipalIDs(ctx)
			assert.NoError(t, err)
			assert.ElementsMatch(t, []string{user1.Name()}, users)
			assert.ElementsMatch(t, []string{role1.Name(), role2.Name()}, roles)

			roles, err = db.GetRoleIDs(ctx, false)
			assert.NoError(t, err)
			assert.ElementsMatch(t, []string{role1.Name()}, roles)
		})
	}
}

func TestGetRoleIDs(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		t.Skip("This test is Couchbase Server and UseViews=false only")
	}

	testCases := []struct {
		isServerless   bool
		includeDeleted bool
		expectError    bool
	}{
		{
			isServerless:   false,
			includeDeleted: false,
			expectError:    false,
		},
		{
			isServerless:   false,
			includeDeleted: true,
			expectError:    true, // GetRoleIDS should be called in serverless mode with includeDelete
		},
		{
			isServerless:   true,
			includeDeleted: false,
			expectError:    false,
		},
		{
			isServerless:   true,
			includeDeleted: true,
			expectError:    false,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestGetRoleIDs {Serverless=%t; includeDelete=%t}", testCase.isServerless, testCase.includeDeleted), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			db, ctx := setupTestDBForBucketWithOptions(t, dbContextConfig)
			defer db.Close(ctx)

			n1QLStore, reset, err := setupN1QLStore(db.Bucket, testCase.isServerless)
			assert.NoError(t, err, "Unable to get n1QLStore for testBucket")
			defer func(n1QLStore base.N1QLStore, isServerless bool) {
				err := reset(n1QLStore, isServerless)
				assert.NoError(t, err, "Reset fn shouldn't return error")
			}(n1QLStore, testCase.isServerless)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

			db.Options.QueryPaginationLimit = 100
			db.ChannelMapper = channels.NewDefaultChannelMapper()
			authenticator := db.Authenticator(ctx)

			rolename1 := uuid.NewString()
			rolename2 := uuid.NewString()

			role1, err := authenticator.NewRole(rolename1, nil)
			assert.NoError(t, err)
			assert.NoError(t, authenticator.Save(role1))

			role2, err := authenticator.NewRole(rolename2, nil)
			assert.NoError(t, err)
			assert.NoError(t, authenticator.Save(role2))

			err = db.DeleteRole(ctx, role2.Name(), false)
			assert.NoError(t, err)
			roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
			assert.NoError(t, err)
			assert.True(t, roleGet.IsDeleted())

			t.Log("role1:", role1.Name())
			t.Log("role2:", role2.Name())

			// assert roles
			roles, err := db.GetRoleIDs(ctx, testCase.includeDeleted)
			if testCase.expectError {
				assert.Error(t, err)
			} else {
				expectedRoles := []string{role1.Name()}
				if testCase.includeDeleted {
					expectedRoles = append(expectedRoles, role2.Name())
				}

				assert.NoError(t, err)
				assert.ElementsMatch(t, expectedRoles, roles)
			}
		})
	}
}
