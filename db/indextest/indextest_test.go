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

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestRoleQuery(t *testing.T) {
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
		t.Run(fmt.Sprintf("Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)

			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			setupN1QLStore(t, database.Bucket, testCase.isServerless)

			authenticator := database.Authenticator(ctx)
			require.NotNil(t, authenticator, "database.Authenticator(ctx) returned nil")
			// Add roles
			for i := 1; i <= 5; i++ {
				role, err := authenticator.NewRole(fmt.Sprintf("role%d", i), base.SetOf("ABC"))
				require.NoError(t, err, "Error creating new role")
				require.NoError(t, authenticator.Save(role))
			}

			// Delete 1 role
			role1, err := authenticator.NewRole("role1", base.SetOf("ABC"))
			require.NoError(t, err)
			err = authenticator.DeleteRole(role1, false, 0)
			require.NoError(t, err)

			testCases := []struct {
				name          string
				expectedRoles int
			}{
				{
					name:          "QueryRoles",
					expectedRoles: 4,
				},
				{
					name:          "QueryAllRoles",
					expectedRoles: 5,
				},
			}
			for _, testCase := range testCases {
				t.Run(testCase.name, func(t *testing.T) {
					var results sgbucket.QueryResultIterator
					var queryErr error
					switch testCase.name {
					case "QueryRoles":
						// Standard query
						results, queryErr = database.QueryRoles(ctx, "", 0)
					case "QueryAllRoles":
						results, queryErr = database.QueryAllRoles(ctx, "", 0)
					default:
						t.Fatalf("Unexpected test case: %s", testCase.name)
					}
					require.NoError(t, queryErr, "Query error")
					defer func() {
						require.NoError(t, results.Close())
					}()
					var row map[string]interface{}
					rowCount := 0
					for results.Next(ctx, &row) {
						rowCount++
					}

					require.Equal(t, testCase.expectedRoles, rowCount)
				})
			}
		})
	}

}

func TestAllPrincipalIDs(t *testing.T) {
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
			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			setupN1QLStore(t, database.Bucket, testCase.isServerless)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)
			t.Run("roleQueryCovered", func(t *testing.T) {
				roleStatement, _ := database.BuildRolesQuery("", 0)
				requireCoveredQuery(t, database, roleStatement, testCase.isServerless)
			})
			t.Run("userQueryCovered", func(t *testing.T) {
				userStatement, _ := database.BuildUsersQuery("", 0)
				requireCoveredQuery(t, database, userStatement, testCase.isServerless)
			})

			database.Options.QueryPaginationLimit = 100
			authenticator := database.Authenticator(ctx)

			rolename1 := uuid.NewString()
			rolename2 := uuid.NewString()
			username := uuid.NewString()

			user1, err := authenticator.NewUser(username, "letmein", nil)
			require.NoError(t, err)
			user1.SetExplicitRoles(channels.TimedSet{rolename1: channels.NewVbSimpleSequence(1), rolename2: channels.NewVbSimpleSequence(1)}, 1)
			require.NoError(t, authenticator.Save(user1))

			role1, err := authenticator.NewRole(rolename1, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role1))

			role2, err := authenticator.NewRole(rolename2, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role2))

			err = database.DeleteRole(ctx, role2.Name(), false)
			require.NoError(t, err)
			roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
			require.NoError(t, err)
			require.True(t, roleGet.IsDeleted())

			t.Log("user1:", user1.Name())
			t.Log("role1:", role1.Name())
			t.Log("role2:", role2.Name())

			// require allprincipals still returns users and deleted roles
			users, roles, err := database.AllPrincipalIDs(ctx)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{user1.Name()}, users)
			require.ElementsMatch(t, []string{role1.Name(), role2.Name()}, roles)

			roles, err = database.GetRoleIDs(ctx, database.UseViews(), false)
			require.NoError(t, err)
			require.ElementsMatch(t, []string{role1.Name()}, roles)
		})
	}
}

func TestGetRoleIDs(t *testing.T) {
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
		t.Run(fmt.Sprintf("Serverless=%t", testCase.isServerless), func(t *testing.T) {
			dbContextConfig := getDatabaseContextOptions(testCase.isServerless)
			database, ctx := db.SetupTestDBWithOptions(t, dbContextConfig)
			defer database.Close(ctx)

			setupN1QLStore(t, database.Bucket, testCase.isServerless)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)

			database.Options.QueryPaginationLimit = 100
			authenticator := database.Authenticator(ctx)

			rolename1 := uuid.NewString()
			rolename2 := uuid.NewString()

			role1, err := authenticator.NewRole(rolename1, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role1))

			role2, err := authenticator.NewRole(rolename2, nil)
			require.NoError(t, err)
			require.NoError(t, authenticator.Save(role2))

			err = database.DeleteRole(ctx, role2.Name(), false)
			require.NoError(t, err)
			roleGet, err := authenticator.GetRoleIncDeleted(role2.Name())
			require.NoError(t, err)
			require.True(t, roleGet.IsDeleted())

			t.Log("role1:", role1.Name())
			t.Log("role2:", role2.Name())

			for _, includeDeleted := range []bool{false, true} {
				t.Run(fmt.Sprintf("includeDeleted=%t", includeDeleted), func(t *testing.T) {
					// require roles
					roles, err := database.GetRoleIDs(ctx, database.UseViews(), includeDeleted)
					expectedRoles := []string{role1.Name()}
					if includeDeleted {
						expectedRoles = append(expectedRoles, role2.Name())
					}

					require.NoError(t, err)
					require.ElementsMatch(t, expectedRoles, roles)
				})
			}
		})
	}
}

func getDatabaseContextOptions(isServerless bool) db.DatabaseContextOptions {
	defaultCacheOptions := db.DefaultCacheOptions()

	return db.DatabaseContextOptions{
		CacheOptions: &defaultCacheOptions,
		Serverless:   isServerless,
	}
}

// setupN1QLStore initializes the indexes for a database. This is normally done by the rest package
func setupN1QLStore(t *testing.T, bucket base.Bucket, isServerless bool) {
	testBucket, ok := bucket.(*base.TestBucket)
	require.True(t, ok)

	hasOnlyDefaultDataStore := len(testBucket.GetNonDefaultDatastoreNames()) == 0

	defaultDataStore := bucket.DefaultDataStore()
	defaultN1QLStore, ok := base.AsN1QLStore(defaultDataStore)
	require.True(t, ok, "Unable to get n1QLStore for defaultDataStore")
	options := db.InitializeIndexOptions{
		NumReplicas: 0,
		Serverless:  isServerless,
		UseXattrs:   base.TestUseXattrs(),
	}
	if hasOnlyDefaultDataStore {
		options.MetadataIndexes = db.IndexesAll
	} else {
		options.MetadataIndexes = db.IndexesMetadataOnly
	}
	ctx := base.CollectionLogCtx(base.TestCtx(t), defaultDataStore.ScopeName(), defaultDataStore.CollectionName())
	require.NoError(t, db.InitializeIndexes(ctx, defaultN1QLStore, options))
	if hasOnlyDefaultDataStore {
		return
	}
	options = db.InitializeIndexOptions{
		NumReplicas:     0,
		Serverless:      isServerless,
		UseXattrs:       base.TestUseXattrs(),
		MetadataIndexes: db.IndexesWithoutMetadata,
	}
	dataStore, err := testBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	n1qlStore, ok := base.AsN1QLStore(dataStore)
	require.True(t, ok)
	require.NoError(t, db.InitializeIndexes(ctx, n1qlStore, options))
}

func requireCoveredQuery(t *testing.T, database *db.Database, statement string, isCovered bool) {
	n1QLStore, ok := base.AsN1QLStore(database.MetadataStore)
	require.True(t, ok)
	plan, explainErr := n1QLStore.ExplainQuery(base.TestCtx(t), statement, nil)
	require.NoError(t, explainErr, "Error generating explain for %+v", statement)

	covered := db.IsCovered(plan)
	planJSON, err := base.JSONMarshal(plan)
	require.NoError(t, err)
	require.Equal(t, isCovered, covered, "query covered by index; expectedToBeCovered: %t, Plan: %s", isCovered, planJSON)
}
