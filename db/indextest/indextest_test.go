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
	"context"
	"fmt"
	"strings"
	"sync"
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

			setupN1QLStore(ctx, t, database.Bucket, testCase.isServerless, db.DefaultNumIndexPartitions)

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
			require.NoError(t, database.DeleteRole(ctx, role1.Name(), false))

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

			setupN1QLStore(ctx, t, database.Bucket, testCase.isServerless, db.DefaultNumIndexPartitions)
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

			setupN1QLStore(ctx, t, database.Bucket, testCase.isServerless, db.DefaultNumIndexPartitions)
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

// TestInitializeIndexes ensures all of SG's indexes can be built using both values of xattrs, with all N1QLStore implementations
func TestInitializeIndexes(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}
	base.LongRunningTest(t)

	tests := []struct {
		xattrs      bool
		collections bool
	}{
		{true, false},
		{false, false},
		{true, true},
		{false, true},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("xattrs=%v collections=%v", test.xattrs, test.collections), func(t *testing.T) {
			options := db.DatabaseContextOptions{
				EnableXattr: test.xattrs,
			}
			if test.collections {
				base.TestRequiresCollections(t)
				// uses Scopes implicitly through test harness
			} else {
				options.Scopes = db.GetScopesOptionsDefaultCollectionOnly(t)
			}
			database, ctx := db.SetupTestDBWithOptions(t, options)
			defer database.Close(ctx)
			collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)

			gocbBucket, err := base.AsGocbV2Bucket(database.Bucket)
			require.NoError(t, err)

			n1qlStore, err := base.NewClusterOnlyN1QLStore(gocbBucket.GetCluster(), gocbBucket.BucketName(), collection.ScopeName, collection.Name)
			require.NoError(t, err)

			// add and drop indexes that may be different from the way the bucket pool expects, so use specific options here for test
			xattrSpecificIndexOptions := db.InitializeIndexOptions{
				NumReplicas:   0,
				Serverless:    database.IsServerless(),
				UseXattrs:     test.xattrs,
				NumPartitions: db.DefaultNumIndexPartitions,
			}
			if database.OnlyDefaultCollection() {
				xattrSpecificIndexOptions.MetadataIndexes = db.IndexesAll
			}

			// Make sure we can drop and reinitialize twice
			for i := 0; i < 2; i++ {
				dropErr := base.DropAllIndexes(ctx, n1qlStore)
				require.NoError(t, dropErr, "Error dropping all indexes on bucket")

				initErr := db.InitializeIndexes(ctx, n1qlStore, xattrSpecificIndexOptions)
				require.NoError(t, initErr, "Error initializing all indexes on bucket")
			}
			allIndexes, err := gocbBucket.GetCluster().Bucket(gocbBucket.BucketName()).Scope(collection.ScopeName).Collection(collection.Name).QueryIndexes().GetAllIndexes(nil)
			require.NoError(t, err)
			for _, index := range allIndexes {
				require.Equal(t, "", index.Partition)
			}
		})
	}
}

// TestInitializeIndexesConcurrentMultiNode simulates a large multi-node SG cluster starting up and racing to create indexes.
func TestInitializeIndexesConcurrentMultiNode(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Pick a high enough number that it's likely to trigger the concurrent/race conditions we're testing for.
	//
	// This number doesn't significantly increase test time since, since there's still the same number of indexes being created.
	// All nodes will just be waiting for the state so we can keep this unrealistically high to increase chances of finding error cases.
	const numSGNodes = 100

	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))
	var wg sync.WaitGroup
	wg.Add(numSGNodes)
	for i := 0; i < numSGNodes; i++ {
		ctx := base.CorrelationIDLogCtx(context.Background(), fmt.Sprintf("test-node-%d", i))
		go func() {
			defer wg.Done()
			setupN1QLStore(ctx, t, bucket, false, db.DefaultNumIndexPartitions)
		}()
	}
	wg.Wait()
}

func TestPartitionedIndexes(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("TestPartitionedIndexes only works with UseXattrs=true")
	}
	numPartitions := uint32(8)
	serverless := false

	database, ctx := db.SetupTestDBWithOptions(t, db.DatabaseContextOptions{NumIndexPartitions: base.Ptr(numPartitions)})
	defer database.Close(ctx)

	setupN1QLStore(ctx, t, database.Bucket, serverless, numPartitions)
	gocbBucket, err := base.AsGocbV2Bucket(database.Bucket)
	require.NoError(t, err)
	for _, dsName := range []sgbucket.DataStoreName{db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionDatastore(), database.MetadataStore} {
		allIndexes, err := gocbBucket.GetCluster().Bucket(gocbBucket.BucketName()).Scope(dsName.ScopeName()).Collection(dsName.CollectionName()).QueryIndexes().GetAllIndexes(nil)
		require.NoError(t, err)
		for _, index := range allIndexes {
			if strings.HasPrefix(index.Name, "sg_allDocs") || strings.HasPrefix(index.Name, "sg_channels") {
				require.True(t, strings.HasSuffix(index.Name, "x1_p8"), "expected 8 partitions for %+v", index)
				require.NotEqual(t, "", index.Partition)
			} else {
				require.Equal(t, "", index.Partition)
				require.True(t, strings.HasSuffix(index.Name, "_x1"), "expected nopartitions for %+v", index)
			}
		}
	}
}
