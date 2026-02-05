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
		useLegacySyncDocsIndex bool
	}{
		{
			useLegacySyncDocsIndex: false,
		},
		{
			useLegacySyncDocsIndex: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("useLegacySyncDocsIndex=%t", testCase.useLegacySyncDocsIndex), func(t *testing.T) {
			database := setupIndexAndDB(t, testIndexCreationOptions{
				useLegacySyncDocsIndex: testCase.useLegacySyncDocsIndex,
				numPartitions:          db.DefaultNumIndexPartitions,
				useXattrs:              base.TestUseXattrs(),
			})

			ctx := database.AddDatabaseLogContext(base.TestCtx(t))
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
					var row map[string]any
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
		useLegacySyncDocsIndex bool
	}{
		{
			useLegacySyncDocsIndex: false,
		},
		{
			useLegacySyncDocsIndex: true,
		},
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges)
	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("TestAllPrincipalIDs with useLegacySyncDocsIndex=%t", testCase.useLegacySyncDocsIndex), func(t *testing.T) {
			database := setupIndexAndDB(t, testIndexCreationOptions{
				useLegacySyncDocsIndex: testCase.useLegacySyncDocsIndex,
				numPartitions:          db.DefaultNumIndexPartitions,
				useXattrs:              base.TestUseXattrs(),
			})

			ctx := database.AddDatabaseLogContext(base.TestCtx(t))

			n1qlStore, ok := database.MetadataStore.(base.N1QLStore)
			require.True(t, ok)
			onlineMetadataIndexes, err := db.GetOnlinePrincipalIndexes(ctx, n1qlStore, database.UseXattrs())
			require.NoError(t, err)
			if testCase.useLegacySyncDocsIndex {
				require.Equal(t, []db.SGIndexType{db.IndexSyncDocs}, onlineMetadataIndexes)
			} else {
				require.ElementsMatch(t, []db.SGIndexType{db.IndexUser, db.IndexRole}, onlineMetadataIndexes)
			}
			t.Run("roleQueryCovered", func(t *testing.T) {
				roleStatement, _ := database.BuildRolesQuery("", 0)
				requireCoveredQuery(t, database, roleStatement, !testCase.useLegacySyncDocsIndex)
			})
			t.Run("userQueryCovered", func(t *testing.T) {
				userStatement, _ := database.BuildUsersQuery("", 0)
				requireCoveredQuery(t, database, userStatement, !testCase.useLegacySyncDocsIndex)
			})

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
		useLegacySyncDocsIndex bool
	}{
		{
			useLegacySyncDocsIndex: false,
		},
		{
			useLegacySyncDocsIndex: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("useLegacySyncDocsIndex=%t", testCase.useLegacySyncDocsIndex), func(t *testing.T) {
			database := setupIndexAndDB(t, testIndexCreationOptions{
				useLegacySyncDocsIndex: testCase.useLegacySyncDocsIndex,
				numPartitions:          db.DefaultNumIndexPartitions,
				useXattrs:              base.TestUseXattrs(),
			})

			ctx := database.AddDatabaseLogContext(base.TestCtx(t))
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

	defaultCollection := base.DefaultScopeAndCollectionName()
	namedCollection := base.ScopeAndCollectionName{Scope: "placeHolderScope", Collection: "placeHolderCollection"}
	tests := []struct {
		xattrs          bool
		collections     bool
		expectedIndexes db.CollectionIndexes
	}{
		{
			xattrs:      true,
			collections: false,
			expectedIndexes: db.CollectionIndexes{
				defaultCollection: map[string]struct{}{
					"sg_access_x1":     struct{}{},
					"sg_allDocs_x1":    struct{}{},
					"sg_channels_x1":   struct{}{},
					"sg_roleAccess_x1": struct{}{},
					"sg_roles_x1":      struct{}{},
					"sg_tombstones_x1": struct{}{},
					"sg_users_x1":      struct{}{},
				},
			},
		},
		{
			xattrs:      false,
			collections: false,
			expectedIndexes: db.CollectionIndexes{
				defaultCollection: map[string]struct{}{
					"sg_access_1":     struct{}{},
					"sg_allDocs_1":    struct{}{},
					"sg_channels_1":   struct{}{},
					"sg_roleAccess_1": struct{}{},
					"sg_roles_1":      struct{}{},
					"sg_users_1":      struct{}{},
				},
			},
		},
		{
			xattrs:      true,
			collections: true,
			expectedIndexes: db.CollectionIndexes{
				defaultCollection: map[string]struct{}{
					"sg_roles_x1": struct{}{},
					"sg_users_x1": struct{}{},
				},
				namedCollection: map[string]struct{}{
					"sg_access_x1":     struct{}{},
					"sg_allDocs_x1":    struct{}{},
					"sg_channels_x1":   struct{}{},
					"sg_roleAccess_x1": struct{}{},
					"sg_tombstones_x1": struct{}{},
				},
			},
		},
		{
			xattrs:      false,
			collections: true,
			expectedIndexes: db.CollectionIndexes{
				defaultCollection: map[string]struct{}{
					"sg_roles_1": struct{}{},
					"sg_users_1": struct{}{},
				},
				namedCollection: map[string]struct{}{
					"sg_access_1":     struct{}{},
					"sg_allDocs_1":    struct{}{},
					"sg_channels_1":   struct{}{},
					"sg_roleAccess_1": struct{}{},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("xattrs=%v collections=%v", test.xattrs, test.collections), func(t *testing.T) {
			ctx := base.TestCtx(t)
			bucket := base.GetTestBucket(t)
			defer bucket.Close(ctx)

			options := db.DatabaseContextOptions{
				EnableXattr: test.xattrs,
			}
			if test.collections {
				base.TestRequiresCollections(t)
				options.Scopes = db.GetScopesOptions(t, bucket, 1)
			} else {
				options.Scopes = db.GetScopesOptionsDefaultCollectionOnly(t)
			}
			database, ctx := db.CreateTestDatabase(t, bucket, options)
			defer database.Close(ctx)

			collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)

			expectedIndexes := test.expectedIndexes
			// namedCollection with the actual collection name after the bucket pool has defined it
			namedCollectionIndexes, ok := expectedIndexes[namedCollection]
			if ok {
				expectedIndexes[collection.ScopeAndCollectionName()] = namedCollectionIndexes
				delete(expectedIndexes, namedCollection)
			}
			require.Equal(t, expectedIndexes, database.GetInUseIndexes())

			gocbBucket, err := base.AsGocbV2Bucket(database.Bucket)
			require.NoError(t, err)

			n1qlStore, err := base.NewClusterOnlyN1QLStore(gocbBucket.GetCluster(), gocbBucket.BucketName(), collection.ScopeName, collection.Name)
			require.NoError(t, err)

			// add and drop indexes that may be different from the way the bucket pool expects, so use specific options here for test
			xattrSpecificIndexOptions := db.InitializeIndexOptions{
				NumReplicas:         0,
				LegacySyncDocsIndex: database.UseLegacySyncDocsIndex(),
				UseXattrs:           test.xattrs,
				NumPartitions:       db.DefaultNumIndexPartitions,
			}
			if database.OnlyDefaultCollection() {
				xattrSpecificIndexOptions.MetadataIndexes = db.IndexesAll
			}

			// Make sure we can drop and reinitialize twice
			for range 2 {
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
			testGetIndexesMeta(t, database, xattrSpecificIndexOptions)

		})
	}
}

func testGetIndexesMeta(t *testing.T, database *db.Database, indexInitOptions db.InitializeIndexOptions) {
	suffix := "x1"
	if !indexInitOptions.UseXattrs {
		suffix = "1"
	}
	accessIndexName := fmt.Sprintf("sg_access_%s", suffix)
	roleAccessIndexName := fmt.Sprintf("sg_roleAccess_%s", suffix)
	nonExistentIndexName := "notanindex"

	dataStore, ok := db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionDatastore().(base.N1QLStore)
	require.True(t, ok, "Expected N1QLStore")
	ctx := base.TestCtx(t)

	// check existing and non existing indexes
	indexes, err := base.GetIndexesMeta(ctx, dataStore, []string{accessIndexName, roleAccessIndexName, nonExistentIndexName})
	require.NoError(t, err)
	require.Len(t, indexes, 2)
	require.Equal(t, base.IndexStateOnline, indexes[accessIndexName].State)
	require.Equal(t, base.IndexStateOnline, indexes[roleAccessIndexName].State)

	// check non existing indexes
	indexes, err = base.GetIndexesMeta(ctx, dataStore, []string{nonExistentIndexName})
	require.NoError(t, err)
	require.Empty(t, indexes)

	// check single index
	indexes, err = base.GetIndexesMeta(ctx, dataStore, []string{accessIndexName})
	require.NoError(t, err)
	require.Len(t, indexes, 1)
	require.Equal(t, base.IndexStateOnline, indexes[accessIndexName].State)

}

// TestInitializeIndexesConcurrentMultiNode simulates a large multi-node SG cluster starting up and racing to create indexes.
func TestInitializeIndexesConcurrentMultiNode(t *testing.T) {

	// Pick a high enough number that it's likely to trigger the concurrent/race conditions we're testing for.
	//
	// This number doesn't significantly increase test time since, since there's still the same number of indexes being created.
	// All nodes will just be waiting for the state so we can keep this unrealistically high to increase chances of finding error cases.
	const numSGNodes = 100

	bucket := base.GetTestBucket(t)
	defer bucket.Close(base.TestCtx(t))
	var wg sync.WaitGroup
	wg.Add(numSGNodes)
	for range numSGNodes {
		go func() {
			defer wg.Done()
			setupIndexes(t, bucket, testIndexCreationOptions{
				numPartitions:          db.DefaultNumIndexPartitions,
				useLegacySyncDocsIndex: false,
				useXattrs:              base.TestUseXattrs(),
			})
		}()
	}
	wg.Wait()
}

func TestPartitionedIndexes(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("TestPartitionedIndexes only works with UseXattrs=true")
	}

	numPartitions := uint32(13)
	database := setupIndexAndDB(t, testIndexCreationOptions{
		useLegacySyncDocsIndex: true,
		numPartitions:          numPartitions,
		useXattrs:              base.TestUseXattrs(),
	})

	gocbBucket, err := base.AsGocbV2Bucket(database.Bucket)
	require.NoError(t, err)
	for _, dsName := range []sgbucket.DataStoreName{db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionDatastore(), database.MetadataStore} {
		allIndexes, err := gocbBucket.GetCluster().Bucket(gocbBucket.BucketName()).Scope(dsName.ScopeName()).Collection(dsName.CollectionName()).QueryIndexes().GetAllIndexes(nil)
		require.NoError(t, err)
		for _, index := range allIndexes {
			if strings.HasPrefix(index.Name, "sg_allDocs") || strings.HasPrefix(index.Name, "sg_channels") {
				require.True(t, strings.HasSuffix(index.Name, "x1_p13"), "expected %d partitions for %+v", numPartitions, index)
				require.NotEqual(t, "", index.Partition)
				require.Equal(t, numPartitions, db.GetIndexPartitionCount(t, gocbBucket, dsName, index.Name))
			} else {
				require.Equal(t, "", index.Partition)
				require.True(t, strings.HasSuffix(index.Name, "_x1"), "expected nopartitions for %+v", index)
				require.Equal(t, uint32(1), db.GetIndexPartitionCount(t, gocbBucket, dsName, index.Name))
			}
		}
	}
}
