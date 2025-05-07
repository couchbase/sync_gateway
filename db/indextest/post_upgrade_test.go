// Copyright 2025-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostUpgradeIndexesSimple(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyQuery)

	testCases := []struct {
		name                   string
		dbOptions              db.DatabaseContextOptions
		indexInitOptions       testIndexCreationOptions
		expectedRemovedIndexes []string
	}{
		{
			name: "non-xattr -> xattr",
			indexInitOptions: testIndexCreationOptions{
				numPartitions:                db.DefaultNumIndexPartitions,
				useXattrs:                    false,
				useLegacySyncDocsIndex:       true,
				forceSingleDefaultCollection: true,
			},
			dbOptions: db.DatabaseContextOptions{
				CacheOptions: base.Ptr(db.DefaultCacheOptions()),
				EnableXattr:  true,
				Scopes:       db.GetScopesOptionsDefaultCollectionOnly(t),
			},
			expectedRemovedIndexes: []string{
				"sg_access_1",
				"sg_allDocs_1",
				"sg_channels_1",
				"sg_roleAccess_1",
				"sg_syncDocs_1",
			},
		},
		{
			name: "xattr -> non-xattr",
			indexInitOptions: testIndexCreationOptions{
				numPartitions:                db.DefaultNumIndexPartitions,
				useXattrs:                    true,
				useLegacySyncDocsIndex:       true,
				forceSingleDefaultCollection: true,
			},
			dbOptions: db.DatabaseContextOptions{
				CacheOptions: base.Ptr(db.DefaultCacheOptions()),
				EnableXattr:  false,
				Scopes:       db.GetScopesOptionsDefaultCollectionOnly(t),
			},
			expectedRemovedIndexes: []string{
				"sg_access_x1",
				"sg_allDocs_x1",
				"sg_channels_x1",
				"sg_roleAccess_x1",
				"sg_syncDocs_x1",
				"sg_tombstones_x1",
			},
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			bucket := base.GetTestBucket(t)
			ctx := base.TestCtx(t)
			defer bucket.Close(ctx)
			setupIndexes(t, bucket, test.indexInitOptions)
			database, ctx := db.CreateTestDatabase(t, bucket, test.dbOptions)
			defer database.Close(ctx)
			collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
			n1qlStore, ok := base.AsN1QLStore(collection.GetCollectionDatastore())
			require.True(t, ok)

			// Running w/ opposite xattrs flag should preview removal of the indexes associated with this db context
			removedIndexes, removeErr := db.RemoveObsoleteIndexes(ctx, n1qlStore, true, database.UseXattrs(), database.UseViews(), db.GetSGIndexes())
			require.ElementsMatch(t, test.expectedRemovedIndexes, removedIndexes)
			require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in preview mode")

			// Running again w/ preview=false to perform cleanup
			removedIndexes, removeErr = db.RemoveObsoleteIndexes(ctx, n1qlStore, false, database.UseXattrs(), database.UseViews(), db.GetSGIndexes())
			require.ElementsMatch(t, test.expectedRemovedIndexes, removedIndexes)
			require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in non-preview mode")

			// One more time to make sure they are actually gone
			removedIndexes, removeErr = db.RemoveObsoleteIndexes(ctx, n1qlStore, false, database.UseXattrs(), database.UseViews(), db.GetSGIndexes())
			require.Len(t, removedIndexes, 0)
			require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in post-cleanup no-op")

		})
	}
}

func TestPostUpgradeIndexesVersionChange(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyQuery)
	for _, useXattrs := range []bool{true, false} {
		t.Run(fmt.Sprintf("xattrs=%t", useXattrs), func(t *testing.T) {
			database := setupIndexAndDB(t, testIndexCreationOptions{
				numPartitions:                db.DefaultNumIndexPartitions,
				useXattrs:                    useXattrs,
				useLegacySyncDocsIndex:       false,
				forceSingleDefaultCollection: true,
			})
			require.Equal(t, useXattrs, database.UseXattrs())
			collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
			n1qlStore, ok := base.AsN1QLStore(collection.GetCollectionDatastore())
			require.True(t, ok)

			ctx := base.TestCtx(t)
			sgIndexes := db.GetSGIndexes()
			// Validate that removeObsoleteIndexes is a no-op for the default case
			removedIndexes, removeErr := db.RemoveObsoleteIndexes(ctx, n1qlStore, true, database.UseXattrs(), database.UseViews(), sgIndexes)
			require.NoError(t, removeErr)
			assert.Len(t, removedIndexes, 0)

			accessIndex := sgIndexes[db.IndexAccess]
			accessIndex.Version = 2
			accessIndex.PreviousVersions = []int{1}
			sgIndexes[db.IndexAccess] = accessIndex

			// Validate that removeObsoleteIndexes now triggers removal of one index
			removedIndexes, removeErr = db.RemoveObsoleteIndexes(ctx, n1qlStore, true, database.UseXattrs(), database.UseViews(), sgIndexes)
			require.NoError(t, removeErr)
			if useXattrs {
				assert.ElementsMatch(t, []string{"sg_access_x1"}, removedIndexes)
			} else {
				assert.ElementsMatch(t, []string{"sg_access_1"}, removedIndexes)
			}
		})
	}
}

func TestPostUpgradeMultipleCollections(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyQuery)

	bucket := base.GetTestBucket(t)
	ctx := base.TestCtx(t)
	defer bucket.Close(ctx)
	numCollections := 2
	setupIndexes(t, bucket, testIndexCreationOptions{
		numPartitions:          db.DefaultNumIndexPartitions,
		useXattrs:              true,  // force use of xattrs to make test space smaller
		useLegacySyncDocsIndex: false, // upgrade is likely to be using legacy sync docs index
		numCollections:         numCollections,
	})
	database, ctx := db.CreateTestDatabase(t, bucket, db.DatabaseContextOptions{
		CacheOptions: base.Ptr(db.DefaultCacheOptions()),
		EnableXattr:  true,
		Scopes:       db.GetScopesOptions(t, bucket, numCollections),
	})

	// make sure RemoveObsoleteIndexes is a no-op before adding obsolete indexes
	for _, preview := range []bool{true, false} {
		t.Run(fmt.Sprintf("no-op remove preview=%t", preview), func(t *testing.T) {
			removedIndexes, removeErr := database.RemoveObsoleteIndexes(ctx, preview)
			require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in no-op case")
			require.Len(t, removedIndexes, 0)
		})
	}
	// create non xattr indexes on the default collection
	setupIndexes(t, bucket, testIndexCreationOptions{
		numPartitions:          db.DefaultNumIndexPartitions,
		useXattrs:              false, // force use of xattrs to make test space smaller
		useLegacySyncDocsIndex: false, // upgrade is likely to be using legacy sync docs index
		numCollections:         numCollections,
	})

	ds1, err := bucket.GetNamedDataStore(0)
	require.NoError(t, err)
	ds1Name := fmt.Sprintf("`%s`.`%s`", ds1.ScopeName(), ds1.CollectionName())
	ds2, err := bucket.GetNamedDataStore(1)
	require.NoError(t, err)
	ds2Name := fmt.Sprintf("`%s`.`%s`", ds2.ScopeName(), ds2.CollectionName())

	// make sure RemoveObsoleteIndexes is a no-op before adding obsolete indexes
	for _, preview := range []bool{true, false} {
		t.Run(fmt.Sprintf("no-op remove preview=%t", preview), func(t *testing.T) {
			removedIndexes, removeErr := database.RemoveObsoleteIndexes(ctx, preview)
			require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in no-op case")

			require.ElementsMatch(t, []string{
				ds1Name + ".sg_access_1",
				ds1Name + ".sg_allDocs_1",
				ds1Name + ".sg_channels_1",
				ds1Name + ".sg_roleAccess_1",
				ds2Name + ".sg_access_1",
				ds2Name + ".sg_allDocs_1",
				ds2Name + ".sg_channels_1",
				ds2Name + ".sg_roleAccess_1",
				"`_default`.`_default`.sg_users_1",
				"`_default`.`_default`.sg_roles_1",
			}, removedIndexes)
		})
	}
}

func TestRemoveIndexesUseViewsTrueAndFalse(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyQuery)

	for _, useXattrs := range []bool{true, false} {
		t.Run(fmt.Sprintf("xattrs=%t", useXattrs), func(t *testing.T) {
			database := setupIndexAndDB(t, testIndexCreationOptions{
				numPartitions:                db.DefaultNumIndexPartitions,
				useXattrs:                    useXattrs,
				useLegacySyncDocsIndex:       false,
				forceSingleDefaultCollection: true,
			})
			require.Equal(t, useXattrs, database.UseXattrs())
			require.False(t, database.UseViews())
			ctx := base.TestCtx(t)
			collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext).GetCollectionDatastore()
			// remove all views at the end of the test
			defer func() {
				viewStore, ok := base.AsViewStore(collection)
				assert.True(t, ok)
				ddocs, err := viewStore.GetDDocs()
				assert.NoError(t, err)

				for ddocName := range ddocs {
					assert.NoError(t, viewStore.DeleteDDoc(ddocName))
				}
			}()

			require.NoError(t, db.InitializeViews(ctx, collection))

			for _, preview := range []bool{true, false} {
				t.Run(fmt.Sprintf("RemoveObsoleteDesignDocs preview=%t", preview), func(t *testing.T) {
					expectedRemovedDesignDocs := []string{"sync_gateway_2.1", "sync_housekeeping_2.1"}
					removedDesignDocs, err := database.RemoveObsoleteDesignDocs(ctx, preview)
					require.NoError(t, err)
					require.ElementsMatch(t, expectedRemovedDesignDocs, removedDesignDocs)
				})
			}

			n1QLStore, ok := base.AsN1QLStore(collection)
			require.True(t, ok)

			var expectedRemovedIndexes []string

			if useXattrs {
				expectedRemovedIndexes = []string{
					"sg_access_x1",
					"sg_allDocs_x1",
					"sg_channels_x1",
					"sg_roleAccess_x1",
					"sg_roles_x1",
					"sg_tombstones_x1",
					"sg_users_x1",
				}
			} else {
				expectedRemovedIndexes = []string{
					"sg_access_1",
					"sg_allDocs_1",
					"sg_channels_1",
					"sg_roleAccess_1",
					"sg_roles_1",
					"sg_users_1",
				}
			}
			preview := true
			useViews := true
			removedIndexes, removeErr := db.RemoveObsoleteIndexes(ctx, n1QLStore, preview, database.UseXattrs(), useViews, db.GetSGIndexes())
			require.NoError(t, removeErr)
			require.ElementsMatch(t, expectedRemovedIndexes, removedIndexes)

			useViews = false
			removedIndexes, removeErr = db.RemoveObsoleteIndexes(ctx, n1QLStore, preview, database.UseXattrs(), useViews, db.GetSGIndexes())
			require.NoError(t, removeErr)
			require.Empty(t, removedIndexes)

		})
	}
}

func TestRemoveObsoleteIndexOnError(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyQuery)
	for _, useXattrs := range []bool{true, false} {
		t.Run(fmt.Sprintf("xattrs=%t", useXattrs), func(t *testing.T) {
			database := setupIndexAndDB(t, testIndexCreationOptions{
				numPartitions:                db.DefaultNumIndexPartitions,
				useXattrs:                    useXattrs,
				useLegacySyncDocsIndex:       false,
				forceSingleDefaultCollection: true,
			})

			collection := db.GetSingleDatabaseCollection(t, database.DatabaseContext)
			n1qlStore, ok := base.AsN1QLStore(collection.GetCollectionDatastore())
			require.True(t, ok)

			testIndexes := db.GetSGIndexes()

			// Use existing versions of IndexAccess and IndexChannels and create an old version that will be removed by obsolete
			// indexes. Resulting from the removal candidates for removeObsoleteIndexes will be:
			// All previous versions and opposite of current xattr setting eg. for this test ran with non-xattrs:
			// [sg_channels_x2 sg_channels_x1 sg_channels_1 sg_access_x2 sg_access_x1 sg_access_1]
			accessIndex := testIndexes[db.IndexAccess]
			accessIndex.Version = 2
			accessIndex.PreviousVersions = []int{1}
			testIndexes[db.IndexAccess] = accessIndex

			channelIndex := testIndexes[db.IndexChannels]
			channelIndex.Version = 2
			channelIndex.PreviousVersions = []int{1}
			testIndexes[db.IndexChannels] = channelIndex

			ctx := base.TestCtx(t)
			removedIndexes, removeErr := db.RemoveObsoleteIndexes(ctx, n1qlStore, false, database.UseXattrs(), database.UseViews(), testIndexes)
			assert.NoError(t, removeErr)

			if useXattrs {
				require.ElementsMatch(t, []string{"sg_channels_x1", "sg_access_x1"}, removedIndexes)
			} else {
				require.ElementsMatch(t, []string{"sg_channels_1", "sg_access_1"}, removedIndexes)
			}
		})
	}
}
