/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestInitializeIndexes ensures all of SG's indexes can be built using both values of xattrs
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
			var db *Database
			var ctx context.Context

			if test.collections {
				base.TestRequiresCollections(t)

				db, ctx = setupTestNamedCollectionDBWithOptions(t, DatabaseContextOptions{EnableXattr: test.xattrs})
			} else {
				db, ctx = setupTestDefaultCollectionDBWithOptions(t, DatabaseContextOptions{EnableXattr: test.xattrs})
			}
			defer db.Close(ctx)
			collection := db.GetSingleDatabaseCollection()

			n1qlStore, isGoCBBucket := base.AsN1QLStore(collection.dataStore)
			require.True(t, isGoCBBucket)

			// Make sure we can drop and reinitialize twice
			for i := 0; i < 2; i++ {
				err := dropAndInitializeIndexes(base.TestCtx(t), n1qlStore, test.xattrs, db.IsServerless())
				require.NoError(t, err, "Error dropping and initialising all indexes on bucket")
			}
			// check to see if current indexes match what is expected by the rest of the test
			// if not we drop and reinitialize these indexes using the overall test environment variables for XATTRS
			err := validateExpectedIndexes(n1qlStore, base.TestUseXattrs(), db.IsServerless())
			if err != nil {
				err = dropAndInitializeIndexes(base.TestCtx(t), n1qlStore, base.TestUseXattrs(), db.IsServerless())
				require.NoError(t, err)
			}
		})
	}

}

// Reset bucket state
func validateExpectedIndexes(niqlStore base.N1QLStore, xattrs, isServerless bool) error {
	// Watch and wait some time for indexes to come online
	err := niqlStore.WaitForIndexesOnline(sgIndexNames(xattrs, isServerless), true)
	if err != nil {
		return err
	}
	return nil
}

// sgIndexNames returns all the names of sync gateway indexes based on XATTRS being enabled or not
func sgIndexNames(xattrs, isServerless bool) []string {
	allSGIndexes := make([]string, 0)

	for _, sgIndex := range sgIndexes {
		fullIndexName := sgIndex.fullIndexName(xattrs)
		if sgIndex.isXattrOnly() && !xattrs {
			continue
		}
		if sgIndex.creationMode == Serverless && !isServerless {
			continue
		}
		allSGIndexes = append(allSGIndexes, fullIndexName)
	}
	return allSGIndexes
}

func TestPostUpgradeIndexesSimple(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	require.True(t, db.Bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql))

	collection := db.GetSingleDatabaseCollection()
	n1qlStore, ok := base.AsN1QLStore(collection.dataStore)
	require.True(t, ok)

	options := InitializeIndexOptions{
		FailFast:        false,
		NumReplicas:     0,
		Serverless:      db.IsServerless(),
		UseXattrs:       !db.UseXattrs(),
		MetadataIndexes: IndexesAll,
	}

	var expectedRemovedIndexes []string
	for _, sgIndex := range sgIndexes {
		if sgIndex.shouldCreate(options) {
			// when xattrs will be true, this index doesn't exist for removal
			if sgIndex.simpleName != indexNames[IndexTombstones] {
				expectedRemovedIndexes = append(expectedRemovedIndexes, sgIndex.fullIndexName(db.UseXattrs()))
			}
		} else if sgIndex.simpleName == indexNames[IndexTombstones] {
			// when xattrs will be false, we don't see this as an index to create, but we do expect to remove it
			expectedRemovedIndexes = append(expectedRemovedIndexes, sgIndex.fullIndexName(db.UseXattrs()))
		}
	}
	sort.Strings(expectedRemovedIndexes)

	// We don't know the current state of the bucket (may already have xattrs enabled), so run
	// an initial cleanup to remove existing obsolete indexes
	removedIndexes, removeErr := removeObsoleteIndexes(n1qlStore, false, db.UseXattrs(), db.UseViews(), sgIndexes)
	log.Printf("removedIndexes: %+v", removedIndexes)
	require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in setup case")

	options.UseXattrs = db.UseXattrs()
	if !db.OnlyDefaultCollection() {
		options.MetadataIndexes = IndexesWithoutMetadata
	}

	defer func() {
		// Restore indexes after test
		assert.NoError(t, InitializeIndexes(ctx, n1qlStore, options))
	}()

	err := InitializeIndexes(ctx, n1qlStore, options)
	require.NoError(t, err)

	// Running w/ opposite xattrs flag should preview removal of the indexes associated with this db context
	removedIndexes, removeErr = removeObsoleteIndexes(n1qlStore, true, !db.UseXattrs(), db.UseViews(), sgIndexes)
	sort.Strings(removedIndexes)
	require.EqualValues(t, expectedRemovedIndexes, removedIndexes)
	require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in preview mode")

	// Running again w/ preview=false to perform cleanup
	removedIndexes, removeErr = removeObsoleteIndexes(n1qlStore, false, !db.UseXattrs(), db.UseViews(), sgIndexes)
	sort.Strings(removedIndexes)
	require.Equal(t, expectedRemovedIndexes, removedIndexes)
	require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in non-preview mode")

	// One more time to make sure they are actually gone
	removedIndexes, removeErr = removeObsoleteIndexes(n1qlStore, false, !db.UseXattrs(), db.UseViews(), sgIndexes)
	require.Len(t, removedIndexes, 0)
	require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in post-cleanup no-op")

}

func TestPostUpgradeIndexesVersionChange(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	require.True(t, db.Bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql))
	collection := db.GetSingleDatabaseCollection()
	n1qlStore, ok := base.AsN1QLStore(collection.dataStore)
	assert.True(t, ok)

	defer func() {
		// Restore indexes after test
		options := InitializeIndexOptions{
			FailFast:    false,
			NumReplicas: 0,
			Serverless:  db.IsServerless(),
			UseXattrs:   db.UseXattrs(),
		}
		err := InitializeIndexes(ctx, n1qlStore, options)
		assert.NoError(t, err)
	}()

	copiedIndexes := copySGIndexes(sgIndexes)

	// Validate that removeObsoleteIndexes is a no-op for the default case
	removedIndexes, removeErr := removeObsoleteIndexes(n1qlStore, true, db.UseXattrs(), db.UseViews(), copiedIndexes)
	log.Printf("removedIndexes: %+v", removedIndexes)
	assert.Equal(t, 0, len(removedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in no-op case")

	// Hack sgIndexes to simulate new version of indexes
	accessIndex := copiedIndexes[IndexAccess]
	restoreIndex := copiedIndexes[IndexAccess]
	defer func() {
		copiedIndexes[IndexAccess] = restoreIndex
	}()

	accessIndex.version = 2
	accessIndex.previousVersions = []int{1}
	copiedIndexes[IndexAccess] = accessIndex

	// Validate that removeObsoleteIndexes now triggers removal of one index
	removedIndexes, removeErr = removeObsoleteIndexes(n1qlStore, true, db.UseXattrs(), db.UseViews(), copiedIndexes)
	log.Printf("removedIndexes: %+v", removedIndexes)
	assert.Equal(t, 1, len(removedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes with hacked sgIndexes")

}

func TestPostUpgradeMultipleCollections(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}
	if !base.TestUseXattrs() {
		t.Skip("For simplicity of the test, run with xattrs=true as the likely upgrade case of multiple collections")
	}

	tb := base.GetTestBucket(t)
	defer tb.Close()

	dbOptions := DatabaseContextOptions{}
	if base.TestsUseNamedCollections() {
		numCollections := 2
		base.RequireNumTestDataStores(t, numCollections)
		dbOptions.Scopes = GetScopesOptions(t, tb, numCollections)
	}

	db, ctx := SetupTestDBForDataStoreWithOptions(t, tb, dbOptions)
	defer db.Close(ctx)

	// make sure RemoveObsoleteIndexes is a no-op before adding obsolete indexes
	for _, preview := range []bool{true, false} {
		removedIndexes, removeErr := db.RemoveObsoleteIndexes(base.TestCtx(t), preview)
		require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in no-op case")
		require.Equal(t, 0, len(removedIndexes))
	}
	useXattrs := false
	options := InitializeIndexOptions{
		FailFast:    false,
		NumReplicas: 0,
		Serverless:  false,
		UseXattrs:   useXattrs,
	}

	for _, dataStore := range db.getDataStores() {
		n1qlStore, ok := base.AsN1QLStore(dataStore)
		assert.True(t, ok)
		err := InitializeIndexes(ctx, n1qlStore, options)
		require.NoError(t, err)
	}

	nonXattrsIndexesPerCollection := len(GetIndexesName(options))
	obsoleteIndexCount := nonXattrsIndexesPerCollection * len(db.getDataStores())

	// make sure RemoveObsoleteIndexes is a no-op before adding obsolete indexes
	for _, preview := range []bool{true, false} {
		removedIndexes, removeErr := db.RemoveObsoleteIndexes(base.TestCtx(t), preview)
		fmt.Println(removedIndexes)
		require.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in no-op case")
		require.Equal(t, obsoleteIndexCount, len(removedIndexes))
	}
}

func TestRemoveIndexesUseViewsTrueAndFalse(t *testing.T) {

	if base.TestsUseNamedCollections() {
		// we could push the restriction up into SG such that views + non-default is disallowed for CBS and allowed for Walrus?
		t.Skip("InitializeViews only works on default collection")
	}

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}
	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	copiedIndexes := copySGIndexes(sgIndexes)

	require.True(t, db.Bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql))
	collection := db.GetSingleDatabaseCollection()
	n1QLStore, ok := base.AsN1QLStore(collection.dataStore)
	assert.True(t, ok)

	viewStore, ok := collection.dataStore.(sgbucket.ViewStore)
	require.True(t, ok)

	_, err := removeObsoleteDesignDocs(ctx, viewStore, !db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(ctx, viewStore, !db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)

	expectedIndexes := int(indexTypeCount)

	if !db.UseXattrs() {
		expectedIndexes--
	}

	options := InitializeIndexOptions{
		FailFast:    false,
		NumReplicas: 0,
		Serverless:  db.IsServerless(),
		UseXattrs:   db.UseXattrs(),
	}
	if db.OnlyDefaultCollection() {
		options.MetadataIndexes = IndexesAll

	}
	defer func() {
		// Restore ddocs after test
		err = InitializeViews(ctx, collection.dataStore)
		assert.NoError(t, err)

		// Restore indexes after test
		err = InitializeIndexes(ctx, n1QLStore, options)
		assert.NoError(t, err)
	}()
	for _, sgIndex := range copiedIndexes {
		if !sgIndex.shouldCreate(options) {
			expectedIndexes--
		}
	}

	removedIndexes, removeErr := removeObsoleteIndexes(n1QLStore, false, db.UseXattrs(), true, copiedIndexes)
	assert.Equal(t, expectedIndexes, len(removedIndexes))
	assert.NoError(t, removeErr)

	removedIndexes, removeErr = removeObsoleteIndexes(n1QLStore, false, db.UseXattrs(), false, copiedIndexes)
	require.Len(t, removedIndexes, 0)
	assert.NoError(t, removeErr)

	// Cleanup design docs created during test
	_, err = removeObsoleteDesignDocs(ctx, viewStore, db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(ctx, viewStore, db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(ctx, viewStore, !db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(ctx, viewStore, !db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)

}

func TestRemoveObsoleteIndexOnError(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db, ctx := setupTestDB(t)
	defer db.Close(ctx)

	dataStore := GetSingleDatabaseCollection(t, db.DatabaseContext).dataStore

	defer func() {
		// Restore indexes after test
		n1qlStore, ok := base.AsN1QLStore(dataStore)
		assert.True(t, ok)
		options := InitializeIndexOptions{
			NumReplicas: 0,
			Serverless:  db.IsServerless(),
			UseXattrs:   db.UseXattrs(),
		}
		err := InitializeIndexes(ctx, n1qlStore, options)
		assert.NoError(t, err)

	}()
	copiedIndexes := copySGIndexes(sgIndexes)
	require.True(t, db.Bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql))

	// Use existing versions of IndexAccess and IndexChannels and create an old version that will be removed by obsolete
	// indexes. Resulting from the removal candidates for removeObsoleteIndexes will be:
	// All previous versions and opposite of current xattr setting eg. for this test ran with non-xattrs:
	// [sg_channels_x2 sg_channels_x1 sg_channels_1 sg_access_x2 sg_access_x1 sg_access_1]
	testIndexes := map[SGIndexType]SGIndex{}

	accessIndex := copiedIndexes[IndexAccess]
	accessIndex.version = 2
	accessIndex.previousVersions = []int{1}
	testIndexes[IndexAccess] = accessIndex

	channelIndex := copiedIndexes[IndexChannels]
	channelIndex.version = 2
	channelIndex.previousVersions = []int{1}
	testIndexes[IndexChannels] = channelIndex

	n1qlStore, ok := base.AsN1QLStore(dataStore)
	require.True(t, ok)

	removedIndex, removeErr := removeObsoleteIndexes(n1qlStore, false, db.UseXattrs(), db.UseViews(), testIndexes)
	assert.NoError(t, removeErr)

	if base.TestUseXattrs() {
		assert.Contains(t, removedIndex, "sg_channels_x1")
	} else {
		assert.Contains(t, removedIndex, "sg_channels_1")
	}

}

func TestIsIndexerError(t *testing.T) {
	var err error
	assert.False(t, isIndexerError(err))
	err = errors.New("MCResponse status=KEY_ENOENT, opcode=0x89, opaque=0")
	assert.False(t, isIndexerError(err))
	err = errors.New("err:[5000]  MCResponse status=KEY_ENOENT, opcode=0x89, opaque=0")
	assert.True(t, isIndexerError(err))
}

// dropAndInitializeIndexes drops and reinitialize all sync gateway indexes
func dropAndInitializeIndexes(ctx context.Context, n1qlStore base.N1QLStore, xattrs, isServerless bool) error {
	dropErr := base.DropAllIndexes(ctx, n1qlStore)
	if dropErr != nil {
		return dropErr
	}

	options := InitializeIndexOptions{
		FailFast:    false,
		NumReplicas: 0,
		Serverless:  isServerless,
		UseXattrs:   xattrs,
	}
	initErr := InitializeIndexes(ctx, n1qlStore, options)
	if initErr != nil {
		return initErr
	}

	// Recreate the primary index required by the test bucket pooling framework
	err := n1qlStore.CreatePrimaryIndex(base.PrimaryIndexName, nil)
	if err != nil {
		return err
	}

	return nil
}
