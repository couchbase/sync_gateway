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
	"errors"
	"fmt"
	"log"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitializeIndexes(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db := setupTestDB(t)
	defer db.Close()

	n1qlStore, isGoCBBucket := base.AsN1QLStore(db.Bucket)
	require.True(t, isGoCBBucket)

	dropErr := base.DropAllBucketIndexes(n1qlStore)
	assert.NoError(t, dropErr, "Error dropping all indexes")

	initErr := InitializeIndexes(n1qlStore, db.UseXattrs(), 0)
	assert.NoError(t, initErr, "Error initializing all indexes")

	// Recreate the primary index required by the test bucket pooling framework
	err := n1qlStore.CreatePrimaryIndex(base.PrimaryIndexName, nil)
	assert.NoError(t, err)

	validateErr := validateAllIndexesOnline(db.Bucket)
	assert.NoError(t, validateErr, "Error validating indexes online")

}

// Reset bucket state
func validateAllIndexesOnline(bucket base.Bucket) error {

	n1QLStore, ok := base.AsN1QLStore(bucket)
	if !ok {
		return fmt.Errorf("Bucket is not gocb bucket: %T", bucket)
	}

	// Retrieve all indexes
	getIndexesStatement := fmt.Sprintf("SELECT indexes.name, indexes.state from system:indexes where keyspace_id = %q", n1QLStore.GetName())
	results, err := n1QLStore.Query(getIndexesStatement, nil, base.RequestPlus, true)
	if err != nil {
		return err
	}

	var indexRow struct {
		Name  string
		State string
	}

	for results.Next(&indexRow) {
		if indexRow.State != base.IndexStateOnline {
			return fmt.Errorf("Index %s is not online", indexRow.Name)
		} else {
			log.Printf("Validated index %s is %s", indexRow.Name, indexRow.State)
		}
	}
	closeErr := results.Close()
	return closeErr
}

func TestPostUpgradeIndexesSimple(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db := setupTestDB(t)
	defer db.Close()

	require.True(t, db.Bucket.IsSupported(sgbucket.DataStoreFeatureN1ql))

	n1qlStore, ok := base.AsN1QLStore(db.Bucket)
	assert.True(t, ok)

	// We have one xattr-only index - adjust expected indexes accordingly
	expectedIndexes := int(indexTypeCount)
	if !db.UseXattrs() {
		expectedIndexes--
	}

	// We don't know the current state of the bucket (may already have xattrs enabled), so run
	// an initial cleanup to remove existing obsolete indexes
	removedIndexes, removeErr := removeObsoleteIndexes(n1qlStore, false, db.UseXattrs(), db.UseViews(), sgIndexes)
	log.Printf("removedIndexes: %+v", removedIndexes)
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in setup case")

	err := InitializeIndexes(n1qlStore, db.UseXattrs(), 0)
	assert.NoError(t, err)

	// Running w/ opposite xattrs flag should preview removal of the indexes associated with this db context
	removedIndexes, removeErr = removeObsoleteIndexes(n1qlStore, true, !db.UseXattrs(), db.UseViews(), sgIndexes)
	assert.Equal(t, int(expectedIndexes), len(removedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in preview mode")

	// Running again w/ preview=false to perform cleanup
	removedIndexes, removeErr = removeObsoleteIndexes(n1qlStore, false, !db.UseXattrs(), db.UseViews(), sgIndexes)
	assert.Equal(t, int(expectedIndexes), len(removedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in non-preview mode")

	// One more time to make sure they are actually gone
	removedIndexes, removeErr = removeObsoleteIndexes(n1qlStore, false, !db.UseXattrs(), db.UseViews(), sgIndexes)
	assert.Equal(t, 0, len(removedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in post-cleanup no-op")

	// Restore indexes after test
	err = InitializeIndexes(n1qlStore, db.UseXattrs(), 0)
	assert.NoError(t, err)
}

func TestPostUpgradeIndexesVersionChange(t *testing.T) {
	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db := setupTestDB(t)
	defer db.Close()

	require.True(t, db.Bucket.IsSupported(sgbucket.DataStoreFeatureN1ql))
	n1qlStore, ok := base.AsN1QLStore(db.Bucket)
	assert.True(t, ok)

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

	// Restore indexes after test
	err := InitializeIndexes(n1qlStore, db.UseXattrs(), 0)
	assert.NoError(t, err)

	validateErr := validateAllIndexesOnline(db.Bucket)
	assert.NoError(t, validateErr, "Error validating indexes online")
}

func TestRemoveIndexesUseViewsTrueAndFalse(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db := setupTestDB(t)
	defer db.Close()

	copiedIndexes := copySGIndexes(sgIndexes)

	require.True(t, db.Bucket.IsSupported(sgbucket.DataStoreFeatureN1ql))
	n1QLStore, ok := base.AsN1QLStore(db.Bucket)
	assert.True(t, ok)

	_, err := removeObsoleteDesignDocs(db.Bucket, !db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(db.Bucket, !db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)

	expectedIndexes := int(indexTypeCount)

	if !db.UseXattrs() {
		expectedIndexes--
	}

	removedIndexes, removeErr := removeObsoleteIndexes(n1QLStore, false, db.UseXattrs(), true, copiedIndexes)
	assert.Equal(t, expectedIndexes, len(removedIndexes))
	assert.NoError(t, removeErr)

	removedIndexes, removeErr = removeObsoleteIndexes(n1QLStore, false, db.UseXattrs(), false, copiedIndexes)
	require.Len(t, removedIndexes, 0)
	assert.NoError(t, removeErr)

	// Cleanup design docs created during test
	_, err = removeObsoleteDesignDocs(db.Bucket, db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(db.Bucket, db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(db.Bucket, !db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(db.Bucket, !db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)

	// Restore ddocs after test
	err = InitializeViews(db.Bucket)
	assert.NoError(t, err)

	// Restore indexes after test
	err = InitializeIndexes(n1QLStore, db.UseXattrs(), 0)
	assert.NoError(t, err)

	validateErr := validateAllIndexesOnline(db.Bucket)
	assert.NoError(t, validateErr, "Error validating indexes online")
}

func TestRemoveObsoleteIndexOnError(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	db := setupTestDB(t)
	defer db.Close()

	leakyBucket := base.NewLeakyBucket(db.Bucket, base.LeakyBucketConfig{DropIndexErrorNames: []string{"sg_access_1", "sg_access_x1"}})
	copiedIndexes := copySGIndexes(sgIndexes)
	require.True(t, db.Bucket.IsSupported(sgbucket.DataStoreFeatureN1ql))

	//Use existing versions of IndexAccess and IndexChannels and create an old version that will be removed by obsolete
	//indexes. Resulting from the removal candidates for removeObsoleteIndexes will be:
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

	removedIndex, removeErr := removeObsoleteIndexes(leakyBucket, false, db.UseXattrs(), db.UseViews(), testIndexes)
	assert.NoError(t, removeErr)

	if base.TestUseXattrs() {
		assert.Contains(t, removedIndex, "sg_channels_x1")
	} else {
		assert.Contains(t, removedIndex, "sg_channels_1")
	}

	// Restore indexes after test
	n1qlStore, _ := base.AsN1QLStore(db.Bucket)
	err := InitializeIndexes(n1qlStore, db.UseXattrs(), 0)
	assert.NoError(t, err)

	validateErr := validateAllIndexesOnline(db.Bucket)
	assert.NoError(t, validateErr, "Error validating indexes online")

}

func TestIsIndexerError(t *testing.T) {
	var err error
	assert.False(t, isIndexerError(err))
	err = errors.New("MCResponse status=KEY_ENOENT, opcode=0x89, opaque=0")
	assert.False(t, isIndexerError(err))
	err = errors.New("err:[5000]  MCResponse status=KEY_ENOENT, opcode=0x89, opaque=0")
	assert.True(t, isIndexerError(err))
}
