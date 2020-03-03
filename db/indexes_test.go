package db

import (
	"errors"
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitializeIndexes(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	goCbBucket, isGoCBBucket := base.AsGoCBBucket(testBucket)
	goassert.True(t, isGoCBBucket)

	dropErr := base.DropAllBucketIndexes(goCbBucket)
	assert.NoError(t, dropErr, "Error dropping all indexes")

	initErr := InitializeIndexes(testBucket, db.UseXattrs(), 0)
	assert.NoError(t, initErr, "Error initializing all indexes")

	validateErr := validateAllIndexesOnline(testBucket)
	assert.NoError(t, validateErr, "Error validating indexes online")

}

// Reset bucket state
func validateAllIndexesOnline(bucket base.Bucket) error {

	gocbBucket, ok := base.AsGoCBBucket(bucket)
	if !ok {
		return fmt.Errorf("Bucket is not gocb bucket: %T", bucket)
	}

	// Retrieve all indexes
	getIndexesStatement := fmt.Sprintf("SELECT indexes.name, indexes.state from system:indexes where keyspace_id = %q", gocbBucket.GetName())
	n1qlQuery := gocb.NewN1qlQuery(getIndexesStatement)
	results, err := gocbBucket.ExecuteN1qlQuery(n1qlQuery, nil)
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

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	gocbBucket, ok := base.AsGoCBBucket(testBucket.Bucket)
	assert.True(t, ok)

	// We have one xattr-only index - adjust expected indexes accordingly
	expectedIndexes := int(indexTypeCount)
	if !db.UseXattrs() {
		expectedIndexes--
	}

	// We don't know the current state of the bucket (may already have xattrs enabled), so run
	// an initial cleanup to remove existing obsolete indexes
	removedIndexes, removeErr := removeObsoleteIndexes(gocbBucket, false, db.UseXattrs(), db.UseViews())
	log.Printf("removedIndexes: %+v", removedIndexes)
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in setup case")

	err := InitializeIndexes(testBucket, db.UseXattrs(), 0)
	assert.NoError(t, err)

	// Running w/ opposite xattrs flag should preview removal of the indexes associated with this db context
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, true, !db.UseXattrs(), db.UseViews())
	goassert.Equals(t, len(removedIndexes), int(expectedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in preview mode")

	// Running again w/ preview=false to perform cleanup
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, false, !db.UseXattrs(), db.UseViews())
	goassert.Equals(t, len(removedIndexes), int(expectedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in non-preview mode")

	// One more time to make sure they are actually gone
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, false, !db.UseXattrs(), db.UseViews())
	goassert.Equals(t, len(removedIndexes), 0)
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in post-cleanup no-op")

}

func TestPostUpgradeIndexesVersionChange(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	gocbBucket, ok := base.AsGoCBBucket(testBucket.Bucket)
	assert.True(t, ok)

	// Validate that removeObsoleteIndexes is a no-op for the default case
	removedIndexes, removeErr := removeObsoleteIndexes(gocbBucket, true, db.UseXattrs(), db.UseViews())
	log.Printf("removedIndexes: %+v", removedIndexes)
	goassert.Equals(t, len(removedIndexes), 0)
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in no-op case")

	// Hack sgIndexes to simulate new version of indexes
	accessIndex := sgIndexes[IndexAccess]
	restoreIndex := sgIndexes[IndexAccess]
	defer func() {
		sgIndexes[IndexAccess] = restoreIndex
	}()

	accessIndex.version = 2
	accessIndex.previousVersions = []int{1}
	sgIndexes[IndexAccess] = accessIndex

	// Validate that removeObsoleteIndexes now triggers removal of one index
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, true, db.UseXattrs(), db.UseViews())
	log.Printf("removedIndexes: %+v", removedIndexes)
	goassert.Equals(t, len(removedIndexes), 1)
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes with hacked sgIndexes")
}

func TestRemoveIndexesUseViewsTrueAndFalse(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	gocbBucket, ok := base.AsGoCBBucket(testBucket.Bucket)
	assert.True(t, ok)

	_, err := removeObsoleteDesignDocs(gocbBucket, !db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(gocbBucket, !db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)

	expectedIndexes := int(indexTypeCount)

	if !db.UseXattrs() {
		expectedIndexes--
	}

	removedIndexes, removeErr := removeObsoleteIndexes(gocbBucket, false, db.UseXattrs(), true)
	assert.Equal(t, expectedIndexes, len(removedIndexes))
	assert.NoError(t, removeErr)

	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, false, db.UseXattrs(), false)
	require.Len(t, removedIndexes, 0)
	assert.NoError(t, removeErr)

	// Cleanup design docs created during test
	_, err = removeObsoleteDesignDocs(gocbBucket, db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(gocbBucket, db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(gocbBucket, !db.UseXattrs(), db.UseViews())
	assert.NoError(t, err)
	_, err = removeObsoleteDesignDocs(gocbBucket, !db.UseXattrs(), !db.UseViews())
	assert.NoError(t, err)
}

func TestRemoveObsoleteIndexOnFail(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	leakyBucket := base.NewLeakyBucket(testBucket.Bucket, base.LeakyBucketConfig{DropIndexErrorNames: []string{"sg_access_1", "sg_access_x1"}})
	b, ok := leakyBucket.(*base.LeakyBucket)
	assert.True(t, ok)

	//Copy references to existing indexes to variable for future use
	oldIndexes := sgIndexes

	//Remove all index references used in testing and restore the earlier indexes
	defer func() {
		sgIndexes = map[SGIndexType]SGIndex{}
		sgIndexes = oldIndexes
	}()

	//Use existing versions of IndexAccess and IndexChannels and create an old version that will be removed by obsolete
	//indexes. Resulting from the removal candidates for removeObsoleteIndexes will be:
	// All previous versions and opposite of current xattr setting eg. for this test ran with non-xattrs:
	// [sg_channels_x2 sg_channels_x1 sg_channels_1 sg_access_x2 sg_access_x1 sg_access_1]
	sgIndexes = map[SGIndexType]SGIndex{}

	accessIndex := oldIndexes[IndexAccess]
	accessIndex.version = 2
	accessIndex.previousVersions = []int{1}
	sgIndexes[IndexAccess] = accessIndex

	channelIndex := oldIndexes[IndexChannels]
	channelIndex.version = 2
	channelIndex.previousVersions = []int{1}
	sgIndexes[IndexChannels] = channelIndex

	removedIndex, removeErr := removeObsoleteIndexes(b, false, db.UseXattrs(), db.UseViews())
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
