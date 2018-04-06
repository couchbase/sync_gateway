package db

import (
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func TestPostUpgradeIndexesSimple(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// We have one xattr-only index - adjust expected indexes accordingly
	expectedIndexes := int(indexTypeCount)
	if !db.UseXattrs() {
		expectedIndexes--
	}

	// We don't know the current state of the bucket (may already have xattrs enabled), so run
	// an initial cleanup to remove existing obsolete indexes
	removedIndexes, removeErr := removeObsoleteIndexes(testBucket.Bucket, false, db.UseXattrs())
	log.Printf("removedIndexes: %+v", removedIndexes)
	assertNoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in setup case")

	// Running w/ opposite xattrs flag should preview removal of the indexes associated with this db context
	removedIndexes, removeErr = removeObsoleteIndexes(testBucket.Bucket, true, !db.UseXattrs())
	assert.Equals(t, len(removedIndexes), int(expectedIndexes))
	assertNoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in preview mode")

	// Running again w/ preview=false to perform cleanup
	removedIndexes, removeErr = removeObsoleteIndexes(testBucket.Bucket, false, !db.UseXattrs())
	assert.Equals(t, len(removedIndexes), int(expectedIndexes))
	assertNoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in non-preview mode")

	// One more time to make sure they are actually gone
	removedIndexes, removeErr = removeObsoleteIndexes(testBucket.Bucket, false, !db.UseXattrs())
	assert.Equals(t, len(removedIndexes), 0)
	assertNoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in post-cleanup no-op")

}

func TestPostUpgradeIndexesVersionChange(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	// Validate that removeObsoleteIndexes is a no-op for the default case
	removedIndexes, removeErr := removeObsoleteIndexes(testBucket.Bucket, true, db.UseXattrs())
	log.Printf("removedIndexes: %+v", removedIndexes)
	assert.Equals(t, len(removedIndexes), 0)
	assertNoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in no-op case")

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
	removedIndexes, removeErr = removeObsoleteIndexes(testBucket.Bucket, true, db.UseXattrs())
	log.Printf("removedIndexes: %+v", removedIndexes)
	assert.Equals(t, len(removedIndexes), 1)
	assertNoError(t, removeErr, "Unexpected error running removeObsoleteIndexes with hacked sgIndexes")
}
