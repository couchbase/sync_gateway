package db

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/gocb"
	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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
	defer base.DropAllBucketIndexes(goCbBucket)

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
	removedIndexes, removeErr := removeObsoleteIndexes(gocbBucket, false, db.UseXattrs())
	log.Printf("removedIndexes: %+v", removedIndexes)
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in setup case")

	// Running w/ opposite xattrs flag should preview removal of the indexes associated with this db context
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, true, !db.UseXattrs())
	goassert.Equals(t, len(removedIndexes), int(expectedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in preview mode")

	// Running again w/ preview=false to perform cleanup
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, false, !db.UseXattrs())
	goassert.Equals(t, len(removedIndexes), int(expectedIndexes))
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes in non-preview mode")

	// One more time to make sure they are actually gone
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, false, !db.UseXattrs())
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
	removedIndexes, removeErr := removeObsoleteIndexes(gocbBucket, true, db.UseXattrs())
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
	removedIndexes, removeErr = removeObsoleteIndexes(gocbBucket, true, db.UseXattrs())
	log.Printf("removedIndexes: %+v", removedIndexes)
	goassert.Equals(t, len(removedIndexes), 1)
	assert.NoError(t, removeErr, "Unexpected error running removeObsoleteIndexes with hacked sgIndexes")
}

func TestRemoveObsoleteIndexOnFail(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Index tests require Couchbase Bucket")
	}

	db, testBucket := setupTestDB(t)
	defer testBucket.Close()
	defer tearDownTestDB(t, db)

	leakyBucket := base.NewLeakyBucket(testBucket.Bucket, base.LeakyBucketConfig{DropIndexErrorCount: 1})
	b, ok := leakyBucket.(*base.LeakyBucket)
	assert.True(t, ok)

	gocbBucket, ok := base.AsGoCBBucket(testBucket.Bucket)
	assert.True(t, ok)

	for _, name := range indexNames {
		err := gocbBucket.CreatePrimaryIndex("sg_"+name+"_x1", &base.N1qlIndexOptions{})
		assert.NoError(t, err)
	}

	removedIndexes, removeError := removeObsoleteIndexes(b, false, db.UseXattrs())
	assert.True(t, len(indexNames) == len(removedIndexes)+1)
	assert.NoError(t, err)
	base.DropAllBucketIndexes(gocbBucket)
}
