package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

// setDesignDocPreviousVersionsForTest sets the previous versions of the design docs for testing purposes and reverts to the original set once the test is done.
func setDesignDocPreviousVersionsForTest(t testing.TB, versions ...string) {
	original := DesignDocPreviousVersions
	t.Cleanup(func() {
		DesignDocPreviousVersions = original
	})
	DesignDocPreviousVersions = versions
}

// assertDesignDocExists ensures that the design doc exists in the bucket.
func assertDesignDocExists(t testing.TB, bucket base.Bucket, ddocName string) bool {
	_, err := bucket.GetDDoc(ddocName)
	return assert.NoErrorf(t, err, "Design doc %s should exist but got an error fetching it: %v", ddocName, err)
}

// assertDesignDocDoesNotExist ensures that the design doc does not exist in the bucket.
func assertDesignDocNotExists(t testing.TB, bucket base.Bucket, ddocName string) bool {
	ddoc, err := bucket.GetDDoc(ddocName)
	if err == nil {
		return assert.Failf(t, "Design doc %s should not exist but but it did: %v", ddocName, ddoc)
	}
	return assert.Truef(t, IsMissingDDocError(err), "Design doc %s should not exist but got a different error fetching it: %v", ddocName, err)
}
