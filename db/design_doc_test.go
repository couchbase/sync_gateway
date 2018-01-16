package db

import (
	"testing"

	"github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
)

func TestRemoveObsoleteDesignDocs(t *testing.T) {

	testBucket := testBucket()
	defer testBucket.Close()
	bucket := testBucket.Bucket
	mapFunction := `function (doc, meta) { emit(); }`

	// Add some design docs in the old format
	err := bucket.PutDDoc(DesignDocSyncGatewayPrefix, sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assertNoError(t, err, "Unable to create design doc (DesignDocSyncGatewayPrefix)")

	err = bucket.PutDDoc(DesignDocSyncHousekeepingPrefix, sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"all_docs": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assertNoError(t, err, "Unable to create design doc (DesignDocSyncHousekeepingPrefix)")

	// Add some user design docs that shouldn't be removed
	err = bucket.PutDDoc("sync_gateway_user_ddoc", sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels_custom": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assertNoError(t, err, "Unable to create design doc (sync_gateway_user_created)")

	// Verify creation was successful
	assertTrue(t, designDocExists(bucket, DesignDocSyncGatewayPrefix), "Design doc doesn't exist")
	assertTrue(t, designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Design doc doesn't exist")
	assertTrue(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc doesn't exist")

	// Invoke removal in preview mode
	removedDDocs, removeErr := removeObsoleteDesignDocs(bucket, true)
	assertNoError(t, removeErr, "Error removing previous design docs")
	assert.Equals(t, len(removedDDocs), 2)
	assertTrue(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
	assertTrue(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

	// Re-verify ddocs still exist (preview)
	assertTrue(t, designDocExists(bucket, DesignDocSyncGatewayPrefix), "Design doc doesn't exist")
	assertTrue(t, designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Design doc doesn't exist")
	assertTrue(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc should exist")

	// Invoke removal in non-preview mode
	removedDDocs, removeErr = removeObsoleteDesignDocs(bucket, false)
	assertNoError(t, removeErr, "Error removing previous design docs")
	assert.Equals(t, len(removedDDocs), 2)
	assertTrue(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
	assertTrue(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

	// Verify ddocs are in expected state
	assertTrue(t, !designDocExists(bucket, DesignDocSyncGatewayPrefix), "Removed design doc still exists")
	assertTrue(t, !designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Removed design doc still exists")
	assertTrue(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc should exist")

}

func designDocExists(bucket base.Bucket, ddocName string) bool {
	var retrievedDDoc interface{}
	err := bucket.GetDDoc(ddocName, &retrievedDDoc)
	if err != nil {
		return false
	}
	if retrievedDDoc == nil {
		return false
	}

	return true
}
