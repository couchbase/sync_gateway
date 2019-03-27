package db

import (
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncGatewayPrefix)")

	err = bucket.PutDDoc(DesignDocSyncHousekeepingPrefix, sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"all_docs": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (DesignDocSyncHousekeepingPrefix)")

	// Add some user design docs that shouldn't be removed
	err = bucket.PutDDoc("sync_gateway_user_ddoc", sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels_custom": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err, "Unable to create design doc (sync_gateway_user_created)")

	// Verify creation was successful
	assert.True(t, designDocExists(bucket, DesignDocSyncGatewayPrefix), "Design doc doesn't exist")
	assert.True(t, designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Design doc doesn't exist")
	assert.True(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc doesn't exist")

	// Invoke removal in preview mode
	removedDDocs, removeErr := removeObsoleteDesignDocs(bucket, true)
	assert.NoError(t, removeErr, "Error removing previous design docs")
	goassert.Equals(t, len(removedDDocs), 2)
	assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
	assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

	// Re-verify ddocs still exist (preview)
	assert.True(t, designDocExists(bucket, DesignDocSyncGatewayPrefix), "Design doc doesn't exist")
	assert.True(t, designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Design doc doesn't exist")
	assert.True(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc should exist")

	// Invoke removal in non-preview mode
	removedDDocs, removeErr = removeObsoleteDesignDocs(bucket, false)
	assert.NoError(t, removeErr, "Error removing previous design docs")
	goassert.Equals(t, len(removedDDocs), 2)
	assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
	assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

	// Verify ddocs are in expected state
	assert.True(t, !designDocExists(bucket, DesignDocSyncGatewayPrefix), "Removed design doc still exists")
	assert.True(t, !designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Removed design doc still exists")
	assert.True(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc should exist")

}

//Test remove obsolete design docs returns the same in both preview and non-preview
func TestRemoveObsoleteDesignDocsErrors(t *testing.T) {

	DesignDocPreviousVersions = []string{"test"}

	leakyBucketConfig := base.LeakyBucketConfig{
		DDocGetErrorCount:    1,
		DDocDeleteErrorCount: 1,
	}
	testBucket := testLeakyBucket(leakyBucketConfig)
	defer testBucket.Close()

	bucket := testBucket
	mapFunction := `function (doc, meta){ emit(); }`

	err := bucket.PutDDoc(DesignDocSyncGatewayPrefix+"_test", sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err)
	err = bucket.PutDDoc(DesignDocSyncHousekeepingPrefix+"_test", sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})

	removedDDocsPreview, _ := removeObsoleteDesignDocs(bucket, true)
	removedDDocsNonPreview, _ := removeObsoleteDesignDocs(bucket, false)

	assert.Equal(t, removedDDocsPreview, removedDDocsNonPreview)

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
