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
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

func TestRemoveObsoleteDesignDocs(t *testing.T) {

	base.ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {
		mapFunction := `function (doc, meta) { emit(); }`

		// Add some design docs in the old format
		err := bucket.PutDDoc(DesignDocSyncGatewayPrefix, &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		assert.NoError(t, err, "Unable to create design doc (DesignDocSyncGatewayPrefix)")

		err = bucket.PutDDoc(DesignDocSyncHousekeepingPrefix, &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"all_docs": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		assert.NoError(t, err, "Unable to create design doc (DesignDocSyncHousekeepingPrefix)")

		// Add some user design docs that shouldn't be removed
		err = bucket.PutDDoc("sync_gateway_user_ddoc", &sgbucket.DesignDoc{
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
		removedDDocs, removeErr := removeObsoleteDesignDocs(bucket, true, true)
		assert.NoError(t, removeErr, "Error removing previous design docs")
		assert.Equal(t, 2, len(removedDDocs))
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

		// Re-verify ddocs still exist (preview)
		assert.True(t, designDocExists(bucket, DesignDocSyncGatewayPrefix), "Design doc doesn't exist")
		assert.True(t, designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Design doc doesn't exist")
		assert.True(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc should exist")

		// Invoke removal in non-preview mode
		removedDDocs, removeErr = removeObsoleteDesignDocs(bucket, false, true)
		assert.NoError(t, removeErr, "Error removing previous design docs")
		assert.Equal(t, 2, len(removedDDocs))
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

		// Verify ddocs are in expected state
		assert.True(t, !designDocExists(bucket, DesignDocSyncGatewayPrefix), "Removed design doc still exists")
		assert.True(t, !designDocExists(bucket, DesignDocSyncHousekeepingPrefix), "Removed design doc still exists")
		assert.True(t, designDocExists(bucket, "sync_gateway_user_ddoc"), "Design doc should exist")
	})
}

func TestRemoveDesignDocsUseViewsTrueAndFalse(t *testing.T) {
	DesignDocPreviousVersions = []string{"2.0"}

	base.ForAllDataStores(t, func(t *testing.T, bucket sgbucket.DataStore) {

		mapFunction := `function (doc, meta){ emit(); }`

		err := bucket.PutDDoc(DesignDocSyncGatewayPrefix+"_2.0", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		assert.NoError(t, err)
		err = bucket.PutDDoc(DesignDocSyncHousekeepingPrefix+"_2.0", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		assert.NoError(t, err)
		err = bucket.PutDDoc(DesignDocSyncGatewayPrefix+"_2.1", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		assert.NoError(t, err)
		err = bucket.PutDDoc(DesignDocSyncHousekeepingPrefix+"_2.1", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		assert.NoError(t, err)

		useViewsTrueRemovalPreview := []string{"sync_gateway_2.0", "sync_housekeeping_2.0"}
		removedDDocsPreview, _ := removeObsoleteDesignDocs(bucket, true, true)
		assert.Equal(t, useViewsTrueRemovalPreview, removedDDocsPreview)

		useViewsFalseRemovalPreview := []string{"sync_gateway_2.0", "sync_housekeeping_2.0", "sync_gateway_2.1", "sync_housekeeping_2.1"}
		removedDDocsPreview, _ = removeObsoleteDesignDocs(bucket, true, false)
		assert.Equal(t, useViewsFalseRemovalPreview, removedDDocsPreview)

		useViewsTrueRemoval := []string{"sync_gateway_2.0", "sync_housekeeping_2.0"}
		removedDDocs, _ := removeObsoleteDesignDocs(bucket, false, true)
		assert.Equal(t, useViewsTrueRemoval, removedDDocs)

		useViewsTrueRemoval = []string{"sync_gateway_2.1", "sync_housekeeping_2.1"}
		removedDDocs, _ = removeObsoleteDesignDocs(bucket, false, false)
		assert.Equal(t, useViewsTrueRemoval, removedDDocs)
	})
}

//Test remove obsolete design docs returns the same in both preview and non-preview
func TestRemoveObsoleteDesignDocsErrors(t *testing.T) {

	DesignDocPreviousVersions = []string{"test"}

	leakyBucketConfig := base.LeakyBucketConfig{
		DDocGetErrorCount:    1,
		DDocDeleteErrorCount: 1,
	}

	bucket := base.NewLeakyBucket(base.GetTestBucket(t), leakyBucketConfig)
	defer bucket.Close()

	mapFunction := `function (doc, meta){ emit(); }`

	err := bucket.PutDDoc(DesignDocSyncGatewayPrefix+"_test", &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	assert.NoError(t, err)
	err = bucket.PutDDoc(DesignDocSyncHousekeepingPrefix+"_test", &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})

	removedDDocsPreview, _ := removeObsoleteDesignDocs(bucket, true, false)
	removedDDocsNonPreview, _ := removeObsoleteDesignDocs(bucket, false, false)

	assert.Equal(t, removedDDocsPreview, removedDDocsNonPreview)

}

func designDocExists(bucket base.Bucket, ddocName string) bool {
	_, err := bucket.GetDDoc(ddocName)
	return err == nil
}
