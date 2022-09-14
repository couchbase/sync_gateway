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
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRemoveObsoleteDesignDocs(t *testing.T) {

	base.ForAllDataStores(t, func(t *testing.T, ctx context.Context, bucket base.Bucket) {
		mapFunction := `function (doc, meta) { emit(); }`

		// Add some design docs in the old format
		err := bucket.PutDDoc(ctx, DesignDocSyncGatewayPrefix, &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		require.NoError(t, err, "Unable to create design doc (DesignDocSyncGatewayPrefix)")

		err = bucket.PutDDoc(ctx, DesignDocSyncHousekeepingPrefix, &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"all_docs": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		require.NoError(t, err, "Unable to create design doc (DesignDocSyncHousekeepingPrefix)")

		// Add some user design docs that shouldn't be removed
		err = bucket.PutDDoc(ctx, "sync_gateway_user_ddoc", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels_custom": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		require.NoError(t, err, "Unable to create design doc (sync_gateway_user_created)")

		// Verify creation was successful
		base.RequireAllAssertions(t,
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncGatewayPrefix),
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncHousekeepingPrefix),
			assertDesignDocExists(t, ctx, bucket, "sync_gateway_user_ddoc"),
		)

		// Invoke removal in preview mode
		removedDDocs, removeErr := removeObsoleteDesignDocs(ctx, bucket, true, true)
		require.NoError(t, removeErr, "Error removing previous design docs")
		assert.Equal(t, 2, len(removedDDocs))
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

		// Re-verify ddocs still exist (preview)
		base.RequireAllAssertions(t,
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncGatewayPrefix),
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncHousekeepingPrefix),
			assertDesignDocExists(t, ctx, bucket, "sync_gateway_user_ddoc"),
		)

		// Invoke removal in non-preview mode
		removedDDocs, removeErr = removeObsoleteDesignDocs(ctx, bucket, false, true)
		require.NoError(t, removeErr, "Error removing previous design docs")
		assert.Equal(t, 2, len(removedDDocs))
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncGatewayPrefix), "Missing design doc from removed set")
		assert.True(t, base.StringSliceContains(removedDDocs, DesignDocSyncHousekeepingPrefix), "Missing design doc from removed set")

		// Verify ddocs are in expected state
		base.RequireAllAssertions(t,
			assertDesignDocNotExists(t, ctx, bucket, DesignDocSyncGatewayPrefix),
			assertDesignDocNotExists(t, ctx, bucket, DesignDocSyncHousekeepingPrefix),
			assertDesignDocExists(t, ctx, bucket, "sync_gateway_user_ddoc"),
		)
	})
}

func TestRemoveDesignDocsUseViewsTrueAndFalse(t *testing.T) {
	setDesignDocPreviousVersionsForTest(t, "2.0")

	base.ForAllDataStores(t, func(t *testing.T, ctx context.Context, bucket base.Bucket) {

		mapFunction := `function (doc, meta){ emit(); }`

		err := bucket.PutDDoc(ctx, DesignDocSyncGatewayPrefix+"_2.0", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		require.NoError(t, err)
		err = bucket.PutDDoc(ctx, DesignDocSyncHousekeepingPrefix+"_2.0", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		require.NoError(t, err)
		err = bucket.PutDDoc(ctx, DesignDocSyncGatewayPrefix+"_2.1", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		require.NoError(t, err)
		err = bucket.PutDDoc(ctx, DesignDocSyncHousekeepingPrefix+"_2.1", &sgbucket.DesignDoc{
			Views: sgbucket.ViewMap{
				"channels": sgbucket.ViewDef{Map: mapFunction},
			},
		})
		require.NoError(t, err)

		// Verify creation was successful
		base.RequireAllAssertions(t,
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncGatewayPrefix+"_2.0"),
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncHousekeepingPrefix+"_2.0"),
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncGatewayPrefix+"_2.1"),
			assertDesignDocExists(t, ctx, bucket, DesignDocSyncHousekeepingPrefix+"_2.1"),
		)

		useViewsTrueRemovalPreview := []string{"sync_gateway_2.0", "sync_housekeeping_2.0"}
		removedDDocsPreview, _ := removeObsoleteDesignDocs(ctx, bucket, true, true)
		assert.Equal(t, useViewsTrueRemovalPreview, removedDDocsPreview)

		useViewsFalseRemovalPreview := []string{"sync_gateway_2.0", "sync_housekeeping_2.0", "sync_gateway_2.1", "sync_housekeeping_2.1"}
		removedDDocsPreview, _ = removeObsoleteDesignDocs(ctx, bucket, true, false)
		assert.Equal(t, useViewsFalseRemovalPreview, removedDDocsPreview)

		useViewsTrueRemoval := []string{"sync_gateway_2.0", "sync_housekeeping_2.0"}
		removedDDocs, _ := removeObsoleteDesignDocs(ctx, bucket, false, true)
		require.Equal(t, useViewsTrueRemoval, removedDDocs)

		useViewsTrueRemoval = []string{"sync_gateway_2.1", "sync_housekeeping_2.1"}
		removedDDocs, _ = removeObsoleteDesignDocs(ctx, bucket, false, false)
		require.Equal(t, useViewsTrueRemoval, removedDDocs)
	})
}

// Test remove obsolete design docs returns the same in both preview and non-preview
func TestRemoveObsoleteDesignDocsErrors(t *testing.T) {
	setDesignDocPreviousVersionsForTest(t, "test")

	ctx, tbucket := base.GetTestBucket(t)
	bucket := base.NewLeakyBucket(tbucket, base.LeakyBucketConfig{})
	defer bucket.Close(ctx)

	mapFunction := `function (doc, meta){ emit(); }`

	err := bucket.PutDDoc(ctx, DesignDocSyncGatewayPrefix+"_test", &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	require.NoError(t, err)
	err = bucket.PutDDoc(ctx, DesignDocSyncHousekeepingPrefix+"_test", &sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"channels": sgbucket.ViewDef{Map: mapFunction},
		},
	})
	require.NoError(t, err)

	// Verify creation was successful
	base.RequireAllAssertions(t,
		assertDesignDocExists(t, ctx, bucket, DesignDocSyncGatewayPrefix+"_test"),
		assertDesignDocExists(t, ctx, bucket, DesignDocSyncHousekeepingPrefix+"_test"),
	)

	lb, ok := base.AsLeakyBucket(bucket)
	require.Truef(t, ok, "bucket is not a leaky bucket")
	lb.SetDDocGetErrorCount(1)
	lb.SetDDocDeleteErrorCount(1)

	removedDDocsPreview, err := removeObsoleteDesignDocs(ctx, bucket, true, false)
	assert.NoError(t, err)
	removedDDocsNonPreview, err := removeObsoleteDesignDocs(ctx, bucket, false, false)
	require.NoError(t, err)

	assert.Equalf(t, removedDDocsPreview, removedDDocsNonPreview, "preview and non-preview should return the same design docs")
}
