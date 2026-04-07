/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"net/http"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRosmarManagementAPI(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Rosmar/Walrus")
	}

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		MutateStartupConfig: func(config *StartupConfig) {
			config.Unsupported.RosmarBucketManagement = base.Ptr(true)
		},
	})
	defer rt.Close()

	bucketName := rt.Bucket().GetName()

	// GET /_rosmar/
	t.Run("GetRosmarBuckets", func(t *testing.T) {
		resp := rt.SendAdminRequest(http.MethodGet, "/_rosmar/", "")
		RequireStatus(t, resp, http.StatusOK)
		var body map[string]base.CollectionNames
		require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
		assert.Contains(t, body, bucketName)
		// Should have default scope/collection at least
		assert.Contains(t, body[bucketName], base.DefaultScope)
	})

	// DELETE scope
	t.Run("DeleteScope", func(t *testing.T) {
		// Create a new scope/collection first
		scopeName := "testscope"
		collectionName := "testcoll"
		dynamicBucket, ok := rt.Bucket().(sgbucket.DynamicDataStoreBucket)
		require.True(t, ok)
		err := dynamicBucket.CreateDataStore(rt.Context(), base.NewScopeAndCollectionName(scopeName, collectionName))
		require.NoError(t, err)

		// Verify it exists in GET /_rosmar/
		resp := rt.SendAdminRequest(http.MethodGet, "/_rosmar/", "")
		var body map[string]base.CollectionNames
		require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
		assert.Contains(t, body, bucketName)
		assert.Contains(t, body[bucketName], scopeName)

		// DELETE /_rosmar/bucket.scope
		resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/_rosmar/%s.%s", bucketName, scopeName), "")
		RequireStatus(t, resp, http.StatusOK)

		// Verify it's gone
		resp = rt.SendAdminRequest(http.MethodGet, "/_rosmar/", "")
		require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
		assert.NotContains(t, body[bucketName], scopeName)
	})

	// DELETE collection
	t.Run("DeleteCollection", func(t *testing.T) {
		scopeName := "testscope2"
		collectionName := "testcoll2"
		dynamicBucket, ok := rt.Bucket().(sgbucket.DynamicDataStoreBucket)
		require.True(t, ok)
		err := dynamicBucket.CreateDataStore(rt.Context(), base.NewScopeAndCollectionName(scopeName, collectionName))
		require.NoError(t, err)

		// DELETE /_rosmar/bucket.scope.collection
		resp := rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/_rosmar/%s.%s.%s", bucketName, scopeName, collectionName), "")
		RequireStatus(t, resp, http.StatusOK)

		// Verify it's gone from ListDataStores
		dsNames, err := rt.Bucket().ListDataStores()
		require.NoError(t, err)
		for _, ds := range dsNames {
			if ds.ScopeName() == scopeName && ds.CollectionName() == collectionName {
				t.Fatalf("Collection should have been deleted")
			}
		}
	})

	// DELETE bucket
	t.Run("DeleteBucket", func(t *testing.T) {
		newBucketName := "deleteme"
		_, err := rosmar.OpenBucketIn(rt.ServerContext().Config.Bootstrap.Server, newBucketName, rosmar.CreateOrOpen)
		require.NoError(t, err)

		// DELETE /_rosmar/bucket
		resp := rt.SendAdminRequest(http.MethodDelete, "/_rosmar/"+newBucketName, "")
		RequireStatus(t, resp, http.StatusOK)

		// Verify it's gone from rosmar.GetBucketNames
		bucketNames := rosmar.GetBucketNames()
		found := false
		for _, b := range bucketNames {
			if b == newBucketName {
				found = true
				break
			}
		}
		assert.False(t, found, "Bucket should have been deleted")
	})

	// DropDataStore behavior cases
	t.Run("DropDataStoreBehavior", func(t *testing.T) {
		resp := rt.SendAdminRequest(http.MethodDelete, "/_rosmar/nonexistent", "")
		RequireStatus(t, resp, http.StatusNotFound)

		// Scope drop fails with 404 since it doesn't contain collections
		resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/_rosmar/%s.nonexistent", bucketName), "")
		RequireStatus(t, resp, http.StatusNotFound)

		// Collection drop silently succeeds even if not found in Rosmar
		resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/_rosmar/%s._default.nonexistent", bucketName), "")
		RequireStatus(t, resp, http.StatusOK)
	})
}
