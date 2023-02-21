// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package indextest

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func requireNoIndexes(t *testing.T, dataStore base.DataStore) {
	collection, err := base.AsCollection(dataStore)
	require.NoError(t, err)
	indexNames, err := collection.GetIndexes()
	require.NoError(t, err)
	require.Len(t, indexNames, 0)

}

func TestSyncGatewayStartupIndexes(t *testing.T) {
	bucket := base.GetTestBucket(t)
	defer bucket.Close()

	// Assert there are no indexes on the datastores, to test server startup
	dsNames, err := bucket.ListDataStores()
	require.NoError(t, err)
	for _, dsName := range dsNames {
		dataStore, err := bucket.NamedDataStore(dsName)
		require.NoError(t, err)
		if !base.TestsDisableGSI() {
			requireNoIndexes(t, dataStore)
		}
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: bucket.NoCloseClone(),
	})
	defer rt.Close()

	_ = rt.Bucket() // initialize RestTester

	if !base.TestsDisableGSI() {
		// use example indexes to make sure metadata and non metadata are created
		indexSyncDocs := "sg_syncDocs"
		indexAccess := "sg_access"
		if base.TestUseXattrs() {
			indexSyncDocs += "_x1"
			indexAccess += "_x1"
		}
		metadataCollection, err := base.AsCollection(bucket.DefaultDataStore())
		require.NoError(t, err)
		indexNames, err := metadataCollection.GetIndexes()
		require.NoError(t, err)

		require.Contains(t, indexNames, indexSyncDocs)

		if base.TestsUseNamedCollections() {
			require.NotContains(t, indexNames, indexAccess)
		}
	}

	// tests sg_users index
	t.Run("testUserQueries", func(t *testing.T) {
		users := []string{"alice", "bob"}

		for _, user := range users {
			response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+user, rest.GetUserPayload(t, user, rest.RestTesterDefaultUserPassword, "", rt.GetSingleTestDatabaseCollection(), []string{"ChannelA"}, nil))
			rest.RequireStatus(t, response, http.StatusCreated)
		}
		response := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_user/", "")
		rest.RequireStatus(t, response, http.StatusOK)

		var responseUsers []string
		err = json.Unmarshal(response.Body.Bytes(), &responseUsers)
		require.NoError(t, err)
		require.Equal(t, users, responseUsers)
	})

	// tests sg_roles index
	t.Run("testRoleQueries", func(t *testing.T) {
		roles := []string{"roleA", "roleB"}

		for _, role := range roles {
			response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+role, rest.GetRolePayload(t, role, rest.RestTesterDefaultUserPassword, rt.GetSingleTestDatabaseCollection(), []string{"ChannelA"}))
			rest.RequireStatus(t, response, http.StatusCreated)
		}
		response := rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_role/", "")
		rest.RequireStatus(t, response, http.StatusOK)

		var responseRoles []string
		err = json.Unmarshal(response.Body.Bytes(), &responseRoles)
		require.NoError(t, err)
		require.Equal(t, roles, responseRoles)
	})

}
