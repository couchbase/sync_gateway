// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package adminapitest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func TestCollectionsSyncImportFunctions(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)

	tb := base.GetTestBucket(t)
	defer tb.Close()

	rt, _ := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb,
		PersistentConfig: true,
	})
	defer rt.Close()

	importFilter1 := `function (doc) { console.log('importFilter1'); return true }`
	importFilter2 := `function (doc) { console.log('importFilter2'); return doc.type == 'onprem'}`
	syncFunction1 := `function (doc) { console.log('syncFunction1'); return true }`
	syncFunction2 := `function (doc) { console.log('syncFunction2'); return doc.type == 'onprem'}`

	dataStore1 := tb.GetNamedDataStore(0)
	dataStore1Name, ok := base.AsDataStoreName(dataStore1)
	require.True(t, ok)
	dataStore2 := tb.GetNamedDataStore(1)
	dataStore2Name, ok := base.AsDataStoreName(dataStore2)
	require.True(t, ok)
	keyspace1 := fmt.Sprintf("%s.%s.%s", "db", dataStore1Name.ScopeName(), dataStore1Name.CollectionName())
	keyspace2 := fmt.Sprintf("%s.%s.%s", "db", dataStore2Name.ScopeName(), dataStore2Name.CollectionName())

	bucketConfig := fmt.Sprintf(
		`{"bucket": "%s",
		  "scopes": {
			"%s": {
				"collections": {
					"%s": {
        					"import_filter": "%s",
        					"sync": "%s"
					},
					"%s": {
        					"import_filter": "%s",
        					"sync": "%s"
					}
				}
			}
		  },
		  "num_index_replicas": 0,
		  "enable_shared_bucket_access": true,
		  "use_views": false}`,
		tb.GetName(), dataStore1Name.ScopeName(), dataStore1Name.CollectionName(),
		importFilter1,
		syncFunction1, dataStore2Name.CollectionName(),
		importFilter2,
		syncFunction2,
	)
	resp := rt.SendAdminRequest(http.MethodPut, "/db/", bucketConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction2, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, resp.Body.String(), importFilter2)

	// Delete all custom functions
	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodDelete, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	// Make sure all are empty
	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	// Replace the original functions
	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/import_filter", keyspace1), importFilter1)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/import_filter", keyspace2), importFilter2)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/sync", keyspace1), syncFunction1)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/_config/sync", keyspace2), syncFunction2)
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, "", resp.Body.String())

	// Validate they are back to original
	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/sync", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, syncFunction2, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace1), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter1, resp.Body.String())

	resp = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_config/import_filter", keyspace2), "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.Equal(t, importFilter2, resp.Body.String())

}
