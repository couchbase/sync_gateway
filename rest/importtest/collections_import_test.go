// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importtest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiCollectionImportFilter(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)
	base.RequireNumTestDataStores(t, 3)

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	scopesConfig := rest.GetCollectionsConfig(t, testBucket, 2)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)

	importFilter1 := `function (doc) { return doc.type == "mobile"}`
	importFilter2 := `function (doc) { return doc.type == "onprem"}`
	importFilter3 := `function (doc) { return doc.type == "private"}`

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = &rest.CollectionConfig{ImportFilter: &importFilter1}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = &rest.CollectionConfig{ImportFilter: &importFilter2}

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: testBucket.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Scopes: scopesConfig,
			},
		},
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 3)
	defer rt.Close()

	_ = rt.Bucket() // populates rest tester
	dataStore1, err := testBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	keyspace1 := "{{.keyspace1}}"
	dataStore2, err := testBucket.GetNamedDataStore(1)
	require.NoError(t, err)
	keyspace2 := "{{.keyspace2}}"

	defaultKeyspace := "db._default._default"
	keyspaceNotFound := fmt.Sprintf("keyspace %s not found", defaultKeyspace)
	dataStores := []base.DataStore{dataStore1, dataStore2}

	// Create three docs in each collection:
	//
	// * matching importFilter1
	// * not matching any import filter
	// * matching importFilter2
	mobileKey := "TestImportFilter1Valid"
	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		_, err := dataStore.Add(mobileKey, 0, mobileBody)
		require.NoError(t, err, "Error writing SDK doc")
	}
	nonMobileKey := "TestImportFilterInvalid"
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["type"] = "non-mobile"
	nonMobileBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		_, err := dataStore.Add(nonMobileKey, 0, nonMobileBody)
		require.NoError(t, err, "Error writing SDK doc")
	}
	onPremKey := "TestImportFilter2Valid"
	onPremBody := make(map[string]interface{})
	onPremBody["type"] = "onprem"
	onPremBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		_, err := dataStore.Add(onPremKey, 0, onPremBody)
		require.NoError(t, err, "Error writing SDK doc")
	}

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand import.
	response := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace1}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	assertDocProperty(t, response, "type", "mobile")

	response = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace2}}/"+onPremKey, "")
	assert.Equal(t, 200, response.Code)
	assertDocProperty(t, response, "type", "onprem")

	// Make sure mobile doc doesn't end up in other collection
	for _, keyspace := range []string{defaultKeyspace, keyspace2} {
		response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, mobileKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}

	// Make sure on prem doc doesn't end up in other collection
	for _, keyspace := range []string{defaultKeyspace, keyspace1} {
		response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, onPremKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}

	// Make sure non-mobile doc is never imported
	for _, keyspace := range []string{defaultKeyspace, keyspace1, keyspace2} {
		response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, nonMobileKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}

	// PUT to existing document that hasn't been imported.
	for _, keyspace := range []string{keyspace1, keyspace2} {
		sgWriteBody := `{"type":"whatever I want - I'm writing through SG",
	                 "channels": "ABC"}`
		response = rt.SendAdminRequest(http.MethodPut, "/"+keyspace+"/"+nonMobileKey, sgWriteBody)
		assert.Equal(t, 201, response.Code)
		assertDocProperty(t, response, "id", "TestImportFilterInvalid")
		assertDocProperty(t, response, "rev", "1-25c26cdf9d7771e07f00be1d13f7fb7c")
	}
	// Add a collection
	scopesConfig = rest.GetCollectionsConfig(t, testBucket, 3)
	dataStoreNames = rest.GetDataStoreNamesFromScopesConfig(scopesConfig)

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = &rest.CollectionConfig{ImportFilter: &importFilter1}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = &rest.CollectionConfig{ImportFilter: &importFilter2}
	scopesConfig[dataStoreNames[2].ScopeName()].Collections[dataStoreNames[2].CollectionName()] = &rest.CollectionConfig{ImportFilter: &importFilter3}

	scopesConfigString, err := json.Marshal(scopesConfig)
	require.NoError(t, err)

	response = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		testBucket.GetName(), base.TestUseXattrs(), string(scopesConfigString)))
	rest.RequireStatus(t, response, http.StatusCreated)

	dataStore3, err := testBucket.GetNamedDataStore(2)
	require.NoError(t, err)

	// Write private doc
	dataStores = []base.DataStore{dataStore1, dataStore2, dataStore3}
	prvKey := "TestImportFilter3Valid"
	prvBody := make(map[string]interface{})
	prvBody["type"] = "private"
	prvBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		_, err := dataStore.Add(prvKey, 0, prvBody)
		require.NoError(t, err, "Error writing SDK doc")
	}

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand import.
	response = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace3}}/"+prvKey, "")
	assert.Equal(t, 200, response.Code)
	assertDocProperty(t, response, "type", "private")

	// Make sure private doc doesn't end up in other collections
	for _, keyspace := range []string{defaultKeyspace, keyspace1, keyspace2} {
		response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, prvKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}

	// remove a collection from the sgw db
	scopesConfig = rest.GetCollectionsConfig(t, testBucket, 2)
	dataStoreNames = rest.GetDataStoreNamesFromScopesConfig(scopesConfig)

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = &rest.CollectionConfig{ImportFilter: &importFilter1}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = &rest.CollectionConfig{ImportFilter: &importFilter2}
	scopesConfigString, err = json.Marshal(scopesConfig)
	require.NoError(t, err)

	response = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		testBucket.GetName(), base.TestUseXattrs(), string(scopesConfigString)))
	rest.RequireStatus(t, response, http.StatusCreated)

	// Write private doc 2
	dataStores = []base.DataStore{dataStore1, dataStore2, dataStore3}
	prvKey = "TestImportFilter3Valid2"
	prvBody = make(map[string]interface{})
	prvBody["type"] = "private"
	prvBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		_, err := dataStore.Add(prvKey, 0, prvBody)
		require.NoError(t, err, "Error writing SDK doc")
	}
	// Write to removed collection and ensure new docs in the collection are not imported
	_, err = dataStore3.Add(prvKey, 0, mobileBody)
	require.NoError(t, err, "Error writing SDK doc")

	for _, keyspace := range []string{defaultKeyspace, keyspace1, keyspace2} {
		response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, prvKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}
}

const collectionsDbConfig = `{
		"bucket": "%s",
		"num_index_replicas": 0,
		"enable_shared_bucket_access": true,
		"scopes": %s,
		"import_docs": true
	}`

func TestMultiCollectionImportDynamicAddCollection(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)
	base.RequireNumTestDataStores(t, 2)

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: testBucket.NoCloseClone(),
		PersistentConfig: true,
	}

	rt := rest.NewRestTester(t, rtConfig)
	defer rt.Close()

	_ = rt.Bucket() // populates rest tester

	dataStore1, err := testBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore2, err := testBucket.GetNamedDataStore(1)
	require.NoError(t, err)

	// write datastore 2
	dataStores := []base.DataStore{dataStore2, dataStore1}

	// write docs to both datastores
	docBody := make(map[string]interface{})
	docBody["foo"] = "bar"
	docCount := 10
	for i, dataStore := range dataStores {
		for j := 0; j < docCount; j++ {
			_, err := dataStore.Add(fmt.Sprintf("datastore%d_%d", i, j), 0, docBody)
			require.NoError(t, err, "Error writing SDK doc")
		}
	}
	scopesConfig := rest.GetCollectionsConfig(t, testBucket, 1)
	scopesConfigJSON, err := json.Marshal(scopesConfig)
	require.NoError(t, err)

	dbName := "db"

	response := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(scopesConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	require.Equal(t, 1, len(rt.GetDbCollections()))
	require.Equal(t, strings.ReplaceAll(dataStore1.GetName(), testBucket.GetName(), dbName), rt.GetSingleKeyspace())

	// Wait for auto-import to work
	_, err = rt.WaitForChanges(docCount, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	for i, dataStore := range dataStores {
		for j := 0; j < docCount; j++ {
			docName := fmt.Sprintf("datastore%d_%d", i, j)
			if dataStore == dataStore1 {
				requireSyncData(rt, dataStore, docName, true)
			} else {
				requireSyncData(rt, dataStore, docName, false)
			}
		}
	}

	require.Equal(t, int64(docCount), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	// update with 2 scopes
	twoScopesConfig := rest.GetCollectionsConfig(t, testBucket, 2)
	twoScopesConfigJSON, err := json.Marshal(twoScopesConfig)
	require.NoError(t, err)

	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(twoScopesConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	require.Equal(t, 2, len(rt.GetDbCollections()))

	// Wait for auto-import to work
	_, err = rt.WaitForChanges(docCount, "/{{.keyspace2}}/_changes", "", true)
	require.NoError(t, err)

	for i, dataStore := range dataStores {
		for j := 0; j < docCount; j++ {
			docName := fmt.Sprintf("datastore%d_%d", i, j)
			requireSyncData(rt, dataStore, docName, true)
		}
	}

	require.Equal(t, int64(docCount), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(twoScopesConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

}

func TestMultiCollectionImportRemoveCollection(t *testing.T) {

	defer db.SuspendSequenceBatching()()
	base.SkipImportTestsIfNotEnabled(t)
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)

	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: testBucket.NoCloseClone(),
		PersistentConfig: true,
	}

	rt := rest.NewRestTester(t, rtConfig)
	defer rt.Close()

	dataStore1, err := testBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore2, err := testBucket.GetNamedDataStore(1)
	require.NoError(t, err)

	docCount := 10
	docBody := make(map[string]interface{})
	docBody["foo"] = "bar"
	for i, dataStore := range []base.DataStore{dataStore1, dataStore2} {
		for j := 0; j < docCount; j++ {
			_, err := dataStore.Add(fmt.Sprintf("datastore%d_%d", i, j), 0, docBody)
			require.NoError(t, err, "Error writing SDK doc")
		}
	}

	// update with 2 scopes
	twoScopesConfig := rest.GetCollectionsConfig(t, testBucket, 2)
	twoScopesConfigJSON, err := json.Marshal(twoScopesConfig)
	require.NoError(t, err)

	dbName := "db"
	response := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(twoScopesConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	require.Equal(t, 2, len(rt.GetDbCollections()))

	// Wait for auto-import to work
	_, err = rt.WaitForChanges(docCount, "/{{.keyspace1}}/_changes", "", true)
	require.NoError(t, err)
	_, err = rt.WaitForChanges(docCount, "/{{.keyspace2}}/_changes", "", true)
	require.NoError(t, err)
	require.Equal(t, int64(docCount*2), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	oneScopeConfig := rest.GetCollectionsConfig(t, testBucket, 1)
	oneScopeConfigJSON, err := json.Marshal(oneScopeConfig)
	require.NoError(t, err)

	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(oneScopeConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	// write datastore2 docs first, so we know they are ignored by import feed
	for i, dataStore := range []base.DataStore{dataStore2, dataStore1} {
		for j := docCount; j < (docCount * 2); j++ {
			_, err := dataStore.Add(fmt.Sprintf("datastore%d_%d", i, j), 0, docBody)
			require.NoError(t, err, "Error writing SDK doc")
		}
	}

	_, err = rt.WaitForChanges(docCount*2, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)
	// there should be 10 documents datastore1_{10..19}
	require.Equal(t, int64(docCount), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

}

func requireSyncData(rt *rest.RestTester, dataStore base.DataStore, docName string, hasSyncData bool) {
	var rawDoc, rawXattr, rawUserXattr []byte
	_, err := dataStore.GetWithXattr(rt.Context(), docName, base.SyncXattrName, rt.GetDatabase().Options.UserXattrKey, &rawDoc, &rawXattr, &rawUserXattr)
	require.NoError(rt.TB, err)
	if hasSyncData {
		require.NotEqual(rt.TB, "", string(rawXattr), "Expected data for %s %s", dataStore.GetName(), docName)
	} else {
		require.Equal(rt.TB, "", string(rawXattr), "Expected no data for %s %s", dataStore.GetName(), docName)
	}
}
