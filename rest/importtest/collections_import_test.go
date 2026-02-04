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
	base.LongRunningTest(t)

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
	mobileBody := make(map[string]any)
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		_, err := dataStore.Add(mobileKey, 0, mobileBody)
		require.NoError(t, err, "Error writing SDK doc")
	}
	nonMobileKey := "TestImportFilterInvalid"
	nonMobileBody := make(map[string]any)
	nonMobileBody["type"] = "non-mobile"
	nonMobileBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		_, err := dataStore.Add(nonMobileKey, 0, nonMobileBody)
		require.NoError(t, err, "Error writing SDK doc")
	}
	onPremKey := "TestImportFilter2Valid"
	onPremBody := make(map[string]any)
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
	prvBody := make(map[string]any)
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
	prvBody = make(map[string]any)
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

const collectionsDbConfigRevsLimit = `{
		"bucket": "%s",
		"num_index_replicas": 0,
		"enable_shared_bucket_access": true,
		"scopes": %s,
		"import_docs": true,
		"revs_limit": 21
	}`

const collectionsDbConfigUpsertRevsLimit = `{
		"revs_limit": 22
	}`
const collectionsDbConfigUpsertScopes = `{
		"scopes": %s
	}`

func TestMultiCollectionImportDynamicAddCollection(t *testing.T) {
	base.LongRunningTest(t)

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
	docBody := make(map[string]any)
	docBody["foo"] = "bar"
	docCount := 10
	for _, dataStore := range dataStores {
		for j := range docCount {
			_, err := dataStore.Add(fmt.Sprintf("datastore%d", j), 0, docBody)
			require.NoError(t, err, "Error writing SDK doc")
		}
	}
	scopesConfig := rest.GetCollectionsConfig(t, testBucket, 1)
	scopesConfigJSON, err := json.Marshal(scopesConfig)
	require.NoError(t, err)

	dbName := "db"

	response := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(scopesConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	require.Len(t, rt.GetDbCollections(), 1)
	require.Equal(t, strings.ReplaceAll(dataStore1.GetName(), testBucket.GetName(), dbName), rt.GetSingleKeyspace())

	// Wait for auto-import to work
	rt.WaitForChanges(docCount, "/{{.keyspace}}/_changes", "", true)

	for _, dataStore := range dataStores {
		for j := range docCount {
			docName := fmt.Sprintf("datastore%d", j)
			if dataStore == dataStore1 {
				requireSyncData(rt, dataStore, docName, true)
			} else {
				requireSyncData(rt, dataStore, docName, false)
			}
		}
	}

	base.RequireWaitForStat(t, rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value, int64(docCount))

	// update config to add second collection
	twoCollectionConfig := rest.GetCollectionsConfig(t, testBucket, 2)
	twoCollectionConfigJSON, err := json.Marshal(twoCollectionConfig)
	require.NoError(t, err)

	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(twoCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	require.Len(t, rt.GetDbCollections(), 2)

	// Wait for auto-import to work
	rt.WaitForChanges(docCount, "/{{.keyspace2}}/_changes", "", true)

	for _, dataStore := range dataStores {
		for j := range docCount {
			docName := fmt.Sprintf("datastore%d", j)
			requireSyncData(rt, dataStore, docName, true)
		}
	}

	base.RequireWaitForStat(t, rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value, int64(docCount))

	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(twoCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

}

func TestMultiCollectionImportRemoveCollection(t *testing.T) {
	base.LongRunningTest(t)

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
	docBody := make(map[string]any)
	docBody["foo"] = "bar"
	for i, dataStore := range []base.DataStore{dataStore1, dataStore2} {
		for j := range docCount {
			_, err := dataStore.Add(fmt.Sprintf("datastore%d_%d", i, j), 0, docBody)
			require.NoError(t, err, "Error writing SDK doc")
		}
	}

	// update with 2 collections
	twoCollectionConfig := rest.GetCollectionsConfig(t, testBucket, 2)
	twoCollectionConfigJSON, err := json.Marshal(twoCollectionConfig)
	require.NoError(t, err)

	dbName := "db"
	response := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(twoCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	require.Len(t, rt.GetDbCollections(), 2)

	// Wait for auto-import to work
	rt.WaitForChanges(docCount, "/{{.keyspace1}}/_changes", "", true)
	rt.WaitForChanges(docCount, "/{{.keyspace2}}/_changes", "", true)
	base.RequireWaitForStat(t, rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value, int64(docCount*2))

	oneScopeConfig := rest.GetCollectionsConfig(t, testBucket, 1)
	oneScopeConfigJSON, err := json.Marshal(oneScopeConfig)
	require.NoError(t, err)

	// Get the persisted config to check import version
	initialImportVersion := GetImportVersion(t, rt, dbName)

	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(oneScopeConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	// write datastore2 docs first, so we know they are ignored by import feed
	for i, dataStore := range []base.DataStore{dataStore2, dataStore1} {
		for j := docCount; j < (docCount * 2); j++ {
			_, err := dataStore.Add(fmt.Sprintf("datastore%d_%d", i, j), 0, docBody)
			require.NoError(t, err, "Error writing SDK doc")
		}
	}

	rt.WaitForChanges(docCount*2, "/{{.keyspace}}/_changes", "", true)
	// there should be 10 documents datastore1_{10..19}
	require.Equal(t, int64(docCount), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	// Get the import version from the persisted config to ensure it didn't change
	updatedImportVersion := GetImportVersion(t, rt, dbName)
	require.Equal(t, initialImportVersion, updatedImportVersion)

}

// TestImportVersionWriteVariations - ensure import version is updated for all different APIs that can modify the collection set
func TestImportVersionWriteVariations(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyDCP)
	base.SkipImportTestsIfNotEnabled(t)
	numCollections := 3
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

	// Set up collection payloads for 1, 2, 3 collections
	oneCollectionConfig := rest.GetCollectionsConfig(t, testBucket, 1)
	oneCollectionConfigJSON, err := json.Marshal(oneCollectionConfig)
	require.NoError(t, err)
	twoCollectionConfig := rest.GetCollectionsConfig(t, testBucket, 2)
	twoCollectionConfigJSON, err := json.Marshal(twoCollectionConfig)
	require.NoError(t, err)
	threeCollectionConfig := rest.GetCollectionsConfig(t, testBucket, 3)
	threeCollectionConfigJSON, err := json.Marshal(threeCollectionConfig)
	require.NoError(t, err)

	// Insert with PUT and single collection
	dbName := "db"
	response := rt.SendAdminRequest(http.MethodPut, "/"+dbName+"/", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(oneCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)

	importVersion := GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(0), importVersion)

	// Update with PUT, change collection set, version should change
	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfig, testBucket.GetName(), string(twoCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)
	importVersion = GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(1), importVersion)

	// Update with PUT, do not change collection set, version should not change
	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfigRevsLimit, testBucket.GetName(), string(twoCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)
	importVersion = GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(1), importVersion)

	// Upsert with POST, do not include or change collection set, version should not change
	response = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", collectionsDbConfigUpsertRevsLimit)
	rest.RequireStatus(t, response, http.StatusCreated)
	importVersion = GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(1), importVersion)

	// Upsert with POST, include scopes but do not change collection set, version should not change
	response = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfigUpsertScopes, string(twoCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)
	importVersion = GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(1), importVersion)

	// Update with POST, add collection, version should change
	response = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfigUpsertScopes, string(threeCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)
	importVersion = GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(2), importVersion)

	// Update with PUT, remove collection, version should not change
	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfigRevsLimit, testBucket.GetName(), string(twoCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)
	importVersion = GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(2), importVersion)

	// Update with POST, remove collection, version should not change
	response = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", fmt.Sprintf(collectionsDbConfigUpsertScopes, string(oneCollectionConfigJSON)))
	rest.RequireStatus(t, response, http.StatusCreated)
	importVersion = GetImportVersion(t, rt, dbName)
	require.Equal(t, uint64(2), importVersion)

}

func GetImportVersion(t *testing.T, rt *rest.RestTester, dbName string) uint64 {
	var databaseConfig rest.DatabaseConfig
	_, err := rt.ServerContext().BootstrapContext.GetConfig(rt.Context(), rt.CustomTestBucket.GetName(), rt.ServerContext().Config.Bootstrap.ConfigGroupID, dbName, &databaseConfig)
	require.NoError(t, err)
	return databaseConfig.ImportVersion
}

func requireSyncData(rt *rest.RestTester, dataStore base.DataStore, docName string, hasSyncData bool) {
	xattrs, _, err := dataStore.GetXattrs(rt.Context(), docName, []string{base.SyncXattrName})
	if hasSyncData {
		require.NoError(rt.TB(), err)
		require.Contains(rt.TB(), xattrs, base.SyncXattrName)
		require.NotEqual(rt.TB(), "", string(xattrs[base.SyncXattrName]), "Expected data for %s %s", dataStore.GetName(), docName)
	} else {
		require.Error(rt.TB(), err)
		require.True(rt.TB(), base.IsXattrNotFoundError(err), "Expected xattr missing error but got %+v", err)
		require.NotContains(rt.TB(), xattrs, base.SyncXattrName)
	}
}
