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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiCollectionImportFilter(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)
	base.RequireNumTestDataStores(t, 3)

	testBucket := base.GetPersistentTestBucket(t)
	defer testBucket.Close()

	scopesConfig := rest.GetCollectionsConfig(t, testBucket, 2)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)

	importFilter1 := `function (doc) { return doc.type == "mobile"}`
	importFilter2 := `function (doc) { return doc.type == "onprem"}`
	importFilter3 := `function (doc) { return doc.type == "private"}`

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = rest.CollectionConfig{ImportFilter: &importFilter1}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = rest.CollectionConfig{ImportFilter: &importFilter2}

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: testBucket.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Scopes: scopesConfig,
			},
		},
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	_ = rt.Bucket() // populates rest tester
	dataStore1, err := rt.TestBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	keyspace1 := "{{.keyspace1}}"
	dataStore2, err := rt.TestBucket.GetNamedDataStore(1)
	require.NoError(t, err)
	keyspace2 := "{{.keyspace2}}"
	//dataStore3, err := rt.TestBucket.GetNamedDataStore(2)
	//require.NoError(t, err)
	//keyspace3 := "{{.keyspace3}}"

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

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = rest.CollectionConfig{ImportFilter: &importFilter1}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = rest.CollectionConfig{ImportFilter: &importFilter2}
	scopesConfig[dataStoreNames[2].ScopeName()].Collections[dataStoreNames[2].CollectionName()] = rest.CollectionConfig{ImportFilter: &importFilter3}

	scopesConfigString, err := json.Marshal(scopesConfig)
	require.NoError(t, err)

	response = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		testBucket.GetName(), base.TestUseXattrs(), string(scopesConfigString)))
	rest.RequireStatus(t, response, http.StatusCreated)
	rt2 := rest.NewRestTesterMultipleCollections(t, rtConfig, 3)
	defer rt2.Close()

	_ = rt2.Bucket()
	dataStore1, err = rt2.TestBucket.GetNamedDataStore(0)
	require.NoError(t, err)
	dataStore2, err = rt2.TestBucket.GetNamedDataStore(1)
	require.NoError(t, err)
	dataStore3, err := rt2.TestBucket.GetNamedDataStore(2)
	require.NoError(t, err)

	// Write private doc
	dataStores = []base.DataStore{dataStore1, dataStore2, dataStore3}
	prvKey := "TestImportFilter3Valid"
	prvBody := make(map[string]interface{})
	prvBody["type"] = "private"
	prvBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		fmt.Println(dataStore.GetName())
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

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = rest.CollectionConfig{ImportFilter: &importFilter1}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = rest.CollectionConfig{ImportFilter: &importFilter2}
	scopesConfigString, err = json.Marshal(scopesConfig)
	require.NoError(t, err)

	// remove a collection
	response = rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		testBucket.GetName(), base.TestUseXattrs(), string(scopesConfigString)))

	rest.RequireStatus(t, response, http.StatusCreated)
	rt2 = rest.NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt2.Close()

	// Write private doc 2
	dataStores = []base.DataStore{dataStore1, dataStore2, dataStore3}
	prvKey = "TestImportFilter3Valid2"
	prvBody = make(map[string]interface{})
	prvBody["type"] = "private"
	prvBody["channels"] = "ABC"
	for _, dataStore := range dataStores {
		fmt.Println(dataStore.GetName())
		_, err := dataStore.Add(prvKey, 0, prvBody)
		require.NoError(t, err, "Error writing SDK doc")
	}
	// Write to removed collection and ensure new docs in the collection are not imported
	_, err = dataStore3.Add(prvKey, 0, mobileBody)
	require.NoError(t, err, "Error writing SDK doc")

	for _, keyspace := range []string{defaultKeyspace, keyspace1, keyspace2} {
		response = rt2.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, prvKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}
}
