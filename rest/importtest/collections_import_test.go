package importtest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMultiCollectionImportFilter(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	importFilter1 := `function (doc) { return doc.type == "mobile"}`
	importFilter2 := `function (doc) { return doc.type == "onprem"}`

	testBucket := base.GetTestBucket(t)
	numCollections := 2
	scopesConfig := rest.GetCollectionsConfig(t, testBucket, numCollections)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)

	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = rest.CollectionConfig{
		ImportFilter: &importFilter1,
	}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = rest.CollectionConfig{
		ImportFilter: &importFilter2,
	}

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: testBucket,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Scopes: scopesConfig,
			},
		},
	}

	rt, keyspaces := rest.NewRestTesterMultipleCollections(t, rtConfig, numCollections)
	defer rt.Close()

	dataStore1 := rt.TestBucket.GetNamedDataStore(1)
	keyspace1 := keyspaces[0]
	dataStore2 := rt.TestBucket.GetNamedDataStore(2)
	keyspace2 := keyspaces[1]

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
	response := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace1, mobileKey), "")
	assert.Equal(t, 200, response.Code)
	assertDocProperty(t, response, "type", "mobile")

	response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace2, onPremKey), "")
	assert.Equal(t, 200, response.Code)
	assertDocProperty(t, response, "type", "onprem")

	// Make sure mobile doc doesn't end up in other collection
	for _, keyspace := range []string{defaultKeyspace, keyspace2} {
		response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, nonMobileKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}

	// Make sure on prem doc doesn't end up in other collection
	for _, keyspace := range []string{defaultKeyspace, keyspace1} {
		response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", keyspace, nonMobileKey), "")
		assert.Equal(t, 404, response.Code)
		if keyspace == defaultKeyspace {
			assertDocProperty(t, response, "reason", keyspaceNotFound)
		} else {
			assertDocProperty(t, response, "reason", "Not imported")

		}
	}

	// PUT to existing document that hasn't been imported.
	for _, keyspace := range keyspaces {
		sgWriteBody := `{"type":"whatever I want - I'm writing through SG",
	                 "channels": "ABC"}`
		response = rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", keyspace, nonMobileKey), sgWriteBody)
		assert.Equal(t, 201, response.Code)
		assertDocProperty(t, response, "id", "TestImportFilterInvalid")
		assertDocProperty(t, response, "rev", "1-25c26cdf9d7771e07f00be1d13f7fb7c")
	}
}
