/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package importtest

import (
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// gocb V2 accepts expiry as a duration and converts to a uint32 epoch time, then does the reverse on retrieval.
// Sync Gateway's bucket interface uses uint32 expiry. The net result is that expiry values written and then read via SG's
// bucket API go through a transformation based on time.Now (or time.Until) that can result in inexact matches.
// assertExpiry validates that the two expiry values are within a 10 second window
func assertExpiry(t testing.TB, expected uint32, actual uint32) {
	assert.True(t, base.DiffUint32(expected, actual) < 10, fmt.Sprintf("Unexpected difference between expected: %v actual %v", expected, actual))
}

// Test import of an SDK delete.
func TestXattrImportOldDoc(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) {
			if (oldDoc == null) {
				channel("oldDocNil")
			}
			if (doc._deleted) {
				channel("docDeleted")
			}
		}`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t,
		&rtConfig)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Test oldDoc behaviour during SDK insert
	key := "TestImportDelete"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestImportDelete"
	docBody["channels"] = "ABC"

	_, err := dataStore.Add(key, 0, docBody)
	assert.NoError(t, err, "Unable to insert doc TestImportDelete")

	// Attempt to get the document via Sync Gateway, to trigger import.  On import of a create, oldDoc should be nil.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/TestImportDelete?redact=false", "")
	assert.Equal(t, 200, response.Code)
	var rawInsertResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")
	assert.True(t, rawInsertResponse.Sync.Channels != nil, "Expected channels not returned for SDK insert")
	log.Printf("insert channels: %+v", rawInsertResponse.Sync.Channels)
	assert.True(t, rest.HasActiveChannel(rawInsertResponse.Sync.Channels, "oldDocNil"), "oldDoc was not nil during import of SDK insert")

	// 2. Test oldDoc behaviour during SDK update

	updatedBody := make(map[string]interface{})
	updatedBody["test"] = "TestImportDelete"
	updatedBody["channels"] = "HBO"

	err = rt.GetSingleDataStore().Set(key, 0, nil, updatedBody)
	assert.NoError(t, err, "Unable to update doc TestImportDelete")

	// Attempt to get the document via Sync Gateway, to trigger import.  On import of a create, oldDoc should be nil.
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/TestImportDelete?redact=false", "")
	assert.Equal(t, 200, response.Code)
	var rawUpdateResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawUpdateResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")

	// If delta sync is enabled, old doc may be available based on the backup used for delta generation, if it hasn't already
	// been converted to a delta
	if !rt.GetDatabase().DeltaSyncEnabled() {
		assert.True(t, rawUpdateResponse.Sync.Channels != nil, "Expected channels not returned for SDK update")
		log.Printf("update channels: %+v", rawUpdateResponse.Sync.Channels)
		assert.True(t, rest.HasActiveChannel(rawUpdateResponse.Sync.Channels, "oldDocNil"), "oldDoc was not nil during import of SDK update")
	}

	// 3. Test oldDoc behaviour during SDK delete
	err = dataStore.Delete(key)
	assert.NoError(t, err, "Unable to delete doc TestImportDelete")

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/TestImportDelete?redact=false", "")
	assert.Equal(t, 200, response.Code)
	var rawDeleteResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawDeleteResponse)
	log.Printf("Post-delete: %s", response.Body.Bytes())
	assert.NoError(t, err, "Unable to unmarshal raw response")
	assert.True(t, rawUpdateResponse.Sync.Channels != nil, "Expected channels not returned for SDK update")
	log.Printf("update channels: %+v", rawDeleteResponse.Sync.Channels)
	if !rt.GetDatabase().DeltaSyncEnabled() {
		assert.True(t, rest.HasActiveChannel(rawDeleteResponse.Sync.Channels, "oldDocNil"), "oldDoc was not nil during import of SDK delete")
	}
	assert.True(t, rest.HasActiveChannel(rawDeleteResponse.Sync.Channels, "docDeleted"), "doc did not set _deleted:true for SDK delete")
}

// Test import ancestor handling
func TestXattrImportOldDocRevHistory(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) {
			if (oldDoc == null) {
				channel("oldDocNil")
			}
		}`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create revision with history
	docID := t.Name()
	version := rt.PutDoc(docID, `{"val":-1}`)
	revID := version.RevID
	collection := rt.GetSingleTestDatabaseCollectionWithUser()

	ctx := rt.Context()
	for i := 0; i < 10; i++ {
		version = rt.UpdateDoc(docID, version, fmt.Sprintf(`{"val":%d}`, i))
		// Purge old revision JSON to simulate expiry, and to verify import doesn't attempt multiple retrievals
		purgeErr := collection.PurgeOldRevisionJSON(ctx, docID, revID)
		require.NoError(t, purgeErr)
		revID = version.RevID
	}

	// 2. Modify doc via SDK
	updatedBody := make(map[string]interface{})
	updatedBody["test"] = "TestAncestorImport"
	err := dataStore.Set(docID, 0, nil, updatedBody)
	assert.NoError(t, err)

	// Attempt to get the document via Sync Gateway, to trigger import
	response := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", docID), "")
	assert.Equal(t, 200, response.Code)
	var rawResponse rest.RawResponse
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &rawResponse))
	log.Printf("raw response: %s", response.Body.Bytes())
	assert.Equal(t, 1, len(rawResponse.Sync.Channels))
	_, ok := rawResponse.Sync.Channels["oldDocNil"]
	assert.True(t, ok)
}

// Validate tombstone w/ xattrs
func TestXattrSGTombstone(t *testing.T) {
	rtConfig := rest.RestTesterConfig{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create doc through SG
	key := "TestXattrSGTombstone"
	docBody := make(map[string]interface{})
	docBody["test"] = key
	docBody["channels"] = "ABC"

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, `{"channels":"ABC"}`)
	assert.Equal(t, 201, response.Code)
	log.Printf("insert response: %s", response.Body.Bytes())
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	// 2. Delete the doc through SG
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, revId), "")
	assert.Equal(t, 200, response.Code)
	log.Printf("delete response: %s", response.Body.Bytes())
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	require.NotEqual(t, "", body["rev"].(string))

	// 3. Attempt to retrieve the doc through the SDK
	deletedValue := make(map[string]interface{})
	_, err := dataStore.Get(key, deletedValue)
	assert.True(t, err != nil, "Expected key not found error trying to retrieve document")

}

// Test cas failure during WriteUpdate, triggering import of SDK write.
func TestXattrImportOnCasFailure(t *testing.T) {

	// TODO: Disabled, as test depends on artificial latency in PutDoc to
	// reliably hit the CAS failure on the SG write.
	// Scenario fully covered by functional test.
	t.Skip("WARNING: TEST DISABLED")

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	// 1. SG Write
	key := "TestCasFailureImport"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestCasFailureImport"
	docBody["SG_write_count"] = "1"

	response := rt.SendAdminRequest("PUT", "/db/TestCasFailureImport", `{"test":"TestCasFailureImport", "write_type":"SG_1"}`)
	assert.Equal(t, 201, response.Code)
	log.Printf("insert response: %s", response.Body.Bytes())
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "1-111c27be37c17f18ae8fe9faa3bb4e0e", body["rev"])
	revId := body["rev"].(string)

	// Attempt a second SG write, to be interrupted by an SDK update.  Should return a conflict
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s?rev=%s", key, revId), `{"write_type":"SG_2"}`)
		assert.Equal(t, 409, response.Code)
		log.Printf("SG CAS failure write response: %s", response.Body.Bytes())
		wg.Done()
	}()

	// Concurrent SDK writes for 10 seconds, one per second
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		sdkBody := make(map[string]interface{})
		sdkBody["test"] = "TestCasFailureImport"
		sdkBody["SDK_write_count"] = i
		err := dataStore.Set(key, 0, nil, sdkBody)
		assert.NoError(t, err, "Unexpected error doing SDK write")
	}

	// wait for SG write to happen
	wg.Wait()

	// Get to see where we ended up
	response = rt.SendAdminRequest("GET", "/db/TestCasFailureImport", "")
	assert.Equal(t, 200, response.Code)
	log.Printf("Final get: %s", response.Body.Bytes())

	// Get raw to see where the rev tree ended up
	response = rt.SendAdminRequest("GET", "/db/_raw/TestCasFailureImport", "")
	assert.Equal(t, 200, response.Code)
	log.Printf("Final get raw: %s", response.Body.Bytes())

}

// Attempt to delete then recreate a document through SG
func TestXattrResurrectViaSG(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	rt.Bucket()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create and import doc
	key := "TestResurrectViaSG"
	docBody := make(map[string]interface{})
	docBody["test"] = key
	docBody["channels"] = "ABC"

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, `{"channels":"ABC"}`)
	assert.Equal(t, 201, response.Code)
	log.Printf("insert response: %s", response.Body.Bytes())
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	// 2. Delete the doc through SG
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, revId), "")
	assert.Equal(t, 200, response.Code)
	log.Printf("delete response: %s", response.Body.Bytes())
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId = body["rev"].(string)

	// 3. Recreate the doc through the SG (with different data)
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, revId), `{"channels":"ABC"}`)
	assert.Equal(t, 201, response.Code)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	assert.Equal(t, "3-70c113f08c6622cd87af68f4f60d12e3", body["rev"])

}

// Attempt to delete then recreate a document through the SDK
func TestXattrResurrectViaSDK(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	// 1. Create and import doc
	key := "TestResurrectViaSDK"
	docBody := make(map[string]interface{})
	docBody["test"] = key
	docBody["channels"] = "ABC"

	dataStore := rt.GetSingleDataStore()

	_, err := dataStore.Add(key, 0, docBody)
	assert.NoError(t, err, "Unable to insert doc TestResurrectViaSDK")

	// Attempt to get the document via Sync Gateway, to trigger import.  On import of a create, oldDoc should be nil.
	rawPath := fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", key)
	response := rt.SendAdminRequest("GET", rawPath, "")
	assert.Equal(t, 200, response.Code)
	var rawInsertResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")

	// 2. Delete the doc through the SDK
	err = dataStore.Delete(key)
	assert.NoError(t, err, "Unable to delete doc TestResurrectViaSDK")

	response = rt.SendAdminRequest("GET", rawPath, "")
	assert.Equal(t, 200, response.Code)
	var rawDeleteResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawDeleteResponse)
	log.Printf("Post-delete: %s", response.Body.Bytes())
	assert.NoError(t, err, "Unable to unmarshal raw response")

	// 3. Recreate the doc through the SDK (with different data)
	updatedBody := make(map[string]interface{})
	updatedBody["test"] = key
	updatedBody["channels"] = "HBO"

	err = dataStore.Set(key, 0, nil, updatedBody)
	assert.NoError(t, err, "Unable to update doc TestResurrectViaSDK")

	// Attempt to get the document via Sync Gateway, to trigger import.
	response = rt.SendAdminRequest("GET", rawPath, "")
	assert.Equal(t, 200, response.Code)
	var rawUpdateResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawUpdateResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")
	_, ok := rawUpdateResponse.Sync.Channels["HBO"]
	assert.True(t, ok, "Didn't find expected channel (HBO) on resurrected doc")

}

// Attempt to delete a document that's already been deleted via the SDK
func TestXattrDoubleDelete(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create and import doc
	key := "TestDoubleDelete"
	docBody := make(map[string]interface{})
	docBody["test"] = key
	docBody["channels"] = "ABC"

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, `{"channels":"ABC"}`)
	assert.Equal(t, 201, response.Code)
	log.Printf("insert response: %s", response.Body.Bytes())
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	// 2. Delete the doc through the SDK
	log.Printf("...............Delete through SDK.....................................")
	deleteErr := rt.GetSingleDataStore().Delete(key)
	assert.NoError(t, deleteErr, "Couldn't delete via SDK")

	log.Printf("...............Delete through SG.......................................")

	// 3. Delete the doc through SG.  Expect a conflict, as the import of the SDK delete will create a new
	//    tombstone revision
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, revId), "")
	assert.Equal(t, 409, response.Code)
	log.Printf("delete response: %s", response.Body.Bytes())
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	require.NotEqual(t, "", body["rev"].(string))

}

func TestViewQueryTombstoneRetrieval(t *testing.T) {
	if !base.TestsDisableGSI() {
		t.Skip("views tests are not applicable under GSI")
	}

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	// 1. Create and import docs
	key := "SG_delete"
	docBody := make(map[string]interface{})
	docBody["test"] = key
	docBody["channels"] = "ABC"

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, `{"channels":"ABC"}`)
	assert.Equal(t, 201, response.Code)
	log.Printf("insert response: %s", response.Body.Bytes())
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	sdk_key := "SDK_delete"
	docBody = make(map[string]interface{})
	docBody["test"] = sdk_key
	docBody["channels"] = "ABC"

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+sdk_key, `{"channels":"ABC"}`)
	assert.Equal(t, 201, response.Code)
	log.Printf("insert response: %s", response.Body.Bytes())
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])

	// 2. Delete SDK_delete through the SDK
	log.Printf("...............Delete through SDK.....................................")
	deleteErr := dataStore.Delete(sdk_key)
	assert.NoError(t, deleteErr, "Couldn't delete via SDK")

	// Trigger import via SG retrieval
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+sdk_key, "")
	assert.Equal(t, 404, response.Code) // expect 404 deleted

	// 3.  Delete SG_delete through SG.
	log.Printf("...............Delete through SG.......................................")
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, revId), "")
	assert.Equal(t, 200, response.Code)
	log.Printf("delete response: %s", response.Body.Bytes())
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	require.NotEqual(t, "", body["rev"].(string))

	log.Printf("TestXattrDeleteDCPMutation done")

	// Attempt to retrieve via view.  Above operations were all synchronous (on-demand import of SDK delete, SG delete), so
	// stale=false view results should be immediately updated.
	results, err := rt.GetDatabase().CollectionChannelViewForTest(t, rt.GetSingleTestDatabaseCollection(), "ABC", 0, 1000)
	require.NoError(t, err, "Error issuing channel view query")
	for _, entry := range results {
		log.Printf("Got view result: %v", entry)
	}
	require.Len(t, results, 2)
	assert.True(t, strings.HasPrefix(results[0].RevID, "2-"), "Unexpected revisions in view results post-delete")
	assert.True(t, strings.HasPrefix(results[1].RevID, "2-"), "Unexpected revisions in view results post-delete")
}

func TestXattrImportFilterOptIn(t *testing.T) {
	importFilter := `function (doc) { return doc.type == "mobile"}`
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:   false,
			ImportFilter: &importFilter,
		}},
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig) // use default collection since we are using default sync function
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create two docs via the SDK, one matching filter
	mobileKey := "TestImportFilterValid"
	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"
	_, err := dataStore.Add(mobileKey, 0, mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	nonMobileKey := "TestImportFilterInvalid"
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["type"] = "non-mobile"
	nonMobileBody["channels"] = "ABC"
	_, err = dataStore.Add(nonMobileKey, 0, nonMobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand import.
	response := rt.SendAdminRequest("GET", "/db/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	assertDocProperty(t, response, "type", "mobile")

	response = rt.SendAdminRequest("GET", "/db/"+nonMobileKey, "")
	assert.Equal(t, 404, response.Code)
	assertDocProperty(t, response, "reason", "Not imported")

	// PUT to existing document that hasn't been imported.
	sgWriteBody := `{"type":"whatever I want - I'm writing through SG",
	                 "channels": "ABC"}`
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", nonMobileKey), sgWriteBody)
	assert.Equal(t, 201, response.Code)
	assertDocProperty(t, response, "id", "TestImportFilterInvalid")
	assertDocProperty(t, response, "rev", "1-25c26cdf9d7771e07f00be1d13f7fb7c")
}

func TestImportFilterLogging(t *testing.T) {
	const errorMessage = `ImportFilterError`
	importFilter := `function (doc) { console.error("` + errorMessage + `"); return doc.type == "mobile"; }`
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			ImportFilter: &importFilter,
			AutoImport:   false,
		}},
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig) // use default collection since we are using default sync function
	defer rt.Close()

	// Add document to bucket
	key := "ValidImport"
	body := make(map[string]interface{})
	body["type"] = "mobile"
	body["channels"] = "A"
	ok, err := rt.GetSingleDataStore().Add(key, 0, body)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Get number of errors before
	numErrors, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	// Attempt to get doc will trigger import
	base.AssertLogContains(t, errorMessage, func() {
		response := rt.SendAdminRequest("GET", "/db/"+key, "")
		assert.Equal(t, http.StatusOK, response.Code)
	})

	// Get number of errors after
	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	// Make sure at least one error was logged
	assert.GreaterOrEqual(t, numErrors+1, numErrorsAfter)

}

// Test scenario where another actor updates a different xattr on a document.  Sync Gateway
// should detect and not import/create new revision during read-triggered import
func TestXattrImportMultipleActorOnDemandGet(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create doc via the SDK
	mobileKey := "TestImportMultiActorUpdate"
	mobileBody := make(map[string]interface{})
	mobileBody["channels"] = "ABC"
	_, err := dataStore.Add(mobileKey, 0, mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the document via Sync Gateway.  Will trigger on-demand import.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	// Extract rev from response for comparison with second GET below
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId, ok := body[db.BodyRev].(string)
	assert.True(t, ok, "No rev included in response")

	// Go get the cas for the doc to use for update
	_, cas, getErr := dataStore.GetRaw(mobileKey)
	assert.NoError(t, getErr, "Error retrieving cas for multi-actor document")

	// Modify the document via the SDK to add a new, non-mobile xattr
	xattrVal := make(map[string]interface{})
	xattrVal["actor"] = "not mobile"

	ctx := base.TestCtx(t)
	_, mutateErr := dataStore.UpdateXattrs(ctx, mobileKey, uint32(0), cas, map[string][]byte{"_nonmobile": base.MustJSONMarshal(t, xattrVal)}, nil)

	assert.NoError(t, mutateErr, "Error updating non-mobile xattr for multi-actor document")

	// Attempt to get the document again via Sync Gateway.  Should not trigger import.
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	newRevId := body[db.BodyRev].(string)
	log.Printf("Retrieved via Sync Gateway after non-mobile update, revId:%v", newRevId)
	assert.Equal(t, revId, newRevId)
}

// Test scenario where another actor updates a different xattr on a document.  Sync Gateway
// should detect and not import/create new revision during write-triggered import
func TestXattrImportMultipleActorOnDemandPut(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create doc via the SDK
	mobileKey := "TestImportMultiActorUpdate"
	mobileBody := make(map[string]interface{})
	mobileBody["channels"] = "ABC"
	_, err := dataStore.Add(mobileKey, 0, mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the document via Sync Gateway.  Will trigger on-demand import.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	// Extract rev from response for comparison with second GET below
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId, ok := body[db.BodyRev].(string)
	assert.True(t, ok, "No rev included in response")

	// Go get the cas for the doc to use for update
	_, cas, getErr := dataStore.GetRaw(mobileKey)
	assert.NoError(t, getErr, "Error retrieving cas for multi-actor document")

	ctx := base.TestCtx(t)
	// Modify the document via the SDK to add a new, non-mobile xattr
	xattrVal := make(map[string]interface{})
	xattrVal["actor"] = "not mobile"
	_, mutateErr := dataStore.UpdateXattrs(ctx, mobileKey, uint32(0), cas, map[string][]byte{"_nonmobile": base.MustJSONMarshal(t, xattrVal)}, nil)
	assert.NoError(t, mutateErr, "Error updating non-mobile xattr for multi-actor document")

	// Attempt to update the document again via Sync Gateway.  Should not trigger import, PUT should be successful,
	// rev should have generation 2.
	putResponse := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", mobileKey, revId), `{"updated":true}`)
	assert.Equal(t, 201, putResponse.Code)
	assert.NoError(t, base.JSONUnmarshal(putResponse.Body.Bytes(), &body))
	log.Printf("Put response details: %s", putResponse.Body.Bytes())
	newRevId, ok := body["rev"].(string)
	assert.True(t, ok, "Unable to cast rev to string")
	log.Printf("Retrieved via Sync Gateway PUT after non-mobile update, revId:%v", newRevId)
	assert.Equal(t, "2-04bb60e2d65acedb2a846daa0ce882ea", newRevId)
}

// Test scenario where another actor updates a different xattr on a document.  Sync Gateway
// should detect and not import/create new revision during feed-based import
func TestXattrImportMultipleActorOnDemandFeed(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: true,
		}},
	}

	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// Create doc via the SDK
	mobileKey := "TestImportMultiActorFeed"
	mobileBody := make(map[string]interface{})
	mobileBody["channels"] = "ABC"
	_, err := dataStore.Add(mobileKey, 0, mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the document via Sync Gateway.  Guarantees initial import is complete
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	// Extract rev from response for comparison with second GET below
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId, ok := body[db.BodyRev].(string)
	assert.True(t, ok, "No rev included in response")

	// Go get the cas for the doc to use for update
	_, cas, getErr := dataStore.GetRaw(mobileKey)
	assert.NoError(t, getErr, "Error retrieving cas for multi-actor document")

	// Check expvars before update
	crcMatchesBefore := rt.GetDatabase().DbStats.Database().Crc32MatchCount.Value()

	ctx := base.TestCtx(t)
	// Modify the document via the SDK to add a new, non-mobile xattr
	xattrVal := make(map[string]interface{})
	xattrVal["actor"] = "not mobile"
	_, mutateErr := dataStore.UpdateXattrs(ctx, mobileKey, uint32(0), cas, map[string][]byte{"_nonmobile": base.MustJSONMarshal(t, xattrVal)}, nil)
	assert.NoError(t, mutateErr, "Error updating non-mobile xattr for multi-actor document")

	// Wait until crc match count changes
	var crcMatchesAfter int64
	for i := 0; i < 20; i++ {
		crcMatchesAfter = rt.GetDatabase().DbStats.Database().Crc32MatchCount.Value()
		// if they changed, import has been processed
		if crcMatchesAfter > crcMatchesBefore {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}

	// Expect one crcMatch, no mismatches
	assert.True(t, crcMatchesAfter-crcMatchesBefore == 1)

	// Get the doc again, validate rev hasn't changed
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	newRevId := body[db.BodyRev].(string)
	log.Printf("Retrieved via Sync Gateway after non-mobile update, revId:%v", newRevId)
	assert.Equal(t, revId, newRevId)

}

// Test scenario where another actor updates a different xattr on a document.  Sync Gateway
// should detect and not import/create new revision during read-triggered import
func TestXattrImportLargeNumbers(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create doc via the SDK
	mobileKey := "TestImportLargeNumbers"
	mobileBody := make(map[string]interface{})
	mobileBody["channels"] = "ABC"
	mobileBody["largeNumber"] = uint64(9223372036854775807)
	_, err := rt.GetSingleDataStore().Add(mobileKey, 0, mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// 2. Attempt to get the document via Sync Gateway.  Will trigger on-demand import.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	// Check the raw bytes, because unmarshalling the response would be another opportunity for the number to get modified
	responseString := string(response.Body.Bytes())
	if !strings.Contains(responseString, `9223372036854775807`) {
		t.Errorf("Response does not contain the expected number format.  Response: %s", responseString)
	}
}

// Structs for manual rev storage validation
type treeDoc struct {
	Meta treeMeta `json:"_sync"`
}
type treeMeta struct {
	RevTree    treeHistory `json:"history"`
	CurrentRev string      `json:"rev"`
	Sequence   uint64      `json:"sequence,omitempty"`
}
type treeHistory struct {
	BodyMap    map[string]string `json:"bodymap"`
	BodyKeyMap map[string]string `json:"bodyKeyMap"`
}

// Test migration of a 1.4 doc with large inline revisions.  Validate they get migrated out of the body
func TestMigrateLargeInlineRevisions(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// Write doc in SG format directly to the bucket
	key := "TestMigrateLargeInlineRevisions"
	bodyString := `
{
  "_sync": {
    "rev": "2-d",
    "flags": 24,
    "sequence": 8,
    "recent_sequences": [4,5,6,7,8],
    "history": {
      "revs": [
        "1-089c019bbfaba27047008599143bc66f",
        "2-b92296d32600ec90dc05ff18ae61a1e8",
        "2-b",
        "2-c",
        "2-d"
      ],
      "parents": [-1,0,0,0,0],
      "bodymap": {
        "1": "{\"value\":\"%s\"}",
        "2": "{\"value\":\"%s\"}",
        "3": "{\"value\":\"%s\"}"
      },
      "channels": [null,null,null,null,null]
    },
    "cas": "",
    "time_saved": "2017-09-14T23:54:25.975220906-07:00"
  },
  "value": "2-d"
}`
	// Inject large property values into the inline bodies
	largeProperty := base.CreateProperty(1000000)
	largeBodyString := fmt.Sprintf(bodyString, largeProperty, largeProperty, largeProperty)

	// Create via the SDK with sync metadata intact
	_, err := rt.GetSingleDataStore().Add(key, 0, []byte(largeBodyString))
	assert.NoError(t, err, "Error writing doc w/ large inline revisions")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+key, "")
	assert.Equal(t, 200, response.Code)

	// Get raw to retrieve metadata, and validate bodies have been moved to xattr
	rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, rawResponse.Code)
	var doc treeDoc
	assert.NoError(t, base.JSONUnmarshal(rawResponse.Body.Bytes(), &doc))
	assert.Equal(t, 3, len(doc.Meta.RevTree.BodyKeyMap))
	assert.Equal(t, 0, len(doc.Meta.RevTree.BodyMap))

}

// Test migration of a 1.4 doc that's been tombstoned
func TestMigrateTombstone(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// Write doc in SG format directly to the bucket
	key := "TestMigrateTombstone"
	bodyString := `
{
    "_deleted": true,
    "_sync": {
        "flags": 1,
        "history": {
            "channels": [
                null,
                null
            ],
            "deleted": [
                1
            ],
            "parents": [
                -1,
                0
            ],
            "revs": [
                "1-f6fa803508c40388de38c9f99729c835",
                "2-6b1e1af9190829c1ceab6f1c8fb9fa3f"
            ]
        },
        "recent_sequences": [
            4,
            5
        ],
        "rev": "2-6b1e1af9190829c1ceab6f1c8fb9fa3f",
        "sequence": 5,
        "time_saved": "2017-11-22T13:24:33.115313269-08:00"
    }
}`

	// Create via the SDK with sync metadata intact
	_, err := rt.GetSingleDataStore().Add(key, 0, []byte(bodyString))
	assert.NoError(t, err, "Error writing tombstoned doc")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+key, "")
	assert.Equal(t, 404, response.Code)

	// Get raw to retrieve metadata, and validate bodies have been moved to xattr
	rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, rawResponse.Code)
	var doc treeDoc
	assert.NoError(t, base.JSONUnmarshal(rawResponse.Body.Bytes(), &doc))
	assert.Equal(t, "2-6b1e1af9190829c1ceab6f1c8fb9fa3f", doc.Meta.CurrentRev)
	assert.Equal(t, uint64(5), doc.Meta.Sequence)

}

// Test migration of a 1.5 doc that already includes some external revision storage from docmeta to xattr.
func TestMigrateWithExternalRevisions(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// Write doc in SG format directly to the bucket
	key := "TestMigrateWithExternalRevisions"
	bodyString := `
{
  "_sync": {
    "rev": "2-d",
    "flags": 24,
    "sequence": 8,
    "recent_sequences": [4,5,6,7,8],
    "history": {
      "revs": [
        "1-089c019bbfaba27047008599143bc66f",
        "2-b92296d32600ec90dc05ff18ae61a1e8",
        "2-b",
        "2-c",
        "2-d"
      ],
      "parents": [-1,0,0,0,0],
      "bodymap": {
        "2": "{\"value\":\"%s\"}",
        "3": "{\"value\":\"%s\"}"
      },
      "bodyKeyMap": {
      	"1": "_sync:rb:test"
      },
      "channels": [null,null,null,null,null]
    },
    "cas": "",
    "time_saved": "2017-09-14T23:54:25.975220906-07:00"
  },
  "value": "2-d"
}`
	// Inject large property values into the inline bodies
	largeProperty := base.CreateProperty(100)
	largeBodyString := fmt.Sprintf(bodyString, largeProperty, largeProperty)

	// Create via the SDK with sync metadata intact
	_, err := rt.GetSingleDataStore().Add(key, 0, []byte(largeBodyString))
	assert.NoError(t, err, "Error writing doc w/ large inline revisions")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+key, "")
	assert.Equal(t, 200, response.Code)

	// Get raw to retrieve metadata, and validate bodies have been moved to xattr
	rawResponse := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", key), "")
	assert.Equal(t, 200, rawResponse.Code)
	var doc treeDoc
	assert.NoError(t, base.JSONUnmarshal(rawResponse.Body.Bytes(), &doc))
	assert.Equal(t, 1, len(doc.Meta.RevTree.BodyKeyMap))
	assert.Equal(t, 2, len(doc.Meta.RevTree.BodyMap))
}

// Write a doc via SDK with an expiry value.  Verify that expiry is preserved when doc is imported via DCP feed
func TestXattrFeedBasedImportPreservesExpiry(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: true,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create docs via the SDK with expiry set
	mobileKey := "TestXattrImportPreservesExpiry"
	mobileKeyNoExpiry := fmt.Sprintf("%s-noexpiry", mobileKey)
	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"

	// Write directly to bucket with an expiry
	expiryUnixEpoch := time.Now().Add(time.Second * 30).Unix()
	_, err := dataStore.Add(mobileKey, uint32(expiryUnixEpoch), mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Verify the expiry is as expected
	beforeExpiry, err := dataStore.GetExpiry(rt.Context(), mobileKey)
	require.NoError(t, err, "Error calling GetExpiry()")
	assertExpiry(t, uint32(expiryUnixEpoch), beforeExpiry)

	// Negative test case -- no expiry
	_, err = dataStore.Add(mobileKeyNoExpiry, 0, mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Wait until the change appears on the changes feed to ensure that it's been imported by this point
	changes, err := rt.WaitForChanges(2, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err, "Error waiting for changes")

	log.Printf("changes: %+v", changes)
	changeEntry := changes.Results[0]
	assert.True(t, changeEntry.ID == mobileKey || changeEntry.ID == mobileKeyNoExpiry)

	// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
	assertXattrSyncMetaRevGeneration(t, dataStore, mobileKey, 1)
	assertXattrSyncMetaRevGeneration(t, dataStore, mobileKeyNoExpiry, 1)

	// Verify the expiry has been preserved after the import
	afterExpiry, err := dataStore.GetExpiry(rt.Context(), mobileKey)
	assert.NoError(t, err, "Error calling GetExpiry()")
	assertExpiry(t, beforeExpiry, afterExpiry)

	// Negative test case -- make sure no expiry was erroneously added by the import
	expiry, err := dataStore.GetExpiry(rt.Context(), mobileKeyNoExpiry)
	assert.NoError(t, err, "Error calling GetExpiry()")
	assert.True(t, expiry == 0)
}

// Test migration of a 1.5 doc that has an expiry value.
func TestFeedBasedMigrateWithExpiry(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: true,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// Write doc in SG format directly to the bucket
	key := "TestFeedBasedMigrateWithExpiry"

	// Create via the SDK with sync metadata intact
	expirySeconds := time.Second * 30
	testExpiry := uint32(time.Now().Add(expirySeconds).Unix())
	bodyString := rawDocWithSyncMeta()
	_, err := dataStore.Add(key, testExpiry, []byte(bodyString))
	assert.NoError(t, err, "Error writing doc w/ expiry")

	// Wait for doc to appear on changes feed
	// Wait until the change appears on the changes feed to ensure that it's been imported by this point
	now := time.Now()
	changes, err := rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err, "Error waiting for changes")
	changeEntry := changes.Results[0]
	assert.Equal(t, key, changeEntry.ID)
	log.Printf("Saw doc on changes feed after %v", time.Since(now))

	// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
	assertXattrSyncMetaRevGeneration(t, dataStore, key, 1)

	// Now get the doc expiry and validate that it has been migrated into the doc metadata
	expiry, err := dataStore.GetExpiry(rt.Context(), key)
	assert.True(t, expiry > 0)
	assert.NoError(t, err, "Error calling getExpiry()")
	log.Printf("expiry: %v", expiry)
	assertExpiry(t, testExpiry, expiry)

}

// Verify that an on-demand import of a null document during write doesn't block the incoming write
func TestOnDemandWriteImportReplacingNullDoc(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	key := t.Name()

	// Write json doc directly to bucket with a null body
	nullBody := []byte("null")
	_, err := dataStore.AddRaw(key, 0, nullBody)
	require.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the doc via Sync Gateway, triggering a cancelled on-demand import of the null document
	response := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+key, "")
	rest.RequireStatus(t, response, http.StatusBadRequest) // import attempted with empty body

	// Attempt to update the doc via Sync Gateway, triggering on-demand import of the null document - should ignore empty body error and proceed with write
	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"
	mobileBody["foo"] = "bar"
	mobileBodyMarshalled, err := base.JSONMarshal(mobileBody)
	assert.NoError(t, err, "Error marshalling body")
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, string(mobileBodyMarshalled))
	rest.RequireStatus(t, response, 201)
}

// Verify that an on-demand import of a nil document during write doesn't block the incoming write
func TestOnDemandWriteImportReplacingNilDoc(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	key := t.Name()

	// Write binary doc directly to bucket with a nil body
	var nilBody []byte
	_, err := dataStore.AddRaw(key, 0, nilBody)
	require.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the doc via Sync Gateway, triggering a cancelled on-demand import of the null document
	response := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+key, "")
	rest.RequireStatus(t, response, 404)

	// Attempt to update the doc via Sync Gateway, triggering on-demand import of the null document
	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"
	mobileBody["foo"] = "bar"
	mobileBodyMarshalled, err := base.JSONMarshal(mobileBody)
	assert.NoError(t, err, "Error marshalling body")
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, string(mobileBodyMarshalled))
	rest.RequireStatus(t, response, 201)
}

// Write a doc via SDK with an expiry value.  Verify that expiry is preserved when doc is imported via on-demand
// import (GET or WRITE)
func TestXattrOnDemandImportPreservesExpiry(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"

	triggerOnDemandViaGet := func(rt *rest.RestTester, key string) {
		rt.SendAdminRequest("GET", "/{{.keyspace}}/"+key, "")
	}
	triggerOnDemandViaWrite := func(rt *rest.RestTester, key string) {
		mobileBody["foo"] = "bar"
		mobileBodyMarshalled, err := base.JSONMarshal(mobileBody)
		assert.NoError(t, err, "Error marshalling body")
		rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, string(mobileBodyMarshalled))
	}

	type testcase struct {
		onDemandCallback      func(*rest.RestTester, string)
		name                  string
		expectedRevGeneration int
	}
	testCases := []testcase{
		{
			onDemandCallback:      triggerOnDemandViaGet,
			name:                  "triggerOnDemandViaGet",
			expectedRevGeneration: 1,
		},
		{
			onDemandCallback:      triggerOnDemandViaWrite,
			name:                  "triggerOnDemandViaWrite",
			expectedRevGeneration: 1,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s", testCase.name), func(t *testing.T) {

			rtConfig := rest.RestTesterConfig{
				SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					AutoImport: false,
				}},
			}
			rt := rest.NewRestTester(t, &rtConfig)
			defer rt.Close()
			dataStore := rt.GetSingleDataStore()

			base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

			key := fmt.Sprintf("TestXattrOnDemandImportPreservesExpiry-%d", i)

			// Write directly to bucket with an expiry
			expiryUnixEpoch := time.Now().Add(time.Second * 30).Unix()
			_, err := dataStore.Add(key, uint32(expiryUnixEpoch), mobileBody)
			require.NoError(t, err, "Error writing SDK doc")

			// Verify the expiry is as expected
			beforeExpiry, err := dataStore.GetExpiry(rt.Context(), key)
			require.NoError(t, err, "Error calling GetExpiry()")
			assertExpiry(t, uint32(expiryUnixEpoch), beforeExpiry)

			testCase.onDemandCallback(rt, key)

			// Wait until the change appears on the changes feed to ensure that it's been imported by this point.
			// This is probably unnecessary in the case of on-demand imports, but it doesn't hurt to leave it in as a double check.
			changes, err := rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
			require.NoError(t, err, "Error waiting for changes")
			changeEntry := changes.Results[0]
			assert.Equal(t, key, changeEntry.ID)

			// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
			assertXattrSyncMetaRevGeneration(t, dataStore, key, testCase.expectedRevGeneration)

			// Verify the expiry has not been changed from the original expiry value
			afterExpiry, err := dataStore.GetExpiry(rt.Context(), key)
			require.NoError(t, err, "Error calling GetExpiry()")
			assertExpiry(t, beforeExpiry, afterExpiry)
		})
	}

}

// Write a doc via SDK with an expiry value.  Verify that expiry is preserved when doc is migrated via on-demand
// import (GET or WRITE)
func TestOnDemandMigrateWithExpiry(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	triggerOnDemandViaGet := func(rt *rest.RestTester, key string) {
		// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
		response := rt.SendAdminRequest("GET", "/db/"+key, "")
		assert.Equal(t, 200, response.Code)
	}
	triggerOnDemandViaWrite := func(rt *rest.RestTester, key string) {
		rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", key), "{}")
	}

	type testcase struct {
		onDemandCallback      func(*rest.RestTester, string)
		name                  string
		expectedRevGeneration int
	}
	testCases := []testcase{
		{
			onDemandCallback:      triggerOnDemandViaGet,
			name:                  "triggerOnDemandViaGet",
			expectedRevGeneration: 1,
		},
		{
			onDemandCallback:      triggerOnDemandViaWrite,
			name:                  "triggerOnDemandViaWrite",
			expectedRevGeneration: 1,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s", testCase.name), func(t *testing.T) {

			key := fmt.Sprintf("TestOnDemandGetWriteMigrateWithExpiry-%d", i)

			rtConfig := rest.RestTesterConfig{
				SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					AutoImport: false,
				}},
			}
			rt := rest.NewRestTesterDefaultCollection(t, &rtConfig)
			defer rt.Close()
			dataStore := rt.GetSingleDataStore()

			base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

			// Create via the SDK with sync metadata intact
			expirySeconds := time.Second * 30
			syncMetaExpiry := time.Now().Add(expirySeconds)
			bodyString := rawDocWithSyncMeta()
			_, err := dataStore.Add(key, uint32(syncMetaExpiry.Unix()), []byte(bodyString))
			assert.NoError(t, err, "Error writing doc w/ expiry")

			testCase.onDemandCallback(rt, key)

			// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
			assertXattrSyncMetaRevGeneration(t, dataStore, key, testCase.expectedRevGeneration)

			// Now get the doc expiry and validate that it has been migrated into the doc metadata
			expiry, err := dataStore.GetExpiry(rt.Context(), key)
			assert.NoError(t, err, "Error calling GetExpiry()")
			assert.True(t, expiry > 0)
			log.Printf("expiry: %v", expiry)
			assertExpiry(t, uint32(syncMetaExpiry.Unix()), expiry)

		})
	}

}

// Write through SG, non-imported SDK write, subsequent SG write
func TestXattrSGWriteOfNonImportedDoc(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	importFilter := `function (doc) { return doc.type == "mobile"}`
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:   false,
			ImportFilter: &importFilter,
		}},
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig) // use default collection since we are using default sync function
	defer rt.Close()

	log.Printf("Starting get bucket....")

	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create doc via SG
	sgWriteKey := "TestImportFilterSGWrite"
	sgWriteBody := `{"type":"whatever I want - I'm writing through SG",
	                 "channels": "ABC"}`
	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", sgWriteKey), sgWriteBody)
	assert.Equal(t, 201, response.Code)
	assertDocProperty(t, response, "rev", "1-25c26cdf9d7771e07f00be1d13f7fb7c")

	// 2. Update via SDK, not matching import filter.  Will not be available via SG
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["type"] = "non-mobile"
	nonMobileBody["channels"] = "ABC"
	err := dataStore.Set(sgWriteKey, 0, nil, nonMobileBody)
	assert.NoError(t, err, "Error updating SG doc from SDK ")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand import.

	response = rt.SendAdminRequest("GET", "/db/"+sgWriteKey, "")
	assert.Equal(t, 404, response.Code)
	assertDocProperty(t, response, "reason", "Not imported")

	// 3. Rewrite through SG - should treat as new insert
	sgWriteBody = `{"type":"SG client rewrite",
	                 "channels": "NBC"}`
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", sgWriteKey), sgWriteBody)
	assert.Equal(t, 201, response.Code)
	// Validate rev is 1, new rev id
	assertDocProperty(t, response, "rev", "1-b9cdd5d413b572476799930f065657a6")
}

// Test to write a binary document to the bucket, ensure it's not imported.
func TestImportBinaryDoc(t *testing.T) {

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	log.Printf("Starting get bucket....")

	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD, base.KeyCache)

	// 1. Write a binary doc through the SDK
	rawBytes := []byte("some bytes")
	err := dataStore.SetRaw("binaryDoc", 0, nil, rawBytes)
	assert.NoError(t, err, "Error writing binary doc through the SDK")

	// 2. Ensure we can't retrieve the document via SG
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/binaryDoc", "")
	assert.True(t, response.Code != 200)
}

// TestImportZeroValueDecimalPlaces tests that docs containing numbers of the form 0.0000 are imported correctly.
func TestImportZeroValueDecimalPlaces(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	rtConfig := rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: true,
		}},
	}

	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	const minDecimalPlaces = 0
	const maxDecimalPlaces = 20

	for i := minDecimalPlaces; i <= maxDecimalPlaces; i++ {
		var docNumber string
		if i == 0 {
			docNumber = "0"
		} else {
			docNumber = "0." + strings.Repeat("0", i)
		}
		docID := "TestImportDecimalScale" + strconv.Itoa(i)
		docBody := []byte(fmt.Sprintf(`{"key":%s}`, docNumber))

		ok, err := dataStore.AddRaw(docID, 0, docBody)
		require.NoError(t, err)
		require.True(t, ok)

		t.Logf("Inserting doc %s: %s", docID, string(docBody))
	}

	changes, err := rt.WaitForChanges((maxDecimalPlaces+1)-minDecimalPlaces, "/{{.keyspace}}/_changes", "", true)
	assert.NoError(t, err, "Error waiting for changes")
	require.Lenf(t, changes.Results, maxDecimalPlaces+1-minDecimalPlaces, "Expected %d changes in: %#v", (maxDecimalPlaces+1)-minDecimalPlaces, changes.Results)
	ctx := base.TestCtx(t)

	for i := minDecimalPlaces; i <= maxDecimalPlaces; i++ {
		docID := "TestImportDecimalScale" + strconv.Itoa(i)
		var syncData db.SyncData
		docBody, xattrs, _, err := dataStore.GetWithXattrs(ctx, docID, []string{base.SyncXattrName})
		require.NoError(t, err)
		require.Contains(t, xattrs, base.SyncXattrName)
		require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))

		assert.NotEqualf(t, "", syncData.CurrentRev, "Expecting non-empty rev ID for imported doc %v", docID)
		assert.Truef(t, strings.HasPrefix(syncData.CurrentRev, "1-"), "Expecting rev 1 for imported doc %v", docID)

		var docNumber string
		if i == 0 {
			docNumber = "0"
		} else {
			docNumber = "0." + strings.Repeat("0", i)
		}
		assert.Contains(t, string(docBody), `"key":`+docNumber)
		t.Logf("Got doc %s: %s with revID: %v", docID, string(docBody), syncData.CurrentRev)
	}

}

// TestImportZeroValueDecimalPlacesScientificNotation tests that docs containing numbers of the form 0e10 are imported correctly.
func TestImportZeroValueDecimalPlacesScientificNotation(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	rtConfig := rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: true,
		}},
	}

	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	const minDecimalPlaces = 0
	const maxDecimalPlaces = 20

	dataStore := rt.GetSingleDataStore()

	for i := minDecimalPlaces; i <= maxDecimalPlaces; i++ {
		var docNumber string
		if i == 0 {
			docNumber = "0"
		} else {
			docNumber = "0E-" + strconv.Itoa(i)
		}
		docID := "TestImportDecimalPlacesScientificNotation" + strconv.Itoa(i)
		docBody := []byte(fmt.Sprintf(`{"key":%s}`, docNumber))

		ok, err := dataStore.AddRaw(docID, 0, docBody)
		require.NoError(t, err)
		require.True(t, ok)

		t.Logf("Inserting doc %s: %s", docID, string(docBody))
	}

	changes, err := rt.WaitForChanges((maxDecimalPlaces+1)-minDecimalPlaces, "/{{.keyspace}}/_changes", "", true)
	assert.NoError(t, err, "Error waiting for changes")
	require.Lenf(t, changes.Results, maxDecimalPlaces+1-minDecimalPlaces, "Expected %d changes in: %#v", (maxDecimalPlaces+1)-minDecimalPlaces, changes.Results)

	ctx := base.TestCtx(t)
	for i := minDecimalPlaces; i <= maxDecimalPlaces; i++ {
		docID := "TestImportDecimalPlacesScientificNotation" + strconv.Itoa(i)
		var syncData db.SyncData
		docBody, xattrs, _, err := dataStore.GetWithXattrs(ctx, docID, []string{base.SyncXattrName})
		require.NoError(t, err)
		require.Contains(t, xattrs, base.SyncXattrName)
		require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))

		assert.NotEqualf(t, "", syncData.CurrentRev, "Expecting non-empty rev ID for imported doc %v", docID)
		assert.Truef(t, strings.HasPrefix(syncData.CurrentRev, "1-"), "Expecting rev 1 for imported doc %v", docID)

		var docNumber string
		if i == 0 {
			docNumber = "0"
		} else {
			docNumber = "0E-" + strconv.Itoa(i)
		}
		assert.Contains(t, string(docBody), `"key":`+docNumber)
		t.Logf("Got doc %s: %s with revID: %v", docID, string(docBody), syncData.CurrentRev)
	}

}

// Test creation of backup revision on import
func TestImportRevisionCopy(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			ImportBackupOldRev: base.BoolPtr(true),
			AutoImport:         false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	key := "TestImportRevisionCopy"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestImportRevisionCopy"
	docBody["channels"] = "ABC"

	// 1. Create via SDK
	_, err := dataStore.Add(key, 0, docBody)
	assert.NoError(t, err, "Unable to insert doc TestImportDelete")

	// 2. Trigger import via SG retrieval
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, response.Code)
	var rawInsertResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")
	rev1id := rawInsertResponse.Sync.Rev

	// 3. Update via SDK
	updatedBody := make(map[string]interface{})
	updatedBody["test"] = "TestImportRevisionCopyModified"
	updatedBody["channels"] = "DEF"
	err = dataStore.Set(key, 0, nil, updatedBody)
	assert.NoError(t, err, fmt.Sprintf("Unable to update doc %s", key))

	// 4. Trigger import of update via SG retrieval
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, response.Code)
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")

	// 5. Flush the rev cache (simulates attempted retrieval by a different SG node, since testing framework isn't great
	//    at simulating multiple SG instances)
	rt.GetSingleTestDatabaseCollection().FlushRevisionCacheForTest()

	// 6. Attempt to retrieve previous revision body
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, rev1id), "")
	assert.Equal(t, 200, response.Code)
}

// Test creation of backup revision on import, when rev is no longer available in rev cache.
func TestImportRevisionCopyUnavailable(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			ImportBackupOldRev: base.BoolPtr(true),
			AutoImport:         false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	if rt.GetDatabase().DeltaSyncEnabled() {
		t.Skipf("Skipping TestImportRevisionCopyUnavailable when delta sync enabled, delta revision backup handling invalidates test")
	}

	dataStore := rt.GetSingleDataStore()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	key := "TestImportRevisionCopy"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestImportRevisionCopy"
	docBody["channels"] = "ABC"

	// 1. Create via SDK
	_, err := dataStore.Add(key, 0, docBody)
	assert.NoError(t, err, "Unable to insert doc TestImportDelete")

	// 2. Trigger import via SG retrieval
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, response.Code)
	var rawInsertResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")
	rev1id := rawInsertResponse.Sync.Rev

	// 3. Flush the rev cache (simulates attempted retrieval by a different SG node, since testing framework isn't great
	//    at simulating multiple SG instances)
	rt.GetSingleTestDatabaseCollection().FlushRevisionCacheForTest()

	// 4. Update via SDK
	updatedBody := make(map[string]interface{})
	updatedBody["test"] = "TestImportRevisionCopyModified"
	updatedBody["channels"] = "DEF"
	err = dataStore.Set(key, 0, nil, updatedBody)
	assert.NoError(t, err, fmt.Sprintf("Unable to update doc %s", key))

	// 5. Trigger import of update via SG retrieval
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, response.Code)
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")

	// 6. Attempt to retrieve previous revision body.  Should return missing, as rev wasn't in rev cache when import occurred.
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, rev1id), "")
	assert.Equal(t, 404, response.Code)
}

func TestImportComputeStatOnDemandGet(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	rtConfig := rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	key := t.Name()
	docBody := make(map[string]interface{})
	docBody["test"] = t.Name()
	docBody["channels"] = "ABC"

	// assert the stat starts at 0 for a new database
	computeStat := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Equal(t, int64(0), computeStat)

	// add doc to bucket
	_, err := dataStore.Add(key, 0, docBody)
	require.NoError(t, err, fmt.Sprintf("Unable to insert doc %s", key))

	// trigger import via SG retrieval
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	rest.RequireStatus(t, response, http.StatusOK)

	// assert the process compute stat has incremented
	computeStat1 := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Greater(t, computeStat1, int64(0))

	// update doc in bucket
	updatedBody := make(map[string]interface{})
	updatedBody["test"] = t.Name() + "Modified"
	updatedBody["channels"] = "DEF"
	err = dataStore.Set(key, 0, nil, updatedBody)
	require.NoError(t, err, fmt.Sprintf("Unable to update doc %s", key))

	// trigger import via SG retrieval
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	rest.RequireStatus(t, response, http.StatusOK)
	// assert the process compute stat has incremented again after another import
	computeStat2 := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Greater(t, computeStat2, computeStat1)

}

func TestImportComputeStatOnDemandWrite(t *testing.T) {

	importFilter := `function (doc) { return doc.type == "mobile"}`
	rtConfig := rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:   false,
			ImportFilter: &importFilter,
		}},
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	key := t.Name()
	docBody := make(map[string]interface{})
	docBody["type"] = "non-mobile"
	docBody["channels"] = "ABC"

	// assert the stat starts at 0 for a new database
	computeStat := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Equal(t, int64(0), computeStat)

	// add doc to bucket
	_, err := dataStore.Add(key, 0, docBody)
	require.NoError(t, err, fmt.Sprintf("Unable to insert doc %s", key))

	// trigger on demand import of this doc - should be rejected
	response := rt.SendAdminRequest("GET", "/db/_raw/"+key, "")
	rest.RequireStatus(t, response, http.StatusNotFound)
	assertDocProperty(t, response, "reason", "Not imported")

	// assert stat still no incremented as import filter rejects the doc
	computeStat1 := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Equal(t, int64(0), computeStat1)

	// rewrite through SG - should treat as new insert
	docBodyString := `{"type":"SG client rewrite",
	                 "channels": "NBC"}`
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", key), docBodyString)
	rest.RequireStatus(t, response, http.StatusCreated)

	// assert stat increases as function OnDemandImportForWrite will have been executed
	computeStat2 := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Greater(t, computeStat2, computeStat1)
}

func TestAutoImportComputeStat(t *testing.T) {

	rtConfig := rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: true,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	key := t.Name()
	docBody := make(map[string]interface{})
	docBody["test"] = t.Name()
	docBody["channels"] = "ABC"

	// assert the stat starts at 0 for a new database
	computeStat := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Equal(t, int64(0), computeStat)

	// add doc to bucket
	_, err := dataStore.Add(key, 0, docBody)
	require.NoError(t, err, fmt.Sprintf("Unable to insert doc %s", key))

	// wait for import to process
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value()
	}, 1)

	// assert the stat increments for the auto import of the above doc
	computeStat1 := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Greater(t, computeStat1, int64(0))
}

func TestQueryResyncImportComputeStat(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}

	rtConfig := rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
			Unsupported: &db.UnsupportedOptions{
				UseQueryBasedResyncManager: true,
			},
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	const numDocs = 100
	var resp *rest.TestResponse

	for i := 0; i < numDocs; i++ {
		resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"random": "doc"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	computeStat := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Equal(t, int64(0), computeStat)

	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_offline", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	require.NoError(t, rt.WaitForDatabaseState(rt.GetDatabase().Name, db.DBOffline))

	resp = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_resync?action=start", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	rt.WaitForResyncStatus(db.BackgroundProcessStateCompleted)

	computeStat1 := rt.GetDatabase().DbStats.DatabaseStats.ImportProcessCompute.Value()
	require.Greater(t, computeStat1, computeStat)

}

// Verify config flag for import creation of backup revision on import
func TestImportRevisionCopyDisabled(t *testing.T) {
	// ImportBackupOldRev not set in config, defaults to false
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	if rt.GetDatabase().DeltaSyncEnabled() {
		t.Skipf("Skipping TestImportRevisionCopyDisabled when delta sync enabled, delta revision backup handling invalidates test")
	}

	dataStore := rt.GetSingleDataStore()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)

	key := "TestImportRevisionCopy"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestImportRevisionCopy"
	docBody["channels"] = "ABC"

	// 1. Create via SDK
	_, err := dataStore.Add(key, 0, docBody)
	assert.NoError(t, err, "Unable to insert doc TestImportDelete")

	// 2. Trigger import via SG retrieval
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, response.Code)
	var rawInsertResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")
	rev1id := rawInsertResponse.Sync.Rev

	// 3. Update via SDK
	updatedBody := make(map[string]interface{})
	updatedBody["test"] = "TestImportRevisionCopyModified"
	updatedBody["channels"] = "DEF"
	err = dataStore.Set(key, 0, nil, updatedBody)
	assert.NoError(t, err, fmt.Sprintf("Unable to update doc %s", key))

	// 4. Trigger import of update via SG retrieval
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+key, "")
	assert.Equal(t, 200, response.Code)
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	assert.NoError(t, err, "Unable to unmarshal raw response")

	// 5. Flush the rev cache (simulates attempted retrieval by a different SG node, since testing framework isn't great
	//    at simulating multiple SG instances)
	rt.GetSingleTestDatabaseCollection().FlushRevisionCacheForTest()

	// 6. Attempt to retrieve previous revision body.  Should fail, as backup wasn't persisted
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", key, rev1id), "")
	assert.Equal(t, 404, response.Code)
}

// Test DCP backfill stats
func TestDcpBackfill(t *testing.T) {

	t.Skip("Test disabled pending CBG-560")

	rt := rest.NewRestTester(t, nil)

	log.Printf("Starting get bucket....")

	dataStore := rt.GetSingleDataStore()

	// Write enough documents directly to the bucket to ensure multiple docs per vbucket (on average)
	docBody := make(map[string]interface{})
	docBody["type"] = "sdk_write"
	for i := 0; i < 2500; i++ {
		err := dataStore.Set(fmt.Sprintf("doc_%d", i), 0, nil, docBody)
		assert.NoError(t, err, fmt.Sprintf("error setting doc_%d", i))
	}

	// Close the previous test context
	rt.Close()

	log.Print("Creating new database context")

	// Create a new context, with import docs enabled, to process backfill
	newRtConfig := rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: true,
		}},
	}
	newRt := rest.NewRestTester(t, &newRtConfig)
	defer newRt.Close()
	log.Printf("Poke the rest tester so it starts DCP processing:")

	backfillComplete := false
	var expectedBackfill, completedBackfill int
	for i := 0; i < 20; i++ {
		importFeedStats := newRt.GetDatabase().DbStats.Database().ImportFeedMapStats
		expectedBackfill, _ := strconv.Atoi(importFeedStats.Get("dcp_backfill_expected").String())
		completedBackfill, _ := strconv.Atoi(importFeedStats.Get("dcp_backfill_completed").String())
		if expectedBackfill > 0 && completedBackfill >= expectedBackfill {
			log.Printf("backfill complete: %d/%d", completedBackfill, expectedBackfill)
			backfillComplete = true
			break
		} else {
			log.Printf("backfill still in progress: %d/%d", completedBackfill, expectedBackfill)
			time.Sleep(1 * time.Second)
		}
	}
	assert.True(t, backfillComplete, fmt.Sprintf("Backfill didn't complete after 20s. Latest: %d/%d", completedBackfill, expectedBackfill))

	log.Printf("done...%s  (%d/%d)", newRt.ServerContext().Database(newRt.Context(), "db").Name, completedBackfill, expectedBackfill)

}

// Validate SG behaviour if there's an unexpected body on a tombstone
func TestUnexpectedBodyOnTombstone(t *testing.T) {

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD)

	// 1. Create doc via the SDK
	mobileKey := "TestTombstoneUpdate"
	mobileBody := make(map[string]interface{})
	mobileBody["channels"] = "ABC"
	_, err := dataStore.Add(mobileKey, 0, mobileBody)
	assert.NoError(t, err, "Error writing SDK doc")

	// Attempt to get the document via Sync Gateway.  Will trigger on-demand import.
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	// Extract rev from response for comparison with second GET below
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	revId, ok := body[db.BodyRev].(string)
	assert.True(t, ok, "No rev included in response")

	// Delete the document via the SDK
	getBody := make(map[string]interface{})
	cas, err := dataStore.Get(mobileKey, &getBody)
	require.NoError(t, err)

	// Attempt to get the document via Sync Gateway.  Will trigger on-demand import, tombstone creation
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)

	ctx := base.TestCtx(t)
	// Modify the document via the SDK to add the body back
	xattrVal := make(map[string]interface{})
	xattrVal["actor"] = "not mobile"
	_, mutateErr := dataStore.UpdateXattrs(ctx, mobileKey, uint32(0), cas, map[string][]byte{"_nonmobile": base.MustJSONMarshal(t, xattrVal)}, nil)
	assert.NoError(t, mutateErr, "Error updating non-mobile xattr for multi-actor document")

	// Attempt to get the document again via Sync Gateway.  Should not trigger import.
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+mobileKey, "")
	assert.Equal(t, 200, response.Code)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	newRevId := body[db.BodyRev].(string)
	log.Printf("Retrieved via Sync Gateway after non-mobile update, revId:%v", newRevId)
	assert.Equal(t, revId, newRevId)
}

func assertDocProperty(t *testing.T, getDocResponse *rest.TestResponse, propertyName string, expectedPropertyValue interface{}) {
	var responseBody map[string]interface{}
	err := base.JSONUnmarshal(getDocResponse.Body.Bytes(), &responseBody)
	assert.NoError(t, err, "Error unmarshalling document response")
	value, ok := responseBody[propertyName]
	assert.True(t, ok, fmt.Sprintf("Expected property %s not found in response %s", propertyName, getDocResponse.Body.Bytes()))
	assert.Equal(t, expectedPropertyValue, value)
}

func rawDocWithSyncMeta() string {

	return `
{
    "_sync": {
        "rev": "1-ca9ad22802b66f662ff171f226211d5c",
        "sequence": 1,
        "recent_sequences": [
            1
        ],
        "history": {
            "revs": [
                "1-ca9ad22802b66f662ff171f226211d5c"
            ],
            "parents": [
                -1
            ],
            "channels": [
                null
            ]
        },
        "cas": "",
        "time_saved": "2017-11-29T12:46:13.456631-08:00"
    }
}
`

}

func assertXattrSyncMetaRevGeneration(t *testing.T, dataStore base.DataStore, key string, expectedRevGeneration int) {
	xattrs, _, err := dataStore.GetXattrs(base.TestCtx(t), key, []string{base.SyncXattrName})
	assert.NoError(t, err, "Error Getting Xattr")
	xattr := map[string]interface{}{}
	require.Contains(t, xattrs, base.SyncXattrName)
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &xattr))
	revision, ok := xattr["rev"]
	assert.True(t, ok)
	generation, _ := db.ParseRevID(base.TestCtx(t), revision.(string))
	log.Printf("assertXattrSyncMetaRevGeneration generation: %d rev: %s", generation, revision)
	assert.True(t, generation == expectedRevGeneration)
}

func TestDeletedEmptyDocumentImport(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport)
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create a document with empty body through SG
	const docId = "doc1"
	response := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docId, `{}`)
	assert.Equal(t, http.StatusCreated, response.Code)

	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "1-ca9ad22802b66f662ff171f226211d5c", body["rev"])

	// Delete the document through SDK
	err := rt.GetSingleDataStore().Delete(docId)
	assert.NoError(t, err, "Unable to delete doc %s", docId)

	// Get the doc and check deleted revision is getting imported
	response = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docId, "")
	assert.Equal(t, http.StatusOK, response.Code)
	rawResponse := make(map[string]interface{})
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawResponse)
	require.NoError(t, err, "Unable to unmarshal raw response")

	assert.True(t, rawResponse[db.BodyDeleted].(bool))
	syncMeta := rawResponse["_sync"].(map[string]interface{})
	assert.Equal(t, "2-5d3308aae9930225ed7f6614cf115366", syncMeta["rev"])
}

// Check deleted document via SDK is getting imported if it is included in through ImportFilter function.
func TestDeletedDocumentImportWithImportFilter(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc) {console.log("Doc in Sync Fn:" + JSON.stringify(doc))}`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
			ImportFilter: base.StringPtr(`function (doc) {
				console.log("Doc in Import Filter:" + JSON.stringify(doc));
				if (doc.channels || doc._deleted) {
					return true
				}
				return false
			}`),
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	dataStore := rt.GetSingleDataStore()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyCRUD, base.KeyJavascript)

	// Create document via SDK
	key := "doc1"
	docBody := db.Body{"key": key, "channels": "ABC"}
	expiry := time.Now().Add(time.Second * 30)
	_, err := dataStore.Add(key, uint32(expiry.Unix()), docBody)
	assert.NoErrorf(t, err, "Unable to insert doc %s", key)

	// Trigger import and check whether created document is getting imported
	endpoint := fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", key)
	response := rt.SendAdminRequest(http.MethodGet, endpoint, "")
	assert.Equal(t, http.StatusOK, response.Code)
	var respBody db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	syncMeta := respBody[base.SyncPropertyName].(map[string]interface{})
	assert.NotEmpty(t, syncMeta["rev"].(string))

	// Delete the document via SDK
	err = dataStore.Delete(key)
	assert.NoErrorf(t, err, "Unable to delete doc %s", key)

	// Trigger import and check whether deleted document is getting imported
	response = rt.SendAdminRequest(http.MethodGet, endpoint, "")
	assert.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &respBody))
	assert.True(t, respBody[db.BodyDeleted].(bool))
	syncMeta = respBody[base.SyncPropertyName].(map[string]interface{})
	assert.NotEmpty(t, syncMeta["rev"].(string))
}

// CBG-1995: Test the support for using an underscore prefix in the top-level body of a document
// CBG-2023: Test preventing underscore attachments
func TestImportInternalPropertiesHandling(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	testCases := []struct {
		name               string
		importBody         map[string]interface{}
		expectReject       bool
		expectedStatusCode *int // Defaults to not 200 (for expectedReject=true) and 200 if expectedReject=false
	}{
		{
			name:         "Valid document with properties and special prop",
			importBody:   map[string]interface{}{"true": false, "_cookie": "is valid"},
			expectReject: false,
		},
		{
			name:         "Valid document with special prop",
			importBody:   map[string]interface{}{"_cookie": "is valid"},
			expectReject: false,
		},
		{
			name:               "Invalid _sync",
			importBody:         map[string]interface{}{"_sync": true},
			expectReject:       true,
			expectedStatusCode: base.IntPtr(500), // Internal server error due to unmarshal error
		},
		{
			name:               "Valid _id",
			importBody:         map[string]interface{}{"_id": "documentid"},
			expectReject:       true,
			expectedStatusCode: base.IntPtr(http.StatusNotFound),
		},
		{
			name:         "Valid _rev",
			importBody:   map[string]interface{}{"_rev": "1-abc"},
			expectReject: true,
		},
		{
			name:         "Valid _deleted",
			importBody:   map[string]interface{}{"_deleted": false},
			expectReject: false,
		},
		{
			name:         "Valid _revisions",
			importBody:   map[string]interface{}{"_revisions": map[string]interface{}{"ids": "1-abc"}},
			expectReject: true,
		},
		{
			name:         "Valid _exp",
			importBody:   map[string]interface{}{"_exp": "123"},
			expectReject: true,
		},
		{
			name:         "Invalid _attachments",
			importBody:   map[string]interface{}{"_attachments": false},
			expectReject: false,
		},
		{
			name:         "Valid _attachments",
			importBody:   map[string]interface{}{"_attachments": map[string]interface{}{"attch": map[string]interface{}{"data": "c2d3IGZ0dw=="}}},
			expectReject: false,
		},
		{
			name:         "_purged false",
			importBody:   map[string]interface{}{"_purged": false},
			expectReject: true,
		},
		{
			name:               "_purged true",
			importBody:         map[string]interface{}{"_purged": true},
			expectReject:       true,
			expectedStatusCode: base.IntPtr(200), // Import gets cancelled and returns 200 and blank body
		},
		{
			name:               "_removed",
			importBody:         map[string]interface{}{"_removed": false},
			expectReject:       true,
			expectedStatusCode: base.IntPtr(404),
		},
		{
			name:         "_sync_cookies",
			importBody:   map[string]interface{}{"_sync_cookies": true},
			expectReject: true,
		},
		{
			name: "Valid user defined uppercase properties", // Uses internal properties names but in upper case
			// Known issue: _SYNC causes unmarshal error when importing document that contains it
			importBody: map[string]interface{}{
				"_ID": true, "_REV": true, "_DELETED": true, "_ATTACHMENTS": true, "_REVISIONS": true,
				"_EXP": true, "_PURGED": true, "_REMOVED": true, "_SYNC_COOKIES": true,
			},
			expectReject: false,
		},
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	for i, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			docID := fmt.Sprintf("test%d", i)
			added, err := rt.GetSingleDataStore().Add(docID, 0, test.importBody)
			require.True(t, added)
			require.NoError(t, err)

			// Perform on-demand import
			resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
			if test.expectReject {
				if test.expectedStatusCode != nil {
					assert.Equal(t, *test.expectedStatusCode, resp.Code)
				} else {
					assert.NotEqual(t, 200, resp.Code)
				}
				return
			}
			if test.expectedStatusCode != nil {
				rest.RequireStatus(rt.TB, resp, *test.expectedStatusCode)
			} else {
				rest.RequireStatus(rt.TB, resp, 200)
			}
			var body db.Body
			require.NoError(rt.TB, base.JSONUnmarshal(resp.Body.Bytes(), &body))

			for key, val := range body {
				assert.EqualValues(t, val, body[key])
			}
		})
	}
}

// Test import of an SDK expiry change
func TestImportTouch(t *testing.T) {
	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc, oldDoc) {
			if (oldDoc == null) {
				channel("oldDocNil")
			}
			if (doc._deleted) {
				channel("docDeleted")
			}
		}`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport: false,
		}},
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	// 1. Create a document
	key := "TestImportTouch"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestImportTouch"
	docBody["channels"] = "ABC"

	_, err := dataStore.Add(key, 0, docBody)
	require.NoError(t, err, "Unable to insert doc TestImportDelete")

	// Attempt to get the document via Sync Gateway, to trigger import.
	response := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", key), "")
	require.Equal(t, 200, response.Code)
	var rawInsertResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawInsertResponse)
	require.NoError(t, err, "Unable to unmarshal raw response")
	initialRev := rawInsertResponse.Sync.Rev

	// 2. Test import behaviour after SDK touch
	_, err = dataStore.Touch(key, 1000000)
	require.NoError(t, err, "Unable to touch doc TestImportTouch")

	// Attempt to get the document via Sync Gateway, to trigger import.
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", key), "")
	require.Equal(t, 200, response.Code)
	var rawUpdateResponse rest.RawResponse
	err = base.JSONUnmarshal(response.Body.Bytes(), &rawUpdateResponse)
	require.NoError(t, err, "Unable to unmarshal raw response")
	require.Equal(t, initialRev, rawUpdateResponse.Sync.Rev)
}
func TestImportingPurgedDocument(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("XATTR based tests not enabled.  Enable via SG_TEST_USE_XATTRS=true environment variable")
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	body := `{"_purged": true, "foo": "bar"}`
	ok, err := rt.GetSingleDataStore().Add("key", 0, []byte(body))
	assert.NoError(t, err)
	assert.True(t, ok)

	numErrors, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	response := rt.SendRequest("GET", "/{{.keyspace}}/key", "")
	fmt.Println(response.Body)

	numErrorsAfter, err := strconv.Atoi(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.String())
	assert.NoError(t, err)

	assert.Equal(t, numErrors, numErrorsAfter)
}

func TestNonImportedDuplicateID(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	body := `{"foo":"bar"}`
	ok, err := rt.GetSingleDataStore().Add("key", 0, []byte(body))

	assert.True(t, ok)
	assert.Nil(t, err)
	res := rt.SendAdminRequest("PUT", "/{{.keyspace}}/key", `{"prop":true}`)
	rest.RequireStatus(t, res, http.StatusConflict)
}

func TestImportOnWriteMigration(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Put doc with sync data / non-xattr
	key := "doc1"
	body := []byte(`{"_sync": { "rev": "1-fc2cf22c5e5007bd966869ebfe9e276a", "sequence": 1, "recent_sequences": [ 1 ], "history": { "revs": [ "1-fc2cf22c5e5007bd966869ebfe9e276a" ], "parents": [ -1], "channels": [ null ] }, "cas": "","value_crc32c": "", "time_saved": "2019-04-10T12:40:04.490083+01:00" }, "value": "foo"}`)
	ok, err := rt.GetSingleDataStore().Add(key, 0, body)
	assert.NoError(t, err)
	assert.True(t, ok)

	// Update doc with xattr - get 409, creates new rev, has old body
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev=1-fc2cf22c5e5007bd966869ebfe9e276a", `{"value":"new"}`)
	rest.RequireStatus(t, response, http.StatusCreated)

	// Update doc with xattr - successful update
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev=2-44ad6f128a2b1f75d0d0bb49b1fc0019", `{"value":"newer"}`)
	rest.RequireStatus(t, response, http.StatusCreated)
}

func TestImportFilterTimeout(t *testing.T) {
	importFilter := `function(doc) { while(true) { } }`

	rtConfig := rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{ImportFilter: &importFilter, AutoImport: false, JavascriptTimeoutSecs: base.Uint32Ptr(1)}}}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig) // use default collection since we are using default sync function
	defer rt.Close()

	added, err := rt.GetSingleDataStore().AddRaw("doc", 0, []byte(fmt.Sprintf(`{"foo": "bar"}`)))
	require.True(t, added)
	require.NoError(t, err)

	syncFnFinishedWG := sync.WaitGroup{}
	syncFnFinishedWG.Add(1)
	go func() {
		response := rt.SendAdminRequest("GET", "/db/doc", ``)
		rest.AssertStatus(t, response, 404)
		syncFnFinishedWG.Done()
	}()
	timeoutErr := rest.WaitWithTimeout(&syncFnFinishedWG, time.Second*15)
	assert.NoError(t, timeoutErr)
}

func TestImportRollback(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server - needs cbgt and import checkpointing")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyDCP)
	const (
		rollbackWithoutFailover = "rollbackWithoutFailover"
		rollbackWithFailover    = "rollbackWithFailoverLogs"
	)
	for _, testType := range []string{rollbackWithoutFailover, rollbackWithFailover} {
		t.Run(testType, func(t *testing.T) {
			ctx := base.TestCtx(t)
			bucket := base.GetTestBucket(t)
			defer bucket.Close(ctx)

			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				CustomTestBucket: bucket.NoCloseClone(),
				PersistentConfig: false,
			})

			key := "importRollbackTest"
			var checkpointPrefix string
			// Create a document
			added, err := rt.GetSingleDataStore().AddRaw(key, 0, []byte(fmt.Sprintf(`{"star": "6"}`)))
			require.True(t, added)
			require.NoError(t, err)

			// wait for doc to be imported
			changes, err := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			require.NoError(t, err)
			lastSeq, ok := changes.Last_Seq.(string)
			require.True(t, ok)

			// Close db while we mess with checkpoints
			db := rt.GetDatabase()
			checkpointPrefix = rt.GetDatabase().MetadataKeys.DCPCheckpointPrefix(db.Options.GroupID)

			rt.Close()

			metaStore := bucket.GetMetadataStore()
			for _, docName := range []string{base.CBGTCfgIndexDefs, base.CBGTCfgNodeDefsKnown, base.CBGTCfgNodeDefsWanted, base.CBGTCfgPlanPIndexes} {
				require.NoError(t, metaStore.Delete(docName), "Couldn't delete %s", docName)
			}

			// fetch the checkpoint for the document's vbucket, modify the checkpoint values to a higher sequence
			vbNo, err := base.GetVbucketForKey(bucket, key)
			require.NoError(t, err)
			checkpointKey := fmt.Sprintf("%s%d", checkpointPrefix, vbNo)
			var checkpointData base.ShardedImportDCPMetadata
			checkpointBytes, _, err := metaStore.GetRaw(checkpointKey)
			require.NoError(t, err)
			require.NoError(t, base.JSONUnmarshal(checkpointBytes, &checkpointData))

			checkpointData.SnapStart = 3000 + checkpointData.SnapStart
			checkpointData.SnapEnd = 3000 + checkpointData.SnapEnd
			checkpointData.SeqStart = 3000 + checkpointData.SeqStart
			checkpointData.SeqEnd = 3000 + checkpointData.SeqEnd
			if testType == rollbackWithFailover {
				existingVbUUID := checkpointData.FailOverLog[0][0]
				checkpointData.FailOverLog = [][]uint64{{existingVbUUID + 1, 0}}
			}
			updatedBytes, err := base.JSONMarshal(checkpointData)
			require.NoError(t, err)

			err = metaStore.SetRaw(checkpointKey, 0, nil, updatedBytes)
			require.NoError(t, err)
			// Reopen the db, expect DCP rollback
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				CustomTestBucket: bucket.NoCloseClone(),
				PersistentConfig: false,
			})
			defer rt2.Close()

			err = rt2.GetSingleDataStore().SetRaw(key, 0, nil, []byte(fmt.Sprintf(`{"star": "7 8 9"}`)))
			require.NoError(t, err)

			// wait for doc update to be imported
			_, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
			require.NoError(t, err)
		})
	}
}

func TestImportUpdateExpiry(t *testing.T) {
	testCases := []struct {
		name         string
		syncFn       string
		startExpiry  uint32
		assertion    func(t require.TestingT, expected, actual interface{}, msgAndArgs ...interface{})
		shouldBeZero bool
	}{
		{
			name:        "Decrease expiry",
			syncFn:      `function(doc, oldDoc, meta) { expiry(1000); }`,
			startExpiry: 2000,
			assertion:   require.Less,
		},
		{
			name:        "Increase expiry",
			syncFn:      `function(doc, oldDoc, meta) { expiry(2000); }`,
			startExpiry: 1000,
			assertion:   require.Greater,
		},
		{
			name:         "Unset TTL",
			syncFn:       `function(doc, oldDoc, meta) { expiry(0); }`,
			startExpiry:  2000,
			shouldBeZero: true,
		},
		{
			name:        "no modification to TTL",
			syncFn:      `function(doc, oldDoc, meta) { }`,
			startExpiry: 2000,
			assertion:   require.LessOrEqual, // in 6.0, we reset the expiry to a new offset so it can be slightly less than the original TTL. In 7.0 + this will be an exact match
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: test.syncFn,
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						AutoImport: false, // set AutoImport false to allow manually testing error conditions on import
					},
				},
			})
			defer rt.Close()

			const docID = "doc1"
			err := rt.GetSingleDataStore().SetRaw(docID, test.startExpiry, nil, []byte(`{"foo": "bar"}`))
			require.NoError(t, err)

			ctx := base.TestCtx(t)
			preImportExp, err := rt.GetSingleDataStore().GetExpiry(ctx, docID)
			require.NoError(t, err)
			// Attempt to get the document via Sync Gateway, to trigger import. Success is successfully importing the body and not throwing an assertion error.
			_ = rt.GetDocBody(docID)
			expiry, err := rt.GetSingleDataStore().GetExpiry(ctx, docID)
			require.NoError(t, err)
			if test.shouldBeZero {
				require.Equal(t, 0, int(expiry))
			} else {
				test.assertion(t, int(expiry), int(preImportExp))
			}
		})
	}
}
