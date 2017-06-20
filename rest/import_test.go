package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
)

func SkipImportTestsIfNotEnabled(t *testing.T) {

	if !base.TestUseXattrs() {
		t.Skip("XATTR based tests not enabled.  Enable via SG_TEST_USE_XATTRS=true environment variable")
	}

	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus until https://github.com/couchbase/sync_gateway/issues/2390")
	}
}

type simpleSync struct {
	Channels map[string]interface{}
	Rev      string
}

type rawResponse struct {
	Sync simpleSync `json:"_sync"`
}

func hasActiveChannel(channelSet map[string]interface{}, channelName string) bool {
	if channelSet == nil {
		return false
	}
	value, ok := channelSet[channelName]
	if !ok || value != nil { // An entry for the channel name with a nil value represents an active channel
		return false
	}

	return true
}

// Test import of an SDK delete.
func TestXattrImportOldDoc(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) {
			if (oldDoc == null) {
				channel("oldDocNil")
			} 
			if (doc._deleted) {
				channel("docDeleted")
			}
		}`}
	defer rt.Close()

	bucket := rt.Bucket()
	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true}`)

	// 1. Test oldDoc behaviour during SDK insert
	key := "TestImportDelete"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestImportDelete"
	docBody["channels"] = "ABC"

	_, err := bucket.Add(key, 0, docBody)
	assertNoError(t, err, "Unable to insert doc TestImportDelete")

	// Attempt to get the document via Sync Gateway, to trigger import.  On import of a create, oldDoc should be nil.
	response := rt.SendAdminRequest("GET", "/db/_raw/TestImportDelete", "")
	assert.Equals(t, response.Code, 200)
	var rawInsertResponse rawResponse
	err = json.Unmarshal(response.Body.Bytes(), &rawInsertResponse)
	assertNoError(t, err, "Unable to unmarshal raw response")
	assertTrue(t, rawInsertResponse.Sync.Channels != nil, "Expected channels not returned for SDK insert")
	log.Printf("insert channels: %+v", rawInsertResponse.Sync.Channels)
	assertTrue(t, hasActiveChannel(rawInsertResponse.Sync.Channels, "oldDocNil"), "oldDoc was not nil during import of SDK insert")

	// 2. Test oldDoc behaviour during SDK update

	updatedBody := make(map[string]interface{})
	updatedBody["test"] = "TestImportDelete"
	updatedBody["channels"] = "HBO"

	err = bucket.Set(key, 0, updatedBody)
	assertNoError(t, err, "Unable to update doc TestImportDelete")

	// Attempt to get the document via Sync Gateway, to trigger import.  On import of a create, oldDoc should be nil.
	response = rt.SendAdminRequest("GET", "/db/_raw/TestImportDelete", "")
	assert.Equals(t, response.Code, 200)
	var rawUpdateResponse rawResponse
	err = json.Unmarshal(response.Body.Bytes(), &rawUpdateResponse)
	assertNoError(t, err, "Unable to unmarshal raw response")
	assertTrue(t, rawUpdateResponse.Sync.Channels != nil, "Expected channels not returned for SDK update")
	log.Printf("update channels: %+v", rawUpdateResponse.Sync.Channels)
	assertTrue(t, hasActiveChannel(rawUpdateResponse.Sync.Channels, "oldDocNil"), "oldDoc was not nil during import of SDK update")

	// 3. Test oldDoc behaviour during SDK delete
	err = bucket.Delete(key)
	assertNoError(t, err, "Unable to delete doc TestImportDelete")

	response = rt.SendAdminRequest("GET", "/db/_raw/TestImportDelete", "")
	assert.Equals(t, response.Code, 200)
	var rawDeleteResponse rawResponse
	err = json.Unmarshal(response.Body.Bytes(), &rawDeleteResponse)
	log.Printf("Post-delete: %s", response.Body.Bytes())
	assertNoError(t, err, "Unable to unmarshal raw response")
	assertTrue(t, hasActiveChannel(rawDeleteResponse.Sync.Channels, "oldDocNil"), "oldDoc was not nil during import of SDK delete")
	assertTrue(t, hasActiveChannel(rawDeleteResponse.Sync.Channels, "docDeleted"), "doc did not set _deleted:true for SDK delete")

}

// Attempt to delete then recreate a document through SG
func TestXattrResurrectViaSG(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	defer rt.Close()

	log.Printf("Starting get bucket....")

	rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

	// 1. Create and import doc
	key := "TestResurrectViaSG"
	docBody := make(map[string]interface{})
	docBody["test"] = key
	docBody["channels"] = "ABC"

	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", key), `{"channels":"ABC"}`)
	assert.Equals(t, response.Code, 201)
	log.Printf("insert response: %s", response.Body.Bytes())
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId := body["rev"].(string)

	// 2. Delete the doc through SG
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/%s?rev=%s", key, revId), "")
	assert.Equals(t, response.Code, 200)
	log.Printf("delete response: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId = body["rev"].(string)

	// 3. Recreate the doc through the SG (with different data)
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s?rev=%s", key, revId), `{"channels":"ABC"}`)
	assert.Equals(t, response.Code, 201)
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	assert.Equals(t, body["rev"], "3-70c113f08c6622cd87af68f4f60d12e3")

}

// Attempt to delete then recreate a document through the SDK
func TestXattrResurrectViaSDK(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	defer rt.Close()

	log.Printf("Starting get bucket....")

	bucket := rt.Bucket()
	log.Printf("Got bucket....'")

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true}`)

	// 1. Create and import doc
	key := "TestResurrectViaSDK"
	docBody := make(map[string]interface{})
	docBody["test"] = key
	docBody["channels"] = "ABC"

	_, err := bucket.Add(key, 0, docBody)
	assertNoError(t, err, "Unable to insert doc TestResurrectViaSDK")

	// Attempt to get the document via Sync Gateway, to trigger import.  On import of a create, oldDoc should be nil.
	rawPath := fmt.Sprintf("/db/_raw/%s", key)
	response := rt.SendAdminRequest("GET", rawPath, "")
	assert.Equals(t, response.Code, 200)
	var rawInsertResponse rawResponse
	err = json.Unmarshal(response.Body.Bytes(), &rawInsertResponse)
	assertNoError(t, err, "Unable to unmarshal raw response")

	// 2. Delete the doc through the SDK
	err = bucket.Delete(key)
	assertNoError(t, err, "Unable to delete doc TestResurrectViaSDK")

	response = rt.SendAdminRequest("GET", rawPath, "")
	assert.Equals(t, response.Code, 200)
	var rawDeleteResponse rawResponse
	err = json.Unmarshal(response.Body.Bytes(), &rawDeleteResponse)
	log.Printf("Post-delete: %s", response.Body.Bytes())
	assertNoError(t, err, "Unable to unmarshal raw response")

	// 3. Recreate the doc through the SDK (with different data)
	updatedBody := make(map[string]interface{})
	updatedBody["test"] = key
	updatedBody["channels"] = "HBO"

	err = bucket.Set(key, 0, updatedBody)
	assertNoError(t, err, "Unable to update doc TestResurrectViaSDK")

	// Attempt to get the document via Sync Gateway, to trigger import.
	response = rt.SendAdminRequest("GET", rawPath, "")
	assert.Equals(t, response.Code, 200)
	var rawUpdateResponse rawResponse
	err = json.Unmarshal(response.Body.Bytes(), &rawUpdateResponse)
	assertNoError(t, err, "Unable to unmarshal raw response")
	_, ok := rawUpdateResponse.Sync.Channels["HBO"]
	assertTrue(t, ok, "Didn't find expected channel (HBO) on resurrected doc")

}
