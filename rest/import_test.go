package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

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

// Validate tombstone w/ xattrs
func TestXattrSGTombstone(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	defer rt.Close()

	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

	// 1. Create doc through SG
	key := "TestXattrSGTombstone"
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

	// 3. Attempt to retrieve the doc through the SDK
	deletedValue := make(map[string]interface{})
	_, err := bucket.Get(key, deletedValue)
	assertTrue(t, err != nil, "Expected key not found error trying to retrieve document")

}

// Test cas failure during WriteUpdate, triggering import of SDK write.
// Disabled, as test depends on artificial latency in PutDoc to reliably hit the CAS failure on the SG write.  Scenario fully covered
// by functional test.
func DisableTestXattrImportOnCasFailure(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{}
	defer rt.Close()

	bucket := rt.Bucket()
	rt.SendAdminRequest("PUT", "/_logging", `{"ImportCas":true}`)

	// 1. SG Write
	key := "TestCasFailureImport"
	docBody := make(map[string]interface{})
	docBody["test"] = "TestCasFailureImport"
	docBody["SG_write_count"] = "1"

	response := rt.SendAdminRequest("PUT", "/db/TestCasFailureImport", `{"test":"TestCasFailureImport", "write_type":"SG_1"}`)
	assert.Equals(t, response.Code, 201)
	log.Printf("insert response: %s", response.Body.Bytes())
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["rev"], "1-111c27be37c17f18ae8fe9faa3bb4e0e")
	revId := body["rev"].(string)

	// Attempt a second SG write, to be interrupted by an SDK update.  Should return a conflict
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s?rev=%s", key, revId), `{"write_type":"SG_2"}`)
		assert.Equals(t, response.Code, 409)
		log.Printf("SG CAS failure write response: %s", response.Body.Bytes())
		wg.Done()
	}()

	// Concurrent SDK writes for 10 seconds, one per second
	for i := 0; i < 10; i++ {
		time.Sleep(1 * time.Second)
		sdkBody := make(map[string]interface{})
		sdkBody["test"] = "TestCasFailureImport"
		sdkBody["SDK_write_count"] = i
		err := bucket.Set(key, 0, sdkBody)
		assertNoError(t, err, "Unexpected error doing SDK write")
	}

	// wait for SG write to happen
	wg.Wait()

	// Get to see where we ended up
	response = rt.SendAdminRequest("GET", "/db/TestCasFailureImport", "")
	assert.Equals(t, response.Code, 200)
	log.Printf("Final get: %s", response.Body.Bytes())

	// Get raw to see where the rev tree ended up
	response = rt.SendAdminRequest("GET", "/db/_raw/TestCasFailureImport", "")
	assert.Equals(t, response.Code, 200)
	log.Printf("Final get raw: %s", response.Body.Bytes())

}

// Attempt to delete then recreate a document through SG
func TestXattrResurrectViaSG(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	defer rt.Close()

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

	bucket := rt.Bucket()

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

// Attempt to delete a document that's already been deleted via the SDK
func TestXattrDoubleDelete(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	defer rt.Close()

	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

	// 1. Create and import doc
	key := "TestDoubleDelete"
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

	// 2. Delete the doc through the SDK
	log.Printf("...............Delete through SDK.....................................")
	deleteErr := bucket.Delete(key)
	assertNoError(t, deleteErr, "Couldn't delete via SDK")

	log.Printf("...............Delete through SG.......................................")

	// 3. Delete the doc through SG.  Expect a conflict, as the import of the SDK delete will create a new
	//    tombstone revision
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/%s?rev=%s", key, revId), "")
	assert.Equals(t, response.Code, 409)
	log.Printf("delete response: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId = body["rev"].(string)

}

func TestViewQueryTombstoneRetrieval(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	defer rt.Close()

	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true}`)

	// 1. Create and import docs
	key := "SG_delete"
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

	sdk_key := "SDK_delete"
	docBody = make(map[string]interface{})
	docBody["test"] = sdk_key
	docBody["channels"] = "ABC"

	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", sdk_key), `{"channels":"ABC"}`)
	assert.Equals(t, response.Code, 201)
	log.Printf("insert response: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)

	// 2. Delete SDK_delete through the SDK
	log.Printf("...............Delete through SDK.....................................")
	deleteErr := bucket.Delete(sdk_key)
	assertNoError(t, deleteErr, "Couldn't delete via SDK")

	// Trigger import via SG retrieval
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/%s", sdk_key), "")
	assert.Equals(t, response.Code, 404) // expect 404 deleted

	// 3.  Delete SG_delete through SG.
	log.Printf("...............Delete through SG.......................................")
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/%s?rev=%s", key, revId), "")
	assert.Equals(t, response.Code, 200)
	log.Printf("delete response: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId = body["rev"].(string)

	log.Printf("TestXattrDeleteDCPMutation done")

	// Attempt to retrieve via view.  Above operations were all synchronous (on-demand import of SDK delete, SG delete), so
	// stale=false view results should be immediately updated.
	results, err := rt.GetDatabase().ChannelViewTest("ABC", 1000, db.ChangesOptions{})
	assertNoError(t, err, "Error issuing channel view query")
	for _, entry := range results {
		log.Printf("Got view result: %v", entry)
	}
	assert.Equals(t, len(results), 2)
	assertTrue(t, strings.HasPrefix(results[0].RevID, "2-") && strings.HasPrefix(results[1].RevID, "2-"), "Unexpected revisions in view results post-delete")
}

func TestXattrImportFilterOptIn(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	importFilter := `function (doc) { return doc.type == "mobile"}`
	rt := RestTester{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DbConfig{
			ImportFilter: &importFilter,
		},
	}
	defer rt.Close()
	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

	// 1. Create two docs via the SDK, one matching filter
	mobileKey := "TestImportFilterValid"
	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"
	_, err := bucket.Add(mobileKey, 0, mobileBody)
	assertNoError(t, err, "Error writing SDK doc")

	nonMobileKey := "TestImportFilterInvalid"
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["type"] = "non-mobile"
	nonMobileBody["channels"] = "ABC"
	_, err = bucket.Add(nonMobileKey, 0, nonMobileBody)
	assertNoError(t, err, "Error writing SDK doc")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand import.
	response := rt.SendAdminRequest("GET", "/db/"+mobileKey, "")
	assert.Equals(t, response.Code, 200)
	assertDocProperty(t, response, "type", "mobile")

	response = rt.SendAdminRequest("GET", "/db/"+nonMobileKey, "")
	assert.Equals(t, response.Code, 404)
	assertDocProperty(t, response, "reason", "Not imported")

	// PUT to existing document that hasn't been imported.
	sgWriteBody := `{"type":"whatever I want - I'm writing through SG",
	                 "channels": "ABC"}`
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", nonMobileKey), sgWriteBody)
	assert.Equals(t, response.Code, 201)
	assertDocProperty(t, response, "id", "TestImportFilterInvalid")
	assertDocProperty(t, response, "rev", "1-25c26cdf9d7771e07f00be1d13f7fb7c")
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

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
	}
	defer rt.Close()
	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

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
	_, err := bucket.Add(key, 0, []byte(largeBodyString))
	assertNoError(t, err, "Error writing doc w/ large inline revisions")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
	response := rt.SendAdminRequest("GET", "/db/"+key, "")
	assert.Equals(t, response.Code, 200)

	// Get raw to retrieve metadata, and validate bodies have been moved to xattr
	rawResponse := rt.SendAdminRequest("GET", "/db/_raw/"+key, "")
	assert.Equals(t, rawResponse.Code, 200)
	var doc treeDoc
	err = json.Unmarshal(rawResponse.Body.Bytes(), &doc)
	assert.Equals(t, len(doc.Meta.RevTree.BodyKeyMap), 3)
	assert.Equals(t, len(doc.Meta.RevTree.BodyMap), 0)

}

// Test migration of a 1.4 doc that's been tombstoned
func TestMigrateTombstone(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
	}
	defer rt.Close()
	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

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
	_, err := bucket.Add(key, 0, []byte(bodyString))
	assertNoError(t, err, "Error writing tombstoned doc")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
	response := rt.SendAdminRequest("GET", "/db/"+key, "")
	assert.Equals(t, response.Code, 404)

	// Get raw to retrieve metadata, and validate bodies have been moved to xattr
	rawResponse := rt.SendAdminRequest("GET", "/db/_raw/"+key, "")
	assert.Equals(t, rawResponse.Code, 200)
	var doc treeDoc
	err = json.Unmarshal(rawResponse.Body.Bytes(), &doc)
	assert.Equals(t, doc.Meta.CurrentRev, "2-6b1e1af9190829c1ceab6f1c8fb9fa3f")
	assert.Equals(t, doc.Meta.Sequence, uint64(5))

}

// Test migration of a 1.5 doc that already includes some external revision storage from docmeta to xattr.
func TestMigrateWithExternalRevisions(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
	}
	defer rt.Close()
	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

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
	_, err := bucket.Add(key, 0, []byte(largeBodyString))
	assertNoError(t, err, "Error writing doc w/ large inline revisions")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
	response := rt.SendAdminRequest("GET", "/db/"+key, "")
	assert.Equals(t, response.Code, 200)

	// Get raw to retrieve metadata, and validate bodies have been moved to xattr
	rawResponse := rt.SendAdminRequest("GET", "/db/_raw/"+key, "")
	assert.Equals(t, rawResponse.Code, 200)
	var doc treeDoc
	err = json.Unmarshal(rawResponse.Body.Bytes(), &doc)
	assert.Equals(t, len(doc.Meta.RevTree.BodyKeyMap), 1)
	assert.Equals(t, len(doc.Meta.RevTree.BodyMap), 2)
}

// Write a doc via SDK with an expiry value.  Verify that expiry is preserved when doc is imported via DCP feed
func TestXattrFeedBasedImportPreservesExpiry(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DbConfig{
			ImportDocs: "continuous",
		},
	}
	defer rt.Close()
	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

	// 1. Create docs via the SDK with expiry set
	mobileKey := "TestXattrImportPreservesExpiry"
	mobileKeyNoExpiry := fmt.Sprintf("%s-noexpiry", mobileKey)
	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"

	// Write directly to bucket with an expiry
	expiryUnixEpoch := time.Now().Add(time.Second * 30).Unix()
	_, err := bucket.Add(mobileKey, uint32(expiryUnixEpoch), mobileBody)
	assertNoError(t, err, "Error writing SDK doc")

	// Negative test case -- no expiry
	_, err = bucket.Add(mobileKeyNoExpiry, 0, mobileBody)
	assertNoError(t, err, "Error writing SDK doc")

	// Wait until the change appears on the changes feed to ensure that it's been imported by this point
	changes, err := rt.WaitForChanges(2, "/db/_changes", "", true)
	assertNoError(t, err, "Error waiting for changes")

	log.Printf("changes: %+v", changes)
	changeEntry := changes.Results[0]
	assert.True(t, changeEntry.ID == mobileKey || changeEntry.ID == mobileKeyNoExpiry)

	// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
	assertXattrSyncMetaRevGeneration(t, bucket, mobileKey, 1)
	assertXattrSyncMetaRevGeneration(t, bucket, mobileKeyNoExpiry, 1)

	// Verify the expiry has been preserved after the import
	gocbBucket := bucket.(*base.CouchbaseBucketGoCB)
	expiry, err := gocbBucket.GetExpiry(mobileKey)
	assertNoError(t, err, "Error calling GetExpiry()")
	assert.True(t, expiry == uint32(expiryUnixEpoch))

	// Negative test case -- make sure no expiry was erroneously added by the the import
	expiry, err = gocbBucket.GetExpiry(mobileKeyNoExpiry)
	assertNoError(t, err, "Error calling GetExpiry()")
	assert.True(t, expiry == 0)

}

// Test migration of a 1.5 doc that has an expiry value.
func TestFeedBasedMigrateWithExpiry(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	rt := RestTester{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DbConfig{
			ImportDocs: "continuous",
		},
	}
	defer rt.Close()
	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

	// Write doc in SG format directly to the bucket
	key := "TestFeedBasedMigrateWithExpiry"

	// Create via the SDK with sync metadata intact
	expirySeconds := time.Second * 30
	testExpiry := time.Now().Add(expirySeconds)
	bodyString := rawDocWithSyncMeta()
	_, err := bucket.Add(key, uint32(testExpiry.Unix()), []byte(bodyString))
	assertNoError(t, err, "Error writing doc w/ expiry")

	// Wait for doc to appear on changes feed
	// Wait until the change appears on the changes feed to ensure that it's been imported by this point
	now := time.Now()
	changes, err := rt.WaitForChanges(1, "/db/_changes", "", true)
	assertNoError(t, err, "Error waiting for changes")
	changeEntry := changes.Results[0]
	assert.Equals(t, changeEntry.ID, key)
	log.Printf("Saw doc on changes feed after %v", time.Since(now))

	// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
	assertXattrSyncMetaRevGeneration(t, bucket, key, 1)

	// Now get the doc expiry and validate that it has been migrated into the doc metadata
	gocbBucket := bucket.(*base.CouchbaseBucketGoCB)
	expiry, err := gocbBucket.GetExpiry(key)
	assert.True(t, expiry > 0)
	assertNoError(t, err, "Error calling getExpiry()")
	log.Printf("expiry: %v", expiry)
	assert.True(t, expiry == uint32(testExpiry.Unix()))

}

// Write a doc via SDK with an expiry value.  Verify that expiry is preserved when doc is imported via on-demand
// import (GET or WRITE)
func TestXattrOnDemandImportPreservesExpiry(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	var rt RestTester

	mobileBody := make(map[string]interface{})
	mobileBody["type"] = "mobile"
	mobileBody["channels"] = "ABC"

	triggerOnDemandViaGet := func(key string) {
		rt.SendAdminRequest("GET", fmt.Sprintf("/db/%s", key), "")
	}
	triggerOnDemandViaWrite := func(key string) {
		mobileBody["foo"] = "bar"
		mobileBodyMarshalled, err := json.Marshal(mobileBody)
		assertNoError(t, err, "Error marshalling body")
		rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", key), string(mobileBodyMarshalled))
	}

	type testcase struct {
		onDemandCallback      func(string)
		name                  string
		expectedRevGeneration int
	}
	testCases := []testcase{
		{
			onDemandCallback: triggerOnDemandViaGet,
			name:             "triggerOnDemandViaGet",
			expectedRevGeneration: 1,
		},
		{
			onDemandCallback: triggerOnDemandViaWrite,
			name:             "triggerOnDemandViaWrite",
			expectedRevGeneration: 1,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s", testCase), func(t *testing.T) {

			rt = RestTester{
				SyncFn:         `function(doc, oldDoc) { channel(doc.channels) }`,
				DatabaseConfig: &DbConfig{},
			}
			defer rt.Close()
			bucket := rt.Bucket()

			rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

			key := fmt.Sprintf("TestXattrOnDemandImportPreservesExpiry-%d", i)

			// Write directly to bucket with an expiry
			expiryUnixEpoch := time.Now().Add(time.Second * 30).Unix()
			_, err := bucket.Add(key, uint32(expiryUnixEpoch), mobileBody)
			assertNoError(t, err, "Error writing SDK doc")

			// Verify the expiry before the on-demand import is triggered
			gocbBucket := bucket.(*base.CouchbaseBucketGoCB)
			expiry, err := gocbBucket.GetExpiry(key)
			assertNoError(t, err, "Error calling GetExpiry()")
			assert.True(t, expiry == uint32(expiryUnixEpoch))

			testCase.onDemandCallback(key)

			// Wait until the change appears on the changes feed to ensure that it's been imported by this point.
			// This is probably unnecessary in the case of on-demand imports, but it doesn't hurt to leave it in as a double check.
			changes, err := rt.WaitForChanges(1, "/db/_changes", "", true)
			assertNoError(t, err, "Error waiting for changes")
			changeEntry := changes.Results[0]
			assert.Equals(t, changeEntry.ID, key)

			// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
			assertXattrSyncMetaRevGeneration(t, bucket, key, testCase.expectedRevGeneration)

			// Verify the expiry has not been changed from the original expiry value
			expiry, err = gocbBucket.GetExpiry(key)
			assertNoError(t, err, "Error calling GetExpiry()")
			assert.True(t, expiry == uint32(expiryUnixEpoch))

		})
	}

}

// Write a doc via SDK with an expiry value.  Verify that expiry is preserved when doc is migrated via on-demand
// import (GET or WRITE)
func TestOnDemandMigrateWithExpiry(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	var rt RestTester

	triggerOnDemandViaGet := func(key string) {
		// Attempt to get the documents via Sync Gateway.  Will trigger on-demand migrate.
		response := rt.SendAdminRequest("GET", "/db/"+key, "")
		assert.Equals(t, response.Code, 200)
	}
	triggerOnDemandViaWrite := func(key string) {
		bodyString := rawDocWithSyncMeta()
		rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", key), bodyString)
	}

	type testcase struct {
		onDemandCallback      func(string)
		name                  string
		expectedRevGeneration int
	}
	testCases := []testcase{
		{
			onDemandCallback: triggerOnDemandViaGet,
			name:             "triggerOnDemandViaGet",
			expectedRevGeneration: 1,
		},
		{
			onDemandCallback: triggerOnDemandViaWrite,
			name:             "triggerOnDemandViaWrite",
			expectedRevGeneration: 2,
		},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprintf("%s", testCase.name), func(t *testing.T) {

			key := fmt.Sprintf("TestOnDemandGetWriteMigrateWithExpiry-%d", i)

			rt = RestTester{
				SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
			}
			defer rt.Close()
			bucket := rt.Bucket()

			rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

			// Create via the SDK with sync metadata intact
			expirySeconds := time.Second * 30
			syncMetaExpiry := time.Now().Add(expirySeconds)
			bodyString := rawDocWithSyncMeta()
			_, err := bucket.Add(key, uint32(syncMetaExpiry.Unix()), []byte(bodyString))
			assertNoError(t, err, "Error writing doc w/ expiry")

			testCase.onDemandCallback(key)

			// Double-check to make sure that it's been imported by checking the Sync Metadata in the xattr
			assertXattrSyncMetaRevGeneration(t, bucket, key, testCase.expectedRevGeneration)

			// Now get the doc expiry and validate that it has been migrated into the doc metadata
			gocbBucket := bucket.(*base.CouchbaseBucketGoCB)
			expiry, err := gocbBucket.GetExpiry(key)
			assertNoError(t, err, "Error calling GetExpiry()")
			assert.True(t, expiry > 0)
			log.Printf("expiry: %v", expiry)
			assert.True(t, expiry == uint32(syncMetaExpiry.Unix()))

		})
	}

}

// Write through SG, non-imported SDK write, subsequent SG write
func TestXattrSGWriteOfNonImportedDoc(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)

	importFilter := `function (doc) { return doc.type == "mobile"}`
	rt := RestTester{
		SyncFn: `function(doc, oldDoc) { channel(doc.channels) }`,
		DatabaseConfig: &DbConfig{
			ImportFilter: &importFilter,
		},
	}
	defer rt.Close()

	log.Printf("Starting get bucket....")

	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true}`)

	// 1. Create doc via SG
	sgWriteKey := "TestImportFilterSGWrite"
	sgWriteBody := `{"type":"whatever I want - I'm writing through SG",
	                 "channels": "ABC"}`
	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", sgWriteKey), sgWriteBody)
	assert.Equals(t, response.Code, 201)
	assertDocProperty(t, response, "rev", "1-25c26cdf9d7771e07f00be1d13f7fb7c")

	// 2. Update via SDK, not matching import filter.  Will not be available via SG
	nonMobileBody := make(map[string]interface{})
	nonMobileBody["type"] = "non-mobile"
	nonMobileBody["channels"] = "ABC"
	err := bucket.Set(sgWriteKey, 0, nonMobileBody)
	assertNoError(t, err, "Error updating SG doc from SDK ")

	// Attempt to get the documents via Sync Gateway.  Will trigger on-demand import.

	response = rt.SendAdminRequest("GET", "/db/"+sgWriteKey, "")
	assert.Equals(t, response.Code, 404)
	assertDocProperty(t, response, "reason", "Not imported")

	// 3. Rewrite through SG - should treat as new insert
	sgWriteBody = `{"type":"SG client rewrite",
	                 "channels": "NBC"}`
	response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/%s", sgWriteKey), sgWriteBody)
	assert.Equals(t, response.Code, 201)
	// Validate rev is 1, new rev id
	assertDocProperty(t, response, "rev", "1-b9cdd5d413b572476799930f065657a6")
}

// Test to write a binary document to the bucket, ensure it's not imported.
func TestImportBinaryDoc(t *testing.T) {

	rt := RestTester{SyncFn: `
		function(doc, oldDoc) { channel(doc.channels) }`}
	defer rt.Close()

	log.Printf("Starting get bucket....")

	bucket := rt.Bucket()

	rt.SendAdminRequest("PUT", "/_logging", `{"Import+":true, "CRUD+":true, "Cache+":true}`)

	// 1. Write a binary doc through the SDK
	rawBytes := []byte("some bytes")
	err := bucket.SetRaw("binaryDoc", 0, rawBytes)
	assertNoError(t, err, "Error writing binary doc through the SDK")

	// 2. Ensure we can't retrieve the document via SG
	response := rt.SendAdminRequest("GET", "/db/binaryDoc", "")
	assert.True(t, response.Code != 200)
}

// Test DCP backfill stats
func TestDcpBackfill(t *testing.T) {

	SkipImportTestsIfNotEnabled(t)
	base.EnableLogKey("DCP")

	rt := RestTester{}

	log.Printf("Starting get bucket....")

	bucket := rt.Bucket()

	// Write enough documents directly to the bucket to ensure multiple docs per vbucket (on average)
	docBody := make(map[string]interface{})
	docBody["type"] = "sdk_write"
	for i := 0; i < 2500; i++ {
		err := bucket.Set(fmt.Sprintf("doc_%d", i), 0, docBody)
		assertNoError(t, err, fmt.Sprintf("error setting doc_%d", i))
	}

	// Close the previous test context
	rt.Close()

	log.Print("Creating new database context")

	// Create a new context, with import docs enabled, to process backfill
	newRt := RestTester{
		DatabaseConfig: &DbConfig{
			ImportDocs: "continuous",
		},
		NoFlush: true,
	}
	log.Printf("Poke the rest tester so it starts DCP processing:")
	bucket = newRt.Bucket()

	backfillComplete := false
	var expectedBackfill, completedBackfill int
	for i := 0; i < 10; i++ {
		expectedBackfill, _ = base.GetExpvarAsInt("syncGateway_dcp", "backfill_expected")
		completedBackfill, _ = base.GetExpvarAsInt("syncGateway_dcp", "backfill_completed")
		if expectedBackfill > 0 && completedBackfill >= expectedBackfill {
			log.Printf("backfill complete: %d/%d", completedBackfill, expectedBackfill)
			backfillComplete = true
			break
		} else {
			log.Printf("backfill still in progress: %d/%d", completedBackfill, expectedBackfill)
			time.Sleep(1 * time.Second)
		}
	}
	assertTrue(t, backfillComplete, fmt.Sprintf("Backfill didn't complete after 20s. Latest: %d/%d", completedBackfill, expectedBackfill))

	log.Printf("done...%s  (%d/%d)", newRt.ServerContext().Database("db").Name, completedBackfill, expectedBackfill)

}

func assertDocProperty(t *testing.T, getDocResponse *TestResponse, propertyName string, expectedPropertyValue interface{}) {
	var responseBody map[string]interface{}
	err := json.Unmarshal(getDocResponse.Body.Bytes(), &responseBody)
	assertNoError(t, err, "Error unmarshalling document response")
	value, ok := responseBody[propertyName]
	assertTrue(t, ok, fmt.Sprintf("Expected property %s not found in response %s", propertyName, getDocResponse.Body.Bytes()))
	assert.Equals(t, value, expectedPropertyValue)
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

func assertXattrSyncMetaRevGeneration(t *testing.T, bucket base.Bucket, key string, expectedRevGeneration int) {
	xattr := map[string]interface{}{}
	_, err := bucket.GetWithXattr(key, "_sync", nil, &xattr)
	assertNoError(t, err, "Error Getting Xattr")
	revision, ok := xattr["rev"]
	assert.True(t, ok)
	generation, _ := db.ParseRevID(revision.(string))
	log.Printf("assertXattrSyncMetaRevGeneration generation: %d rev: %s", generation, revision)
	assert.True(t, generation == expectedRevGeneration)
}
