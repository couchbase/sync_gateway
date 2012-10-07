// database_test.go

package basecouch

import (
	"log"
	"testing"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/sdegutis/go.assert"
)

var gTestBucket *couchbase.Bucket

func init() {
	var err error
	gTestBucket, err = ConnectToBucket("http://localhost:8091", "default", "couchdb")
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
}

func TestDatabase(t *testing.T) {
	db, err := CreateDatabase(gTestBucket, "testdb")
	assertNoError(t, err, "Couldn't create database")
	defer func() {
		err = db.Delete()
		status, _ := ErrorAsHTTPStatus(err)
		if status != 200 && status != 404 {
			assertNoError(t, err, "Couldn't delete database")
		}
	}()

	// Test creating & updating a document:
    log.Printf("Create rev 1...")
	body := Body{"key1": "value1", "key2": 1234}
	revid, err := db.Put("doc1", body)
	assertNoError(t, err, "Couldn't create document")
	assert.Equals(t, revid, body["_rev"])
	assert.Equals(t, revid, "1-cb0c9a22be0e5a1b01084ec019defa81")

    log.Printf("Create rev 2...")
	body["key1"] = "new value"
	body["key2"] = float64(4321) // otherwise the DeepEquals call below fails
	revid, err = db.Put("doc1", body)
    body["_id"] = "doc1"
	assertNoError(t, err, "Couldn't update document")
	assert.Equals(t, revid, body["_rev"])
	assert.Equals(t, revid, "2-488724414d0ed6b398d6d2aeb228d797")

	// Retrieve the document:
    log.Printf("Retrieve doc...")
	gotbody, err := db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, body)

	gotbody, err = db.GetRev("doc1", revid, false)
	assertNoError(t, err, "Couldn't get document with rev")
	assert.DeepEquals(t, gotbody, body)

	gotbody, err = db.GetRev("doc1", "bogusrev", false)
    status,_ := ErrorAsHTTPStatus(err)
	assert.Equals(t, status, 404)
    
    // Test the _revisions property:
    log.Printf("Check _revisions...")
    gotbody, err = db.GetRev("doc1", revid, true)
    revisions := gotbody["_revisions"].(Body)
    assert.Equals(t, revisions["start"], 2)
    assert.DeepEquals(t, revisions["ids"],
                         []string{"488724414d0ed6b398d6d2aeb228d797",
                                  "cb0c9a22be0e5a1b01084ec019defa81"})

	// Test RevDiff:
	missing, possible, err := db.RevDiff("doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"2-488724414d0ed6b398d6d2aeb228d797"})
	assertNoError(t, err, "RevDiff failed")
	assert.True(t, missing == nil)
	assert.True(t, possible == nil)

	missing, possible, err = db.RevDiff("doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assertNoError(t, err, "RevDiff failed")
	assert.DeepEquals(t, missing, []string{"3-foo"})
	assert.DeepEquals(t, possible, []string{"2-488724414d0ed6b398d6d2aeb228d797"})

	missing, possible, err = db.RevDiff("nosuchdoc",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assertNoError(t, err, "RevDiff failed")
	assert.DeepEquals(t, missing, []string{"1-cb0c9a22be0e5a1b01084ec019defa81",
		"3-foo"})
	assert.True(t, possible == nil)

	// Test PutExistingRev:
	body["_rev"] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = float64(4444)
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		"1-cb0c9a22be0e5a1b01084ec019defa81"}
	err = db.PutExistingRev("doc1", body, history)
	assertNoError(t, err, "PutExistingRev failed")

	// Retrieve the document:
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, body)
}
