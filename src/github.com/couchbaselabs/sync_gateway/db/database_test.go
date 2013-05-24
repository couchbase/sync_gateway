//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"fmt"
	"log"
	"testing"

	"github.com/couchbaselabs/go.assert"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

//const kTestURL = "http://localhost:8091"
const kTestURL = "walrus:"

var gTestBucket base.Bucket

func init() {
	var err error
	gTestBucket, err = ConnectToBucket(kTestURL, "default", "sync_gateway_tests")
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
}

func setupTestDB(t *testing.T) *Database {
	context, err := NewDatabaseContext("db", gTestBucket)
	assertNoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assertNoError(t, err, "Couldn't create database 'db'")
	return db
}

func tearDownTestDB(t *testing.T, db *Database) {
	err := db.Delete()
	status, _ := base.ErrorAsHTTPStatus(err)
	if status != 200 && status != 404 {
		assertNoError(t, err, "Couldn't delete database 'db'")
	}
}

func assertHTTPError(t *testing.T, err error, status int) {
	httpErr, ok := err.(*base.HTTPError)
	if !ok {
		assert.Errorf(t, "assertHTTPError: Expected an HTTP %d; got error %T %v", status, err, err)
	} else {
		assert.Equals(t, httpErr.Status, 500)
	}
}

func TestDatabase(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

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

	gotbody, err = db.GetRev("doc1", revid, false, nil)
	assertNoError(t, err, "Couldn't get document with rev")
	assert.DeepEquals(t, gotbody, body)

	gotbody, err = db.GetRev("doc1", "bogusrev", false, nil)
	status, _ := base.ErrorAsHTTPStatus(err)
	assert.Equals(t, status, 404)

	// Test the _revisions property:
	log.Printf("Check _revisions...")
	gotbody, err = db.GetRev("doc1", revid, true, nil)
	revisions := gotbody["_revisions"].(Body)
	assert.Equals(t, revisions["start"], 2)
	assert.DeepEquals(t, revisions["ids"],
		[]string{"488724414d0ed6b398d6d2aeb228d797",
			"cb0c9a22be0e5a1b01084ec019defa81"})

	// Test RevDiff:
	log.Printf("Check RevDiff...")
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
	log.Printf("Check PutExistingRev...")
	body["_rev"] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = float64(4444)
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		"1-cb0c9a22be0e5a1b01084ec019defa81"}
	err = db.PutExistingRev("doc1", body, history)
	assertNoError(t, err, "PutExistingRev failed")

	// Retrieve the document:
	log.Printf("Check Get...")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, body)
}

func TestAllDocs(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Lower the log capacity to 50 to ensure the test will overflow, causing logs to be truncated,
	// so the changes feed will have to backfill from its view.
	oldMaxLogLength := MaxChangeLogLength
	MaxChangeLogLength = 50
	defer func() { MaxChangeLogLength = oldMaxLogLength }()

	base.LogKeys["Changes"] = true
	defer func() { base.LogKeys["Changes"] = false }()

	db.ChannelMapper, _ = channels.NewDefaultChannelMapper()

	ids := make([]IDAndRev, 100)
	for i := 0; i < 100; i++ {
		channels := []string{"all"}
		if i%10 == 0 {
			channels = append(channels, "KFJC")
		}
		body := Body{"serialnumber": i, "channels": channels}
		ids[i].DocID = fmt.Sprintf("alldoc-%02d", i)
		revid, err := db.Put(ids[i].DocID, body)
		ids[i].RevID = revid
		assertNoError(t, err, "Couldn't create document")
	}

	alldocs, err := db.AllDocIDs()
	assertNoError(t, err, "AllDocIDs failed")
	assert.Equals(t, len(alldocs), 100)
	for i, entry := range alldocs {
		assert.DeepEquals(t, entry, ids[i])
	}

	// Now delete one document and try again:
	_, err = db.DeleteDoc(ids[23].DocID, ids[23].RevID)
	assertNoError(t, err, "Couldn't delete doc 23")

	alldocs, err = db.AllDocIDs()
	assertNoError(t, err, "AllDocIDs failed")
	assert.Equals(t, len(alldocs), 99)
	for i, entry := range alldocs {
		j := i
		if i >= 23 {
			j++
		}
		assert.DeepEquals(t, entry, ids[j])
	}

	// Inspect the channel log to confirm that it's only got the last 50 sequences.
	// There are 101 sequences overall, so the 1st one it has should be #52.
	log, _ := db.GetChangeLog("all", 0)
	assert.Equals(t, log.Since, uint64(51))
	assert.Equals(t, len(log.Entries), 50)
	assert.Equals(t, int(log.Entries[0].Sequence), 52)

	// Now check the changes feed:
	var options ChangesOptions
	changes, err := db.GetChanges(channels.SetOf("all"), options)
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 100)
	for i, change := range changes {
		seq := i + 1
		if i >= 23 {
			seq++
		}
		assert.Equals(t, change.Seq, fmt.Sprintf("all:%d", seq))
		assert.Equals(t, change.Deleted, i == 99)
		var removed channels.Set
		if i == 99 {
			removed = channels.SetOf("all")
		}
		assert.DeepEquals(t, change.Removed, removed)
	}

	options.IncludeDocs = true
	changes, err = db.GetChanges(channels.SetOf("KFJC"), options)
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 10)
	for i, change := range changes {
		assert.Equals(t, change.Seq, fmt.Sprintf("KFJC:%d", 10*i+1))
		assert.Equals(t, change.ID, ids[10*i].DocID)
		assert.Equals(t, change.Deleted, false)
		assert.DeepEquals(t, change.Removed, channels.Set(nil))
		assert.Equals(t, change.Doc["serialnumber"], float64(10*i))
	}

	// Trying to add the existing log should fail with no error
	added, err := db.AddChangeLog("all", *log)
	assertNoError(t, err, "add channel log")
	assert.False(t, added)

	// Delete the channel log to test if it can be rebuilt:
	assertNoError(t, db.Bucket.Delete(channelLogDocID("all")), "delete channel log")

	// Get the changes feed; result should still be correct:
	changes, err = db.GetChanges(channels.SetOf("all"), options)
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 100)

	// Verify it was rebuilt
	log, err = db.GetChangeLog("all", 0)
	assertNoError(t, err, "GetChangeLog")
	assert.Equals(t, len(log.Entries), 50)
	assert.Equals(t, int(log.Entries[0].Sequence), 52)
}

func TestConflicts(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper, _ = channels.NewDefaultChannelMapper()

	base.LogKeys["CRUD"] = true
	base.LogKeys["Changes"] = true

	body := Body{"n": 1, "channels": []string{"all"}}
	assertNoError(t, db.PutExistingRev("doc", body, []string{"1-a"}), "add 1-a")

	log, _ := db.GetChangeLog("all", 0)
	assert.Equals(t, len(log.Entries), 1)
	assert.Equals(t, int(log.Since), 0)

	body["n"] = 2
	assertNoError(t, db.PutExistingRev("doc", body, []string{"2-b", "1-a"}), "add 2-b")
	body["n"] = 3
	assertNoError(t, db.PutExistingRev("doc", body, []string{"2-a", "1-a"}), "add 2-a")

	log, _ = db.GetChangeLog("all", 0)
	assert.Equals(t, len(log.Entries), 3)
	assert.Equals(t, int(log.Since), 0)
	assert.DeepEquals(t, log.Entries[0], channels.LogEntry{Sequence: 1})
	assert.DeepEquals(t, log.Entries[1], channels.LogEntry{Sequence: 2, DocID: "doc", RevID: "2-b"})
	assert.DeepEquals(t, log.Entries[2], channels.LogEntry{Sequence: 3, DocID: "doc", RevID: "2-a", Hidden: true})

	changes, err := db.GetChanges(channels.SetOf("all"), ChangesOptions{Conflicts: true})
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 2)
	// (CouchDB would merge these into one entry, but the gateway doesn't.)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     "all:2",
		ID:      "doc",
		Changes: []ChangeRev{{"rev": "2-b"}}})
	assert.DeepEquals(t, changes[1], &ChangeEntry{
		Seq:     "all:3",
		ID:      "doc",
		Changes: []ChangeRev{{"rev": "2-a"}}})

	changes, err = db.GetChanges(channels.SetOf("all"), ChangesOptions{Conflicts: false})
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     "all:2",
		ID:      "doc",
		Changes: []ChangeRev{{"rev": "2-b"}}})
}

func TestInvalidChannel(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	db.ChannelMapper, _ = channels.NewDefaultChannelMapper()

	body := Body{"channels": []string{"bad name"}}
	_, err := db.Put("doc", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionValidation(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	var err error
	db.ChannelMapper, err = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)
	assertNoError(t, err, "Couldn't create channel mapper")

	body := Body{"users": []string{"username"}, "userChannels": []string{"BBC1"}}
	_, err = db.Put("doc1", body)
	assertNoError(t, err, "")

	body = Body{"users": []string{"role:rolename"}, "userChannels": []string{"BBC1"}}
	_, err = db.Put("doc2", body)
	assertNoError(t, err, "")

	body = Body{"users": []string{"bad username"}, "userChannels": []string{"BBC1"}}
	_, err = db.Put("doc3", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"role:bad rolename"}, "userChannels": []string{"BBC1"}}
	_, err = db.Put("doc4", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"roll:over"}, "userChannels": []string{"BBC1"}}
	_, err = db.Put("doc5", body)
	assertHTTPError(t, err, 500)

	body = Body{"users": []string{"username"}, "userChannels": []string{"bad name"}}
	_, err = db.Put("doc6", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunction(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	authenticator := auth.NewAuthenticator(db.Bucket, db)

	var err error
	db.ChannelMapper, err = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)
	assertNoError(t, err, "Couldn't create channel mapper")

	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("Netflix"))
	user.SetRoleNames([]string{"animefan", "tumblr"})
	authenticator.Save(user)

	body := Body{"users": []string{"naomi"}, "userChannels": []string{"Hulu"}}
	_, err = db.Put("doc1", body)
	assertNoError(t, err, "")

	body = Body{"users": []string{"role:animefan"}, "userChannels": []string{"CrunchyRoll"}}
	_, err = db.Put("doc2", body)
	assertNoError(t, err, "")

	// Create the role _after_ creating the documents, to make sure the previously-indexed access
	// privileges are applied.
	role, _ := authenticator.NewRole("animefan", nil)
	authenticator.Save(role)

	user, _ = authenticator.GetUser("naomi")
	expected := channels.SetOf("Hulu", "Netflix").AtSequence(1)
	assert.DeepEquals(t, user.Channels(), expected)

	expected.AddChannel("CrunchyRoll", 2)
	assert.DeepEquals(t, user.InheritedChannels(), expected)
}

func TestDocIDs(t *testing.T) {
	var db *Database
	assert.Equals(t, db.realDocID(""), "")
	assert.Equals(t, db.realDocID("_"), "")
	assert.Equals(t, db.realDocID("_foo"), "")
	assert.Equals(t, db.realDocID("foo"), "foo")
	assert.Equals(t, db.realDocID("_design/foo"), "_design/foo")
}
