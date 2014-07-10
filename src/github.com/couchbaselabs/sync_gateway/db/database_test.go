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
	"time"

	"github.com/couchbaselabs/go.assert"

	"github.com/couchbaselabs/sync_gateway/auth"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
	"github.com/robertkrimen/otto/underscore"
)

//const kTestURL = "http://localhost:8091"
const kTestURL = "walrus:"

func init() {
	base.LogNoColor()
	//base.LogKeys["CRUD"] = true
	//base.LogKeys["CRUD+"] = true
	underscore.Disable() // It really slows down unit tests (by making otto.New take a lot longer)
}

func testBucket() base.Bucket {
	bucket, err := ConnectToBucket(base.BucketSpec{
		Server:     kTestURL,
		BucketName: "sync_gateway_tests"})
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	return bucket
}

func setupTestDB(t *testing.T) *Database {
	context, err := NewDatabaseContext("db", testBucket(), false)
	assertNoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assertNoError(t, err, "Couldn't create database 'db'")
	return db
}

func tearDownTestDB(t *testing.T, db *Database) {
	db.Close()
}

func assertHTTPError(t *testing.T, err error, status int) {
	httpErr, ok := err.(*base.HTTPError)
	if !ok {
		assert.Errorf(t, "assertHTTPError: Expected an HTTP %d; got error %T %v", status, err, err)
	} else {
		assert.Equals(t, httpErr.Status, status)
	}
}

func TestDatabase(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Test creating & updating a document:
	log.Printf("Create rev 1...")
	body := Body{"key1": "value1", "key2": 1234}
	rev1id, err := db.Put("doc1", body)
	assertNoError(t, err, "Couldn't create document")
	assert.Equals(t, rev1id, body["_rev"])
	assert.Equals(t, rev1id, "1-cb0c9a22be0e5a1b01084ec019defa81")

	log.Printf("Create rev 2...")
	body["key1"] = "new value"
	body["key2"] = int64(4321)
	rev2id, err := db.Put("doc1", body)
	body["_id"] = "doc1"
	assertNoError(t, err, "Couldn't update document")
	assert.Equals(t, rev2id, body["_rev"])
	assert.Equals(t, rev2id, "2-488724414d0ed6b398d6d2aeb228d797")

	// Retrieve the document:
	log.Printf("Retrieve doc...")
	gotbody, err := db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, body)

	log.Printf("Retrieve rev 1...")
	gotbody, err = db.GetRev("doc1", rev1id, false, nil)
	assertNoError(t, err, "Couldn't get document with rev 1")
	assert.DeepEquals(t, gotbody, Body{"key1": "value1", "key2": 1234, "_id": "doc1", "_rev": rev1id})

	log.Printf("Retrieve rev 2...")
	gotbody, err = db.GetRev("doc1", rev2id, false, nil)
	assertNoError(t, err, "Couldn't get document with rev")
	assert.DeepEquals(t, gotbody, body)

	gotbody, err = db.GetRev("doc1", "bogusrev", false, nil)
	status, _ := base.ErrorAsHTTPStatus(err)
	assert.Equals(t, status, 404)

	// Test the _revisions property:
	log.Printf("Check _revisions...")
	gotbody, err = db.GetRev("doc1", rev2id, true, nil)
	revisions := gotbody["_revisions"].(Body)
	assert.Equals(t, revisions["start"], 2)
	assert.DeepEquals(t, revisions["ids"],
		[]string{"488724414d0ed6b398d6d2aeb228d797",
			"cb0c9a22be0e5a1b01084ec019defa81"})

	// Test RevDiff:
	log.Printf("Check RevDiff...")
	missing, possible := db.RevDiff("doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"2-488724414d0ed6b398d6d2aeb228d797"})
	assert.True(t, missing == nil)
	assert.True(t, possible == nil)

	missing, possible = db.RevDiff("doc1",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assert.DeepEquals(t, missing, []string{"3-foo"})
	assert.DeepEquals(t, possible, []string{"2-488724414d0ed6b398d6d2aeb228d797"})

	missing, possible = db.RevDiff("nosuchdoc",
		[]string{"1-cb0c9a22be0e5a1b01084ec019defa81",
			"3-foo"})
	assert.DeepEquals(t, missing, []string{"1-cb0c9a22be0e5a1b01084ec019defa81",
		"3-foo"})
	assert.True(t, possible == nil)

	// Test PutExistingRev:
	log.Printf("Check PutExistingRev...")
	body["_rev"] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		"1-cb0c9a22be0e5a1b01084ec019defa81"}
	err = db.PutExistingRev("doc1", body, history)
	assertNoError(t, err, "PutExistingRev failed")

	// Retrieve the document:
	log.Printf("Check Get...")
	gotbody, err = db.Get("doc1")
	assertNoError(t, err, "Couldn't get document")
	assert.DeepEquals(t, gotbody, body)

	// Compact and check how many obsolete revs were deleted:
	revsDeleted, err := db.Compact()
	assertNoError(t, err, "Compact failed")
	assert.Equals(t, revsDeleted, 2)
}

func TestGetDeleted(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	body := Body{"key1": 1234}
	rev1id, err := db.Put("doc1", body)
	assertNoError(t, err, "Put")

	rev2id, err := db.DeleteDoc("doc1", rev1id)
	assertNoError(t, err, "DeleteDoc")

	// Get the deleted doc with its history; equivalent to GET with ?revs=true
	body, err = db.GetRev("doc1", rev2id, true, nil)
	assertNoError(t, err, "GetRev")
	expectedResult := Body{
		"_id":        "doc1",
		"_rev":       rev2id,
		"_deleted":   true,
		"_revisions": Body{"start": 2, "ids": []string{"bc6d97f6e97c0d034a34f8aac2bf8b44", "dfd5e19813767eeddd08270fc5f385cd"}},
	}
	assert.DeepEquals(t, body, expectedResult)

	// Try again but with a user who doesn't have access to this revision (see #179)
	authenticator := auth.NewAuthenticator(db.Bucket, db)
	db.user, err = authenticator.GetUser("")
	assertNoError(t, err, "GetUser")
	db.user.SetExplicitChannels(nil)

	body, err = db.GetRev("doc1", rev2id, true, nil)
	assertNoError(t, err, "GetRev")
	assert.DeepEquals(t, body, expectedResult)
}

func TestAllDocs(t *testing.T) {
	// base.LogKeys["Cache"] = true
	// base.LogKeys["Changes"] = true
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Lower the log expiration time to zero so no more than 50 items will be kept.
	oldChannelCacheAge := ChannelCacheAge
	ChannelCacheAge = 0
	defer func() { ChannelCacheAge = oldChannelCacheAge }()

	/*
		base.LogKeys["Changes"] = true
		base.LogKeys["Changes+"] = true
		defer func() {
			base.LogKeys["Changes"] = false
			base.LogKeys["Changes+"] = false
		}()
	*/

	db.ChannelMapper = channels.NewDefaultChannelMapper()

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
		ids[i].Sequence = uint64(i + 1)
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
	db.changeCache.waitForSequence(101)
	log := db.GetChangeLog("all", 0)
	assert.Equals(t, len(log), 50)
	assert.Equals(t, int(log[0].Sequence), 52)

	// Now check the changes feed:
	var options ChangesOptions
	options.Terminator = make(chan bool)
	defer close(options.Terminator)
	changes, err := db.GetChanges(channels.SetOf("all"), options)
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 100)
	for i, change := range changes {
		seq := i + 1
		if i >= 23 {
			seq++
		}
		assert.Equals(t, change.Seq, SequenceID{Seq: uint64(seq)})
		assert.Equals(t, change.Deleted, i == 99)
		var removed base.Set
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
		assert.Equals(t, change.Seq, SequenceID{Seq: uint64(10*i + 1)})
		assert.Equals(t, change.ID, ids[10*i].DocID)
		assert.Equals(t, change.Deleted, false)
		assert.DeepEquals(t, change.Removed, base.Set(nil))
		assert.Equals(t, change.Doc["serialnumber"], int64(10*i))
	}
}

// Unit test for bug #314
func TestChangesAfterChannelAdded(t *testing.T) {
	base.LogKeys["Cache"] = true
	base.LogKeys["Changes"] = true
	base.LogKeys["Changes+"] = true
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Create a doc on two channels (sequence 1):
	revid, _ := db.Put("doc1", Body{"channels": []string{"ABC", "PBS"}})

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")

	// Check the _changes feed:
	db.changeCache.waitForSequence(1)
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), ChangesOptions{Since: SequenceID{Seq: 1}})
	assertNoError(t, err, "Couldn't GetChanges")

	assert.Equals(t, len(changes), 2)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 1, TriggeredBy: 2},
		ID:      "doc1",
		Changes: []ChangeRev{{"rev": revid}}})
	assert.DeepEquals(t, changes[1], &ChangeEntry{
		Seq:     SequenceID{Seq: 2},
		ID:      "_user/naomi",
		Changes: []ChangeRev{}})

	// Add a new doc (sequence 3):
	revid, _ = db.Put("doc2", Body{"channels": []string{"PBS"}})

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 2 (the user doc) and isn't stuck waiting for it.
	db.changeCache.waitForSequence(3)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: SequenceID{Seq: 2}})
	assertNoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 3},
		ID:      "doc2",
		Changes: []ChangeRev{{"rev": revid}}})
}

func TestConflicts(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	//base.LogKeys["Cache"] = true
	// base.LogKeys["CRUD"] = true
	// base.LogKeys["Changes"] = true

	// Create rev 1 of "doc":
	body := Body{"n": 1, "channels": []string{"all", "1"}}
	assertNoError(t, db.PutExistingRev("doc", body, []string{"1-a"}), "add 1-a")

	time.Sleep(50 * time.Millisecond) // Wait for tap feed to catch up

	log := db.GetChangeLog("all", 0)
	assert.Equals(t, len(log), 1)

	// Create two conflicting changes:
	body["n"] = 2
	body["channels"] = []string{"all", "2b"}
	assertNoError(t, db.PutExistingRev("doc", body, []string{"2-b", "1-a"}), "add 2-b")
	body["n"] = 3
	body["channels"] = []string{"all", "2a"}
	assertNoError(t, db.PutExistingRev("doc", body, []string{"2-a", "1-a"}), "add 2-a")

	time.Sleep(50 * time.Millisecond) // Wait for tap feed to catch up

	// Verify the change with the higher revid won:
	gotBody, err := db.Get("doc")
	assert.DeepEquals(t, gotBody, Body{"_id": "doc", "_rev": "2-b", "n": int64(2),
		"channels": []interface{}{"all", "2b"}})

	// Verify we can still get the other two revisions:
	gotBody, err = db.GetRev("doc", "1-a", false, nil)
	assert.DeepEquals(t, gotBody, Body{"_id": "doc", "_rev": "1-a", "n": 1,
		"channels": []string{"all", "1"}})
	gotBody, err = db.GetRev("doc", "2-a", false, nil)
	assert.DeepEquals(t, gotBody, Body{"_id": "doc", "_rev": "2-a", "n": 3,
		"channels": []string{"all", "2a"}})

	// Verify the change-log of the "all" channel:
	db.changeCache.waitForSequence(3)
	log = db.GetChangeLog("all", 0)
	assert.Equals(t, len(log), 1)
	assert.Equals(t, log[0].Sequence, uint64(3))
	assert.Equals(t, log[0].DocID, "doc")
	assert.Equals(t, log[0].RevID, "2-b")
	assert.Equals(t, log[0].Flags, uint8(channels.Hidden|channels.Branched|channels.Conflict))

	// Verify the _changes feed:
	options := ChangesOptions{
		Conflicts: true,
	}
	changes, err := db.GetChanges(channels.SetOf("all"), options)
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:      SequenceID{Seq: 3},
		ID:       "doc",
		Changes:  []ChangeRev{{"rev": "2-b"}, {"rev": "2-a"}},
		branched: true})

	// Delete 2-b; verify this makes 2-a current:
	rev3, err := db.DeleteDoc("doc", "2-b")
	assertNoError(t, err, "delete 2-b")
	gotBody, err = db.Get("doc")
	assert.DeepEquals(t, gotBody, Body{"_id": "doc", "_rev": "2-a", "n": int64(3),
		"channels": []interface{}{"all", "2a"}})

	// Verify channel assignments are correct for channels defined by 2-a:
	doc, _ := db.GetDoc("doc")
	chan2a, found := doc.Channels["2a"]
	assert.True(t, found)
	assert.True(t, chan2a == nil)             // currently in 2a
	assert.True(t, doc.Channels["2b"] != nil) // has been removed from 2b

	// Verify the _changes feed:
	db.changeCache.waitForSequence(4)
	changes, err = db.GetChanges(channels.SetOf("all"), options)
	assertNoError(t, err, "Couldn't GetChanges")
	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:      SequenceID{Seq: 4},
		ID:       "doc",
		Changes:  []ChangeRev{{"rev": "2-a"}, {"rev": rev3}},
		branched: true})
}

func TestSyncFnOnPush(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		log("doc _id = "+doc._id+", _rev = "+doc._rev);
		if (oldDoc)
			log("oldDoc _id = "+oldDoc._id+", _rev = "+oldDoc._rev);
		channel(doc.channels);
	}`)

	// Create first revision:
	body := Body{"key1": "value1", "key2": 1234, "channels": []string{"public"}}
	rev1id, err := db.Put("doc1", body)
	assertNoError(t, err, "Couldn't create document")

	// Add several revisions at once to a doc, as on a push:
	log.Printf("Check PutExistingRev...")
	body["_rev"] = "4-four"
	body["key1"] = "fourth value"
	body["key2"] = int64(4444)
	body["channels"] = "clibup"
	history := []string{"4-four", "3-three", "2-488724414d0ed6b398d6d2aeb228d797",
		rev1id}
	err = db.PutExistingRev("doc1", body, history)
	assertNoError(t, err, "PutExistingRev failed")

	// Check that the doc has the correct channel (test for issue #300)
	doc, err := db.GetDoc("doc1")
	assert.DeepEquals(t, doc.Channels, channels.ChannelMap{
		"clibup": nil, // i.e. it is currently in this channel (no removal)
		"public": &channels.ChannelRemoval{Seq: 2, RevID: "4-four"},
	})
	assert.DeepEquals(t, doc.History["4-four"].Channels, base.SetOf("clibup"))
}

func TestInvalidChannel(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	body := Body{"channels": []string{"bad name"}}
	_, err := db.Put("doc", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunctionValidation(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)

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
	//base.LogKeys["CRUD"] = true
	//base.LogKeys["Access"] = true

	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	authenticator := auth.NewAuthenticator(db.Bucket, db)

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)

	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("Netflix"))
	user.SetExplicitRoles(channels.TimedSet{"animefan": 1, "tumblr": 1})
	assertNoError(t, authenticator.Save(user), "Save")

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

	user, err = authenticator.GetUser("naomi")
	assertNoError(t, err, "GetUser")
	expected := channels.AtSequence(channels.SetOf("Hulu", "Netflix"), 1)
	assert.DeepEquals(t, user.Channels(), expected)

	expected.AddChannel("CrunchyRoll", 2)
	assert.DeepEquals(t, user.InheritedChannels(), expected)
}

func TestDocIDs(t *testing.T) {
	assert.Equals(t, realDocID(""), "")
	assert.Equals(t, realDocID("_"), "")
	assert.Equals(t, realDocID("_foo"), "")
	assert.Equals(t, realDocID("foo"), "foo")
	assert.Equals(t, realDocID("_design/foo"), "_design/foo")
}

func TestUpdateDesignDoc(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	_, err := db.Put("_design/official", Body{})
	assertNoError(t, err, "add design doc as admin")

	authenticator := auth.NewAuthenticator(db.Bucket, db)
	db.user, _ = authenticator.NewUser("naomi", "letmein", channels.SetOf("Netflix"))
	_, err = db.Put("_design/pwn3d", Body{})
	assertHTTPError(t, err, 403)
}

func TestImport(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Add docs to the underlying bucket:
	for i := 1; i <= 20; i++ {
		db.Bucket.Add(fmt.Sprintf("alreadyHere%d", i), 0, Body{"key1": i, "key2": "hi"})
	}

	// Make sure they aren't visible thru the gateway:
	doc, err := db.GetDoc("alreadyHere1")
	assert.Equals(t, doc, (*document)(nil))
	assert.Equals(t, err.(*base.HTTPError).Status, 404)

	// Import them:
	err = db.ApplySyncFun("", true)
	assertNoError(t, err, "ApplySyncFun")

	// Now they're visible:
	doc, err = db.GetDoc("alreadyHere1")
	base.Log("doc = %+v", doc)
	assert.True(t, doc != nil)
	assertNoError(t, err, "can't get doc")
}

//////// BENCHMARKS

func BenchmarkDatabase(b *testing.B) {
	base.SetLogLevel(2) // disables logging
	for i := 0; i < b.N; i++ {
		bucket, _ := ConnectToBucket(base.BucketSpec{
			Server:     kTestURL,
			BucketName: fmt.Sprintf("b-%d", i)})
		context, _ := NewDatabaseContext("db", bucket, false)
		db, _ := CreateDatabase(context)

		body := Body{"key1": "value1", "key2": 1234}
		db.Put(fmt.Sprintf("doc%d", i), body)

		db.Close()
	}
}
