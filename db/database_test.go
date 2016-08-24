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

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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
		BucketName: "sync_gateway_tests"}, nil)
	if err != nil {
		log.Fatalf("Couldn't connect to bucket: %v", err)
	}
	return bucket
}

func testLeakyBucket(config base.LeakyBucketConfig) base.Bucket {
	testBucket := testBucket()
	leakyBucket := base.NewLeakyBucket(testBucket, config)
	return leakyBucket
}

func setupTestDB(t *testing.T) *Database {
	return setupTestDBWithCacheOptions(t, CacheOptions{})
}

func setupTestDBForShadowing(t *testing.T) *Database {
	dbcOptions := DatabaseContextOptions{
		TrackDocs: true,
	}
	context, err := NewDatabaseContext("db", testBucket(), false, dbcOptions)
	assertNoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assertNoError(t, err, "Couldn't create database 'db'")
	return db
}

func setupTestDBWithCacheOptions(t *testing.T, options CacheOptions) *Database {

	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	context, err := NewDatabaseContext("db", testBucket(), false, dbcOptions)
	assertNoError(t, err, "Couldn't create context for database 'db'")
	db, err := CreateDatabase(context)
	assertNoError(t, err, "Couldn't create database 'db'")
	return db
}

func setupTestLeakyDBWithCacheOptions(t *testing.T, options CacheOptions, leakyOptions base.LeakyBucketConfig) *Database {
	dbcOptions := DatabaseContextOptions{
		CacheOptions: &options,
	}
	context, err := NewDatabaseContext("db", testLeakyBucket(leakyOptions), false, dbcOptions)
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

type AllDocsEntry struct {
	IDAndRev
	Channels []string
}

func (e AllDocsEntry) Equal(e2 AllDocsEntry) bool {
	return e.DocID == e2.DocID && e.RevID == e2.RevID && e.Sequence == e2.Sequence &&
		base.SetFromArray(e.Channels).Equals(base.SetFromArray(e2.Channels))
}

var options ForEachDocIDOptions

func allDocIDs(db *Database) (docs []AllDocsEntry, err error) {
	err = db.ForEachDocID(func(doc IDAndRev, channels []string) bool {
		docs = append(docs, AllDocsEntry{
			IDAndRev: doc,
			Channels: channels,
		})
		return true
	}, options)
	return
}

func TestAllDocs(t *testing.T) {
	// base.LogKeys["Cache"] = true
	// base.LogKeys["Changes"] = true
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Lower the log expiration time to zero so no more than 50 items will be kept.
	oldChannelCacheAge := DefaultChannelCacheAge
	DefaultChannelCacheAge = 0
	defer func() { DefaultChannelCacheAge = oldChannelCacheAge }()

	/*
		base.LogKeys["Changes"] = true
		base.LogKeys["Changes+"] = true
		defer func() {
			base.LogKeys["Changes"] = false
			base.LogKeys["Changes+"] = false
		}()
	*/

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	ids := make([]AllDocsEntry, 100)
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
		ids[i].Channels = channels
		assertNoError(t, err, "Couldn't create document")
	}

	alldocs, err := allDocIDs(db)
	assertNoError(t, err, "AllDocIDs failed")
	assert.Equals(t, len(alldocs), 100)
	for i, entry := range alldocs {
		assert.True(t, entry.Equal(ids[i]))
	}

	// Now delete one document and try again:
	_, err = db.DeleteDoc(ids[23].DocID, ids[23].RevID)
	assertNoError(t, err, "Couldn't delete doc 23")

	alldocs, err = allDocIDs(db)
	assertNoError(t, err, "AllDocIDs failed")
	assert.Equals(t, len(alldocs), 99)
	for i, entry := range alldocs {
		j := i
		if i >= 23 {
			j++
		}
		assert.True(t, entry.Equal(ids[j]))
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

// Unit test for bug #673
func TestUpdatePrincipal(t *testing.T) {

	var logKeys = map[string]bool{
		"Cache":    true,
		"Changes":  true,
		"Changes+": true,
	}

	base.UpdateLogKeys(logKeys, true)

	db := setupTestDB(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Validate that a call to UpdatePrincipals with no changes to the user doesn't allocate a sequence
	userInfo, err := db.GetPrincipal("naomi", true)
	userInfo.ExplicitChannels = base.SetOf("ABC")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "Unable to update principal")

	nextSeq, err := db.sequences.nextSequence()
	assert.Equals(t, nextSeq, uint64(1))

	// Validate that a call to UpdatePrincipals with changes to the user does allocate a sequence
	userInfo, err = db.GetPrincipal("naomi", true)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "Unable to update principal")

	nextSeq, err = db.sequences.nextSequence()
	assert.Equals(t, nextSeq, uint64(3))
}

func TestConflicts(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	/*
		var logKeys = map[string]bool {
			"Cache": true,
			"Changes": true,
			"Changes+": true,
		}

		base.UpdateLogKeys(logKeys, true)
	*/

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

	body := Body{"channels": []string{"bad,name"}}
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

	body = Body{"users": []string{"username"}, "userChannels": []string{"bad,name"}}
	_, err = db.Put("doc6", body)
	assertHTTPError(t, err, 500)
}

func TestAccessFunction(t *testing.T) {

	/*
		var logKeys = map[string]bool {
			"CRUD": true,
			"Access": true,
		}

		base.UpdateLogKeys(logKeys, true)
	*/

	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	authenticator := auth.NewAuthenticator(db.Bucket, db)

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)

	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("Netflix"))
	user.SetExplicitRoles(channels.TimedSet{"animefan": channels.NewVbSimpleSequence(1), "tumblr": channels.NewVbSimpleSequence(1)})
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
	expected := channels.AtSequence(channels.SetOf("Hulu", "Netflix", "!"), 1)
	assert.DeepEquals(t, user.Channels(), expected)

	expected.AddChannel("CrunchyRoll", 2)
	assert.DeepEquals(t, user.InheritedChannels(), expected)
}

func CouchbaseTestAccessFunctionWithVbuckets(t *testing.T) {
	//base.LogKeys["CRUD"] = true
	//base.LogKeys["Access"] = true

	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	db.SequenceType = ClockSequenceType

	authenticator := auth.NewAuthenticator(db.Bucket, db)

	var err error
	db.ChannelMapper = channels.NewChannelMapper(`function(doc){access(doc.users,doc.userChannels);}`)

	user, _ := authenticator.NewUser("bernard", "letmein", channels.SetOf("Netflix"))
	assertNoError(t, authenticator.Save(user), "Save")

	body := Body{"users": []string{"bernard"}, "userChannels": []string{"ABC"}}
	_, err = db.Put("doc1", body)
	assertNoError(t, err, "")
	time.Sleep(100 * time.Millisecond)

	user, err = authenticator.GetUser("bernard")
	assertNoError(t, err, "GetUser")
	expected := channels.TimedSetFromString("ABC:5.1,Netflix:1,!:1")
	assert.DeepEquals(t, user.Channels(), expected)

	body = Body{"users": []string{"bernard"}, "userChannels": []string{"NBC"}}
	_, err = db.Put("doc2", body)
	assertNoError(t, err, "")
	time.Sleep(100 * time.Millisecond)

	user, err = authenticator.GetUser("bernard")
	assertNoError(t, err, "GetUser")
	expected = channels.TimedSetFromString("ABC:5.1,NBC:12.1,Netflix:1,!:1")
	assert.DeepEquals(t, user.Channels(), expected)

	// Have another doc assign a new channel, and one of the previously present channels
	body = Body{"users": []string{"bernard"}, "userChannels": []string{"ABC", "PBS"}}
	_, err = db.Put("doc3", body)
	assertNoError(t, err, "")
	time.Sleep(100 * time.Millisecond)

	user, err = authenticator.GetUser("bernard")
	assertNoError(t, err, "GetUser")
	expected = channels.TimedSetFromString("ABC:5.1,NBC:12.1,PBS:11.1,Netflix:1,!:1")
	assert.DeepEquals(t, user.Channels(), expected)

}

func TestDocIDs(t *testing.T) {
	assert.Equals(t, realDocID(""), "")
	assert.Equals(t, realDocID("_"), "")
	assert.Equals(t, realDocID("_foo"), "")
	assert.Equals(t, realDocID("foo"), "foo")
	assert.Equals(t, realDocID("_design/foo"), "")
	assert.Equals(t, realDocID("_sync:rev:x"), "")
}

func TestUpdateDesignDoc(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	err := db.PutDesignDoc("official", DesignDoc{})
	assertNoError(t, err, "add design doc as admin")

	// Validate retrieval of the design doc by admin
	var result DesignDoc
	err = db.GetDesignDoc("official", &result)
	assertNoError(t, err, "retrieve design doc as admin")

	authenticator := auth.NewAuthenticator(db.Bucket, db)
	db.user, _ = authenticator.NewUser("naomi", "letmein", channels.SetOf("Netflix"))
	err = db.PutDesignDoc("_design/pwn3d", DesignDoc{})
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
	count, err := db.UpdateAllDocChannels(false, true)
	assertNoError(t, err, "ApplySyncFun")
	assert.Equals(t, count, 20)

	// Now they're visible:
	doc, err = db.GetDoc("alreadyHere1")
	base.Logf("doc = %+v", doc)
	assert.True(t, doc != nil)
	assertNoError(t, err, "can't get doc")
}

func TestPostWithExistingId(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{"_id": customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, err := db.Post(body)
	assert.True(t, rev1id != "")
	assert.True(t, docid == customDocId)
	assertNoError(t, err, "Couldn't create document")

	// Test retrieval
	doc, err := db.GetDoc(customDocId)
	assert.True(t, doc != nil)
	assertNoError(t, err, "Unable to retrieve doc using custom id")

	// Test that standard UUID creation still works:
	log.Printf("Create document with existing id...")
	body = Body{"notAnId": customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, err = db.Post(body)
	assert.True(t, rev1id != "")
	assert.True(t, docid != customDocId)
	assertNoError(t, err, "Couldn't create document")

	// Test retrieval
	doc, err = db.GetDoc(docid)
	assert.True(t, doc != nil)
	assertNoError(t, err, "Unable to retrieve doc using generated uuid")

}

// Unit test for issue #507
func TestPutWithUserSpecialProperty(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{"_id": customDocId, "key1": "value1", "_key2": "existing"}
	docid, rev1id, err := db.Post(body)
	assert.True(t, rev1id == "")
	assert.True(t, docid == "")
	assert.True(t, err.Error() == "400 user defined top level properties beginning with '_' are not allowed in document body")
}

// Unit test for issue #976
func TestWithNullPropertyKey(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Test creating a document with null property key
	customDocId := "customIdValue"
	log.Printf("Create document with empty property key")
	body := Body{"_id": customDocId, "": "value1"}
	docid, rev1id, _ := db.Post(body)
	assert.True(t, rev1id != "")
	assert.True(t, docid != "")
}

// Unit test for issue #507
func TestPostWithUserSpecialProperty(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Test creating a document with existing id property:
	customDocId := "customIdValue"
	log.Printf("Create document with existing id...")
	body := Body{"_id": customDocId, "key1": "value1", "key2": "existing"}
	docid, rev1id, err := db.Post(body)
	assert.True(t, rev1id != "")
	assert.True(t, docid == customDocId)
	assertNoError(t, err, "Couldn't create document")

	// Test retrieval
	doc, err := db.GetDoc(customDocId)
	assert.True(t, doc != nil)
	assertNoError(t, err, "Unable to retrieve doc using custom id")

	// Test that posting an update with a user special property does not update the
	//document
	log.Printf("Update document with existing id...")
	body = Body{"_id": customDocId, "_rev": rev1id, "_special": "value", "key1": "value1", "key2": "existing"}
	_, err = db.Put(docid, body)
	assert.True(t, err.Error() == "400 user defined top level properties beginning with '_' are not allowed in document body")

	// Test retrieval gets rev1
	doc, err = db.GetDoc(docid)
	assert.True(t, doc != nil)
	assert.True(t, doc.CurrentRev == rev1id)
	assertNoError(t, err, "Unable to retrieve doc using generated uuid")

}

func TestIncrRetrySuccess(t *testing.T) {
	leakyBucketConfig := base.LeakyBucketConfig{
		IncrTemporaryFailCount: 2,
	}
	leakyBucket := testLeakyBucket(leakyBucketConfig)
	defer leakyBucket.Close()
	seqAllocator, _ := newSequenceAllocator(leakyBucket)
	err := seqAllocator.reserveSequences(1)
	assert.True(t, err == nil)

}

func TestIncrRetryFail(t *testing.T) {
	leakyBucketConfig := base.LeakyBucketConfig{
		IncrTemporaryFailCount: 10,
	}
	leakyBucket := testLeakyBucket(leakyBucketConfig)
	defer leakyBucket.Close()
	seqAllocator, _ := newSequenceAllocator(leakyBucket)
	err := seqAllocator.reserveSequences(1)
	log.Printf("Got error: %v", err)
	assert.True(t, err != nil)

}

func TestRecentSequenceHistory(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	// Validate recent sequence is written
	body := Body{"val": "one"}
	revid, err := db.Put("doc1", body)

	assert.True(t, revid != "")
	doc, err := db.GetDoc("doc1")
	assert.True(t, err == nil)
	assert.DeepEquals(t, doc.RecentSequences, []uint64{1})

	// Add a few revisions - validate they are retained when less than max (20)
	for i := 0; i < 3; i++ {
		revid, err = db.Put("doc1", body)
	}

	doc, err = db.GetDoc("doc1")
	assert.True(t, err == nil)
	assert.DeepEquals(t, doc.RecentSequences, []uint64{1, 2, 3, 4})

	// Add many sequences to validate pruning when past max (20)
	for i := 0; i < kMaxRecentSequences; i++ {
		revid, err = db.Put("doc1", body)
		// Sleep needed to ensure consistent results when running single-threaded vs. multi-threaded test:
		// without it we can't predict the relative update times of nextSequence and RecentSequences
		time.Sleep(5 * time.Millisecond)
	}

	db.changeCache.waitForSequence(24)
	revid, err = db.Put("doc1", body)
	doc, err = db.GetDoc("doc1")
	assert.True(t, err == nil)
	log.Printf("recent:%d, max:%d", len(doc.RecentSequences), kMaxRecentSequences)
	assert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

	// Ensure pruning works when sequences aren't sequential
	doc2Body := Body{"val": "two"}
	for i := 0; i < kMaxRecentSequences; i++ {
		revid, err = db.Put("doc1", body)
		revid, err = db.Put("doc2", doc2Body)
	}

	db.changeCache.waitForSequence(64)
	revid, err = db.Put("doc1", body)
	doc, err = db.GetDoc("doc1")
	assert.True(t, err == nil)
	log.Printf("Recent sequences: %v (shouldn't exceed %v)", len(doc.RecentSequences), kMaxRecentSequences)
	assert.True(t, len(doc.RecentSequences) <= kMaxRecentSequences)

}

//////// BENCHMARKS

func BenchmarkDatabase(b *testing.B) {
	base.SetLogLevel(2) // disables logging
	for i := 0; i < b.N; i++ {
		bucket, _ := ConnectToBucket(base.BucketSpec{
			Server:     kTestURL,
			BucketName: fmt.Sprintf("b-%d", i)}, nil)
		context, _ := NewDatabaseContext("db", bucket, false, DatabaseContextOptions{})
		db, _ := CreateDatabase(context)

		body := Body{"key1": "value1", "key2": 1234}
		db.Put(fmt.Sprintf("doc%d", i), body)

		db.Close()
	}
}
