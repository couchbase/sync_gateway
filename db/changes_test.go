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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

// Unit test for bug #314
func TestChangesAfterChannelAdded(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)
	_testChangesAfterChannelAdded(t, db)
}

func printChanges(changes []*ChangeEntry) {
	for _, change := range changes {
		log.Printf("Change:%+v", change)
	}
}

func getLastSeq(changes []*ChangeEntry) SequenceID {
	if len(changes) > 0 {
		return changes[len(changes)-1].Seq
	}
	return SequenceID{}
}

func getZeroSequence(db *Database) ChangesOptions {
	if db.SequenceType == IntSequenceType {
		return ChangesOptions{Since: SequenceID{Seq: 0}}
	} else {
		return ChangesOptions{Since: SequenceID{Clock: base.NewSequenceClockImpl()}}
	}
}

func _testChangesAfterChannelAdded(t *testing.T, db *Database) {
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Hash+")
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Create a doc on two channels (sequence 1):
	revid, _ := db.Put("doc1", Body{"channels": []string{"ABC", "PBS"}})
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")

	// Check the _changes feed:
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)
	db.Bucket.Dump()
	if changeCache, ok := db.changeCache.(*kvChangeIndex); ok {
		changeCache.reader.indexReadBucket.Dump()
	}
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	time.Sleep(1000 * time.Millisecond)
	assert.Equals(t, len(changes), 3)
	assert.DeepEquals(t, changes[0], &ChangeEntry{ // Seq 1, from ABC
		Seq:     SequenceID{Seq: 1},
		ID:      "doc1",
		Changes: []ChangeRev{{"rev": revid}}})
	assert.DeepEquals(t, changes[1], &ChangeEntry{ // Seq 1, from PBS backfill
		Seq:     SequenceID{Seq: 1, TriggeredBy: 2},
		ID:      "doc1",
		Changes: []ChangeRev{{"rev": revid}}})
	assert.DeepEquals(t, changes[2], &ChangeEntry{ // Seq 2, from ABC and PBS
		Seq:     SequenceID{Seq: 2},
		ID:      "_user/naomi",
		Changes: []ChangeRev{}})
	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Add a new doc (sequence 3):
	revid, _ = db.Put("doc2", Body{"channels": []string{"PBS"}})

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 2 (the user doc) and isn't stuck waiting for it.
	db.changeCache.waitForSequence(3)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	assertNoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 3},
		ID:      "doc2",
		Changes: []ChangeRev{{"rev": revid}}})

	// validate from zero
	changes, err = db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
}

func TestIndexChangesAdminBackfill(t *testing.T) {
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Hash+")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	user.SetSequence(1)
	authenticator.Save(user)

	// Create docs on multiple channels:
	db.Put("both_1", Body{"channels": []string{"ABC", "PBS"}})
	db.Put("doc0000609", Body{"channels": []string{"PBS"}})
	db.Put("doc0000799", Body{"channels": []string{"ABC"}})
	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed:
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 3)

	// Modify user to have access to both channels:
	log.Println("Get Principal")
	userInfo, err := db.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")
	time.Sleep(100 * time.Millisecond)

	// Write a few more docs (that should be returned as non-backfill)
	db.Put("doc_nobackfill_1", Body{"channels": []string{"PBS"}})
	db.Put("doc_nobackfill_2", Body{"channels": []string{"PBS"}})
	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed:
	log.Println("Get User")
	db.user, _ = authenticator.GetUser("naomi")
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 5)
	verifyChange(t, changes, "both_1", true)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)

}

func TestIndexChangesRestartBackfill(t *testing.T) {
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Hash+")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	user.SetSequence(1)
	authenticator.Save(user)

	// Create docs on multiple channels:
	db.Put("both_1", Body{"channels": []string{"ABC", "PBS"}})
	db.Put("doc0000609", Body{"channels": []string{"PBS"}})
	db.Put("doc0000799", Body{"channels": []string{"ABC"}})
	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed:
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 3)

	// Modify user to have access to both channels:
	log.Println("Get Principal")
	userInfo, err := db.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")
	time.Sleep(100 * time.Millisecond)

	// Write a few more docs (that should be returned as non-backfill)
	db.Put("doc_nobackfill_1", Body{"channels": []string{"PBS"}})
	db.Put("doc_nobackfill_2", Body{"channels": []string{"PBS"}})
	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed:
	log.Println("Get User")
	db.user, _ = authenticator.GetUser("naomi")
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())
	log.Println("Get Changes")
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 5)
	verifyChange(t, changes, "both_1", true)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)

	// Request the changes from midway through backfill
	lastSeq, _ = db.ParseSequenceID("14-0:504.3")
	log.Println("Get Changes")
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 3)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)

}

func CouchbaseTestIndexChangesAccessBackfill(t *testing.T) {

	// Not walrus compatible, until we add support for meta.vb and meta.vbseq to walrus views.
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	db.ChannelMapper = channels.NewChannelMapper(`function(doc, oldDoc) {
		if (doc.accessGrant) {
			console.log("access grant to " + doc.accessGrant);
			access(doc.accessGrant, "PBS");
		}
		channel(doc.channels);
	}`)

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	authenticator.Save(user)

	// Create docs on multiple channels:
	_, err := db.Put("both_1", Body{"channels": []string{"ABC", "PBS"}})
	assertNoError(t, err, "Put failed")
	_, err = db.Put("doc0000609", Body{"channels": []string{"PBS"}})
	assertNoError(t, err, "Put failed")
	_, err = db.Put("doc0000799", Body{"channels": []string{"ABC"}})
	assertNoError(t, err, "Put failed")

	time.Sleep(2000 * time.Millisecond)

	// Check the _changes feed:
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 2)

	// Write a doc to grant user access to PBS:
	db.Put("doc_grant", Body{"accessGrant": "naomi"})
	time.Sleep(1000 * time.Millisecond)

	// Write a few more docs (that should be returned as non-backfill)
	db.Put("doc_nobackfill_1", Body{"channels": []string{"PBS"}})
	db.Put("doc_nobackfill_2", Body{"channels": []string{"PBS"}})
	time.Sleep(1000 * time.Millisecond)

	// Check the _changes feed:
	log.Println("Get User")
	db.user, _ = authenticator.GetUser("naomi")
	db.changeCache.waitForSequence(1)
	time.Sleep(1000 * time.Millisecond)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())
	log.Println("Get Changes")
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	assert.Equals(t, len(changes), 5)
	verifyChange(t, changes, "both_1", true)
	verifyChange(t, changes, "doc0000609", true)
	verifyChange(t, changes, "doc_nobackfill_1", false)
	verifyChange(t, changes, "doc_nobackfill_2", false)

}

func TestIndexChangesAfterChannelAdded(t *testing.T) {
	db := setupTestDBForChangeIndex(t)
	defer tearDownTestDB(t, db)
	base.EnableLogKey("IndexChanges")
	base.EnableLogKey("Hash+")
	base.EnableLogKey("Changes+")
	base.EnableLogKey("Backfill")
	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("naomi", "letmein", channels.SetOf("ABC"))
	user.SetSequence(1)
	authenticator.Save(user)

	// Create a doc on two channels (sequence 1):
	_, err := db.Put("doc1", Body{"channels": []string{"ABC", "PBS"}})
	assertNoError(t, err, "Put failed")
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	// Modify user to have access to both channels (sequence 2):
	userInfo, err := db.GetPrincipal("naomi", true)
	assert.True(t, userInfo != nil)
	userInfo.ExplicitChannels = base.SetOf("ABC", "PBS")
	_, err = db.UpdatePrincipal(*userInfo, true, true)
	assertNoError(t, err, "UpdatePrincipal failed")

	// Check the _changes feed:
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)
	db.Bucket.Dump()
	if changeCache, ok := db.changeCache.(*kvChangeIndex); ok {
		changeCache.reader.indexReadBucket.Dump()
	}
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	time.Sleep(250 * time.Millisecond)
	assert.Equals(t, len(changes), 2)
	verifyChange(t, changes, "_user/naomi", false)
	verifyChange(t, changes, "doc1", false)

	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Add a new doc (sequence 3):
	_, err = db.Put("doc2", Body{"channels": []string{"PBS"}})
	assertNoError(t, err, "Put failed")

	time.Sleep(100 * time.Millisecond)

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 2 (the user doc) and isn't stuck waiting for it.
	db.changeCache.waitForSequence(3)
	// changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: db.ParseSequenceID(lastSeq)})
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	printChanges(changes)
	assertNoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equals(t, len(changes), 1)

	verifyChange(t, changes, "doc2", false)

	// validate from zero
	log.Println("From zero:")
	//changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: SequenceID{Seq: 1, TriggeredBy: 2}})
	changes, err = db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
}

// Verifies that a doc is present in the ChangeEntry array, and whether it's sent as part of a backfill (based on triggeredBy value in the sequence)
func verifyChange(t *testing.T, changes []*ChangeEntry, docID string, isBackfill bool) {
	for _, change := range changes {
		if change.ID == docID {
			hasBackfill := change.Seq.TriggeredBy > 0
			if isBackfill != hasBackfill {
				assertFailed(t, fmt.Sprintf("VerifyChange failed for docID %s: expected backfill: %v, actual backfill: %v", docID, isBackfill, hasBackfill))
			}
			return
		}
	}
	assertFailed(t, fmt.Sprintf("VerifyChange failed - DocID %s not found in set", docID))
}
