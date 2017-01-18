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
	"encoding/json"
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

func TestDocDeletionFromChannelCoalescedRemoved(t *testing.T) {
	base.EnableLogKey("*")
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel A
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("alice", "letmein", channels.SetOf("A"))
	authenticator.Save(user)

	// Create a doc on two channels (sequence 1):
	revid, _ := db.Put("alpha", Body{"channels": []string{"A", "B"}})
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	if changeCache, ok := db.changeCache.(*kvChangeIndex); ok {
		changeCache.reader.indexReadBucket.Dump()
	}
	db.user, _ = authenticator.GetUser("alice")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	time.Sleep(1000 * time.Millisecond)
	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{ // Seq 1, from A
		Seq:     SequenceID{Seq: 1},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": revid}}})
	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Get raw document from the bucket
	rv, _, _ := db.Bucket.GetRaw("alpha") // cas, err

	//Unmarshall into nested maps
	var x map[string]interface{}
	json.Unmarshal(rv, &x)
	assert.True(t, err == nil)

	sync := x["_sync"].(map[string]interface{})
	sync["sequence"] = 3
	sync["rev"] = "3-e99405a23fa102238fa8c3fd499b15bc"
	sync["recent_sequences"] = []uint64{1, 2, 3}

	cm := make(channels.ChannelMap)
	cm["A"] = &channels.ChannelRemoval{Seq: 2, RevID: "2-e99405a23fa102238fa8c3fd499b15bc"}
	sync["channels"] = cm

	history := sync["history"].(map[string]interface{})
	history["revs"] = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	history["parents"] = []int{-1, 0, 1}
	history["channels"] = []base.Set{base.SetOf("A", "B"), base.SetOf("B"), base.SetOf("B")}


	//Marshall back to JSON
	b, err := json.Marshal(x)

	// Update raw document in the bucket
	db.Bucket.SetRaw("alpha", 0, b)

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 3 and isn't stuck waiting for it.
	db.changeCache.waitForSequence(3)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	assertNoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 2},
		ID:      "alpha",
		Removed: base.SetOf("A"),
		allRemoved: true,
		Changes: []ChangeRev{{"rev": "2-e99405a23fa102238fa8c3fd499b15bc"}}})

	printChanges(changes)
}

func TestDocDeletionFromChannelCoalesced(t *testing.T) {
	db := setupTestDB(t)
	defer tearDownTestDB(t, db)

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel A
	authenticator := db.Authenticator()
	user, _ := authenticator.NewUser("alice", "letmein", channels.SetOf("A"))
	authenticator.Save(user)

	// Create a doc on two channels (sequence 1):
	revid, _ := db.Put("alpha", Body{"channels": []string{"A", "B"}})
	db.changeCache.waitForSequence(1)
	time.Sleep(100 * time.Millisecond)

	if changeCache, ok := db.changeCache.(*kvChangeIndex); ok {
		changeCache.reader.indexReadBucket.Dump()
	}
	db.user, _ = authenticator.GetUser("alice")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	time.Sleep(1000 * time.Millisecond)

	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{ // Seq 1, from A
		Seq:     SequenceID{Seq: 1},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": revid}}})
	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Get raw document from the bucket
	rv, _, _ := db.Bucket.GetRaw("alpha") // cas, err

	//Unmarshall into nested maps
	var x map[string]interface{}
	json.Unmarshal(rv, &x)

	assert.True(t, err == nil)

	sync := x["_sync"].(map[string]interface{})
	sync["sequence"] = 3
	sync["rev"] = "3-e99405a23fa102238fa8c3fd499b15bc"
	sync["recent_sequences"] = []uint64{1, 2, 3}

	history := sync["history"].(map[string]interface{})
	history["revs"] = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	history["parents"] = []int{-1, 0, 1}
	history["channels"] = []base.Set{base.SetOf("A", "B"), base.SetOf("A", "B"), base.SetOf("A", "B")}

	//Marshall back to JSON
	b, err := json.Marshal(x)

	// Update raw document in the bucket
	db.Bucket.SetRaw("alpha", 0, b)

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 3 (the modified document) and isn't stuck waiting for it.
	db.changeCache.waitForSequence(3)

	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	assertNoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 3},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": "3-e99405a23fa102238fa8c3fd499b15bc"}}})

	printChanges(changes)
}
