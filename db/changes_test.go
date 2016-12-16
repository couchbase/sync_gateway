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

func _testDocDeletionFromChannelCoalescedRemoved(t *testing.T, db *Database) {
	//a := rt.ServerContext().Database("db").Authenticator()

	db.ChannelMapper = channels.NewDefaultChannelMapper()

	// Create a user with access to channel ABC
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
	db.user, _ = authenticator.GetUser("naomi")
	changes, err := db.GetChanges(base.SetOf("*"), getZeroSequence(db))
	assertNoError(t, err, "Couldn't GetChanges")
	printChanges(changes)
	time.Sleep(1000 * time.Millisecond)
	assert.Equals(t, len(changes), 3)
	assert.DeepEquals(t, changes[0], &ChangeEntry{ // Seq 1, from A
		Seq:     SequenceID{Seq: 1},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": revid}}})
	assert.DeepEquals(t, changes[1], &ChangeEntry{ // Seq 1, from B backfill
		Seq:     SequenceID{Seq: 1, TriggeredBy: 2},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": revid}}})
	assert.DeepEquals(t, changes[2], &ChangeEntry{ // Seq 2, from A and B
		Seq:     SequenceID{Seq: 2},
		ID:      "_user/alice",
		Changes: []ChangeRev{}})
	lastSeq := getLastSeq(changes)
	lastSeq, _ = db.ParseSequenceID(lastSeq.String())

	// Get raw document from the bucket
	rv, _, _ := db.Bucket.GetRaw("alpha") // cas, err
	//log.Printf("Raw Doc rev1 looks like: %s", rv)
	var doc document
	err = json.Unmarshal(rv, &doc)
	assert.True(t, err == nil)

	doc.syncData.Sequence = 3
	doc.syncData.CurrentRev = "3-e99405a23fa102238fa8c3fd499b15bc"
	doc.syncData.RecentSequences = []uint64{1, 2, 3}

	historyJson, err := json.Marshal(doc.syncData.History)
	var rtl revTreeList
	err = json.Unmarshal(historyJson, &rtl)
	assert.True(t, err == nil)

	rtl.Revs = []string{revid, "2-e99405a23fa102238fa8c3fd499b15bc", "3-e99405a23fa102238fa8c3fd499b15bc"}
	rtl.Parents = []int{-1, 0, 1}

	historyJson, err = json.Marshal(rtl)
	err = json.Unmarshal(historyJson, &doc.syncData.History)

	cm := make(channels.ChannelMap)
	cm["A"] = &channels.ChannelRemoval{Seq: 2, RevID: "2-e99405a23fa102238fa8c3fd499b15bc"}
	doc.syncData.Channels = cm

	b, err := json.Marshal(doc)

	// Update raw document in the bucket
	db.Bucket.SetRaw("alpha", 0, b)

	// Check the _changes feed -- this is to make sure the changeCache properly received
	// sequence 2 (the user doc) and isn't stuck waiting for it.
	db.changeCache.waitForSequence(3)
	changes, err = db.GetChanges(base.SetOf("*"), ChangesOptions{Since: lastSeq})

	assertNoError(t, err, "Couldn't GetChanges (2nd)")

	assert.Equals(t, len(changes), 1)
	assert.DeepEquals(t, changes[0], &ChangeEntry{
		Seq:     SequenceID{Seq: 2},
		ID:      "alpha",
		Changes: []ChangeRev{{"rev": "2-e99405a23fa102238fa8c3fd499b15bc"}}})

	printChanges(changes)
}
