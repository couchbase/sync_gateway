//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"

	"bytes"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

type indexTester struct {
	restTester
	_indexBucket base.Bucket
}

func initRestTester(sequenceType db.SequenceType, syncFn string) indexTester {
	if sequenceType == db.ClockSequenceType {
		return initIndexTester(true, syncFn)
	} else {
		return initIndexTester(false, syncFn)
	}
}

func initIndexTester(useBucketIndex bool, syncFn string) indexTester {

	it := indexTester{}
	it.syncFn = syncFn

	it._sc = NewServerContext(&ServerConfig{
		Facebook: &FacebookConfig{},
	})

	var syncFnPtr *string
	if len(it.syncFn) > 0 {
		syncFnPtr = &it.syncFn
	}

	serverName := "walrus:"
	//serverName := "http://localhost:8091"
	bucketName := "sg_bucket"
	indexBucketName := "sg_index_bucket"

	feedType := "tap"
	if useBucketIndex {
		feedType = "dcp"
	}

	dbConfig := &DbConfig{
		BucketConfig: BucketConfig{
			Server: &serverName,
			Bucket: &bucketName},
		Name:     "db",
		Sync:     syncFnPtr,
		FeedType: feedType,
	}

	if useBucketIndex {
		channelIndexConfig := &ChannelIndexConfig{
			BucketConfig: BucketConfig{
				Server: &serverName,
				Bucket: &indexBucketName,
			},
			IndexWriter: true,
		}
		dbConfig.ChannelIndex = channelIndexConfig
	}

	dbContext, err := it._sc.AddDatabaseFromConfig(dbConfig)

	if useBucketIndex {
		_, err := base.SeedTestPartitionMap(dbContext.GetIndexBucket(), 64)
		if err != nil {
			panic(fmt.Sprintf("Error from seed partition map: %v", err))
		}
	}

	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}

	it._bucket = it._sc.Database("db").Bucket

	if useBucketIndex {
		it._indexBucket = it._sc.Database("db").GetIndexBucket()
	}

	return it
}

func (it *indexTester) Close() {
	it._sc.Close()
}

func (it *indexTester) ServerContext() *ServerContext {
	return it._sc
}

func TestDocDeletionFromChannel(t *testing.T) {
	// See https://github.com/couchbase/couchbase-lite-ios/issues/59
	// base.LogKeys["Changes"] = true
	// base.LogKeys["Cache"] = true

	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`}
	a := rt.ServerContext().Database("db").Authenticator()

	// Create user:
	alice, _ := a.NewUser("alice", "letmein", channels.SetOf("zero"))
	a.Save(alice)

	// Create a doc Alice can see:
	response := rt.send(request("PUT", "/db/alpha", `{"channel":"zero"}`))

	// Check the _changes feed:
	rt.ServerContext().Database("db").WaitForPendingChanges()
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))

	assert.Equals(t, changes.Results[0].ID, "alpha")
	rev1 := changes.Results[0].Changes[0]["rev"]

	// Delete the document:
	assertStatus(t, rt.send(request("DELETE", "/db/alpha?rev="+rev1, "")), 200)

	// Get the updates from the _changes feed:
	rt.waitForPendingChanges()
	response = rt.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", since),
		"", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)

	assert.Equals(t, changes.Results[0].ID, "alpha")
	assert.Equals(t, changes.Results[0].Deleted, true)
	assert.DeepEquals(t, changes.Results[0].Removed, base.SetOf("zero"))
	rev2 := changes.Results[0].Changes[0]["rev"]

	// Now get the deleted revision:
	response = rt.send(requestByUser("GET", "/db/alpha?rev="+rev2, "", "alice"))
	assert.Equals(t, response.Code, 200)
	log.Printf("Deletion looks like: %s", response.Body.Bytes())
	var docBody db.Body
	json.Unmarshal(response.Body.Bytes(), &docBody)
	assert.DeepEquals(t, docBody, db.Body{"_id": "alpha", "_rev": rev2, "_deleted": true})

	// Access without deletion revID shouldn't be allowed (since doc is not in Alice's channels):
	response = rt.send(requestByUser("GET", "/db/alpha", "", "alice"))
	assert.Equals(t, response.Code, 403)

	// A bogus rev ID should return a 404:
	response = rt.send(requestByUser("GET", "/db/alpha?rev=bogus", "", "alice"))
	assert.Equals(t, response.Code, 404)

	// Get the old revision, which should still be accessible:
	response = rt.send(requestByUser("GET", "/db/alpha?rev="+rev1, "", "alice"))
	assert.Equals(t, response.Code, 200)
}


// The form in which a RevTree is stored in JSON. For space-efficiency it's stored as an array of
// rev IDs, with a parallel array of parent indexes. Ordering in the arrays doesn't matter.
// So the parent of Revs[i] is Revs[Parents[i]] (unless Parents[i] == -1, which denotes a root.)
// This is used to directly manipulate the doc _sync data in unit tests
type tRevTreeList struct {
	Revs       []string          `json:"revs"`              // The revision IDs
	Parents    []int             `json:"parents"`           // Index of parent of each revision (-1 if root)
	Deleted    []int             `json:"deleted,omitempty"` // Indexes of revisions that are deletions
	Bodies_Old []string          `json:"bodies,omitempty"`  // JSON of each revision (legacy)
	BodyMap    map[string]string `json:"bodymap,omitempty"` // JSON of each revision
	Channels   []base.Set        `json:"channels"`
}

// The sync-gateway metadata stored in the "_sync" property of a Couchbase document.
type tSyncData struct {
	CurrentRev      string              `json:"rev"`
	NewestRev       string              `json:"new_rev,omitempty"` // Newest rev, if different from CurrentRev
	Flags           uint8               `json:"flags,omitempty"`
	Sequence        uint64              `json:"sequence,omitempty"`
	UnusedSequences []uint64            `json:"unused_sequences,omitempty"` // unused due to update conflicts
	RecentSequences []uint64            `json:"recent_sequences,omitempty"` // recent sequences for this doc - used in server dedup handling
	History         tRevTreeList        `json:"history"`
	Channels        channels.ChannelMap `json:"channels,omitempty"`
	Access          db.UserAccessMap    `json:"access,omitempty"`
	RoleAccess      db.UserAccessMap    `json:"role_access,omitempty"`
	Expiry          *time.Time          `json:"exp,omitempty"` // Document expiry.  Information only - actual expiry/delete handling is done by bucket storage.  Needs to be pointer for omitempty to work (see https://github.com/golang/go/issues/4357)

	// Fields used by bucket-shadowing:
	UpstreamCAS *uint64 `json:"upstream_cas,omitempty"` // CAS value of remote doc
	UpstreamRev string  `json:"upstream_rev,omitempty"` // Rev ID remote doc was saved as

	// Only used for performance metrics:
	TimeSaved time.Time `json:"time_saved,omitempty"` // Timestamp of save.

	// Backward compatibility (the "deleted" field was, um, deleted in commit 4194f81, 2/17/14)
	Deleted_OLD bool `json:"deleted,omitempty"`
}

type tDoc struct {
	Sync    tSyncData `json:"_sync"`
	Channel string    `json:"channel"`
}

func TestDocDeletionFromChannelCoalesced(t *testing.T) {
	rt := restTester{syncFn: `function(doc) {channel(doc.channels)}`}
	a := rt.ServerContext().Database("db").Authenticator()

	// Create user:
	alice, _ := a.NewUser("alice", "letmein", channels.SetOf("A"))
	a.Save(alice)

	// Create a doc Alice can see:
	response := rt.send(request("PUT", "/db/alpha", `{"channels":["A","B"]}`))
	// Check the _changes feed:
	rt.ServerContext().Database("db").WaitForPendingChanges()
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))

	assert.Equals(t, changes.Results[0].ID, "alpha")
	rev1 := changes.Results[0].Changes[0]["rev"]

	// Get raw document from the bucket
	rv, _, _ := rt._bucket.GetRaw("alpha") // cas, err
	var doc tDoc
	err := json.Unmarshal(rv, &doc)
	assert.True(t, err == nil)

	doc.Sync.CurrentRev = "3-e99405a23fa102238fa8c3fd499b15bc"
	doc.Sync.RecentSequences = []uint64{1, 2, 3}

	doc.Sync.History.Revs = []string{rev1, "2-2f9f49cf41ef37e98f9d74819361a378", "3-e99405a23fa102238fa8c3fd499b15bc"}
	doc.Sync.History.Parents = []int{-1, 0, 1}

	b, err := json.Marshal(doc)

	// Update raw document in the bucket
	rt._bucket.SetRaw("alpha", 0, b)

	// Get the updates from the _changes feed:
	rt.waitForPendingChanges()
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "alpha")
}

func TestDocDeletionFromChannelCoalescedRemoved(t *testing.T) {
	rt := restTester{syncFn: `function(doc) {channel(doc.channels)}`}
	a := rt.ServerContext().Database("db").Authenticator()

	// Create user:
	alice, _ := a.NewUser("alice", "letmein", channels.SetOf("A"))
	a.Save(alice)

	// Create a doc Alice can see:
	response := rt.send(request("PUT", "/db/alpha", `{"channels":["A","B"]}`))

	// Check the _changes feed:
	rt.ServerContext().Database("db").WaitForPendingChanges()
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))

	assert.Equals(t, changes.Results[0].ID, "alpha")
	rev1 := changes.Results[0].Changes[0]["rev"]

	// Get raw document from the bucket
	rv, _, _ := rt._bucket.GetRaw("alpha") // cas, err
	//log.Printf("Raw Doc rev1 looks like: %s", rv)
	var doc tDoc
	err := json.Unmarshal(rv, &doc)
	assert.True(t, err == nil)

	doc.Sync.Sequence = 3
	doc.Sync.CurrentRev = "3-e99405a23fa102238fa8c3fd499b15bc"
	doc.Sync.RecentSequences = []uint64{1, 2, 3}

	doc.Sync.History.Revs = []string{rev1, "2-2f9f49cf41ef37e98f9d74819361a378", "3-e99405a23fa102238fa8c3fd499b15bc"}
	doc.Sync.History.Parents = []int{-1, 0, 1}

	cm := make(channels.ChannelMap)
	cm["A"] = &channels.ChannelRemoval{Seq: 2, RevID: "2-e99405a23fa102238fa8c3fd499b15bc"}
	doc.Sync.Channels = cm

	b, err := json.Marshal(doc)

	// Update raw document in the bucket
	rt._bucket.SetRaw("alpha", 0, b)

	// Get the updates from the _changes feed:
	rt.waitForPendingChanges()
	response = rt.send(requestByUser("GET", "/db/_changes", "", "alice"))
	changes.Results = nil
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since = changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(2))
	assert.Equals(t, changes.Results[0].ID, "alpha")
}

func TestPostChangesInteger(t *testing.T) {

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChanges(t, it)
}

func postChanges(t *testing.T, it indexTester) {

	response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response = it.sendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/abc1", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
	changesResponse := it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 3)

}

func TestPostChangesSinceInteger(t *testing.T) {
	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChangesSince(t, it)
}

func TestPostChangesWithQueryString(t *testing.T) {
	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()

	// Put several documents
	response := it.sendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/abc1", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	it.waitForPendingChanges()

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Test basic properties
	changesJSON := `{"heartbeat":50, "feed":"normal", "limit":1, "since":"3"}`
	changesResponse := it.sendAdminRequest("POST", "/db/_changes?feed=longpoll&limit=10&since=0&heartbeat=50000", changesJSON)

	err := json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 4)

	// Test channel filter
	var filteredChanges struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	changesJSON = `{"feed":"longpoll"}`
	changesResponse = it.sendAdminRequest("POST", "/db/_changes?feed=longpoll&filter=sync_gateway/bychannel&channels=ABC", changesJSON)

	err = json.Unmarshal(changesResponse.Body.Bytes(), &filteredChanges)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(filteredChanges.Results), 1)
}

// Basic _changes test with since value
func postChangesSince(t *testing.T, it indexTester) {
	response := it.sendAdminRequest("PUT", "/_logging", `{"*":true}`)

	//response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user
	response = it.sendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@bb.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	// Put several documents
	response = it.sendAdminRequest("PUT", "/db/pbs1-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/samevbdiffchannel-0000609", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/samevbdiffchannel-0000799", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs2-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs3-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
	changesResponse := it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err := json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("Changes:%s", changesResponse.Body.Bytes())
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)

	// Put several more documents, some to the same vbuckets
	response = it.sendAdminRequest("PUT", "/db/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs4", `{"value":4, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	time.Sleep(1 * time.Second)
	changesJSON = fmt.Sprintf(`{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"%s"}`, changes.Last_Seq)
	changesResponse = it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("Changes:%s", changesResponse.Body.Bytes())
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 3)

}

func TestPostChangesChannelFilterInteger(t *testing.T) {
	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChangesChannelFilter(t, it)
}

// Test _changes with channel filter
func postChangesChannelFilter(t *testing.T, it indexTester) {

	response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response = it.sendAdminRequest("PUT", "/db/pbs1-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/samevbdiffchannel-0000609", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/samevbdiffchannel-0000799", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs2-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs3-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	changesJSON := `{"filter":"sync_gateway/bychannel", "channels":"PBS"}`
	changesResponse := it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 4)

	// Put several more documents, some to the same vbuckets
	response = it.sendAdminRequest("PUT", "/db/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs4", `{"value":4, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	time.Sleep(100 * time.Millisecond)
	changesResponse = it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	for _, result := range changes.Results {
		log.Printf("changes result:%+v", result)
	}
	assert.Equals(t, len(changes.Results), 7)

}

func TestPostChangesAdminChannelGrantInteger(t *testing.T) {
	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChangesAdminChannelGrant(t, it)
}

// _changes with admin-based channel grant
func postChangesAdminChannelGrant(t *testing.T, it indexTester) {
	response := it.sendAdminRequest("PUT", "/_logging", `{"Backfill":true}`)

	//response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user with access to channel ABC:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents in channel ABC and PBS
	response = it.sendAdminRequest("PUT", "/db/pbs-1", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs-2", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs-3", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs-4", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/abc-1", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Issue simple changes request
	changesResponse := it.send(requestByUser("GET", "/db/_changes", "", "bernard"))
	assertStatus(t, changesResponse, 200)

	log.Printf("Response:%+v", changesResponse.Body)
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 1)

	// Update the user doc to grant access to PBS
	response = it.sendAdminRequest("PUT", "/db/_user/bernard", `{"admin_channels":["ABC", "PBS"]}`)
	assertStatus(t, response, 200)

	time.Sleep(500 * time.Millisecond)

	// Issue a new changes request with since=last_seq ensure that user receives all records for channel PBS
	changesResponse = it.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", changes.Last_Seq),
		"", "bernard"))
	assertStatus(t, changesResponse, 200)

	log.Printf("Response:%+v", changesResponse.Body)
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	assert.Equals(t, len(changes.Results), 5) // 4 PBS docs, plus the updated user doc

	// Write a few more docs
	response = it.sendAdminRequest("PUT", "/db/pbs-5", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/abc-2", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)

	time.Sleep(500 * time.Millisecond)

	// Issue another changes request - ensure we don't backfill again
	changesResponse = it.send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", changes.Last_Seq),
		"", "bernard"))
	assertStatus(t, changesResponse, 200)
	log.Printf("Response:%+v", changesResponse.Body)
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	assert.Equals(t, len(changes.Results), 2) // 2 docs

}

// Test low sequence handling of late arriving sequences to a continuous changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequence(t *testing.T) {

	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	shortWaitCache := &CacheConfig{
		CachePendingSeqMaxWait: &pendingMaxWait,
		CachePendingSeqMaxNum:  &maxNum,
		CacheSkippedSeqMaxWait: &skippedMaxWait,
	}

	rt := restTester{syncFn: `function(doc) {channel(doc.channel)}`, cacheConfig: shortWaitCache}
	testDb := rt.ServerContext().Database("db")

	response := rt.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "Debug":true}`)
	// Create user:
	assertStatus(t, rt.sendAdminRequest("GET", "/db/_user/bernard", ""), 404)
	response = rt.sendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@couchbase.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	WriteDirect(testDb, []string{"PBS"}, 2)
	WriteDirect(testDb, []string{"PBS"}, 5)
	WriteDirect(testDb, []string{"PBS"}, 6)
	testDb.WaitForSequenceWithMissing(6)

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	response = rt.send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 4)
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))
	assert.Equals(t, changes.Last_Seq, "2::6")

	// Send another changes request before any changes, with the last_seq received from the last changes ("2::6")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 0)

	// Send a missing doc - low sequence should move to 3
	WriteDirect(testDb, []string{"PBS"}, 3)
	testDb.WaitForSequence(3)

	// WaitForSequence doesn't wait for low sequence to be updated on each channel - additional delay to ensure
	// low is updated before making the next changes request.
	time.Sleep(50 * time.Millisecond)

	// Send another changes request with the same since ("2::6") to ensure we see data once there are changes
	response = rt.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 3)

	// Send a later doc - low sequence still 3, high sequence goes to 7
	WriteDirect(testDb, []string{"PBS"}, 7)
	testDb.WaitForSequenceWithMissing(7)

	// Send another changes request with the same since ("2::6") to ensure we see data once there are changes
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)

}

func TestChangesActiveOnlyInteger(t *testing.T) {
	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	changesActiveOnly(t, it)
}

func TestOneShotChangesWithExplicitDocIds(t *testing.T) {

	var logKeys = map[string]bool{
		"TEST": true,
	}

	base.UpdateLogKeys(logKeys, true)

	rt := restTester{syncFn: `function(doc) {channel(doc.channels)}`}

	// Create user1
	response := rt.sendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)
	// Create user2
	response = rt.sendAdminRequest("PUT", "/db/_user/user2", `{"email":"user2@couchbase.com", "password":"letmein", "admin_channels":["beta"]}`)
	assertStatus(t, response, 201)
	// Create user3
	response = rt.sendAdminRequest("PUT", "/db/_user/user3", `{"email":"user3@couchbase.com", "password":"letmein", "admin_channels":["alpha","beta"]}`)
	assertStatus(t, response, 201)

	// Create user4
	response = rt.sendAdminRequest("PUT", "/db/_user/user4", `{"email":"user4@couchbase.com", "password":"letmein", "admin_channels":[]}`)
	assertStatus(t, response, 201)

	// Create user5
	response = rt.sendAdminRequest("PUT", "/db/_user/user5", `{"email":"user5@couchbase.com", "password":"letmein", "admin_channels":["*"]}`)
	assertStatus(t, response, 201)

	//Create docs
	assertStatus(t, rt.sendRequest("PUT", "/db/doc1", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc2", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc3", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/doc4", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/docA", `{"channels":["beta"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/docB", `{"channels":["beta"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/docC", `{"channels":["beta"]}`), 201)
	assertStatus(t, rt.sendRequest("PUT", "/db/docD", `{"channels":["beta"]}`), 201)

	// Create struct to hold changes response
	var changes struct {
		Results []db.ChangeEntry
	}

	//User has access to single channel
	body := `{"filter":"_doc_ids", "doc_ids":["doc4", "doc1", "docA", "b0gus"]}`
	request, _ := http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user1", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err := json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Results[1].ID, "doc4")

	//User has access to different single channel
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "docB", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user2", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)
	assert.Equals(t, changes.Results[2].ID, "docD")

	//User has access to multiple channels
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Results[3].ID, "docD")

	//User has no channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user4", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 0)

	//User has "*" channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user5", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 5)

	//User has "*" channel access, override POST with GET params
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	request, _ = http.NewRequest("POST", `/db/_changes?doc_ids=["docC","doc1"]`, bytes.NewBufferString(body))
	request.SetBasicAuth("user5", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)

	//User has "*" channel access, use GET
	request, _ = http.NewRequest("GET", `/db/_changes?filter=_doc_ids&doc_ids=["docC","doc1","docD"]`, nil)
	request.SetBasicAuth("user5", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)

	//User has "*" channel access, use GET with doc_ids plain comma separated list
	request, _ = http.NewRequest("GET", `/db/_changes?filter=_doc_ids&doc_ids=docC,doc1,doc2,docD`, nil)
	request.SetBasicAuth("user5", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)

	//Admin User
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "docA"]}`
	response = rt.sendAdminRequest("POST", "/db/_changes", body)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)

	//Use since value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)
	assert.Equals(t, changes.Results[2].ID, "docD")

	//Use since value and limit value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6, "limit":1}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "doc4")

	//test parameter include_docs=true
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "include_docs":true }`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Results[3].ID, "docD")
	assert.Equals(t, changes.Results[3].Doc["_id"], "docD")

	//test parameter style=all_docs
	//Create a conflict revision on docC
	assertStatus(t, rt.sendRequest("POST", "/db/_bulk_docs", `{"new_edits":false, "docs": [{"_id": "docC", "_rev": "2-b4afc58d8e61a6b03390e19a89d26643","foo": "bat", "channels":["beta"]}]}`), 201)

	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "style":"all_docs"}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Results[3].ID, "docC")
	assert.Equals(t, len(changes.Results[3].Changes), 2)

}

// Test _changes with channel filter
func changesActiveOnly(t *testing.T, it indexTester) {

	response := it.sendAdminRequest("PUT", "/_logging", `{"HTTP":true, "Changes":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS", "ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response = it.sendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/activeDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	partialRemovalRev := body["rev"].(string)
	assertStatus(t, response, 201)

	response = it.sendAdminRequest("PUT", "/db/conflictedDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Create a conflict, then tombstone it
	response = it.sendAdminRequest("POST", "/db/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictTombstone"}], "new_edits":false}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("DELETE", "/db/conflictedDoc?rev=1-conflictTombstone", "")
	assertStatus(t, response, 200)

	// Create a conflict, and don't tombstone it
	response = it.sendAdminRequest("POST", "/db/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictActive"}], "new_edits":false}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	changesResponse := it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)

	// Delete
	response = it.sendAdminRequest("DELETE", fmt.Sprintf("/db/deletedDoc?rev=%s", deletedRev), "")
	assertStatus(t, response, 200)

	// Removed
	response = it.sendAdminRequest("PUT", "/db/removedDoc", fmt.Sprintf(`{"_rev":%q, "channel":["HBO"]}`, removedRev))
	assertStatus(t, response, 201)

	// Partially removed
	response = it.sendAdminRequest("PUT", "/db/partialRemovalDoc", fmt.Sprintf(`{"_rev":%q, "channel":["PBS"]}`, partialRemovalRev))
	assertStatus(t, response, 201)

	time.Sleep(100 * time.Millisecond)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	changesResponse = it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 3)
		}
	}

	// Active only, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 3)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}
	// Active only, GET
	changes.Results = nil
	changesResponse = it.send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true", "", "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 3)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

}

//////// HELPERS:

func assertNoError(t *testing.T, err error, message string) {
	if err != nil {
		t.Fatalf("%s: %v", message, err)
	}
}

func assertTrue(t *testing.T, success bool, message string) {
	if !success {
		t.Fatalf("%s", message)
	}
}

/*
// Index partitions for testing
func SeedPartitionMap(bucket base.Bucket, numPartitions uint16) error {
	maxVbNo := uint16(1024)
	partitionDefs := make(base.PartitionStorageSet, numPartitions)
	vbPerPartition := maxVbNo / numPartitions
	for partition := uint16(0); partition < numPartitions; partition++ {
		storage := base.PartitionStorage{
			Index: partition,
			VbNos: make([]uint16, vbPerPartition),
		}
		for index := uint16(0); index < vbPerPartition; index++ {
			vb := partition*vbPerPartition + index
			storage.VbNos[index] = append(storage.VbNos, vb)
		}
		partitionDefs[partition] = storage
	}

	// Persist to bucket
	value, err := json.Marshal(partitionDefs)
	if err != nil {
		return err
	}
	bucket.SetRaw(base.KIndexPartitionKey, 0, value)
	return nil
}
*/

func WriteDirect(testDb *db.DatabaseContext, channelArray []string, sequence uint64) {
	docId := fmt.Sprintf("doc-%v", sequence)
	WriteDirectWithKey(testDb, docId, channelArray, sequence)
}

func WriteDirectWithKey(testDb *db.DatabaseContext, key string, channelArray []string, sequence uint64) {

	rev := "1-a"
	chanMap := make(map[string]*channels.ChannelRemoval, 10)

	for _, channel := range channelArray {
		chanMap[channel] = nil
	}
	var syncData struct {
		CurrentRev string              `json:"rev"`
		Sequence   uint64              `json:"sequence,omitempty"`
		Channels   channels.ChannelMap `json:"channels,omitempty"`
		TimeSaved  time.Time           `json:"time_saved,omitempty"`
	}
	syncData.CurrentRev = rev
	syncData.Sequence = sequence
	syncData.Channels = chanMap
	syncData.TimeSaved = time.Now()
	//syncData := fmt.Sprintf(`{"rev":"%s", "sequence":%d, "channels":%s, "TimeSaved":"%s"}`, rev, sequence, chanMap, time.Now())

	testDb.Bucket.Add(key, 0, db.Body{"_sync": syncData, "key": key})
}
