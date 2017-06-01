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
	"strings"
	"sync"
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
	RestTester
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
	it.SyncFn = syncFn

	it.RestTesterServerContext = NewServerContext(&ServerConfig{
		Facebook: &FacebookConfig{},
	})

	var syncFnPtr *string
	if len(it.SyncFn) > 0 {
		syncFnPtr = &it.SyncFn
	}

	// TODO: this should be able to use either a Walrus or a Couchbase bucket

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
		}
		dbConfig.ChannelIndex = channelIndexConfig
	}

	dbContext, err := it.RestTesterServerContext.AddDatabaseFromConfig(dbConfig)

	if useBucketIndex {
		_, err := base.SeedTestPartitionMap(dbContext.GetIndexBucket(), 64)
		if err != nil {
			panic(fmt.Sprintf("Error from seed partition map: %v", err))
		}
	}

	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}

	it.RestTesterBucket = it.RestTesterServerContext.Database("db").Bucket

	if useBucketIndex {
		it._indexBucket = it.RestTesterServerContext.Database("db").GetIndexBucket()
	}

	return it
}

func (it *indexTester) Close() {
	it.RestTesterServerContext.Close()
}

func (it *indexTester) ServerContext() *ServerContext {
	return it.RestTesterServerContext
}

func TestDocDeletionFromChannel(t *testing.T) {
	// See https://github.com/couchbase/couchbase-lite-ios/issues/59
	// base.LogKeys["Changes"] = true
	// base.LogKeys["Cache"] = true

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`}
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()

	// Create user:
	alice, _ := a.NewUser("alice", "letmein", channels.SetOf("zero"))
	a.Save(alice)

	// Create a doc Alice can see:
	response := rt.Send(request("PUT", "/db/alpha", `{"channel":"zero"}`))

	// Check the _changes feed:
	rt.ServerContext().Database("db").WaitForPendingChanges()
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))

	assert.Equals(t, changes.Results[0].ID, "alpha")
	rev1 := changes.Results[0].Changes[0]["rev"]

	// Delete the document:
	assertStatus(t, rt.Send(request("DELETE", "/db/alpha?rev="+rev1, "")), 200)

	// Get the updates from the _changes feed:
	rt.WaitForPendingChanges()
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", since),
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
	response = rt.Send(requestByUser("GET", "/db/alpha?rev="+rev2, "", "alice"))
	assert.Equals(t, response.Code, 200)
	log.Printf("Deletion looks like: %s", response.Body.Bytes())
	var docBody db.Body
	json.Unmarshal(response.Body.Bytes(), &docBody)
	assert.DeepEquals(t, docBody, db.Body{"_id": "alpha", "_rev": rev2, "_deleted": true})

	// Access without deletion revID shouldn't be allowed (since doc is not in Alice's channels):
	response = rt.Send(requestByUser("GET", "/db/alpha", "", "alice"))
	assert.Equals(t, response.Code, 403)

	// A bogus rev ID should return a 404:
	response = rt.Send(requestByUser("GET", "/db/alpha?rev=bogus", "", "alice"))
	assert.Equals(t, response.Code, 404)

	// Get the old revision, which should still be accessible:
	response = rt.Send(requestByUser("GET", "/db/alpha?rev="+rev1, "", "alice"))
	assert.Equals(t, response.Code, 200)
}

func TestPostChangesInteger(t *testing.T) {

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChanges(t, it)
}

func postChanges(t *testing.T, it indexTester) {

	response := it.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response = it.SendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc1", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 3)

}

// Tests race between waking up the changes feed, and detecting that the user doc has changed
func TestPostChangesUserTiming(t *testing.T) {

	it := initIndexTester(false, `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel)}`)
	defer it.Close()

	response := it.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("bernard"))
	assert.True(t, err == nil)
	a.Save(bernard)

	var wg sync.WaitGroup

	// Put several documents to channel PBS
	response = it.SendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	wg.Add(1)
	go func() {
		defer wg.Done()
		var changes struct {
			Results  []db.ChangeEntry
			Last_Seq string
		}
		changesJSON := `{"style":"all_docs", "timeout":6000, "feed":"longpoll", "limit":50, "since":"0"}`
		changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
		// Validate that the user receives backfill plus the new doc
		err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
		assertNoError(t, err, "Error unmarshalling changes response")
		assert.Equals(t, len(changes.Results), 4)
	}()

	// Wait for changes feed to get into wait mode, even when running under race
	time.Sleep(2 * time.Second)

	// Put a doc in channel bernard, that also grants bernard access to channel PBS
	response = it.SendAdminRequest("PUT", "/db/grant1", `{"value":1, "channel":["bernard"], "accessUser":"bernard", "accessChannel":"PBS"}`)
	assertStatus(t, response, 201)
	wg.Wait()

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
	response := it.SendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc1", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	it.WaitForPendingChanges()

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Test basic properties
	changesJSON := `{"heartbeat":50, "feed":"normal", "limit":1, "since":"3"}`
	changesResponse := it.SendAdminRequest("POST", "/db/_changes?feed=longpoll&limit=10&since=0&heartbeat=50000", changesJSON)

	err := json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 4)

	// Test channel filter
	var filteredChanges struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	changesJSON = `{"feed":"longpoll"}`
	changesResponse = it.SendAdminRequest("POST", "/db/_changes?feed=longpoll&filter=sync_gateway/bychannel&channels=ABC", changesJSON)

	err = json.Unmarshal(changesResponse.Body.Bytes(), &filteredChanges)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(filteredChanges.Results), 1)
}

// Basic _changes test with since value
func postChangesSince(t *testing.T, it indexTester) {
	response := it.SendAdminRequest("PUT", "/_logging", `{"*":true}`)

	//response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user
	response = it.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@bb.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	// Put several documents
	response = it.SendAdminRequest("PUT", "/db/pbs1-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/samevbdiffchannel-0000609", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/samevbdiffchannel-0000799", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs3-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err := json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("Changes:%s", changesResponse.Body.Bytes())
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)

	// Put several more documents, some to the same vbuckets
	response = it.SendAdminRequest("PUT", "/db/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs4", `{"value":4, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	time.Sleep(1 * time.Second)
	changesJSON = fmt.Sprintf(`{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"%s"}`, changes.Last_Seq)
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
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

	response := it.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response = it.SendAdminRequest("PUT", "/db/pbs1-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/samevbdiffchannel-0000609", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/samevbdiffchannel-0000799", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs3-0000609", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	changesJSON := `{"filter":"sync_gateway/bychannel", "channels":"PBS"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 4)

	// Put several more documents, some to the same vbuckets
	response = it.SendAdminRequest("PUT", "/db/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs4", `{"value":4, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	time.Sleep(100 * time.Millisecond)
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
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
	response := it.SendAdminRequest("PUT", "/_logging", `{"Backfill":true}`)

	//response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user with access to channel ABC:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents in channel ABC and PBS
	response = it.SendAdminRequest("PUT", "/db/pbs-1", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs-2", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs-3", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs-4", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc-1", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Issue simple changes request
	changesResponse := it.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	assertStatus(t, changesResponse, 200)

	log.Printf("Response:%+v", changesResponse.Body)
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 1)

	// Update the user doc to grant access to PBS
	response = it.SendAdminRequest("PUT", "/db/_user/bernard", `{"admin_channels":["ABC", "PBS"]}`)
	assertStatus(t, response, 200)

	time.Sleep(500 * time.Millisecond)

	// Issue a new changes request with since=last_seq ensure that user receives all records for channel PBS
	changesResponse = it.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", changes.Last_Seq),
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
	response = it.SendAdminRequest("PUT", "/db/pbs-5", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc-2", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)

	time.Sleep(500 * time.Millisecond)

	// Issue another changes request - ensure we don't backfill again
	changesResponse = it.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", changes.Last_Seq),
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

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`, CacheConfig: shortWaitCache}
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	response := rt.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "Debug":true}`)
	// Create user:
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/bernard", ""), 404)
	response = rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@couchbase.com", "password":"letmein", "admin_channels":["PBS"]}`)
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
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 4)
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))
	assert.Equals(t, changes.Last_Seq, "2::6")

	// Send another changes request before any changes, with the last_seq received from the last changes ("2::6")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
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
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 3)

	// Send a later doc - low sequence still 3, high sequence goes to 7
	WriteDirect(testDb, []string{"PBS"}, 7)
	testDb.WaitForSequenceWithMissing(7)

	// Send another changes request with the same since ("2::6") to ensure we see data once there are changes
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 1)

}

func TestUnusedSequences(t *testing.T) {

	// Only do 10 iterations if running against walrus.  If against a live couchbase server,
	// just do single iteration.  (Takes approx 30s)
	numIterations := 10
	if !base.UnitTestUrlIsWalrus() {
		numIterations = 1
	}

	for i := 0; i < numIterations; i++ {
		_testConcurrentDelete(t)
		_testConcurrentPutAsDelete(t)
		_testConcurrentUpdate(t)
		_testConcurrentNewEditsFalseDelete(t)
	}
}

func _testConcurrentDelete(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Cache+", "Changes+", "CRUD", "CRUD+"})

	rt := RestTester{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId := body["rev"].(string)

	// Issue concurrent deletes
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			deleteResponse := rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/doc1?rev=%s", revId), "")
			log.Printf("Delete #%d got response code: %d", i, deleteResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, panics on timeout
	rt.ServerContext().Database("db").WaitForPendingChanges()

	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)

	// Wait for writes to be processed and indexed
	rt.ServerContext().Database("db").WaitForPendingChanges()

}

func _testConcurrentPutAsDelete(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Cache+", "Changes+", "CRUD", "CRUD+"})

	rt := RestTester{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId := body["rev"].(string)

	// Issue concurrent deletes
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			deleteResponse := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc1?rev=%s", revId), `{"_deleted":true}`)
			log.Printf("Delete #%d got response code: %d", i, deleteResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, panics on timeout
	rt.ServerContext().Database("db").WaitForPendingChanges()

	// Write another doc, to validate sequences restart
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)

	rt.ServerContext().Database("db").WaitForPendingChanges()
}

func _testConcurrentUpdate(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Cache+", "Changes+", "CRUD", "CRUD+"})

	rt := RestTester{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId := body["rev"].(string)

	// Issue concurrent updates
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			updateResponse := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc1?rev=%s", revId), `{"value":10}`)
			log.Printf("Update #%d got response code: %d", i, updateResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, panics on timeout
	rt.ServerContext().Database("db").WaitForPendingChanges()

	// Write another doc, to validate sequences restart
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)

	rt.ServerContext().Database("db").WaitForPendingChanges()
}

func _testConcurrentNewEditsFalseDelete(t *testing.T) {
	base.ParseLogFlags([]string{"Cache", "Cache+", "Changes+", "CRUD", "CRUD+", "HTTP", "HTTP+"})

	rt := RestTester{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId := body["rev"].(string)
	revIdHash := strings.TrimPrefix(revId, "1-")

	deleteRequestBody := fmt.Sprintf(`{"_rev": "2-foo", "_revisions": {"start": 2, "ids": ["foo", "%s"]}, "_deleted":true}`, revIdHash)
	// Issue concurrent deletes
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			deleteResponse := rt.SendAdminRequest("PUT", "/db/doc1?rev=2-foo&new_edits=false", deleteRequestBody)
			log.Printf("Delete #%d got response code: %d", i, deleteResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, panics on timeout
	rt.ServerContext().Database("db").WaitForPendingChanges()

	// Write another doc, to see where sequences are at
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	rt.ServerContext().Database("db").WaitForPendingChanges()

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

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channels)}`}
	defer rt.Close()

	// Create user1
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha"]}`)
	assertStatus(t, response, 201)
	// Create user2
	response = rt.SendAdminRequest("PUT", "/db/_user/user2", `{"email":"user2@couchbase.com", "password":"letmein", "admin_channels":["beta"]}`)
	assertStatus(t, response, 201)
	// Create user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", `{"email":"user3@couchbase.com", "password":"letmein", "admin_channels":["alpha","beta"]}`)
	assertStatus(t, response, 201)

	// Create user4
	response = rt.SendAdminRequest("PUT", "/db/_user/user4", `{"email":"user4@couchbase.com", "password":"letmein", "admin_channels":[]}`)
	assertStatus(t, response, 201)

	// Create user5
	response = rt.SendAdminRequest("PUT", "/db/_user/user5", `{"email":"user5@couchbase.com", "password":"letmein", "admin_channels":["*"]}`)
	assertStatus(t, response, 201)

	//Create docs
	assertStatus(t, rt.SendRequest("PUT", "/db/doc1", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc2", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc3", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/doc4", `{"channels":["alpha"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/docA", `{"channels":["beta"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/docB", `{"channels":["beta"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/docC", `{"channels":["beta"]}`), 201)
	assertStatus(t, rt.SendRequest("PUT", "/db/docD", `{"channels":["beta"]}`), 201)

	// Create struct to hold changes response
	var changes struct {
		Results []db.ChangeEntry
	}

	//User has access to single channel
	body := `{"filter":"_doc_ids", "doc_ids":["doc4", "doc1", "docA", "b0gus"]}`
	request, _ := http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user1", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err := json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Results[1].ID, "doc4")

	//User has access to different single channel
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "docB", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user2", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)
	assert.Equals(t, changes.Results[2].ID, "docD")

	//User has access to multiple channels
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Results[3].ID, "docD")

	//User has no channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user4", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 0)

	//User has "*" channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 5)

	//User has "*" channel access, override POST with GET params
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	request, _ = http.NewRequest("POST", `/db/_changes?doc_ids=["docC","doc1"]`, bytes.NewBufferString(body))
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 2)

	//User has "*" channel access, use GET
	request, _ = http.NewRequest("GET", `/db/_changes?filter=_doc_ids&doc_ids=["docC","doc1","docD"]`, nil)
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)

	//User has "*" channel access, use GET with doc_ids plain comma separated list
	request, _ = http.NewRequest("GET", `/db/_changes?filter=_doc_ids&doc_ids=docC,doc1,doc2,docD`, nil)
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)

	//Admin User
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "docA"]}`
	response = rt.SendAdminRequest("POST", "/db/_changes", body)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)

	//Use since value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 3)
	assert.Equals(t, changes.Results[2].ID, "docD")

	//Use since value and limit value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6, "limit":1}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].ID, "doc4")

	//test parameter include_docs=true
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "include_docs":true }`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Results[3].ID, "docD")
	assert.Equals(t, changes.Results[3].Doc["_id"], "docD")

	//test parameter style=all_docs
	//Create a conflict revision on docC
	assertStatus(t, rt.SendRequest("POST", "/db/_bulk_docs", `{"new_edits":false, "docs": [{"_id": "docC", "_rev": "2-b4afc58d8e61a6b03390e19a89d26643","foo": "bat", "channels":["beta"]}]}`), 201)

	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "style":"all_docs"}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, err, nil)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Results[3].ID, "docC")
	assert.Equals(t, len(changes.Results[3].Changes), 2)

}

// Test _changes with channel filter
func changesActiveOnly(t *testing.T, it indexTester) {

	response := it.SendAdminRequest("PUT", "/_logging", `{"HTTP":true, "Changes":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS", "ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response = it.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	partialRemovalRev := body["rev"].(string)
	assertStatus(t, response, 201)

	response = it.SendAdminRequest("PUT", "/db/conflictedDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Create a conflict, then tombstone it
	response = it.SendAdminRequest("POST", "/db/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictTombstone"}], "new_edits":false}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("DELETE", "/db/conflictedDoc?rev=1-conflictTombstone", "")
	assertStatus(t, response, 200)

	// Create a conflict, and don't tombstone it
	response = it.SendAdminRequest("POST", "/db/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictActive"}], "new_edits":false}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)

	// Delete
	response = it.SendAdminRequest("DELETE", fmt.Sprintf("/db/deletedDoc?rev=%s", deletedRev), "")
	assertStatus(t, response, 200)

	// Removed
	response = it.SendAdminRequest("PUT", "/db/removedDoc", fmt.Sprintf(`{"_rev":%q, "channel":["HBO"]}`, removedRev))
	assertStatus(t, response, 201)

	// Partially removed
	response = it.SendAdminRequest("PUT", "/db/partialRemovalDoc", fmt.Sprintf(`{"_rev":%q, "channel":["PBS"]}`, partialRemovalRev))
	assertStatus(t, response, 201)

	time.Sleep(100 * time.Millisecond)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
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
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
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
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true", "", "bernard"))
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
