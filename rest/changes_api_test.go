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
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbaselabs/go.assert"
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

	// TODO: this should be able to use either a Walrus or a Couchbase bucket.
	//       When supported, set dbConfig.UseViews conditionally

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
		UseViews: true, // walrus only supports views
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

// Reproduces issue #2383 by forcing a partial error from the view on the first changes request.
func TestReproduce2383(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip LeakyBucket test when running in integration")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	var rt RestTester
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	user, err := a.NewUser("ben", "letmein", channels.SetOf("PBS"))
	assertNoError(t, err, "Error creating new user")
	a.Save(user)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	testDb := rt.ServerContext().Database("db")

	testDb.WaitForSequence(3)
	testDb.FlushChannelCache()

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	leakyBucket, ok := rt.Bucket().(*base.LeakyBucket)
	assertTrue(t, ok, "Bucket was not of type LeakyBucket")
	// Force a partial error for the first ViewCustom call we make to initialize an invalid cache.
	leakyBucket.SetFirstTimeViewCustomPartialError(true)

	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")

	// In the first changes request since cache flush, we're forcing a nil cache with no error. Thereforce we'd expect to see zero results.
	assert.Equals(t, len(changes.Results), 0)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")

	// Now we should expect 3 results, as the invalid cache was not persisted.
	// The second call to ViewCustom will succeed and properly create the channel cache.
	assert.Equals(t, len(changes.Results), 3)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

	// if we create another doc, the cache will get updated with the latest doc.
	response = rt.SendAdminRequest("PUT", "/db/doc4", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	testDb.WaitForSequence(4)

	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")

	assert.Equals(t, len(changes.Results), 4)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

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
	assert.DeepEquals(t, docBody, db.Body{db.BodyId: "alpha", db.BodyRev: rev2, db.BodyDeleted: true})

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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP|base.KeyAccel)()

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()

	postChanges(t, it)
}

func postChanges(t *testing.T, it indexTester) {

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response := it.SendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
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

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP|base.KeyAccel)()

	it := initIndexTester(false, `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel)}`)
	defer it.Close()

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("bernard"))
	assert.True(t, err == nil)
	a.Save(bernard)

	var wg sync.WaitGroup

	// Put several documents to channel PBS
	response := it.SendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
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

		if len(changes.Results) != 3 {
			log.Printf("len(changes.Results) != 3, dumping changes response for diagnosis")
			log.Printf("changes: %+v", changes)
			log.Printf("changesResponse status code: %v.  Headers: %+v", changesResponse.Code, changesResponse.HeaderMap)
			log.Printf("changesResponse raw body: %s", changesResponse.Body.String())
		}
		assert.Equals(t, len(changes.Results), 3)
	}()

	// Wait for changes feed to get into wait mode where it is blocked on the longpoll changes feed response
	time.Sleep(5 * time.Second)

	// Put a doc in channel bernard, that also grants bernard access to channel PBS
	response = it.SendAdminRequest("PUT", "/db/grant1", `{"value":1, "accessUser":"bernard", "accessChannel":"PBS"}`)
	assertStatus(t, response, 201)
	wg.Wait()

}

func TestPostChangesSinceInteger(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

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

	// Create user
	response := it.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@bb.com", "password":"letmein", "admin_channels":["PBS"]}`)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP|base.KeyAccel)()

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()

	postChangesChannelFilter(t, it)
}

// Test _changes with channel filter
func postChangesChannelFilter(t *testing.T, it indexTester) {

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response := it.SendAdminRequest("PUT", "/db/pbs1-0000609", `{"channel":["PBS"]}`)
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

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP|base.KeyAccel)()

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChangesAdminChannelGrant(t, it)
}

// _changes with admin-based channel grant
func postChangesAdminChannelGrant(t *testing.T, it indexTester) {

	// Create user with access to channel ABC:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents in channel ABC and PBS
	response := it.SendAdminRequest("PUT", "/db/pbs-1", `{"channel":["PBS"]}`)
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

	if base.TestUseXattrs() {
		t.Skip("This test cannot run in xattr mode until WriteDirect() is updated.  See https://github.com/couchbase/sync_gateway/issues/2666#issuecomment-311183219")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyChanges)()

	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	numIndexReplicas := uint(0)
	shortWaitConfig := &DbConfig{
		CacheConfig: &CacheConfig{
			CachePendingSeqMaxWait: &pendingMaxWait,
			CachePendingSeqMaxNum:  &maxNum,
			CacheSkippedSeqMaxWait: &skippedMaxWait,
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	// Create user:
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/bernard", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@couchbase.com", "password":"letmein", "admin_channels":["PBS"]}`)
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

// Test low sequence handling of late arriving sequences to a one-shot changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequenceOneShotUser(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test cannot run in xattr mode until WriteDirect() is updated.  See https://github.com/couchbase/sync_gateway/issues/2666#issuecomment-311183219")
	}

	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	numIndexReplicas := uint(0)
	shortWaitConfig := &DbConfig{
		CacheConfig: &CacheConfig{
			CachePendingSeqMaxWait: &pendingMaxWait,
			CachePendingSeqMaxNum:  &maxNum,
			CacheSkippedSeqMaxWait: &skippedMaxWait,
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	response := rt.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "Debug":true}`)
	// Create user:
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/bernard", ""), 404)
	response = rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@couchbase.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	// Simulate 4 non-skipped writes (seq 2,3,4,5)
	WriteDirect(testDb, []string{"PBS"}, 2)
	WriteDirect(testDb, []string{"PBS"}, 3)
	WriteDirect(testDb, []string{"PBS"}, 4)
	WriteDirect(testDb, []string{"PBS"}, 5)
	testDb.WaitForSequenceWithMissing(5)

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq string
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes 1 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 5) // Includes user doc
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))
	assert.Equals(t, changes.Last_Seq, "5")

	// Skip sequence 6, write docs 7-10
	WriteDirect(testDb, []string{"PBS"}, 7)
	WriteDirect(testDb, []string{"PBS"}, 8)
	WriteDirect(testDb, []string{"PBS"}, 9)
	WriteDirect(testDb, []string{"PBS"}, 10)
	testDb.WaitForSequenceWithMissing(10)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 2 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Last_Seq, "5::10")

	// Write a few more docs
	WriteDirect(testDb, []string{"PBS"}, 11)
	WriteDirect(testDb, []string{"PBS"}, 12)
	testDb.WaitForSequenceWithMissing(12)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 3 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Last_Seq, "5::12")

	// Write another doc, then the skipped doc - both should be sent, last_seq should move to 13
	WriteDirect(testDb, []string{"PBS"}, 13)
	WriteDirect(testDb, []string{"PBS"}, 6)
	testDb.WaitForSequence(13)

	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 4 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)

	// Valid results here under race conditions should be:
	//    receive sequences 6-13, last seq 13
	//    receive sequence 13, last seq 5::13
	//    receive sequence 6-12, last seq 12
	//    receive no sequences, last seq 5::12
	// Valid but unexpected results (no data loss):
	//    receive sequence 6, last sequence 6
	//    receive sequence 6, last sequence 12
	// Invalid results (data loss):
	//    receive sequence 13, last seq 13
	//    receives sequence 6, last seq 13
	switch len(changes.Results) {
	case 0:
		assert.Equals(t, changes.Last_Seq, "5::12")
	case 1:
		switch changes.Last_Seq {
		case "5::13":
			assert.Equals(t, changes.Results[0].Seq.Seq, uint64(13))
		case "6":
			assert.Equals(t, changes.Results[0].Seq.Seq, uint64(6))
			log.Printf("Didn't expect last:6 w/ seq 6")
		case "12":
			assert.Equals(t, changes.Results[0].Seq.Seq, uint64(6))
			log.Printf("Didn't expect last:12 w/ seq 6")
		default:
			t.Errorf("invalid response.  Last_Seq: %v  changes: %v", changes.Last_Seq, changes.Results[0])
		}
	case 7:
		assert.Equals(t, changes.Last_Seq, "12")
	case 8:
		assert.Equals(t, changes.Last_Seq, "13")
	default:
		t.Errorf("Unexpected number of changes results.  Last_Seq: %v  len(changes): %v", changes.Last_Seq, len(changes.Results))
	}

	assert.Equals(t, len(changes.Results), 8)
	assert.Equals(t, changes.Last_Seq, "13")
}

// Test low sequence handling of late arriving sequences to a one-shot changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequenceOneShotAdmin(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test cannot run in xattr mode until WriteDirect() is updated.  See https://github.com/couchbase/sync_gateway/issues/2666#issuecomment-311183219")
	}

	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	numIndexReplicas := uint(0)
	shortWaitConfig := &DbConfig{
		CacheConfig: &CacheConfig{
			CachePendingSeqMaxWait: &pendingMaxWait,
			CachePendingSeqMaxNum:  &maxNum,
			CacheSkippedSeqMaxWait: &skippedMaxWait,
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	response := rt.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "Debug":true}`)

	// Simulate 5 non-skipped writes (seq 1,2,3,4,5)
	WriteDirect(testDb, []string{"PBS"}, 1)
	WriteDirect(testDb, []string{"PBS"}, 2)
	WriteDirect(testDb, []string{"PBS"}, 3)
	WriteDirect(testDb, []string{"PBS"}, 4)
	WriteDirect(testDb, []string{"PBS"}, 5)
	testDb.WaitForSequenceWithMissing(5)

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq string
	}
	response = rt.SendAdminRequest("GET", "/db/_changes", "")
	log.Printf("_changes 1 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 5)
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))
	assert.Equals(t, changes.Last_Seq, "5")

	// Skip sequence 6, write docs 7-10
	WriteDirect(testDb, []string{"PBS"}, 7)
	WriteDirect(testDb, []string{"PBS"}, 8)
	WriteDirect(testDb, []string{"PBS"}, 9)
	WriteDirect(testDb, []string{"PBS"}, 10)
	testDb.WaitForSequenceWithMissing(10)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.SendAdminRequest("POST", "/db/_changes", changesJSON)
	log.Printf("_changes 2 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Last_Seq, "5::10")

	// Write a few more docs
	WriteDirect(testDb, []string{"PBS"}, 11)
	WriteDirect(testDb, []string{"PBS"}, 12)
	testDb.WaitForSequenceWithMissing(12)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.SendAdminRequest("POST", "/db/_changes", changesJSON)
	log.Printf("_changes 3 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Last_Seq, "5::12")

	// Write another doc, then the skipped doc - both should be sent, last_seq should move to 13
	WriteDirect(testDb, []string{"PBS"}, 13)
	WriteDirect(testDb, []string{"PBS"}, 6)
	testDb.WaitForSequence(13)

	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.SendAdminRequest("POST", "/db/_changes", changesJSON)
	log.Printf("_changes 4 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)

	// Valid results here under race conditions should be:
	//    receive sequences 6-13, last seq 13
	//    receive sequence 13, last seq 5::13
	//    receive sequence 6-12, last seq 12
	//    receive no sequences, last seq 5::12
	// Valid but unexpected results (no data loss):
	//    receive sequence 6, last sequence 6
	//    receive sequence 6, last sequence 12
	// Invalid results (data loss):
	//    receive sequence 13, last seq 13
	//    receives sequence 6, last seq 13
	switch len(changes.Results) {
	case 0:
		assert.Equals(t, changes.Last_Seq, "5::12")
	case 1:
		switch changes.Last_Seq {
		case "5::13":
			assert.Equals(t, changes.Results[0].Seq.Seq, uint64(13))
		case "6":
			assert.Equals(t, changes.Results[0].Seq.Seq, uint64(6))
			log.Printf("Didn't expect last:6 w/ seq 6")
		case "12":
			assert.Equals(t, changes.Results[0].Seq.Seq, uint64(6))
			log.Printf("Didn't expect last:12 w/ seq 6")
		default:
			t.Errorf("invalid response.  Last_Seq: %v  changes: %v", changes.Last_Seq, changes.Results[0])
		}
	case 7:
		assert.Equals(t, changes.Last_Seq, "12")
	case 8:
		assert.Equals(t, changes.Last_Seq, "13")
	default:
		t.Errorf("Unexpected number of changes results.  Last_Seq: %v  len(changes): %v", changes.Last_Seq, len(changes.Results))
	}

	assert.Equals(t, len(changes.Results), 8)
	assert.Equals(t, changes.Last_Seq, "13")
}

// Test low sequence handling of late arriving sequences to a longpoll changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequenceLongpollUser(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test cannot run in xattr mode until WriteDirect() is updated.  See https://github.com/couchbase/sync_gateway/issues/2666#issuecomment-311183219")
	}

	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	numIndexReplicas := uint(0)
	shortWaitConfig := &DbConfig{
		CacheConfig: &CacheConfig{
			CachePendingSeqMaxWait: &pendingMaxWait,
			CachePendingSeqMaxNum:  &maxNum,
			CacheSkippedSeqMaxWait: &skippedMaxWait,
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	response := rt.SendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "Debug":true}`)
	// Create user:
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/bernard", ""), 404)
	response = rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@couchbase.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	// Simulate 4 non-skipped writes (seq 2,3,4,5)
	WriteDirect(testDb, []string{"PBS"}, 2)
	WriteDirect(testDb, []string{"PBS"}, 3)
	WriteDirect(testDb, []string{"PBS"}, 4)
	WriteDirect(testDb, []string{"PBS"}, 5)
	testDb.WaitForSequenceWithMissing(5)

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq string
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes 1 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 5) // Includes user doc
	since := changes.Results[0].Seq
	assert.Equals(t, since.Seq, uint64(1))
	assert.Equals(t, changes.Last_Seq, "5")

	// Skip sequence 6, write docs 7-10
	WriteDirect(testDb, []string{"PBS"}, 7)
	WriteDirect(testDb, []string{"PBS"}, 8)
	WriteDirect(testDb, []string{"PBS"}, 9)
	WriteDirect(testDb, []string{"PBS"}, 10)
	testDb.WaitForSequenceWithMissing(10)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 2 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 4)
	assert.Equals(t, changes.Last_Seq, "5::10")

	// Write a few more docs
	WriteDirect(testDb, []string{"PBS"}, 11)
	WriteDirect(testDb, []string{"PBS"}, 12)
	testDb.WaitForSequenceWithMissing(12)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 3 looks like: %s", response.Body.Bytes())
	json.Unmarshal(response.Body.Bytes(), &changes)
	assert.Equals(t, len(changes.Results), 2)
	assert.Equals(t, changes.Last_Seq, "5::12")

	// Issue a longpoll changes request.  Will block.
	var longpollWg sync.WaitGroup
	longpollWg.Add(1)
	go func() {
		defer longpollWg.Done()
		longpollChangesJSON := fmt.Sprintf(`{"since":"%s", "feed":"longpoll"}`, changes.Last_Seq)
		log.Printf("starting longpoll changes w/ JSON: %s", longpollChangesJSON)
		longPollResponse := rt.Send(requestByUser("POST", "/db/_changes", longpollChangesJSON, "bernard"))
		log.Printf("longpoll changes looks like: %s", longPollResponse.Body.Bytes())
		json.Unmarshal(longPollResponse.Body.Bytes(), &changes)
		// Expect to get 6 through 12
		assert.Equals(t, len(changes.Results), 7)
		assert.Equals(t, changes.Last_Seq, "12")
	}()

	// Wait for longpoll to get into wait mode
	// TODO: Would be preferable to add an expvar tracking the number of changes feeds in wait mode, and use that to
	//       detect when the longpoll changes goes into wait mode instead of a sleep.
	//       Such an expvar would be useful for diagnostic purposes as well.
	//       Deferring this change until post-2.1.1, due to potential performance implications.  Using sleep until that enhancement is
	//       made.
	time.Sleep(2 * time.Second)

	// Write the skipped doc, wait for longpoll to return
	WriteDirect(testDb, []string{"PBS"}, 6)
	//WriteDirect(testDb, []string{"PBS"}, 13)
	longpollWg.Wait()

}

func TestUnusedSequences(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache|base.KeyChanges|base.KeyCRUD|base.KeyHTTP)()

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
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP)()

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	changesActiveOnly(t, it)
}

func TestOneShotChangesWithExplicitDocIds(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

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
	assert.Equals(t, changes.Results[3].Doc[db.BodyId], "docD")

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

func updateTestDoc(rt RestTester, docid string, revid string, body string) (newRevId string, err error) {
	path := fmt.Sprintf("/db/%s", docid)
	if revid != "" {
		path = fmt.Sprintf("%s?rev=%s", path, revid)
	}
	response := rt.SendRequest("PUT", path, body)
	if response.Code != 200 && response.Code != 201 {
		return "", fmt.Errorf("createDoc got unexpected response code : %d", response.Code)
	}

	var responseBody db.Body
	json.Unmarshal(response.Body.Bytes(), &responseBody)
	return responseBody["rev"].(string), nil
}

// Validate retrieval of various document body types using include_docs.
func TestChangesIncludeDocs(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channels)}`}
	testDB := rt.GetDatabase()
	testDB.RevsLimit = 3
	defer rt.Close()

	// Create user1
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha", "beta"]}`)
	assertStatus(t, response, 201)

	//Create docs for each scenario
	// Active
	_, err := updateTestDoc(rt, "doc_active", "", `{"type": "active", "channels":["alpha"]}`)
	assertNoError(t, err, "Error updating doc")

	// Multi-revision
	var revid string
	revid, err = updateTestDoc(rt, "doc_multi_rev", "", `{"type": "active", "channels":["alpha"], "v":1}`)
	assertNoError(t, err, "Error updating doc")
	_, err = updateTestDoc(rt, "doc_multi_rev", revid, `{"type": "active", "channels":["alpha"], "v":2}`)
	assertNoError(t, err, "Error updating doc")

	// Tombstoned
	revid, err = updateTestDoc(rt, "doc_tombstone", "", `{"type": "tombstone", "channels":["alpha"]}`)
	assertNoError(t, err, "Error updating doc")
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/doc_tombstone?rev=%s", revid), "")
	assertStatus(t, response, 200)

	// Removed
	revid, err = updateTestDoc(rt, "doc_removed", "", `{"type": "removed", "channels":["alpha"]}`)
	assertNoError(t, err, "Error updating doc")
	_, err = updateTestDoc(rt, "doc_removed", revid, `{"type": "removed"}`)
	assertNoError(t, err, "Error updating doc")

	// No access (no channels)
	_, err = updateTestDoc(rt, "doc_no_channels", "", `{"type": "no_channels", "channels":["gamma"]}`)
	assertNoError(t, err, "Error updating doc")

	// No access (other channels)
	_, err = updateTestDoc(rt, "doc_no_access", "", `{"type": "no_access", "channels":["gamma"]}`)
	assertNoError(t, err, "Error updating doc")

	// Removal, pruned from rev tree
	var prunedRevId string
	prunedRevId, err = updateTestDoc(rt, "doc_pruned", "", `{"type": "pruned", "channels":["alpha"]}`)
	assertNoError(t, err, "Error updating doc")
	// Generate more revs than revs_limit (3)
	revid = prunedRevId
	for i := 0; i < 5; i++ {
		revid, err = updateTestDoc(rt, "doc_pruned", revid, `{"type": "pruned", "channels":["gamma"]}`)
		assertNoError(t, err, "Error updating doc")
	}

	// Doc w/ attachment
	revid, err = updateTestDoc(rt, "doc_attachment", "", `{"type": "attachments", "channels":["alpha"]}`)
	assertNoError(t, err, "Error updating doc")
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "text/plain"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response = rt.SendRequestWithHeaders("PUT", fmt.Sprintf("/db/doc_attachment/attach1?rev=%s", revid), attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	// Doc w/ large numbers
	_, err = updateTestDoc(rt, "doc_large_numbers", "", `{"type": "large_numbers", "channels":["alpha"], "largeint":1234567890, "largefloat":1234567890.1234}`)
	assertNoError(t, err, "Error updating doc")

	// Conflict
	revid, err = updateTestDoc(rt, "doc_conflict", "", `{"type": "conflict", "channels":["alpha"]}`)
	_, err = updateTestDoc(rt, "doc_conflict", revid, `{"type": "conflict", "channels":["alpha"]}`)
	newEdits_conflict := `{"type": "conflict", "channels":["alpha"],
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "19a316235cdd9d695d73765dc527d903"]}}`
	response = rt.SendAdminRequest("PUT", "/db/doc_conflict?new_edits=false", newEdits_conflict)
	assertStatus(t, response, 201)

	// Resolved conflict
	revid, err = updateTestDoc(rt, "doc_resolved_conflict", "", `{"type": "resolved_conflict", "channels":["alpha"]}`)
	_, err = updateTestDoc(rt, "doc_resolved_conflict", revid, `{"type": "resolved_conflict", "channels":["alpha"]}`)
	newEdits_conflict = `{"type": "resolved_conflict", "channels":["alpha"]},
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "4e123c0497a1a6975540977ec127c06c"]}}`
	response = rt.SendAdminRequest("PUT", "/db/doc_resolved_conflict?new_edits=false", newEdits_conflict)
	response = rt.SendAdminRequest("DELETE", "/db/doc_resolved_conflict?rev=2-conflicting_rev", "")

	expectedResults := make([]string, 10)
	expectedResults[0] = `{"seq":1,"id":"_user/user1","changes":[]}`
	expectedResults[1] = `{"seq":2,"id":"doc_active","doc":{"_id":"doc_active","_rev":"1-d59fda97ac4849f6a754fbcf4b522980","channels":["alpha"],"type":"active"},"changes":[{"rev":"1-d59fda97ac4849f6a754fbcf4b522980"}]}`
	expectedResults[2] = `{"seq":4,"id":"doc_multi_rev","doc":{"_id":"doc_multi_rev","_rev":"2-db2cf770921c3764b2d213ee0cbb5f45","channels":["alpha"],"type":"active","v":2},"changes":[{"rev":"2-db2cf770921c3764b2d213ee0cbb5f45"}]}`
	expectedResults[3] = `{"seq":6,"id":"doc_tombstone","deleted":true,"removed":["alpha"],"doc":{"_deleted":true,"_id":"doc_tombstone","_rev":"2-5bd8eb422f30e8d455940672e9e76549"},"changes":[{"rev":"2-5bd8eb422f30e8d455940672e9e76549"}]}`
	expectedResults[4] = `{"seq":8,"id":"doc_removed","removed":["alpha"],"doc":{"_id":"doc_removed","_removed":true,"_rev":"2-d15cb77d1dbe1cc06d27310de5b75914"},"changes":[{"rev":"2-d15cb77d1dbe1cc06d27310de5b75914"}]}`
	expectedResults[5] = `{"seq":12,"id":"doc_pruned","removed":["alpha"],"doc":{"_id":"doc_pruned","_removed":true,"_rev":"2-5afcb73bd3eb50615470e3ba54b80f00"},"changes":[{"rev":"2-5afcb73bd3eb50615470e3ba54b80f00"}]}`
	expectedResults[6] = `{"seq":18,"id":"doc_attachment","doc":{"_attachments":{"attach1":{"content_type":"text/plain","digest":"sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=","length":30,"revpos":2,"stub":true}},"_id":"doc_attachment","_rev":"2-0db6aecd6b91981e7f97c95ca64b5019","channels":["alpha"],"type":"attachments"},"changes":[{"rev":"2-0db6aecd6b91981e7f97c95ca64b5019"}]}`
	expectedResults[7] = `{"seq":19,"id":"doc_large_numbers","doc":{"_id":"doc_large_numbers","_rev":"1-2721633d9000e606e9c642e98f2f5ae7","channels":["alpha"],"largefloat":1234567890.1234,"largeint":1234567890,"type":"large_numbers"},"changes":[{"rev":"1-2721633d9000e606e9c642e98f2f5ae7"}]}`
	expectedResults[8] = `{"seq":22,"id":"doc_conflict","doc":{"_id":"doc_conflict","_rev":"2-conflicting_rev","channels":["alpha"],"type":"conflict"},"changes":[{"rev":"2-conflicting_rev"}]}`
	expectedResults[9] = `{"seq":24,"id":"doc_resolved_conflict","doc":{"_id":"doc_resolved_conflict","_rev":"2-251ba04e5889887152df5e7a350745b4","channels":["alpha"],"type":"resolved_conflict"},"changes":[{"rev":"2-251ba04e5889887152df5e7a350745b4"}]}`
	changesResponse := rt.Send(requestByUser("GET", "/db/_changes?include_docs=true", "", "user1"))

	// If we unmarshal results to db.ChangeEntry, json numbers get mangled by the test.  Validate against RawMessage to simplify.
	var changes struct {
		Results []*json.RawMessage
	}
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), len(expectedResults))

	for index, result := range changes.Results {
		assert.Equals(t, fmt.Sprintf("%s", *result), expectedResults[index])
	}

	// Flush the rev cache, and issue changes again to ensure successful handling for rev cache misses
	testDB.FlushRevisionCache()
	// Also nuke temporary revision backup of doc_pruned.  Validates that the body for the pruned revision is generated correctly when no longer resident in the rev cache
	testDB.Bucket.Delete("_sync:rev:doc_pruned:34:2-5afcb73bd3eb50615470e3ba54b80f00")

	postFlushChangesResponse := rt.Send(requestByUser("GET", "/db/_changes?include_docs=true", "", "user1"))

	var postFlushChanges struct {
		Results []*json.RawMessage
	}
	err = json.Unmarshal(postFlushChangesResponse.Body.Bytes(), &postFlushChanges)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(postFlushChanges.Results), len(expectedResults))

	for index, result := range postFlushChanges.Results {
		assert.Equals(t, fmt.Sprintf("%s", *result), expectedResults[index])
	}

	// Validate include_docs=false, style=all_docs permutations
	expectedStyleAllDocs := make([]string, 10)
	expectedStyleAllDocs[0] = `{"seq":1,"id":"_user/user1","changes":[]}`
	expectedStyleAllDocs[1] = `{"seq":2,"id":"doc_active","changes":[{"rev":"1-d59fda97ac4849f6a754fbcf4b522980"}]}`
	expectedStyleAllDocs[2] = `{"seq":4,"id":"doc_multi_rev","changes":[{"rev":"2-db2cf770921c3764b2d213ee0cbb5f45"}]}`
	expectedStyleAllDocs[3] = `{"seq":6,"id":"doc_tombstone","deleted":true,"removed":["alpha"],"changes":[{"rev":"2-5bd8eb422f30e8d455940672e9e76549"}]}`
	expectedStyleAllDocs[4] = `{"seq":8,"id":"doc_removed","removed":["alpha"],"changes":[{"rev":"2-d15cb77d1dbe1cc06d27310de5b75914"}]}`
	expectedStyleAllDocs[5] = `{"seq":12,"id":"doc_pruned","removed":["alpha"],"changes":[{"rev":"2-5afcb73bd3eb50615470e3ba54b80f00"}]}`
	expectedStyleAllDocs[6] = `{"seq":18,"id":"doc_attachment","changes":[{"rev":"2-0db6aecd6b91981e7f97c95ca64b5019"}]}`
	expectedStyleAllDocs[7] = `{"seq":19,"id":"doc_large_numbers","changes":[{"rev":"1-2721633d9000e606e9c642e98f2f5ae7"}]}`
	expectedStyleAllDocs[8] = `{"seq":22,"id":"doc_conflict","changes":[{"rev":"2-conflicting_rev"},{"rev":"2-869a7167ccbad634753105568055bd61"}]}`
	expectedStyleAllDocs[9] = `{"seq":24,"id":"doc_resolved_conflict","changes":[{"rev":"2-251ba04e5889887152df5e7a350745b4"}]}`

	styleAllDocsChangesResponse := rt.Send(requestByUser("GET", "/db/_changes?style=all_docs", "", "user1"))
	var allDocsChanges struct {
		Results []*json.RawMessage
	}
	err = json.Unmarshal(styleAllDocsChangesResponse.Body.Bytes(), &allDocsChanges)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(allDocsChanges.Results), len(expectedStyleAllDocs))
	for index, result := range allDocsChanges.Results {
		assert.Equals(t, fmt.Sprintf("%s", *result), expectedStyleAllDocs[index])
	}

	// Validate style=all_docs, include_docs=true permutations.  Only modified doc from include_docs test is doc_conflict (adds open revisions)
	expectedResults[8] = `{"seq":22,"id":"doc_conflict","doc":{"_id":"doc_conflict","_rev":"2-conflicting_rev","channels":["alpha"],"type":"conflict"},"changes":[{"rev":"2-conflicting_rev"},{"rev":"2-869a7167ccbad634753105568055bd61"}]}`

	combinedChangesResponse := rt.Send(requestByUser("GET", "/db/_changes?style=all_docs&include_docs=true", "", "user1"))
	var combinedChanges struct {
		Results []*json.RawMessage
	}

	err = json.Unmarshal(combinedChangesResponse.Body.Bytes(), &combinedChanges)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(combinedChanges.Results), len(expectedResults))
	for index, result := range combinedChanges.Results {
		assert.Equals(t, fmt.Sprintf("%s", *result), expectedResults[index])
	}
}

// Test _changes with channel filter
func changesActiveOnly(t *testing.T, it indexTester) {

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS", "ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response := it.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
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

// Tests view backfill and validates that results are prepended to cache
func TestChangesViewBackfill(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rt := RestTester{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS", "ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	testDb := rt.ServerContext().Database("db")
	testDb.WaitForSequence(3)

	// Flush the channel cache
	testDb.FlushChannelCache()

	// Add a few more docs (to increment the channel cache's validFrom)
	response = rt.SendAdminRequest("PUT", "/db/doc4", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/doc5", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	testDb.WaitForSequence(5)

	// Issue a since=0 changes request.  Validate that there's a view-based backfill
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"since":0, "limit":50}`
	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	queryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%s", queryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	assert.Equals(t, base.GetExpvarAsString("syncGateway_changeCache", "view_queries"), queryCount)

}

// Tests view backfill and validates that results are prepended to cache
func TestChangesViewBackfillStarChannel(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rt := RestTester{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("*"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/db/doc5", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc4", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	testDb := rt.ServerContext().Database("db")
	testDb.WaitForSequence(3)

	// Flush the channel cache
	testDb.FlushChannelCache()

	// Add a few more docs (to increment the channel cache's validFrom)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	testDb.WaitForSequence(5)

	// Issue a since=0 changes request.  Validate that there's a view-based backfill
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"since":0, "limit":50}`
	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for index, entry := range changes.Results {
		// Expects docs in sequence order from 1-5
		assert.Equals(t, entry.Seq.Seq, uint64(index+1))
		log.Printf("Entry:%+v", entry)
	}
	queryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%s", queryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for index, entry := range changes.Results {
		// Expects docs in sequence order from 1-5
		assert.Equals(t, entry.Seq.Seq, uint64(index+1))
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	assert.Equals(t, base.GetExpvarAsString("syncGateway_changeCache", "view_queries"), queryCount)

}

// Tests view backfill with slow query, checks duplicate handling for cache entries if a document is updated after query runs, but before document is
// prepended to the cache.  Reproduces #3475
func TestChangesViewBackfillSlowQuery(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip test with LeakyBucket dependency test when running in integration")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rt := RestTester{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put rev1 of document
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	var body db.Body
	json.Unmarshal(response.Body.Bytes(), &body)
	assert.Equals(t, body["ok"], true)
	revId := body["rev"].(string)

	testDb := rt.ServerContext().Database("db")
	testDb.WaitForSequence(1)

	log.Printf("about to flush")
	// Flush the channel cache
	testDb.FlushChannelCache()
	log.Printf("flush done")

	// Write another doc, to initialize the cache (and guarantee overlap)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	testDb.WaitForSequence(2)

	// Set up PostQueryCallback on bucket - will be invoked when changes triggers the cache backfill view query

	leakyBucket, ok := rt.Bucket().(*base.LeakyBucket)
	assertTrue(t, ok, "Bucket was not of type LeakyBucket")
	postQueryCallback := func(ddoc, viewName string, params map[string]interface{}) {
		log.Printf("Got callback for %s, %s, %v", ddoc, viewName, params)
		// Check which channel the callback was invoked for
		startkey, ok := params["startkey"].([]interface{})
		log.Printf("startkey: %v %T", startkey, startkey)
		channelName := ""
		if ok && len(startkey) > 1 {
			channelName, _ = startkey[0].(string)
		}
		if viewName == "channels" && channelName == "PBS" {
			// Update doc1
			log.Printf("Putting doc w/ revid:%s", revId)
			updateResponse := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/doc1?rev=%s", revId), `{"modified":true, "channels":["PBS"]}`)
			assertStatus(t, updateResponse, 201)
			testDb.WaitForSequence(3)
		}

	}
	leakyBucket.SetPostQueryCallback(postQueryCallback)

	// Issue a since=0 changes request.  Will cause the following:
	//   1. Retrieves doc2 from the cache
	//   2. View query retrieves doc1 rev-1
	//   3. PostQueryCallback forces doc1 rev-2 to be added to the cache
	//   4. doc1 rev-1 is prepended to the cache
	//   5. Returns 1 + 2 (doc 2, doc 1 rev-1).  This is correct (doc1 rev2 wasn't cached when the changes query was issued),
	//      but now the cache has two versions of doc1.  Subsequent changes request will returns duplicates.
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"since":0, "limit":50}`
	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 2)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	queryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%s", queryCount)

	leakyBucket.SetPostQueryCallback(nil)

	// Issue another since=0 changes request - cache SHOULD only have a single rev for doc1
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 2)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	updatedQueryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After second changes request, query count is :%s", updatedQueryCount)
	assert.Equals(t, updatedQueryCount, queryCount)

}

func TestChangesActiveOnlyWithLimit(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges)()

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS", "ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response := it.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc0", `{"channel":["PBS"]}`)
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

	//Create additional active docs
	response = it.SendAdminRequest("PUT", "/db/activeDoc1", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc2", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc3", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc4", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc5", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// TODO: Make db.waitForSequence public and use that instead of a sleep here
	time.Sleep(100 * time.Millisecond)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 10)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 3)
		}
	}

	// Active only NO Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// Active only with Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}
	// Active only with Limit, GET
	changes.Results = nil
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true&limit=5", "", "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}
}

// Test active-only and limit handling during cache backfill from the view.  Flushes the channel cache
// prior to changes requests in order to force view backfill.  Covers https://github.com/couchbase/sync_gateway/issues/2955 in
// additional to general view handling.
func TestChangesActiveOnlyWithLimitAndViewBackfill(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS", "ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response := it.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc0", `{"channel":["PBS"]}`)
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

	// Get pre-delete changes
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

	//Create additional active docs
	response = it.SendAdminRequest("PUT", "/db/activeDoc1", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc2", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc3", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc4", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc5", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// TODO: Make db.waitForSequence public and use that instead of a sleep here
	time.Sleep(100 * time.Millisecond)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 10)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 3)
		}
	}

	// Active only NO Limit, POST
	testDb := it.ServerContext().Database("db")
	testDb.FlushChannelCache()

	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// Active only with Limit, POST
	testDb.FlushChannelCache()
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// Active only with Limit, GET
	testDb.FlushChannelCache()
	changes.Results = nil
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true&limit=5", "", "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	testDb.FlushChannelCache()
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// No limit active only, GET, followed by normal (https://github.com/couchbase/sync_gateway/issues/2955)
	testDb.FlushChannelCache()
	changes.Results = nil
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true", "", "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	var updatedChanges struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesResponse = it.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &updatedChanges)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(updatedChanges.Results), 10)

}

func TestChangesActiveOnlyWithLimitLowRevCache(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	cacheSize := 2
	shortWaitConfig := &DbConfig{
		CacheConfig: &CacheConfig{
			ChannelCacheMinLength: &cacheSize,
			ChannelCacheMaxLength: &cacheSize,
		},
	}

	rt := RestTester{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	defer rt.Close()

	///it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	//defer it.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS", "ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response := rt.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/activeDoc0", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	json.Unmarshal(response.Body.Bytes(), &body)
	partialRemovalRev := body["rev"].(string)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/conflictedDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Create a conflict, then tombstone it
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictTombstone"}], "new_edits":false}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("DELETE", "/db/conflictedDoc?rev=1-conflictTombstone", "")
	assertStatus(t, response, 200)

	// Create a conflict, and don't tombstone it
	response = rt.SendAdminRequest("POST", "/db/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictActive"}], "new_edits":false}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Get pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)

	// Delete
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/deletedDoc?rev=%s", deletedRev), "")
	assertStatus(t, response, 200)

	// Removed
	response = rt.SendAdminRequest("PUT", "/db/removedDoc", fmt.Sprintf(`{"_rev":%q, "channel":["HBO"]}`, removedRev))
	assertStatus(t, response, 201)

	// Partially removed
	response = rt.SendAdminRequest("PUT", "/db/partialRemovalDoc", fmt.Sprintf(`{"_rev":%q, "channel":["PBS"]}`, partialRemovalRev))
	assertStatus(t, response, 201)

	//Create additional active docs
	response = rt.SendAdminRequest("PUT", "/db/activeDoc1", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/activeDoc2", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/activeDoc3", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/activeDoc4", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/activeDoc5", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	time.Sleep(100 * time.Millisecond)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 10)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 3)
		}
	}

	// Active only NO Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// Active only with Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}
	// Active only with Limit, GET
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true&limit=5", "", "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equals(t, len(entry.Changes), 2)
		}
	}
}

// Test _changes returning conflicts
func TestChangesIncludeConflicts(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache|base.KeyChanges|base.KeyCRUD)()

	rt := RestTester{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	defer rt.Close()

	// Create conflicted document documents
	response := rt.SendAdminRequest("PUT", "/db/conflictedDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Create two conflicting revisions
	response = rt.SendAdminRequest("PUT", "/db/conflictedDoc?new_edits=false", `{"_revisions":{"start":2, "ids":["conflictOne", "82214a562e80c8fa7b2361719847bc73"]}, "value":"c1", "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/conflictedDoc?new_edits=false", `{"_revisions":{"start":2, "ids":["conflictTwo", "82214a562e80c8fa7b2361719847bc73"]}, "value":"c2", "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Get changes
	rt.ServerContext().Database("db").WaitForPendingChanges()

	changesResponse := rt.SendAdminRequest("GET", "/db/_changes?style=all_docs", "")
	err := json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("changes response: %s", changesResponse.Body.Bytes())
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, len(changes.Results[0].Changes), 2)

}

// Test _changes handling large sequence values - ensures no truncation of large ints.
func TestChangesLargeSequences(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("TestChangesLargeSequences doesn't support walrus - needs to customize _sync:seq prior to db creation")
	}

	initialSeq := uint64(9223372036854775807)
	rt := RestTester{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`,
		InitSyncSeq: initialSeq}
	defer rt.Close()

	// Create document
	response := rt.SendAdminRequest("PUT", "/db/largeSeqDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Get changes
	rt.ServerContext().Database("db").WaitForPendingChanges()

	changesResponse := rt.SendAdminRequest("GET", "/db/_changes?since=9223372036854775800", "")
	err := json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 1)
	assert.Equals(t, changes.Results[0].Seq.Seq, uint64(9223372036854775808))
	assert.Equals(t, changes.Last_Seq, "9223372036854775808")

	// Validate incoming since value isn't being truncated
	changesResponse = rt.SendAdminRequest("GET", "/db/_changes?since=9223372036854775808", "")
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 0)

	// Validate incoming since value isn't being truncated
	changesResponse = rt.SendAdminRequest("POST", "/db/_changes", `{"since":9223372036854775808}`)
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 0)

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

func WriteDirect(testDb *db.DatabaseContext, channelArray []string, sequence uint64) {
	docId := fmt.Sprintf("doc-%v", sequence)
	WriteDirectWithKey(testDb, docId, channelArray, sequence)
}

func WriteDirectWithKey(testDb *db.DatabaseContext, key string, channelArray []string, sequence uint64) {

	if base.TestUseXattrs() {
		panic(fmt.Sprintf("WriteDirectWithKey() cannot be used in tests that are xattr enabled"))
	}

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
