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
	"sync"
	"testing"
	"time"

	"github.com/couchbaselabs/go.assert"

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
		Persona:  &PersonaConfig{},
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
		err := base.SeedTestPartitionMap(dbContext.GetIndexBucket(), 64)
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
	time.Sleep(100 * time.Millisecond)
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

func TestPostChangesInteger(t *testing.T) {

	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChanges(t, it)
}

func TestPostChangesClock(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
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

func TestPostChangesSameVbucket(t *testing.T) {

	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("PBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents with ids that hash to the same vbucket
	response = it.sendAdminRequest("PUT", "/db/pbs0000609", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs0000799", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs0003428", `{"value":3, "channel":["PBS"]}`)
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

func TestPostChangesSinceClock(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChangesSince(t, it)
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

func TestPostChangesChannelFilterClock(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
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
	time.Sleep(10 * time.Millisecond)
	changesResponse = it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	for _, result := range changes.Results {
		log.Printf("changes result:%+v", result)
	}
	assert.Equals(t, len(changes.Results), 7)

}

func TestMultiChannelUserAndDocs(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "Debug":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("ABC", "NBC", "CBS"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put documents in ABC
	for a := 0; a < 30; a++ {
		// Put a doc with id "ABC_[a]"
		url := fmt.Sprintf("/db/ABC_%d", a)
		response = it.sendAdminRequest("PUT", url, `{"channel":["ABC"]}`)
		assertStatus(t, response, 201)
	}

	// Put documents in NBC, CBS
	for nc := 0; nc < 40; nc++ {
		url := fmt.Sprintf("/db/NBC_CBS_%d", nc)
		response = it.sendAdminRequest("PUT", url, `{"channel":["NBC","CBS"]}`)
		assertStatus(t, response, 201)
	}

	// Put documents in ABC, NBC, CBS
	for anc := 0; anc < 60; anc++ {
		url := fmt.Sprintf("/db/ABC_NBC_CBS_%d", anc)
		response = it.sendAdminRequest("PUT", url, `{"channel":["ABC","NBC","CBS"]}`)
		assertStatus(t, response, 201)
	}

	time.Sleep(10 * time.Millisecond)

	type simpleChangeResult struct {
		Seq string
		ID  string
	}

	var changes struct {
		Results  []simpleChangeResult
		Last_Seq interface{}
	}
	//changesJSON := `{"filter":"sync_gateway/bychannel", "channels":"PBS"}`
	changesResponse := it.send(requestByUser("GET", "/db/_changes", "", "bernard"))

	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 130)
	/*
		for _, result := range changes.Results {
			log.Printf("result: {%+v, %+v}", result.ID, result.Seq)
		}
		log.Printf("last_seq:%v", changes.Last_Seq)
	*/
}

func TestDocDeduplication(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	var writeResponse struct {
		Id  string
		Rev string
	}
	revIds := make([]string, 10)
	// Put documents in ABC
	for a := 0; a < 10; a++ {
		// Put a doc with id "ABC_[a]"
		url := fmt.Sprintf("/db/ABC_%d", a)
		response = it.sendAdminRequest("PUT", url, `{"channel":["ABC"]}`)
		json.Unmarshal(response.Body.Bytes(), &writeResponse)
		revIds[a] = writeResponse.Rev
		assertStatus(t, response, 201)
	}

	// Update some docs.  Based on body, new rev should be 2-2962c4281696f5b17dfdccd02d858114

	url := fmt.Sprintf("/db/ABC_3?rev=%s", revIds[3])
	response = it.sendAdminRequest("PUT", url, `{"modified":true, "channel":["ABC"]}`)

	url = fmt.Sprintf("/db/ABC_6?rev=%s", revIds[6])
	response = it.sendAdminRequest("PUT", url, `{"modified":true, "channel":["ABC"]}`)

	url = fmt.Sprintf("/db/ABC_7?rev=%s", revIds[7])
	response = it.sendAdminRequest("PUT", url, `{"modified":true, "channel":["ABC"]}`)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	time.Sleep(1 * time.Second)

	//changesJSON := `{"filter":"sync_gateway/bychannel", "channels":"PBS"}`
	changesResponse := it.send(requestByUser("GET", "/db/_changes", "", "bernard"))

	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), 10)

	// Verify that we're getting the correct revision for modified docs
	for _, change := range changes.Results {
		log.Printf("change: %+v", change)
		var expectedRev string
		if change.ID == "ABC_3" || change.ID == "ABC_6" || change.ID == "ABC_7" {
			expectedRev = "2-2962c4281696f5b17dfdccd02d858114"
		} else {
			expectedRev = revIds[0]
		}
		rev, ok := change.Changes[0]["rev"]
		assert.Equals(t, ok, true)
		assert.Equals(t, rev, expectedRev)
	}

}

func TestIndexChangesMultipleRevisions(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	var writeResponse struct {
		Id  string
		Rev string
	}

	// Start 10 goroutines, creating 100 docs each, in channel ABC
	var wg sync.WaitGroup
	numWriters := 0
	docsPerWriter := 100
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			revIds := make([]string, docsPerWriter)
			for j := 0; j < docsPerWriter; j++ {
				docID := fmt.Sprintf("doc-%d", i*docsPerWriter+j)
				url := fmt.Sprintf("/db/%s", docID)
				response = it.sendAdminRequest("PUT", url, `{"channel":["ABC"]}`)
				json.Unmarshal(response.Body.Bytes(), &writeResponse)
				revIds[j] = writeResponse.Rev
			}
			// write revisions
			for j := 0; j < docsPerWriter; j++ {
				docID := fmt.Sprintf("doc-%d", i*docsPerWriter+j)
				url := fmt.Sprintf("/db/%s?rev=%s", docID, revIds[j])
				response = it.sendAdminRequest("PUT", url, `{"modified":true, "channel":["ABC"]}`)

			}

		}(i)
	}
	wg.Wait()
	// Wait for indexing
	time.Sleep(1 * time.Second)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesResponse := it.send(requestByUser("GET", "/db/_changes", "", "bernard"))

	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
	assertNoError(t, err, "Error unmarshalling changes response")
	assert.Equals(t, len(changes.Results), numWriters*docsPerWriter)

}

func TestPostChangesAdminChannelGrantInteger(t *testing.T) {
	it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChangesAdminChannelGrant(t, it)
}

func TempTestPostChangesAdminChannelGrantClock(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	postChangesAdminChannelGrant(t, it)
	it._bucket.Dump()
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

func TestChangesActiveOnlyClock(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel);}`)
	defer it.Close()
	changesActiveOnly(t, it)
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

	time.Sleep(10 * time.Millisecond)

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

func DisableTestChangesAccessNotifyInteger(t *testing.T) {

	it := initIndexTester(false, `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);}`)
	defer it.Close()
	postChangesAccessNotify(t, it)
}

// Disabled until walrus supports vb info in view meta.  Currently requires real Couchbase connection to include vb info in the view response.
func DisabledTestChangesAccessNotifyClock(t *testing.T) {
	it := initIndexTester(true, `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);}`)
	defer it.Close()
	postChangesAccessNotify(t, it)
}

func postChangesAccessNotify(t *testing.T, it indexTester) {

	response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
	assert.True(t, response != nil)

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf("ABC"))
	assert.True(t, err == nil)
	a.Save(bernard)

	// Put several documents in channel PBS
	response = it.sendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.sendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Start longpoll changes request
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var changes struct {
			Results  []db.ChangeEntry
			Last_Seq db.SequenceID
		}
		changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
		changesResponse := it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
		err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
		assert.Equals(t, len(changes.Results), 3)
	}()

	// Wait for changes to start.  Unreliable as we don't know how long it will take the above goroutine to
	// initialize the longpoll request when running under race - needs a better approach long-term
	time.Sleep(1 * time.Second)

	// Put document that triggers access grant for user, PBS
	response = it.sendAdminRequest("PUT", "/db/access1", `{"accessUser":"bernard", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

	wg.Wait()
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
