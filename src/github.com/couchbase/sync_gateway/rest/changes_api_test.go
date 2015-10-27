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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
)

type indexTester struct {
	restTester
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
	bucketName := "sg_bucket"

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
		indexBucketName := "sg_index_bucket"
		channelIndexConfig := &ChannelIndexConfig{
			BucketConfig: BucketConfig{
				Server: &serverName,
				Bucket: &indexBucketName,
			},
			IndexWriter: true,
		}
		dbConfig.ChannelIndex = channelIndexConfig
	}

	_, err := it._sc.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}

	it._bucket = it._sc.Database("db").Bucket

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
	assert.Equals(t, since, db.SequenceID{Seq: 1})

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
	response := it.sendAdminRequest("PUT", "/_logging", `{"Poll+":true}`)

	//response := it.sendAdminRequest("PUT", "/_logging", `{"Changes":true, "Changes+":true, "HTTP":true, "DIndex+":true}`)
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
	changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
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
	time.Sleep(1 * time.Second)
	changesJSON = fmt.Sprintf(`{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"%s"}`, changes.Last_Seq)
	changesResponse = it.send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = json.Unmarshal(changesResponse.Body.Bytes(), &changes)
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
	assert.Equals(t, len(changes.Results), 7)

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
