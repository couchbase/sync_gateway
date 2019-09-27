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
	"context"
	"encoding/json"
	"errors"
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
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type indexTester struct {
	RestTester
	_indexBucket base.Bucket
}

func initRestTester(syncFn string, testing testing.TB) *indexTester {
	return initIndexTester(syncFn, testing)
}

func initIndexTester(syncFn string, tb testing.TB) *indexTester {

	it := &indexTester{RestTester: *NewRestTester(tb, nil)}
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

	feedType := "tap"

	dbConfig := &DbConfig{
		BucketConfig: BucketConfig{
			Server: &serverName,
			Bucket: &bucketName},
		Name:     "db",
		Sync:     syncFnPtr,
		FeedType: feedType,
		UseViews: true, // walrus only supports views
	}

	_, err := it.RestTesterServerContext.AddDatabaseFromConfig(dbConfig)
	if err != nil {
		panic(fmt.Sprintf("Error from AddDatabaseFromConfig: %v", err))
	}

	it.RestTesterBucket = it.RestTesterServerContext.Database("db").Bucket

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

	rt := NewRestTester(t, nil)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()
	user, err := a.NewUser("ben", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err, "Error creating new user")
	a.Save(user)

	testDb := rt.ServerContext().Database("db")

	// Put several documents
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(3)
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.Wait()
	testDb.FlushChannelCache()

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	leakyBucket, ok := rt.Bucket().(*base.LeakyBucket)
	assert.True(t, ok, "Bucket was not of type LeakyBucket")
	// Force a partial error for the first ViewCustom call we make to initialize an invalid cache.
	leakyBucket.SetFirstTimeViewCustomPartialError(true)

	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")

	// In the first changes request since cache flush, we're forcing a nil cache with no error. Thereforce we'd expect to see zero results.
	assert.Equal(t, 0, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")

	// Now we should expect 3 results, as the invalid cache was not persisted.
	// The second call to ViewCustom will succeed and properly create the channel cache.
	assert.Equal(t, 3, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

	// if we create another doc, the cache will get updated with the latest doc.
	cacheWaiter.Add(1)
	response = rt.SendAdminRequest("PUT", "/db/doc4", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.Wait()

	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")

	assert.Equal(t, 4, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

}

func TestDocDeletionFromChannel(t *testing.T) {
	// See https://github.com/couchbase/couchbase-lite-ios/issues/59
	// base.LogKeys["Changes"] = true
	// base.LogKeys["Cache"] = true

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	a := rt.ServerContext().Database("db").Authenticator()

	// Create user:
	alice, _ := a.NewUser("alice", "letmein", channels.SetOf(t, "zero"))
	a.Save(alice)

	// Create a doc Alice can see:
	response := rt.Send(request("PUT", "/db/alpha", `{"channel":"zero"}`))

	// Check the _changes feed:
	require.NoError(t, rt.WaitForPendingChanges())
	var changes struct {
		Results []db.ChangeEntry
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 1, len(changes.Results))
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)

	assert.Equal(t, "alpha", changes.Results[0].ID)
	rev1 := changes.Results[0].Changes[0]["rev"]

	// Delete the document:
	assertStatus(t, rt.Send(request("DELETE", "/db/alpha?rev="+rev1, "")), 200)

	// Get the updates from the _changes feed:
	rt.WaitForPendingChanges()
	response = rt.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", since),
		"", "alice"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	changes.Results = nil
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 1, len(changes.Results))

	assert.Equal(t, "alpha", changes.Results[0].ID)
	assert.Equal(t, true, changes.Results[0].Deleted)
	goassert.DeepEquals(t, changes.Results[0].Removed, base.SetOf("zero"))
	rev2 := changes.Results[0].Changes[0]["rev"]

	// Now get the deleted revision:
	response = rt.Send(requestByUser("GET", "/db/alpha?rev="+rev2, "", "alice"))
	assert.Equal(t, 200, response.Code)
	log.Printf("Deletion looks like: %s", response.Body.Bytes())
	var docBody db.Body
	base.JSONUnmarshal(response.Body.Bytes(), &docBody)
	goassert.DeepEquals(t, docBody, db.Body{db.BodyId: "alpha", db.BodyRev: rev2, db.BodyDeleted: true})

	// Access without deletion revID shouldn't be allowed (since doc is not in Alice's channels):
	response = rt.Send(requestByUser("GET", "/db/alpha", "", "alice"))
	assert.Equal(t, 403, response.Code)

	// A bogus rev ID should return a 404:
	response = rt.Send(requestByUser("GET", "/db/alpha?rev=bogus", "", "alice"))
	assert.Equal(t, 404, response.Code)

	// Get the old revision, which should still be accessible:
	response = rt.Send(requestByUser("GET", "/db/alpha?rev="+rev1, "", "alice"))
	assert.Equal(t, 200, response.Code)
}

func TestPostChangesInteger(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP)()

	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
	defer it.Close()

	postChanges(t, it)
}

func postChanges(t *testing.T, it *indexTester) {

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err)
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

	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 3, len(changes.Results))

}

// Tests race between waking up the changes feed, and detecting that the user doc has changed
func TestPostChangesUserTiming(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP)()

	it := initIndexTester(`function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel)}`, t)
	defer it.Close()

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "bernard"))
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

	caughtUpCount := base.ExpvarVar2Int(it.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsCaughtUp))

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
		err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
		assert.NoError(t, err, "Error unmarshalling changes response")

		if len(changes.Results) != 3 {
			log.Printf("len(changes.Results) != 3, dumping changes response for diagnosis")
			log.Printf("changes: %+v", changes)
			log.Printf("changesResponse status code: %v.  Headers: %+v", changesResponse.Code, changesResponse.Header())
			log.Printf("changesResponse raw body: %s", changesResponse.Body.String())
		}
		assert.Equal(t, 3, len(changes.Results))
	}()

	// Wait for changes feed to get into wait mode where it is blocked on the longpoll changes feed response
	it.GetDatabase().WaitForCaughtUp(caughtUpCount + 1)

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

	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
	defer it.Close()

	postChangesSince(t, it)
}

func TestPostChangesWithQueryString(t *testing.T) {
	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
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

	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 4, len(changes.Results))

	// Test channel filter
	var filteredChanges struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	changesJSON = `{"feed":"longpoll"}`
	changesResponse = it.SendAdminRequest("POST", "/db/_changes?feed=longpoll&filter=sync_gateway/bychannel&channels=ABC", changesJSON)

	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &filteredChanges)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 1, len(filteredChanges.Results))
}

// Basic _changes test with since value
func postChangesSince(t *testing.T, it *indexTester) {

	// Create user
	response := it.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@bb.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter := it.GetDatabase().NewDCPCachingCountWaiter(t)

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
	cacheWaiter.AddAndWait(5)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("Changes:%s", changesResponse.Body.Bytes())
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))

	// Put several more documents, some to the same vbuckets
	response = it.SendAdminRequest("PUT", "/db/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs4", `{"value":4, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	cacheWaiter.AddAndWait(4)

	changesJSON = fmt.Sprintf(`{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"%s"}`, changes.Last_Seq)
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("Changes:%s", changesResponse.Body.Bytes())
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 3, len(changes.Results))

}

func TestPostChangesChannelFilterInteger(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP)()

	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
	defer it.Close()

	postChangesChannelFilter(t, it)
}

// Test _changes with channel filter
func postChangesChannelFilter(t *testing.T, it *indexTester) {

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err)
	a.Save(bernard)

	cacheWaiter := it.GetDatabase().NewDCPCachingCountWaiter(t)
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
	cacheWaiter.AddAndWait(5)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	changesJSON := `{"filter":"sync_gateway/bychannel", "channels":"PBS"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))

	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 4, len(changes.Results))

	// Put several more documents, some to the same vbuckets
	response = it.SendAdminRequest("PUT", "/db/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/pbs4", `{"value":4, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	cacheWaiter.AddAndWait(4)
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	for _, result := range changes.Results {
		log.Printf("changes result:%+v", result)
	}
	assert.Equal(t, 7, len(changes.Results))

}

func TestPostChangesAdminChannelGrantInteger(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP)()

	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
	defer it.Close()
	postChangesAdminChannelGrant(t, it)
}

// _changes with admin-based channel grant
func postChangesAdminChannelGrant(t *testing.T, it *indexTester) {

	// Create user with access to channel ABC:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "ABC"))
	assert.NoError(t, err)
	a.Save(bernard)

	cacheWaiter := it.GetDatabase().NewDCPCachingCountWaiter(t)

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
	cacheWaiter.AddAndWait(5)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Issue simple changes request
	changesResponse := it.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	assertStatus(t, changesResponse, 200)

	log.Printf("Response:%+v", changesResponse.Body)
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 1, len(changes.Results))

	// Update the user doc to grant access to PBS
	response = it.SendAdminRequest("PUT", "/db/_user/bernard", `{"admin_channels":["ABC", "PBS"]}`)
	assertStatus(t, response, 200)

	time.Sleep(500 * time.Millisecond)

	// Issue a new changes request with since=last_seq ensure that user receives all records for channel PBS
	changesResponse = it.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", changes.Last_Seq),
		"", "bernard"))
	assertStatus(t, changesResponse, 200)

	log.Printf("Response:%+v", changesResponse.Body)
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	assert.Equal(t, 5, len(changes.Results)) // 4 PBS docs, plus the updated user doc

	// Write a few more docs
	response = it.SendAdminRequest("PUT", "/db/pbs-5", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/abc-2", `{"channel":["ABC"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.AddAndWait(2)

	// Issue another changes request - ensure we don't backfill again
	changesResponse = it.Send(requestByUser("GET", fmt.Sprintf("/db/_changes?since=%s", changes.Last_Seq),
		"", "bernard"))
	assertStatus(t, changesResponse, 200)
	log.Printf("Response:%+v", changesResponse.Body)
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	assert.Equal(t, 2, len(changes.Results)) // 2 docs

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
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := NewRestTester(t, &rtConfig)
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
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 6))

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	require.Equal(t, 4, len(changes.Results))
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "2::6", changes.Last_Seq.String())

	// Send another changes request before any changes, with the last_seq received from the last changes ("2::6")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 0, len(changes.Results))

	// Send a missing doc - low sequence should move to 3
	WriteDirect(testDb, []string{"PBS"}, 3)
	require.NoError(t, rt.WaitForSequence(3))

	// WaitForSequence doesn't wait for low sequence to be updated on each channel - additional delay to ensure
	// low is updated before making the next changes request.
	time.Sleep(50 * time.Millisecond)

	// Send another changes request with the same since ("2::6") to ensure we see data once there are changes
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 3, len(changes.Results))

	// Send a later doc - low sequence still 3, high sequence goes to 7
	WriteDirect(testDb, []string{"PBS"}, 7)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 7))

	// Send another changes request with the same since ("2::6") to ensure we see data once there are changes
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 1, len(changes.Results))

}

// Test low sequence handling of late arriving sequences to a one-shot changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequenceOneShotUser(t *testing.T) {

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
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	// Create user:
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/bernard", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@couchbase.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	// Simulate 4 non-skipped writes (seq 2,3,4,5)
	WriteDirect(testDb, []string{"PBS"}, 2)
	WriteDirect(testDb, []string{"PBS"}, 3)
	WriteDirect(testDb, []string{"PBS"}, 4)
	WriteDirect(testDb, []string{"PBS"}, 5)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 5))

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq string
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes 1 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 5, len(changes.Results)) // Includes user doc
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "5", changes.Last_Seq)

	// Skip sequence 6, write docs 7-10
	WriteDirect(testDb, []string{"PBS"}, 7)
	WriteDirect(testDb, []string{"PBS"}, 8)
	WriteDirect(testDb, []string{"PBS"}, 9)
	WriteDirect(testDb, []string{"PBS"}, 10)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 10))

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 2 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 4, len(changes.Results))
	assert.Equal(t, "5::10", changes.Last_Seq)

	// Write a few more docs
	WriteDirect(testDb, []string{"PBS"}, 11)
	WriteDirect(testDb, []string{"PBS"}, 12)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 12))

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 3 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 2, len(changes.Results))
	assert.Equal(t, "5::12", changes.Last_Seq)

	// Write another doc, then the skipped doc - both should be sent, last_seq should move to 13
	WriteDirect(testDb, []string{"PBS"}, 13)
	WriteDirect(testDb, []string{"PBS"}, 6)
	require.NoError(t, rt.WaitForSequence(13))

	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 4 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)

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
		assert.Equal(t, "5::12", changes.Last_Seq)
	case 1:
		switch changes.Last_Seq {
		case "5::13":
			assert.Equal(t, uint64(13), changes.Results[0].Seq.Seq)
		case "6":
			assert.Equal(t, uint64(6), changes.Results[0].Seq.Seq)
			log.Printf("Didn't expect last:6 w/ seq 6")
		case "12":
			assert.Equal(t, uint64(6), changes.Results[0].Seq.Seq)
			log.Printf("Didn't expect last:12 w/ seq 6")
		default:
			t.Errorf("invalid response.  Last_Seq: %v  changes: %v", changes.Last_Seq, changes.Results[0])
		}
	case 7:
		assert.Equal(t, "12", changes.Last_Seq)
	case 8:
		assert.Equal(t, "13", changes.Last_Seq)
	default:
		t.Errorf("Unexpected number of changes results.  Last_Seq: %v  len(changes): %v", changes.Last_Seq, len(changes.Results))
	}

}

// Test low sequence handling of late arriving sequences to a one-shot changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequenceOneShotAdmin(t *testing.T) {

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
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	// Simulate 5 non-skipped writes (seq 1,2,3,4,5)
	WriteDirect(testDb, []string{"PBS"}, 1)
	WriteDirect(testDb, []string{"PBS"}, 2)
	WriteDirect(testDb, []string{"PBS"}, 3)
	WriteDirect(testDb, []string{"PBS"}, 4)
	WriteDirect(testDb, []string{"PBS"}, 5)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 5))

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq string
	}
	response := rt.SendAdminRequest("GET", "/db/_changes", "")
	log.Printf("_changes 1 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 5, len(changes.Results))
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "5", changes.Last_Seq)

	// Skip sequence 6, write docs 7-10
	WriteDirect(testDb, []string{"PBS"}, 7)
	WriteDirect(testDb, []string{"PBS"}, 8)
	WriteDirect(testDb, []string{"PBS"}, 9)
	WriteDirect(testDb, []string{"PBS"}, 10)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 10))

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.SendAdminRequest("POST", "/db/_changes", changesJSON)
	log.Printf("_changes 2 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 4, len(changes.Results))
	assert.Equal(t, "5::10", changes.Last_Seq)

	// Write a few more docs
	WriteDirect(testDb, []string{"PBS"}, 11)
	WriteDirect(testDb, []string{"PBS"}, 12)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 12))

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.SendAdminRequest("POST", "/db/_changes", changesJSON)
	log.Printf("_changes 3 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 2, len(changes.Results))
	assert.Equal(t, "5::12", changes.Last_Seq)

	// Write another doc, then the skipped doc - both should be sent, last_seq should move to 13
	WriteDirect(testDb, []string{"PBS"}, 13)
	WriteDirect(testDb, []string{"PBS"}, 6)
	require.NoError(t, rt.WaitForSequence(13))

	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.SendAdminRequest("POST", "/db/_changes", changesJSON)
	log.Printf("_changes 4 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)

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
		assert.Equal(t, "5::12", changes.Last_Seq)
	case 1:
		switch changes.Last_Seq {
		case "5::13":
			assert.Equal(t, uint64(13), changes.Results[0].Seq.Seq)
		case "6":
			assert.Equal(t, uint64(6), changes.Results[0].Seq.Seq)
			log.Printf("Didn't expect last:6 w/ seq 6")
		case "12":
			assert.Equal(t, uint64(6), changes.Results[0].Seq.Seq)
			log.Printf("Didn't expect last:12 w/ seq 6")
		default:
			t.Errorf("invalid response.  Last_Seq: %v  changes: %v", changes.Last_Seq, changes.Results[0])
		}
	case 7:
		assert.Equal(t, "12", changes.Last_Seq)
	case 8:
		assert.Equal(t, "13", changes.Last_Seq)
	default:
		t.Errorf("Unexpected number of changes results.  Last_Seq: %v  len(changes): %v", changes.Last_Seq, len(changes.Results))
	}

}

// Test low sequence handling of late arriving sequences to a longpoll changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequenceLongpollUser(t *testing.T) {

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
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")

	// Create user:
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/bernard", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"email":"bernard@couchbase.com", "password":"letmein", "admin_channels":["PBS"]}`)
	assertStatus(t, response, 201)

	// Simulate 4 non-skipped writes (seq 2,3,4,5)
	WriteDirect(testDb, []string{"PBS"}, 2)
	WriteDirect(testDb, []string{"PBS"}, 3)
	WriteDirect(testDb, []string{"PBS"}, 4)
	WriteDirect(testDb, []string{"PBS"}, 5)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 5))

	// Check the _changes feed:
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq string
	}
	response = rt.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	log.Printf("_changes 1 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 5, len(changes.Results)) // Includes user doc
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "5", changes.Last_Seq)

	// Skip sequence 6, write docs 7-10
	WriteDirect(testDb, []string{"PBS"}, 7)
	WriteDirect(testDb, []string{"PBS"}, 8)
	WriteDirect(testDb, []string{"PBS"}, 9)
	WriteDirect(testDb, []string{"PBS"}, 10)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 10))

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 2 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 4, len(changes.Results))
	assert.Equal(t, "5::10", changes.Last_Seq)

	// Write a few more docs
	WriteDirect(testDb, []string{"PBS"}, 11)
	WriteDirect(testDb, []string{"PBS"}, 12)
	require.NoError(t, testDb.WaitForSequenceNotSkipped(context.TODO(), 12))

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	response = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	log.Printf("_changes 3 looks like: %s", response.Body.Bytes())
	base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, 2, len(changes.Results))
	assert.Equal(t, "5::12", changes.Last_Seq)

	caughtUpCount := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsCaughtUp))
	// Issue a longpoll changes request.  Will block.
	var longpollWg sync.WaitGroup
	longpollWg.Add(1)
	go func() {
		defer longpollWg.Done()
		longpollChangesJSON := fmt.Sprintf(`{"since":"%s", "feed":"longpoll"}`, changes.Last_Seq)
		log.Printf("starting longpoll changes w/ JSON: %s", longpollChangesJSON)
		longPollResponse := rt.Send(requestByUser("POST", "/db/_changes", longpollChangesJSON, "bernard"))
		log.Printf("longpoll changes looks like: %s", longPollResponse.Body.Bytes())
		base.JSONUnmarshal(longPollResponse.Body.Bytes(), &changes)
		// Expect to get 6 through 12
		assert.Equal(t, 7, len(changes.Results))
		assert.Equal(t, "12", changes.Last_Seq)
	}()

	// Wait for longpoll to get into wait mode
	require.NoError(t, rt.GetDatabase().WaitForCaughtUp(caughtUpCount+1))

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

func TestConcurrentDelete(t *testing.T) {
	_testConcurrentDelete(t)
}

func _testConcurrentDelete(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.Equal(t, true, body["ok"])
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
	require.NoError(t, rt.WaitForPendingChanges())

	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)

	// Wait for writes to be processed and indexed
	require.NoError(t, rt.WaitForPendingChanges())

}

func _testConcurrentPutAsDelete(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.Equal(t, true, body["ok"])
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
	require.NoError(t, rt.WaitForPendingChanges())

	// Write another doc, to validate sequences restart
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)

	require.NoError(t, rt.WaitForPendingChanges())
}

func _testConcurrentUpdate(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.Equal(t, true, body["ok"])
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
	require.NoError(t, rt.WaitForPendingChanges())

	// Write another doc, to validate sequences restart
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)

	require.NoError(t, rt.WaitForPendingChanges())
}

func _testConcurrentNewEditsFalseDelete(t *testing.T) {

	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	var body db.Body
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.Equal(t, true, body["ok"])
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
	require.NoError(t, rt.WaitForPendingChanges())

	// Write another doc, to see where sequences are at
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channel":"PBS"}`)
	assertStatus(t, response, 201)
	require.NoError(t, rt.WaitForPendingChanges())

}

func TestChangesActiveOnlyInteger(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP)()

	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
	defer it.Close()
	changesActiveOnly(t, it)
}

func TestOneShotChangesWithExplicitDocIds(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`}
	rt := NewRestTester(t, &rtConfig)
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
	err := base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(changes.Results))
	assert.Equal(t, "doc4", changes.Results[1].ID)

	//User has access to different single channel
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "docB", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user2", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(changes.Results))
	assert.Equal(t, "docD", changes.Results[2].ID)

	//User has access to multiple channels
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(changes.Results))
	assert.Equal(t, "docD", changes.Results[3].ID)

	//User has no channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user4", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(changes.Results))

	//User has "*" channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 5, len(changes.Results))

	//User has "*" channel access, override POST with GET params
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	request, _ = http.NewRequest("POST", `/db/_changes?doc_ids=["docC","doc1"]`, bytes.NewBufferString(body))
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, len(changes.Results))

	//User has "*" channel access, use GET
	request, _ = http.NewRequest("GET", `/db/_changes?filter=_doc_ids&doc_ids=["docC","doc1","docD"]`, nil)
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(changes.Results))

	//User has "*" channel access, use GET with doc_ids plain comma separated list
	request, _ = http.NewRequest("GET", `/db/_changes?filter=_doc_ids&doc_ids=docC,doc1,doc2,docD`, nil)
	request.SetBasicAuth("user5", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(changes.Results))

	//Admin User
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "docA"]}`
	response = rt.SendAdminRequest("POST", "/db/_changes", body)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(changes.Results))

	//Use since value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 3, len(changes.Results))
	assert.Equal(t, "docD", changes.Results[2].ID)

	//Use since value and limit value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6, "limit":1}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "doc4", changes.Results[0].ID)

	//test parameter include_docs=true
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "include_docs":true }`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(changes.Results))
	assert.Equal(t, "docD", changes.Results[3].ID)
	var docBody db.Body
	assert.NoError(t, base.JSONUnmarshal(changes.Results[3].Doc, &docBody))
	assert.Equal(t, "docD", docBody[db.BodyId])

	//test parameter style=all_docs
	//Create a conflict revision on docC
	assertStatus(t, rt.SendRequest("POST", "/db/_bulk_docs", `{"new_edits":false, "docs": [{"_id": "docC", "_rev": "2-b4afc58d8e61a6b03390e19a89d26643","foo": "bat", "channels":["beta"]}]}`), 201)

	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "style":"all_docs"}`
	request, _ = http.NewRequest("POST", "/db/_changes", bytes.NewBufferString(body))
	request.SetBasicAuth("user3", "letmein")
	response = rt.Send(request)
	assertStatus(t, response, 200)
	err = base.JSONUnmarshal(response.Body.Bytes(), &changes)
	assert.Equal(t, nil, err)
	assert.Equal(t, 4, len(changes.Results))
	assert.Equal(t, "docC", changes.Results[3].ID)
	assert.Equal(t, 2, len(changes.Results[3].Changes))
}

func updateTestDoc(rt *RestTester, docid string, revid string, body string) (newRevId string, err error) {
	path := fmt.Sprintf("/db/%s", docid)
	if revid != "" {
		path = fmt.Sprintf("%s?rev=%s", path, revid)
	}
	response := rt.SendRequest("PUT", path, body)
	if response.Code != 200 && response.Code != 201 {
		return "", fmt.Errorf("createDoc got unexpected response code : %d", response.Code)
	}

	var responseBody db.Body
	base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	return responseBody["rev"].(string), nil
}

// Validate retrieval of various document body types using include_docs.
func TestChangesIncludeDocs(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyNone)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`}
	rt := NewRestTester(t, &rtConfig)
	testDB := rt.GetDatabase()
	testDB.RevsLimit = 3
	defer rt.Close()

	// Create user1
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"email":"user1@couchbase.com", "password":"letmein", "admin_channels":["alpha", "beta"]}`)
	assertStatus(t, response, 201)

	//Create docs for each scenario
	// Active
	_, err := updateTestDoc(rt, "doc_active", "", `{"type": "active", "channels":["alpha"]}`)
	assert.NoError(t, err, "Error updating doc")

	// Multi-revision
	var revid string
	revid, err = updateTestDoc(rt, "doc_multi_rev", "", `{"type": "active", "channels":["alpha"], "v":1}`)
	assert.NoError(t, err, "Error updating doc")
	_, err = updateTestDoc(rt, "doc_multi_rev", revid, `{"type": "active", "channels":["alpha"], "v":2}`)
	assert.NoError(t, err, "Error updating doc")

	// Tombstoned
	revid, err = updateTestDoc(rt, "doc_tombstone", "", `{"type": "tombstone", "channels":["alpha"]}`)
	assert.NoError(t, err, "Error updating doc")
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/doc_tombstone?rev=%s", revid), "")
	assertStatus(t, response, 200)

	// Removed
	revid, err = updateTestDoc(rt, "doc_removed", "", `{"type": "removed", "channels":["alpha"]}`)
	assert.NoError(t, err, "Error updating doc")
	_, err = updateTestDoc(rt, "doc_removed", revid, `{"type": "removed"}`)
	assert.NoError(t, err, "Error updating doc")

	// No access (no channels)
	_, err = updateTestDoc(rt, "doc_no_channels", "", `{"type": "no_channels", "channels":["gamma"]}`)
	assert.NoError(t, err, "Error updating doc")

	// No access (other channels)
	_, err = updateTestDoc(rt, "doc_no_access", "", `{"type": "no_access", "channels":["gamma"]}`)
	assert.NoError(t, err, "Error updating doc")

	// Removal, pruned from rev tree
	var prunedRevId string
	prunedRevId, err = updateTestDoc(rt, "doc_pruned", "", `{"type": "pruned", "channels":["alpha"]}`)
	assert.NoError(t, err, "Error updating doc")
	// Generate more revs than revs_limit (3)
	revid = prunedRevId
	for i := 0; i < 5; i++ {
		revid, err = updateTestDoc(rt, "doc_pruned", revid, `{"type": "pruned", "channels":["gamma"]}`)
		assert.NoError(t, err, "Error updating doc")
	}

	// Doc w/ attachment
	revid, err = updateTestDoc(rt, "doc_attachment", "", `{"type": "attachments", "channels":["alpha"]}`)
	assert.NoError(t, err, "Error updating doc")
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "text/plain"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response = rt.SendRequestWithHeaders("PUT", fmt.Sprintf("/db/doc_attachment/attach1?rev=%s", revid), attachmentBody, reqHeaders)
	assertStatus(t, response, 201)

	// Doc w/ large numbers
	_, err = updateTestDoc(rt, "doc_large_numbers", "", `{"type": "large_numbers", "channels":["alpha"], "largeint":1234567890, "largefloat":1234567890.1234}`)
	assert.NoError(t, err, "Error updating doc")

	// Conflict
	revid, err = updateTestDoc(rt, "doc_conflict", "", `{"type": "conflict", "channels":["alpha"]}`)
	assert.NoError(t, err)
	_, err = updateTestDoc(rt, "doc_conflict", revid, `{"type": "conflict", "channels":["alpha"]}`)
	assert.NoError(t, err)
	newEdits_conflict := `{"type": "conflict", "channels":["alpha"],
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "19a316235cdd9d695d73765dc527d903"]}}`
	response = rt.SendAdminRequest("PUT", "/db/doc_conflict?new_edits=false", newEdits_conflict)
	assertStatus(t, response, 201)

	// Resolved conflict
	revid, err = updateTestDoc(rt, "doc_resolved_conflict", "", `{"type": "resolved_conflict", "channels":["alpha"]}`)
	assert.NoError(t, err)
	_, err = updateTestDoc(rt, "doc_resolved_conflict", revid, `{"type": "resolved_conflict", "channels":["alpha"]}`)
	assert.NoError(t, err)
	newEdits_conflict = `{"type": "resolved_conflict", "channels":["alpha"]},
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "4e123c0497a1a6975540977ec127c06c"]}}`
	response = rt.SendAdminRequest("PUT", "/db/doc_resolved_conflict?new_edits=false", newEdits_conflict)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("DELETE", "/db/doc_resolved_conflict?rev=2-conflicting_rev", "")
	assertStatus(t, response, 201)

	expectedResults := make([]string, 10)
	expectedResults[0] = `{"seq":1,"id":"_user/user1","changes":[]}`
	expectedResults[1] = `{"seq":2,"id":"doc_active","doc":{"_id":"doc_active","_rev":"1-d59fda97ac4849f6a754fbcf4b522980","channels":["alpha"],"type":"active"},"changes":[{"rev":"1-d59fda97ac4849f6a754fbcf4b522980"}]}`
	expectedResults[2] = `{"seq":4,"id":"doc_multi_rev","doc":{"_id":"doc_multi_rev","_rev":"2-db2cf770921c3764b2d213ee0cbb5f45","channels":["alpha"],"type":"active","v":2},"changes":[{"rev":"2-db2cf770921c3764b2d213ee0cbb5f45"}]}`
	expectedResults[3] = `{"seq":6,"id":"doc_tombstone","deleted":true,"removed":["alpha"],"doc":{"_deleted":true,"_id":"doc_tombstone","_rev":"2-5bd8eb422f30e8d455940672e9e76549"},"changes":[{"rev":"2-5bd8eb422f30e8d455940672e9e76549"}]}`
	expectedResults[4] = `{"seq":8,"id":"doc_removed","removed":["alpha"],"doc":{"_id":"doc_removed","_removed":true,"_rev":"2-d15cb77d1dbe1cc06d27310de5b75914"},"changes":[{"rev":"2-d15cb77d1dbe1cc06d27310de5b75914"}]}`
	expectedResults[5] = `{"seq":12,"id":"doc_pruned","removed":["alpha"],"doc":{"_id":"doc_pruned","_removed":true,"_rev":"2-5afcb73bd3eb50615470e3ba54b80f00"},"changes":[{"rev":"2-5afcb73bd3eb50615470e3ba54b80f00"}]}`
	expectedResults[6] = `{"seq":18,"id":"doc_attachment","doc":{"_attachments":{"attach1":{"content_type":"text/plain","digest":"sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=","length":30,"revpos":2,"stub":true}},"_id":"doc_attachment","_rev":"2-0b0457923508d99ec1929d2316d14cf2","channels":["alpha"],"type":"attachments"},"changes":[{"rev":"2-0b0457923508d99ec1929d2316d14cf2"}]}`
	expectedResults[7] = `{"seq":19,"id":"doc_large_numbers","doc":{"_id":"doc_large_numbers","_rev":"1-2721633d9000e606e9c642e98f2f5ae7","channels":["alpha"],"largefloat":1234567890.1234,"largeint":1234567890,"type":"large_numbers"},"changes":[{"rev":"1-2721633d9000e606e9c642e98f2f5ae7"}]}`
	expectedResults[8] = `{"seq":22,"id":"doc_conflict","doc":{"_id":"doc_conflict","_rev":"2-conflicting_rev","channels":["alpha"],"type":"conflict"},"changes":[{"rev":"2-conflicting_rev"}]}`
	expectedResults[9] = `{"seq":24,"id":"doc_resolved_conflict","doc":{"_id":"doc_resolved_conflict","_rev":"2-251ba04e5889887152df5e7a350745b4","channels":["alpha"],"type":"resolved_conflict"},"changes":[{"rev":"2-251ba04e5889887152df5e7a350745b4"}]}`
	changesResponse := rt.Send(requestByUser("GET", "/db/_changes?include_docs=true", "", "user1"))

	var changes changesResults
	assert.NoError(t, base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes))
	assert.Equal(t, len(expectedResults), len(changes.Results))

	for index, result := range changes.Results {
		var expectedChange db.ChangeEntry
		assert.NoError(t, base.JSONUnmarshal([]byte(expectedResults[index]), &expectedChange))
		assert.Equal(t, result, expectedChange)
	}

	// Flush the rev cache, and issue changes again to ensure successful handling for rev cache misses
	testDB.FlushRevisionCacheForTest()
	// Also nuke temporary revision backup of doc_pruned.  Validates that the body for the pruned revision is generated correctly when no longer resident in the rev cache
	assert.NoError(t, testDB.Bucket.Delete("_sync:rev:doc_pruned:34:2-5afcb73bd3eb50615470e3ba54b80f00"))

	postFlushChangesResponse := rt.Send(requestByUser("GET", "/db/_changes?include_docs=true", "", "user1"))

	var postFlushChanges changesResults
	assert.NoError(t, base.JSONUnmarshal(postFlushChangesResponse.Body.Bytes(), &postFlushChanges))
	assert.Equal(t, len(expectedResults), len(postFlushChanges.Results))

	for index, result := range postFlushChanges.Results {
		var expectedChange db.ChangeEntry
		assert.NoError(t, base.JSONUnmarshal([]byte(expectedResults[index]), &expectedChange))
		assert.Equal(t, result, expectedChange)
	}

	// Validate include_docs=false, style=all_docs permutations
	expectedStyleAllDocs := make([]string, 10)
	expectedStyleAllDocs[0] = `{"seq":1,"id":"_user/user1","changes":[]}`
	expectedStyleAllDocs[1] = `{"seq":2,"id":"doc_active","changes":[{"rev":"1-d59fda97ac4849f6a754fbcf4b522980"}]}`
	expectedStyleAllDocs[2] = `{"seq":4,"id":"doc_multi_rev","changes":[{"rev":"2-db2cf770921c3764b2d213ee0cbb5f45"}]}`
	expectedStyleAllDocs[3] = `{"seq":6,"id":"doc_tombstone","deleted":true,"removed":["alpha"],"changes":[{"rev":"2-5bd8eb422f30e8d455940672e9e76549"}]}`
	expectedStyleAllDocs[4] = `{"seq":8,"id":"doc_removed","removed":["alpha"],"changes":[{"rev":"2-d15cb77d1dbe1cc06d27310de5b75914"}]}`
	expectedStyleAllDocs[5] = `{"seq":12,"id":"doc_pruned","removed":["alpha"],"changes":[{"rev":"2-5afcb73bd3eb50615470e3ba54b80f00"}]}`
	expectedStyleAllDocs[6] = `{"seq":18,"id":"doc_attachment","changes":[{"rev":"2-0b0457923508d99ec1929d2316d14cf2"}]}`
	expectedStyleAllDocs[7] = `{"seq":19,"id":"doc_large_numbers","changes":[{"rev":"1-2721633d9000e606e9c642e98f2f5ae7"}]}`
	expectedStyleAllDocs[8] = `{"seq":22,"id":"doc_conflict","changes":[{"rev":"2-conflicting_rev"},{"rev":"2-869a7167ccbad634753105568055bd61"}]}`
	expectedStyleAllDocs[9] = `{"seq":24,"id":"doc_resolved_conflict","changes":[{"rev":"2-251ba04e5889887152df5e7a350745b4"}]}`

	styleAllDocsChangesResponse := rt.Send(requestByUser("GET", "/db/_changes?style=all_docs", "", "user1"))
	var allDocsChanges struct {
		Results []*json.RawMessage
	}
	err = base.JSONUnmarshal(styleAllDocsChangesResponse.Body.Bytes(), &allDocsChanges)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, len(expectedStyleAllDocs), len(allDocsChanges.Results))
	for index, result := range allDocsChanges.Results {
		assert.Equal(t, fmt.Sprintf("%s", *result), expectedStyleAllDocs[index])
	}

	// Validate style=all_docs, include_docs=true permutations.  Only modified doc from include_docs test is doc_conflict (adds open revisions)
	expectedResults[8] = `{"seq":22,"id":"doc_conflict","doc":{"_id":"doc_conflict","_rev":"2-conflicting_rev","channels":["alpha"],"type":"conflict"},"changes":[{"rev":"2-conflicting_rev"},{"rev":"2-869a7167ccbad634753105568055bd61"}]}`

	combinedChangesResponse := rt.Send(requestByUser("GET", "/db/_changes?style=all_docs&include_docs=true", "", "user1"))
	var combinedChanges changesResults
	assert.NoError(t, base.JSONUnmarshal(combinedChangesResponse.Body.Bytes(), &combinedChanges))
	assert.Equal(t, len(expectedResults), len(combinedChanges.Results))

	for index, result := range combinedChanges.Results {
		var expectedChange db.ChangeEntry
		assert.NoError(t, base.JSONUnmarshal([]byte(expectedResults[index]), &expectedChange))
		assert.Equal(t, result, expectedChange)
	}
}

// Test _changes with channel filter
func changesActiveOnly(t *testing.T, it *indexTester) {

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response := it.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
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
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))

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
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 3, len(entry.Changes))
		}
	}

	// Active only, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 3, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}
	// Active only, GET
	changes.Results = nil
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true", "", "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 3, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}
}

// Tests view backfills into empty cache and validates that results are prepended to cache
func TestChangesViewBackfillFromQueryOnly(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err)
	a.Save(bernard)

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)
	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/abc%d", i), `{"channels":["ABC"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/pbs%d", i), `{"channels":["PBS"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/nbc%d", i), `{"channels":["NBC"]}`)
		assertStatus(t, response, 201)
		cacheWaiter.Add(3)
	}

	cacheWaiter.Wait()

	// Flush the channel cache
	testDb.FlushChannelCache()

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Initialize query count
	queryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")

	// Issue a since=0 changes request.  Validate that there is a view-based backfill
	changes.Results = nil
	changesJSON := `{"since":0, "limit":50}`
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there has been a view query
	secondQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, queryCount+1, secondQueryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	thirdQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, secondQueryCount, thirdQueryCount)

}

// Validate that non-contiguous query results (due to limit) are not prepended to the cache
func TestChangesViewBackfillNonContiguousQueryResults(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err)
	a.Save(bernard)

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/abc%d", i), `{"channels":["ABC"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/pbs%d", i), `{"channels":["PBS"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/nbc%d", i), `{"channels":["NBC"]}`)
		assertStatus(t, response, 201)
		cacheWaiter.Add(3)
	}
	cacheWaiter.Wait()

	// Flush the channel cache
	testDb.FlushChannelCache()

	// Issue a since=0 changes request, with limit less than the number of PBS documents
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Issue a since=0, limit 5 changes request.  Results shouldn't be prepended to the cache, since they aren't
	// contiguous with the cache's validFrom.
	changes.Results = nil
	changesJSON := `{"since":0, "limit":5}`
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

	queryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%d", queryCount)

	// Issue another since=0, limit 5 changes request.  Validate that there is another a view-based backfill
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there has been one more view query
	secondQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, queryCount+1, secondQueryCount)

	// Issue a since=20, limit 5 changes request.  Results should be prepended to the cache (seqs 23, 26, 29)
	changes.Results = nil
	changesJSON = `{"since":20, "limit":5}`
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 3, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there has been one more view query
	thirdQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, secondQueryCount+1, thirdQueryCount)

	// Issue a since=20, limit 5 changes request again.  Results should be served from the cache (seqs 23, 26, 29)
	changes.Results = nil
	changesJSON = `{"since":20, "limit":5}`
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 3, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there hasn't been another view query
	fourthQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, thirdQueryCount, fourthQueryCount)
}

// Tests multiple view backfills and validates that results are prepended to cache
func TestChangesViewBackfillFromPartialQueryOnly(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err)
	a.Save(bernard)

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/abc%d", i), `{"channels":["ABC"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/pbs%d", i), `{"channels":["PBS"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/nbc%d", i), `{"channels":["NBC"]}`)
		assertStatus(t, response, 201)
		cacheWaiter.Add(3)
	}

	cacheWaiter.Wait()

	// Flush the channel cache
	testDb.FlushChannelCache()

	// Issue a since=n changes request, where n > 0 and is a non-PBS sequence.  Validate that there's a view-based backfill
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"since":15, "limit":50}`
	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

	queryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%d", queryCount)

	// Issue a since=0 changes request.  Expect a second view query to retrieve the additional results
	changes.Results = nil
	changesJSON = `{"since":0, "limit":50}`
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there has been one more view query
	secondQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, queryCount+1, secondQueryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	thirdQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, secondQueryCount, thirdQueryCount)

}

// Tests multiple view backfills and validates that results are prepended to cache
func TestChangesViewBackfillNoOverlap(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err)
	a.Save(bernard)

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/abc%d", i), `{"channels":["ABC"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/pbs%d", i), `{"channels":["PBS"]}`)
		assertStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/nbc%d", i), `{"channels":["NBC"]}`)
		assertStatus(t, response, 201)
		cacheWaiter.Add(3)
	}

	cacheWaiter.Wait()

	// Flush the channel cache
	testDb.FlushChannelCache()

	// Write some more docs to the bucket, with a gap before the first PBS sequence
	response := rt.SendAdminRequest("PUT", "/db/abc11", `{"channels":["ABC"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/abc12", `{"channels":["ABC"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/abc13", `{"channels":["ABC"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/abc14", `{"channels":["ABC"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs11", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.Add(5)
	cacheWaiter.Wait()

	// Issue a since=n changes request, where 0 < n < 30 (high sequence at cache init)Validate that there's a view-based backfill
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"since":15, "limit":50}`
	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 6, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}

	queryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%d", queryCount)

	// Issue the same changes request.  Validate that there is not another a view-based backfill
	changes.Results = nil
	changesJSON = `{"since":15, "limit":50}`
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 6, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there has been one more view query
	secondQueryCount, _ := base.GetExpvarAsInt("syncGateway_changeCache", "view_queries")
	assert.Equal(t, queryCount, secondQueryCount)

}

// Tests view backfill and validates that results are prepended to cache
func TestChangesViewBackfill(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	a.Save(bernard)

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.AddAndWait(3)

	// Flush the channel cache
	testDb.FlushChannelCache()

	// Add a few more docs (to increment the channel cache's validFrom)
	response = rt.SendAdminRequest("PUT", "/db/doc4", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/doc5", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.AddAndWait(2)

	// Issue a since=0 changes request.  Validate that there's a view-based backfill
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"since":0, "limit":50}`
	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	queryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%s", queryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	assert.Equal(t, base.GetExpvarAsString("syncGateway_changeCache", "view_queries"), queryCount)

}

// Tests view backfill and validates that results are prepended to cache
func TestChangesViewBackfillStarChannel(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "*"))
	assert.NoError(t, err)
	a.Save(bernard)

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/db/doc5", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc4", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	cacheWaiter.Add(3)
	cacheWaiter.Wait()

	// Flush the channel cache
	testDb.FlushChannelCache()

	// Add a few more docs (to increment the channel cache's validFrom)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.Add(2)
	cacheWaiter.Wait()

	// Issue a since=0 changes request.  Validate that there's a view-based backfill
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesJSON := `{"since":0, "limit":50}`
	changes.Results = nil
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for index, entry := range changes.Results {
		// Expects docs in sequence order from 1-5
		assert.Equal(t, uint64(index+1), entry.Seq.Seq)
		log.Printf("Entry:%+v", entry)
	}
	queryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%s", queryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for index, entry := range changes.Results {
		// Expects docs in sequence order from 1-5
		assert.Equal(t, uint64(index+1), entry.Seq.Seq)
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	assert.Equal(t, queryCount, base.GetExpvarAsString("syncGateway_changeCache", "view_queries"))

}

// Tests view backfill with slow query, checks duplicate handling for cache entries if a document is updated after query runs, but before document is
// prepended to the cache.  Reproduces #3475
func TestChangesViewBackfillSlowQuery(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip test with LeakyBucket dependency test when running in integration")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS"))
	assert.NoError(t, err)
	a.Save(bernard)

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(1)

	// Put rev1 of document
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	var body db.Body
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	cacheWaiter.Wait()

	log.Printf("about to flush")
	// Flush the channel cache
	testDb.FlushChannelCache()
	log.Printf("flush done")

	// Write another doc, to initialize the cache (and guarantee overlap)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.Add(1)
	cacheWaiter.Wait()

	// Set up PostQueryCallback on bucket - will be invoked when changes triggers the cache backfill view query

	leakyBucket, ok := rt.Bucket().(*base.LeakyBucket)
	assert.True(t, ok, "Bucket was not of type LeakyBucket")
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
			cacheWaiter.Add(1)
			cacheWaiter.Wait()
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
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 2, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	queryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After initial changes request, query count is :%s", queryCount)

	leakyBucket.SetPostQueryCallback(nil)

	// Issue another since=0 changes request - cache SHOULD only have a single rev for doc1
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 2, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	updatedQueryCount := base.GetExpvarAsString("syncGateway_changeCache", "view_queries")
	log.Printf("After second changes request, query count is :%s", updatedQueryCount)
	assert.Equal(t, queryCount, updatedQueryCount)

}

func TestChangesActiveOnlyWithLimit(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges)()

	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
	defer it.Close()

	testDb := it.ServerContext().Database("db")

	// Create user:
	a := testDb.Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	a.Save(bernard)

	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put several documents
	var body db.Body
	response := it.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc0", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	response = it.SendAdminRequest("PUT", "/db/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
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
	cacheWaiter.AddAndWait(8)

	// Pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))

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

	cacheWaiter.AddAndWait(8)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 3, len(entry.Changes))
		}
	}

	// Active only NO Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 8, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// Active only with Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}
	// Active only with Limit, GET
	changes.Results = nil
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true&limit=5", "", "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 8, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}
}

// Test active-only and limit handling during cache backfill from the view.  Flushes the channel cache
// prior to changes requests in order to force view backfill.  Covers https://github.com/couchbase/sync_gateway/issues/2955 in
// additional to general view handling.
func TestChangesActiveOnlyWithLimitAndViewBackfill(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	it := initIndexTester(`function(doc) {channel(doc.channel);}`, t)
	defer it.Close()

	// Create user:
	a := it.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	a.Save(bernard)

	cacheWaiter := it.ServerContext().Database("db").NewDCPCachingCountWaiter(t)
	// Put several documents
	var body db.Body
	response := it.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = it.SendAdminRequest("PUT", "/db/activeDoc0", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	response = it.SendAdminRequest("PUT", "/db/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
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

	cacheWaiter.AddAndWait(8)

	// Get pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	changesResponse := it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))

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

	cacheWaiter.AddAndWait(8)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 3, len(entry.Changes))
		}
	}

	// Active only NO Limit, POST
	testDb := it.ServerContext().Database("db")
	testDb.FlushChannelCache()

	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 8, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// Active only with Limit, POST
	testDb.FlushChannelCache()
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// Active only with Limit, GET
	testDb.FlushChannelCache()
	changes.Results = nil
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true&limit=5", "", "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	testDb.FlushChannelCache()
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes.Results = nil
	changesResponse = it.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 8, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// No limit active only, GET, followed by normal (https://github.com/couchbase/sync_gateway/issues/2955)
	testDb.FlushChannelCache()
	changes.Results = nil
	changesResponse = it.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true", "", "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 8, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	var updatedChanges struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}
	changesResponse = it.Send(requestByUser("GET", "/db/_changes", "", "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &updatedChanges)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(updatedChanges.Results))

}

func TestChangesActiveOnlyWithLimitLowRevCache(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeyChanges|base.KeyCache)()

	cacheSize := 2
	shortWaitConfig := &DbConfig{
		CacheConfig: &CacheConfig{
			ChannelCacheConfig: &ChannelCacheConfig{
				MinLength: &cacheSize,
				MaxLength: &cacheSize,
			},
		},
	}

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	///it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	//defer it.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	a.Save(bernard)

	// Put several documents
	var body db.Body
	response := rt.SendAdminRequest("PUT", "/db/deletedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	deletedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/removedDoc", `{"channel":["PBS"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
	removedRev := body["rev"].(string)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/activeDoc0", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	base.JSONUnmarshal(response.Body.Bytes(), &body)
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
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))

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
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 10, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 3, len(entry.Changes))
		}
	}

	// Active only NO Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 8, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// Active only with Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}
	// Active only with Limit, GET
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("GET", "/db/_changes?style=all_docs&active_only=true&limit=5", "", "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 5, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes.Results = nil
	changesResponse = rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 8, len(changes.Results))
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			assert.Equal(t, 2, len(entry.Changes))
		}
	}
}

// Test _changes returning conflicts
func TestChangesIncludeConflicts(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache|base.KeyChanges|base.KeyCRUD)()

	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := NewRestTester(t, &rtConfig)
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
	require.NoError(t, rt.WaitForPendingChanges())

	changesResponse := rt.SendAdminRequest("GET", "/db/_changes?style=all_docs", "")
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("changes response: %s", changesResponse.Body.Bytes())
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, 2, len(changes.Results[0].Changes))

}

// Test _changes handling large sequence values - ensures no truncation of large ints.
// NOTE: this test currently fails if it triggers a N1QL query, due to CBG-361.  It's been modified
// to force the use of views until that's fixed.
func TestChangesLargeSequences(t *testing.T) {

	if base.UnitTestUrlIsWalrus() {
		t.Skip("TestChangesLargeSequences doesn't support walrus - needs to customize _sync:seq prior to db creation")
	}

	initialSeq := uint64(9223372036854775807)
	rtConfig := RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`,
		InitSyncSeq:    initialSeq,
		DatabaseConfig: &DbConfig{UseViews: true},
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create document
	response := rt.SendAdminRequest("PUT", "/db/largeSeqDoc", `{"channel":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Get changes
	require.NoError(t, rt.WaitForPendingChanges())

	changesResponse := rt.SendAdminRequest("GET", "/db/_changes?since=9223372036854775800", "")
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, uint64(9223372036854775808), changes.Results[0].Seq.Seq)
	assert.Equal(t, "9223372036854775808", changes.Last_Seq)

	// Validate incoming since value isn't being truncated
	changesResponse = rt.SendAdminRequest("GET", "/db/_changes?since=9223372036854775808", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 0, len(changes.Results))

	// Validate incoming since value isn't being truncated
	changesResponse = rt.SendAdminRequest("POST", "/db/_changes", `{"since":9223372036854775808}`)
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Equal(t, 0, len(changes.Results))

}

func TestIncludeDocsWithPrincipals(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	testDb := rt.ServerContext().Database("db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put users
	response := rt.SendAdminRequest("PUT", "/db/_user/includeDocsUser", `{"name":"includeDocsUser","password":"letmein", "admin_channels":["*"]}`)
	assertStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/db/_user/includeDocsUser2", `{"name":"includeDocsUser2","password":"letmein", "admin_channels":["*"]}`)
	assertStatus(t, response, 201)

	// Put several documents
	response = rt.SendAdminRequest("PUT", "/db/doc1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/doc3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	cacheWaiter.AddAndWait(3)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	// Get as admin
	changes.Results = nil
	changesResponse := rt.SendAdminRequest("GET", "/db/_changes?include_docs=true", "")
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	log.Printf("admin response: %s", changesResponse.Body.Bytes())
	assert.NoError(t, err, "Error unmarshalling changes response")
	// Expect three docs, no user docs
	assert.Equal(t, 3, len(changes.Results))

	// Get as user
	changes.Results = nil
	userChangesResponse := rt.Send(requestByUser("GET", "/db/_changes?include_docs=true", "", "includeDocsUser"))
	err = base.JSONUnmarshal(userChangesResponse.Body.Bytes(), &changes)
	log.Printf("userChangesResponse: %s", userChangesResponse.Body.Bytes())
	assert.NoError(t, err, "Error unmarshalling changes response")
	// Expect three docs and the authenticated user's user doc
	assert.Equal(t, 4, len(changes.Results))

}

// Validate that an admin channel grant wakes up a waiting changes request
func TestChangesAdminChannelGrantLongpollNotify(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges|base.KeyHTTP)()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// Create user with access to channel ABC:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "ABC"))
	assert.NoError(t, err)
	a.Save(bernard)

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/db/pbs-1", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs-2", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs-3", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs-4", `{"channels":["PBS"]}`)
	assertStatus(t, response, 201)

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq interface{}
	}

	caughtUpCount := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsCaughtUp))

	// Issue longpoll changes request
	var longpollWg sync.WaitGroup
	longpollWg.Add(1)
	go func() {
		defer longpollWg.Done()
		longpollChangesJSON := `{"since":0, "feed":"longpoll"}`
		longPollResponse := rt.Send(requestByUser("POST", "/db/_changes", longpollChangesJSON, "bernard"))
		log.Printf("longpoll changes response looks like: %s", longPollResponse.Body.Bytes())
		base.JSONUnmarshal(longPollResponse.Body.Bytes(), &changes)
		// Expect to get 4 docs plus user doc
		assert.Equal(t, 5, len(changes.Results))
	}()

	rt.GetDatabase().WaitForCaughtUp(caughtUpCount + 1)

	// Update the user doc to grant access to PBS
	response = rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"admin_channels":["ABC", "PBS"]}`)
	assertStatus(t, response, 200)

	// Wait for longpoll to return
	longpollWg.Wait()

}

// Validate handling when a single channel cache is compacted while changes request is in wait mode
func TestCacheCompactDuringChangesWait(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyCache)()

	numIndexReplicas := uint(0)
	smallCacheSize := 100
	minimumChannelCacheConfig := &DbConfig{
		CacheConfig: &CacheConfig{
			ChannelCacheConfig: &ChannelCacheConfig{
				MaxNumber: &smallCacheSize,
			},
		},
		NumIndexReplicas: &numIndexReplicas,
	}
	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`, DatabaseConfig: minimumChannelCacheConfig}

	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	caughtUpCount := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsCaughtUp))

	// Get 100 changes requests into wait mode (each for a different channel)
	changesURLPattern := "/db/_changes?filter=sync_gateway/bychannel&feed=longpoll&since=0&channels=%s"
	var longpollWg sync.WaitGroup
	queryCount := 100
	for i := 0; i < queryCount; i++ {
		longpollWg.Add(1)
		go func(i int) {
			defer longpollWg.Done()
			var changes struct {
				Results  []db.ChangeEntry
				Last_Seq interface{}
			}
			channelName := fmt.Sprintf("chan_%d", i)
			changesURL := fmt.Sprintf(changesURLPattern, channelName)
			changesResponse := rt.SendAdminRequest("GET", changesURL, "")
			base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
			// Expect to get 1 doc
			assert.Equal(t, 1, len(changes.Results))
		}(i)
	}

	// Wait for all goroutines to get into wait mode
	caughtUpErr := rt.GetDatabase().WaitForCaughtUp(caughtUpCount + int64(queryCount))
	assert.NoError(t, caughtUpErr)

	// Wait for compaction to stop
	compactErr := waitForCompactStopped(rt.GetDatabase())
	assert.NoError(t, compactErr)

	// Validate that cache has been compacted to LWM <= size <= HWM.  Actual size will vary, as
	// channels may be added after initial compaction to LWM (but not enough to retrigger compaction).
	cacheSize := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsCache().Get(base.StatKeyChannelCacheNumChannels))
	log.Printf("Cache size after compaction: %v", cacheSize)
	assert.True(t, cacheSize >= 60)
	assert.True(t, cacheSize <= 80)

	channelSet := make([]string, queryCount)
	for i := 0; i < queryCount; i++ {
		channelSet[i] = fmt.Sprintf("chan_%d", i)
	}

	// Write a single doc to all 100 channels, ensure everyone gets it
	channelDocBody := make(map[string]interface{})
	channelDocBody["channels"] = channelSet
	bodyBytes, err := base.JSONMarshal(channelDocBody)
	assert.NoError(t, err, "Marshal error for 100 channel doc")

	putResponse := rt.SendAdminRequest("PUT", "/db/100ChannelDoc", string(bodyBytes))
	assertStatus(t, putResponse, 201)

	// Wait for longpolls to return
	longpollWg.Wait()
}

func TestTombstoneCompaction(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Walrus does not support Xattrs")
	}

	if !base.TestUseXattrs() {
		t.Skip("If running with no xattrs compact acts as a no-op")
	}

	rt := NewRestTester(t, nil)
	rt.GetDatabase().PurgeInterval = 0
	defer rt.Close()

	compactionTotal := 0
	queryTotal := 0

	TestCompact := func(numDocs int) {

		count := 0

		for count < numDocs {
			count++
			response := rt.SendAdminRequest("POST", "/db/", `{"foo":"bar"}`)
			assert.Equal(t, 200, response.Code)
			var body db.Body
			err := base.JSONUnmarshal(response.Body.Bytes(), &body)
			assert.NoError(t, err)
			revId := body["rev"].(string)
			docId := body["id"].(string)

			response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/%s?rev=%s", docId, revId), "")
			assert.Equal(t, 200, response.Code)
		}

		rt.SendAdminRequest("POST", "/db/_compact", "")

		compactionTotal += numDocs
		assert.Equal(t, compactionTotal, int(base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumTombstonesCompacted))))

		queryTotal += numDocs/db.QueryTombstoneBatch + 1
		assert.Equal(t, queryTotal, int(base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsGsiViews().Get(fmt.Sprintf(base.StatKeyN1qlQueryCountExpvarFormat, db.QueryTypeTombstones)))))
	}

	// Multiples of Batch Size
	TestCompact(db.QueryTombstoneBatch)
	TestCompact(db.QueryTombstoneBatch * 4)

	// Smaller Than Batch Size
	TestCompact(2)
	TestCompact(db.QueryTombstoneBatch / 4)

	// Larger than Batch Size
	TestCompact(db.QueryTombstoneBatch + 20)
}

func waitForCompactStopped(dbc *db.DatabaseContext) error {
	for i := 0; i < 100; i++ {
		compactRunning := dbc.CacheCompactActive()
		if !compactRunning {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("waitForCompactStopped didn't stop")
}

//////// HELPERS:

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

	testDb.Bucket.Add(key, 0, db.Body{base.SyncXattrName: syncData, "key": key})
}
