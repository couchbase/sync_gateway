//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package changestest

import (
	"errors"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Reproduces issue #2383 by forcing a partial error from the view on the first changes request.
func TestReproduce2383(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip LeakyBucket test when running in integration")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	user, err := a.NewUser("ben", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err, "Error creating new user")
	assert.NoError(t, a.Save(user))

	testDb := rt.ServerContext().Database(ctx, "db")

	// Put several documents
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(3)
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc3", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.Wait()
	collection, _ := rt.GetSingleTestDatabaseCollection()
	assert.NoError(t, collection.FlushChannelCache(ctx))

	leakyDataStore, ok := base.AsLeakyDataStore(rt.TestBucket.GetSingleDataStore())
	require.True(t, ok)

	// Force a partial error for the first ViewCustom call we make to initialize an invalid cache.
	leakyDataStore.SetFirstTimeViewCustomPartialError(true)

	changes := rt.PostChanges("/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben")

	// In the first changes request since cache flush, we're forcing a nil cache with no error. Thereforce we'd expect to see zero results.
	require.Len(t, changes.Results, 0)

	changes = rt.PostChanges("/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben")
	// Now we should expect 3 results, as the invalid cache was not persisted.
	// The second call to ViewCustom will succeed and properly create the channel cache.
	require.Len(t, changes.Results, 3)

	// if we create another doc, the cache will get updated with the latest doc.
	cacheWaiter.Add(1)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc4", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.Wait()

	changes = rt.PostChanges("/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=PBS", "{}", "ben")
	require.Len(t, changes.Results, 4)
}

func TestDocDeletionFromChannel(t *testing.T) {
	// See https://github.com/couchbase/couchbase-lite-ios/issues/59
	// base.LogKeys["Changes"] = true
	// base.LogKeys["Cache"] = true

	rtConfig := rest.RestTesterConfig{
		SyncFn:       `function(doc) {channel(doc.channel)}`,
		GuestEnabled: true,
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

	// Create user:
	alice, _ := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "zero"))
	assert.NoError(t, a.Save(alice))

	// Create a doc Alice can see:
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/alpha", `{"channel":"zero"}`)
	rest.RequireStatus(t, response, http.StatusCreated)

	// Check the _changes feed:
	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "alice")
	require.Len(t, changes.Results, 1)
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)

	assert.Equal(t, "alpha", changes.Results[0].ID)
	rev1 := changes.Results[0].Changes[0]["rev"]

	// Delete the document:
	rest.RequireStatus(t, rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/alpha?rev=%s", rev1), ""), 200)

	// Get the updates from the _changes feed:
	rt.WaitForPendingChanges()
	changes = rt.GetChanges("/{{.keyspace}}/_changes?since="+since.String(), "alice")
	require.Len(t, changes.Results, 1)

	assert.Equal(t, "alpha", changes.Results[0].ID)
	assert.Equal(t, true, changes.Results[0].Deleted)
	assert.Equal(t, base.SetOf("zero"), changes.Results[0].Removed)
	rev2 := changes.Results[0].Changes[0]["rev"]

	// Now get the deleted revision:
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/alpha?rev="+rev2, "", "alice")
	assert.Equal(t, 200, response.Code)
	log.Printf("Deletion looks like: %s", response.Body.Bytes())
	var docBody db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &docBody))
	assert.Equal(t, db.Body{db.BodyId: "alpha", db.BodyRev: rev2, db.BodyDeleted: true}, docBody)

	// Access without deletion revID shouldn't be allowed (since doc is not in Alice's channels):
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/alpha", "", "alice")
	assert.Equal(t, 403, response.Code)

	// A bogus rev ID should return a 404:
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/alpha?rev=bogus", "", "alice")
	assert.Equal(t, 404, response.Code)

	// Get the old revision, which should still be accessible:
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/alpha?rev="+rev1, "", "alice")
	assert.Equal(t, 200, response.Code)
}

// TestChangesFeedOnInheritedChannelsFromRoles:
//   - Create a role with access to channel "A"
//   - Create a user with the role just created
//   - Put some docs on the database with channel "A" assigned
//   - Run changes feed as the user with a channel filter on it to channel A
func TestChangesFeedOnInheritedChannelsFromRoles(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// create role with collection channel access set to channel A
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/role", rest.GetRolePayload(t, "", rt.GetSingleDataStore(), []string{"A"}))
	rest.RequireStatus(t, resp, http.StatusCreated)

	// create user and assign the role create above to that user
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", `{"name": "alice", "password": "letmein", "admin_roles": ["role"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Put a some doc on the database with channel A assigned
	for i := range 5 {
		resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"source": "rt", "channels":["A"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Start changes feed as the user filtered to channel A, expect 6 changes (5 docs + user doc)
	rt.WaitForChanges(6, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=A", "alice", false)
}

// TestChangesFeedOnInheritedChannelsFromRolesDefaultCollection:
//   - Same concept as TestChangesFeedOnInheritedChannelsFromRoles just with use of default collection instead
//   - Create a role with access to channel "A"
//   - Create a user with the role just created
//   - Put some docs on the database with channel "A" assigned
//   - Run changes feed as the user with a channel filter on it to channel A
func TestChangesFeedOnInheritedChannelsFromRolesDefaultCollection(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// create role with collection channel access set to channel A
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/role", rest.GetRolePayload(t, "", rt.GetSingleDataStore(), []string{"A"}))
	rest.RequireStatus(t, resp, http.StatusCreated)

	// create user and assign the role create above to that user
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", `{"name": "alice", "password": "letmein", "admin_roles": ["role"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Put a some doc on the database with channel A assigned
	for i := range 5 {
		resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"source": "rt", "channels":["A"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Start changes feed as the user filtered to channel A, expect 6 changes (5 docs + user doc)
	rt.WaitForChanges(6, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=A", "alice", false)
}

func TestPostChanges(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1", `{"value":1, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc1", `{"value":1, "channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2", `{"value":2, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs3", `{"value":3, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	rt.WaitForChanges(3, "/{{.keyspace}}/_changes?style=all_docs&heartbeat=300000&feed=longpoll&limit=50&since=0", "bernard", false)
}

// Tests race between waking up the changes feed, and detecting that the user doc has changed
func TestPostChangesUserTiming(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel)}`})
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "bernard"))
	assert.True(t, err == nil)
	assert.NoError(t, a.Save(bernard))

	var wg sync.WaitGroup

	// Put several documents to channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1", `{"value":1, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2", `{"value":2, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs3", `{"value":3, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// make sure docs are written to change cache
	rt.WaitForPendingChanges()

	caughtUpCount := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()

	wg.Add(1)
	go func() {
		defer wg.Done()
		changesJSON := `{"style":"all_docs", "timeout":6000, "feed":"longpoll", "limit":50, "since":"0"}`
		changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
		require.Len(t, changes.Results, 3)
	}()

	// Wait for changes feed to get into wait mode where it is blocked on the longpoll changes feed response
	require.NoError(t, rt.GetDatabase().WaitForCaughtUp(caughtUpCount+1))

	// Put a doc in channel bernard, that also grants bernard access to channel PBS
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/grant1", `{"value":1, "accessUser":"bernard", "accessChannel":"PBS"}`)
	rest.RequireStatus(t, response, 201)
	wg.Wait()

}

func TestPostChangesSinceInteger(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn:         `function(doc) {channel(doc.channel);}`,
			DatabaseConfig: &rest.DatabaseConfig{}, // force use of default scope and collection
		})
	defer rt.Close()

	postChangesSince(t, rt)
}

func TestPostChangesWithQueryString(t *testing.T) {
	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1", `{"value":1, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc1", `{"value":1, "channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2", `{"value":2, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs3", `{"value":3, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	rt.WaitForPendingChanges()

	// Test basic properties
	changesJSON := `{"heartbeat":50, "feed":"normal", "limit":1, "since":"3"}`
	changes := rt.PostChangesAdmin("/{{.keyspace}}/_changes?feed=longpoll&limit=10&since=0&heartbeat=50000", changesJSON)
	require.Len(t, changes.Results, 4)

	// Test channel filter
	changesJSON = `{"longpoll":true}`
	filteredChanges := rt.PostChangesAdmin("/{{.keyspace}}/_changes?feed=longpoll&filter=sync_gateway/bychannel&channels=ABC", changesJSON)
	require.Len(t, filteredChanges.Results, 1)
}

// Basic _changes test with since value
func postChangesSince(t *testing.T, rt *rest.RestTester) {
	rt.CreateUser("bernard", []string{"PBS"})

	cacheWaiter := rt.GetDatabase().NewDCPCachingCountWaiter(t)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1-0000609", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/samevbdiffchannel-0000609", `{"channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/samevbdiffchannel-0000799", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2-0000609", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs3-0000609", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(5)

	changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)

	// Put several more documents, some to the same vbuckets
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs4", `{"value":4, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(4)

	changesJSON = fmt.Sprintf(`{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"%s"}`, changes.Last_Seq)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 3)
}

func TestPostChangesChannelFilterInteger(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	postChangesChannelFilter(t, rt)
}

// Test _changes with channel filter
func postChangesChannelFilter(t *testing.T, rt *rest.RestTester) {

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	cacheWaiter := rt.GetDatabase().NewDCPCachingCountWaiter(t)
	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1-0000609", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/samevbdiffchannel-0000609", `{"channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/samevbdiffchannel-0000799", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2-0000609", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs3-0000609", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(5)

	changesJSON := `{"filter":"` + base.ByChannelFilter + `", "channels":"PBS"}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 4)

	// Put several more documents, some to the same vbuckets
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1-0000799", `{"value":1, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc1-0000609", `{"value":1, "channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2-0000799", `{"value":2, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs4", `{"value":4, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(4)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 7)
}

func TestPostChangesAdminChannelGrant(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: `function(doc) {channel(doc.channel);}`,
		})
	defer rt.Close()

	// Create user with access to channel ABC:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	cacheWaiter := rt.GetDatabase().NewDCPCachingCountWaiter(t)

	// Put several documents in channel ABC and PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-1", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-2", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-3", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-4", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc-1", `{"channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(5)

	// Issue simple changes request
	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)

	// Update the user doc to grant access to PBS
	response = rt.SendAdminRequest("PUT", "/db/_user/bernard", rest.GetUserPayload(t, "", "", "", rt.GetSingleDataStore(), []string{"ABC", "PBS"}, nil))
	rest.RequireStatus(t, response, 200)

	// Issue a new changes request with since=last_seq ensure that user receives all records for channel PBS
	rt.WaitForPendingChanges()
	changes = rt.GetChanges(fmt.Sprintf("/{{.keyspace}}/_changes?since=%s", changes.Last_Seq), "bernard")
	require.Len(t, changes.Results, 5) // 4 PBS docs, plus the updated user doc

	// Write a few more docs
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-5", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc-2", `{"channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.AddAndWait(2)

	// Issue another changes request - ensure we don't backfill again
	rt.WaitForPendingChanges()
	changes = rt.GetChanges("/{{.keyspace}}/_changes?since="+changes.Last_Seq.String(), "bernard")
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	require.Len(t, changes.Results, 2) // 2 docs

}

func TestPostChangesAdminChannelGrantRemoval(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	defer db.SuspendSequenceBatching()()

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	// Create user with access to channel ABC:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	cacheWaiter := rt.GetDatabase().NewDCPCachingCountWaiter(t)

	// Put several documents in channel PBS
	const (
		pbs2ID = "pbs2"
		pbs3ID = "pbs3"
	)
	_ = rt.PutDoc("pbs-1", `{"channel":["PBS"]}`)
	pbs2Version := rt.PutDoc(pbs2ID, `{"channel":["PBS"]}`)
	pbs3Version := rt.PutDoc(pbs3ID, `{"channel":["PBS"]}`)
	_ = rt.PutDoc("pbs-4", `{"channel":["PBS"]}`)

	// Put several documents in channel HBO
	rt.PutDoc("hbo-1", `{"channel":["HBO"]}`)
	_ = rt.PutDoc("hbo-2", `{"channel":["HBO"]}`)

	// Put several documents in channel PBS and HBO
	const (
		mix1ID = "mix-1"
		mix2ID = "mix-2"
		mix3ID = "mix-3"
		mix4ID = "mix-4"
	)

	mix1Version := rt.PutDoc(mix1ID, `{"channel":["PBS","HBO"]}`)
	mix2Version := rt.PutDoc(mix2ID, `{"channel":["PBS","HBO"]}`)
	mix3Version := rt.PutDoc(mix3ID, `{"channel":["PBS","HBO"]}`)
	mix4Version := rt.PutDoc("mix-4", `{"channel":["PBS","HBO"]}`)

	// Put several documents in channel ABC
	_ = rt.PutDoc("abc-1", `{"channel":["ABC"]}`)
	const abc2ID = "abc-2"
	const abc3ID = "abc-3"
	abc2Version := rt.PutDoc(abc2ID, `{"channel":["ABC"]}`)
	abc3Version := rt.PutDoc(abc3ID, `{"channel":["ABC"]}`)
	cacheWaiter.AddAndWait(13)

	// Update some docs to remove channel
	_ = rt.UpdateDoc(pbs2ID, pbs2Version, `{}`)
	_ = rt.UpdateDoc(mix1ID, mix1Version, `{"channel":["PBS"]}`)
	_ = rt.UpdateDoc(mix2ID, mix2Version, `{"channel":["HBO"]}`)
	_ = rt.UpdateDoc(mix4ID, mix4Version, `{}`)

	// Validate that tombstones are also not sent as part of backfill:
	//  Case 1: Delete a document in a single channel (e.g. pbs-3), and validate it doesn't get included in backfill
	//  Case 2: Delete a document in a multiple channels (e.g. mix-3), and validate it doesn't get included in backfill
	rt.DeleteDoc(pbs3ID, pbs3Version)
	rt.DeleteDoc(mix3ID, mix3Version)

	// Test Scenario:
	//   1. Document mix-5 is in channels PBS, HBO (rev 1)
	//   2. Document is deleted (rev 2)
	//   3. Document is recreated, only in channel PBS (rev 3)
	const mix5ID = "mix-5"
	mix5Version := rt.PutDoc(mix5ID, `{"channel":["PBS","HBO"]}`)
	rt.DeleteDoc(mix5ID, mix5Version)
	_ = rt.PutDoc(mix5ID, `{"channel":["PBS"]}`)

	// Test Scenario:
	//   1. Document mix-6 is in channels PBS, HBO (rev 1)
	//   2. Document is updated only be in channel HBO (rev 2)
	//   3. Document is updated AGAIN to remove all channels (rev 3)
	const mix6ID = "mix-6"
	mix6Version := rt.PutDoc(mix6ID, `{"channel":["PBS","HBO"]}`)
	mix6Version = rt.UpdateDoc(mix6ID, mix6Version, `{"channel":["HBO"]}`)
	_ = rt.UpdateDoc(mix6ID, mix6Version, `{}`)

	// Test Scenario:
	//   1. Delete abc-2 from channel ABC
	//   2. Update abc-3 to remove from channel ABC
	rt.DeleteDoc(abc2ID, abc2Version)
	_ = rt.UpdateDoc(abc3ID, abc3Version, `{}`)

	// Issue changes request and check the results
	expectedResults := []string{
		`{"seq":11,"id":"abc-1","changes":[{"rev":"1-0143105976caafbda3b90cf82948dc64"}]}`,
		`{"seq":26,"id":"abc-2","deleted":true,"removed":["ABC"],"changes":[{"rev":"2-6055be21d970eb690f48452505ea02ed"}]}`,
		`{"seq":27,"id":"abc-3","removed":["ABC"],"changes":[{"rev":"2-09b89154aa9a0e1620da0d86528d406a"}]}`,
	}
	changes := rt.WaitForChanges(len(expectedResults), "/{{.keyspace}}/_changes", "bernard", false)

	for index, result := range changes.Results {
		var expectedChange db.ChangeEntry
		assert.NoError(t, base.JSONUnmarshal([]byte(expectedResults[index]), &expectedChange))
		assert.Equal(t, expectedChange.Seq, result.Seq)
		assert.Equal(t, expectedChange.ID, result.ID)
		assert.Equal(t, expectedChange.Changes, result.Changes)
		assert.Equal(t, expectedChange.Deleted, result.Deleted)
		assert.Equal(t, expectedChange.Removed, result.Removed)
	}

	// Update the user doc to grant access to PBS, HBO in addition to ABC
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/bernard", rest.GetUserPayload(t, "", "", "", rt.GetSingleDataStore(), []string{"ABC", "PBS", "HBO"}, nil))
	rest.RequireStatus(t, response, http.StatusOK)

	// Issue a new changes request with since=last_seq ensure that user receives all records for channels PBS, HBO.
	expectedResults = []string{
		`{"seq":"28:1","id":"pbs-1","changes":[{"rev":"1-82214a562e80c8fa7b2361719847bc73"}]}`,
		`{"seq":"28:4","id":"pbs-4","changes":[{"rev":"1-82214a562e80c8fa7b2361719847bc73"}]}`,
		`{"seq":"28:5","id":"hbo-1","changes":[{"rev":"1-46f8c67c004681619052ee1a1cc8e104"}]}`,
		`{"seq":"28:6","id":"hbo-2","changes":[{"rev":"1-46f8c67c004681619052ee1a1cc8e104"}]}`,
		`{"seq":"28:15","id":"mix-1","changes":[{"rev":"2-0321dde33081a5ef566eecbe42ca3583"}]}`,
		`{"seq":"28:16","id":"mix-2","changes":[{"rev":"2-5dcb551a0eb59eef3d98c64c29033d02"}]}`,
		`{"seq":"28:22","id":"mix-5","changes":[{"rev":"3-8192afec7aa6986420be1d57f1677960"}]}`,
		`{"seq":28,"id":"_user/bernard","changes":[]}`,
	}
	changes = rt.WaitForChanges(len(expectedResults),
		fmt.Sprintf("/{{.keyspace}}/_changes?since=%s", changes.Last_Seq), "bernard", false)

	for index, result := range changes.Results {
		var expectedChange db.ChangeEntry
		assert.NoError(t, base.JSONUnmarshal([]byte(expectedResults[index]), &expectedChange))
		assert.Equal(t, expectedChange.Seq, result.Seq)
		assert.Equal(t, expectedChange.ID, result.ID)
		assert.Equal(t, expectedChange.Changes, result.Changes)
		assert.Equal(t, expectedChange.Deleted, result.Deleted)
		assert.Equal(t, expectedChange.Removed, result.Removed)
	}

	// Write a few more docs
	_ = rt.PutDoc("pbs-5", `{"channel":["PBS"]}`)
	_ = rt.PutDoc("abc-4", `{"channel":["ABC"]}`)
	_ = rt.PutDoc("hbo-3", `{"channel":["HBO"]}`)
	_ = rt.PutDoc("mix-7", `{"channel":["ABC", "PBS", "HBO"]}`)

	cacheWaiter.AddAndWait(4)

	// Issue a changes request with a compound since value from the last changes response
	// ensure we don't backfill from the start, but have everything from the compound sequence onwards
	expectedResults = []string{
		`{"seq":"28:6","id":"hbo-2","changes":[{"rev":"1-46f8c67c004681619052ee1a1cc8e104"}]}`,
		`{"seq":"28:15","id":"mix-1","changes":[{"rev":"2-0321dde33081a5ef566eecbe42ca3583"}]}`,
		`{"seq":"28:16","id":"mix-2","changes":[{"rev":"2-5dcb551a0eb59eef3d98c64c29033d02"}]}`,
		`{"seq":"28:22","id":"mix-5","changes":[{"rev":"3-8192afec7aa6986420be1d57f1677960"}]}`,
		`{"seq":28,"id":"_user/bernard","changes":[]}`,
		`{"seq":29,"id":"pbs-5","changes":[{"rev":"1-82214a562e80c8fa7b2361719847bc73"}]}`,
		`{"seq":30,"id":"abc-4","changes":[{"rev":"1-0143105976caafbda3b90cf82948dc64"}]}`,
		`{"seq":31,"id":"hbo-3","changes":[{"rev":"1-46f8c67c004681619052ee1a1cc8e104"}]}`,
		`{"seq":32,"id":"mix-7","changes":[{"rev":"1-32f69cdbf1772a8e064f15e928a18f85"}]}`,
	}
	changes = rt.WaitForChanges(len(expectedResults),
		fmt.Sprintf("/{{.keyspace}}/_changes?since=28:5"), "bernard", false)

	for index, result := range changes.Results {
		var expectedChange db.ChangeEntry
		assert.NoError(t, base.JSONUnmarshal([]byte(expectedResults[index]), &expectedChange))
		assert.Equal(t, expectedChange.Seq, result.Seq)
		assert.Equal(t, expectedChange.ID, result.ID)
		assert.Equal(t, expectedChange.Changes, result.Changes)
		assert.Equal(t, expectedChange.Deleted, result.Deleted)
		assert.Equal(t, expectedChange.Removed, result.Removed)
	}
}

func TestPostChangesAdminChannelGrantRemovalWithLimit(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	// Create user with access to channel ABC:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	cacheWaiter := rt.GetDatabase().NewDCPCachingCountWaiter(t)

	// Put several documents in channel PBS
	pbs1 := rt.PutDoc("pbs-1", `{"channel":["PBS"]}`)
	pbs2 := rt.PutDoc("pbs-2", `{"channel":["PBS"]}`)
	pbs3 := rt.PutDoc("pbs-3", `{"channel":["PBS"]}`)
	pbs4 := rt.PutDoc("pbs-4", `{"channel":["PBS"]}`)
	cacheWaiter.AddAndWait(4)

	// Mark the first four PBS docs as removals
	_ = rt.PutDoc("pbs-1", fmt.Sprintf(`{"_rev":%q}`, pbs1.RevTreeID))
	_ = rt.PutDoc("pbs-2", fmt.Sprintf(`{"_rev":%q}`, pbs2.RevTreeID))
	_ = rt.PutDoc("pbs-3", fmt.Sprintf(`{"_rev":%q}`, pbs3.RevTreeID))
	_ = rt.PutDoc("pbs-4", fmt.Sprintf(`{"_rev":%q}`, pbs4.RevTreeID))

	cacheWaiter.AddAndWait(4)

	// Add another pbs doc (with a higher sequence than the removals)
	_ = rt.PutDoc("pbs-5", `{"channel":["PBS"]}`)
	cacheWaiter.AddAndWait(1)

	// Grant user access to channel PBS
	userResponse := rt.SendAdminRequest("PUT", "/{{.db}}/_user/bernard", rest.GetUserPayload(t, "", "", "", rt.GetSingleDataStore(), []string{"ABC", "PBS"}, nil))
	rest.RequireStatus(t, userResponse, 200)

	// Put several documents in channel ABC
	_ = rt.PutDoc("abc-1", `{"channel":["ABC"]}`)
	_ = rt.PutDoc("abc-2", `{"channel":["ABC"]}`)
	_ = rt.PutDoc("abc-3", `{"channel":["ABC"]}`)
	cacheWaiter.AddAndWait(3)

	// Issue changes request with limit less than 5.  Expect to get pbs-5, user doc, and abc-1
	changes := rt.WaitForChanges(3, "/{{.keyspace}}/_changes?limit=3", "bernard", false)
	assert.Equal(t, "pbs-5", changes.Results[0].ID)
	assert.Equal(t, "_user/bernard", changes.Results[1].ID)
	assert.Equal(t, "abc-1", changes.Results[2].ID)
	lastSeq := changes.Last_Seq

	// Issue a second changes request, expect to see last 2 documents.
	moreChanges := rt.WaitForChanges(2, fmt.Sprintf("/{{.keyspace}}/_changes?limit=3&since=%s", lastSeq), "bernard", false)
	assert.Equal(t, "abc-2", moreChanges.Results[0].ID)
	assert.Equal(t, "abc-3", moreChanges.Results[1].ID)
}

// TestChangesFromCompoundSinceViaDocGrant ensures that a changes feed with a compound since value returns the correct result after a dynamic channel grant.
func TestChangesFromCompoundSinceViaDocGrant(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyHTTP)

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	defer db.SuspendSequenceBatching()()

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: `function(doc) {
	channel(doc.channel);
	if (doc.grants) {
		access(doc.grants.users, doc.grants.channels);
	}
}`})
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	// Create user with access to channel NBC:
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	alice, err := a.NewUser("alice", "letmein", channels.BaseSetOf(t, "NBC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(alice))

	// Create user with access to channel ABC:
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	cacheWaiter := rt.GetDatabase().NewDCPCachingCountWaiter(t)

	// Create docs in various channels
	_ = rt.PutDoc("pbs-1", `{"channel":["PBS"]}`)
	_ = rt.PutDoc("hbo-1", `{"channel":["HBO"]}`)

	const pbs2ID = "pbs-2"
	pbs2Version := rt.PutDoc(pbs2ID, `{"channel":["PBS"]}`)
	hbo2 := rt.PutDoc("hbo-2", `{"channel":["HBO"]}`)
	cacheWaiter.AddAndWait(4)

	// remove channels/tombstone a couple of docs to ensure they're not backfilled after a dynamic grant
	_ = rt.PutDoc("hbo-2", fmt.Sprintf(`{"_rev":%q}`, hbo2.RevTreeID))
	rt.DeleteDoc(pbs2ID, pbs2Version)
	cacheWaiter.AddAndWait(2)

	_ = rt.PutDoc("abc-1", `{"channel":["ABC"]}`)
	cacheWaiter.AddAndWait(1)

	// Issue changes request and check the results, expect only the one doc in ABC
	expectedResults := []string{
		`{"seq":7,"id":"abc-1","changes":[{"rev":"1-0143105976caafbda3b90cf82948dc64"}]}`,
	}
	changes := rt.WaitForChanges(len(expectedResults), "/{{.keyspace}}/_changes", "bernard", false)
	for index, result := range changes.Results {
		assertChangeEntryMatches(t, expectedResults[index], result)
	}

	// create doc that dynamically grants both users access to PBS and HBO
	_ = rt.PutDoc("grant-1", `{"channel": ["NBC"], "grants": {"users": ["alice", "bernard"], "channels": ["NBC", "PBS", "HBO"]}}`)

	cacheWaiter.AddAndWait(1)

	// Issue a new changes request with since=last_seq ensure that user receives all records for channels PBS, HBO.
	expectedResults = []string{
		`{"seq":"8:1","id":"pbs-1","changes":[{"rev":"1-82214a562e80c8fa7b2361719847bc73"}]}`,
		`{"seq":"8:2","id":"hbo-1","changes":[{"rev":"1-46f8c67c004681619052ee1a1cc8e104"}]}`,
		`{"seq":8,"id":"grant-1","changes":[{"rev":"1-c5098bb14d12d647c901850ff6a6292a"}]}`,
	}
	changes = rt.WaitForChanges(3,
		fmt.Sprintf("/{{.keyspace}}/_changes?since=%s", changes.Last_Seq), "bernard", false)
	for index, result := range changes.Results {
		assertChangeEntryMatches(t, expectedResults[index], result)
	}

	// Write another doc
	_ = rt.PutDoc("mix-1", `{"channel":["ABC", "PBS", "HBO"]}`)

	fetchedDoc, _, err := collection.GetDocWithXattrs(ctx, "mix-1", db.DocUnmarshalSync)
	require.NoError(t, err)
	mixSource, mixVersion := fetchedDoc.HLV.GetCurrentVersion()

	cacheWaiter.AddAndWait(1)

	// Issue a changes request with a compound since value from the last changes response
	// ensure we don't backfill from the start, but have everything from the compound sequence onwards
	expectedResults = []string{
		`{"seq":"8:2","id":"hbo-1","changes":[{"rev":"1-46f8c67c004681619052ee1a1cc8e104"}]}`,
		`{"seq":8,"id":"grant-1","changes":[{"rev":"1-c5098bb14d12d647c901850ff6a6292a"}]}`,
		fmt.Sprintf(`{"seq":9,"id":"mix-1","changes":[{"rev":"1-32f69cdbf1772a8e064f15e928a18f85"}], "current_version":{"source_id": "%s", "version": "%d"}}`, mixSource, mixVersion),
	}

	rt.Run("grant via existing channel", func(t *testing.T) {
		changes = rt.WaitForChanges(len(expectedResults), "/{{.keyspace}}/_changes?since=8:1", "alice", false)
		for index, result := range changes.Results {
			assertChangeEntryMatches(t, expectedResults[index], result)
		}
	})

	rt.Run("grant via new channel", func(t *testing.T) {
		changes = rt.WaitForChanges(len(expectedResults), "/{{.keyspace}}/_changes?since=8:1", "bernard", false)
		for index, result := range changes.Results {
			assertChangeEntryMatches(t, expectedResults[index], result)
		}
	})
}

// TODO: enhance to compare source/version when expectedChanges are updated to include
func assertChangeEntryMatches(t *testing.T, expectedChangeEntryString string, result db.ChangeEntry) {
	var expectedChange db.ChangeEntry
	require.NoError(t, base.JSONUnmarshal([]byte(expectedChangeEntryString), &expectedChange))
	assert.Equal(t, expectedChange.Seq, result.Seq)
	assert.Equal(t, expectedChange.ID, result.ID)
	assert.Equal(t, expectedChange.Changes, result.Changes)
	assert.Equal(t, expectedChange.Deleted, result.Deleted)
	assert.Equal(t, expectedChange.Removed, result.Removed)

	if expectedChange.Doc != nil {
		// result.Doc is json.RawMessage, and properties may not be in the same order for a direct comparison
		var expectedBody db.Body
		var resultBody db.Body
		assert.NoError(t, expectedBody.Unmarshal(expectedChange.Doc))
		assert.NoError(t, resultBody.Unmarshal(result.Doc))

		// strip out _cv - it's not deterministic in tests like _rev is, but also tests really shouldn't be comparing full bodies! It's so brittle...
		delete(resultBody, db.BodyCV)

		db.AssertEqualBodies(t, expectedBody, resultBody)
	} else {
		assert.Equal(t, expectedChange.Doc, result.Doc)
	}
}

// Ensures that changes feed goroutines blocked on a ChangeWaiter are closed when the changes feed is terminated.
// Reproduces CBG-1113 and #1329 (even with the fix in PR #1360)
// Tests all combinations of HTTP feed types, admin/non-admin, and with and without a manual notify to wake up.
func TestChangeWaiterExitOnChangesTermination(t *testing.T) {
	base.LongRunningTest(t)

	const username = "bernard"

	tests := []struct {
		feedType     string
		manualNotify bool
		username     string // "" for admin
	}{
		{"normal", false, username},
		{"normal", true, username},
		{"normal", false, ""},
		{"normal", true, ""},
		{"continuous", false, username},
		{"continuous", true, username},
		{"continuous", false, ""},
		{"continuous", true, ""},
		{"longpoll", false, username},
		{"longpoll", true, username},
		{"longpoll", false, ""},
		{"longpoll", true, ""},
		// Can't test websocket feeds in the same way as other REST types
		// for manual websocket testing:
		// $ echo "{}" | wsd -url='ws://127.0.0.1:4985/db1/_changes?since=0&feed=websocket&continuous=true'
	}

	sendRequestFn := func(rt *rest.RestTester, username string, method, resource, body string) *rest.TestResponse {
		if username == "" {
			return rt.SendAdminRequest(method, resource, body)
		}
		return rt.SendUserRequest(method, resource, body, username)
	}

	for _, test := range tests {
		testName := fmt.Sprintf("%v user:%v manualNotify:%t", test.feedType, test.username, test.manualNotify)
		t.Run(testName, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

			rt := rest.NewRestTester(t, nil)
			defer rt.Close()

			activeCaughtUpStatWaiter := rt.GetDatabase().NewStatWaiter(rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp, t)
			totalCaughtUpStatWaiter := rt.GetDatabase().NewStatWaiter(rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp, t)

			if test.username != "" {
				a := rt.GetDatabase().Authenticator(base.TestCtx(t))
				bernard, err := a.NewUser(username, "letmein", base.Set{})
				require.NoError(t, err)
				require.NoError(t, a.Save(bernard))
			}

			// Create a doc and send an initial changes
			resp := sendRequestFn(rt, test.username, http.MethodPut, "/{{.keyspace}}/doc1", `{"foo":"bar"}`)
			rest.RequireStatus(t, resp, http.StatusCreated)
			c := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)

			lastSeq := c.Last_Seq.String()

			resp = sendRequestFn(rt, test.username, http.MethodGet, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&feed=%s&timeout=100", lastSeq, test.feedType), "")
			rest.RequireStatus(t, resp, http.StatusOK)

			// normal does not wait for any further changes, so does not have any replications in a "caught up" state.
			if test.feedType != "normal" {
				totalCaughtUpStatWaiter.AddAndWait(1)
			}

			// write a doc to force change waiters to be triggered
			if test.manualNotify {
				resp = sendRequestFn(rt, test.username, http.MethodPut, "/{{.keyspace}}/doc2", `{"foo":"bar"}`)
				rest.RequireStatus(t, resp, http.StatusCreated)
				rt.WaitForPendingChanges()
			}

			// wait for zero
			activeCaughtUpStatWaiter.Wait()
		})
	}
}

// Test low sequence handling of late arriving sequences to a continuous changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequence(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges)

	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	shortWaitConfig := &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		CacheConfig: &rest.CacheConfig{
			ChannelCacheConfig: &rest.ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
	}}
	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	collection, _ := rt.GetSingleTestDatabaseCollection()

	rt.CreateUser("bernard", []string{"PBS"})

	// Simulate seq 3 and 4 being delayed - write 1,2,5,6
	db.WriteDirect(t, collection, []string{"PBS"}, 2)
	db.WriteDirect(t, collection, []string{"PBS"}, 5)
	db.WriteDirect(t, collection, []string{"PBS"}, 6)
	rt.WaitForSequenceNotSkipped(6)

	// Check the _changes feed:
	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
	require.Len(t, changes.Results, 4)
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "2::6", changes.Last_Seq.String())

	// Send another changes request before any changes, with the last_seq received from the last changes ("2::6")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 0)

	// Send a missing doc - low sequence should move to 3
	db.WriteDirect(t, collection, []string{"PBS"}, 3)
	rt.WaitForSequence(3)

	// WaitForSequence doesn't wait for low sequence to be updated on each channel - additional delay to ensure
	// low is updated before making the next changes request.
	time.Sleep(50 * time.Millisecond)

	// Send another changes request with the same since ("2::6") to ensure we see data once there are changes
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 3)

	// Send a later doc - low sequence still 3, high sequence goes to 7
	db.WriteDirect(t, collection, []string{"PBS"}, 7)
	rt.WaitForSequenceNotSkipped(7)

	// Send another changes request with the same since ("2::6") to ensure we see data once there are changes
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 1)
}

// Test low sequence handling of late arriving sequences to a one-shot changes feed, ensuring that
// subsequent requests for the current low sequence value don't return results (avoids loops for
// longpoll as well as clients doing repeated one-off changes requests - see #1309)
func TestChangesLoopingWhenLowSequenceOneShotUser(t *testing.T) {

	if base.TestUseXattrs() {
		t.Skip("This test cannot run in xattr mode until WriteDirect() is updated.  See https://github.com/couchbase/sync_gateway/issues/2666#issuecomment-311183219")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges)
	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	shortWaitConfig := &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		CacheConfig: &rest.CacheConfig{
			ChannelCacheConfig: &rest.ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
	}}
	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	collection, _ := rt.GetSingleTestDatabaseCollection()

	rt.CreateUser("bernard", []string{"PBS"})

	// Simulate 4 non-skipped writes (seq 2,3,4,5)
	db.WriteDirect(t, collection, []string{"PBS"}, 2)
	db.WriteDirect(t, collection, []string{"PBS"}, 3)
	db.WriteDirect(t, collection, []string{"PBS"}, 4)
	db.WriteDirect(t, collection, []string{"PBS"}, 5)
	rt.WaitForSequenceNotSkipped(5)

	// Check the _changes feed:
	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
	require.Len(t, changes.Results, 5)
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "5", changes.Last_Seq.String())

	// Skip sequence 6, write docs 7-10
	db.WriteDirect(t, collection, []string{"PBS"}, 7)
	db.WriteDirect(t, collection, []string{"PBS"}, 8)
	db.WriteDirect(t, collection, []string{"PBS"}, 9)
	db.WriteDirect(t, collection, []string{"PBS"}, 10)
	rt.WaitForSequenceNotSkipped(10)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 4)
	assert.Equal(t, "5::10", changes.Last_Seq.String())

	// Write a few more docs
	db.WriteDirect(t, collection, []string{"PBS"}, 11)
	db.WriteDirect(t, collection, []string{"PBS"}, 12)
	rt.WaitForSequenceNotSkipped(12)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 2)
	assert.Equal(t, "5::12", changes.Last_Seq.String())

	// Write another doc, then the skipped doc - both should be sent, last_seq should move to 13
	db.WriteDirect(t, collection, []string{"PBS"}, 13)
	db.WriteDirect(t, collection, []string{"PBS"}, 6)
	rt.WaitForSequence(13)

	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")

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
		assert.Equal(t, "5::12", changes.Last_Seq.String())
	case 1:
		switch changes.Last_Seq.String() {
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
		assert.Equal(t, "12", changes.Last_Seq.String())
	case 8:
		assert.Equal(t, "13", changes.Last_Seq.String())
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

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges)
	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	shortWaitConfig := &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		CacheConfig: &rest.CacheConfig{
			ChannelCacheConfig: &rest.ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
	}}
	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	const user = "alice"
	rt.CreateUser(user, []string{"PBS"})
	collection, _ := rt.GetSingleTestDatabaseCollection()

	// Simulate 5 non-skipped writes (seq 1,2,3,4,5)
	db.WriteDirect(t, collection, []string{"PBS"}, 1)
	db.WriteDirect(t, collection, []string{"PBS"}, 2)
	db.WriteDirect(t, collection, []string{"PBS"}, 3)
	db.WriteDirect(t, collection, []string{"PBS"}, 4)
	db.WriteDirect(t, collection, []string{"PBS"}, 5)
	rt.WaitForSequenceNotSkipped(5)

	// Check the _changes feed:
	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", user)
	require.Len(t, changes.Results, 5)
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "5", changes.Last_Seq.String())

	// Skip sequence 6, write docs 7-10
	db.WriteDirect(t, collection, []string{"PBS"}, 7)
	db.WriteDirect(t, collection, []string{"PBS"}, 8)
	db.WriteDirect(t, collection, []string{"PBS"}, 9)
	db.WriteDirect(t, collection, []string{"PBS"}, 10)
	rt.WaitForSequenceNotSkipped(10)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChangesAdmin("/{{.keyspace}}/_changes", changesJSON)
	require.Len(t, changes.Results, 4)
	assert.Equal(t, "5::10", changes.Last_Seq.String())

	// Write a few more docs
	db.WriteDirect(t, collection, []string{"PBS"}, 11)
	db.WriteDirect(t, collection, []string{"PBS"}, 12)
	rt.WaitForSequenceNotSkipped(12)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChangesAdmin("/{{.keyspace}}/_changes", changesJSON)
	require.Len(t, changes.Results, 2)
	assert.Equal(t, "5::12", changes.Last_Seq.String())

	// Write another doc, then the skipped doc - both should be sent, last_seq should move to 13
	db.WriteDirect(t, collection, []string{"PBS"}, 13)
	db.WriteDirect(t, collection, []string{"PBS"}, 6)
	rt.WaitForSequence(13)

	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChangesAdmin("/{{.keyspace}}/_changes", changesJSON)

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
		switch changes.Last_Seq.String() {
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
		assert.Equal(t, "12", changes.Last_Seq.String())
	case 8:
		assert.Equal(t, "13", changes.Last_Seq.String())
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

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges)

	pendingMaxWait := uint32(5)
	maxNum := 50
	skippedMaxWait := uint32(120000)

	shortWaitConfig := &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		AllowConflicts: base.Ptr(true),
		CacheConfig: &rest.CacheConfig{
			ChannelCacheConfig: &rest.ChannelCacheConfig{
				MaxWaitPending: &pendingMaxWait,
				MaxNumPending:  &maxNum,
				MaxWaitSkipped: &skippedMaxWait,
			},
		},
	}}
	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	collection, _ := rt.GetSingleTestDatabaseCollection()

	rt.CreateUser("bernard", []string{"PBS"})

	// Simulate 4 non-skipped writes (seq 2,3,4,5)
	db.WriteDirect(t, collection, []string{"PBS"}, 2)
	db.WriteDirect(t, collection, []string{"PBS"}, 3)
	db.WriteDirect(t, collection, []string{"PBS"}, 4)
	db.WriteDirect(t, collection, []string{"PBS"}, 5)
	rt.WaitForSequenceNotSkipped(5)

	// Check the _changes feed:
	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
	require.Len(t, changes.Results, 5)
	since := changes.Results[0].Seq
	assert.Equal(t, uint64(1), since.Seq)
	assert.Equal(t, "5", changes.Last_Seq.String())

	// Skip sequence 6, write docs 7-10
	db.WriteDirect(t, collection, []string{"PBS"}, 7)
	db.WriteDirect(t, collection, []string{"PBS"}, 8)
	db.WriteDirect(t, collection, []string{"PBS"}, 9)
	db.WriteDirect(t, collection, []string{"PBS"}, 10)
	rt.WaitForSequenceNotSkipped(10)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON := fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 4)
	assert.Equal(t, "5::10", changes.Last_Seq.String())

	// Write a few more docs
	db.WriteDirect(t, collection, []string{"PBS"}, 11)
	db.WriteDirect(t, collection, []string{"PBS"}, 12)
	rt.WaitForSequenceNotSkipped(12)

	// Send another changes request with the last_seq received from the last changes ("5")
	changesJSON = fmt.Sprintf(`{"since":"%s"}`, changes.Last_Seq)
	log.Printf("sending changes JSON: %s", changesJSON)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 2)
	assert.Equal(t, "5::12", changes.Last_Seq.String())

	caughtUpCount := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()
	// Issue a longpoll changes request.  Will block.
	var longpollWg sync.WaitGroup
	longpollWg.Add(1)
	go func() {
		defer longpollWg.Done()
		longpollChangesJSON := fmt.Sprintf(`{"since":"%s", "feed":"longpoll"}`, changes.Last_Seq)
		log.Printf("starting longpoll changes w/ JSON: %s", longpollChangesJSON)
		changes := rt.PostChanges("/{{.keyspace}}/_changes", longpollChangesJSON, "bernard")
		// Expect to get 6 through 12
		require.Len(t, changes.Results, 7)
		assert.Equal(t, "12", changes.Last_Seq.String())
	}()

	// Wait for longpoll to get into wait mode
	require.NoError(t, rt.GetDatabase().WaitForCaughtUp(caughtUpCount+1))

	// Write the skipped doc, wait for longpoll to return
	db.WriteDirect(t, collection, []string{"PBS"}, 6)
	// WriteDirect(testDb, []string{"PBS"}, 13)
	longpollWg.Wait()

}

func TestUnusedSequences(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyCRUD, base.KeyHTTP)

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

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	// Issue concurrent deletes
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			deleteResponse := rt.SendAdminRequest("DELETE", "/{{.keyspace}}/doc1?rev="+revId, "")
			log.Printf("Delete #%d got response code: %d", i, deleteResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, or fails test
	rt.WaitForPendingChanges()

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)

	// Wait for writes to be processed and indexed
	rt.WaitForPendingChanges()

}

func _testConcurrentPutAsDelete(t *testing.T) {

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	// Issue concurrent deletes
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			deleteResponse := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev="+revId, `{"_deleted":true}`)
			log.Printf("Delete #%d got response code: %d", i, deleteResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, or fails test
	rt.WaitForPendingChanges()

	// Write another doc, to validate sequences restart
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)

	rt.WaitForPendingChanges()
}

func _testConcurrentUpdate(t *testing.T) {

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	// Issue concurrent updates
	var wg sync.WaitGroup
	for i := 1; i <= 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			updateResponse := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev="+revId, `{"value":10}`)
			log.Printf("Update #%d got response code: %d", i, updateResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, or fails test
	rt.WaitForPendingChanges()

	// Write another doc, to validate sequences restart
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)

	rt.WaitForPendingChanges()
}

func _testConcurrentNewEditsFalseDelete(t *testing.T) {

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create doc
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
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
			deleteResponse := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev=2-foo&new_edits=false", deleteRequestBody)
			log.Printf("Delete #%d got response code: %d", i, deleteResponse.Code)
		}(i)
	}
	wg.Wait()

	// WaitForPendingChanges waits up to 2 seconds for all allocated sequences to be cached, or fails test
	rt.WaitForPendingChanges()

	// Write another doc, to see where sequences are at
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channel":"PBS"}`)
	rest.RequireStatus(t, response, 201)
	rt.WaitForPendingChanges()

}

func TestChangesActiveOnlyInteger(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	rt.GetDatabase().EnableAllowConflicts(rt.TB())
	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents
	var body db.Body
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/deletedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	deletedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	removedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	partialRemovalRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/conflictedDoc", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// Create a conflict, then tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictTombstone"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/conflictedDoc?rev=1-conflictTombstone", "")
	rest.RequireStatus(t, response, 200)

	// Create a conflict, and don't tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictActive"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)

	// Pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	err = rt.WaitForCondition(func() bool {
		changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
		return len(changes.Results) == 5
	})
	assert.NoError(t, err)

	// Delete
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/deletedDoc?rev="+deletedRev, "")
	rest.RequireStatus(t, response, 200)

	// Removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", fmt.Sprintf(`{"_rev":%q, "channel":["HBO"]}`, removedRev))
	rest.RequireStatus(t, response, 201)

	// Partially removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", fmt.Sprintf(`{"_rev":%q, "channel":["PBS"]}`, partialRemovalRev))
	rest.RequireStatus(t, response, 201)

	// Normal changes
	changesJSON = `{"style":"all_docs"}`
	var changes rest.ChangesResults
	err = rt.WaitForCondition(func() bool {
		changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
		return len(changes.Results) == 5
	})
	assert.NoError(t, err)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 3)
		}
	}

	// Active only, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	err = rt.WaitForCondition(func() bool {
		changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
		return len(changes.Results) == 3
	})
	require.NoError(t, err)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only, GET
	err = rt.WaitForCondition(func() bool {
		changes = rt.GetChanges("/{{.keyspace}}/_changes?style=all_docs&active_only=true", "bernard")
		return len(changes.Results) == 3
	})
	require.NoError(t, err)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}
}

func TestOneShotChangesWithExplicitDocIds(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyNone)

	defer db.SuspendSequenceBatching()()

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function(doc) {
			if (doc.type == "grant") {
				access(doc.user, doc.channels)
			} else {
				channel(doc.channels)
			}
		}`,
	}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig)
	defer rt.Close()

	rt.GetDatabase().EnableAllowConflicts(rt.TB())
	// Create user1
	rt.CreateUser("user1", []string{"alpha"})
	rt.CreateUser("user2", []string{"beta"})
	rt.CreateUser("user3", []string{"alpha", "beta"})
	rt.CreateUser("user4", nil)
	rt.CreateUser("user5", []string{"*"})

	// Create docs
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":["alpha"]}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels":["alpha"]}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc3", `{"channels":["alpha"]}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc4", `{"channels":["alpha"]}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/docA", `{"channels":["beta"]}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/docB", `{"channels":["beta"]}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/docC", `{"channels":["beta"]}`), 201)
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/docD", `{"channels":["beta"]}`), 201)

	rt.WaitForPendingChanges()

	// User has access to single channel
	body := `{"filter":"_doc_ids", "doc_ids":["doc4", "doc1", "docA", "b0gus"]}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", body, "user1")
	require.Len(t, changes.Results, 2)
	assert.Equal(t, "doc4", changes.Results[1].ID)

	// User has access to different single channel
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "docB", "docD", "doc1"]}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user2")
	require.Len(t, changes.Results, 3)
	assert.Equal(t, "docD", changes.Results[2].ID)

	// User has access to multiple channels
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user3")
	require.Len(t, changes.Results, 4)
	assert.Equal(t, "docD", changes.Results[3].ID)

	// User has no channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"]}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user4")
	require.Len(t, changes.Results, 0)

	// User has "*" channel access
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user5")
	require.Len(t, changes.Results, 5)

	// User has "*" channel access, override POST with GET params
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1", "docA"]}`
	changes = rt.PostChanges(`/{{.keyspace}}/_changes?doc_ids=["docC","doc1"]`, body, "user5")
	require.Len(t, changes.Results, 2)

	// User has "*" channel access, use GET
	changes = rt.GetChanges(`/{{.keyspace}}/_changes?filter=_doc_ids&doc_ids=["docC","doc1","docD"]`, "user5")
	require.Len(t, changes.Results, 3)

	// User has "*" channel access, use GET with doc_ids plain comma separated list
	changes = rt.GetChanges(`/{{.keyspace}}/_changes?filter=_doc_ids&doc_ids=docC,doc1,doc2,docD`, "user5")
	require.Len(t, changes.Results, 4)

	// Admin User
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "docA"]}`
	changes = rt.PostChangesAdmin("/{{.keyspace}}/_changes", body)
	require.Len(t, changes.Results, 4)

	// Use since value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user3")
	require.Len(t, changes.Results, 3)
	assert.Equal(t, "docD", changes.Results[2].ID)

	// Use since value and limit value to restrict results
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "since":6, "limit":1}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user3")
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "doc4", changes.Results[0].ID)

	// test parameter include_docs=true
	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "include_docs":true }`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user3")
	require.Len(t, changes.Results, 4)
	assert.Equal(t, "docD", changes.Results[3].ID)
	var docBody db.Body
	assert.NoError(t, base.JSONUnmarshal(changes.Results[3].Doc, &docBody))
	assert.Equal(t, "docD", docBody[db.BodyId])

	// test parameter style=all_docs
	// Create a conflict revision on docC
	rest.RequireStatus(t, rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"new_edits":false, "docs": [{"_id": "docC","_rev": "2-b4afc58d8e61a6b03390e19a89d26643","foo": "bat", "channels":["beta"]}]}`), 201)

	rt.WaitForPendingChanges()

	body = `{"filter":"_doc_ids", "doc_ids":["docC", "b0gus", "doc4", "docD", "doc1"], "style":"all_docs"}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", body, "user3")
	require.Len(t, changes.Results, 4)
	assert.Equal(t, "docC", changes.Results[3].ID)
	require.Len(t, changes.Results[3].Changes, 2)
}

func updateTestDoc(rt *rest.RestTester, docid string, revid string, body string) (newRevId string, err error) {
	path := fmt.Sprintf("/{{.keyspace}}/%s", docid)
	if revid != "" {
		path = fmt.Sprintf("%s?rev=%s", path, revid)
	}
	response := rt.SendAdminRequest("PUT", path, body)
	if response.Code != 200 && response.Code != 201 {
		return "", fmt.Errorf("createDoc got unexpected response code : %d", response.Code)
	}

	var responseBody db.Body
	_ = base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	return responseBody["rev"].(string), nil
}

// Validate retrieval of various document body types using include_docs.
func TestChangesIncludeDocs(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`}
	rt := rest.NewRestTester(t, &rtConfig)
	testDB := rt.GetDatabase()
	testDB.RevsLimit = 3
	rt.GetDatabase().EnableAllowConflicts(rt.TB())
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	rt.CreateUser("user1", []string{"alpha", "beta"})

	// Create docs for each scenario
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
	response := rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace}}/doc_tombstone?rev=%s", revid), "")
	rest.RequireStatus(t, response, 200)

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
	for range 5 {
		docVersion := rt.UpdateDoc("doc_pruned", db.DocVersion{RevTreeID: revid}, `{"type": "pruned", "channels":["gamma"]}`)
		revid = docVersion.RevTreeID
	}

	// Doc w/ attachment
	revid, err = updateTestDoc(rt, "doc_attachment", "", `{"type": "attachments", "channels":["alpha"]}`)
	assert.NoError(t, err, "Error updating doc")
	attachmentBody := "this is the body of attachment"
	attachmentContentType := "text/plain"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	response = rt.SendAdminRequestWithHeaders("PUT", fmt.Sprintf("/{{.keyspace}}/doc_attachment/attach1?rev=%s", revid),
		attachmentBody, reqHeaders)
	rest.RequireStatus(t, response, 201)

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
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc_conflict?new_edits=false", newEdits_conflict)
	rest.RequireStatus(t, response, 201)

	// Resolved conflict
	revid, err = updateTestDoc(rt, "doc_resolved_conflict", "", `{"type": "resolved_conflict", "channels":["alpha"]}`)
	assert.NoError(t, err)
	_, err = updateTestDoc(rt, "doc_resolved_conflict", revid, `{"type": "resolved_conflict", "channels":["alpha"]}`)
	assert.NoError(t, err)
	newEdits_conflict = `{"type": "resolved_conflict", "channels":["alpha"],
                   "_revisions": {"start": 2, "ids": ["conflicting_rev", "4e123c0497a1a6975540977ec127c06c"]}}`
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc_resolved_conflict?new_edits=false", newEdits_conflict)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/doc_resolved_conflict?rev=2-conflicting_rev", "")
	rest.RequireStatus(t, response, 200)

	rt.WaitForPendingChanges()
	changes := rt.GetChanges("/{{.keyspace}}/_changes?include_docs=true", "user1")

	expectedResults := make([]string, 10)
	expectedResults[0] = `{"seq":1,"id":"_user/user1","changes":[]}`
	expectedResults[1] = `{"seq":2,"id":"doc_active","doc":{"_id":"doc_active","_rev":"1-d59fda97ac4849f6a754fbcf4b522980","channels":["alpha"],"type":"active"},"changes":[{"rev":"1-d59fda97ac4849f6a754fbcf4b522980"}]}`
	expectedResults[2] = `{"seq":4,"id":"doc_multi_rev","doc":{"_id":"doc_multi_rev","_rev":"2-db2cf770921c3764b2d213ee0cbb5f45","channels":["alpha"],"type":"active","v":2},"changes":[{"rev":"2-db2cf770921c3764b2d213ee0cbb5f45"}]}`
	expectedResults[3] = `{"seq":6,"id":"doc_tombstone","deleted":true,"removed":["alpha"],"doc":{"_deleted":true,"_id":"doc_tombstone","_rev":"2-5bd8eb422f30e8d455940672e9e76549"},"changes":[{"rev":"2-5bd8eb422f30e8d455940672e9e76549"}]}`
	expectedResults[4] = `{"seq":8,"id":"doc_removed","removed":["alpha"],"doc":{"_id":"doc_removed","_removed":true,"_rev":"2-d15cb77d1dbe1cc06d27310de5b75914"},"changes":[{"rev":"2-d15cb77d1dbe1cc06d27310de5b75914"}]}`
	// for doc_pruned 4.0 cannot differentiate between channel removal or missing document - document body will not be returned here
	expectedResults[5] = `{"seq":12,"id":"doc_pruned","removed":["alpha"],"changes":[{"rev":"2-5afcb73bd3eb50615470e3ba54b80f00"}]}`
	expectedResults[6] = `{"seq":18,"id":"doc_attachment","doc":{"_attachments":{"attach1":{"content_type":"text/plain","digest":"sha1-nq0xWBV2IEkkpY3ng+PEtFnCcVY=","length":30,"revpos":2,"stub":true}},"_id":"doc_attachment","_rev":"2-0b0457923508d99ec1929d2316d14cf2","channels":["alpha"],"type":"attachments"},"changes":[{"rev":"2-0b0457923508d99ec1929d2316d14cf2"}]}`
	expectedResults[7] = `{"seq":19,"id":"doc_large_numbers","doc":{"_id":"doc_large_numbers","_rev":"1-2721633d9000e606e9c642e98f2f5ae7","channels":["alpha"],"largefloat":1234567890.1234,"largeint":1234567890,"type":"large_numbers"},"changes":[{"rev":"1-2721633d9000e606e9c642e98f2f5ae7"}]}`
	expectedResults[8] = `{"seq":22,"id":"doc_conflict","doc":{"_id":"doc_conflict","_rev":"2-conflicting_rev","channels":["alpha"],"type":"conflict"},"changes":[{"rev":"2-conflicting_rev"}]}`
	expectedResults[9] = `{"seq":26,"id":"doc_resolved_conflict","doc":{"_id":"doc_resolved_conflict","_rev":"2-251ba04e5889887152df5e7a350745b4","channels":["alpha"],"type":"resolved_conflict"},"changes":[{"rev":"2-251ba04e5889887152df5e7a350745b4"}]}`

	for index, result := range changes.Results {
		assertChangeEntryMatches(t, expectedResults[index], result)
	}

	// Flush the rev cache, and issue changes again to ensure successful handling for rev cache misses
	rt.GetDatabase().FlushRevisionCacheForTest()
	// Also nuke temporary revision backup of doc_pruned.  Validates that the body for the pruned revision is generated correctly when no longer resident in the rev cache
	err = collection.PurgeOldRevisionJSON(ctx, "doc_pruned", prunedRevId)
	require.NoError(t, err)

	// since 4.0 we cannot differentiate between an old channel removal and a missing document - in this case the document body is not returned
	expectedResults[5] = `{"seq":12,"id":"doc_pruned","removed":["alpha"],"changes":[{"rev":"2-5afcb73bd3eb50615470e3ba54b80f00"}]}`

	postFlushChanges := rt.GetChanges("/{{.keyspace}}/_changes?include_docs=true", "user1")

	assert.Equal(t, len(expectedResults), len(postFlushChanges.Results))

	for index, result := range postFlushChanges.Results {

		assertChangeEntryMatches(t, expectedResults[index], result)
		var expectedChange db.ChangeEntry
		assert.NoError(t, base.JSONUnmarshal([]byte(expectedResults[index]), &expectedChange))
		if expectedChange.Doc != nil {
			// result.Doc is json.RawMessage, and properties may not be in the same order for a direct comparison
			var expectedBody db.Body
			var resultBody db.Body
			assert.NoError(t, expectedBody.Unmarshal(expectedChange.Doc))
			assert.NoError(t, resultBody.Unmarshal(result.Doc))

			// strip out _cv - it's not deterministic in tests like _rev is, but also tests really shouldn't be comparing full bodies! It's so brittle...
			delete(resultBody, db.BodyCV)

			db.AssertEqualBodies(t, expectedBody, resultBody)
		} else {
			assert.Equal(t, expectedChange.Doc, result.Doc)
		}
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
	expectedStyleAllDocs[9] = `{"seq":26,"id":"doc_resolved_conflict","changes":[{"rev":"2-251ba04e5889887152df5e7a350745b4"},{"rev":"3-f25ad98ef169791adec6c1d385717b84"}]}`

	allDocsChanges := rt.GetChanges("/{{.keyspace}}/_changes?style=all_docs", "user1")
	for index, result := range allDocsChanges.Results {
		assert.Equal(t, expectedStyleAllDocs[index], fmt.Sprintf("%s", base.MustJSONMarshal(t, result)))
	}

	// Validate style=all_docs, include_docs=true permutations.  Only modified doc from include_docs test is doc_conflict (adds open revisions)
	expectedResults[8] = `{"seq":22,"id":"doc_conflict","doc":{"_id":"doc_conflict","_rev":"2-conflicting_rev","channels":["alpha"],"type":"conflict"},"changes":[{"rev":"2-conflicting_rev"},{"rev":"2-869a7167ccbad634753105568055bd61"}]}`
	expectedResults[9] = `{"seq":26,"id":"doc_resolved_conflict","doc":{"_id":"doc_resolved_conflict","_rev":"2-251ba04e5889887152df5e7a350745b4","channels":["alpha"],"type":"resolved_conflict"},"changes":[{"rev":"2-251ba04e5889887152df5e7a350745b4"},{"rev":"3-f25ad98ef169791adec6c1d385717b84"}]}`

	combinedChanges := rt.GetChanges("/{{.keyspace}}/_changes?style=all_docs&include_docs=true", "user1")
	assert.Equal(t, len(expectedResults), len(combinedChanges.Results))

	for index, result := range combinedChanges.Results {
		assertChangeEntryMatches(t, expectedResults[index], result)
	}
}

// Tests view backfills into empty cache and validates that results are prepended to cache
func TestChangesViewBackfillFromQueryOnly(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)
	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/abc%d", i), `{"channels":["ABC"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/pbs%d", i), `{"channels":["PBS"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/nbc%d", i), `{"channels":["NBC"]}`)
		rest.RequireStatus(t, response, 201)
		cacheWaiter.Add(3)
	}

	cacheWaiter.Wait()

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// Initialize query count
	queryCount := testDb.DbStats.Cache().ViewQueries.Value()

	// Issue a since=0 changes request.  Validate that there is a view-based backfill
	changesJSON := `{"since":0, "limit":50}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 10)
	// Validate that there has been a view query
	secondQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, queryCount+1, secondQueryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 10)
	thirdQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, secondQueryCount, thirdQueryCount)

}

// Validate that non-contiguous query results (due to limit) are not prepended to the cache
func TestChangesViewBackfillNonContiguousQueryResults(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/abc%d", i), `{"channels":["ABC"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/pbs%d", i), `{"channels":["PBS"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/nbc%d", i), `{"channels":["NBC"]}`)
		rest.RequireStatus(t, response, 201)
		cacheWaiter.Add(3)
	}
	cacheWaiter.Wait()

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// Issue a since=0 changes request, with limit less than the number of PBS documents

	// Issue a since=0, limit 5 changes request.  Results shouldn't be prepended to the cache, since they aren't
	// contiguous with the cache's validFrom.
	changesJSON := `{"since":0, "limit":5}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)

	queryCount := testDb.DbStats.Cache().ViewQueries.Value()
	log.Printf("After initial changes request, query count is :%d", queryCount)

	// Issue another since=0, limit 5 changes request.  Validate that there is another a view-based backfill
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)
	// Validate that there has been one more view query
	secondQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, queryCount+1, secondQueryCount)

	// Issue a since=20, limit 5 changes request.  Results should be prepended to the cache (seqs 23, 26, 29)
	changesJSON = `{"since":20, "limit":5}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 3)
	// Validate that there has been one more view query
	thirdQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, secondQueryCount+1, thirdQueryCount)

	// Issue a since=20, limit 5 changes request again.  Results should be served from the cache (seqs 23, 26, 29)
	changesJSON = `{"since":20, "limit":5}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 3)
	// Validate that there hasn't been another view query
	fourthQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, thirdQueryCount, fourthQueryCount)
}

// Tests multiple view backfills and validates that results are prepended to cache
func TestChangesViewBackfillFromPartialQueryOnly(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/abc%d", i), `{"channels":["ABC"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/pbs%d", i), `{"channels":["PBS"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/nbc%d", i), `{"channels":["NBC"]}`)
		rest.RequireStatus(t, response, 201)
		cacheWaiter.Add(3)
	}

	cacheWaiter.Wait()

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// Issue a since=n changes request, where n > 0 and is a non-PBS sequence.  Validate that there's a view-based backfill
	changesJSON := `{"since":15, "limit":50}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)

	queryCount := testDb.DbStats.Cache().ViewQueries.Value()
	log.Printf("After initial changes request, query count is :%d", queryCount)

	// Issue a since=0 changes request.  Expect a second view query to retrieve the additional results
	changesJSON = `{"since":0, "limit":50}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 10)
	// Validate that there has been one more view query
	secondQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, queryCount+1, secondQueryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 10)
	// Validate that there haven't been any more view queries
	thirdQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, secondQueryCount, thirdQueryCount)

}

// Tests multiple view backfills and validates that results are prepended to cache
func TestChangesViewBackfillNoOverlap(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Write 30 docs to the bucket, 10 from channel PBS.  Expected sequence assignment:
	//  1, 4, 7, ...   ABC
	//  2, 5, 8, ...   PBS
	//  3, 6, 9, ...   NBC
	for i := 1; i <= 10; i++ {
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/abc%d", i), `{"channels":["ABC"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/pbs%d", i), `{"channels":["PBS"]}`)
		rest.RequireStatus(t, response, 201)
		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/nbc%d", i), `{"channels":["NBC"]}`)
		rest.RequireStatus(t, response, 201)
		cacheWaiter.Add(3)
	}

	cacheWaiter.Wait()

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// Write some more docs to the bucket, with a gap before the first PBS sequence
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc11", `{"channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc12", `{"channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc13", `{"channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc14", `{"channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs11", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.Add(5)
	cacheWaiter.Wait()

	// Issue a since=n changes request, where 0 < n < 30 (high sequence at cache init)Validate that there's a view-based backfill
	changesJSON := `{"since":15, "limit":50}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 6)

	queryCount := testDb.DbStats.Cache().ViewQueries.Value()
	log.Printf("After initial changes request, query count is :%d", queryCount)

	// Issue the same changes request.  Validate that there is not another a view-based backfill
	changesJSON = `{"since":15, "limit":50}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 6)
	// Validate that there has been one more view query
	secondQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, queryCount, secondQueryCount)

}

// Tests view backfill and validates that results are prepended to cache
func TestChangesViewBackfill(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc3", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.AddAndWait(3)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// Add a few more docs (to increment the channel cache's validFrom)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc4", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc5", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.AddAndWait(2)

	// Issue a since=0 changes request.  Validate that there's a view-based backfill
	changesJSON := `{"since":0, "limit":50}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)
	queryCount := testDb.DbStats.Cache().ViewQueries.Value()
	log.Printf("After initial changes request, query count is: %d", queryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)
	// Validate that there haven't been any more view queries
	assert.Equal(t, testDb.DbStats.Cache().ViewQueries.Value(), queryCount)

}

// Tests view backfill and validates that results are prepended to cache
func TestChangesViewBackfillStarChannel(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "*"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc5", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc4", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc3", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.Add(3)
	cacheWaiter.Wait()

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// Add a few more docs (to increment the channel cache's validFrom)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.Add(2)
	cacheWaiter.Wait()

	// Issue a since=0 changes request.  Validate that there's a view-based backfill
	changesJSON := `{"since":0, "limit":50}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 5)
	for index, entry := range changes.Results {
		// Expects docs in sequence order from 1-5
		assert.Equal(t, uint64(index+1), entry.Seq.Seq)
		log.Printf("Entry:%+v", entry)
	}
	queryCount := testDb.DbStats.Cache().ViewQueries.Value()
	log.Printf("After initial changes request, query count is: %d", queryCount)

	// Issue another since=0 changes request.  Validate that there is not another a view-based backfill
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)
	for index, entry := range changes.Results {
		// Expects docs in sequence order from 1-5
		assert.Equal(t, uint64(index+1), entry.Seq.Seq)
		log.Printf("Entry:%+v", entry)
	}
	// Validate that there haven't been any more view queries
	assert.Equal(t, queryCount, testDb.DbStats.Cache().ViewQueries.Value())

}

// Tests query backfill with limit
func TestChangesQueryBackfillWithLimit(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	testDb := rt.GetDatabase()
	ctx := rt.Context()

	changesLimitTests := []struct {
		name                string // test name
		queryLimit          int    // CacheOptions.ChannelQueryLimit
		requestLimit        int    // _changes request limit
		totalDocuments      int    // Total document in the user's channel (documents also created in non-user channel)
		expectedQueryCount  int64  // Expected number of channel queries performed for channel
		expectedResultCount int    // Expected number of documents returned by _changes
	}{
		{ // queryLimit < requestLimit < totalDocuments
			name:                "case1",
			queryLimit:          5,
			requestLimit:        15,
			totalDocuments:      20,
			expectedQueryCount:  3,
			expectedResultCount: 15,
		},
		{ // queryLimit <  totalDocuments < requestLimit
			name:                "case2",
			queryLimit:          5,
			requestLimit:        25,
			totalDocuments:      20,
			expectedQueryCount:  5,
			expectedResultCount: 20,
		},
		{ // requestLimit < queryLimit < totalDocuments
			name:                "case3",
			queryLimit:          10,
			requestLimit:        5,
			totalDocuments:      20,
			expectedQueryCount:  1,
			expectedResultCount: 5,
		},
		{ // requestLimit < totalDocuments < queryLimit
			name:                "case4",
			queryLimit:          25,
			requestLimit:        15,
			totalDocuments:      20,
			expectedQueryCount:  1,
			expectedResultCount: 15,
		},
		{ // totalDocuments < queryLimit < requestLimit
			name:                "case5",
			queryLimit:          25,
			requestLimit:        30,
			totalDocuments:      20,
			expectedQueryCount:  1,
			expectedResultCount: 20,
		},
		{ // totalDocuments < requestLimit < queryLimit
			name:                "case6",
			queryLimit:          30,
			requestLimit:        25,
			totalDocuments:      20,
			expectedQueryCount:  1,
			expectedResultCount: 20,
		},
		{ // totalDocuments = requestLimit < queryLimit
			name:                "case7",
			queryLimit:          30,
			requestLimit:        20,
			totalDocuments:      20,
			expectedQueryCount:  1,
			expectedResultCount: 20,
		},
		{ // totalDocuments = queryLimit < requestLimit
			name:                "case8",
			queryLimit:          20,
			requestLimit:        30,
			totalDocuments:      20,
			expectedQueryCount:  2,
			expectedResultCount: 20,
		},
		{ // queryLimit < totalDocuments = requestLimit
			name:                "case9",
			queryLimit:          20,
			requestLimit:        30,
			totalDocuments:      30,
			expectedQueryCount:  2,
			expectedResultCount: 30,
		},
		{ // requestLimit < totalDocuments = queryLimit
			name:                "case10",
			queryLimit:          30,
			requestLimit:        30,
			totalDocuments:      20,
			expectedQueryCount:  1,
			expectedResultCount: 20,
		},
		{ // queryLimit = requestLimit < totalDocuments
			name:                "case11",
			queryLimit:          20,
			requestLimit:        20,
			totalDocuments:      30,
			expectedQueryCount:  1,
			expectedResultCount: 20,
		},
		{ // totalDocuments < requestLimit = queryLimit
			name:                "case12",
			queryLimit:          30,
			requestLimit:        30,
			totalDocuments:      20,
			expectedQueryCount:  1,
			expectedResultCount: 20,
		},
	}

	for _, test := range changesLimitTests {

		rt.Run(test.name, func(t *testing.T) {
			testDb.Options.CacheOptions.ChannelQueryLimit = test.queryLimit

			// Create user
			username := "user_" + test.name
			a := testDb.Authenticator(ctx)
			testUser, err := a.NewUser(username, "letmein", channels.BaseSetOf(t, test.name))
			assert.NoError(t, err)
			assert.NoError(t, a.Save(testUser))

			// Put documents
			cacheWaiter := testDb.NewDCPCachingCountWaiter(t)
			for i := 1; i <= test.totalDocuments; i++ {
				response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s_%d", test.name, i), fmt.Sprintf(`{"channels":["%s"]}`, test.name))
				rest.RequireStatus(t, response, 201)
				// write a document not in the channel, to ensure high sequence as query end value doesn't bypass query edge cases
				response = rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s_%d_other", test.name, i), `{"channels":["other"]}`)
				rest.RequireStatus(t, response, 201)
			}
			cacheWaiter.AddAndWait(test.totalDocuments * 2)

			collection, ctx := rt.GetSingleTestDatabaseCollection()
			// Flush the channel cache
			assert.NoError(t, collection.FlushChannelCache(ctx))
			startQueryCount := testDb.GetChannelQueryCount()

			// Issue a since=0 changes request.
			changesJSON := fmt.Sprintf(`{"since":0, "limit":%d}`, test.requestLimit)
			changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, username)
			require.Equal(t, test.expectedResultCount, len(changes.Results))

			// Validate expected number of queries
			finalQueryCount := testDb.GetChannelQueryCount()
			// We expect one query for the public channel, so need to subtract one for the expected count for the channel
			assert.Equal(t, test.expectedQueryCount, finalQueryCount-startQueryCount-1)
		})
	}

}

// Tests query backfill with limit
func TestMultichannelChangesQueryBackfillWithLimit(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()
	testDb := rt.GetDatabase()
	ctx := rt.Context()

	testDb.Options.CacheOptions.ChannelQueryLimit = 5

	// Create user with access to three channels
	username := "user_" + t.Name()
	a := testDb.Authenticator(ctx)
	testUser, err := a.NewUser(username, "letmein", channels.BaseSetOf(t, "ch1", "ch2", "ch3"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(testUser))

	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put documents with varying channel values:
	//    Sequences      Channels
	//    ---------      --------
	//     1-10			ch1
	//     11-20		ch1, ch3
	//     21-30		ch1
	//     31-40		ch2
	//     41-50		ch1, ch2, ch3
	channelSets := []string{`"ch1"`, `"ch1", "ch3"`, `"ch1"`, `"ch2"`, `"ch1", "ch2", "ch3"`}

	for i := range 50 {
		channelSet := channelSets[i/10]
		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.keyspace}}/%s_%d", t.Name(), i), fmt.Sprintf(`{"channels":[%s]}`, channelSet))
		rest.RequireStatus(t, response, 201)
	}
	cacheWaiter.AddAndWait(50)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// 1. Issue a since=0 changes request, validate results
	changesJSON := fmt.Sprintf(`{"since":0}`)
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, username)
	require.Len(t, changes.Results, 50)

	// Verify results ordering
	for i := range 50 {
		assert.Equal(t, fmt.Sprintf("%s_%d", t.Name(), i), changes.Results[i].ID)
	}

	// 2. Same again, but with limit on the changes request
	assert.NoError(t, collection.FlushChannelCache(ctx))
	changesJSON = fmt.Sprintf(`{"since":0, "limit":25}`)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, username)
	require.Len(t, changes.Results, 25)

	// Verify results ordering
	for i := range 25 {
		assert.Equal(t, fmt.Sprintf("%s_%d", t.Name(), i), changes.Results[i].ID)
	}

}

// Tests star channel query backfill with limit
func TestChangesQueryStarChannelBackfillLimit(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	queryLimit := 5
	rtConfig.DatabaseConfig = &rest.DatabaseConfig{DbConfig: rest.DbConfig{CacheConfig: &rest.CacheConfig{ChannelCacheConfig: &rest.ChannelCacheConfig{DeprecatedQueryLimit: &queryLimit}}}}
	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig) // not sure why this doesn't work
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "*"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put 10 documents
	for i := 1; i <= 10; i++ {
		key := fmt.Sprintf("%s%d", t.Name(), i)
		response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+key, `{"starChannel":"istrue"}`)
		rest.RequireStatus(t, response, 201)
	}

	cacheWaiter.AddAndWait(10)

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))
	startQueryCount := testDb.DbStats.Cache().ViewQueries.Value()

	// Issue a since=0 changes request.  Validate that there's a view-based backfill
	changesJSON := `{"since":0, "limit":7}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Equal(t, len(changes.Results), 7)
	finalQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	assert.Equal(t, int64(2), finalQueryCount-startQueryCount)
}

// Tests view backfill with slow query, checks duplicate handling for cache entries if a document is updated after query runs, but before document is
// prepended to the cache.  Reproduces #3475
func TestChangesViewBackfillSlowQuery(t *testing.T) {

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip test with LeakyBucket dependency test when running in integration")
	}

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc, oldDoc){channel(doc.channels);}`}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	testDb := rt.ServerContext().Database(ctx, "db")
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(1)

	// Put rev1 of document
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revId := body["rev"].(string)

	cacheWaiter.Wait()

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Flush the channel cache
	assert.NoError(t, collection.FlushChannelCache(ctx))

	// Write another doc, to initialize the cache (and guarantee overlap)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.Add(1)
	cacheWaiter.Wait()

	// Set up PostQueryCallback on bucket - will be invoked when changes triggers the cache backfill view query

	postQueryCallback := func(ddoc, viewName string, params map[string]any) {
		log.Printf("Got callback for %s, %s, %v", ddoc, viewName, params)
		// Check which channel the callback was invoked for
		startkey, ok := params["startkey"].([]any)
		log.Printf("startkey: %v %T", startkey, startkey)
		channelName := ""
		if ok && len(startkey) > 1 {
			channelName, _ = startkey[0].(string)
		}
		if viewName == "channels" && channelName == "PBS" {
			// Update doc1
			log.Printf("Putting doc w/ revid:%s", revId)
			updateResponse := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?rev="+revId, `{"modified":true, "channels":["PBS"]}`)
			rest.RequireStatus(t, updateResponse, 201)
			cacheWaiter.Add(1)
			cacheWaiter.Wait()
		}

	}

	leakyDataStore, ok := base.AsLeakyDataStore(rt.TestBucket.GetSingleDataStore())
	require.True(t, ok)

	leakyDataStore.SetPostQueryCallback(postQueryCallback)

	// Issue a since=0 changes request.  Will cause the following:
	//   1. Retrieves doc2 from the cache
	//   2. View query retrieves doc1 rev-1
	//   3. PostQueryCallback forces doc1 rev-2 to be added to the cache
	//   4. doc1 rev-1 is prepended to the cache
	//   5. Returns 1 + 2 (doc 2, doc 1 rev-1).  This is correct (doc1 rev2 wasn't cached when the changes query was issued),
	//      but now the cache has two versions of doc1.  Subsequent changes request will returns duplicates.
	changesJSON := `{"since":0, "limit":50}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 2)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
	}
	queryCount := testDb.DbStats.Cache().ViewQueries.Value()
	log.Printf("After initial changes request, query count is: %d", queryCount)

	leakyDataStore.SetPostQueryCallback(nil)

	// Issue another since=0 changes request - cache SHOULD only have a single rev for doc1
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 2)
	// Validate that there haven't been any more view queries
	updatedQueryCount := testDb.DbStats.Cache().ViewQueries.Value()
	log.Printf("After second changes request, query count is: %d", updatedQueryCount)
	assert.Equal(t, queryCount, updatedQueryCount)

}

func TestChangesActiveOnlyWithLimit(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	rt.GetDatabase().EnableAllowConflicts(rt.TB())
	ctx := rt.Context()
	testDb := rt.ServerContext().Database(ctx, "db")

	// Create user:
	a := testDb.Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))
	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put several documents
	var body db.Body
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/deletedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	deletedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	removedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc0", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	partialRemovalRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/conflictedDoc", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.AddAndWait(1)

	// Create a conflict, then tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictTombstone"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/conflictedDoc?rev=1-conflictTombstone", "")
	rest.RequireStatus(t, response, 200)
	cacheWaiter.AddAndWait(1)

	// Create a conflict, and don't tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictActive"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	// Pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)

	// Delete
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/deletedDoc?rev="+deletedRev, "")
	rest.RequireStatus(t, response, 200)
	cacheWaiter.AddAndWait(1)

	// Removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", fmt.Sprintf(`{"_rev":%q, "channel":["HBO"]}`, removedRev))
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	// Partially removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", fmt.Sprintf(`{"_rev":%q, "channel":["PBS"]}`, partialRemovalRev))
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	// Create additional active docs
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc1", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc2", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc3", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc4", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc5", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	// Normal changes
	rt.WaitForPendingChanges()
	changesJSON = `{"style":"all_docs"}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 10)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 3)
		}
	}

	// Active only NO Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only with Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}
	// Active only with Limit, GET
	changes = rt.GetChanges("/{{.keyspace}}/_changes?style=all_docs&active_only=true&limit=5", "bernard")
	require.Len(t, changes.Results, 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}
}

// Test active-only and limit handling during cache backfill from the view.  Flushes the channel cache
// prior to changes requests in order to force view backfill.  Covers https://github.com/couchbase/sync_gateway/issues/2955 in
// additional to general view handling.
func TestChangesActiveOnlyWithLimitAndViewBackfill(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	rt.GetDatabase().EnableAllowConflicts(rt.TB())

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	cacheWaiter := rt.ServerContext().Database(ctx, "db").NewDCPCachingCountWaiter(t)
	// Put several documents
	var body db.Body
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/deletedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	deletedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	removedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc0", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	partialRemovalRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/conflictedDoc", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(5)

	// Create a conflict, then tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictTombstone"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/conflictedDoc?rev=1-conflictTombstone", "")
	rest.RequireStatus(t, response, 200)
	cacheWaiter.AddAndWait(1)

	// Create a conflict, and don't tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictActive"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)
	cacheWaiter.AddAndWait(1)

	// Get pre-delete changes
	changesJSON := `{"style":"all_docs"}`
	changes := rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)

	// Delete
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/deletedDoc?rev="+deletedRev, "")
	rest.RequireStatus(t, response, 200)

	// Removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", fmt.Sprintf(`{"_rev":%q, "channel":["HBO"]}`, removedRev))
	rest.RequireStatus(t, response, 201)

	// Partially removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", fmt.Sprintf(`{"_rev":%q, "channel":["PBS"]}`, partialRemovalRev))
	rest.RequireStatus(t, response, 201)

	// Create additional active docs
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc1", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc2", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc3", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc4", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc5", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.AddAndWait(8)

	// Normal changes
	rt.WaitForPendingChanges()
	changesJSON = `{"style":"all_docs"}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 10)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 3)
		}
	}

	collection, ctx := rt.GetSingleTestDatabaseCollection()
	// Active only NO Limit, POST
	assert.NoError(t, collection.FlushChannelCache(ctx))

	changesJSON = `{"style":"all_docs", "active_only":true}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only with Limit, POST
	assert.NoError(t, collection.FlushChannelCache(ctx))
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only with Limit, GET
	assert.NoError(t, collection.FlushChannelCache(ctx))
	changes = rt.GetChanges("/{{.keyspace}}/_changes?style=all_docs&active_only=true&limit=5", "bernard")
	require.Len(t, changes.Results, 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	assert.NoError(t, collection.FlushChannelCache(ctx))
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// No limit active only, GET, followed by normal (https://github.com/couchbase/sync_gateway/issues/2955)
	assert.NoError(t, collection.FlushChannelCache(ctx))
	changes = rt.GetChanges("/{{.keyspace}}/_changes?style=all_docs&active_only=true", "bernard")
	require.Len(t, changes.Results, 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	changes = rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
	require.Len(t, changes.Results, 10)
}

func TestChangesActiveOnlyWithLimitLowRevCache(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	cacheSize := 2
	shortWaitConfig := &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		CacheConfig: &rest.CacheConfig{
			ChannelCacheConfig: &rest.ChannelCacheConfig{
				MinLength: &cacheSize,
				MaxLength: &cacheSize,
			},
		},
	}}

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel)}`, DatabaseConfig: shortWaitConfig}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	rt.GetDatabase().EnableAllowConflicts(rt.TB())

	// /it := initIndexTester(false, `function(doc) {channel(doc.channel);}`)
	// defer it.Close()

	// Create user:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS", "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents
	var body db.Body
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/deletedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	deletedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", `{"channel":["PBS"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	removedRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc0", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", `{"channel":["PBS","ABC"]}`)
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	partialRemovalRev := body["rev"].(string)
	rest.RequireStatus(t, response, 201)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/conflictedDoc", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// Create a conflict, then tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictTombstone"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/conflictedDoc?rev=1-conflictTombstone", "")
	rest.RequireStatus(t, response, 200)

	// Create a conflict, and don't tombstone it
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id":"conflictedDoc","channel":["PBS"], "_rev":"1-conflictActive"}], "new_edits":false}`)
	rest.RequireStatus(t, response, 201)

	// Get pre-delete changes
	rt.WaitForChanges(5, "/{{.keyspace}}/_changes?style=all_docs", "bernard", false)

	// Delete
	response = rt.SendAdminRequest("DELETE", "/{{.keyspace}}/deletedDoc?rev="+deletedRev, "")
	rest.RequireStatus(t, response, 200)

	// Removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/removedDoc", fmt.Sprintf(`{"_rev":%q, "channel":["HBO"]}`, removedRev))
	rest.RequireStatus(t, response, 201)

	// Partially removed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/partialRemovalDoc", fmt.Sprintf(`{"_rev":%q, "channel":["PBS"]}`, partialRemovalRev))
	rest.RequireStatus(t, response, 201)

	// Create additional active docs
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc1", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc2", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc3", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc4", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/activeDoc5", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// Normal changes
	changes := rt.WaitForChanges(10, "/{{.keyspace}}/_changes?style=all_docs", "bernard", false)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 3)
		}
	}

	// Active only NO Limit, POST
	changesJSON := `{"style":"all_docs", "active_only":true}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only with Limit, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":5}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}
	// Active only with Limit, GET
	changes = rt.GetChanges("/{{.keyspace}}/_changes?style=all_docs&active_only=true&limit=5", "bernard")
	require.Len(t, changes.Results, 5)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}

	// Active only with Limit set higher than number of revisions, POST
	changesJSON = `{"style":"all_docs", "active_only":true, "limit":15}`
	changes = rt.PostChanges("/{{.keyspace}}/_changes", changesJSON, "bernard")
	require.Len(t, changes.Results, 8)
	for _, entry := range changes.Results {
		log.Printf("Entry:%+v", entry)
		// validate conflicted handling
		if entry.ID == "conflictedDoc" {
			require.Len(t, entry.Changes, 2)
		}
	}
}

// Test _changes returning conflicts
func TestChangesIncludeConflicts(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache, base.KeyChanges, base.KeyCRUD)

	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc,oldDoc) {
			 channel(doc.channel)
		 }`,
	}
	rt := rest.NewRestTester(t, &rtConfig)
	defer rt.Close()

	rt.GetDatabase().EnableAllowConflicts(rt.TB())

	// Create conflicted document documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/conflictedDoc", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// Create two conflicting revisions
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/conflictedDoc?new_edits=false", `{"_revisions":{"start":2, "ids":["conflictOne", "82214a562e80c8fa7b2361719847bc73"]}, "value":"c1", "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/conflictedDoc?new_edits=false", `{"_revisions":{"start":2, "ids":["conflictTwo", "82214a562e80c8fa7b2361719847bc73"]}, "value":"c2", "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	rt.WaitForPendingChanges()
	changes := rt.PostChangesAdmin("/{{.keyspace}}/_changes?style=all_docs", "{}")
	require.Len(t, changes.Results, 1)
	require.Len(t, changes.Results[0].Changes, 2)

}

// Test _changes handling large sequence values - ensures no truncation of large ints.
func TestChangesLargeSequences(t *testing.T) {
	t.Skip("Skipping test due to known issue with large sequence numbers under GSI/views/rosmar")
	largeInitialSeq := uint64(9223372036854775807)
	require.Equal(t, strconv.FormatUint(largeInitialSeq, 10), strconv.FormatUint(math.MaxInt64, 10))
	for _, initialSeq := range []uint64{2, largeInitialSeq} {
		t.Run(fmt.Sprintf("initialSeq=%d", initialSeq), func(t *testing.T) {
			userSeq := initialSeq + 1
			docSeq := initialSeq + 2
			rtConfig := rest.RestTesterConfig{
				SyncFn:      channels.DocChannelsSyncFunction,
				InitSyncSeq: initialSeq,
			}
			rt := rest.NewRestTester(t, &rtConfig)
			defer rt.Close()

			const user = "alice"

			rt.CreateUser(user, []string{"PBS"}) // 9223372036854775808

			docID := "largeSeqDocForChanges"
			userDoc := "_user/" + user
			// Create document
			rt.PutDoc(docID, `{"channels":["PBS"]}`) // 9223372036854775809
			rt.WaitForPendingChanges()
			seq := rt.GetDocumentSequence(docID)

			// Incorrect results
			// Couchbase Server Views / GSI / rosmar views
			// - alice can not access the document
			response := rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/"+docID, "", user)
			require.Equal(t, response.Code, http.StatusOK)

			// Incorrect results
			//
			// rosmar views:
			// - returns sequence as 9223372036854776000
			assert.Equal(t, strconv.FormatUint(initialSeq+2, 10), strconv.FormatUint(seq, 10))
			assert.Equal(t, strconv.FormatUint(seq, 10), strconv.FormatUint(docSeq, 10))
			assert.Equal(t, docSeq, seq)

			changes := rt.GetChanges("/{{.keyspace}}/_changes?since="+strconv.FormatUint(initialSeq, 10), user)
			// Incorrect results
			//
			// Couchbase Server Views:
			// - returns only largeSeqDocForChanges, not principal doc with the wrong sequence 9223372036854775808 (expected 9223372036854775809)
			// rosmar views:
			// - returns only largeSeqDocForChanges, not principal doc with the wrong sequence 9223372036854775808 (expected 9223372036854775809)
			// GSI:
			// - returns only largeSeqDocForChanges, not principal doc with the wrong sequence 9223372036854775808 (expected 9223372036854775809)
			assert.Len(t, changes.Results, 2)
			assert.Equal(t, userDoc, changes.Results[0].ID)
			assert.Equal(t, userSeq, changes.Results[0].Seq.Seq)

			if assert.Len(t, changes.Results, 2) {
				assert.Equal(t, docID, changes.Results[1].ID)
				assert.Equal(t, docSeq, changes.Results[1].Seq.Seq)
				assert.Equal(t, strconv.FormatUint(docSeq, 10), changes.Last_Seq.String())
			}
			// start at expected sequence of largeSeqDocForChanges
			// Incorrect results

			// Couchbase Server Views:
			// - returns no docs, even though we expect largeSeqDocForChanges
			// rosmar views:
			// - returns no docs, even though we expect largeSeqDocForChanges
			// GSI:
			// - returns no docs, even though we expect largeSeqDocForChanges
			changes = rt.GetChanges("/{{.keyspace}}/_changes?since="+strconv.FormatUint(userSeq, 10), user)
			if assert.Len(t, changes.Results, 1) {
				assert.Equal(t, docID, changes.Results[0].ID)
				assert.Equal(t, docSeq, changes.Results[0].Seq.Seq)
				assert.Equal(t, strconv.FormatUint(docSeq, 10), changes.Last_Seq.String())
			}
			// Incorrect results

			// Couchbase Server Views:
			// - returns largeSeqDocForChanges, with the wrong sequence 9223372036854775808 (expected 9223372036854775809)
			// - this is different than non admin GetChanges above, which doesn't return any
			// rosmar views:
			// - returns largeSeqDocForChanges, with the wrong sequence 9223372036854775808 (expected 9223372036854775809)
			// - this is different than non admin GetChanges above, which doesn't return any
			// GSI:
			// - returns no documents, even though we expect largeSeqDocForChanges
			changes = rt.PostChangesAdmin("/{{.keyspace}}/_changes", fmt.Sprintf(`{"since":%d}`, userSeq))
			if assert.Len(t, changes.Results, 1) {
				assert.Equal(t, docID, changes.Results[0].ID)
				assert.Equal(t, docSeq, changes.Results[0].Seq.Seq)
				assert.Equal(t, strconv.FormatUint(docSeq, 10), changes.Last_Seq.String())
			}
		})
	}
}

func TestIncludeDocsWithPrincipals(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	ctx := rt.Context()
	testDb := rt.ServerContext().Database(ctx, "db")

	cacheWaiter := testDb.NewDCPCachingCountWaiter(t)

	// Put users
	rt.CreateUser("includeDocsUser", []string{"*"})
	rt.CreateUser("includeDocsUserr2", []string{"*"})

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc3", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	cacheWaiter.AddAndWait(3)

	// Get as admin
	changes := rt.PostChangesAdmin("/{{.keyspace}}/_changes?include_docs=true", "{}")
	// Expect three docs, no user docs
	require.Len(t, changes.Results, 3)

	// Get as user
	userChanges := rt.GetChanges("/{{.keyspace}}/_changes?include_docs=true", "includeDocsUser")
	// Expect three docs and the authenticated user's user doc
	require.Len(t, userChanges.Results, 4)

}

// Validate that an admin channel grant wakes up a waiting changes request
func TestChangesAdminChannelGrantLongpollNotify(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyHTTP)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// Create user with access to channel ABC:
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-1", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-2", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-3", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-4", `{"channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// make sure docs are written to change cache
	rt.WaitForPendingChanges()

	caughtUpCount := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()

	// Issue longpoll changes request
	var longpollWg sync.WaitGroup
	longpollWg.Add(1)
	go func() {
		defer longpollWg.Done()
		longpollChangesJSON := `{"since":0, "feed":"longpoll"}`
		changes := rt.PostChanges("/{{.keyspace}}/_changes", longpollChangesJSON, "bernard")
		// Expect to get 4 docs plus user doc
		require.Len(t, changes.Results, 5)
	}()

	require.NoError(t, rt.GetDatabase().WaitForCaughtUp(caughtUpCount+1))

	// Update the user doc to grant access to PBS
	response = rt.SendAdminRequest("PUT", "/db/_user/bernard", rest.GetUserPayload(t, "", "", "", rt.GetSingleDataStore(), []string{"ABC", "PBS"}, nil))
	rest.RequireStatus(t, response, 200)

	// Wait for longpoll to return
	longpollWg.Wait()

}

// Validate handling when a single channel cache is compacted while changes request is in wait mode
func TestCacheCompactDuringChangesWait(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyCache)

	smallCacheSize := 100
	minimumChannelCacheConfig := &rest.DatabaseConfig{DbConfig: rest.DbConfig{
		CacheConfig: &rest.CacheConfig{
			ChannelCacheConfig: &rest.ChannelCacheConfig{
				MaxNumber: &smallCacheSize,
			},
		},
	}}
	rtConfig := rest.RestTesterConfig{SyncFn: `function(doc) {channel(doc.channels)}`, DatabaseConfig: minimumChannelCacheConfig}

	rt := rest.NewRestTesterDefaultCollection(t, &rtConfig)
	defer rt.Close()

	caughtUpCount := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()

	// Get 100 changes requests into wait mode (each for a different channel)
	changesURLPattern := "/db/_changes?filter=sync_gateway/bychannel&feed=longpoll&since=0&channels=%s"
	var longpollWg sync.WaitGroup
	queryCount := 100
	for i := range queryCount {
		longpollWg.Add(1)
		go func(i int) {
			defer longpollWg.Done()
			channelName := fmt.Sprintf("chan_%d", i)
			changesURL := fmt.Sprintf(changesURLPattern, channelName)
			changes := rt.PostChangesAdmin(changesURL, "{}")
			// Expect to get 1 doc
			require.Len(t, changes.Results, 1)
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
	cacheSize := rt.GetDatabase().DbStats.Cache().ChannelCacheNumChannels.Value()
	log.Printf("Cache size after compaction: %v", cacheSize)
	assert.True(t, cacheSize >= 60)
	assert.True(t, cacheSize <= 80)

	channelSet := make([]string, queryCount)
	for i := range queryCount {
		channelSet[i] = fmt.Sprintf("chan_%d", i)
	}

	// Write a single doc to all 100 channels, ensure everyone gets it
	channelDocBody := make(map[string]any)
	channelDocBody["channels"] = channelSet
	bodyBytes, err := base.JSONMarshal(channelDocBody)
	assert.NoError(t, err, "Marshal error for 100 channel doc")

	putResponse := rt.SendAdminRequest("PUT", "/db/100ChannelDoc", string(bodyBytes))
	rest.RequireStatus(t, putResponse, 201)

	// Wait for longpolls to return
	longpollWg.Wait()
}

func TestTombstoneCompaction(t *testing.T) {
	base.LongRunningTest(t)

	if !base.TestUseXattrs() {
		t.Skip("If running with no xattrs compact acts as a no-op")
	}

	tests := []struct {
		numDocs                      int
		runAsScheduledBackgroundTask bool
	}{
		// Multiples of Batch Size
		{numDocs: db.QueryTombstoneBatch},
		{numDocs: db.QueryTombstoneBatch * 4},
		// Smaller Than Batch Size
		{numDocs: 2},
		{numDocs: db.QueryTombstoneBatch / 4},
		// Larger than Batch Size
		{numDocs: db.QueryTombstoneBatch + 20},
	}

	var rt *rest.RestTester
	numCollections := 1

	if base.TestsUseNamedCollections() {
		numCollections = 2
		rt = rest.NewRestTesterMultipleCollections(t, nil, numCollections)
	} else {
		rt = rest.NewRestTester(t, nil)
	}
	defer rt.Close()

	// force compaction
	rt.GetDatabase().Options.TestPurgeIntervalOverride = base.Ptr(time.Duration(0))

	for _, test := range tests {
		for _, runAsScheduledBackgroundTask := range []bool{false, true} {
			t.Run(fmt.Sprintf("numDocs:%d asBackgroundTask:%v", test.numDocs, runAsScheduledBackgroundTask), func(t *testing.T) {

				// seed with tombstones
				for count := 0; count < test.numDocs; count++ {
					for _, keyspace := range rt.GetKeyspaces() {
						response := rt.SendAdminRequest("POST", fmt.Sprintf("/%s/", keyspace), `{"foo":"bar"}`)
						assert.Equal(t, http.StatusOK, response.Code)
						var body db.Body
						err := base.JSONUnmarshal(response.Body.Bytes(), &body)
						assert.NoError(t, err)
						revID := body["rev"].(string)
						docID := body["id"].(string)

						response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/%s/%s?rev=%s", keyspace, docID, revID), "")
						assert.Equal(t, http.StatusOK, response.Code)
					}
				}

				expectedCompactions := test.numDocs * numCollections
				expectedBatches := (test.numDocs/db.QueryTombstoneBatch + 1) * numCollections

				numCompactionsBefore := int(rt.GetDatabase().DbStats.Database().NumTombstonesCompacted.Value())
				var numBatchesBefore int
				if base.TestsDisableGSI() {
					numBatchesBefore = int(rt.GetDatabase().DbStats.Query(fmt.Sprintf(base.StatViewFormat, db.DesignDocSyncHousekeeping(), db.ViewTombstones)).QueryCount.Value())
				} else {
					numBatchesBefore = int(rt.GetDatabase().DbStats.Query(db.QueryTypeTombstones).QueryCount.Value())
				}

				numIdleKvOpsBefore := int(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value())
				numIdleQueryOpsBefore := int(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleQueryOps.Value())

				if runAsScheduledBackgroundTask {
					database, err := db.CreateDatabase(rt.GetDatabase())
					require.NoError(t, err)
					purgedCount, err := database.Compact(base.TestCtx(t), false, nil, base.NewSafeTerminator(), true)
					require.NoError(t, err)
					require.Equal(t, expectedCompactions, purgedCount)

					numIdleKvOpsAfter := int(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value())
					numIdleQueryOpsAfter := int(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleQueryOps.Value())

					// cannot do equal here because there are other idle kv ops unrelated to compaction
					assert.GreaterOrEqual(t, numIdleKvOpsAfter-numIdleKvOpsBefore, expectedCompactions)
					assert.Equal(t, numIdleQueryOpsAfter-numIdleQueryOpsBefore, expectedBatches)
				} else {
					resp := rt.SendAdminRequest("POST", "/{{.db}}/_compact", "")
					rest.RequireStatus(t, resp, http.StatusOK)
					err := rt.WaitForCondition(func() bool {
						return rt.GetDatabase().TombstoneCompactionManager.GetRunState() == db.BackgroundProcessStateCompleted
					})
					assert.NoError(t, err)

					numIdleKvOpsAfter := int(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value())
					numIdleQueryOpsAfter := int(base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleQueryOps.Value())

					// ad-hoc compactions don't invoke idle ops - but we do have other idle kv ops so can't ensure it stays zero
					assert.GreaterOrEqual(t, numIdleKvOpsAfter-numIdleKvOpsBefore, 0)
					assert.Equal(t, numIdleQueryOpsAfter-numIdleQueryOpsBefore, 0)
				}

				actualCompactions := int(rt.GetDatabase().DbStats.Database().NumTombstonesCompacted.Value()) - numCompactionsBefore
				require.Equal(t, expectedCompactions, actualCompactions)

				var actualBatches int
				if base.TestsDisableGSI() {
					actualBatches = int(rt.GetDatabase().DbStats.Query(fmt.Sprintf(base.StatViewFormat, db.DesignDocSyncHousekeeping(), db.ViewTombstones)).QueryCount.Value()) - numBatchesBefore
				} else {
					actualBatches = int(rt.GetDatabase().DbStats.Query(db.QueryTypeTombstones).QueryCount.Value()) - numBatchesBefore
				}
				require.Equal(t, expectedBatches, actualBatches)
			})
		}
	}
}

// TestOneShotGrantTiming simulates a one-shot changes feed returning before a previously issued grant has been
// buffered over DCP.
func TestOneShotGrantTiming(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyHTTP)

	defer db.SuspendSequenceBatching()()

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: `function(doc) {
				channel(doc.channel);
				if (doc.accessUser != "") {
					access(doc.accessUser, doc.accessChannel)
				}
			}`,
		})
	defer rt.Close()

	// Create user with access to no channels
	ctx := rt.Context()
	database := rt.GetDatabase()
	a := database.Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", nil)
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-1", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-2", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-3", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-4", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// Allocate a sequence but do not write a doc for it - will block DCP buffering until sequence is skipped
	slowSequence, seqErr := db.AllocateTestSequence(database)
	require.NoError(t, seqErr)
	log.Printf("Allocated slowSequence: %v", slowSequence)

	// Write a document granting user access to PBS
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/grantDoc", `{"accessUser":"bernard", "accessChannel":"PBS"}`)
	rest.RequireStatus(t, response, 201)

	// Issue normal one-shot changes request.  Expect no results as granting document hasn't been buffered (blocked by
	// slowSequence)
	// do not call WaitForPendingChanges() here due to slow sequence release
	changes := rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
	require.Len(t, changes.Results, 0)

	// Release the slow sequence and wait for it to be processed over DCP
	releaseErr := db.ReleaseTestSequence(base.DatabaseLogCtx(base.TestCtx(t), database.Name, nil), database, slowSequence)
	require.NoError(t, releaseErr)

	// Issue normal one-shot changes request.  Expect results as granting document buffering is unblocked
	rt.WaitForPendingChanges()
	changes = rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
	require.Len(t, changes.Results, 4)
}

// TestOneShotGrantRequestPlus simulates a one-shot changes feed being made before a previously issued grant has been
// buffered over DCP.  When requestPlus is set, changes feed should block until grant is processed.
func TestOneShotGrantRequestPlus(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyHTTP)

	defer db.SuspendSequenceBatching()() // Required for slow sequence simulation

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: `function(doc) {
				channel(doc.channel);
				if (doc.accessUser != "") {
					access(doc.accessUser, doc.accessChannel)
				}
			}`,
		})
	defer rt.Close()

	// Create user with access to no channels
	ctx := rt.Context()
	database := rt.GetDatabase()
	a := database.Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", nil)
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-1", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-2", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-3", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-4", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// Allocate a sequence but do not write a doc for it - will block DCP buffering until sequence is skipped
	slowSequence, seqErr := db.AllocateTestSequence(database)
	require.NoError(t, seqErr)

	// Write a document granting user access to PBS
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/grantDoc", `{"accessUser":"bernard", "accessChannel":"PBS"}`)
	rest.RequireStatus(t, response, 201)

	caughtUpStart := database.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp.Value()

	// avoid calling WaitForPendingChanges() here due to slow sequence release
	var oneShotComplete sync.WaitGroup
	// Issue a GET requestPlus one-shot changes request in a separate goroutine.
	oneShotComplete.Add(1)
	go func() {
		defer oneShotComplete.Done()
		changes := rt.GetChanges("/{{.keyspace}}/_changes?request_plus=true", "bernard")
		require.Len(t, changes.Results, 4)
	}()

	// Issue a POST requestPlus one-shot changes request in a separate goroutine.
	oneShotComplete.Add(1)
	go func() {
		defer oneShotComplete.Done()
		changes := rt.PostChanges("/{{.keyspace}}/_changes", `{"request_plus":true}`, "bernard")
		require.Len(t, changes.Results, 4)
	}()

	// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
	require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+2))

	// Release the slow sequence and wait for it to be processed over DCP
	releaseErr := db.ReleaseTestSequence(base.DatabaseLogCtx(base.TestCtx(t), database.Name, nil), database, slowSequence)
	require.NoError(t, releaseErr)
	rt.WaitForPendingChanges()

	oneShotComplete.Wait()
}

// TestOneShotGrantRequestPlusDbConfig simulates a one-shot changes feed being made before a previously issued grant has been
// buffered over DCP.  When requestPlus is set via config, changes feed should block until grant is processed.
func TestOneShotGrantRequestPlusDbConfig(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeyHTTP)

	defer db.SuspendSequenceBatching()()

	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: `function(doc) {
				channel(doc.channel);
				if (doc.accessUser != "") {
					access(doc.accessUser, doc.accessChannel)
				}
			}`,
			DatabaseConfig: &rest.DatabaseConfig{
				DbConfig: rest.DbConfig{
					ChangesRequestPlus: base.Ptr(true),
				},
			},
		})
	defer rt.Close()

	// Create user with access to no channels
	ctx := rt.Context()
	database := rt.GetDatabase()
	a := database.Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", nil)
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-1", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-2", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-3", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-4", `{"channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	// Allocate a sequence but do not write a doc for it - will block DCP buffering until sequence is skipped
	slowSequence, seqErr := db.AllocateTestSequence(database)
	require.NoError(t, seqErr)
	log.Printf("Allocated slowSequence: %v", slowSequence)

	// Write a document granting user access to PBS
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/grantDoc", `{"accessUser":"bernard", "accessChannel":"PBS"}`)
	rest.RequireStatus(t, response, 201)

	// Issue one-shot GET changes request explicitly setting request_plus=false (should override config value).
	// Expect no results as granting document hasn't been buffered (blocked by slowSequence)
	changes := rt.GetChanges("/{{.keyspace}}/_changes?request_plus=false", "bernard")
	require.Len(t, changes.Results, 0)

	// Issue one-shot POST changes request explicitly setting request_plus=false (should override config value).
	// Expect no results as granting document hasn't been buffered (blocked by slowSequence)
	changes = rt.PostChanges("/{{.keyspace}}/_changes", `{"request_plus":false}`, "bernard")
	require.Len(t, changes.Results, 0)

	caughtUpStart := database.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp.Value()

	var oneShotComplete sync.WaitGroup
	// Issue a GET one-shot changes request in a separate goroutine.  Should run as request plus based on config
	oneShotComplete.Add(1)
	go func() {
		defer oneShotComplete.Done()
		changes := rt.GetChanges("/{{.keyspace}}/_changes", "bernard")
		require.Len(t, changes.Results, 4)
	}()

	// Issue a POST one-shot changes request in a separate goroutine. Should run as request plus based on config
	oneShotComplete.Add(1)
	go func() {
		defer oneShotComplete.Done()
		changes := rt.PostChanges("/{{.keyspace}}/_changes", `{}`, "bernard")
		require.Len(t, changes.Results, 4)
	}()

	// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
	require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+2))

	// Release the slow sequence and wait for it to be processed over DCP
	releaseErr := db.ReleaseTestSequence(base.DatabaseLogCtx(base.TestCtx(t), database.Name, nil), database, slowSequence)
	require.NoError(t, releaseErr)
	rt.WaitForPendingChanges()

	oneShotComplete.Wait()
}

func waitForCompactStopped(dbc *db.DatabaseContext) error {
	for range 100 {
		compactRunning := dbc.CacheCompactActive()
		if !compactRunning {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return errors.New("waitForCompactStopped didn't stop")
}
