//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReadChangesOptionsFromJSON(t *testing.T) {

	ctx := base.TestCtx(t)
	h := &handler{}
	h.server = NewServerContext(ctx, &StartupConfig{}, false)
	defer h.server.Close(ctx)

	// Basic case, no heartbeat, no timeout
	optStr := `{"feed":"longpoll", "since": "123456:78", "limit":123, "style": "all_docs",
				"include_docs": true, "filter": "Melitta", "channels": "ABC,BBC"}`
	feed, options, filter, channelsArray, _, _, err := h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, "longpoll", feed)

	assert.Equal(t, uint64(78), options.Since.Seq)
	assert.Equal(t, uint64(123456), options.Since.TriggeredBy)
	assert.Equal(t, 123, options.Limit)
	assert.Equal(t, true, options.Conflicts)
	assert.Equal(t, true, options.IncludeDocs)
	assert.Equal(t, uint64(kDefaultHeartbeatMS), options.HeartbeatMs)
	assert.Equal(t, uint64(kDefaultTimeoutMS), options.TimeoutMs)

	assert.Equal(t, "Melitta", filter)
	assert.Equal(t, []string{"ABC", "BBC"}, channelsArray)

	// Attempt to set heartbeat, timeout to valid values
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":30000, "timeout":60000}`
	_, options, _, _, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(30000), options.HeartbeatMs)
	assert.Equal(t, uint64(60000), options.TimeoutMs)

	// Attempt to set valid timeout, no heartbeat
	optStr = `{"feed":"longpoll", "since": "1", "timeout":2000}`
	_, options, _, _, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(2000), options.TimeoutMs)

	// Disable heartbeat, timeout by explicitly setting to zero
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":0, "timeout":0}`
	_, options, _, _, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), options.HeartbeatMs)
	assert.Equal(t, uint64(0), options.TimeoutMs)

	// Attempt to set heartbeat less than minimum heartbeat, timeout greater than max timeout
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":1000, "timeout":1000000}`
	_, options, _, _, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(kMinHeartbeatMS), options.HeartbeatMs)
	assert.Equal(t, uint64(kMaxTimeoutMS), options.TimeoutMs)

	// Set max heartbeat in server context, attempt to set heartbeat greater than max
	h.server.Config.Replicator.MaxHeartbeat = base.NewConfigDuration(time.Minute)
	optStr = `{"feed":"longpoll", "since": "1", "heartbeat":90000}`
	_, options, _, _, _, _, err = h.readChangesOptionsFromJSON([]byte(optStr))
	assert.NoError(t, err)
	assert.Equal(t, uint64(60000), options.HeartbeatMs)
}

// Test for wrong _changes entries for user joining a populated channel
func TestUserJoiningPopulatedChannel(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyCache, base.KeyAccess, base.KeyCRUD, base.KeyChanges)

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channels)}`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	assert.NoError(t, a.Save(guest))

	rt.CreateUser("user1", []string{"alpha"})

	// Create 100 docs
	for i := 0; i < 100; i++ {
		docpath := fmt.Sprintf("/{{.keyspace}}/doc%d", i)
		RequireStatus(t, rt.SendRequest("PUT", docpath, `{"foo": "bar", "channels":["alpha"]}`), 201)
	}

	limit := 50
	changesResults := rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?limit=%d", limit), "user1", false)
	since := changesResults.Results[49].Seq
	assert.Equal(t, "doc48", changesResults.Results[49].ID)

	// // Check the _changes feed with  since and limit, to get second half of feed
	changesResults = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?since=\"%s\"&limit=%d", since, limit), "user1", false)
	assert.Equal(t, "doc98", changesResults.Results[49].ID)

	rt.CreateUser("user2", []string{"alpha"})

	// Retrieve all changes for user2 with no limits
	changesResults = rt.WaitForChanges(101, fmt.Sprintf("/{{.keyspace}}/_changes"), "user2", false)
	assert.Equal(t, "doc99", changesResults.Results[99].ID)

	rt.CreateUser("user3", []string{"alpha"})

	getUserResponse := rt.SendAdminRequest("GET", "/db/_user/user3", "")
	RequireStatus(t, getUserResponse, 200)
	log.Printf("create user response: %s", getUserResponse.Body.Bytes())

	// Get the sequence from the user doc to validate against the triggered by value in the changes results
	user3, _ := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetUser("user3")
	userSequence := user3.Sequence()

	// Get first 50 document changes.
	changesResults = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?limit=%d", limit), "user3", false)
	since = changesResults.Results[49].Seq
	assert.Equal(t, "doc49", changesResults.Results[49].ID)
	assert.Equal(t, userSequence, since.TriggeredBy)

	// // Get remainder of changes i.e. no limit parameter
	changesResults = rt.WaitForChanges(51, fmt.Sprintf("/{{.keyspace}}/_changes?since=\"%s\"", since), "user3", false)
	assert.Equal(t, "doc99", changesResults.Results[49].ID)

	rt.CreateUser("user4", []string{"alpha"})
	// Get the sequence from the user doc to validate against the triggered by value in the changes results
	user4, err := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetUser("user4")
	require.NoError(t, err)
	user4Sequence := user4.Sequence()

	changesResults = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?limit=%d", limit), "user4", false)
	since = changesResults.Results[49].Seq
	assert.Equal(t, "doc49", changesResults.Results[49].ID)
	assert.Equal(t, user4Sequence, since.TriggeredBy)

	// // Check the _changes feed with  since and limit, to get second half of feed
	changesResults = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&limit=%d", since, limit), "user4", false)
	assert.Equal(t, "doc99", changesResults.Results[49].ID)

}

// TestWebhookWinningRevChangedEvent ensures the winning_rev_changed event is only fired for a winning revision change, and checks that document_changed is always fired.
func TestWebhookWinningRevChangedEvent(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyEvents)

	wg := sync.WaitGroup{}

	var WinningRevChangedCount uint32
	var DocumentChangedCount uint32

	handler := func(w http.ResponseWriter, r *http.Request) {
		var body db.Body
		d := base.JSONDecoder(r.Body)
		require.NoError(t, d.Decode(&body))
		require.Contains(t, body, db.BodyId)
		require.Contains(t, body, db.BodyRev)

		event := r.URL.Query().Get("event")
		switch event {
		case "WinningRevChanged":
			atomic.AddUint32(&WinningRevChangedCount, 1)
		case "DocumentChanged":
			atomic.AddUint32(&DocumentChangedCount, 1)
		default:
			t.Fatalf("unknown event type: %s", event)
		}

		wg.Done()
	}

	s := httptest.NewServer(http.HandlerFunc(handler))
	defer s.Close()

	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			EventHandlers: &EventHandlerConfig{
				DocumentChanged: []*EventConfig{
					{Url: s.URL + "?event=DocumentChanged", Filter: "function(doc){return true;}", HandlerType: "webhook"},
					{Url: s.URL + "?event=WinningRevChanged", Filter: "function(doc){return true;}", HandlerType: "webhook",
						Options: map[string]interface{}{db.EventOptionDocumentChangedWinningRevOnly: true},
					},
				},
			},
			AllowConflicts: base.Ptr(true),
		},
		}}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	wg.Add(2)
	const docID = "doc1"
	version1 := rt.PutDoc(docID, `{"foo":"bar"}`)

	// push winning branch
	wg.Add(2)
	res := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?new_edits=false", `{"foo":"buzz","_revisions":{"start":3,"ids":["buzz","bar","`+version1.RevID+`"]}}`)
	RequireStatus(t, res, http.StatusCreated)
	winningVersion := DocVersionFromPutResponse(t, res)

	// push non-winning branch
	wg.Add(1)
	_ = rt.PutNewEditsFalse(docID, NewDocVersionFromFakeRev("2-buzzzzz"), &version1, `{"foo":"buzzzzz"}`)
	RequireStatus(t, res, http.StatusCreated)

	wg.Wait()
	assert.Equal(t, 2, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 3, int(atomic.LoadUint32(&DocumentChangedCount)))

	// tombstone the winning branch and ensure we get a rev changed message for the promoted branch
	wg.Add(2)
	rt.DeleteDoc(docID, winningVersion)

	wg.Wait()
	assert.Equal(t, 3, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 4, int(atomic.LoadUint32(&DocumentChangedCount)))

	// push a separate winning branch
	wg.Add(2)
	res = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?new_edits=false", `{"foo":"quux","_revisions":{"start":4,"ids":["quux", "buzz","bar","`+version1.RevID+`"]}}`)
	RequireStatus(t, res, http.StatusCreated)
	newWinningVersion := DocVersionFromPutResponse(t, res)

	// tombstone the winning branch, we should get a second webhook fired for rev 2-buzzzzz now it's been resurrected
	wg.Add(2)
	rt.DeleteDoc(docID, newWinningVersion)

	wg.Wait()
	assert.Equal(t, 5, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 6, int(atomic.LoadUint32(&DocumentChangedCount)))
}

// TestJumpInSequencesAtAllocatorSkippedSequenceFill:
//   - High level test
//   - Add a doc through Sync Gateway
//   - Alter that allocated sequence to be higher value. Mocking this document arriving from different env
//     (e.g. via XDCR)
//   - Wait for this sequence to arrive over cache feed and wait for skipped sequences to subsequently fill
//   - Update this doc again, triggering unused sequence range release
//   - Write another doc and assert that the changes feed returns all expected docs
func TestJumpInSequencesAtAllocatorSkippedSequenceFill(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test requires xattrs because it writes directly to the xattr")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport: false,
			CacheConfig: &CacheConfig{
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxWaitPending: base.Ptr(uint32(10)),
				},
			},
		}},
	})
	defer rt.Close()
	ctx := base.TestCtx(t)

	vrs := rt.PutDoc("doc", `{"prop":true}`)

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_changes", "")
	RequireStatus(t, resp, http.StatusOK)

	ds := rt.GetSingleDataStore()
	xattrs, cas, err := ds.GetXattrs(ctx, "doc", []string{base.SyncXattrName})
	require.NoError(t, err)

	var retrievedXattr map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedXattr))
	retrievedXattr["sequence"] = uint64(20)
	newXattrVal := map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, retrievedXattr),
	}

	_, err = ds.UpdateXattrs(ctx, "doc", 0, cas, newXattrVal, nil)
	require.NoError(t, err)

	// wait for value to move from pending to cache and skipped list to fill
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rt.GetDatabase().UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(1), rt.GetDatabase().DbStats.CacheStats.SkippedSequenceSkiplistNodes.Value())
	}, time.Second*10, time.Millisecond*100)

	docVrs := rt.UpdateDoc("doc", vrs, `{"prob": "lol"}`)

	// wait skipped list to be emptied by release of sequence range
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rt.GetDatabase().UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(0), rt.GetDatabase().DbStats.CacheStats.PendingSeqLen.Value())
		assert.Equal(c, int64(0), rt.GetDatabase().DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(0), rt.GetDatabase().DbStats.CacheStats.SkippedSequenceSkiplistNodes.Value())
	}, time.Second*10, time.Millisecond*100)

	doc1Vrs := rt.PutDoc("doc1", `{"prop":true}`)

	changes := rt.WaitForChanges(2, "/{{.keyspace}}/_changes", "", true)
	changes.RequireDocIDs(t, []string{"doc1", "doc"})
	changes.RequireRevID(t, []string{docVrs.RevID, doc1Vrs.RevID})
}

// TestJumpInSequencesAtAllocatorRangeInPending:
//   - High level test
//   - Add a doc through Sync Gateway
//   - Alter that allocated sequence to be higher value. Mocking this document arriving from different env
//     (e.g. via XDCR)
//   - Wait for this sequence to arrive over cache feed and subsequently pushed to pending
//   - Update this doc again, triggering unused sequence range release
//   - Write another doc and assert that the changes feed returns all expected docs
func TestJumpInSequencesAtAllocatorRangeInPending(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test requires xattrs because it writes directly to the xattr")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport: false,
			CacheConfig: &CacheConfig{
				ChannelCacheConfig: &ChannelCacheConfig{
					MaxWaitPending: base.Ptr(uint32(1500)),
				},
			},
		}},
	})
	defer rt.Close()
	ctx := base.TestCtx(t)

	vrs := rt.PutDoc("doc", `{"prop":true}`)

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_changes", "")
	RequireStatus(t, resp, http.StatusOK)

	ds := rt.GetSingleDataStore()
	xattrs, cas, err := ds.GetXattrs(ctx, "doc", []string{base.SyncXattrName})
	require.NoError(t, err)

	var retrievedXattr map[string]interface{}
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &retrievedXattr))
	retrievedXattr["sequence"] = uint64(20)
	newXattrVal := map[string][]byte{
		base.SyncXattrName: base.MustJSONMarshal(t, retrievedXattr),
	}

	_, err = ds.UpdateXattrs(ctx, "doc", 0, cas, newXattrVal, nil)
	require.NoError(t, err)

	// wait for value top be added to pending
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rt.GetDatabase().UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(1), rt.GetDatabase().DbStats.CacheStats.PendingSeqLen.Value())
	}, time.Second*10, time.Millisecond*100)

	docVrs := rt.UpdateDoc("doc", vrs, `{"prob": "lol"}`)

	// assert that nothing has been pushed to skipped
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		rt.GetDatabase().UpdateCalculatedStats(ctx)
		assert.Equal(c, int64(0), rt.GetDatabase().DbStats.CacheStats.NumCurrentSeqsSkipped.Value())
		assert.Equal(c, int64(0), rt.GetDatabase().DbStats.CacheStats.SkippedSequenceSkiplistNodes.Value())
	}, time.Second*10, time.Millisecond*100)

	doc1Vrs := rt.PutDoc("doc1", `{"prop":true}`)

	changes := rt.WaitForChanges(2, "/{{.keyspace}}/_changes", "", true)
	changes.RequireDocIDs(t, []string{"doc1", "doc"})
	changes.RequireRevID(t, []string{docVrs.RevID, doc1Vrs.RevID})
}

// TestContinuousChangesUserDeleted ensures that deleting a user terminates a continuous _changes feed
// running for that user.
func TestContinuousChangesUserDeleted(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	const (
		username = "alice"
		channel  = "chan1"
	)
	rt.CreateUser(username, []string{channel})
	rt.PutDoc("beforeDelete", `{"channels":["`+channel+`"]}`)

	rt.WaitForPendingChanges()

	caughtUpCount := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()
	feed := rt.StartContinuousChanges("/{{.keyspace}}/_changes?feed=continuous&since=0", username)

	require.NoError(t, rt.GetDatabase().WaitForCaughtUp(caughtUpCount+1))

	rt.DeleteUser(username)
	RequireStatus(t, rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/_changes", "", username), http.StatusUnauthorized)

	changes := feed.RequireEnded()
	base.RequireWaitForStat(t, rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value, 0)

	require.Len(t, changes, 2)
	require.Equal(t, "_user/"+username, changes[0].ID)
	require.Equal(t, "beforeDelete", changes[1].ID)
}

// TestLongpollChangesUserDeleted is the longpoll counterpart to TestContinuousChangesUserDeleted.
func TestLongpollChangesUserDeleted(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	const (
		username = "alice"
		channel  = "chan1"
	)
	rt.CreateUser(username, []string{channel})

	rt.WaitForPendingChanges()
	since := rt.GetChanges("/{{.keyspace}}/_changes", username).Last_Seq
	caughtUpCount := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()

	var changes ChangesResults
	feedDone := make(chan struct{})
	go func() {
		defer close(feedDone)
		changes = rt.PostChanges("/{{.keyspace}}/_changes", fmt.Sprintf(`{"since":"%s", "feed":"longpoll"}`, since), username)
	}()

	require.NoError(t, rt.GetDatabase().WaitForCaughtUp(caughtUpCount+1))

	rt.DeleteUser(username)

	select {
	case <-feedDone:
	case <-time.After(30 * time.Second):
		require.Fail(t, "longpoll changes feed still running after user delete")
	}

	require.Empty(t, changes.Results)
}

// TestContinuousChangesRolePurge ensures that purging a role revokes the role's channels from a
// continuous _changes feed already running for a user who holds that role.  limit=1 terminates the feed
// after the next change it sends, so the test can assert on which document that was.
func TestContinuousChangesRolePurge(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	const (
		username = "alice"
		roleName = "chan1-role"
		// roleChannel is granted only via the role, userChannel is granted directly
		roleChannel = "chan1"
		userChannel = "chan2"
	)
	rt.CreateRole(roleName, []string{roleChannel})
	rt.CreateUser(username, []string{userChannel}, roleName)

	// Prove the role grant is in effect before the purge
	rt.PutDoc("beforePurge", `{"channels":["`+roleChannel+`"]}`)
	changes := rt.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", username, false)
	require.Equal(t, "beforePurge", changes.Results[len(changes.Results)-1].ID)

	// Start the feed at the current sequence, so the only entry it can send is one of the documents
	// written after the purge below
	rt.WaitForPendingChanges()
	since := rt.GetChanges("/{{.keyspace}}/_changes", username).Last_Seq
	caughtUpCount := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplCaughtUp.Value()
	feed := rt.StartContinuousChanges(fmt.Sprintf("/{{.keyspace}}/_changes?feed=continuous&limit=1&since=%s", since), username)

	require.NoError(t, rt.GetDatabase().WaitForCaughtUp(caughtUpCount+1))

	// The purge has to be notified before the writes below, otherwise the feed wakes on the document
	// notification alone and is still holding the stale channel set.
	userWaiter := rt.NewUserWaiter(username)
	RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/{{.db}}/_role/"+roleName+"?purge=true", ""), http.StatusOK)
	require.True(t, db.WaitForUserWaiterChange(userWaiter))

	// The feed sends in sequence order, so a feed still serving the revoked channel would send
	// afterPurgeRevoked first.
	rt.PutDoc("afterPurgeRevoked", `{"channels":["`+roleChannel+`"]}`)
	rt.PutDoc("afterPurgeAllowed", `{"channels":["`+userChannel+`"]}`)

	feedChanges := feed.RequireEnded()
	require.Len(t, feedChanges, 1)
	require.Equal(t, "afterPurgeAllowed", feedChanges[0].ID)
}
