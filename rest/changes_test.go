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
	collection := rt.GetSingleTestDatabaseCollection()

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	assert.NoError(t, a.Save(guest))

	// Create user1
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", GetUserPayload(t, "user1", "letmein", "user1@couchbase.com", collection, []string{"alpha"}, nil))
	RequireStatus(t, response, 201)

	// Create 100 docs
	for i := 0; i < 100; i++ {
		docpath := fmt.Sprintf("/{{.keyspace}}/doc%d", i)
		RequireStatus(t, rt.SendRequest("PUT", docpath, `{"foo": "bar", "channels":["alpha"]}`), 201)
	}

	limit := 50
	changesResults, err := rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?limit=%d", limit), "user1", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	since := changesResults.Results[49].Seq
	assert.Equal(t, "doc48", changesResults.Results[49].ID)

	// // Check the _changes feed with  since and limit, to get second half of feed
	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?since=\"%s\"&limit=%d", since, limit), "user1", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	assert.Equal(t, "doc98", changesResults.Results[49].ID)

	// Create user2
	response = rt.SendAdminRequest("PUT", "/db/_user/user2", GetUserPayload(t, "user2", "letmein", "user2@couchbase.com", collection, []string{"alpha"}, nil))
	RequireStatus(t, response, 201)

	// Retrieve all changes for user2 with no limits
	changesResults, err = rt.WaitForChanges(101, fmt.Sprintf("/{{.keyspace}}/_changes"), "user2", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 101)
	assert.Equal(t, "doc99", changesResults.Results[99].ID)

	// Create user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", GetUserPayload(t, "user3", "letmein", "user3@couchbase.com", collection, []string{"alpha"}, nil))
	RequireStatus(t, response, 201)

	getUserResponse := rt.SendAdminRequest("GET", "/db/_user/user3", "")
	RequireStatus(t, getUserResponse, 200)
	log.Printf("create user response: %s", getUserResponse.Body.Bytes())

	// Get the sequence from the user doc to validate against the triggered by value in the changes results
	user3, _ := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetUser("user3")
	userSequence := user3.Sequence()

	// Get first 50 document changes.
	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?limit=%d", limit), "user3", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	since = changesResults.Results[49].Seq
	assert.Equal(t, "doc49", changesResults.Results[49].ID)
	assert.Equal(t, userSequence, since.TriggeredBy)

	// // Get remainder of changes i.e. no limit parameter
	changesResults, err = rt.WaitForChanges(51, fmt.Sprintf("/{{.keyspace}}/_changes?since=\"%s\"", since), "user3", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 51)
	assert.Equal(t, "doc99", changesResults.Results[49].ID)

	// Create user4
	response = rt.SendAdminRequest("PUT", "/db/_user/user4", GetUserPayload(t, "user4", "letmein", "user4@couchbase.com", collection, []string{"alpha"}, nil))
	RequireStatus(t, response, 201)
	// Get the sequence from the user doc to validate against the triggered by value in the changes results
	user4, _ := rt.GetDatabase().Authenticator(base.TestCtx(t)).GetUser("user4")
	user4Sequence := user4.Sequence()

	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?limit=%d", limit), "user4", false)
	assert.NoError(t, err, "Unexpected error")
	require.Len(t, changesResults.Results, 50)
	since = changesResults.Results[49].Seq
	assert.Equal(t, "doc49", changesResults.Results[49].ID)
	assert.Equal(t, user4Sequence, since.TriggeredBy)

	// // Check the _changes feed with  since and limit, to get second half of feed
	changesResults, err = rt.WaitForChanges(50, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&limit=%d", since, limit), "user4", false)
	assert.Equal(t, nil, err)
	require.Len(t, changesResults.Results, 50)
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
		},
		}}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	wg.Add(2)
	const docID = "doc1"
	version1 := rt.PutDoc(docID, `{"foo":"bar"}`)

	// push winning branch
	wg.Add(2)
	res := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?new_edits=false", `{"foo":"buzz","_revisions":{"start":3,"ids":["buzz","bar","`+version1.RevTreeID+`"]}}`)
	RequireStatus(t, res, http.StatusCreated)
	winningVersion := DocVersionFromPutResponse(t, res)

	// push non-winning branch
	wg.Add(1)
	_ = rt.PutNewEditsFalse(docID, NewDocVersionFromFakeRev("2-buzzzzz"), version1, `{"foo":"buzzzzz"}`)
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
	res = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?new_edits=false", `{"foo":"quux","_revisions":{"start":4,"ids":["quux", "buzz","bar","`+version1.RevTreeID+`"]}}`)
	RequireStatus(t, res, http.StatusCreated)
	newWinningVersion := DocVersionFromPutResponse(t, res)

	// tombstone the winning branch, we should get a second webhook fired for rev 2-buzzzzz now it's been resurrected
	wg.Add(2)
	rt.DeleteDoc(docID, newWinningVersion)

	wg.Wait()
	assert.Equal(t, 5, int(atomic.LoadUint32(&WinningRevChangedCount)))
	assert.Equal(t, 6, int(atomic.LoadUint32(&DocumentChangedCount)))
}

func TestCVPopulationOnChangesViaAPI(t *testing.T) {
	t.Skip("Disabled until REST support for version is added")
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channels)}`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	ctx := base.TestCtx(t)
	collection := rt.GetSingleTestDatabaseCollection()
	bucketUUID := rt.GetDatabase().EncodedBucketUUID
	const DocID = "doc1"

	// activate channel cache
	_, err := rt.WaitForChanges(0, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+DocID, `{"channels": ["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	require.NoError(t, collection.WaitForPendingChanges(base.TestCtx(t)))

	changes, err := rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	fetchedDoc, _, err := collection.GetDocWithXattr(ctx, DocID, db.DocUnmarshalCAS)
	require.NoError(t, err)

	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, bucketUUID, changes.Results[0].CurrentVersion.SourceID)
	assert.Equal(t, fetchedDoc.Cas, changes.Results[0].CurrentVersion.Value)
}

func TestCVPopulationOnDocIDChanges(t *testing.T) {
	t.Skip("Disabled until REST support for version is added")
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channels)}`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	ctx := base.TestCtx(t)
	collection := rt.GetSingleTestDatabaseCollection()
	bucketUUID := rt.GetDatabase().EncodedBucketUUID
	const DocID = "doc1"

	// activate channel cache
	_, err := rt.WaitForChanges(0, "/{{.keyspace}}/_changes", "", true)
	require.NoError(t, err)

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+DocID, `{"channels": ["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	require.NoError(t, collection.WaitForPendingChanges(base.TestCtx(t)))

	changes, err := rt.WaitForChanges(1, fmt.Sprintf(`/{{.keyspace}}/_changes?filter=_doc_ids&doc_ids=%s`, DocID), "", true)
	require.NoError(t, err)

	fetchedDoc, _, err := collection.GetDocWithXattr(ctx, DocID, db.DocUnmarshalCAS)
	require.NoError(t, err)

	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, bucketUUID, changes.Results[0].CurrentVersion.SourceID)
	assert.Equal(t, fetchedDoc.Cas, changes.Results[0].CurrentVersion.Value)
}
