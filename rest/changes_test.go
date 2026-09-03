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
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/websocket"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
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

// TestChangesSinceLogging drives a range of since values through every changes entry point SGW exposes -
// GET, POST, longpoll, continuous, admin, websocket and BLIP subChanges - and asserts the existing changes
// logging reports the client's unparsed since whenever it differs from the parsed value.
//
// The cases cover the four sequence wire forms, and for each, variants that SequenceID.String() normalizes
// by dropping a component (see the omission rules on db/sequence_id.go intSeqToString).  Without the raw
// value appended, each of those was indistinguishable in the logs from a well-formed sequence.
func TestChangesSinceLogging(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyChanges, base.KeySyncMsg)

	const (
		username = "alice"
		numDocs  = 5
	)

	cases := []struct {
		rawSince  string // since value sent by the client
		wantSince string // how it should render in the log: normalized, plus "(raw: X)" when they differ
	}{
		// {Seq} - the raw and normalized forms can never diverge, so no raw is appended
		{"0", "0"},
		{"2", "2"},
		{"999", "999"}, // past the end of the feed

		// {TriggeredBy, Seq}
		{"10:5", "10:5"},           // valid backfill, Seq < TriggeredBy
		{"5:10", "10 (raw: 5:10)"}, // TriggeredBy < Seq, TriggeredBy dropped
		{"5:5", "5 (raw: 5:5)"},    // TriggeredBy == Seq, TriggeredBy dropped

		// {LowSeq, Seq}
		{"2::5", "2::5"},          // valid, LowSeq < Seq
		{"3::2", "2 (raw: 3::2)"}, // LowSeq > Seq, LowSeq dropped
		{"5::5", "5 (raw: 5::5)"}, // LowSeq == Seq, LowSeq dropped

		// {LowSeq, TriggeredBy, Seq}
		{"2:10:5", "2:10:5"},               // valid, LowSeq < TriggeredBy and Seq < TriggeredBy
		{"12:10:5", "10:5 (raw: 12:10:5)"}, // backfill active but LowSeq >= TriggeredBy, LowSeq dropped
		{"2:5:10", "2::10 (raw: 2:5:10)"},  // no backfill (Seq >= TriggeredBy), TriggeredBy dropped
		{"12:5:10", "10 (raw: 12:5:10)"},   // no backfill and LowSeq >= Seq, both dropped
		{"5:5:5", "5 (raw: 5:5:5)"},        // all equal, both dropped

		// A leading zero component parses away entirely, so the raw value is preserved and reported even
		// though the parsed sequence is indistinguishable from a plain "5".
		{"0:5", "5 (raw: 0:5)"},
		{"0::5", "5 (raw: 0::5)"},
	}

	// Every REST shape converges on MultiChangesFeed, which logs the options via ChangesOptions.String().
	// The trailing comma anchors the end of the value: AssertLogContains is a substring match, so
	// `{Since: 2,` must not be allowed to match an actual `{Since: 2::5,`.
	wantREST := func(wantSince string) string {
		return fmt.Sprintf("options: {Since: %s,", wantSince)
	}
	// BLIP logs via logEndpointEntry -> SubChangesParams.String(), anchored by the trailing space.
	wantBLIP := func(wantSince string) string {
		return fmt.Sprintf("Type:subChanges Since:%s ", wantSince)
	}

	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channels)}`,
	})
	defer rt.Close()
	rt.CreateUser(username, []string{"alpha"})
	for i := range numDocs {
		rt.PutDoc(fmt.Sprintf("doc%d", i), `{"channels":["alpha"]}`)
	}
	rt.WaitForPendingChanges()

	// One server for every websocket case - the mock handler can't be hijacked for the upgrade.
	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()
	wsURL := strings.Replace(srv.URL, "http://", "ws://", 1) +
		"/" + rt.GetSingleKeyspace() + "/_changes?feed=websocket"

	// sendWebSocketChanges opens a websocket changes feed, sends options as the first message, and reads
	// the first response.  Errors are returned rather than asserted so the caller can assert outside the
	// AssertLogContains closure - see the note on the REST cases below.
	sendWebSocketChanges := func(url, options string) (received []byte, err error) {
		conn, err := websocket.Dial(url, "", srv.URL)
		if err != nil {
			return nil, err
		}
		defer func() { _ = conn.Close() }()
		if err := websocket.Message.Send(conn, options); err != nil {
			return nil, err
		}
		err = websocket.Message.Receive(conn, &received)
		return received, err
	}

	// wsOptions builds the first websocket message.  timeout bounds the server-side feed: without it the
	// options default to kDefaultTimeoutMS (5 minutes), so each case would leave a feed running long
	// after the client hangs up.
	wsOptions := func(since string) string {
		return fmt.Sprintf(`{"since":%q,"timeout":100}`, since)
	}

	for _, tc := range cases {
		// Named by rawSince, not wantSince - the raw values are all distinct, but several cases share a
		// normalized value, and duplicate t.Run names get silently suffixed.
		t.Run("since="+tc.rawSince, func(t *testing.T) {
			// Requests go inside the AssertLogContains closure, assertions outside it.  The helper
			// restores the console logger without a defer, so a require failure inside the closure
			// leaves the global logger writing into an orphaned buffer and corrupts later captures.
			// That's also why these use SendUserRequest rather than rt.GetChanges/PostChanges, which
			// assert status internally.
			t.Run("GET normal", func(t *testing.T) {
				var resp *TestResponse
				base.AssertLogContains(t, wantREST(tc.wantSince), func() {
					resp = rt.SendUserRequest(http.MethodGet,
						"/{{.keyspace}}/_changes?since="+tc.rawSince, "", username)
				})
				RequireStatus(t, resp, http.StatusOK)
			})

			t.Run("POST normal", func(t *testing.T) {
				var resp *TestResponse
				body := fmt.Sprintf(`{"since":%q}`, tc.rawSince)
				base.AssertLogContains(t, wantREST(tc.wantSince), func() {
					resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", body, username)
				})
				RequireStatus(t, resp, http.StatusOK)
			})

			t.Run("GET longpoll", func(t *testing.T) {
				var resp *TestResponse
				uri := fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&feed=longpoll&timeout=100", tc.rawSince)
				base.AssertLogContains(t, wantREST(tc.wantSince), func() {
					resp = rt.SendUserRequest(http.MethodGet, uri, "", username)
				})
				RequireStatus(t, resp, http.StatusOK)
			})

			t.Run("POST longpoll", func(t *testing.T) {
				var resp *TestResponse
				body := fmt.Sprintf(`{"since":%q,"feed":"longpoll","timeout":100}`, tc.rawSince)
				base.AssertLogContains(t, wantREST(tc.wantSince), func() {
					resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", body, username)
				})
				RequireStatus(t, resp, http.StatusOK)
			})

			t.Run("GET continuous", func(t *testing.T) {
				var resp *TestResponse
				uri := fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&feed=continuous&timeout=100", tc.rawSince)
				base.AssertLogContains(t, wantREST(tc.wantSince), func() {
					resp = rt.SendUserRequest(http.MethodGet, uri, "", username)
				})
				RequireStatus(t, resp, http.StatusOK)
			})

			t.Run("GET admin", func(t *testing.T) {
				var resp *TestResponse
				base.AssertLogContains(t, wantREST(tc.wantSince), func() {
					resp = rt.SendAdminRequest(http.MethodGet,
						"/{{.keyspace}}/_changes?since="+tc.rawSince, "")
				})
				RequireStatus(t, resp, http.StatusOK)
			})

			// The websocket feed reads its options - including since - from the first websocket message,
			// after the HTTP upgrade request has already been handled.
			t.Run("websocket", func(t *testing.T) {
				var received []byte
				var err error
				base.AssertLogContains(t, wantREST(tc.wantSince), func() {
					received, err = sendWebSocketChanges(wsURL, wsOptions(tc.rawSince))
				})
				require.NoError(t, err)
				require.NotEmpty(t, received)
			})
		})
	}

	// A websocket feed's options come entirely from its first message - the query string's since is
	// discarded - so the logged value must be the message's, not the query string's.  Uses since values
	// absent from the cases above so none of them can bleed into this capture window.
	t.Run("websocket uses the message since, not the query string", func(t *testing.T) {
		var received []byte
		var err error
		base.AssertLogContains(t, wantREST("4 (raw: 7::4)"), func() {
			received, err = sendWebSocketChanges(wsURL+"&since=41", wsOptions("7::4"))
		})
		require.NoError(t, err)
		require.NotEmpty(t, received)
	})

	// BLIP runs in its own block rather than wrapping the cases above: btcRunner.Run executes its closure
	// once per subprotocol, and the REST logging path has no subprotocol dependency, so nesting would
	// double every REST case for no coverage.  It's also structurally required - Run fails if already
	// inside one, and NewBlipTesterClientOptsWithRT fails if not inside one.
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		// A separate RestTester from the REST phase above, which also keeps NumPullReplTotalOneShot free
		// of REST traffic for the barrier below.
		blipRT := NewRestTester(t, &RestTesterConfig{
			SyncFn:       `function(doc) {channel(doc.channels)}`,
			GuestEnabled: true, // for the blip client
		})
		defer blipRT.Close()
		for i := range numDocs {
			blipRT.PutDoc(fmt.Sprintf("doc%d", i), `{"channels":["alpha"]}`)
		}
		blipRT.WaitForPendingChanges()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(blipRT, nil)
		defer btc.Close()

		pullStats := blipRT.GetDatabase().DbStats.CBLReplicationPull()
		baseTotal := pullStats.NumPullReplTotalOneShot.Value()

		for i, tc := range cases {
			t.Run("since="+tc.rawSince, func(t *testing.T) {
				// StartPullSince blocks on the subChanges response, which go-blip sends only once
				// handleSubChanges has returned - so the log line is already emitted.  Deliberately no
				// WaitForDoc: cases like 999 deliver nothing, and it would spin for its full timeout.
				base.AssertLogContains(t, wantBLIP(tc.wantSince), func() {
					btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Since: tc.rawSince})
				})
				// Only one subChanges may be outstanding per collection (activeSubChanges in
				// db/blip_handler.go), and the flag is cleared asynchronously by the changes goroutine.
				// Wait on the total first: the active count is also 0 before that goroutine starts, so
				// waiting on it alone can return immediately while the flag is still set.  Once the total
				// has advanced, the active count reaching 0 does imply the flag is clear - its defer is
				// registered before the flag-clearing one, so it unwinds after it.
				base.RequireWaitForStat(t, pullStats.NumPullReplTotalOneShot.Value, baseTotal+int64(i)+1)
				base.RequireWaitForStat(t, pullStats.NumPullReplActiveOneShot.Value, 0)
			})
		}
	})
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
	rt.WaitForPendingChanges()

	// Create 100 docs
	for i := range 100 {
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
	changesResults = rt.WaitForChanges(101, "/{{.keyspace}}/_changes", "user2", false)
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
		require.Contains(t, maps.Keys(body), db.BodyId)
		require.Contains(t, maps.Keys(body), db.BodyRev)

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
						Options: map[string]any{db.EventOptionDocumentChangedWinningRevOnly: true},
					},
				},
			},
		},
		}}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	rt.GetDatabase().EnableAllowConflicts(rt.TB())

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

// TestJumpInSequencesAtAllocatorSkippedSequenceFill:
//   - High level test
//   - Add a doc through Sync Gateway
//   - Alter that allocated sequence to be higher value. Mocking this document arriving from different env
//     (e.g. via XDCR)
//   - Wait for this sequence to arrive over cache feed and wait for skipped sequences to subsequently fill
//   - Update this doc again, triggering unused sequence range release
//   - Write another doc and assert that the changes feed returns all expected docs
func TestJumpInSequencesAtAllocatorSkippedSequenceFill(t *testing.T) {

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

	var retrievedXattr map[string]any
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
	changes.RequireRevID(t, []string{docVrs.RevTreeID, doc1Vrs.RevTreeID})
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

	var retrievedXattr map[string]any
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
	changes.RequireRevID(t, []string{docVrs.RevTreeID, doc1Vrs.RevTreeID})
}

func TestCVPopulationOnChangesViaAPI(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channels)}`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	bucketUUID := rt.GetDatabase().EncodedSourceID
	const DocID = "doc1"

	// activate channel cache
	_ = rt.WaitForChanges(0, "/{{.keyspace}}/_changes", "", true)

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+DocID, `{"channels": ["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	rt.WaitForPendingChanges()

	changes := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?version_type=cv", "", true)

	fetchedDoc, _, err := collection.GetDocWithXattrs(ctx, DocID, db.DocUnmarshalSync)
	require.NoError(t, err)

	entryCV := db.GetChangeEntryCV(t, &changes.Results[0])
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, bucketUUID, entryCV.SourceID)
	assert.Equal(t, fetchedDoc.HLV.Version, entryCV.Value)
}

func TestCVPopulationOnDocIDChanges(t *testing.T) {
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channels)}`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	collection, ctx := rt.GetSingleTestDatabaseCollection()
	bucketUUID := rt.GetDatabase().EncodedSourceID
	const DocID = "doc1"

	// activate channel cache
	_ = rt.WaitForChanges(0, "/{{.keyspace}}/_changes", "", true)

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+DocID, `{"channels": ["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	rt.WaitForPendingChanges()

	changes := rt.WaitForChanges(1, fmt.Sprintf(`/{{.keyspace}}/_changes?version_type=cv&filter=_doc_ids&doc_ids=%s`, DocID), "", true)

	fetchedDoc, _, err := collection.GetDocWithXattrs(ctx, DocID, db.DocUnmarshalSync)
	require.NoError(t, err)

	entryCV := db.GetChangeEntryCV(t, &changes.Results[0])
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, bucketUUID, entryCV.SourceID)
	assert.Equal(t, fetchedDoc.HLV.Version, entryCV.Value)
}

// TestChangesVersionType tests the /_changes REST endpoint with different version_type parameters for each possible underlying feed type and HTTP method.
func TestChangesVersionType(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	doc1 := "doc1"
	doc1Body := `{"foo":"bar"}`
	rt.PutDoc(doc1, doc1Body)
	doc2 := "doc2"
	doc2Body := `{"buzz":"quux"}`
	rt.PutDoc(doc2, doc2Body)

	rt.WaitForPendingChanges()

	tests := []struct {
		name                      string
		changesRequestMethod      string
		changesRequestQueryParams string
		changesRequestBody        string
		expectedStatus            int
		expectedVersionType       db.ChangesVersionType
		expectedDocs              int
	}{
		{
			name:                      "invalid version_type",
			changesRequestMethod:      http.MethodGet,
			changesRequestQueryParams: "?version_type=invalid",
			expectedStatus:            http.StatusBadRequest,
		},
		{
			name:                      "empty version_type",
			changesRequestMethod:      http.MethodGet,
			changesRequestQueryParams: "",
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeRevTreeID,
			expectedDocs:              2,
		},
		{
			name:                      "rev version_type",
			changesRequestMethod:      http.MethodGet,
			changesRequestQueryParams: "?version_type=rev&include_docs=true",
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeRevTreeID,
			expectedDocs:              2,
		},
		{
			name:                      "cv version_type",
			changesRequestMethod:      http.MethodGet,
			changesRequestQueryParams: "?version_type=cv&include_docs=true",
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeCV,
			expectedDocs:              2,
		},
		{
			name:                      "rev docid filter",
			changesRequestMethod:      http.MethodGet,
			changesRequestQueryParams: "?version_type=rev&filter=_doc_ids&doc_ids=doc1&include_docs=true",
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeRevTreeID,
			expectedDocs:              1,
		},
		{
			name:                      "cv docid filter",
			changesRequestMethod:      http.MethodGet,
			changesRequestQueryParams: "?version_type=cv&filter=_doc_ids&doc_ids=doc1&include_docs=true",
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeCV,
			expectedDocs:              1,
		},
		{
			name:                      "rev post",
			changesRequestMethod:      http.MethodPost,
			changesRequestQueryParams: "",
			changesRequestBody:        `{"version_type":"rev"}`,
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeRevTreeID,
			expectedDocs:              2,
		},
		{
			name:                      "cv post",
			changesRequestMethod:      http.MethodPost,
			changesRequestQueryParams: "",
			changesRequestBody:        `{"version_type":"cv"}`,
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeCV,
			expectedDocs:              2,
		},
		{
			name:                      "cv docid filter post",
			changesRequestMethod:      http.MethodPost,
			changesRequestQueryParams: "",
			changesRequestBody:        `{"version_type":"cv", "filter":"_doc_ids", "doc_ids":["doc1"]}`,
			expectedStatus:            http.StatusOK,
			expectedVersionType:       db.ChangesVersionTypeCV,
			expectedDocs:              1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require.NotEmptyf(t, test.changesRequestMethod, "Test case %q requires a changesRequestMethod to be set", test.name)

			if test.expectedStatus != http.StatusOK {
				resp := rt.SendAdminRequest(test.changesRequestMethod, fmt.Sprintf("/{{.keyspace}}/_changes%s", test.changesRequestQueryParams), test.changesRequestBody)
				RequireStatus(t, resp, test.expectedStatus)
				return
			}

			resp := rt.SendAdminRequest(test.changesRequestMethod, fmt.Sprintf("/{{.keyspace}}/_changes%s", test.changesRequestQueryParams), test.changesRequestBody)
			RequireStatus(t, resp, test.expectedStatus)
			var changesResults ChangesResults
			require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &changesResults))
			require.Len(t, changesResults.Results, test.expectedDocs)
			for _, changeEntry := range changesResults.Results {
				for _, change := range changeEntry.Changes {
					require.Len(t, change, 1) // ensure only one version type is present
					// and that it was the expected one (and we have a value)
					versionValue, ok := change[test.expectedVersionType]
					require.Truef(t, ok, "Expected version type %s, got %v", test.expectedVersionType, change)
					require.NotEmpty(t, versionValue)
				}
				if strings.Contains(test.changesRequestQueryParams, "include_docs=true") {
					var expectedBody string
					switch changeEntry.ID {
					case doc1:
						expectedBody = doc1Body
					case doc2:
						expectedBody = doc2Body
					}
					require.Contains(t, string(changeEntry.Doc), expectedBody[1:len(expectedBody)-1]) // strip {}s from doc body - 1.x API stamps additional properties so accommodate
				}
			}
		})
	}
}

func TestDocIDChangesVersionCVWithLegacyRev(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	docID1 := "doc1"
	docID2 := "doc2"
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

	// create doc with legacy revID
	_, _ = collection.CreateDocNoHLV(t, ctx, docID1, db.Body{"foo": "bar"})
	// create doc normally, this will have a CV allocated
	rt.PutDoc(docID2, `{"bar":"foo"}`)
	rt.WaitForPendingChanges()

	// issue docID changes feed
	resp := rt.SendAdminRequest(http.MethodGet, `/{{.keyspace}}/_changes?version_type=cv&filter=_doc_ids&doc_ids=["doc1","doc2"]&include_docs=true`, "")
	RequireStatus(t, resp, http.StatusOK)

	var changesResults ChangesResults
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &changesResults))
	require.Len(t, changesResults.Results, 2)
	for _, changeEntry := range changesResults.Results {
		require.Len(t, changeEntry.Changes, 1) // ensure only one version type is present
		for _, change := range changeEntry.Changes {
			if changeEntry.ID == docID1 {
				// doc1 was created with a legacy revID, so should have a revID version type
				_, ok := change[db.ChangesVersionTypeRevTreeID]
				assert.Truef(t, ok, "Expected version type %s, got %v", db.ChangesVersionTypeRevTreeID, change)
			} else {
				// doc2 was created normally so should have a CV version type
				_, ok := change[db.ChangesVersionTypeCV]
				assert.Truef(t, ok, "Expected version type %s, got %v", db.ChangesVersionTypeCV, change)
			}
		}
	}
}

func TestChangesFeedCVWithOldRevOnlyData(t *testing.T) {
	ctx := base.TestCtx(t)
	rt := NewRestTester(t, nil)
	defer rt.Close()

	seq, err := db.AllocateTestSequence(ctx, rt.GetDatabase())
	require.NoError(t, err)
	oldDoc := "oldDoc"
	oldDocBody := `{"body_field":"1234"}`
	oldDocSyncData := fmt.Appendf(nil, `{"sequence":%d,"rev":{"rev": "1-abc"},"history":{"revs":["1-abc"],"parents":[-1],"channels":[null]},"value_crc32c":"%s"}`, seq, base.Crc32cHashString([]byte(oldDocBody)))
	_, err = rt.GetSingleDataStore().WriteWithXattrs(t.Context(), oldDoc, 0, 0, []byte(oldDocBody), map[string][]byte{base.SyncXattrName: oldDocSyncData}, nil, nil)
	require.NoError(t, err)

	newDoc := "newDoc"
	newDocBody := `{"foo":"bar"}`
	rt.PutDoc(newDoc, newDocBody)

	rt.WaitForPendingChanges()

	resp := rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_changes?version_type=cv&include_docs=true", "")
	RequireStatus(t, resp, http.StatusOK)
	var changesResults ChangesResults
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &changesResults))
	require.Len(t, changesResults.Results, 2)
	for i, changeEntry := range changesResults.Results {
		for _, change := range changeEntry.Changes {
			require.Len(t, change, 1) // ensure only one version type is present
			// and that it was the expected one (and we have a value)
			var expectedType db.ChangesVersionType
			if i == 0 {
				// first doc was written with a RevID and no CV available
				expectedType = db.ChangesVersionTypeRevTreeID
			} else {
				expectedType = db.ChangesVersionTypeCV
			}
			versionValue, ok := change[expectedType]
			require.Truef(t, ok, "Expected version type %s, got %v", expectedType, change)
			require.NotEmpty(t, versionValue)
		}
		var expectedBody string
		switch changeEntry.ID {
		case oldDoc:
			expectedBody = oldDocBody
		case newDoc:
			expectedBody = newDocBody
		}
		require.Contains(t, string(changeEntry.Doc), expectedBody[1:len(expectedBody)-1]) // strip {}s from doc body - 1.x API stamps additional properties so accommodate
	}
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

	changes := feed.RequireEnded("continuous changes feed still running after user delete")
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

	base.RequireChanClosed(t, feedDone, "longpoll changes feed still running after user delete")

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
	db.WaitForUserWaiterChange(t, userWaiter)

	// The feed sends in sequence order, so a feed still serving the revoked channel would send
	// afterPurgeRevoked first.
	rt.PutDoc("afterPurgeRevoked", `{"channels":["`+roleChannel+`"]}`)
	rt.PutDoc("afterPurgeAllowed", `{"channels":["`+userChannel+`"]}`)

	feedChanges := feed.RequireEnded("continuous changes feed did not reach its limit")
	require.Len(t, feedChanges, 1)
	require.Equal(t, "afterPurgeAllowed", feedChanges[0].ID)
}
