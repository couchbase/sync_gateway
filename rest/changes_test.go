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
	"strings"
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

	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	guest, err := a.GetUser("")
	assert.NoError(t, err)
	guest.SetDisabled(false)
	assert.NoError(t, a.Save(guest))

	rt.CreateUser("user1", []string{"alpha"})

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

	fetchedDoc, _, err := collection.GetDocWithXattrs(ctx, DocID, db.DocUnmarshalCAS)
	require.NoError(t, err)

	entryCV := db.GetChangeEntryCV(t, &changes.Results[0])
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, bucketUUID, entryCV.SourceID)
	assert.Equal(t, fetchedDoc.Cas, entryCV.Value)
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

	fetchedDoc, _, err := collection.GetDocWithXattrs(ctx, DocID, db.DocUnmarshalCAS)
	require.NoError(t, err)

	entryCV := db.GetChangeEntryCV(t, &changes.Results[0])
	assert.Equal(t, "doc1", changes.Results[0].ID)
	assert.Equal(t, bucketUUID, entryCV.SourceID)
	assert.Equal(t, fetchedDoc.Cas, entryCV.Value)
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
	resp := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf(`/{{.keyspace}}/_changes?version_type=cv&filter=_doc_ids&doc_ids=["doc1","doc2"]&include_docs=true`), "")
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
	rt := NewRestTester(t, nil)
	defer rt.Close()

	seq, err := db.AllocateTestSequence(rt.GetDatabase())
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
