/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"encoding/json"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLegacyProposeChanges:
//   - Build propose changes request of docs that are all new to SGW in legacy format
//   - Assert that the response is as expected (empty response)
func TestLegacyProposeChanges(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
		blipProtocols:   []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	proposeChangesRequest := bt.newRequest()
	proposeChangesRequest.SetProfile("proposeChanges")
	proposeChangesRequest.SetCompressed(true)

	// According to proposeChanges spec:
	// proposedChanges entries are of the form: [docID, revID, serverRevID]
	// where serverRevID is optional
	changesBody := `
[["foo", "1-abc"],
["foo2", "1-abc"]]
`
	proposeChangesRequest.SetBody([]byte(changesBody))
	sent := bt.sender.Send(proposeChangesRequest)
	assert.True(t, sent)
	proposeChangesResponse := proposeChangesRequest.Response()
	body, err := proposeChangesResponse.Body()
	assert.NoError(t, err, "Error getting changes response body")

	var changeList [][]interface{}
	err = base.JSONUnmarshal(body, &changeList)
	assert.NoError(t, err, "Error getting changes response body")

	assert.Len(t, changeList, 0)
}

// TestProposeChangesHandlingWithExistingRevs:
//   - Build up propose changes request for conflicting and non conflicting docs with legacy revs
//   - Assert that the response sent from SGW is as expected
func TestProposeChangesHandlingWithExistingRevs(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
		blipProtocols:   []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()
	rt := bt.restTester

	resp := rt.PutDoc("conflictingInsert", `{"version":1}`)
	conflictingInsertRev := resp.RevTreeID

	resp = rt.PutDoc("conflictingUpdate", `{"version":1}`)
	conflictingUpdateRev1 := resp.RevTreeID
	conflictingUpdateRev2 := rt.UpdateDocRev("conflictingUpdate", resp.RevTreeID, `{"version":2}`)

	resp = rt.PutDoc("newUpdate", `{"version":1}`)
	newUpdateRev1 := resp.RevTreeID

	type proposeChangesCase struct {
		key           string
		revID         string
		parentRevID   string
		expectedValue interface{}
	}

	proposeChangesCases := []proposeChangesCase{
		proposeChangesCase{
			key:           "conflictingInsert",
			revID:         "1-abc",
			parentRevID:   "",
			expectedValue: map[string]interface{}{"status": float64(db.ProposedRev_Conflict), "rev": conflictingInsertRev},
		},
		proposeChangesCase{
			key:           "newInsert",
			revID:         "1-abc",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_OK),
		},
		proposeChangesCase{
			key:           "conflictingUpdate",
			revID:         "2-abc",
			parentRevID:   conflictingUpdateRev1,
			expectedValue: map[string]interface{}{"status": float64(db.ProposedRev_Conflict), "rev": conflictingUpdateRev2},
		},
		proposeChangesCase{
			key:           "newUpdate",
			revID:         "2-abc",
			parentRevID:   newUpdateRev1,
			expectedValue: float64(db.ProposedRev_OK),
		},
	}

	proposeChangesRequest := bt.newRequest()
	proposeChangesRequest.SetProfile("proposeChanges")
	proposeChangesRequest.SetCompressed(true)
	proposeChangesRequest.Properties[db.ProposeChangesConflictsIncludeRev] = "true"

	proposedChanges := make([][]interface{}, 0)
	for _, c := range proposeChangesCases {
		changeEntry := []interface{}{
			c.key,
			c.revID,
		}
		if c.parentRevID != "" {
			changeEntry = append(changeEntry, c.parentRevID)
		}
		proposedChanges = append(proposedChanges, changeEntry)
	}
	proposeChangesBody, marshalErr := json.Marshal(proposedChanges)
	require.NoError(t, marshalErr)

	proposeChangesRequest.SetBody(proposeChangesBody)
	sent := bt.sender.Send(proposeChangesRequest)
	assert.True(t, sent)
	proposeChangesResponse := proposeChangesRequest.Response()
	bodyReader, err := proposeChangesResponse.BodyReader()
	require.NoError(t, err)

	var changeList []interface{}
	decoder := base.JSONDecoder(bodyReader)
	decodeErr := decoder.Decode(&changeList)
	require.NoError(t, decodeErr)

	for i, entry := range changeList {
		assert.Equal(t, proposeChangesCases[i].expectedValue, entry)
	}
}

// TestProcessLegacyRev:
//   - Create doc on SGW
//   - Push new revision of this doc form client in legacy rev mode
//   - Assert that the new doc is created and given a new source version pair
//   - Send a new rev that SGW hasn;t yet seen unsolicited and assert that the doc is added correctly and given a source version pair
func TestProcessLegacyRev(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
		blipProtocols:   []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()
	rt := bt.restTester
	collection, _ := rt.GetSingleTestDatabaseCollection()

	// simulate a doc being updated on CBL pre upgrade ad that new change being pushed to SGW in legacy mode

	// add doc to SGW
	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	// Send another rev of same doc
	history := []string{rev1ID}
	sent, _, _, err := bt.SendRevWithHistory("doc1", "2-bcd", history, []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	assert.NoError(t, err)
	require.NoError(t, rt.WaitForVersion("doc1", DocVersion{RevTreeID: "2-bcd"}))

	// assert we can fetch this doc rev
	resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1?rev=2-bcd", "")
	RequireStatus(t, resp, 200)

	// assert this legacy doc has been given source version pair
	docSource, docVrs := collection.GetDocumentCurrentVersion(t, "doc1")
	assert.Equal(t, docVersion.CV.SourceID, docSource)
	assert.NotEqual(t, docVersion.CV.Value, docVrs)

	// try new rev to process
	_, _, _, err = bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)
	assert.NoError(t, err)

	require.NoError(t, rt.WaitForVersion("foo", DocVersion{RevTreeID: "1-abc"}))
	// assert we can fetch this doc rev
	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/foo?rev=1-abc", "")
	RequireStatus(t, resp, 200)

	// assert this legacy doc has been given source version pair
	docSource, docVrs = collection.GetDocumentCurrentVersion(t, "doc1")
	assert.NotEqual(t, "", docSource)
	assert.NotEqual(t, uint64(0), docVrs)
}

// TestChangesResponseLegacyRev:
//   - Create doc
//   - Update doc through SGW, creating a new revision
//   - Send subChanges request and have custom changes handler to force a revID change being constructed
//   - Have custom rev handler to assert the subsequent rev message is as expected with cv as rev + full rev
//     tree in history. No hlv in history is expected here.
func TestChangesResponseLegacyRev(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
		blipProtocols:   []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()
	rt := bt.restTester

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	docVersion2 := rt.UpdateDocDirectly("doc1", docVersion, db.Body{"test": "update"})

	receivedChangesRequestWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {
		defer revsFinishedWg.Done()
		log.Printf("received rev request")

		// assert the rev property contains cv
		rev := request.Properties["rev"]
		assert.Equal(t, docVersion2.CV.String(), rev)

		// assert that history contain current revID and previous revID
		history := request.Properties["history"]
		historyList := strings.Split(history, ",")
		assert.Len(t, historyList, 2)
		assert.Equal(t, docVersion2.RevTreeID, historyList[0])
		assert.Equal(t, docVersion.RevTreeID, historyList[1])
	}

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		knownRevs := []interface{}{}

		if string(body) != "null" {
			var changesReqs [][]interface{}
			err = base.JSONUnmarshal(body, &changesReqs)
			require.NoError(t, err)

			knownRevs = make([]interface{}, len(changesReqs))

			for i, changesReq := range changesReqs {
				docID := changesReq[1].(string)
				revID := changesReq[2].(string)
				log.Printf("change: %s %s", docID, revID)

				// fill known rev with revision 1 of doc1, this will replicate a situation where client has legacy rev of
				// a document that SGW had a newer version of
				knownRevs[i] = []string{rev1ID}
			}
		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseValBytes, err := base.JSONMarshal(knownRevs)
			require.NoError(t, err)
			response.SetBody(emptyResponseValBytes)
		}
		receivedChangesRequestWg.Done()
	}

	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	// changes will be called again with empty changes so hence the wait group of 2
	receivedChangesRequestWg.Add(2)

	// expect 1 rev message
	revsFinishedWg.Add(1)

	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	timeoutErr := WaitWithTimeout(&receivedChangesRequestWg, time.Second*5)
	require.NoError(t, timeoutErr, "Timed out waiting")

	timeoutErr = WaitWithTimeout(&revsFinishedWg, time.Second*5)
	require.NoError(t, timeoutErr, "Timed out waiting")

}

// TestChangesResponseWithHLVInHistory:
//   - Create doc
//   - Update doc with hlv agent to mock update from a another peer
//   - Send subChanges request and have custom changes handler to force a revID change being constructed
//   - Have custom rev handler to asser the subsequent rev message is as expected with cv as rev and pv + full rev
//     tree in history
func TestChangesResponseWithHLVInHistory(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
		blipProtocols:   []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()
	rt := bt.restTester
	collection, ctx := rt.GetSingleTestDatabaseCollection()

	docVersion := rt.PutDocDirectly("doc1", db.Body{"test": "doc"})
	rev1ID := docVersion.RevTreeID

	newDoc, _, err := collection.GetDocWithXattrs(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)

	agent := db.NewHLVAgent(t, rt.GetSingleDataStore(), "newSource", base.VvXattrName)
	_ = agent.UpdateWithHLV(ctx, "doc1", newDoc.Cas, newDoc.HLV)

	newDoc, err = collection.GetDocument(ctx, "doc1", db.DocUnmarshalAll)
	require.NoError(t, err)

	receivedChangesRequestWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {
		defer revsFinishedWg.Done()
		log.Printf("received rev request")

		// assert the rev property contains cv
		rev := request.Properties["rev"]
		assert.Equal(t, newDoc.HLV.GetCurrentVersionString(), rev)

		// assert that history contain current revID and previous revID + pv of HLV
		history := request.Properties["history"]
		historyList := strings.Split(history, ",")
		assert.Len(t, historyList, 3)
		assert.Equal(t, newDoc.CurrentRev, historyList[1])
		assert.Equal(t, docVersion.RevTreeID, historyList[2])
		assert.Equal(t, docVersion.CV.String(), historyList[0])
	}

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		knownRevs := []interface{}{}

		if string(body) != "null" {
			var changesReqs [][]interface{}
			err = base.JSONUnmarshal(body, &changesReqs)
			require.NoError(t, err)

			knownRevs = make([]interface{}, len(changesReqs))

			for i, changesReq := range changesReqs {
				docID := changesReq[1].(string)
				revID := changesReq[2].(string)
				log.Printf("change: %s %s", docID, revID)

				// fill known rev with revision 1 of doc1, this will replicate a situation where client has legacy rev of
				// a document that SGW had a newer version of
				knownRevs[i] = []string{rev1ID}
			}
		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseValBytes, err := base.JSONMarshal(knownRevs)
			require.NoError(t, err)
			response.SetBody(emptyResponseValBytes)
		}
		receivedChangesRequestWg.Done()
	}

	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	// changes will be called again with empty changes so hence the wait group of 2
	receivedChangesRequestWg.Add(2)

	// expect 1 rev message
	revsFinishedWg.Add(1)

	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	timeoutErr := WaitWithTimeout(&receivedChangesRequestWg, time.Second*5)
	require.NoError(t, timeoutErr, "Timed out waiting")

	timeoutErr = WaitWithTimeout(&revsFinishedWg, time.Second*5)
	require.NoError(t, timeoutErr, "Timed out waiting")
}
