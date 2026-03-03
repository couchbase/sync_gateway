/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"text/template"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/gocb/v2"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test performs the following steps against the Sync Gateway passive blip replicator:
//
// - Setup
//   - Create an httptest server listening on a port that wraps the Sync Gateway Admin Handler
//   - Make a BLIP/Websocket client connection to Sync Gateway
//
// - Test
//   - Verify Sync Gateway will accept the doc revision that is about to be sent
//   - Send the doc revision in a rev request
//   - Call changes endpoint and verify that it knows about the revision just sent
//   - Call subChanges api and make sure we get expected changes back
//
// Replication Spec: https://github.com/couchbase/couchbase-lite-core/wiki/Replication-Protocol#proposechanges
func TestBlipPushRevisionInspectChanges(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{GuestEnabled: true})
	defer bt.Close()
	bt.restTester.GetDatabase().EnableAllowConflicts(bt.TB())

	// Verify Sync Gateway will accept the doc revision that is about to be sent
	var changeList [][]any
	changesRequest := bt.newRequest()
	changesRequest.SetProfile("changes")
	changesRequest.SetBody([]byte(`[["1", "foo", "1-abc", false]]`)) // [sequence, docID, revID]
	sent := bt.sender.Send(changesRequest)
	assert.True(t, sent)
	changesResponse := changesRequest.Response()
	assert.Equal(t, changesRequest.SerialNumber(), changesResponse.SerialNumber())
	body, err := changesResponse.Body()
	assert.NoError(t, err, "Error reading changes response body")
	err = base.JSONUnmarshal(body, &changeList)
	require.NoError(t, err, "Error unmarshalling response body: %s", body)
	require.Len(t, changeList, 1) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow := changeList[0]
	assert.Len(t, changeRow, 0) // Should be empty, meaning the server is saying it doesn't have the revision yet

	// Send the doc revision in a rev request
	revResponse := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)

	_, err = revResponse.Body()
	assert.NoError(t, err, "Error unmarshalling response body")

	// Call changes with a hypothetical new revision, assert that it returns last pushed revision
	var changeList2 [][]any
	changesRequest2 := bt.newRequest()
	changesRequest2.SetProfile("changes")
	changesRequest2.SetBody([]byte(`[["2", "foo", "2-xyz", false]]`)) // [sequence, docID, revID]
	sent2 := bt.sender.Send(changesRequest2)
	assert.True(t, sent2)
	changesResponse2 := changesRequest2.Response()
	assert.Equal(t, changesRequest2.SerialNumber(), changesResponse2.SerialNumber())
	body2, err := changesResponse2.Body()
	assert.NoError(t, err, "Error reading changes response body")
	err = base.JSONUnmarshal(body2, &changeList2)
	assert.NoError(t, err, "Error unmarshalling response body")
	assert.Len(t, changeList2, 1) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow2 := changeList2[0]
	assert.Len(t, changeRow2, 1) // Should have 1 item in row, which is the rev id of the previous revision pushed
	assert.Equal(t, "1-abc", changeRow2[0])

	// Call subChanges api and make sure we get expected changes back
	receivedChangesRequestWg := sync.WaitGroup{}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		if string(body) != "null" {

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]any{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")
			assert.Len(t, changeListReceived, 1)
			change := changeListReceived[0] // [1,"foo","1-abc"]
			assert.Len(t, change, 3)
			assert.Equal(t, float64(1), change[0].(float64)) // Expect sequence to be 1, since first item in DB
			assert.Equal(t, "foo", change[1])                // Doc id of pushed rev
			assert.Equal(t, "1-abc", change[2])              // Rev id of pushed rev

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []any{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

		receivedChangesRequestWg.Done()

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	sent = bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	// Also expect the "changes" profile handler above to be called back again with an empty request that
	// will be ignored since body will be "null" hence the incrementing for the wait group by 2
	receivedChangesRequestWg.Add(2)
	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	// Wait until we got the expected callback on the "changes" profile handler
	WaitWithTimeout(t, &receivedChangesRequestWg, time.Second*5)
}

// Start subChanges w/ continuous=true, batchsize=10
// Make several updates
// Wait until we get the expected updates
func TestContinuousChangesSubscription(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)

	bt := NewBlipTester(t)
	defer func() {
		unsubChangesRequest := bt.newRequest()
		blip.NewRequest()
		unsubChangesRequest.SetProfile(db.MessageUnsubChanges)
		assert.True(t, bt.sender.Send(unsubChangesRequest))
		unsubChangesResponse := unsubChangesRequest.Response()
		assert.Equal(t, "", unsubChangesResponse.Properties[db.BlipErrorCode])
		bt.Close()
	}()
	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receivedChangesWg := sync.WaitGroup{}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	lastReceivedSeq := float64(0)
	var numbatchesReceived int32
	nonIntegerSequenceReceived := false
	changeCount := 0
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		require.NoError(t, err)
		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]any{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Len(t, change, 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					assert.True(t, receivedSeq > lastReceivedSeq)
					lastReceivedSeq = receivedSeq
				} else {
					nonIntegerSequenceReceived = true
					log.Printf("Unexpected non-integer sequence received: %v", change[0])
				}

				// Verify doc id and rev id have expected vals
				docID := change[1].(string)
				assert.True(t, strings.HasPrefix(docID, "foo"))
				assert.Equal(t, "1-abc", change[2]) // Rev id of pushed rev
				changeCount++
				receivedChangesWg.Done()
			}

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []any{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	for i := 1; i < 1500; i++ {
		// // Add a change: Send an unsolicited doc revision in a rev request
		receivedChangesWg.Add(1)
		bt.SendRev(
			fmt.Sprintf("foo-%d", i),
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
	}

	// Wait until all expected changes are received by change handler
	WaitWithTimeout(t, &receivedChangesWg, time.Second*30)

	// Since batch size was set to 10, and 15 docs were added, expect at _least_ 2 batches
	numBatchesReceivedSnapshot := atomic.LoadInt32(&numbatchesReceived)
	assert.True(t, numBatchesReceivedSnapshot >= 2)

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")
}

// Make several updates
// Start subChanges w/ continuous=false, batchsize=20
// Validate we get the expected updates and changes ends
func TestBlipOneShotChangesSubscription(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTester(t)
	defer bt.Close()

	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receivedChangesWg := sync.WaitGroup{}
	receivedCaughtUpChange := false

	// Build set of docids
	docIdsReceived := make(map[string]bool)
	for i := 1; i < 105; i++ {
		docIdsReceived[fmt.Sprintf("preOneShot-%d", i)] = false
	}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	lastReceivedSeq := float64(0)
	var numbatchesReceived int32
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		require.NoError(t, err)

		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]any{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Len(t, change, 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				require.True(t, ok, "Expected sequence to be float64, was %T", change[0])
				assert.True(t, receivedSeq > lastReceivedSeq)
				lastReceivedSeq = receivedSeq

				// Verify doc id and rev id have expected vals
				docID := change[1].(string)
				assert.True(t, strings.HasPrefix(docID, "preOneShot"))
				assert.Equal(t, "1-abc", change[2]) // Rev id of pushed rev
				docIdsReceived[docID] = true
				receivedChangesWg.Done()
			}

		} else {
			receivedCaughtUpChange = true
			receivedChangesWg.Done()

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []any{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Increment waitgroup to account for the expected 'caught up' nil changes entry.
	receivedChangesWg.Add(1)

	cacheWaiter := bt.DatabaseContext().NewDCPCachingCountWaiter(t)
	cacheWaiter.Add(len(docIdsReceived))
	// Add documents
	for docID := range docIdsReceived {
		// // Add a change: Send an unsolicited doc revision in a rev request
		bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		receivedChangesWg.Add(1)
	}

	// Wait for documents to be processed and available for changes
	cacheWaiter.Wait()

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	// Wait until all expected changes are received by change handler
	WaitWithTimeout(t, &receivedChangesWg, time.Second*60)

	// Since batch size was set to 10, and 15 docs were added, expect at _least_ 2 batches
	numBatchesReceivedSnapshot := atomic.LoadInt32(&numbatchesReceived)
	assert.True(t, numBatchesReceivedSnapshot >= 2)

	// Validate all expected documents were received.
	for docID, received := range docIdsReceived {
		if !received {
			t.Errorf("Did not receive expected doc %s in changes", docID)
		}
	}

	// Validate that the 'caught up' message was sent
	assert.True(t, receivedCaughtUpChange)

	require.Equal(t, int64(1), bt.restTester.GetDatabase().DbStats.CBLReplicationPullStats.NumPullReplTotalOneShot.Value())
	base.RequireWaitForStat(t, bt.restTester.GetDatabase().DbStats.CBLReplicationPullStats.NumPullReplActiveOneShot.Value, 0)
}

// Test subChanges w/ docID filter
func TestBlipSubChangesDocIDFilter(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTester(t)
	defer bt.Close()
	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receivedChangesWg := sync.WaitGroup{}
	receivedCaughtUpChange := false

	// Build set of docids
	docIDsSent := make([]string, 0)
	docIDsExpected := make([]string, 0)
	docIDsReceived := make(map[string]bool)
	for i := 1; i <= 100; i++ {
		docID := fmt.Sprintf("docIDFiltered-%d", i)
		docIDsSent = append(docIDsSent, docID)
		// Filter to every 5th doc
		if i%5 == 0 {
			docIDsExpected = append(docIDsExpected, docID)
			docIDsReceived[docID] = false
		}
	}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	lastReceivedSeq := float64(0)
	var numbatchesReceived int32

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		require.NoError(t, err)

		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]any{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Len(t, change, 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				require.True(t, ok, "Expected sequence to be float64, was %T", change[0])
				assert.True(t, receivedSeq > lastReceivedSeq)
				lastReceivedSeq = receivedSeq

				// Verify doc id and rev id have expected vals
				docID := change[1].(string)
				assert.True(t, strings.HasPrefix(docID, "docIDFiltered"))
				assert.Equal(t, "1-abc", change[2]) // Rev id of pushed rev
				log.Printf("Changes got docID: %s", docID)

				// Ensure we only receive expected docs
				_, isExpected := docIDsReceived[docID]
				if !isExpected {
					t.Errorf("Received unexpected docId: %s", docID)
				} else {
					// Add to received set, to ensure we get all expected docs
					docIDsReceived[docID] = true
					receivedChangesWg.Done()
				}

			}

		} else {
			receivedCaughtUpChange = true
			receivedChangesWg.Done()

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []any{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Increment waitgroup to account for the expected 'caught up' nil changes entry.
	receivedChangesWg.Add(1)

	cacheWaiter := bt.DatabaseContext().NewDCPCachingCountWaiter(t)

	// Add documents
	for _, docID := range docIDsSent {
		// // Add a change: Send an unsolicited doc revision in a rev request
		bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
	}
	receivedChangesWg.Add(len(docIDsExpected))

	// Wait for documents to be processed and available for changes
	// 105 docs +
	cacheWaiter.AddAndWait(len(docIDsExpected))

	// TODO: Attempt a subChanges w/ continuous=true and docID filter

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 5 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)

	body := db.SubChangesBody{DocIDs: docIDsExpected}
	bodyBytes, err := base.JSONMarshal(body)
	assert.NoError(t, err, "Error marshalling subChanges body.")

	subChangesRequest.SetBody(bodyBytes)

	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	// Wait until all expected changes are received by change handler
	WaitWithTimeout(t, &receivedChangesWg, time.Second*15)

	// Since batch size was set to 10, and 15 docs were added, expect at _least_ 2 batches
	numBatchesReceivedSnapshot := atomic.LoadInt32(&numbatchesReceived)
	assert.True(t, numBatchesReceivedSnapshot >= 2)
	// Validate that the 'caught up' message was sent
	assert.True(t, receivedCaughtUpChange)

	// Validate all expected documents were received.
	for docID, received := range docIDsReceived {
		if !received {
			t.Errorf("Did not receive expected doc %s in changes", docID)
		}
	}

	// Validate that the 'caught up' message was sent
	assert.True(t, receivedCaughtUpChange)

}

// Push proposed changes and ensure that the server accepts them
//
// 1. Start sync gateway in no-conflicts mode
// 2. Send changes push request with multiple doc revisions
// 3. Make sure there are no panics
// 4. Make sure that the server responds to accept the changes (empty array)
func TestProposedChangesNoConflictsMode(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		GuestEnabled: true,
	})
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

	var changeList [][]any
	err = base.JSONUnmarshal(body, &changeList)
	assert.NoError(t, err, "Error getting changes response body")

	// The common case of an empty array response tells the sender to send all of the proposed revisions,
	// so the changeList returned by Sync Gateway is expected to be empty
	assert.Len(t, changeList, 0)

}

// Validate SG sends conflicting rev when requested
func TestProposedChangesIncludeConflictingRev(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		GuestEnabled: true,
	})
	defer bt.Close()

	// Write existing docs to server directly (not via blip)
	rt := bt.restTester
	resp := rt.PutDoc("conflictingInsert", `{"version":1}`)
	conflictingInsertRev := resp.RevTreeID

	resp = rt.PutDoc("matchingInsert", `{"version":1}`)
	matchingInsertRev := resp.RevTreeID

	resp = rt.PutDoc("conflictingUpdate", `{"version":1}`)
	conflictingUpdateRev1 := resp.RevTreeID
	conflictingUpdateRev2 := rt.UpdateDocRev("conflictingUpdate", resp.RevTreeID, `{"version":2}`)

	resp = rt.PutDoc("matchingUpdate", `{"version":1}`)
	matchingUpdateRev1 := resp.RevTreeID
	matchingUpdateRev2 := rt.UpdateDocRev("matchingUpdate", resp.RevTreeID, `{"version":2}`)

	resp = rt.PutDoc("newUpdate", `{"version":1}`)
	newUpdateRev1 := resp.RevTreeID

	type proposeChangesCase struct {
		key           string
		revID         string
		parentRevID   string
		expectedValue any
	}

	proposeChangesCases := []proposeChangesCase{
		proposeChangesCase{
			key:           "conflictingInsert",
			revID:         "1-abc",
			parentRevID:   "",
			expectedValue: map[string]any{"status": float64(db.ProposedRev_Conflict), "rev": conflictingInsertRev},
		},
		proposeChangesCase{
			key:           "newInsert",
			revID:         "1-abc",
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_OK),
		},
		proposeChangesCase{
			key:           "matchingInsert",
			revID:         matchingInsertRev,
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_Exists),
		},
		proposeChangesCase{
			key:           "conflictingUpdate",
			revID:         "2-abc",
			parentRevID:   conflictingUpdateRev1,
			expectedValue: map[string]any{"status": float64(db.ProposedRev_Conflict), "rev": conflictingUpdateRev2},
		},
		proposeChangesCase{
			key:           "newUpdate",
			revID:         "2-abc",
			parentRevID:   newUpdateRev1,
			expectedValue: float64(db.ProposedRev_OK),
		},
		proposeChangesCase{
			key:           "matchingUpdate",
			revID:         matchingUpdateRev2,
			parentRevID:   matchingUpdateRev1,
			expectedValue: float64(db.ProposedRev_Exists),
		},
	}

	proposeChangesRequest := bt.newRequest()
	proposeChangesRequest.SetProfile("proposeChanges")
	proposeChangesRequest.SetCompressed(true)
	proposeChangesRequest.Properties[db.ProposeChangesConflictsIncludeRev] = "true"

	// proposedChanges entries are of the form: [docID, revID, parentRevID], where parentRevID is optional
	proposedChanges := make([][]any, 0)
	for _, c := range proposeChangesCases {
		changeEntry := []any{
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
	assert.NoError(t, err, "Error getting changes response body reader")

	var changeList []any
	decoder := base.JSONDecoder(bodyReader)
	decodeErr := decoder.Decode(&changeList)
	require.NoError(t, decodeErr)

	for i, entry := range changeList {
		assert.Equal(t, proposeChangesCases[i].expectedValue, entry)
	}

}

// Connect to public port with authentication
func TestPublicPortAuthentication(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	})
	defer rt.Close()
	const (
		user1 = "user1"
		user2 = "user2"
	)
	rt.CreateUser(user1, []string{user1})
	rt.CreateUser(user2, []string{"*"})
	btUser1 := NewBlipTesterFromSpecWithRT(rt,
		&BlipTesterSpec{
			connectingUsername: user1,
		})
	defer btUser1.Close()

	// Send the user1 doc
	btUser1.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["user1"]}`),
		blip.Properties{},
	)

	btUser2 := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{
		connectingUsername: user2,
	})
	defer btUser2.Close()

	// Send the user2 doc, which is in a "random" channel, but it should be accessible due to * channel access
	btUser2.SendRev(
		"foo2",
		"1-abcd",
		[]byte(`{"key": "val", "channels": ["NBC"]}`),
		blip.Properties{},
	)

	// Assert that user1 received a single expected change
	changesChannelUser1 := btUser1.WaitForNumChanges(1)
	assert.Len(t, changesChannelUser1, 1)
	change := changesChannelUser1[0]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo", revId: "1-abc", sequence: "*", deleted: base.Ptr(false)})
	// Assert that user2 received user1's change as well as it's own change
	changesChannelUser2 := btUser2.WaitForNumChanges(2)
	assert.Len(t, changesChannelUser2, 2)
	change = changesChannelUser2[0]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo", revId: "1-abc", sequence: "*", deleted: base.Ptr(false)})

	change = changesChannelUser2[1]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo2", revId: "1-abcd", sequence: "*", deleted: base.Ptr(false)})

}

// Connect to public port with authentication, and validate user update during a replication
func TestPublicPortAuthenticationUserUpdate(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Initialize restTester here, so that we can use custom sync function, and later modify user
	syncFunction := `
function(doc, oldDoc) {
  requireAccess("ABC")
}

`
	rtConfig := RestTesterConfig{
		SyncFn: syncFunction,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	const username = "user1"
	rt.CreateUser(username, nil)

	// Create bliptester that is connected as user1, with no access to channel ABC
	bt := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{
		connectingUsername: "user1",
	})
	defer bt.Close()

	// Attempt to send a doc, should be rejected
	rq := bt.newRevMessage("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})
	bt.Send(rq)
	require.Equal(t, "403", rq.Response().Properties["Error-Code"], "Expected 403 error code on rev send when user has no channel access, %s", rq.Properties["Error-Code"])

	// Set up a ChangeWaiter for this test, to block until the user change notification happens
	dbc := rt.GetDatabase()
	ctx := rt.Context()
	user1, err := dbc.Authenticator(ctx).GetUser(username)
	require.NoError(t, err)

	userDb, err := db.GetDatabase(dbc, user1)
	require.NoError(t, err)

	userWaiter := userDb.NewUserWaiter()

	// Update the user to grant them access to ABC
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", GetUserPayload(t, "user1", "", "", rt.GetSingleDataStore(), []string{"ABC"}, nil))
	RequireStatus(t, response, 200)

	// Wait for notification
	db.WaitForUserWaiterChange(t, userWaiter)

	// Attempt to send the doc again, should succeed if the blip context also received notification
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)

	// Validate that the doc was written (GET request doesn't get a 404)
	getResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/foo", "")
	RequireStatus(t, getResponse, 200)

}

// Start subChanges w/ continuous=true, batchsize=20
// Write a doc that grants access to itself for the active replication's user
func TestContinuousChangesDynamicGrant(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)
	// Initialize restTester here, so that we can use custom sync function, and later modify user
	syncFunction := `
function(doc, oldDoc) {
  access(doc.accessUser, doc.accessChannel)
  channel(doc.channels)
}

`

	rtConfig := RestTesterConfig{SyncFn: syncFunction}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	const user1 = "user1"
	rt.CreateUser(user1, nil)
	// Create bliptester that is connected as user1, with no access to channel ABC
	bt := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{
		connectingUsername: user1,
	})
	defer bt.Close()

	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receivedChangesWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	lastReceivedSeq := float64(0)
	var numbatchesReceived int32
	nonIntegerSequenceReceived := false
	changeCount := 0
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		require.NoError(t, err)
		responseVal := [][]any{}
		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]any{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Len(t, change, 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					assert.True(t, receivedSeq > lastReceivedSeq)
					lastReceivedSeq = receivedSeq
				} else {
					nonIntegerSequenceReceived = true
					log.Printf("Unexpected non-integer sequence received: %v", change[0])
				}

				revID := change[2].(string)
				responseVal = append(responseVal, []any{revID})
				changeCount++
				receivedChangesWg.Done()
			}

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			responseValBytes, err := base.JSONMarshal(responseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(responseValBytes)
		}

	}

	// -------- Rev handler callback --------
	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {
		defer revsFinishedWg.Done()
		body, err := request.Body()
		require.NoError(t, err)

		var doc RestDocument
		err = base.JSONUnmarshal(body, &doc)
		if err != nil {
			panic(fmt.Sprintf("Unexpected err: %v", err))
		}
		log.Printf("got rev message: %+v", doc)
		_, isRemoved := doc[db.BodyRemoved]
		assert.False(t, isRemoved)

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	// Write a doc that grants user1 access to channel ABC, and doc is also in channel ABC
	receivedChangesWg.Add(1)
	revsFinishedWg.Add(1)
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/grantDoc", `{"accessUser":"user1", "accessChannel":"ABC", "channels":["ABC"]}`)
	RequireStatus(t, response, 201)
	rt.WaitForPendingChanges()

	// Wait until all expected changes are received by change handler
	WaitWithTimeout(t, &receivedChangesWg, time.Second*5)
	WaitWithTimeout(t, &revsFinishedWg, time.Second*5)

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")

}

// Start subChanges w/ continuous=true, batchsize=20
// Start sending rev messages for documents that grant access to themselves for the active replication's user
func TestConcurrentRefreshUser(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)
	// Initialize restTester here, so that we can use custom sync function, and later modify user
	syncFunction := `
function(doc, oldDoc) {
  access(doc.accessUser, doc.accessChannel)
  channel(doc.channels)
}

`
	rtConfig := RestTesterConfig{SyncFn: syncFunction}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	const user1 = "user1"
	rt.CreateUser(user1, nil)
	// Create bliptester that is connected as user1, with no access to channel ABC
	bt := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{
		connectingUsername: user1,
	})
	defer bt.Close()

	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receivedChangesWg := sync.WaitGroup{}
	revsFinishedWg := sync.WaitGroup{}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	lastReceivedSeq := float64(0)
	var numbatchesReceived int32
	nonIntegerSequenceReceived := false
	changeCount := 0
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		require.NoError(t, err)
		responseVal := [][]any{}
		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]any{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Len(t, change, 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					assert.True(t, receivedSeq > lastReceivedSeq)
					lastReceivedSeq = receivedSeq
				} else {
					nonIntegerSequenceReceived = true
					log.Printf("Unexpected non-integer sequence received: %v", change[0])
				}

				revID := change[2].(string)
				responseVal = append(responseVal, []any{revID})
				changeCount++
				receivedChangesWg.Done()
			}

		}

		if !request.NoReply() {
			// Send changes response
			// TODO: Sleeping here to avoid race in CBG-462, which appears to be occurring when there's very low latency
			// between the sendBatchOfChanges request and the response
			time.Sleep(10 * time.Millisecond)
			response := request.Response()
			responseValBytes, err := base.JSONMarshal(responseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(responseValBytes)
		}

	}

	// -------- Rev handler callback --------
	bt.blipContext.HandlerForProfile["rev"] = func(request *blip.Message) {
		defer revsFinishedWg.Done()
		body, err := request.Body()
		require.NoError(t, err)

		var doc RestDocument
		err = base.JSONUnmarshal(body, &doc)
		if err != nil {
			panic(fmt.Sprintf("Unexpected err: %v", err))
		}
		_, isRemoved := doc[db.BodyRemoved]
		require.False(t, isRemoved, fmt.Sprintf("Document %v shouldn't be removed", request.Properties[db.RevMessageID]))

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	assert.Equal(t, subChangesRequest.SerialNumber(), subChangesResponse.SerialNumber())

	// Simulate sending docs from the client
	receivedChangesWg.Add(100)
	revsFinishedWg.Add(100)
	// Sending revs may take a while if using views (GSI=false) due to the CBS views engine taking a while to execute the queries
	// regarding rebuilding the users access grants (due to the constant invalidation of this).
	// This blip tester is running as the user so the users access grants are rebuilt instantly when invalidated instead of the usual lazy-loading.
	for i := range 100 {
		docID := fmt.Sprintf("foo_%d", i)
		bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"accessUser": "user1",
			"accessChannel":"`+docID+`",
			"channels":["`+docID+`"]}`),
			blip.Properties{},
		)
	}

	// Wait until all expected changes are received by change handler
	WaitWithTimeout(t, &receivedChangesWg, time.Second*30)
	WaitWithTimeout(t, &revsFinishedWg, time.Second*30)

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")

}

// Test send and retrieval of a doc.
//
//	Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetRev(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	const user1 = "user1"
	rt.CreateUser(user1, nil)
	btSpec := BlipTesterSpec{
		connectingUsername: user1,
	}
	bt := NewBlipTesterFromSpecWithRT(rt, &btSpec)
	defer bt.Close()

	// Send non-deleted rev
	bt.SendRev("sendAndGetRev", "1-abc", []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{})

	// Get non-deleted rev
	response := bt.restTester.SendAdminRequest("GET", "/{{.keyspace}}/sendAndGetRev?rev=1-abc", "")
	RequireStatus(t, response, 200)
	var responseBody RestDocument
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	_, ok := responseBody[db.BodyDeleted]
	assert.False(t, ok)

	// Tombstone the document
	history := []string{"1-abc"}
	bt.SendRevWithHistory("sendAndGetRev", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{"deleted": "true"})

	// Get the tombstoned document
	response = bt.restTester.SendAdminRequest("GET", "/{{.keyspace}}/sendAndGetRev?rev=2-bcd", "")
	RequireStatus(t, response, 200)
	responseBody = RestDocument{}
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	require.Contains(t, responseBody, db.BodyDeleted)
	deletedValue, deletedOK := responseBody[db.BodyDeleted].(bool)
	assert.True(t, deletedOK)
	assert.True(t, deletedValue)
}

// Sends many revisions concurrently and ensures that SG limits the processing on the server-side with MaxConcurrentRevs
func TestBlipSendConcurrentRevs(t *testing.T) {

	const (
		maxConcurrentRevs    = 10
		concurrentSendRevNum = 50
	)
	rt := NewRestTester(t, &RestTesterConfig{
		LeakyBucketConfig: &base.LeakyBucketConfig{
			UpdateCallback: func(_ string) {
				time.Sleep(time.Millisecond * 5) // slow down rosmar - it's too quick to be throttled
			},
		},
		maxConcurrentRevs: base.Ptr(maxConcurrentRevs),
	})
	defer rt.Close()
	const user1 = "user1"
	rt.CreateUser(user1, nil)
	btSpec := BlipTesterSpec{
		connectingUsername: user1,
	}
	bt := NewBlipTesterFromSpecWithRT(rt, &btSpec)
	defer bt.Close()

	wg := sync.WaitGroup{}
	wg.Add(concurrentSendRevNum)
	for i := range concurrentSendRevNum {
		docID := fmt.Sprintf("%s-%d", t.Name(), i)
		go func() {
			defer wg.Done()
			bt.SendRev(docID, "1-abc", []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{})
		}()
	}

	WaitWithTimeout(t, &wg, time.Second*30)

	throttleCount := rt.GetDatabase().DbStats.CBLReplicationPush().WriteThrottledCount.Value()
	throttleTime := rt.GetDatabase().DbStats.CBLReplicationPush().WriteThrottledTime.Value()
	throttleDuration := time.Duration(throttleTime) * time.Nanosecond

	assert.Greater(t, throttleCount, int64(0), "Expected throttled revs")
	assert.Greater(t, throttleTime, int64(0), "Expected non-zero throttled revs time")

	t.Logf("Throttled revs: %d, Throttled duration: %s", throttleCount, throttleDuration)
}

// Test send and retrieval of a doc with a large numeric value.  Ensure proper large number handling.
//
//	Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetLargeNumberRev(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	const user1 = "user1"
	rt.CreateUser(user1, nil)
	btSpec := BlipTesterSpec{
		connectingUsername: user1,
	}
	bt := NewBlipTesterFromSpecWithRT(rt, &btSpec)
	defer bt.Close()

	// Send non-deleted rev
	bt.SendRev("largeNumberRev", "1-abc", []byte(`{"key": "val", "largeNumber":9223372036854775807, "channels": ["user1"]}`), blip.Properties{})

	// Get non-deleted rev
	response := bt.restTester.SendAdminRequest("GET", "/{{.keyspace}}/largeNumberRev?rev=1-abc", "")
	RequireStatus(t, response, 200) // Check the raw bytes, because unmarshalling the response would be another opportunity for the number to get modified
	responseString := string(response.Body.Bytes())
	if !strings.Contains(responseString, `9223372036854775807`) {
		t.Errorf("Response does not contain the expected number format.  Response: %s", responseString)
	}
}

func AssertChangeEquals(t *testing.T, change []any, expectedChange ExpectedChange) {
	if err := expectedChange.Equals(change); err != nil {
		t.Errorf("Change %+v does not equal expected change: %+v.  Error: %v", change, expectedChange, err)
	}
}

// Make sure it's not possible to have two outstanding subChanges w/ continuous=true.
// Expected behavior is that the first continuous change subscription should get discarded in favor of 2nd.
func TestConcurrentChangesSubscriptions(t *testing.T) {
	// TODO: Write tests to cover scenario
	t.Skip("not tested")
}

// Create a continuous changes subscription that has docs in multiple channels, and make sure
// all docs are received
func TestMultiChannelContinousChangesSubscription(t *testing.T) {
	// TODO: Write tests to cover scenario
	t.Skip("not tested")
}

// Test setting and getting checkpoints
func TestBlipSetCheckpoint(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	const user1 = "user1"
	rt.CreateUser(user1, nil)
	btSpec := BlipTesterSpec{
		connectingUsername: user1,
	}
	bt := NewBlipTesterFromSpecWithRT(rt, &btSpec)
	defer bt.Close()

	// Create new checkpoint
	checkpointBody := []byte(`{"client_seq":"1000"}`)
	resp := bt.SetCheckpoint("testclient", "", checkpointBody)
	assert.Equal(t, "", resp.Properties["Error-Code"])

	checkpointRev := resp.Rev()
	assert.Equal(t, "0-1", checkpointRev)

	// Validate checkpoint existence in bucket (local file name "/" needs to be URL encoded as %252F)
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/checkpoint%252Ftestclient", "")
	RequireStatus(t, response, 200)
	var responseBody map[string]any
	err := base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	require.NoError(t, err)
	assert.Equal(t, "1000", responseBody["client_seq"])

	// Attempt to update the checkpoint with previous rev
	checkpointBody = []byte(`{"client_seq":"1005"}`)
	resp = bt.SetCheckpoint("testclient", checkpointRev, checkpointBody)
	assert.Equal(t, "", resp.Properties["Error-Code"])
	checkpointRev = resp.Rev()
	assert.Equal(t, "0-2", checkpointRev)
}

// Test no-conflicts mode replication (proposeChanges endpoint)
func TestNoConflictsModeReplication(t *testing.T) {
	// TODO: Write tests to cover scenario
	t.Skip("not tested")
}

// Reproduce issue where ReloadUser was not being called, and so it was
// using a stale channel access grant for the user.
// Reproduces https://github.com/couchbase/sync_gateway/issues/2717
func TestReloadUser(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	syncFn := `
		function(doc) {
			if (doc._id == "access1") {
				// if its an access grant doc, grant access
				access(doc.accessUser, doc.accessChannel);
			} else {
                // otherwise if its a normal access doc, require access then add to channels
				requireAccess("PBS");
				channel(doc.channels);
			}
		}
    `

	// Setup
	rtConfig := RestTesterConfig{
		SyncFn: syncFn,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	const username = "user1"
	rt.CreateUser(username, nil)
	ctx := rt.Context()
	bt := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{
		connectingUsername: username,
	})
	defer bt.Close()

	// Set up a ChangeWaiter for this test, to block until the user change notification happens
	dbc := rt.GetDatabase()
	user1, err := dbc.Authenticator(ctx).GetUser(username)
	require.NoError(t, err)

	userDb, err := db.GetDatabase(dbc, user1)
	require.NoError(t, err)

	userWaiter := userDb.NewUserWaiter()

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	RequireStatus(t, response, 201)

	// Wait for notification
	db.WaitForUserWaiterChange(t, userWaiter)

	// Add a doc in the PBS channel
	addRevResponse := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Make assertions on response to make sure the change was accepted
	addRevResponseBody, err := addRevResponse.Body()
	assert.NoError(t, err, "Unexpected error")
	errorCode, hasErrorCode := addRevResponse.Properties["Error-Code"]
	assert.False(t, hasErrorCode)
	if hasErrorCode {
		t.Fatalf("Unexpected error sending revision.  Error code: %v.  Response body: %s", errorCode, addRevResponseBody)
	}

}

// Grant a user access to a channel via the Sync Function and a doc change, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaSyncFunction(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Setup
	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channels); access(doc.accessUser, doc.accessChannel);}`,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	const username = "user1"
	rt.CreateUser(username, nil)
	bt := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{
		connectingUsername: username,
	})
	defer bt.Close()

	// Add a doc in the PBS channel
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	RequireStatus(t, response, 201)

	// Add another doc in the PBS channel
	bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Make sure we can see it by getting changes
	changes := bt.WaitForNumChanges(2)
	log.Printf("changes: %+v", changes)
	assert.Len(t, changes, 2)

}

// Grant a user access to a channel via the REST Admin API, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaAdminApi(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
		syncFn:             channels.DocChannelsSyncFunction,
	})
	defer bt.Close()
	dataStore := bt.restTester.GetSingleDataStore()

	// Add a doc in the PBS channel
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Update the user doc to grant access to PBS
	response := bt.restTester.SendAdminRequest("PUT", "/db/_user/user1", GetUserPayload(t, "user1", "", "", dataStore, []string{"user1", "PBS"}, nil))
	RequireStatus(t, response, 200)

	// Add another doc in the PBS channel
	bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Make sure we can see both docs in the changes
	changes := bt.WaitForNumChanges(2)
	assert.Len(t, changes, 2)

}

func TestCheckpoint(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
	})
	defer bt.Close()

	client := "testClient"

	// Get the checkpoint -- expect to be missing at this point
	request := bt.newRequest()
	request.SetCompressed(true)
	request.SetProfile("getCheckpoint")
	request.Properties["client"] = client
	sent := bt.sender.Send(request)
	if !sent {
		panic(fmt.Sprintf("Failed to get checkpoint for client: %v", client))
	}
	checkpointResponse := request.Response()

	// Expect to get no checkpoint
	errorcode, ok := checkpointResponse.Properties["Error-Code"]
	assert.True(t, ok)
	assert.Equal(t, "404", errorcode)

	// Set a checkpoint
	requestSetCheckpoint := bt.newRequest()
	requestSetCheckpoint.SetCompressed(true)
	requestSetCheckpoint.SetProfile("setCheckpoint")
	requestSetCheckpoint.Properties["client"] = client
	checkpointBody := db.Body{"Key": "Value"}
	assert.NoError(t, requestSetCheckpoint.SetJSONBody(checkpointBody))
	// requestSetCheckpoint.Properties["rev"] = "rev1"
	sent = bt.sender.Send(requestSetCheckpoint)
	if !sent {
		panic(fmt.Sprintf("Failed to set checkpoint for client: %v", client))
	}
	checkpointResponse = requestSetCheckpoint.Response()
	body, err := checkpointResponse.Body()
	assert.NoError(t, err, "Unexpected error")
	log.Printf("responseSetCheckpoint body: %s", body)

	// Get the checkpoint and make sure it has the expected value
	requestGetCheckpoint2 := bt.newRequest()
	requestGetCheckpoint2.SetCompressed(true)
	requestGetCheckpoint2.SetProfile("getCheckpoint")
	requestGetCheckpoint2.Properties["client"] = client
	sent = bt.sender.Send(requestGetCheckpoint2)
	if !sent {
		panic(fmt.Sprintf("Failed to get checkpoint for client: %v", client))
	}
	checkpointResponse = requestGetCheckpoint2.Response()
	body, err = checkpointResponse.Body()
	assert.NoError(t, err, "Unexpected error")
	log.Printf("body: %s", body)
	assert.True(t, strings.Contains(string(body), "Key"))
	assert.True(t, strings.Contains(string(body), "Value"))

}

// Put a revision that is rejected by the sync function and assert that Sync Gateway
// returns an error code
func TestPutInvalidRevSyncFnReject(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	syncFn := `
		function(doc) {
			requireAccess("PBS");
			channel(doc.channels);
		}
    `

	// Setup
	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	const user1 = "user1"
	rt.CreateUser(user1, nil)
	bt := NewBlipTesterFromSpecWithRT(rt, &BlipTesterSpec{
		connectingUsername: user1,
	})
	defer bt.Close()

	// Add a doc that will be rejected by sync function, since user
	// does not have access to the CNN channel
	revRequest := bt.newRequest()
	revRequest.SetCompressed(false)
	revRequest.SetProfile("rev")
	revRequest.Properties["id"] = "foo"
	revRequest.Properties["rev"] = "1-aaa"
	revRequest.Properties["deleted"] = "false"
	revRequest.SetBody([]byte(`{"key": "val", "channels": ["CNN"]}`))
	sent := bt.sender.Send(revRequest)
	assert.True(t, sent)

	revResponse := revRequest.Response()

	// Since doc is rejected by sync function, expect a 403 error
	errorCode, hasErrorCode := revResponse.Properties["Error-Code"]
	assert.True(t, hasErrorCode)
	require.Equal(t, "403", errorCode)

	// Make sure that a one-off GetChanges() returns no documents
	changes := bt.GetChanges()
	assert.Len(t, changes, 0)

}

func TestPutInvalidRevMalformedBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	const username = "user1"
	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: username,
	})
	defer bt.Close()

	// Add a doc that will be rejected by sync function, since user
	// does not have access to the CNN channel
	revRequest := blip.NewRequest()
	revRequest.SetCompressed(false)
	revRequest.SetProfile("rev")
	revRequest.Properties["deleted"] = "false"
	revRequest.SetBody([]byte(`{"key": "val", "channels": [" MALFORMED JSON DOC`))

	sent := bt.sender.Send(revRequest)
	assert.True(t, sent)

	revResponse := revRequest.Response()

	// Since doc is rejected by sync function, expect a 403 error
	errorCode, hasErrorCode := revResponse.Properties["Error-Code"]
	assert.True(t, hasErrorCode)
	require.Equal(t, "400", errorCode)

	// Make sure that a one-off GetChanges() returns no documents
	changes := bt.GetChanges()
	assert.Len(t, changes, 0)

}

func TestPutRevNoConflictsMode(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	const username = "user1"
	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: username,
	})
	defer bt.Close()

	bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})

	// expected failure
	bt.SendRevExpectConflict("foo", "1-def", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "true"})
	bt.SendRevExpectConflict("foo", "1-ghi", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "false"})
}

func TestPutRevConflictsMode(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
	})
	defer bt.Close()

	bt.restTester.GetDatabase().EnableAllowConflicts(bt.TB())

	bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})

	bt.SendRev("foo", "1-def", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "false"})

	bt.SendRevExpectConflict("foo", "1-ghi", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "true"})
}

// TestPutRevV4:
//   - Create blip tester to run with V4 protocol
//   - Use send rev with CV defined in rev field and history field with PV/MV defined
//   - Retrieve the doc from bucket and assert that the HLV is set to what has been sent over the blip tester
func TestPutRevV4(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester with v4 protocol
	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		allowConflicts:     false,
		connectingUsername: "user1",
		blipProtocols:      []string{db.CBMobileReplicationV4.SubprotocolString()},
	})
	defer bt.Close()
	collection, ctx := bt.restTester.GetSingleTestDatabaseCollection()

	docID := t.Name()

	// 1. Send rev with history
	history := "1@b, 2@a"
	bt.SendRev(docID, "3@c", []byte(`{"key": "val"}`), blip.Properties{"history": history})

	// Validate against the bucket doc's HLV
	doc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalNoHistory)
	require.NoError(t, err)
	require.Equal(t, db.HybridLogicalVector{
		CurrentVersionCAS: doc.Cas,
		SourceID:          "c",
		Version:           3,
		PreviousVersions:  db.HLVVersions{"a": 2, "b": 1},
	}, *doc.HLV)

	// 2. Update the document with a non-conflicting revision, where only cv is updated
	bt.SendRev(docID, "4@c", []byte(`{"key": "val"}`), blip.Properties{"history": history})

	// Validate against the bucket doc's HLV
	doc, _, err = collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalNoHistory)
	require.NoError(t, err)
	require.Equal(t, db.HybridLogicalVector{
		CurrentVersionCAS: doc.Cas,
		SourceID:          "c",
		Version:           4,
		PreviousVersions:  db.HLVVersions{"a": 2, "b": 1},
	}, *doc.HLV)

	// 3. Update the document again with a non-conflicting revision from a different source (previous cv moved to pv)
	updatedHistory := "1@b, 2@a, 4@c"
	bt.SendRev(docID, "1@d", []byte(`{"key": "val"}`), blip.Properties{"history": updatedHistory})

	// Validate against the bucket doc's HLV
	doc, _, err = collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalNoHistory)
	require.NoError(t, err)
	require.Equal(t, db.HybridLogicalVector{
		CurrentVersionCAS: doc.Cas,
		SourceID:          "d",
		Version:           1,
		PreviousVersions:  db.HLVVersions{"c": 4, "a": 2, "b": 1},
	}, *doc.HLV)

	// 4. Update the document again with a non-conflicting revision from a different source, and additional sources in history (previous cv moved to pv, and pv expanded)
	updatedHistory = "1@b, 2@a, 4@c, 1@d"
	bt.SendRev(docID, "1@e", []byte(`{"key": "val"}`), blip.Properties{"history": updatedHistory})

	// Validate against the bucket doc's HLV
	doc, _, err = collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalNoHistory)
	require.NoError(t, err)
	require.Equal(t, db.HybridLogicalVector{
		CurrentVersionCAS: doc.Cas,
		SourceID:          "e",
		Version:           1,
		PreviousVersions:  db.HLVVersions{"d": 1, "c": 4, "a": 2, "b": 1},
	}, *doc.HLV)

	// 5. Attempt to update the document again with a conflicting revision from a different source (previous cv not in pv), expect conflict
	bt.SendRevExpectConflict(docID, db.EncodeTestVersion("1@pqr"), []byte(`{"key": "val"}`), blip.Properties{"history": db.EncodeTestHistory(updatedHistory)})

	// 6. Test sending rev with merge versions included in history (note new key)
	newDocID := t.Name() + "_2"
	mvHistory := "3@d, 3@e; 1@b, 2@a"
	bt.SendRev(newDocID, "3@c", []byte(`{"key": "val"}`), blip.Properties{"history": mvHistory})

	// assert on bucket doc
	doc, _, err = collection.GetDocWithXattrs(ctx, newDocID, db.DocUnmarshalNoHistory)
	require.NoError(t, err)

	require.Equal(t, db.HybridLogicalVector{
		CurrentVersionCAS: doc.Cas,
		SourceID:          "c",
		Version:           3,
		MergeVersions:     db.HLVVersions{"d": 3, "e": 3},
		PreviousVersions:  db.HLVVersions{"a": 2, "b": 1},
	}, *doc.HLV)
}

// Repro attempt for SG #3281
//
// - Set up a user w/ access to channel A
// - Write two revision of a document (both in channel A)
// - Write two more revisions of the document, no longer in channel A
// - Have the user issue a rev request for rev 3.
//
// Expected:
// - Users gets a removed:true response
//
// Actual:
// - Same as Expected (this test is unable to repro SG #3281, but is being left in as a regression test)
func TestGetRemovedDoc(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()
	const (
		user1 = "user1"
		user2 = "user2"
	)
	rt.CreateUser(user1, []string{user1})
	rt.CreateUser(user2, []string{user1})
	btSpec := BlipTesterSpec{
		connectingUsername: user1,
		blipProtocols:      []string{db.CBMobileReplicationV2.SubprotocolString()},
	}
	bt := NewBlipTesterFromSpecWithRT(rt, &btSpec)
	defer bt.Close()

	// Workaround data race (https://gist.github.com/tleyden/0ace70b8a38b76a7beee95529610b6cf) that happens because
	// there are multiple goroutines accessing the bt.blipContext.HandlerForProfile map.
	// The workaround uses a separate blipTester, and therefore a separate context.  It uses a different
	// user to avoid an error when the NewBlipTesterFromSpec tries to create the user (eg, user1 already exists error)
	btSpec2 := BlipTesterSpec{
		connectingUsername: user1,
		blipProtocols:      []string{db.CBMobileReplicationV2.SubprotocolString()},
	}
	bt2 := NewBlipTesterFromSpecWithRT(rt, &btSpec2)
	defer bt2.Close()

	// Add rev-1 in channel user1
	bt.SendRev("foo", "1-abc", []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{})

	// Add rev-2 in channel user1
	history := []string{"1-abc"}
	bt.SendRevWithHistory("foo", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{"noconflicts": "true"})

	// wait for rev 2 to arrive to cache cache
	rt.WaitForPendingChanges()

	// Try to get rev 2 via BLIP API and assert that _removed == false
	resultDoc, err := bt.GetDocAtRev("foo", "2-bcd")
	require.NoError(t, err, "Unexpected Error")
	assert.False(t, resultDoc.IsRemoved())

	// Add rev-3, remove from channel user1 and put into channel another_channel
	history = []string{"2-bcd", "1-abc"}
	bt.SendRevWithHistory("foo", "3-cde", history, []byte(`{"key": "val", "channels": ["another_channel"]}`), blip.Properties{"noconflicts": "true"})

	// Add rev-4, keeping it in channel another_channel
	history = []string{"3-cde", "2-bcd", "1-abc"}
	bt.SendRevWithHistory("foo", "4-def", history, []byte("{}"), blip.Properties{"noconflicts": "true", "deleted": "true"})

	rt.WaitForPendingChanges()

	// Flush rev cache in case this prevents the bug from showing up (didn't make a difference)
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Delete any temp revisions in case this prevents the bug from showing up (didn't make a difference)
	tempRevisionDocID := base.RevPrefix + "foo:5:3-cde"
	err = rt.GetSingleDataStore().Delete(tempRevisionDocID)
	assert.NoError(t, err, "Unexpected Error")

	// Try to get rev 3 via BLIP API and assert that there's a norev - modern clients will receive a replacement rev instead
	_, err = bt2.GetDocAtRev("foo", "3-cde")
	require.ErrorContains(t, err, "404 missing", "Expected norev 404 error")

	// Try to get rev 3 via REST API, and assert that it's now missing
	headers := map[string]string{}
	headers["Authorization"] = GetBasicAuthHeader(t, btSpec.connectingUsername, RestTesterDefaultUserPassword)
	response := rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/foo?rev=3-cde", "", headers)
	RequireStatus(t, response, http.StatusNotFound)
}

// Reproduce issue SG #3738
//
// - Add 5 docs to channel ABC
// - Purge one doc via _purge REST API
// - Flush rev cache
// - Send subChanges request
// - Reply to all changes saying all docs are wanted
// - Wait to receive rev messages for all 5 docs
//   - Expected: receive all 5 docs (4 revs and 1 norev)
//   - Actual: only receive 4 docs (4 revs)
func TestMissingNoRev(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	bt := NewBlipTesterFromSpecWithRT(rt, nil)
	defer bt.Close()

	// Create 5 docs
	for i := range 5 {
		docID := fmt.Sprintf("doc-%d", i)
		docRev := fmt.Sprintf("1-abc%d", i)
		bt.SendRev(docID, docRev, []byte(`{"key": "val", "channels": ["ABC"]}`), blip.Properties{})
	}

	// Pull docs, expect to pull 5 docs since none of them has purged yet.
	bt.WaitForNumDocsViaChanges(5)

	// Purge one doc
	doc0Id := fmt.Sprintf("doc-%d", 0)
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	err := collection.Purge(ctx, doc0Id, true)
	assert.NoError(t, err, "failed")

	// Flush rev cache
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Pull docs, expect to pull 4 since one was purged.  (also expect to NOT get stuck)
	bt.WaitForNumDocsViaChanges(4)

}

// TestSendReplacementRevision ensures that an alternative revision is sent instead of a norev when a client opts for replacement revs.
// Create doc with rev 1-..., make the client request changes, and then update the document underneath the client's changes request to force a norev, with 2-... being sent as an optional replacement
func TestSendReplacementRevision(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyHTTP, base.KeyHTTPResp, base.KeyCRUD, base.KeySync, base.KeySyncMsg)

	userChannels := []string{"ABC", "DEF"}
	rev1Channel := "ABC"

	tests := []struct {
		name                      string
		expectReplacementRev      bool
		clientSendReplacementRevs bool
		replacementRevChannel     string
		replicationChannels       string // empty string means no filter (user's accessible channels)
	}{
		{
			name:                      "no replacements",
			expectReplacementRev:      false,
			clientSendReplacementRevs: false,
			replacementRevChannel:     "ABC",
			replicationChannels:       "ABC,XYZ",
		},
		{
			name:                      "opt-in",
			expectReplacementRev:      true,
			clientSendReplacementRevs: true,
			replacementRevChannel:     "ABC",
			replicationChannels:       "ABC,XYZ",
		},
		{
			name:                      "opt-in no channel filter",
			expectReplacementRev:      true,
			clientSendReplacementRevs: true,
			replacementRevChannel:     "ABC",
			replicationChannels:       "", // no filter - uses user's accessible channels
		},
		{
			name:                      "opt-in filtered channel",
			expectReplacementRev:      false,
			clientSendReplacementRevs: true,
			replacementRevChannel:     "DEF", // accessible but filtered out
			replicationChannels:       "ABC,XYZ",
		},
		{
			name:                      "opt-in inaccessible channel",
			expectReplacementRev:      false,
			clientSendReplacementRevs: true,
			replacementRevChannel:     "XYZ", // inaccessible
			replicationChannels:       "ABC,XYZ",
		},
	}

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				rt := NewRestTester(t,
					&RestTesterConfig{
						SyncFn: channels.DocChannelsSyncFunction,
					})
				defer rt.Close()

				const alice = "alice"
				rt.CreateUser(alice, userChannels)

				docID := test.name
				version1 := rt.PutDoc(docID, fmt.Sprintf(`{"foo":"bar","channels":["%s"]}`, rev1Channel))
				updatedVersion := make(chan DocVersion)
				collection, ctx := rt.GetSingleTestDatabaseCollection()

				// underneath the client's response to changes - we'll update the document so the requested rev is not available by the time SG receives the changes response.
				changesEntryCallbackFn := func(changeEntryDocID, changeEntryRevID string) {
					if changeEntryDocID == docID && changeEntryRevID == version1.RevTreeID || changeEntryRevID == version1.CV.String() {
						updatedVersion <- rt.UpdateDoc(docID, version1, fmt.Sprintf(`{"foo":"buzz","channels":["%s"]}`, test.replacementRevChannel))

						// also purge revision backup and flush cache to ensure request for rev 1-... cannot be fulfilled
						err := collection.PurgeOldRevisionJSON(ctx, docID, version1.RevTreeID)
						require.NoError(t, err)
						rt.GetDatabase().FlushRevisionCacheForTest()
					}
				}

				opts := &BlipTesterClientOpts{
					Username:             alice,
					sendReplacementRevs:  test.clientSendReplacementRevs,
					changesEntryCallback: changesEntryCallbackFn,
				}
				btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
				defer btc.Close()

				// one shot or else we'll carry on to send rev 2-... normally, and we can't assert correctly on the final state of the client
				rt.WaitForPendingChanges()
				btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Channels: test.replicationChannels, Continuous: false})

				// block until we've written the update and got the new version to use in assertions
				version2 := <-updatedVersion

				if test.expectReplacementRev {
					// version 2 was sent instead
					_ = btcRunner.SingleCollection(btc.id).WaitForVersion(docID, version2)

					// rev message with a replacedRev property referring to the originally requested rev
					msg2, ok := btcRunner.SingleCollection(btc.id).GetPullRevMessage(docID, version2)
					require.True(t, ok)
					assert.Equal(t, db.MessageRev, msg2.Profile())
					if btc.UseHLV() {
						assert.Equal(t, version2.CV.String(), msg2.Properties[db.RevMessageRev])
						assert.Equal(t, version1.CV.String(), msg2.Properties[db.RevMessageReplacedRev])
					} else {
						assert.Equal(t, version2.RevTreeID, msg2.Properties[db.RevMessageRev])
						assert.Equal(t, version1.RevTreeID, msg2.Properties[db.RevMessageReplacedRev])
					}

					// the blip test framework records a message entry for the originally requested rev as well, but it should point to the message sent for rev 2
					// this is an artifact of the test framework to make assertions for tests not explicitly testing replacement revs easier
					msg1, ok := btcRunner.SingleCollection(btc.id).GetPullRevMessage(docID, version1)
					require.True(t, ok)
					assert.Equal(t, msg1, msg2)

					base.RequireWaitForStat(t, rt.GetDatabase().DbStats.CBLReplicationPull().ReplacementRevSendCount.Value, 1)
					base.RequireWaitForStat(t, rt.GetDatabase().DbStats.CBLReplicationPull().NoRevSendCount.Value, 0)
				} else {
					// requested revision (or any alternative) did not get replicated
					data := btcRunner.SingleCollection(btc.id).WaitForVersion(docID, version1)
					assert.Nil(t, data)

					// no message for rev 2
					_, ok := btcRunner.SingleCollection(btc.id).GetPullRevMessage(docID, version2)
					require.False(t, ok)

					// norev message for the requested rev
					msg, ok := btcRunner.SingleCollection(btc.id).GetPullRevMessage(docID, version1)
					require.True(t, ok)
					assert.Equal(t, db.MessageNoRev, msg.Profile())

					base.RequireWaitForStat(t, rt.GetDatabase().DbStats.CBLReplicationPull().NoRevSendCount.Value, 1)
					base.RequireWaitForStat(t, rt.GetDatabase().DbStats.CBLReplicationPull().ReplacementRevSendCount.Value, 0)
				}
			})
		}
	})
}

// TestBlipPullRevMessageHistory tests that a simple pull replication contains history in the rev message.
func TestBlipPullRevMessageHistory(t *testing.T) {
	base.LongRunningTest(t)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()
		client.ClientDeltas = true

		btcRunner.StartPull(client.id)

		const docID = "doc1"
		// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
		version1 := rt.PutDoc(docID, `{"hello": "world!"}`)

		data := btcRunner.WaitForVersion(client.id, docID, version1)
		assert.Equal(t, `{"hello":"world!"}`, string(data))

		// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
		version2 := rt.UpdateDoc(docID, version1, `{"hello": "alice"}`)

		data = btcRunner.WaitForVersion(client.id, docID, version2)
		assert.Equal(t, `{"hello":"alice"}`, string(data))

		msg, ok := btcRunner.GetPullRevMessage(client.id, docID, version2)
		require.True(t, ok)
		client.AssertOnBlipHistory(t, msg, version1)
	})
}

// TestPullReplicationUpdateOnOtherHLVAwarePeer:
//   - Main purpose is to test if history is correctly populated on HLV aware replication
//   - Making use of HLV agent to mock a doc from a HLV aware peer coming over replicator
//   - Update this same doc through sync gateway then assert that the history is populated with the old current version
func TestPullReplicationUpdateOnOtherHLVAwarePeer(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // V4 replication only test

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()
		collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		btcRunner.StartPull(client.id)

		const docID = "doc1"
		otherSource := "otherSource"
		hlvHelper := db.NewHLVAgent(t, rt.GetSingleDataStore(), otherSource, "_vv")
		existingHLVKey := "doc1"
		cas := hlvHelper.InsertWithHLV(ctx, existingHLVKey, nil)

		// force import of this write
		_, _ = rt.GetDoc(docID)
		bucketDoc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)

		// create doc version of the above doc write
		version1 := DocVersion{
			RevTreeID: bucketDoc.GetRevTreeID(),
			CV: db.Version{
				SourceID: hlvHelper.Source,
				Value:    cas,
			},
		}

		_ = btcRunner.WaitForVersion(client.id, docID, version1)

		// update the above doc
		version2 := rt.UpdateDoc(docID, version1, `{"hello": "world!"}`)

		data := btcRunner.WaitForVersion(client.id, docID, version2)
		assert.Equal(t, `{"hello":"world!"}`, string(data))

		msg, ok := btcRunner.GetPullRevMessage(client.id, docID, version2)
		require.True(t, ok)

		client.AssertOnBlipHistory(t, msg, version1)
	})
}

func TestBlipClientSendDelete(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		btcRunner.StartPush(client.id)

		// add doc and wait for arrival at rest tester
		const docID = "doc1"
		docVersion := btcRunner.AddRev(client.id, docID, EmptyDocVersion(), []byte(`{"key": "val"}`))
		rt.WaitForVersion(docID, docVersion)

		// delete doc and wait for deletion at rest tester
		deleteVersion := btcRunner.DeleteDoc(client.id, docID, &docVersion)
		rt.WaitForTombstone(docID, deleteVersion)
	})
}

// Reproduces CBG-617 (a client using activeOnly for the initial replication, and then still expecting to get subsequent tombstones afterwards)
func TestActiveOnlyContinuous(t *testing.T) {
	base.LongRunningTest(t)

	rtConfig := &RestTesterConfig{GuestEnabled: true}

	btcRunner := NewBlipTesterClientRunner(t)
	const docID = "doc1"

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()

		version := rt.PutDoc(docID, `{"test": true}`)

		// start an initial pull
		btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Continuous: true, Since: "0", ActiveOnly: true})
		rev := btcRunner.WaitForVersion(btc.id, docID, version)
		assert.Equal(t, `{"test":true}`, string(rev))

		// delete the doc and make sure the client still gets the tombstone replicated
		deletedVersion := rt.DeleteDoc(docID, version)

		rev = btcRunner.WaitForVersion(btc.id, docID, deletedVersion)
		assert.Equal(t, `{}`, string(rev))
	})
}

// Test that exercises Sync Gateway's norev handler
func TestBlipNorev(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rtConfig := &RestTesterConfig{GuestEnabled: true}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()

		norevMsg := db.NewNoRevMessage()
		norevMsg.SetId("docid")
		norevMsg.SetRev("1-a")
		require.NoError(t, norevMsg.SetSequence(db.SequenceID{Seq: 50}))
		norevMsg.SetError("404")
		norevMsg.SetReason("couldn't send xyz")
		btc.addCollectionProperty(norevMsg.Message)

		// Couchbase Lite always sends noreply=true for norev messages
		// but set to false so we can block waiting for a reply
		norevMsg.SetNoReply(false)

		// Request that the handler used to process the message is sent back in the response
		norevMsg.Properties[db.SGShowHandler] = "true"

		btc.pushReplication.sendMsg(norevMsg.Message)

		// Check that the response we got back was processed by the norev handler
		resp := norevMsg.Response()
		assert.NotNil(t, resp)
		assert.Equal(t, "handleNoRev", resp.Properties[db.SGHandler])
	})
}

// TestNoRevSetSeq makes sure the correct string is used with the corresponding function
func TestNoRevSetSeq(t *testing.T) {
	norevMsg := db.NewNoRevMessage()
	assert.Equal(t, "", norevMsg.Properties[db.NorevMessageSeq])
	assert.Equal(t, "", norevMsg.Properties[db.NorevMessageSequence])

	require.NoError(t, norevMsg.SetSequence(db.SequenceID{Seq: 50}))
	assert.Equal(t, "50", norevMsg.Properties[db.NorevMessageSequence])

	norevMsg.SetSeq(db.SequenceID{Seq: 60})
	assert.Equal(t, "60", norevMsg.Properties[db.NorevMessageSeq])

}

func TestRemovedMessageWithAlternateAccess(t *testing.T) {
	base.LongRunningTest(t)

	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
		defer rt.Close()

		const user = "user"
		rt.CreateUser(user, []string{"A", "B"})
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:        user,
			ClientDeltas:    false,
			SendRevocations: true,
		})
		defer btc.Close()

		const docID = "doc"
		version := rt.PutDoc(docID, `{"channels": ["A", "B"]}`)

		changes := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", true)
		assert.Equal(t, "doc", changes.Results[0].ID)
		RequireChangeRev(t, version, changes.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)

		btcRunner.StartOneshotPull(btc.id)
		_ = btcRunner.WaitForVersion(btc.id, docID, version)

		version = rt.UpdateDoc(docID, version, `{"channels": ["B"]}`)

		changes = rt.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&revocations=true", changes.Last_Seq), "user", true)
		assert.Equal(t, docID, changes.Results[0].ID)
		RequireChangeRev(t, version, changes.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)

		btcRunner.StartOneshotPull(btc.id)
		_ = btcRunner.WaitForVersion(btc.id, docID, version)

		version = rt.UpdateDoc(docID, version, `{"channels": []}`)
		const docMarker = "docmarker"
		docMarkerVersion := rt.PutDoc(docMarker, `{"channels": ["!"]}`)

		changes = rt.WaitForChanges(2, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&revocations=true", changes.Last_Seq), "user", true)
		assert.Equal(t, "doc", changes.Results[0].ID)
		RequireChangeRev(t, version, changes.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)
		assert.Equal(t, "3-1bc9dd04c8a257ba28a41eaad90d32de", changes.Results[0].Changes[0]["rev"])
		assert.False(t, changes.Results[0].Revoked)
		assert.Equal(t, "docmarker", changes.Results[1].ID)
		RequireChangeRev(t, docMarkerVersion, changes.Results[1].Changes[0], db.ChangesVersionTypeRevTreeID)
		assert.Equal(t, "1-999bcad4aab47f0a8a24bd9d3598060c", changes.Results[1].Changes[0]["rev"])
		assert.False(t, changes.Results[1].Revoked)

		btcRunner.StartOneshotPull(btc.id)
		_ = btcRunner.WaitForVersion(btc.id, docMarker, docMarkerVersion)

		changesMsg := btc.getMostRecentChangesMessage()

		var messageBody []any
		require.NoError(t, changesMsg.ReadJSONBody(&messageBody))
		require.Len(t, messageBody, 3)
		require.Len(t, messageBody[0], 4) // Rev 2 of doc, being sent as removal from channel A
		require.Len(t, messageBody[1], 4) // Rev 3 of doc, being sent as removal from channel B
		require.Len(t, messageBody[2], 3)

		deletedFlags, err := messageBody[0].([]any)[3].(json.Number).Int64()
		id := messageBody[0].([]any)[1]
		require.NoError(t, err)
		assert.Equal(t, "doc", id)
		assert.Equal(t, int64(4), deletedFlags)
	})
}

// TestRemovedMessageWithAlternateAccessAndChannelFilteredReplication tests the following scenario:
//
//	User has access to channel A and B
//	  Document rev 1 is in A and B
//	  Document rev 2 is in channel C
//	  Document rev 3 is in channel B
//	User issues changes requests with since=0 for channel A
//	  Revocation should not be issued because the user currently has access to channel B, even though they didn't
//	  have access to the removal revision (rev 2).  CBG-2277
func TestRemovedMessageWithAlternateAccessAndChannelFilteredReplication(t *testing.T) {
	base.LongRunningTest(t)

	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
		defer rt.Close()

		const user = "user"
		rt.CreateUser(user, []string{"A", "B"})
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:        user,
			ClientDeltas:    false,
			SendRevocations: true,
		})
		defer btc.Close()

		const (
			docID = "doc"
		)
		version := rt.PutDoc(docID, `{"channels": ["A", "B"]}`)

		changes := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", true)
		assert.Equal(t, docID, changes.Results[0].ID)
		RequireChangeRev(t, version, changes.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)

		btcRunner.StartOneshotPull(btc.id)
		_ = btcRunner.WaitForVersion(btc.id, docID, version)

		version = rt.UpdateDoc(docID, version, `{"channels": ["C"]}`)
		rt.WaitForPendingChanges()

		// At this point changes should send revocation, as document isn't in any of the user's channels
		changes = rt.WaitForChanges(1, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=A&since=0&revocations=true", "user", true)
		assert.Equal(t, docID, changes.Results[0].ID)
		RequireChangeRev(t, version, changes.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)

		btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Channels: "A", Continuous: false})
		_ = btcRunner.WaitForVersion(btc.id, docID, version)

		_ = rt.UpdateDoc(docID, version, `{"channels": ["B"]}`)
		markerID := "docmarker"
		markerVersion := rt.PutDoc(markerID, `{"channels": ["A"]}`)
		rt.WaitForPendingChanges()

		// Revocation should not be sent over blip, as document is now in user's channels - only marker document should be received
		changes = rt.WaitForChanges(2, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=A&since=0&revocations=true", "user", true)
		assert.Equal(t, "doc", changes.Results[0].ID)
		assert.Equal(t, markerID, changes.Results[1].ID)

		btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Channels: "A", Continuous: false})
		_ = btcRunner.WaitForVersion(btc.id, markerID, markerVersion)

		changesMsg := btc.getMostRecentChangesMessage()
		var messageBody []any
		require.NoError(t, changesMsg.ReadJSONBody(&messageBody))
		require.Len(t, messageBody, 1)
		require.Len(t, messageBody[0], 3) // marker doc
		require.Equal(t, "docmarker", messageBody[0].([]any)[1])
	})
}

// Make sure that a client cannot open multiple subChanges subscriptions on a single blip context (SG #3222)
// - Open a one-off subChanges request, ensure it works.
// - Open a subsequent continuous request, and ensure it works.
// - Open another continuous subChanges, and asserts that it gets an error on the 2nd one, because the first is still running.
// - Open another one-off subChanges request, assert we still get an error.
//
// Asserts on stats to test for regression of CBG-1824: Make sure SubChangesOneShotActive gets decremented when one shot
// sub changes request has completed
func TestMultipleOutstandingChangesSubscriptions(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// TODO: CBG-2653: change this to use NewBlipTester
	bt := NewBlipTesterDefaultCollection(t)
	defer bt.Close()

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {
		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []any{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}
	}

	pullStats := bt.restTester.GetDatabase().DbStats.CBLReplicationPull()
	require.EqualValues(t, 0, pullStats.NumPullReplTotalContinuous.Value())
	require.EqualValues(t, 0, pullStats.NumPullReplActiveContinuous.Value())
	require.EqualValues(t, 0, pullStats.NumPullReplTotalOneShot.Value())
	require.EqualValues(t, 0, pullStats.NumPullReplActiveOneShot.Value())
	require.EqualValues(t, 0, pullStats.NumPullReplSinceZero.Value())

	// Open an initial continuous = false subChanges request, which we'd expect to release the lock after it's "caught up".
	subChangesRequest := bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	require.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	require.Equal(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())
	errorCode := subChangesResponse.Properties["Error-Code"]
	log.Printf("errorCode: %v", errorCode)
	respBody, err := subChangesResponse.Body()
	require.NoError(t, err)
	require.Equal(t, "", errorCode, "resp: %s", respBody)

	base.RequireWaitForStat(t, pullStats.NumPullReplTotalOneShot.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveOneShot.Value, 0)
	base.RequireWaitForStat(t, pullStats.NumPullReplTotalContinuous.Value, 0)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveContinuous.Value, 0)
	base.RequireWaitForStat(t, pullStats.NumPullReplSinceZero.Value, 1)

	// Send continuous subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest = bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.SetCompressed(false)
	sent = bt.sender.Send(subChangesRequest)
	require.True(t, sent)
	subChangesResponse = subChangesRequest.Response()
	require.Equal(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())
	errorCode = subChangesResponse.Properties["Error-Code"]
	log.Printf("errorCode: %v", errorCode)
	respBody, err = subChangesResponse.Body()
	require.NoError(t, err)
	require.Equal(t, "", errorCode, "resp: %s", respBody)

	base.RequireWaitForStat(t, pullStats.NumPullReplTotalOneShot.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveOneShot.Value, 0)
	base.RequireWaitForStat(t, pullStats.NumPullReplTotalContinuous.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveContinuous.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplSinceZero.Value, 2)

	// Send a second continuous subchanges request, expect an error
	subChangesRequest = bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.SetCompressed(false)
	sent = bt.sender.Send(subChangesRequest)
	require.True(t, sent)
	subChangesResponse = subChangesRequest.Response()
	require.Equal(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())
	errorCode = subChangesResponse.Properties["Error-Code"]
	log.Printf("errorCode2: %v", errorCode)
	assert.Equal(t, "500", errorCode)

	base.RequireWaitForStat(t, pullStats.NumPullReplTotalOneShot.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveOneShot.Value, 0)
	base.RequireWaitForStat(t, pullStats.NumPullReplTotalContinuous.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveContinuous.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplSinceZero.Value, 2)

	// Even a subsequent continuous = false subChanges request should return an error. This isn't restricted to only continuous changes.
	subChangesRequest = bt.newRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	subChangesRequest.SetCompressed(false)
	sent = bt.sender.Send(subChangesRequest)
	require.True(t, sent)
	subChangesResponse = subChangesRequest.Response()
	require.Equal(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())
	errorCode = subChangesResponse.Properties["Error-Code"]
	log.Printf("errorCode: %v", errorCode)
	respBody, err = subChangesResponse.Body()
	require.NoError(t, err)
	assert.Equal(t, "500", errorCode, "resp: %s", respBody)

	base.RequireWaitForStat(t, pullStats.NumPullReplTotalOneShot.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveOneShot.Value, 0)
	base.RequireWaitForStat(t, pullStats.NumPullReplTotalContinuous.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveContinuous.Value, 1)
	base.RequireWaitForStat(t, pullStats.NumPullReplSinceZero.Value, 2)

	bt.sender.Close() // Close continuous sub changes feed

	base.RequireWaitForStat(t, pullStats.NumPullReplActiveOneShot.Value, 0)
	base.RequireWaitForStat(t, pullStats.NumPullReplActiveContinuous.Value, 0)
}

func TestBlipInternalPropertiesHandling(t *testing.T) {
	base.LongRunningTest(t)

	testCases := []struct {
		name                        string
		inputBody                   map[string]any
		rejectMsg                   string
		errorCode                   string
		skipDocContentsVerification *bool
	}{
		{
			name:      "Valid document",
			inputBody: map[string]any{"document": "is valid"},
		},
		{
			name:      "Valid document with special prop",
			inputBody: map[string]any{"_cookie": "is valid"},
		},
		{
			name:      "Invalid _sync",
			inputBody: map[string]any{"_sync": true},
			errorCode: "404",
			rejectMsg: "top-level property '_sync' is a reserved internal property",
		},
		{
			name:      "Valid _id",
			inputBody: map[string]any{"_id": "documentid"},
			errorCode: "404",
			rejectMsg: "top-level property '_id' is a reserved internal property",
		},
		{
			name:      "Valid _rev",
			inputBody: map[string]any{"_rev": "1-abc"},
			errorCode: "404",
			rejectMsg: "top-level property '_rev' is a reserved internal property",
		},
		{
			name:      "Valid _deleted",
			inputBody: map[string]any{"_deleted": false},
			errorCode: "404",
			rejectMsg: "top-level property '_deleted' is a reserved internal property",
		},
		{
			name:      "Invalid _attachments",
			inputBody: map[string]any{"_attachments": false},
			errorCode: "400",
			rejectMsg: "Invalid _attachments",
		},
		{
			name:                        "Valid _attachments",
			inputBody:                   map[string]any{"_attachments": map[string]any{"attch": map[string]any{"data": "c2d3IGZ0dw=="}}},
			skipDocContentsVerification: base.Ptr(true),
		},
		{
			name:                        "_revisions",
			inputBody:                   map[string]any{"_revisions": false},
			skipDocContentsVerification: base.Ptr(true),
			rejectMsg:                   "top-level property '_revisions' is a reserved internal property",
			errorCode:                   "404",
		},
		{
			name:                        "Valid _exp",
			inputBody:                   map[string]any{"_exp": "123"},
			skipDocContentsVerification: base.Ptr(true),
		},
		{
			name:      "Invalid _exp",
			inputBody: map[string]any{"_exp": "abc"},
			errorCode: "400",
			rejectMsg: "Unable to parse expiry",
		},
		{
			name:      "_purged",
			inputBody: map[string]any{"_purged": false},
			rejectMsg: "user defined top-level property '_purged' is not allowed",
			errorCode: "400",
		},
		{
			name:      "_removed",
			inputBody: map[string]any{"_removed": false},
			rejectMsg: "revision is not accessible",
			errorCode: "404",
		},
		{
			name:      "_sync_cookies",
			inputBody: map[string]any{"_sync_cookies": true},
			rejectMsg: "user defined top-level properties that start with '_sync_' are not allowed",
			errorCode: "400",
		},
		{
			name: "Valid user defined uppercase properties", // Uses internal properties names but in upper case
			// Known issue: _SYNC causes unmarshal error when not using xattrs
			inputBody: map[string]any{
				"_ID": true, "_REV": true, "_DELETED": true, "_ATTACHMENTS": true, "_REVISIONS": true,
				"_EXP": true, "_PURGED": true, "_REMOVED": true, "_SYNC_COOKIES": true,
			},
		},
	}

	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		// Setup
		rt := NewRestTester(t,
			&RestTesterConfig{
				GuestEnabled: true,
			})
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		// Track last sequence for next changes feed
		var changes ChangesResults

		for i, test := range testCases {
			rt.Run(test.name, func(t *testing.T) {
				docID := fmt.Sprintf("test%d", i)
				rawBody, err := json.Marshal(test.inputBody)
				require.NoError(t, err)

				// push each rev manually so we can error check the replication synchronously
				revRequest := blip.NewRequest()
				revRequest.SetProfile(db.MessageRev)
				revRequest.Properties[db.RevMessageID] = docID
				revRequest.Properties[db.RevMessageRev] = "1-abc" // use a fake rev
				revRequest.SetBody(rawBody)
				client.addCollectionProperty(revRequest)
				client.pushReplication.sendMsg(revRequest)
				resp := revRequest.Response()
				respBody, err := resp.Body()
				require.NoError(t, err)
				if test.rejectMsg != "" {
					require.Contains(t, string(respBody), test.rejectMsg)
					require.Equal(t, test.errorCode, resp.Properties["Error-Code"])
					return
				}
				require.Len(t, respBody, 0, "Expected nil response body got %s", string(respBody))

				// Wait for rev to be received on RT
				rt.WaitForPendingChanges()
				changes = rt.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s", changes.Last_Seq), "", true)

				var bucketDoc map[string]any
				_, err = rt.GetSingleDataStore().Get(docID, &bucketDoc)
				assert.NoError(t, err)
				body := rt.GetDocBody(docID)
				// Confirm input body is in the bucket doc
				if test.skipDocContentsVerification == nil || !*test.skipDocContentsVerification {
					for k, v := range test.inputBody {
						assert.Equal(t, v, bucketDoc[k])
						assert.Equal(t, v, body[k])
					}
				}
			})
		}
	})
}

// CBG-2053: Test that the handleRev stats still increment correctly when going through the processRev function with
// the stat mapping (processRevStats)
func TestProcessRevIncrementsStat(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := NewSGRTestRunner(t)

	sgrRunner.Run(func(t *testing.T) {
		activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)
		activeCtx := activeRT.Context()
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)

		replicationID := SafeDocumentName(t, t.Name())

		stats, err := base.SyncGatewayStats.NewDBStats("test", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		ar, err := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
			ID:                     replicationID,
			Direction:              db.ActiveReplicatorTypePull,
			ActiveDB:               &db.Database{DatabaseContext: activeRT.GetDatabase()},
			RemoteDBURL:            remoteURL,
			Continuous:             true,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !activeRT.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)

		// Confirm all stats starting on 0
		require.NotNil(t, ar.Pull)
		pullStats := ar.Pull.GetStats()
		require.EqualValues(t, 0, pullStats.HandleRevCount.Value())
		require.EqualValues(t, 0, pullStats.HandleRevBytes.Value())
		require.EqualValues(t, 0, pullStats.HandlePutRevCount.Value())

		const docID = "doc"
		version := remoteRT.CreateTestDoc(docID)

		assert.NoError(t, ar.Start(activeCtx))
		defer func() { require.NoError(t, ar.Stop()) }()

		activeRT.WaitForPendingChanges()
		sgrRunner.WaitForVersion(docID, activeRT, version)

		base.RequireWaitForStat(t, pullStats.HandleRevCount.Value, 1)
		assert.NotEqualValues(t, 0, pullStats.HandleRevBytes.Value())
		// Confirm connected client count has not increased, which uses same processRev code
		assert.EqualValues(t, 0, pullStats.HandlePutRevCount.Value())
	})
}

// Attempt to send rev as GUEST when read-only guest is enabled
func TestSendRevAsReadOnlyGuest(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{
		GuestEnabled: true,
	})
	defer bt.Close()

	// Send rev as guest with read-only=false
	revRequest := bt.newRequest()
	revRequest.SetCompressed(false)
	revRequest.SetProfile("rev")
	revRequest.Properties["deleted"] = "false"
	revRequest.Properties["id"] = "writeGuest"
	revRequest.Properties["rev"] = "1-abc"
	revRequest.SetBody([]byte(`{"key": "val"}`))

	sent := bt.sender.Send(revRequest)
	assert.True(t, sent)
	revResponse := revRequest.Response()

	errorCode, ok := revResponse.Properties[db.BlipErrorCode]
	require.False(t, ok)
	require.Equal(t, errorCode, "")

	body, err := revResponse.Body()
	require.NoError(t, err)
	log.Printf("response body: %s", body)

	// Send rev as guest with read-only=true
	bt.DatabaseContext().Options.UnsupportedOptions.GuestReadOnly = true

	revRequest = bt.newRequest()
	revRequest.SetCompressed(false)
	revRequest.SetProfile("rev")
	revRequest.Properties["deleted"] = "false"
	revRequest.Properties["id"] = "readOnlyGuest"
	revRequest.Properties["rev"] = "1-abc"
	revRequest.SetBody([]byte(`{"key": "val"}`))

	sent = bt.sender.Send(revRequest)
	assert.True(t, sent)
	revResponse = revRequest.Response()

	errorCode, ok = revResponse.Properties[db.BlipErrorCode]
	assert.True(t, ok)
	assert.Equal(t, "403", errorCode)

	body, err = revResponse.Body()
	require.NoError(t, err)
	log.Printf("response body: %s", body)

}

// Tests changes made in CBG-2151 to return errors from sendRevision unless it's a document not found error,
// in which case a noRev should be sent.
func TestSendRevisionNoRevHandling(t *testing.T) {
	base.LongRunningTest(t)

	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip LeakyBucket test when running in integration")
	}
	testCases := []struct {
		error       error
		expectNoRev bool
	}{
		{
			error:       gocb.ErrDocumentNotFound,
			expectNoRev: true,
		},
		{
			error:       gocb.ErrOverload,
			expectNoRev: false,
		},
	}
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		for _, test := range testCases {
			t.Run(fmt.Sprintf("%s", test.error), func(t *testing.T) {
				docName := fmt.Sprintf("%s", test.error)
				rt := NewRestTester(t,
					&RestTesterConfig{
						GuestEnabled:     true,
						CustomTestBucket: base.GetTestBucket(t).LeakyBucketClone(base.LeakyBucketConfig{}),
					})
				defer rt.Close()

				leakyDataStore, ok := base.AsLeakyDataStore(rt.Bucket().DefaultDataStore())
				require.True(t, ok)

				btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
				defer btc.Close()

				// Change noRev handler so it's known when a noRev is received
				receivedNoRevs := make(chan *blip.Message)
				btc.pullReplication.bt.blipContext.HandlerForProfile[db.MessageNoRev] = func(msg *blip.Message) {
					fmt.Println("Received noRev", msg.Properties)
					receivedNoRevs <- msg
				}

				version := rt.PutDoc(docName, `{"foo": "bar"}`)

				// Make the LeakyBucket return an error
				leakyDataStore.SetGetRawCallback(func(key string) error {
					return test.error
				})
				leakyDataStore.SetGetWithXattrCallback(func(key string) error {
					return test.error
				})

				// Flush cache so document has to be retrieved from the leaky bucket
				rt.GetDatabase().FlushRevisionCacheForTest()

				btcRunner.StartPull(btc.id)

				// Wait 3 seconds for noRev to be received
				select {
				case msg := <-receivedNoRevs:
					if test.expectNoRev {
						assert.Equal(t, docName, msg.Properties["id"])
					} else {
						require.Fail(t, "Received unexpected noRev message", msg)
					}
				case <-time.After(3 * time.Second):
					if test.expectNoRev {
						require.Fail(t, "Didn't receive expected noRev")
					}
				}

				// Make sure document did not get replicated
				_, found := btcRunner.GetVersion(btc.id, docName, version)
				assert.False(t, found)
			})
		}
	})
}

func TestUnsubChanges(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rtConfig := &RestTesterConfig{GuestEnabled: true}

	btcRunner := NewBlipTesterClientRunner(t)
	const (
		doc1ID = "doc1ID"
		doc2ID = "doc2ID"
	)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, rtConfig)
		defer rt.Close()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer btc.Close()
		// Confirm no error message or panic is returned in response
		btcRunner.UnsubPullChanges(btc.id)

		// Sub changes
		btcRunner.StartPull(btc.id)

		doc1Version := rt.PutDoc(doc1ID, `{"key": "val1"}`)
		_ = btcRunner.WaitForVersion(btc.id, doc1ID, doc1Version)

		activeReplStat := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplActiveContinuous
		require.EqualValues(t, 1, activeReplStat.Value())

		btcRunner.UnsubPullChanges(btc.id)
		// Wait for unsub changes to stop the sub changes being sent before sending document up
		base.RequireWaitForStat(t, activeReplStat.Value, 0)

		// Confirm no more changes are being sent
		doc2Version := rt.PutDoc(doc2ID, `{"key": "val1"}`)
		err := rt.WaitForConditionWithOptions(func() bool {
			_, found := btcRunner.GetVersion(btc.id, "doc2", doc2Version)
			return found
		}, 10, 100)
		assert.Error(t, err)

		// Confirm no error message is still returned when no subchanges active
		btcRunner.UnsubPullChanges(btc.id)

		// Confirm the pull replication can be restarted and it syncs doc2
		btcRunner.StartPull(btc.id)
		_ = btcRunner.WaitForVersion(btc.id, doc2ID, doc2Version)
	})
}

// TestRequestPlusPull tests that a one-shot pull replication waits for pending changes when request plus is set on the replication.
func TestRequestPlusPull(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyDCP, base.KeyChanges, base.KeyHTTP)
	defer db.SuspendSequenceBatching()() // Required for slow sequence simulation

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {
				channel(doc.channel);
				if (doc.accessUser != "") {
					access(doc.accessUser, doc.accessChannel)
				}
			}`,
	}
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()
		database := rt.GetDatabase()

		const bernard = "bernard"
		rt.CreateUser(bernard, nil)
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username: bernard,
		})
		defer client.Close()

		// Put a doc in channel PBS
		response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-1", `{"channel":["PBS"]}`)
		RequireStatus(t, response, 201)

		// Allocate a sequence but do not write a doc for it - will block DCP buffering until sequence is skipped
		slowSequence, seqErr := db.AllocateTestSequence(database)
		require.NoError(t, seqErr)

		// Write a document granting user 'bernard' access to PBS
		response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/grantDoc", `{"accessUser":"bernard", "accessChannel":"PBS"}`)
		RequireStatus(t, response, 201)

		caughtUpStart := database.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp.Value()

		// Start a regular one-shot pull
		btcRunner.StartPullSince(client.id, BlipTesterPullOptions{Continuous: true, RequestPlus: true})

		// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
		require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+1))

		// Release the slow sequence
		releaseErr := db.ReleaseTestSequence(base.TestCtx(t), database, slowSequence)
		require.NoError(t, releaseErr)

		// The one-shot pull should unblock and replicate the document in the granted channel
		data := btcRunner.WaitForDoc(client.id, "pbs-1")
		assert.Equal(t, `{"channel":["PBS"]}`, string(data))
	})
}

// TestRequestPlusPull tests that a one-shot pull replication waits for pending changes when request plus is set on the db config.
func TestRequestPlusPullDbConfig(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyDCP, base.KeyChanges, base.KeyHTTP)
	defer db.SuspendSequenceBatching()() // Required for slow sequence simulation

	rtConfig := RestTesterConfig{
		SyncFn: `function(doc) {
				channel(doc.channel);
				if (doc.accessUser != "") {
					access(doc.accessUser, doc.accessChannel)
				}
			}`,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				ChangesRequestPlus: base.Ptr(true),
			},
		},
	}

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()
		database := rt.GetDatabase()

		const bernard = "bernard"
		rt.CreateUser(bernard, nil)
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username: bernard,
		})
		defer client.Close()

		// Put a doc in channel PBS
		response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs-1", `{"channel":["PBS"]}`)
		RequireStatus(t, response, 201)

		// Allocate a sequence but do not write a doc for it - will block DCP buffering until sequence is skipped
		slowSequence, seqErr := db.AllocateTestSequence(database)
		require.NoError(t, seqErr)

		// Write a document granting user 'bernard' access to PBS
		response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/grantDoc", `{"accessUser":"bernard", "accessChannel":"PBS"}`)
		RequireStatus(t, response, 201)

		caughtUpStart := database.DbStats.CBLReplicationPull().NumPullReplTotalCaughtUp.Value()

		// Start a regular one-shot pull
		btcRunner.StartOneshotPull(client.id)

		// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
		require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+1))

		// Release the slow sequence
		releaseErr := db.ReleaseTestSequence(base.TestCtx(t), database, slowSequence)
		require.NoError(t, releaseErr)

		// The one-shot pull should unblock and replicate the document in the granted channel
		data := btcRunner.WaitForDoc(client.id, "pbs-1")
		assert.Equal(t, `{"channel":["PBS"]}`, string(data))
	})
}

// TestBlipRefreshUser makes sure there is no panic if a user gets deleted during a replication
func TestBlipRefreshUser(t *testing.T) {

	t.Skip("CBG-3512 known test flake")
	/*
		This probably happens because:

		1. The unsubChanges comes in before the delete mutation arrives over the caching DCP feed. This fails the test (doesn’t return 503)

		2. The delete mutation arrives over the caching feed, notifies the changes feed, and the changes feed errors out before the unsubChanges call is made. The test passes in this case

		3. The delete mutation arrives over the caching feed, and the unsubChanges call is made before the changes feed is notified/errors out. The test also passes in this case

			The problem is that adding a doc after the DELETE happens means that it might be guaranteed to hit (2) and never hit (3). I don't see how to make the test as is guarantee to catch this panic.
	*/
	rtConfig := RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	}
	const docID = "doc1"

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		const username = "bernard"
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username: username,
		})
		defer btc.Close()

		version := rt.PutDoc(docID, `{"channels": ["chan1"]}`)

		// Start a regular one-shot pull
		btcRunner.StartPullSince(btc.id, BlipTesterPullOptions{Continuous: true, Since: "0"})

		_ = btcRunner.WaitForDoc(btc.id, docID)

		_, ok := btcRunner.GetVersion(btc.id, docID, version)
		require.True(t, ok)

		// delete user with an active blip connection
		response := rt.SendAdminRequest(http.MethodDelete, "/{{.db}}/_user/"+username, "")
		RequireStatus(t, response, http.StatusOK)

		rt.WaitForPendingChanges()

		// further requests will 500, but shouldn't panic
		unsubChangesRequest := blip.NewRequest()
		unsubChangesRequest.SetProfile(db.MessageUnsubChanges)
		btc.addCollectionProperty(unsubChangesRequest)
		btc.pullReplication.sendMsg(unsubChangesRequest)

		testResponse := unsubChangesRequest.Response()
		require.Equal(t, strconv.Itoa(db.CBLReconnectErrorCode), testResponse.Properties[db.BlipErrorCode])
		body, err := testResponse.Body()
		require.NoError(t, err)
		require.NotContains(t, string(body), "Panic:")
	})
}

func TestImportInvalidSyncGetsNoRev(t *testing.T) {
	base.LongRunningTest(t)

	if !base.TestUseXattrs() {
		t.Skip("Test performs import, not valid for non-xattr mode")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyMigrate, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyCache, base.KeyChanges, base.KeySGTest)
	btcRunner := NewBlipTesterClientRunner(t)
	docID := "doc" + t.Name()
	docID2 := "doc2" + t.Name()

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{
			AutoImport: base.Ptr(false),
			SyncFn:     channels.DocChannelsSyncFunction,
		})
		defer rt.Close()

		const bob = "bob"
		rt.CreateUser(bob, []string{"ABC"})
		collection, ctx := rt.GetSingleTestDatabaseCollection()

		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username: bob,
		})
		defer btc.Close()
		version := rt.PutDoc(docID, `{"some":"data", "channels":["ABC"]}`)
		version2 := rt.PutDoc(docID2, `{"some":"data", "channels":["ABC"]}`)
		rt.WaitForPendingChanges()

		// get changes resident in channel cache
		changes := rt.PostChangesAdmin("/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=ABC", "{}")
		require.Len(t, changes.Results, 2)

		rt.GetDatabase().FlushRevisionCacheForTest()

		// alter sync data to be invalid
		cas, err := collection.GetCollectionDatastore().Get(docID, nil)
		require.NoError(t, err)
		_, err = collection.GetCollectionDatastore().WriteWithXattrs(ctx, docID, 0, cas, []byte(`{"foo" : "bar"}`), map[string][]byte{base.SyncXattrName: []byte(`{"rev": "1-cd809becc169215072fd567eebd8b8de","sequence": 1,"recent_sequences": [1],"attachments": {}, "history": {},"cas": "","time_saved": "2017-11-29T12:46:13.456631-08:00"}`)}, nil, nil)
		require.NoError(t, err)
		// add a doc with invalid inline sync data
		err = collection.GetCollectionDatastore().SetRaw(docID2, 0, nil, []byte(`{"some":"data", "_sync": {}}`))
		require.NoError(t, err)

		btcRunner.StartOneshotPull(btc.id)
		msg := btcRunner.WaitForPullRevMessage(btc.id, docID, version)
		require.Equal(t, db.MessageNoRev, msg.Profile())

		msg = btcRunner.WaitForPullRevMessage(btc.id, docID2, version2)
		require.Equal(t, db.MessageNoRev, msg.Profile())
	})
}

// TestOnDemandImportBlipFailure turns off feed-based import to be able to trigger on-demand import
// during a blip pull replication, by:
//  1. Write a document that's accessible to the client, and force import of this doc
//  2. Update the document
//  3. Flush the rev cache
//  4. Perform a pull replication.  Client will get a changes message for the initial revision of the document,
//     then SGW will detect the document needs to be imported when the rev is requested.  Should triggers norev handling
//     in the case where the import was unsuccessful
func TestOnDemandImportBlipFailure(t *testing.T) {
	base.LongRunningTest(t)

	if !base.TestUseXattrs() {
		t.Skip("Test performs import, not valid for non-xattr mode")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyCache, base.KeyChanges, base.KeySGTest)
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		syncFn := `function(doc) {
						if (doc.invalid) {
							throw("invalid document")
						}
						channel(doc.channel)
					}`
		importFilter := `function(doc) {
						if (doc.doNotImport) {
							return false
						}
						return true
					}`

		rt := NewRestTester(t, &RestTesterConfig{
			SyncFn:       syncFn,
			ImportFilter: importFilter,
			AutoImport:   base.Ptr(false),
		})
		defer rt.Close()

		testCases := []struct {
			name        string
			channel     string // used to avoid cross-traffic between tests
			updatedBody []byte
		}{

			{
				name:        "_id property",
				channel:     "a",
				updatedBody: []byte(`{"_id": "doc1"}`),
			},
			{
				name:        "_exp property",
				channel:     "b",
				updatedBody: []byte(`{"_exp": 1}`),
			},
			{
				name:        "_rev property",
				channel:     "c",
				updatedBody: []byte(`{"_rev": "abc1"}`),
			},
			{
				name:        "_revisions property",
				channel:     "d",
				updatedBody: []byte(`{"_revisions": {"start": 0, "ids": ["foo", "def]"}}`),
			},
			{
				name:        "_purged property",
				channel:     "e",
				updatedBody: []byte(`{"_purged": true}`),
			},
			{
				name:        "invalid json",
				channel:     "f",
				updatedBody: []byte(``),
			},
			{
				name:        "rejected by sync function",
				channel:     "g",
				updatedBody: []byte(`{"invalid": true}`),
			},
			{
				name:        "rejected by import filter",
				channel:     "h",
				updatedBody: []byte(`{"doNotImport": true}`),
			},
		}
		for i, testCase := range testCases {
			rt.Run(testCase.name, func(t *testing.T) {
				docID := fmt.Sprintf("doc%d_%s,", i, testCase.name)
				validBody := fmt.Sprintf(`{"foo":"bar", "channel":%q}`, testCase.channel)
				username := fmt.Sprintf("user_%d", i)

				rt.CreateUser(username, []string{testCase.channel})
				revID := rt.PutDoc(docID, validBody)

				// Wait for initial revision to arrive over DCP before mutating
				rt.WaitForPendingChanges()

				// Issue a changes request for the channel before updating the document, to ensure the valid revision is
				// resident in the channel cache (query results may be unreliable in the case of the 'invalid json' update)
				changes := rt.PostChangesAdmin("/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels="+testCase.channel, "{}")
				require.Len(t, changes.Results, 1)

				err := rt.GetSingleDataStore().SetRaw(docID, 0, nil, testCase.updatedBody)
				require.NoError(t, err)

				rt.WaitForPendingChanges()
				rt.GetDatabase().FlushRevisionCacheForTest()

				btc2 := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
					Username: username,
				})
				defer btc2.Close()

				btcRunner.StartOneshotPull(btc2.id)

				msg := btcRunner.WaitForPullRevMessage(btc2.id, docID, revID)
				require.Equal(t, db.MessageNoRev, msg.Profile())
			})
		}
	})
}

// TestBlipDatabaseClose verifies that the client connection is closed when the database is closed.
// Starts a continuous pull replication then updates the db to trigger a close.
func TestBlipDatabaseClose(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTesterPersistentConfig(t)
		defer rt.Close()
		const alice = "alice"
		rt.CreateUser(alice, []string{"*"})
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt,
			&BlipTesterClientOpts{
				Username: alice,
			})
		defer btc.Close()
		var blipContextClosed atomic.Bool
		btcRunner.clients[btc.id].pullReplication.bt.blipContext.OnExitCallback = func() {
			log.Printf("on exit callback invoked")
			blipContextClosed.Store(true)
		}

		// put a doc, and make sure blip connection is established
		markerDoc := "markerDoc"
		markerDocVersion := rt.PutDoc(markerDoc, `{"mark": "doc"}`)
		rt.WaitForPendingChanges()
		btcRunner.StartPull(btc.id)

		btcRunner.WaitForVersion(btc.id, markerDoc, markerDocVersion)

		RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/{{.db}}/", ""), http.StatusOK)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, blipContextClosed.Load())
		}, time.Second*10, time.Millisecond*100)
	})
}

func TestPutRevBlip(t *testing.T) {
	bt := NewBlipTesterFromSpec(t, BlipTesterSpec{GuestEnabled: true, blipProtocols: []string{db.CBMobileReplicationV4.SubprotocolString()}})
	defer bt.Close()

	bt.SendRev(
		"foo",
		"2@stZPWD8vS/O3nsx9yb2Brw",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)

	bt.SendRev(
		"foo",
		"fa1@stZPWD8vS/O3nsx9yb2Brw",
		[]byte(`{"key": "val2"}`),
		blip.Properties{},
	)
}

func TestBlipMergeVersions(t *testing.T) {
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // requires hlv
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTesterPersistentConfig(t)
		defer rt.Close()
		const alice = "alice"
		rt.CreateUser(alice, []string{"*"})
		opts := &BlipTesterClientOpts{
			Username: alice,
		}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		docID := t.Name()
		revRequest := blip.NewRequest()
		revRequest.SetProfile(db.MessageRev)
		revRequest.Properties[db.RevMessageID] = docID
		revRequest.Properties[db.RevMessageRev] = "3@CBL1"
		revRequest.Properties[db.RevMessageHistory] = "2@DEF,2@GHI;"
		revRequest.SetBody([]byte(`{"key": "val"}`))
		btc.addCollectionProperty(revRequest)
		btc.pushReplication.sendMsg(revRequest)

		resp := revRequest.Response()
		respBody, err := resp.Body()
		require.NoError(t, err)
		require.Empty(t, string(respBody))

		collection, ctx := rt.GetSingleTestDatabaseCollection()
		doc, _, err := collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalNoHistory)
		require.NoError(t, err)
		require.Equal(t, db.HybridLogicalVector{
			CurrentVersionCAS: doc.Cas,
			Version:           3,
			SourceID:          "CBL1",
			MergeVersions: db.HLVVersions{
				"DEF": 2,
				"GHI": 2,
			}}, *doc.HLV)

		btcRunner.StartPull(btc.id)
		btcRunner.WaitForDoc(btc.id, docID)

		revMsg := btcRunner.WaitForPullRevMessage(btc.id, docID, DocVersion{CV: db.Version{SourceID: "CBL1", Value: 3}})

		require.Equal(t, "3@CBL1", revMsg.Properties[db.RevMessageRev])
		// mv is not ordered so either string is valid
		require.Contains(t, []string{"2@DEF,2@GHI;", "2@GHI,2@DEF;"}, revMsg.Properties[db.RevMessageHistory])

		revRequest = blip.NewRequest()
		revRequest.SetProfile(db.MessageRev)
		revRequest.Properties[db.RevMessageID] = docID
		revRequest.Properties[db.RevMessageRev] = "4@CBL1"
		revRequest.SetBody([]byte(`{"key": "val2"}`))
		btc.addCollectionProperty(revRequest)
		btc.pushReplication.sendMsg(revRequest)

		resp = revRequest.Response()
		respBody, err = resp.Body()
		require.NoError(t, err)
		require.Empty(t, string(respBody))
		doc, _, err = collection.GetDocWithXattrs(ctx, docID, db.DocUnmarshalNoHistory)
		require.NoError(t, err)
		require.Equal(t, db.HybridLogicalVector{
			CurrentVersionCAS: doc.Cas,
			Version:           4,
			SourceID:          "CBL1",
			PreviousVersions: db.HLVVersions{
				"DEF": 2,
				"GHI": 2,
			}}, *doc.HLV)
	})
}

// Starts a continuous pull replication then updates the db to trigger a close.
func TestChangesFeedExitDisconnect(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		var shouldChannelQueryError atomic.Bool
		rt := NewRestTester(t, &RestTesterConfig{
			LeakyBucketConfig: &base.LeakyBucketConfig{
				QueryCallback: func(ddoc, viewname string, params map[string]any) error {
					if viewname == "channels" && shouldChannelQueryError.Load() {
						return gocb.ErrTimeout
					}
					return nil
				},
				N1QLQueryCallback: func(_ context.Context, statement string, params map[string]any, consistency base.ConsistencyMode, adhoc bool) error {
					if strings.Contains(statement, "sg_channels") && shouldChannelQueryError.Load() {
						return gocb.ErrTimeout
					}
					return nil
				},
			},
		})
		defer rt.Close()
		const alice = "alice"
		rt.CreateUser(alice, []string{"*"})
		rt.CreateTestDoc("doc1")
		rt.WaitForPendingChanges()
		collection, ctx := rt.GetSingleTestDatabaseCollection()
		err := collection.FlushChannelCache(ctx)
		require.NoError(t, err)
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt,
			&BlipTesterClientOpts{Username: alice},
		)
		defer btc.Close()
		var blipContextClosed atomic.Bool
		btcRunner.clients[btc.id].pullReplication.bt.blipContext.OnExitCallback = func() {
			blipContextClosed.Store(true)
		}

		shouldChannelQueryError.Store(true)
		btcRunner.StartPull(btc.id)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.True(c, blipContextClosed.Load())
		}, time.Second*10, time.Millisecond*100)
	})
}

func TestBlipPushRevOnResurrection(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache, base.KeySGTest)
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &RestTesterConfig{
			PersistentConfig: true,
		})
		defer rt.Close()

		dbConfig := rt.NewDbConfig()
		dbConfig.AutoImport = base.Ptr(false)
		RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)
		docID := "doc1"
		rt.CreateTestDoc(docID)

		rt.PurgeDoc(docID)

		RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID, ""), http.StatusNotFound)

		const alice = "alice"
		rt.CreateUser(alice, nil)
		opts := &BlipTesterClientOpts{Username: "alice"}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		btcRunner.StartPush(btc.id)
		docVersion := btcRunner.AddRev(btc.id, docID, EmptyDocVersion(), []byte(`{"resurrect":true}`))
		rt.WaitForVersion(docID, docVersion)
	})
}

func TestBlipPullConflict(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeySync, base.KeySyncMsg, base.KeySGTest)
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.SkipSubtest[RevtreeSubtestName] = true

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTesterPersistentConfig(t)
		defer rt.Close()

		const (
			alice   = "alice"
			cblBody = `{"actor": "cbl"}`
			sgBody  = `{"actor": "sg"}`
			docID   = "doc1"
		)
		rt.CreateUser(alice, []string{"*"})
		sgVersion := rt.PutDoc(docID, `{"actor": "sg"}`)

		opts := &BlipTesterClientOpts{
			Username: alice,
		}
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)
		defer btc.Close()

		client := btcRunner.SingleCollection(btc.id)
		preConflictCBLVersion := btcRunner.AddRev(btc.id, docID, EmptyDocVersion(), []byte(cblBody))
		require.NotEqual(t, sgVersion, preConflictCBLVersion)
		_, preConflictHLV, _ := client.GetDoc(docID)
		require.Empty(t, preConflictHLV.PreviousVersions)
		require.Empty(t, preConflictHLV.MergeVersions)

		btcRunner.StartOneshotPull(btc.id)

		// expect resolution as CBL wins (local wins)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			body, postConflictHLV, _ := client.GetDoc(docID)
			assert.Equal(c, db.HybridLogicalVector{
				CurrentVersionCAS: 0,
				Version:           preConflictCBLVersion.CV.Value,
				SourceID:          preConflictCBLVersion.CV.SourceID,
				PreviousVersions: db.HLVVersions{
					sgVersion.CV.SourceID: sgVersion.CV.Value,
				},
			}, *postConflictHLV)
			assert.Equal(c, string(body), cblBody)
		}, time.Second*10, time.Millisecond*10)
	})
}

func TestPushHLVOntoLegacyRev(t *testing.T) {
	t.Skip("CBG-4909 skipping due to conflict resolution not yet implemented for CBL rev tree")

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true // revtree subtest not relevant to this test
	btcRunner.Run(func(t *testing.T) {
		// Steps:
		// 1. Rev 1-abc is created on SGW
		// 2. Client pulls this revision (one shot)
		// 3. Doc is mutated on SGW to get 2-abc (legacy rev only)
		// 4. Doc is updated on CBL to get 100@CBL1 (HLV)
		// 5. Client attempts to push this doc update
		rt := NewRestTester(t,
			&RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
			})
		defer rt.Close()

		docID := SafeDocumentName(t, t.Name())

		const alice = "alice"
		rt.CreateUser(alice, []string{"ABC"})

		opts := &BlipTesterClientOpts{Username: alice}
		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, opts)

		// create legacy doc on rt and have CBL pull it
		doc := rt.CreateDocNoHLV(docID, db.Body{"channels": []string{"ABC"}})
		btcRunner.StartOneshotPull(client.id)

		docInitVersion := doc.ExtractDocVersion()
		btcRunner.WaitForVersion(client.id, docID, docInitVersion)

		// update doc again in legacy mode on rt
		_ = rt.CreateDocNoHLV(docID, db.Body{"channels": []string{"ABC"}, "_rev": docInitVersion.RevTreeID})

		// update doc on client to have vv given to it and attempt to push it
		newVersion := btcRunner.AddRev(client.id, docID, &docInitVersion, []byte(`{"channels": ["ABC"]}`))

		btcRunner.StartPush(client.id)
		btcRunner.StartPull(client.id)

		rt.WaitForVersion(docID, newVersion)
	})
}

func TestTombstoneCount(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	rtConfig := RestTesterConfig{
		GuestEnabled: true,
	}
	btcRunner := NewBlipTesterClientRunner(t)

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		btcRunner.StartPush(client.id)

		const docID1 = "doc1"
		docVersion := btcRunner.AddRev(client.id, docID1, EmptyDocVersion(), []byte(`{"key": "val"}`))
		rt.WaitForVersion(docID1, docVersion)

		deleteVersion := btcRunner.DeleteDoc(client.id, docID1, &docVersion)
		rt.WaitForTombstone(docID1, deleteVersion)
		assert.Equal(t, int64(1), rt.GetDatabase().DbStats.DatabaseStats.TombstoneCount.Value())

		const docID2 = "doc2"
		version := rt.CreateTestDoc(docID2)
		rt.DeleteDoc(docID2, version)
		assert.Equal(t, int64(2), rt.GetDatabase().DbStats.DatabaseStats.TombstoneCount.Value())
	})

}

func TestBlipNoRevOnCorruptHistory(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelTrace, base.KeyHTTP, base.KeySync, base.KeySyncMsg)
	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTesterPersistentConfig(t)
		defer rt.Close()

		const user = "user"
		rt.CreateUser(user, []string{"*"})
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username: user,
		})
		defer btc.Close()

		ctx := rt.Context()
		seq, err := rt.GetDatabase().NextSequence(ctx)
		require.NoError(t, err)
		// document contains an invalid revtree
		//
		// 3-c is a child of 3-d, a revision must be one or more generations higher than it its parent, not equal
		badSyncRaw := `{
			"cas": "expand",
			"channel_set":  null,
			"channels": null,
			"channel_set_history": null,
			"history": {
				"parents": [
					3,
					0,
					-1,
					2
				],
				"revs": [
					"3-d",
					"3-c",
					"1-a",
					"2-b"
				]
			},
			"rev": "3-c",
			"sequence": {{.seq}},
			"value_crc32c": "expand"
		}`
		tmpl := template.Must(template.New("badSync").Option("missingkey=error").Parse(badSyncRaw))
		var badSyncData bytes.Buffer
		require.NoError(t, tmpl.Execute(&badSyncData, map[string]any{
			"seq": seq,
		}))

		docID := SafeDocumentName(t, t.Name())
		mutateInOptions := db.DefaultMutateInOpts()
		_, err = rt.GetSingleDataStore().WriteWithXattrs(
			ctx,
			docID,
			0,
			0,
			[]byte(`{"key":"value"}`),
			map[string][]byte{
				base.SyncXattrName: badSyncData.Bytes(),
			},
			nil,
			mutateInOptions,
		)
		require.NoError(t, err)

		expectedVersion := DocVersion{RevTreeID: "3-c"}
		rt.WaitForVersion(docID, DocVersion{RevTreeID: expectedVersion.RevTreeID})

		btcRunner.StartOneshotPull(btc.id)
		msg := btcRunner.WaitForPullRevMessage(btc.id, docID, expectedVersion)
		require.Equal(t, db.MessageNoRev, msg.Profile())
	})
}

func TestBlipNoRevOnCorruptHistoryDelta(t *testing.T) {
	base.TestRequiresDeltaSync(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyCache, base.KeyCRUD, base.KeySGTest)
	btcRunner := NewBlipTesterClientRunner(t)
	// a norev message is only sent on delta sync when v2 protocol is used, otherwise the deleted flag on a changes
	// message is used
	btcRunner.RunSubprotocolV2(func(t *testing.T) {
		rt := NewRestTester(t,
			&RestTesterConfig{
				PersistentConfig: true,
				SyncFn:           channels.DocChannelsSyncFunction,
			},
		)
		defer rt.Close()

		dbConfig := rt.NewDbConfig()
		dbConfig.DeltaSync = &DeltaSyncConfig{Enabled: base.Ptr(true)}
		dbConfig.AutoImport = false
		RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

		const user = "user"
		const channelA = "A"
		rt.CreateUser(user, []string{channelA})
		btc := btcRunner.NewBlipTesterClientOptsWithRT(rt, &BlipTesterClientOpts{
			Username:     user,
			ClientDeltas: true,
		})
		defer btc.Close()

		ctx := rt.Context()
		docID := SafeDocumentName(t, t.Name())
		docRev1 := rt.CreateDocNoHLV(docID, db.Body{"delta": true, "channels": []string{channelA}})
		btcRunner.StartOneshotPull(btc.id)
		btcRunner.WaitForVersion(btc.id, docID, DocVersion{RevTreeID: docRev1.GetRevTreeID()})
		seq, err := rt.GetDatabase().NextSequence(ctx)
		require.NoError(t, err)
		// document contains an invalid revtree
		//
		// 3-c is a child of 3-d, a revision must be one or more generations higher than it its parent, not equal
		badSyncRaw := `{
			"cas": "expand",
			"channel_set":  [
				{
					"end": {{.rev3seq}},
					"name": "A",
					"start": {{.rev1seq}}
				}
			],
			"channels": {
				"A": {
					"rev": "{{.rev1}}",
					"seq": {{.rev3seq}}
				}
			},
			"channel_set_history": null,
			"history": {
				"parents": [
					3,
					0,
					-1,
					2
				],
				"revs": [
					"3-d",
					"3-c",
					"{{.rev1}}",
					"2-b"
				]
			},
			"rev": "3-c",
			"sequence": {{.rev3seq}},
			"value_crc32c": "expand"
		}`
		tmpl := template.Must(template.New("badSync").Option("missingkey=error").Parse(badSyncRaw))
		var badSyncData bytes.Buffer
		require.NoError(t, tmpl.Execute(&badSyncData, map[string]any{
			"rev1":    docRev1.GetRevTreeID(),
			"rev1seq": docRev1.Sequence,
			"rev3seq": seq,
		}))

		mutateInOptions := db.DefaultMutateInOpts()
		_, err = rt.GetSingleDataStore().WriteWithXattrs(
			ctx,
			docID,
			0,
			docRev1.Cas,
			[]byte(`{"key":"value"}`),
			map[string][]byte{
				base.SyncXattrName: badSyncData.Bytes(),
			},
			nil,
			mutateInOptions,
		)
		require.NoError(t, err)

		expectedVersion := DocVersion{RevTreeID: "3-c"}
		rt.WaitForVersion(docID, expectedVersion)

		btcRunner.StartOneshotPull(btc.id)
		msg := btcRunner.WaitForPullRevMessage(btc.id, docID, expectedVersion)
		require.Equal(t, db.MessageNoRev, msg.Profile())
	})
}

func TestMOUDeletedOnTombstone(t *testing.T) {
	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	col := bucket.GetSingleDataStore()

	docID := t.Name()
	docBody := map[string]any{"foo": "bar"}

	rtConfig := RestTesterConfig{
		CustomTestBucket: bucket,
		GuestEnabled:     true,
	}

	btcRunner := NewBlipTesterClientRunner(t)
	btcRunner.SkipSubtest[RevtreeSubtestName] = true

	btcRunner.Run(func(t *testing.T) {
		rt := NewRestTester(t, &rtConfig)
		defer rt.Close()

		client := btcRunner.NewBlipTesterClientOptsWithRT(rt, nil)
		defer client.Close()

		btcRunner.StartPull(client.ID())
		btcRunner.StartPush(client.ID())

		err := col.Set(docID, 0, &sgbucket.UpsertOptions{}, docBody)
		require.NoError(t, err)

		rt.WaitForDoc(docID)
		version, _ := rt.GetDoc(docID)

		btcRunner.WaitForVersion(client.ID(), docID, version)

		tombStoneVersion := btcRunner.DeleteDoc(client.ID(), docID, &version)
		rt.WaitForTombstone(docID, tombStoneVersion)

		rawDoc := rt.GetRawDoc(docID)
		mou, _ := rawDoc.Xattrs.RawDocXattrsOthers[base.MouXattrName]
		assert.Nil(t, mou)

	})
}
