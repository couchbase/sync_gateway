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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/gocb/v2"
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

	bt, err := NewBlipTester(t)
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	// Verify Sync Gateway will accept the doc revision that is about to be sent
	var changeList [][]interface{}
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
	assert.NoError(t, err, "Error unmarshalling response body")
	require.Equal(t, 1, len(changeList)) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow := changeList[0]
	assert.Equal(t, 0, len(changeRow)) // Should be empty, meaning the server is saying it doesn't have the revision yet

	// Send the doc revision in a rev request
	_, _, revResponse, err := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)
	assert.NoError(t, err)

	_, err = revResponse.Body()
	assert.NoError(t, err, "Error unmarshalling response body")

	// Call changes with a hypothetical new revision, assert that it returns last pushed revision
	var changeList2 [][]interface{}
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
	assert.Equal(t, 1, len(changeList2)) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow2 := changeList2[0]
	assert.Equal(t, 1, len(changeRow2)) // Should have 1 item in row, which is the rev id of the previous revision pushed
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
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")
			assert.Equal(t, 1, len(changeListReceived))
			change := changeListReceived[0] // [1,"foo","1-abc"]
			assert.Equal(t, 3, len(change))
			assert.Equal(t, float64(1), change[0].(float64)) // Expect sequence to be 1, since first item in DB
			assert.Equal(t, "foo", change[1])                // Doc id of pushed rev
			assert.Equal(t, "1-abc", change[2])              // Rev id of pushed rev

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
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
	timeoutErr := WaitWithTimeout(&receivedChangesRequestWg, time.Second*5)
	assert.NoError(t, timeoutErr, "Timed out waiting")
}

// Start subChanges w/ continuous=true, batchsize=10
// Make several updates
// Wait until we get the expected updates
func TestContinuousChangesSubscription(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)

	bt, err := NewBlipTester(t)
	require.NoError(t, err, "Error creating BlipTester")
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
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Equal(t, 3, len(change))

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
			emptyResponseVal := []interface{}{}
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
		_, _, revResponse, err := bt.SendRev(
			fmt.Sprintf("foo-%d", i),
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		require.NoError(t, err)

		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")

	}

	// Wait until all expected changes are received by change handler
	require.NoError(t, WaitWithTimeout(&receivedChangesWg, time.Second*30))

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

	bt, err := NewBlipTester(t)
	assert.NoError(t, err, "Error creating BlipTester")
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
	nonIntegerSequenceReceived := false
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		require.NoError(t, err)

		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Equal(t, 3, len(change))

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
			emptyResponseVal := []interface{}{}
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
		_, _, revResponse, err := bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		require.NoError(t, err)
		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")
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
	// receivedChangesWg.Wait()
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*60)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")

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

	// Create a few more changes, validate that they aren't sent (subChanges has been closed).
	// Validated by the prefix matching in the subChanges callback, as well as waitgroup check below.
	for i := 0; i < 5; i++ {
		// // Add a change: Send an unsolicited doc revision in a rev request
		_, _, revResponse, err := bt.SendRev(
			fmt.Sprintf("postOneShot_%d", i),
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		require.NoError(t, err)
		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")
		receivedChangesWg.Add(1)
	}

	// Wait long enough to ensure the changes aren't being sent
	expectedTimeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*1)
	if expectedTimeoutErr == nil {
		t.Errorf("Received additional changes after one-shot should have been closed.")
	}

	// Validate integer sequences
	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")
}

// Test subChanges w/ docID filter
func TestBlipSubChangesDocIDFilter(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTester(t)
	require.NoError(t, err)
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
	nonIntegerSequenceReceived := false

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		require.NoError(t, err)

		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Equal(t, 3, len(change))

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
				assert.True(t, strings.HasPrefix(docID, "docIDFiltered"))
				assert.Equal(t, "1-abc", change[2]) // Rev id of pushed rev
				log.Printf("Changes got docID: %s", docID)

				// Ensure we only receive expected docs
				_, isExpected := docIDsReceived[docID]
				if !isExpected {
					t.Errorf(fmt.Sprintf("Received unexpected docId: %s", docID))
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
			emptyResponseVal := []interface{}{}
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
		_, _, revResponse, err := bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		assert.NoError(t, err)
		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")
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
	// receivedChangesWg.Wait()
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*15)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")

	// Since batch size was set to 10, and 15 docs were added, expect at _least_ 2 batches
	numBatchesReceivedSnapshot := atomic.LoadInt32(&numbatchesReceived)
	assert.True(t, numBatchesReceivedSnapshot >= 2)

	// Validate all expected documents were received.
	for docID, received := range docIDsReceived {
		if !received {
			t.Errorf("Did not receive expected doc %s in changes", docID)
		}
	}

	// Validate that the 'caught up' message was sent
	assert.True(t, receivedCaughtUpChange)

	// Validate integer sequences
	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")
}

// Push proposed changes and ensure that the server accepts them
//
// 1. Start sync gateway in no-conflicts mode
// 2. Send changes push request with multiple doc revisions
// 3. Make sure there are no panics
// 4. Make sure that the server responds to accept the changes (empty array)
func TestProposedChangesNoConflictsMode(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
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

	// The common case of an empty array response tells the sender to send all of the proposed revisions,
	// so the changeList returned by Sync Gateway is expected to be empty
	assert.Equal(t, 0, len(changeList))

}

// Validate SG sends conflicting rev when requested
func TestProposedChangesIncludeConflictingRev(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	// Write existing docs to server directly (not via blip)
	rt := bt.restTester
	resp := rt.PutDoc("conflictingInsert", `{"version":1}`)
	conflictingInsertRev := resp.Rev

	resp = rt.PutDoc("matchingInsert", `{"version":1}`)
	matchingInsertRev := resp.Rev

	resp = rt.PutDoc("conflictingUpdate", `{"version":1}`)
	conflictingUpdateRev1 := resp.Rev
	resp = rt.UpdateDoc("conflictingUpdate", resp.Rev, `{"version":2}`)
	conflictingUpdateRev2 := resp.Rev

	resp = rt.PutDoc("matchingUpdate", `{"version":1}`)
	matchingUpdateRev1 := resp.Rev
	resp = rt.UpdateDoc("matchingUpdate", resp.Rev, `{"version":2}`)
	matchingUpdateRev2 := resp.Rev

	resp = rt.PutDoc("newUpdate", `{"version":1}`)
	newUpdateRev1 := resp.Rev

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
			key:           "matchingInsert",
			revID:         matchingInsertRev,
			parentRevID:   "",
			expectedValue: float64(db.ProposedRev_Exists),
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
	assert.NoError(t, err, "Error getting changes response body reader")

	var changeList []interface{}
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

	// Create bliptester that is connected as user1, with access to the user1 channel
	btUser1, err := NewBlipTesterFromSpec(t,
		BlipTesterSpec{
			connectingUsername: "user1",
			connectingPassword: "1234",
			syncFn:             channels.DocChannelsSyncFunction,
		})
	require.NoError(t, err)
	defer btUser1.Close()

	// Send the user1 doc
	_, _, _, err = btUser1.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["user1"]}`),
		blip.Properties{},
	)
	require.NoError(t, err, "Error sending revision")

	// Create bliptester that is connected as user2, with access to the * channel
	btUser2, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername:          "user2",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // user2 has access to all channels
	}, btUser1.restTester) // re-use rest tester, otherwise it will create a new underlying bucket in walrus case
	require.NoError(t, err, "Error creating BlipTester")
	defer btUser2.Close()

	// Send the user2 doc, which is in a "random" channel, but it should be accessible due to * channel access
	_, _, _, err = btUser2.SendRev(
		"foo2",
		"1-abcd",
		[]byte(`{"key": "val", "channels": ["NBC"]}`),
		blip.Properties{},
	)
	require.NoError(t, err, "Error sending revision")

	// Assert that user1 received a single expected change
	changesChannelUser1 := btUser1.WaitForNumChanges(1)
	assert.Equal(t, 1, len(changesChannelUser1))
	change := changesChannelUser1[0]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo", revId: "1-abc", sequence: "*", deleted: base.BoolPtr(false)})

	// Assert that user2 received user1's change as well as it's own change
	changesChannelUser2 := btUser2.WaitForNumChanges(2)
	assert.Equal(t, 2, len(changesChannelUser2))
	change = changesChannelUser2[0]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo", revId: "1-abc", sequence: "*", deleted: base.BoolPtr(false)})

	change = changesChannelUser2[1]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo2", revId: "1-abcd", sequence: "*", deleted: base.BoolPtr(false)})

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
	var rt = NewRestTester(t, &rtConfig)
	defer rt.Close()
	ctx := rt.Context()
	collection := rt.GetSingleTestDatabaseCollection()

	// Create bliptester that is connected as user1, with no access to channel ABC
	bt, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}, rt)
	assert.NoError(t, err, "Error creating BlipTester")

	// Attempt to send a doc, should be rejected
	_, _, _, sendErr := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)
	assert.Error(t, sendErr, "Expected error sending rev (403 sg missing channel access)")

	// Set up a ChangeWaiter for this test, to block until the user change notification happens
	dbc := rt.GetDatabase()
	user1, err := dbc.Authenticator(ctx).GetUser("user1")
	require.NoError(t, err)

	userDb, err := db.GetDatabase(dbc, user1)
	require.NoError(t, err)

	userWaiter := userDb.NewUserWaiter()

	// Update the user to grant them access to ABC
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", GetUserPayload(t, "user1", "", "", collection, []string{"ABC"}, nil))
	RequireStatus(t, response, 200)

	// Wait for notification
	require.True(t, db.WaitForUserWaiterChange(userWaiter))

	// Attempt to send the doc again, should succeed if the blip context also received notification
	_, _, _, sendErr = bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
		blip.Properties{},
	)
	assert.NoError(t, sendErr)

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

	// Create bliptester that is connected as user1, with no access to channel ABC
	bt, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}, rt)
	assert.NoError(t, err, "Error creating BlipTester")
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
		responseVal := [][]interface{}{}
		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Equal(t, 3, len(change))

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
				responseVal = append(responseVal, []interface{}{revID})
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
	require.NoError(t, rt.WaitForPendingChanges())

	// Wait until all expected changes are received by change handler
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*5)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")

	revTimeoutErr := WaitWithTimeout(&revsFinishedWg, time.Second*5)
	assert.NoError(t, revTimeoutErr, "Timed out waiting for all revs.")

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")

}

// Start subChanges w/ continuous=true, batchsize=20
// Start sending rev messages for documents that grant access to themselves for the active replication's user
func TestConcurrentRefreshUser(t *testing.T) {
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

	// Create bliptester that is connected as user1, with no access to channel ABC
	bt, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}, rt)
	assert.NoError(t, err, "Error creating BlipTester")
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
		responseVal := [][]interface{}{}
		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.Equal(t, 3, len(change))

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
				responseVal = append(responseVal, []interface{}{revID})
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
	beforeChangesSent := time.Now().UnixMilli()
	// Sending revs may take a while if using views (GSI=false) due to the CBS views engine taking a while to execute the queries
	// regarding rebuilding the users access grants (due to the constant invalidation of this).
	// This blip tester is running as the user so the users access grants are rebuilt instantly when invalidated instead of the usual lazy-loading.
	for i := 0; i < 100; i++ {
		docID := fmt.Sprintf("foo_%d", i)
		_, _, _, sendErr := bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"accessUser": "user1",
			"accessChannel":"`+docID+`",
			"channels":["`+docID+`"]}`),
			blip.Properties{},
		)
		require.NoError(t, sendErr)
	}

	// Wait until all expected changes are received by change handler
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*30)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")
	fmt.Println("Revs sent and changes received in", time.Now().UnixMilli()-beforeChangesSent, "ms")

	revTimeoutErr := WaitWithTimeout(&revsFinishedWg, time.Second*30)
	assert.NoError(t, revTimeoutErr, "Timed out waiting for all revs.")

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")

}

// Test send and retrieval of a doc.
//
//	Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetRev(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}
	bt, err := NewBlipTesterFromSpecWithRT(t, &btSpec, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Send non-deleted rev
	sent, _, resp, err := bt.SendRev("sendAndGetRev", "1-abc", []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{})
	assert.True(t, sent)
	assert.NoError(t, err)
	assert.Equal(t, "", resp.Properties["Error-Code"])

	// Get non-deleted rev
	response := bt.restTester.SendAdminRequest("GET", "/{{.keyspace}}/sendAndGetRev?rev=1-abc", "")
	RequireStatus(t, response, 200)
	var responseBody RestDocument
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	_, ok := responseBody[db.BodyDeleted]
	assert.False(t, ok)

	// Tombstone the document
	history := []string{"1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("sendAndGetRev", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{"deleted": "true"})
	assert.True(t, sent)
	assert.NoError(t, err)
	assert.Equal(t, "", resp.Properties["Error-Code"])

	// Get the tombstoned document
	response = bt.restTester.SendAdminRequest("GET", "/{{.keyspace}}/sendAndGetRev?rev=2-bcd", "")
	RequireStatus(t, response, 200)
	responseBody = RestDocument{}
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	deletedValue, deletedOK := responseBody[db.BodyDeleted].(bool)
	assert.True(t, deletedOK)
	assert.True(t, deletedValue)
}

// Test send and retrieval of a doc with a large numeric value.  Ensure proper large number handling.
//
//	Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetLargeNumberRev(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	rt := NewRestTester(t, nil)
	defer rt.Close()
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}
	bt, err := NewBlipTesterFromSpecWithRT(t, &btSpec, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Send non-deleted rev
	sent, _, resp, err := bt.SendRev("largeNumberRev", "1-abc", []byte(`{"key": "val", "largeNumber":9223372036854775807, "channels": ["user1"]}`), blip.Properties{})
	assert.True(t, sent)
	assert.NoError(t, err)
	assert.Equal(t, "", resp.Properties["Error-Code"])

	// Get non-deleted rev
	response := bt.restTester.SendAdminRequest("GET", "/{{.keyspace}}/largeNumberRev?rev=1-abc", "")
	RequireStatus(t, response, 200) // Check the raw bytes, because unmarshalling the response would be another opportunity for the number to get modified
	responseString := string(response.Body.Bytes())
	if !strings.Contains(responseString, `9223372036854775807`) {
		t.Errorf("Response does not contain the expected number format.  Response: %s", responseString)
	}
}

func AssertChangeEquals(t *testing.T, change []interface{}, expectedChange ExpectedChange) {
	if err := expectedChange.Equals(change); err != nil {
		t.Errorf("Change %+v does not equal expected change: %+v.  Error: %v", change, expectedChange, err)
	}
}

// Make sure it's not possible to have two outstanding subChanges w/ continuous=true.
// Expected behavior is that the first continous change subscription should get discarded in favor of 2nd.
func TestConcurrentChangesSubscriptions(t *testing.T) {
	// TODO: Write tests to cover scenario
	t.Skip("not tested")
}

// Create a continous changes subscription that has docs in multiple channels, and make sure
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
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}
	bt, err := NewBlipTesterFromSpecWithRT(t, &btSpec, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Create new checkpoint
	checkpointBody := []byte(`{"client_seq":"1000"}`)
	sent, _, resp, err := bt.SetCheckpoint("testclient", "", checkpointBody)
	assert.True(t, sent)
	assert.NoError(t, err)
	assert.Equal(t, "", resp.Properties["Error-Code"])

	checkpointRev := resp.Rev()
	assert.Equal(t, "0-1", checkpointRev)

	// Validate checkpoint existence in bucket (local file name "/" needs to be URL encoded as %252F)
	response := rt.SendAdminRequest("GET", "/{{.keyspace}}/_local/checkpoint%252Ftestclient", "")
	RequireStatus(t, response, 200)
	var responseBody map[string]interface{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	require.NoError(t, err)
	assert.Equal(t, "1000", responseBody["client_seq"])

	// Attempt to update the checkpoint with previous rev
	checkpointBody = []byte(`{"client_seq":"1005"}`)
	sent, _, resp, err = bt.SetCheckpoint("testclient", checkpointRev, checkpointBody)
	assert.True(t, sent)
	assert.NoError(t, err)
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
	ctx := rt.Context()
	bt, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Set up a ChangeWaiter for this test, to block until the user change notification happens
	dbc := rt.GetDatabase()
	user1, err := dbc.Authenticator(ctx).GetUser("user1")
	require.NoError(t, err)

	userDb, err := db.GetDatabase(dbc, user1)
	require.NoError(t, err)

	userWaiter := userDb.NewUserWaiter()

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	RequireStatus(t, response, 201)

	// Wait for notification
	require.True(t, db.WaitForUserWaiterChange(userWaiter))

	// Add a doc in the PBS channel
	_, _, addRevResponse, err := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)
	assert.NoError(t, err)

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
	bt, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Add a doc in the PBS channel
	_, _, _, err = bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	require.NoError(t, err)

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	RequireStatus(t, response, 201)

	// Add another doc in the PBS channel
	_, _, _, err = bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)
	require.NoError(t, err)

	// Make sure we can see it by getting changes
	changes := bt.WaitForNumChanges(2)
	log.Printf("changes: %+v", changes)
	assert.Equal(t, 2, len(changes))

}

// Grant a user access to a channel via the REST Admin API, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaAdminApi(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		syncFn:             channels.DocChannelsSyncFunction,
	})
	require.NoError(t, err)
	defer bt.Close()
	collection := bt.restTester.GetSingleTestDatabaseCollection()

	// Add a doc in the PBS channel
	_, _, _, _ = bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Update the user doc to grant access to PBS
	response := bt.restTester.SendAdminRequest("PUT", "/db/_user/user1", GetUserPayload(t, "user1", "", "", collection, []string{"user1", "PBS"}, nil))
	RequireStatus(t, response, 200)

	// Add another doc in the PBS channel
	_, _, _, _ = bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Make sure we can see both docs in the changes
	changes := bt.WaitForNumChanges(2)
	assert.Equal(t, 2, len(changes))

}

func TestCheckpoint(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
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
	bt, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
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
	assert.Equal(t, 0, len(changes))

}

func TestPutInvalidRevMalformedBody(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername:          "user1",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // All channels
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
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
	assert.Equal(t, 0, len(changes))

}

func TestPutRevNoConflictsMode(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode:    true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)                            // no error
	assert.Equal(t, "", resp.Properties["Error-Code"]) // no error

	sent, _, resp, err = bt.SendRev("foo", "1-def", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "true"})
	assert.True(t, sent)
	assert.NotEqual(t, nil, err)                          // conflict error
	assert.Equal(t, "409", resp.Properties["Error-Code"]) // conflict

	sent, _, resp, err = bt.SendRev("foo", "1-ghi", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "false"})
	assert.True(t, sent)
	assert.NotEqual(t, nil, err)                          // conflict error
	assert.Equal(t, "409", resp.Properties["Error-Code"]) // conflict

}

func TestPutRevConflictsMode(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode:    false,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)                            // no error
	assert.Equal(t, "", resp.Properties["Error-Code"]) // no error

	sent, _, resp, err = bt.SendRev("foo", "1-def", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "false"})
	assert.True(t, sent)
	assert.NoError(t, err)                             // no error
	assert.Equal(t, "", resp.Properties["Error-Code"]) // no error

	sent, _, resp, err = bt.SendRev("foo", "1-ghi", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "true"})
	assert.True(t, sent)
	assert.NotEqual(t, nil, err)                          // conflict error
	assert.Equal(t, "409", resp.Properties["Error-Code"]) // conflict

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

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		blipProtocols:      []string{db.BlipCBMobileReplicationV2},
	}
	bt, err := NewBlipTesterFromSpecWithRT(t, &btSpec, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Workaround data race (https://gist.github.com/tleyden/0ace70b8a38b76a7beee95529610b6cf) that happens because
	// there are multiple goroutines accessing the bt.blipContext.HandlerForProfile map.
	// The workaround uses a separate blipTester, and therefore a separate context.  It uses a different
	// user to avoid an error when the NewBlipTesterFromSpec tries to create the user (eg, user1 already exists error)
	btSpec2 := BlipTesterSpec{
		connectingUsername:          "user2",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"user1"}, // so it can see user1's docs
		blipProtocols:               []string{db.BlipCBMobileReplicationV2},
	}
	bt2, err := NewBlipTesterFromSpecWithRT(t, &btSpec2, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt2.Close()

	// Add rev-1 in channel user1
	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{})
	assert.True(t, sent)
	require.NoError(t, err)                        // no error
	assert.Empty(t, resp.Properties["Error-Code"]) // no error

	// Add rev-2 in channel user1
	history := []string{"1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("foo", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{"noconflicts": "true"})
	assert.True(t, sent)
	require.NoError(t, err)                        // no error
	assert.Empty(t, resp.Properties["Error-Code"]) // no error

	require.NoError(t, rt.WaitForPendingChanges())

	// Try to get rev 2 via BLIP API and assert that _removed == false
	resultDoc, err := bt.GetDocAtRev("foo", "2-bcd")
	require.NoError(t, err, "Unexpected Error")
	assert.False(t, resultDoc.IsRemoved())

	// Add rev-3, remove from channel user1 and put into channel another_channel
	history = []string{"2-bcd", "1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("foo", "3-cde", history, []byte(`{"key": "val", "channels": ["another_channel"]}`), blip.Properties{"noconflicts": "true"})
	assert.True(t, sent)
	assert.NoError(t, err)                         // no error
	assert.Empty(t, resp.Properties["Error-Code"]) // no error

	// Add rev-4, keeping it in channel another_channel
	history = []string{"3-cde", "2-bcd", "1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("foo", "4-def", history, []byte("{}"), blip.Properties{"noconflicts": "true", "deleted": "true"})
	assert.True(t, sent)
	assert.NoError(t, err)                         // no error
	assert.Empty(t, resp.Properties["Error-Code"]) // no error

	require.NoError(t, rt.WaitForPendingChanges())

	// Flush rev cache in case this prevents the bug from showing up (didn't make a difference)
	rt.GetSingleTestDatabaseCollection().FlushRevisionCacheForTest()

	// Delete any temp revisions in case this prevents the bug from showing up (didn't make a difference)
	tempRevisionDocID := base.RevPrefix + "foo:5:3-cde"
	err = rt.GetSingleDataStore().Delete(tempRevisionDocID)
	assert.NoError(t, err, "Unexpected Error")

	// Try to get rev 3 via BLIP API and assert that _removed == true
	resultDoc, err = bt2.GetDocAtRev("foo", "3-cde")
	assert.NoError(t, err, "Unexpected Error")
	assert.True(t, resultDoc.IsRemoved())

	// Try to get rev 3 via REST API, and assert that _removed == true
	headers := map[string]string{}
	headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(btSpec.connectingUsername+":"+btSpec.connectingPassword))
	response := rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/foo?rev=3-cde", "", headers)
	restDocument := response.GetRestDocument()
	assert.True(t, restDocument.IsRemoved())

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
	ctx := rt.Context()

	bt, err := NewBlipTesterFromSpecWithRT(t, nil, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	require.NoError(t, rt.WaitForDBOnline())

	// Create 5 docs
	for i := 0; i < 5; i++ {
		docID := fmt.Sprintf("doc-%d", i)
		docRev := fmt.Sprintf("1-abc%d", i)
		sent, _, resp, err := bt.SendRev(docID, docRev, []byte(`{"key": "val", "channels": ["ABC"]}`), blip.Properties{})
		assert.True(t, sent)
		require.NoError(t, err, "resp is %s", resp)
	}

	// Pull docs, expect to pull 5 docs since none of them has purged yet.
	docs, ok := bt.WaitForNumDocsViaChanges(5)
	require.True(t, ok)
	assert.Len(t, docs, 5)

	// Purge one doc
	doc0Id := fmt.Sprintf("doc-%d", 0)
	err = rt.GetSingleTestDatabaseCollectionWithUser().Purge(ctx, doc0Id)
	assert.NoError(t, err, "failed")

	// Flush rev cache
	rt.GetSingleTestDatabaseCollection().FlushRevisionCacheForTest()

	// Pull docs, expect to pull 4 since one was purged.  (also expect to NOT get stuck)
	docs, ok = bt.WaitForNumDocsViaChanges(4)
	assert.True(t, ok)
	assert.Len(t, docs, 4)

}

// TestBlipPullRevMessageHistory tests that a simple pull replication contains history in the rev message.
func TestBlipPullRevMessageHistory(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		}},
		GuestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()
	client.ClientDeltas = true

	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 12345678901234567890}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageHistory])
}

// Reproduces CBG-617 (a client using activeOnly for the initial replication, and then still expecting to get subsequent tombstones afterwards)
func TestActiveOnlyContinuous(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc1", `{"test":true}`)
	RequireStatus(t, resp, http.StatusCreated)
	var docResp struct {
		Rev string `json:"rev"`
	}
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &docResp))

	// start an initial pull
	require.NoError(t, btc.StartPullSince("true", "0", "true"))
	rev, found := btc.WaitForRev("doc1", docResp.Rev)
	assert.True(t, found)
	assert.Equal(t, `{"test":true}`, string(rev))

	// delete the doc and make sure the client still gets the tombstone replicated
	resp = rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/doc1?rev="+docResp.Rev, ``)
	RequireStatus(t, resp, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &docResp))

	rev, found = btc.WaitForRev("doc1", docResp.Rev)
	assert.True(t, found)
	assert.Equal(t, `{}`, string(rev))
}

// Test that exercises Sync Gateway's norev handler
func TestBlipNorev(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	norevMsg := db.NewNoRevMessage()
	norevMsg.SetId("docid")
	norevMsg.SetRev("1-a")
	norevMsg.SetSequence(db.SequenceID{Seq: 50})
	norevMsg.SetError("404")
	norevMsg.SetReason("couldn't send xyz")
	btc.addCollectionProperty(norevMsg.Message)

	// Couchbase Lite always sends noreply=true for norev messages
	// but set to false so we can block waiting for a reply
	norevMsg.SetNoReply(false)

	// Request that the handler used to process the message is sent back in the response
	norevMsg.Properties[db.SGShowHandler] = "true"

	assert.NoError(t, btc.pushReplication.sendMsg(norevMsg.Message))

	// Check that the response we got back was processed by the norev handler
	resp := norevMsg.Response()
	assert.NotNil(t, resp)
	assert.Equal(t, "handleNoRev", resp.Properties[db.SGHandler])
}

// TestNoRevSetSeq makes sure the correct string is used with the corresponding function
func TestNoRevSetSeq(t *testing.T) {
	norevMsg := db.NewNoRevMessage()
	assert.Equal(t, "", norevMsg.Properties[db.NorevMessageSeq])
	assert.Equal(t, "", norevMsg.Properties[db.NorevMessageSequence])

	norevMsg.SetSequence(db.SequenceID{Seq: 50})
	assert.Equal(t, "50", norevMsg.Properties[db.NorevMessageSequence])

	norevMsg.SetSeq(db.SequenceID{Seq: 60})
	assert.Equal(t, "60", norevMsg.Properties[db.NorevMessageSeq])

}

func TestRemovedMessageWithAlternateAccess(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	resp := rt.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "test", "", collection, []string{"A", "B"}, nil))
	RequireStatus(t, resp, http.StatusCreated)

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	require.NoError(t, err)
	defer btc.Close()

	docRevID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"A", "B"}})

	changes, err := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, "1-9b49fa26d87ad363b2b08de73ff029a9", changes.Results[0].Changes[0]["rev"])

	err = btc.StartOneshotPull()
	assert.NoError(t, err)
	_, ok := btc.WaitForRev("doc", "1-9b49fa26d87ad363b2b08de73ff029a9")
	assert.True(t, ok)

	docRevID = rt.CreateDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{"B"}})

	changes, err = rt.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&revocations=true", changes.Last_Seq), "user", true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, "2-f0d4cbcdd4a9ec835799055fdba45263", changes.Results[0].Changes[0]["rev"])

	err = btc.StartOneshotPull()
	assert.NoError(t, err)
	_, ok = btc.WaitForRev("doc", "2-f0d4cbcdd4a9ec835799055fdba45263")
	assert.True(t, ok)

	_ = rt.CreateDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{}})
	_ = rt.CreateDocReturnRev(t, "docmarker", "", map[string]interface{}{"channels": []string{"!"}})

	changes, err = rt.WaitForChanges(2, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s&revocations=true", changes.Last_Seq), "user", true)
	require.NoError(t, err)
	assert.Len(t, changes.Results, 2)
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, "3-1bc9dd04c8a257ba28a41eaad90d32de", changes.Results[0].Changes[0]["rev"])
	assert.False(t, changes.Results[0].Revoked)
	assert.Equal(t, "docmarker", changes.Results[1].ID)
	assert.Equal(t, "1-999bcad4aab47f0a8a24bd9d3598060c", changes.Results[1].Changes[0]["rev"])
	assert.False(t, changes.Results[1].Revoked)

	err = btc.StartOneshotPull()
	assert.NoError(t, err)
	_, ok = btc.WaitForRev("docmarker", "1-999bcad4aab47f0a8a24bd9d3598060c")
	assert.True(t, ok)

	messages := btc.pullReplication.GetMessages()

	var highestMsgSeq uint32
	var highestSeqMsg blip.Message
	// Grab most recent changes message
	for _, message := range messages {
		messageBody, err := message.Body()
		require.NoError(t, err)
		if message.Properties["Profile"] == db.MessageChanges && string(messageBody) != "null" {
			if highestMsgSeq < uint32(message.SerialNumber()) {
				highestMsgSeq = uint32(message.SerialNumber())
				highestSeqMsg = message
			}
		}
	}

	var messageBody []interface{}
	err = highestSeqMsg.ReadJSONBody(&messageBody)
	assert.NoError(t, err)
	require.Len(t, messageBody, 3)
	require.Len(t, messageBody[0], 4) // Rev 2 of doc, being sent as removal from channel A
	require.Len(t, messageBody[1], 4) // Rev 3 of doc, being sent as removal from channel B
	require.Len(t, messageBody[2], 3)

	deletedFlags, err := messageBody[0].([]interface{})[3].(json.Number).Int64()
	docID := messageBody[0].([]interface{})[1]
	require.NoError(t, err)
	assert.Equal(t, "doc", docID)
	assert.Equal(t, int64(4), deletedFlags)
}

// TestRemovedMessageWithAlternateAccessAndChannelFilteredReplication tests the following scenario:
//   User has access to channel A and B
//     Document rev 1 is in A and B
//     Document rev 2 is in channel C
//     Document rev 3 is in channel B
//   User issues changes requests with since=0 for channel A
//     Revocation should not be issued because the user currently has access to channel B, even though they didn't
//     have access to the removal revision (rev 2).  CBG-2277

func TestRemovedMessageWithAlternateAccessAndChannelFilteredReplication(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	resp := rt.SendAdminRequest("PUT", "/db/_user/user", GetUserPayload(t, "user", "test", "", collection, []string{"A", "B"}, nil))
	RequireStatus(t, resp, http.StatusCreated)

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	require.NoError(t, err)
	defer btc.Close()

	docRevID := rt.CreateDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"A", "B"}})

	changes, err := rt.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0&revocations=true", "user", true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, "1-9b49fa26d87ad363b2b08de73ff029a9", changes.Results[0].Changes[0]["rev"])

	err = btc.StartOneshotPull()
	assert.NoError(t, err)
	_, ok := btc.WaitForRev("doc", "1-9b49fa26d87ad363b2b08de73ff029a9")
	assert.True(t, ok)

	docRevID = rt.CreateDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{"C"}})

	// At this point changes should send revocation, as document isn't in any of the user's channels
	changes, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=A&since=0&revocations=true", "user", true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, docRevID, changes.Results[0].Changes[0]["rev"])

	err = btc.StartOneshotPullFiltered("A")
	assert.NoError(t, err)
	_, ok = btc.WaitForRev("doc", docRevID)
	assert.True(t, ok)

	_ = rt.CreateDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{"B"}})
	markerDocRevID := rt.CreateDocReturnRev(t, "docmarker", "", map[string]interface{}{"channels": []string{"A"}})

	// Revocation should not be sent over blip, as document is now in user's channels - only marker document should be received
	changes, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes?filter=sync_gateway/bychannel&channels=A&since=0&revocations=true", "user", true)
	require.NoError(t, err)
	assert.Len(t, changes.Results, 2) // _changes still gets two results, as we don't support 3.0 removal handling over REST API
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, "docmarker", changes.Results[1].ID)

	err = btc.StartOneshotPullFiltered("A")
	assert.NoError(t, err)
	_, ok = btc.WaitForRev("docmarker", markerDocRevID)
	assert.True(t, ok)

	messages := btc.pullReplication.GetMessages()

	var highestMsgSeq uint32
	var highestSeqMsg blip.Message
	// Grab most recent changes message
	for _, message := range messages {
		messageBody, err := message.Body()
		require.NoError(t, err)
		if message.Properties["Profile"] == db.MessageChanges && string(messageBody) != "null" {
			if highestMsgSeq < uint32(message.SerialNumber()) {
				highestMsgSeq = uint32(message.SerialNumber())
				highestSeqMsg = message
			}
		}
	}

	var messageBody []interface{}
	err = highestSeqMsg.ReadJSONBody(&messageBody)
	assert.NoError(t, err)
	require.Len(t, messageBody, 1)
	require.Len(t, messageBody[0], 3) // marker doc
	require.Equal(t, "docmarker", messageBody[0].([]interface{})[1])
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
			emptyResponseVal := []interface{}{}
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

	// Send continous subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
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
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	testCases := []struct {
		name                        string
		inputBody                   map[string]interface{}
		expectReject                bool
		skipDocContentsVerification *bool
	}{
		{
			name:         "Valid document",
			inputBody:    map[string]interface{}{"document": "is valid"},
			expectReject: false,
		},
		{
			name:         "Valid document with special prop",
			inputBody:    map[string]interface{}{"_cookie": "is valid"},
			expectReject: false,
		},
		{
			name:         "Invalid _sync",
			inputBody:    map[string]interface{}{"_sync": true},
			expectReject: true,
		},
		{
			name:         "Valid _id",
			inputBody:    map[string]interface{}{"_id": "documentid"},
			expectReject: true,
		},
		{
			name:         "Valid _rev",
			inputBody:    map[string]interface{}{"_rev": "1-abc"},
			expectReject: true,
		},
		{
			name:         "Valid _deleted",
			inputBody:    map[string]interface{}{"_deleted": false},
			expectReject: true,
		},
		{
			name:         "Invalid _attachments",
			inputBody:    map[string]interface{}{"_attachments": false},
			expectReject: true,
		},
		{
			name:                        "Valid _attachments",
			inputBody:                   map[string]interface{}{"_attachments": map[string]interface{}{"attch": map[string]interface{}{"data": "c2d3IGZ0dw=="}}},
			expectReject:                false,
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                        "_revisions",
			inputBody:                   map[string]interface{}{"_revisions": false},
			expectReject:                true,
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:                        "Valid _exp",
			inputBody:                   map[string]interface{}{"_exp": "123"},
			expectReject:                false,
			skipDocContentsVerification: base.BoolPtr(true),
		},
		{
			name:         "Invalid _exp",
			inputBody:    map[string]interface{}{"_exp": "abc"},
			expectReject: true,
		},
		{
			name:         "_purged",
			inputBody:    map[string]interface{}{"_purged": false},
			expectReject: true,
		},
		{
			name:         "_removed",
			inputBody:    map[string]interface{}{"_removed": false},
			expectReject: true,
		},
		{
			name:         "_sync_cookies",
			inputBody:    map[string]interface{}{"_sync_cookies": true},
			expectReject: true,
		},
		{
			name: "Valid user defined uppercase properties", // Uses internal properties names but in upper case
			// Known issue: _SYNC causes unmarshal error when not using xattrs
			inputBody: map[string]interface{}{
				"_ID": true, "_REV": true, "_DELETED": true, "_ATTACHMENTS": true, "_REVISIONS": true,
				"_EXP": true, "_PURGED": true, "_REMOVED": true, "_SYNC_COOKIES": true,
			},
			expectReject: false,
		},
	}

	// Setup
	rt := NewRestTester(t,
		&RestTesterConfig{
			GuestEnabled: true,
		})
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	// Track last sequence for next changes feed
	var changes ChangesResults
	changes.Last_Seq = "0"

	for i, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			docID := fmt.Sprintf("test%d", i)
			rawBody, err := json.Marshal(test.inputBody)
			require.NoError(t, err)

			_, err = client.PushRev(docID, "", rawBody)

			if test.expectReject {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Wait for rev to be received on RT
			err = rt.WaitForPendingChanges()
			require.NoError(t, err)
			changes, err = rt.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%s", changes.Last_Seq), "", true)
			require.NoError(t, err)

			var bucketDoc map[string]interface{}
			_, err = rt.GetSingleDataStore().Get(docID, &bucketDoc)
			assert.NoError(t, err)
			body := rt.GetDoc(docID)
			// Confirm input body is in the bucket doc
			if test.skipDocContentsVerification == nil || !*test.skipDocContentsVerification {
				for k, v := range test.inputBody {
					assert.Equal(t, v, bucketDoc[k])
					assert.Equal(t, v, body[k])
				}
			}
		})
	}
}

// CBG-2053: Test that the handleRev stats still increment correctly when going through the processRev function with
// the stat mapping (processRevStats)
func TestProcessRevIncrementsStat(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	activeRT, remoteRT, remoteURLString, teardown := SetupSGRPeers(t)
	defer teardown()
	activeCtx := activeRT.Context()

	remoteURL, _ := url.Parse(remoteURLString)

	stats, err := base.SyncGatewayStats.NewDBStats("test", false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar, err := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:                  t.Name(),
		Direction:           db.ActiveReplicatorTypePull,
		ActiveDB:            &db.Database{DatabaseContext: activeRT.GetDatabase()},
		RemoteDBURL:         remoteURL,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !activeRT.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)

	// Confirm all stats starting on 0
	require.NotNil(t, ar.Pull)
	pullStats := ar.Pull.GetStats()
	require.EqualValues(t, 0, pullStats.HandleRevCount.Value())
	require.EqualValues(t, 0, pullStats.HandleRevBytes.Value())
	require.EqualValues(t, 0, pullStats.HandlePutRevCount.Value())

	rev := remoteRT.CreateDoc(t, "doc")

	assert.NoError(t, ar.Start(activeCtx))
	defer func() { require.NoError(t, ar.Stop()) }()

	err = activeRT.WaitForPendingChanges()
	require.NoError(t, err)
	err = activeRT.WaitForRev("doc", rev)
	require.NoError(t, err)

	_, ok := base.WaitForStat(pullStats.HandleRevCount.Value, 1)
	require.True(t, ok)
	assert.NotEqualValues(t, 0, pullStats.HandleRevBytes.Value())
	// Confirm connected client count has not increased, which uses same processRev code
	assert.EqualValues(t, 0, pullStats.HandlePutRevCount.Value())
}

// Attempt to send rev as GUEST when read-only guest is enabled
func TestSendRevAsReadOnlyGuest(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		GuestEnabled:    true,
	})
	assert.NoError(t, err, "Error creating BlipTester")
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

			btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
			require.NoError(t, err)
			defer btc.Close()

			// Change noRev handler so it's known when a noRev is received
			recievedNoRevs := make(chan *blip.Message)
			btc.pullReplication.bt.blipContext.HandlerForProfile[db.MessageNoRev] = func(msg *blip.Message) {
				fmt.Println("Received noRev", msg.Properties)
				recievedNoRevs <- msg
			}

			resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docName, `{"foo":"bar"}`)
			RequireStatus(t, resp, http.StatusCreated)

			// Make the LeakyBucket return an error
			leakyDataStore.SetGetRawCallback(func(key string) error {
				return test.error
			})

			// Flush cache so document has to be retrieved from the leaky bucket
			rt.GetSingleTestDatabaseCollection().FlushRevisionCacheForTest()

			err = btc.StartPull()
			require.NoError(t, err)

			// Wait 3 seconds for noRev to be received
			select {
			case msg := <-recievedNoRevs:
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
			_, found := btc.GetRev(docName, RespRevID(t, resp))
			assert.False(t, found)
		})
	}
}

func TestUnsubChanges(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})

	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()
	// Confirm no error message or panic is returned in response
	response, err := btc.UnsubPullChanges()
	assert.NoError(t, err)
	assert.Empty(t, response)

	// Sub changes
	err = btc.StartPull()
	require.NoError(t, err)
	resp := rt.UpdateDoc("doc1", "", `{"key":"val1"}`)
	_, found := btc.WaitForRev("doc1", resp.Rev)
	require.True(t, found)

	activeReplStat := rt.GetDatabase().DbStats.CBLReplicationPull().NumPullReplActiveContinuous
	require.EqualValues(t, 1, activeReplStat.Value())

	// Unsub changes
	response, err = btc.UnsubPullChanges()
	assert.NoError(t, err)
	assert.Empty(t, response)
	// Wait for unsub changes to stop the sub changes being sent before sending document up
	activeReplVal, _ := base.WaitForStat(activeReplStat.Value, 0)
	assert.EqualValues(t, 0, activeReplVal)

	// Confirm no more changes are being sent
	resp = rt.UpdateDoc("doc2", "", `{"key":"val1"}`)
	err = rt.WaitForConditionWithOptions(func() bool {
		_, found = btc.GetRev("doc2", resp.Rev)
		return found
	}, 10, 100)
	assert.Error(t, err)

	// Confirm no error message is still returned when no subchanges active
	response, err = btc.UnsubPullChanges()
	assert.NoError(t, err)
	assert.Empty(t, response)

	// Confirm the pull replication can be restarted and it syncs doc2
	err = btc.StartPull()
	require.NoError(t, err)
	_, found = btc.WaitForRev("doc2", resp.Rev)
	assert.True(t, found)
}

// TestRequestPlusPull tests that a one-shot pull replication waits for pending changes when request plus is set on the replication.
func TestRequestPlusPull(t *testing.T) {

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
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	database := rt.GetDatabase()

	// Initialize blip tester client (will create user)
	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username: "bernard",
	})
	require.NoError(t, err)
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
	err = client.StartOneshotPullRequestPlus()
	assert.NoError(t, err)

	// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
	require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+1))

	// Release the slow sequence
	releaseErr := db.ReleaseTestSequence(database, slowSequence)
	require.NoError(t, releaseErr)

	// The one-shot pull should unblock and replicate the document in the granted channel
	data, ok := client.WaitForDoc("pbs-1")
	assert.True(t, ok)
	assert.Equal(t, `{"channel":["PBS"]}`, string(data))

}

// TestRequestPlusPull tests that a one-shot pull replication waits for pending changes when request plus is set on the db config.
func TestRequestPlusPullDbConfig(t *testing.T) {

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
				ChangesRequestPlus: base.BoolPtr(true),
			},
		},
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	database := rt.GetDatabase()

	// Initialize blip tester client (will create user)
	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username: "bernard",
	})
	require.NoError(t, err)
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
	err = client.StartOneshotPull()
	assert.NoError(t, err)

	// Wait for the one-shot changes feed to go into wait mode before releasing the slow sequence
	require.NoError(t, database.WaitForTotalCaughtUp(caughtUpStart+1))

	// Release the slow sequence
	releaseErr := db.ReleaseTestSequence(database, slowSequence)
	require.NoError(t, releaseErr)

	// The one-shot pull should unblock and replicate the document in the granted channel
	data, ok := client.WaitForDoc("pbs-1")
	assert.True(t, ok)
	assert.Equal(t, `{"channel":["PBS"]}`, string(data))

}
