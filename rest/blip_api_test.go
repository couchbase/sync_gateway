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
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
)

// This test performs the following steps against the Sync Gateway passive blip replicator:
//
// - Setup
//   - Create an httptest server listening on a port that wraps the Sync Gateway Admin Handler
//   - Make a BLIP/Websocket client connection to Sync Gateway
// - Test
//   - Verify Sync Gateway will accept the doc revision that is about to be sent
//   - Send the doc revision in a rev request
//   - Call changes endpoint and verify that it knows about the revision just sent
//   - Call subChanges api and make sure we get expected changes back
//
// Replication Spec: https://github.com/couchbase/couchbase-lite-core/wiki/Replication-Protocol#proposechanges
func TestBlipPushRevisionInspectChanges(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	bt, err := NewBlipTester(t)
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	// Verify Sync Gateway will accept the doc revision that is about to be sent
	var changeList [][]interface{}
	changesRequest := blip.NewRequest()
	changesRequest.SetProfile("changes")
	changesRequest.SetBody([]byte(`[["1", "foo", "1-abc", false]]`)) // [sequence, docID, revID]
	sent := bt.sender.Send(changesRequest)
	goassert.True(t, sent)
	changesResponse := changesRequest.Response()
	goassert.Equals(t, changesResponse.SerialNumber(), changesRequest.SerialNumber())
	body, err := changesResponse.Body()
	assert.NoError(t, err, "Error reading changes response body")
	err = base.JSONUnmarshal(body, &changeList)
	assert.NoError(t, err, "Error unmarshalling response body")
	goassert.Equals(t, len(changeList), 1) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow := changeList[0]
	goassert.Equals(t, len(changeRow), 0) // Should be empty, meaning the server is saying it doesn't have the revision yet

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
	changesRequest2 := blip.NewRequest()
	changesRequest2.SetProfile("changes")
	changesRequest2.SetBody([]byte(`[["2", "foo", "2-xyz", false]]`)) // [sequence, docID, revID]
	sent2 := bt.sender.Send(changesRequest2)
	goassert.True(t, sent2)
	changesResponse2 := changesRequest2.Response()
	goassert.Equals(t, changesResponse2.SerialNumber(), changesRequest2.SerialNumber())
	body2, err := changesResponse2.Body()
	assert.NoError(t, err, "Error reading changes response body")
	err = base.JSONUnmarshal(body2, &changeList2)
	assert.NoError(t, err, "Error unmarshalling response body")
	goassert.Equals(t, len(changeList2), 1) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow2 := changeList2[0]
	goassert.Equals(t, len(changeRow2), 1) // Should have 1 item in row, which is the rev id of the previous revision pushed
	goassert.Equals(t, changeRow2[0], "1-abc")

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
			goassert.Equals(t, len(changeListReceived), 1)
			change := changeListReceived[0] // [1,"foo","1-abc"]
			goassert.Equals(t, len(change), 3)
			goassert.Equals(t, change[0].(float64), float64(1)) // Expect sequence to be 1, since first item in DB
			goassert.Equals(t, change[1], "foo")                // Doc id of pushed rev
			goassert.Equals(t, change[2], "1-abc")              // Rev id of pushed rev

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
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	sent = bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	receivedChangesRequestWg.Add(1)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

	// Also expect the "changes" profile handler above to be called back again with an empty request that
	// will be ignored since body will be "null"
	receivedChangesRequestWg.Add(1)

	// Wait until we got the expected callback on the "changes" profile handler
	timeoutErr := WaitWithTimeout(&receivedChangesRequestWg, time.Second*5)
	assert.NoError(t, timeoutErr, "Timed out waiting")
}

// Start subChanges w/ continuous=true, batchsize=20
// Make several updates
// Wait until we get the expected updates
func TestContinuousChangesSubscription(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)()

	bt, err := NewBlipTester(t)
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receivedChangesWg := sync.WaitGroup{}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	lastReceivedSeq := float64(0)
	var numbatchesReceived int32
	nonIntegerSequenceReceived := false
	changeCount := 0
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		body, err := request.Body()
		log.Printf("got change with body %s, count %d", body, changeCount)
		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				goassert.Equals(t, len(change), 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					goassert.True(t, receivedSeq > lastReceivedSeq)
					lastReceivedSeq = receivedSeq
				} else {
					nonIntegerSequenceReceived = true
					log.Printf("Unexpected non-integer sequence received: %v", change[0])
				}

				// Verify doc id and rev id have expected vals
				docId := change[1].(string)
				goassert.True(t, strings.HasPrefix(docId, "foo"))
				goassert.Equals(t, change[2], "1-abc") // Rev id of pushed rev
				changeCount++
				receivedChangesWg.Done()
			}

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			// TODO: Sleeping here to avoid race in CBG-462, which appears to be occurring when there's very low latency
			// between the sendBatchOfChanges request and the response
			time.Sleep(10 * time.Millisecond)
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

	for i := 1; i < 1500; i++ {
		//// Add a change: Send an unsolicited doc revision in a rev request
		receivedChangesWg.Add(1)
		_, _, revResponse, err := bt.SendRev(
			fmt.Sprintf("foo-%d", i),
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		assert.NoError(t, err)

		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")

	}

	// Wait until all expected changes are received by change handler
	// receivedChangesWg.Wait()
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*30)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")

	// Since batch size was set to 10, and 15 docs were added, expect at _least_ 2 batches
	numBatchesReceivedSnapshot := atomic.LoadInt32(&numbatchesReceived)
	goassert.True(t, numBatchesReceivedSnapshot >= 2)

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")

}

// Make several updates
// Start subChanges w/ continuous=false, batchsize=20
// Validate we get the expected updates and changes ends
func TestBlipOneShotChangesSubscription(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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

		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				goassert.Equals(t, len(change), 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					goassert.True(t, receivedSeq > lastReceivedSeq)
					lastReceivedSeq = receivedSeq
				} else {
					nonIntegerSequenceReceived = true
					log.Printf("Unexpected non-integer sequence received: %v", change[0])
				}

				// Verify doc id and rev id have expected vals
				docId := change[1].(string)
				goassert.True(t, strings.HasPrefix(docId, "preOneShot"))
				goassert.Equals(t, change[2], "1-abc") // Rev id of pushed rev
				docIdsReceived[docId] = true
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
	for docID, _ := range docIdsReceived {
		//// Add a change: Send an unsolicited doc revision in a rev request
		_, _, revResponse, err := bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		assert.NoError(t, err)
		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")
		receivedChangesWg.Add(1)
	}

	// Wait for documents to be processed and available for changes
	cacheWaiter.Wait()

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

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
		//// Add a change: Send an unsolicited doc revision in a rev request
		_, _, revResponse, err := bt.SendRev(
			fmt.Sprintf("postOneShot_%d", i),
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		assert.NoError(t, err)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	bt, err := NewBlipTester(t)
	assert.NoError(t, err, "Error creating BlipTester")
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

		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = base.JSONUnmarshal(body, &changeListReceived)
			assert.NoError(t, err, "Error unmarshalling changes received")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				goassert.Equals(t, len(change), 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					goassert.True(t, receivedSeq > lastReceivedSeq)
					lastReceivedSeq = receivedSeq
				} else {
					nonIntegerSequenceReceived = true
					log.Printf("Unexpected non-integer sequence received: %v", change[0])
				}

				// Verify doc id and rev id have expected vals
				docId := change[1].(string)
				goassert.True(t, strings.HasPrefix(docId, "docIDFiltered"))
				goassert.Equals(t, change[2], "1-abc") // Rev id of pushed rev
				log.Printf("Changes got docID: %s", docId)

				// Ensure we only receive expected docs
				_, isExpected := docIDsReceived[docId]
				if !isExpected {
					t.Errorf(fmt.Sprintf("Received unexpected docId: %s", docId))
				} else {
					// Add to received set, to ensure we get all expected docs
					docIDsReceived[docId] = true
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
		//// Add a change: Send an unsolicited doc revision in a rev request
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
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 5 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)

	body := db.SubChangesBody{DocIDs: docIDsExpected}
	bodyBytes, err := base.JSONMarshal(body)
	assert.NoError(t, err, "Error marshalling subChanges body.")

	subChangesRequest.SetBody(bodyBytes)

	sent := bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

	// Wait until all expected changes are received by change handler
	// receivedChangesWg.Wait()
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*15)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")

	// Since batch size was set to 10, and 15 docs were added, expect at _least_ 2 batches
	numBatchesReceivedSnapshot := atomic.LoadInt32(&numbatchesReceived)
	goassert.True(t, numBatchesReceivedSnapshot >= 2)

	// Validate all expected documents were received.
	for docID, received := range docIDsReceived {
		if !received {
			t.Errorf("Did not receive expected doc %s in changes", docID)
		}
	}

	// Validate that the 'caught up' message was sent
	goassert.True(t, receivedCaughtUpChange)

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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode: true,
		guestEnabled:    true,
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	proposeChangesRequest := blip.NewRequest()
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
	goassert.True(t, sent)
	proposeChangesResponse := proposeChangesRequest.Response()
	body, err := proposeChangesResponse.Body()
	assert.NoError(t, err, "Error getting changes response body")

	var changeList [][]interface{}
	err = base.JSONUnmarshal(body, &changeList)
	assert.NoError(t, err, "Error getting changes response body")

	// The common case of an empty array response tells the sender to send all of the proposed revisions,
	// so the changeList returned by Sync Gateway is expected to be empty
	goassert.Equals(t, len(changeList), 0)

}

// Connect to public port with authentication
func TestPublicPortAuthentication(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create bliptester that is connected as user1, with access to the user1 channel
	btUser1, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Error creating BlipTester")
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
	goassert.Equals(t, len(changesChannelUser1), 1)
	change := changesChannelUser1[0]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo", revId: "1-abc", sequence: "*", deleted: base.BoolPtr(false)})

	// Assert that user2 received user1's change as well as it's own change
	changesChannelUser2 := btUser2.WaitForNumChanges(2)
	goassert.Equals(t, len(changesChannelUser2), 2)
	change = changesChannelUser2[0]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo", revId: "1-abc", sequence: "*", deleted: base.BoolPtr(false)})

	change = changesChannelUser2[1]
	AssertChangeEquals(t, change, ExpectedChange{docId: "foo2", revId: "1-abcd", sequence: "*", deleted: base.BoolPtr(false)})

}

// Connect to public port with authentication, and validate user update during a replication
func TestPublicPortAuthenticationUserUpdate(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Initialize restTester here, so that we can use custom sync function, and later modify user
	syncFunction := `
function(doc, oldDoc) {
  requireAccess("ABC")
}

`
	rtConfig := RestTesterConfig{SyncFn: syncFunction}
	var rt = NewRestTester(t, &rtConfig)
	defer rt.Close()

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
	user1, err := dbc.Authenticator().GetUser("user1")
	require.NoError(t, err)

	userDb, err := db.GetDatabase(dbc, user1)
	require.NoError(t, err)

	userWaiter := userDb.NewUserWaiter()

	// Update the user to grant them access to ABC
	response := rt.SendAdminRequest("PUT", "/db/_user/user1", `{"admin_channels":["ABC"]}`)
	assertStatus(t, response, 200)

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
	getResponse := rt.SendAdminRequest("GET", "/db/foo", "")
	assertStatus(t, getResponse, 200)

}

// Start subChanges w/ continuous=true, batchsize=20
// Write a doc that grants access to itself for the active replication's user
func TestContinuousChangesDynamicGrant(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)()
	// Initialize restTester here, so that we can use custom sync function, and later modify user
	syncFunction := `
function(doc, oldDoc) {
  access(doc.accessUser, doc.accessChannel)
  channel(doc.channels)
}

`

	rtConfig := RestTesterConfig{SyncFn: syncFunction}
	var rt = NewRestTester(t, &rtConfig)
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
				goassert.Equals(t, len(change), 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					goassert.True(t, receivedSeq > lastReceivedSeq)
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
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

	// Write a doc that grants user1 access to channel ABC, and doc is also in channel ABC
	receivedChangesWg.Add(1)
	revsFinishedWg.Add(1)
	response := rt.SendAdminRequest("PUT", "/db/grantDoc", `{"accessUser":"user1", "accessChannel":"ABC", "channels":["ABC"]}`)
	assertStatus(t, response, 201)

	// Wait until all expected changes are received by change handler
	// receivedChangesWg.Wait()
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*5)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")

	revTimeoutErr := WaitWithTimeout(&revsFinishedWg, time.Second*5)
	assert.NoError(t, revTimeoutErr, "Timed out waiting for all revs.")

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")

}

// Start subChanges w/ continuous=true, batchsize=20
// Start goroutine sending rev messages for documents that grant access to themselves for the active replication's user

func TestConcurrentRefreshUser(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCache)()
	// Initialize restTester here, so that we can use custom sync function, and later modify user
	syncFunction := `
function(doc, oldDoc) {
  access(doc.accessUser, doc.accessChannel)
  channel(doc.channels)
}

`
	rtConfig := RestTesterConfig{SyncFn: syncFunction}
	var rt = NewRestTester(t, &rtConfig)
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
				goassert.Equals(t, len(change), 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq, ok := change[0].(float64)
				if ok {
					goassert.True(t, receivedSeq > lastReceivedSeq)
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

		var doc RestDocument
		err = base.JSONUnmarshal(body, &doc)
		if err != nil {
			panic(fmt.Sprintf("Unexpected err: %v", err))
		}
		_, isRemoved := doc[db.BodyRemoved]
		assert.False(t, isRemoved, fmt.Sprintf("Document %v shouldn't be removed", request.Properties[db.RevMessageId]))

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

	// Start goroutine to simulate sending docs from the client

	receivedChangesWg.Add(100)
	revsFinishedWg.Add(100)
	go func() {
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
			assert.NoError(t, sendErr)
		}
	}()

	// Wait until all expected changes are received by change handler
	// receivedChangesWg.Wait()
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*60)
	assert.NoError(t, timeoutErr, "Timed out waiting for all changes.")

	revTimeoutErr := WaitWithTimeout(&revsFinishedWg, time.Second*60)
	assert.NoError(t, revTimeoutErr, "Timed out waiting for all revs.")

	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")

}

// Test send and retrieval of a doc.
//   Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetRev(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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
	goassert.True(t, sent)
	assert.NoError(t, err)
	goassert.Equals(t, resp.Properties["Error-Code"], "")

	// Get non-deleted rev
	response := bt.restTester.SendAdminRequest("GET", "/db/sendAndGetRev?rev=1-abc", "")
	assertStatus(t, response, 200)
	var responseBody RestDocument
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	_, ok := responseBody[db.BodyDeleted]
	goassert.False(t, ok)

	// Tombstone the document
	history := []string{"1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("sendAndGetRev", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{"deleted": "true"})
	goassert.True(t, sent)
	assert.NoError(t, err)
	goassert.Equals(t, resp.Properties["Error-Code"], "")

	// Get the tombstoned document
	response = bt.restTester.SendAdminRequest("GET", "/db/sendAndGetRev?rev=2-bcd", "")
	assertStatus(t, response, 200)
	responseBody = RestDocument{}
	assert.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	deletedValue, deletedOK := responseBody[db.BodyDeleted].(bool)
	goassert.True(t, deletedOK)
	goassert.True(t, deletedValue)
}

// Test send and retrieval of a doc with a large numeric value.  Ensure proper large number handling.
//   Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetLargeNumberRev(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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
	goassert.True(t, sent)
	assert.NoError(t, err)
	goassert.Equals(t, resp.Properties["Error-Code"], "")

	// Get non-deleted rev
	response := bt.restTester.SendAdminRequest("GET", "/db/largeNumberRev?rev=1-abc", "")
	assertStatus(t, response, 200) // Check the raw bytes, because unmarshalling the response would be another opportunity for the number to get modified
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

// Test adding / retrieving attachments
func TestAttachments(t *testing.T) {
	// TODO: Write tests to cover scenario
	t.Skip("not tested")
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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
	goassert.True(t, sent)
	assert.NoError(t, err)
	goassert.Equals(t, resp.Properties["Error-Code"], "")

	checkpointRev := resp.Rev()
	goassert.Equals(t, checkpointRev, "0-1")

	// Validate checkpoint existence in bucket (local file name "/" needs to be URL encoded as %252F)
	response := rt.SendAdminRequest("GET", "/db/_local/checkpoint%252Ftestclient", "")
	assertStatus(t, response, 200)
	var responseBody map[string]interface{}
	err = base.JSONUnmarshal(response.Body.Bytes(), &responseBody)
	goassert.Equals(t, responseBody["client_seq"], "1000")

	// Attempt to update the checkpoint with previous rev
	checkpointBody = []byte(`{"client_seq":"1005"}`)
	sent, _, resp, err = bt.SetCheckpoint("testclient", checkpointRev, checkpointBody)
	goassert.True(t, sent)
	assert.NoError(t, err)
	goassert.Equals(t, resp.Properties["Error-Code"], "")
	checkpointRev = resp.Rev()
	goassert.Equals(t, checkpointRev, "0-2")
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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
	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()
	bt, err := NewBlipTesterFromSpecWithRT(t, &BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	}, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Set up a ChangeWaiter for this test, to block until the user change notification happens
	dbc := rt.GetDatabase()
	user1, err := dbc.Authenticator().GetUser("user1")
	require.NoError(t, err)

	userDb, err := db.GetDatabase(dbc, user1)
	require.NoError(t, err)

	userWaiter := userDb.NewUserWaiter()

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/db/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

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
	goassert.False(t, hasErrorCode)
	if hasErrorCode {
		t.Fatalf("Unexpected error sending revision.  Error code: %v.  Response body: %s", errorCode, addRevResponseBody)
	}

}

// Grant a user access to a channel via the Sync Function and a doc change, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaSyncFunction(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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
	_, _, _, _ = bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/db/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Add another doc in the PBS channel
	_, _, _, _ = bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Make sure we can see it by getting changes
	changes := bt.WaitForNumChanges(2)
	log.Printf("changes: %+v", changes)
	goassert.Equals(t, len(changes), 2)

}

// Grant a user access to a channel via the REST Admin API, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaAdminApi(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Add a doc in the PBS channel
	_, _, _, _ = bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Update the user doc to grant access to PBS
	response := bt.restTester.SendAdminRequest("PUT", "/db/_user/user1", `{"admin_channels":["user1", "PBS"]}`)
	assertStatus(t, response, 200)

	// Add another doc in the PBS channel
	_, _, _, _ = bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Make sure we can see both docs in the changes
	changes := bt.WaitForNumChanges(2)
	goassert.Equals(t, len(changes), 2)

}

func TestCheckpoint(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	client := "testClient"

	// Get the checkpoint -- expect to be missing at this point
	request := blip.NewRequest()
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
	goassert.True(t, ok)
	goassert.Equals(t, errorcode, "404")

	// Set a checkpoint
	requestSetCheckpoint := blip.NewRequest()
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
	requestGetCheckpoint2 := blip.NewRequest()
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
	goassert.True(t, strings.Contains(string(body), "Key"))
	goassert.True(t, strings.Contains(string(body), "Value"))

}

// Test Attachment replication behavior described here: https://github.com/couchbase/couchbase-lite-core/wiki/Replication-Protocol
// - Put attachment via blip
// - Verifies that getAttachment won't return attachment "out of context" of a rev request
// - Get attachment via REST and verifies it returns the correct content
func TestPutAttachmentViaBlipGetViaRest(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	attachmentBody := "attach"
	digest := db.Sha1DigestKey([]byte(attachmentBody))

	input := SendRevWithAttachmentInput{
		docId:            "doc",
		revId:            "1-rev1",
		attachmentName:   "myAttachment",
		attachmentLength: len(attachmentBody),
		attachmentBody:   attachmentBody,
		attachmentDigest: digest,
	}
	bt.SendRevWithAttachment(input)

	// Try to fetch the attachment directly via getAttachment, expected to fail w/ 403 error for security reasons
	// since it's not in the context of responding to a "rev" request from the peer.
	getAttachmentRequest := blip.NewRequest()
	getAttachmentRequest.SetProfile(db.MessageGetAttachment)
	getAttachmentRequest.Properties[db.GetAttachmentDigest] = input.attachmentDigest
	getAttachmentRequest.Properties[db.GetAttachmentID] = input.docId
	sent := bt.sender.Send(getAttachmentRequest)
	if !sent {
		panic(fmt.Sprintf("Failed to send request for doc: %v", input.docId))
	}
	getAttachmentResponse := getAttachmentRequest.Response()
	errorCode, hasErrorCode := getAttachmentResponse.Properties["Error-Code"]
	goassert.Equals(t, errorCode, "403") // "Attachment's doc not being synced"
	goassert.True(t, hasErrorCode)

	// Get the attachment via REST api and make sure it matches the attachment pushed earlier
	response := bt.restTester.SendAdminRequest("GET", fmt.Sprintf("/db/%s/%s", input.docId, input.attachmentName), ``)
	goassert.Equals(t, response.Body.String(), input.attachmentBody)

}

func TestPutAttachmentViaBlipGetViaBlip(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername:          "user1",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // All channels
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	attachmentBody := "attach"
	digest := db.Sha1DigestKey([]byte(attachmentBody))

	// Send revision with attachment
	input := SendRevWithAttachmentInput{
		docId:            "doc",
		revId:            "1-rev1",
		attachmentName:   "myAttachment",
		attachmentLength: len(attachmentBody),
		attachmentBody:   attachmentBody,
		attachmentDigest: digest,
	}
	sent, _, _ := bt.SendRevWithAttachment(input)
	goassert.True(t, sent)

	// Get all docs and attachment via subChanges request
	allDocs, ok := bt.WaitForNumDocsViaChanges(1)
	require.True(t, ok)

	// make assertions on allDocs -- make sure attachment is present w/ expected body
	require.Len(t, allDocs, 1)
	retrievedDoc := allDocs[input.docId]

	// doc assertions
	goassert.Equals(t, retrievedDoc.ID(), input.docId)
	goassert.Equals(t, retrievedDoc.RevID(), input.revId)

	// attachment assertions
	attachments, err := retrievedDoc.GetAttachments()
	goassert.True(t, err == nil)
	goassert.Equals(t, len(attachments), 1)
	retrievedAttachment := attachments[input.attachmentName]
	require.NotNil(t, retrievedAttachment)
	goassert.Equals(t, string(retrievedAttachment.Data), input.attachmentBody)
	goassert.Equals(t, retrievedAttachment.Length, len(attachmentBody))
	goassert.Equals(t, input.attachmentDigest, retrievedAttachment.Digest)

}

// Reproduces the issue seen in https://github.com/couchbase/couchbase-lite-core/issues/790
// Makes sure that Sync Gateway rejects attachments sent to it that does not match the given digest and/or length
func TestPutInvalidAttachment(t *testing.T) {

	tests := []struct {
		name                  string
		correctAttachmentBody string
		invalidAttachmentBody string
		expectedType          blip.MessageType
		expectedErrorCode     string
	}{
		{
			name:                  "truncated",
			correctAttachmentBody: "attach",
			invalidAttachmentBody: "att", // truncate so length and digest are incorrect
			expectedType:          blip.ErrorType,
			expectedErrorCode:     strconv.Itoa(http.StatusBadRequest),
		},
		{
			name:                  "malformed",
			correctAttachmentBody: "attach",
			invalidAttachmentBody: "attahc", // swap two chars so only digest doesn't match
			expectedType:          blip.ErrorType,
			expectedErrorCode:     strconv.Itoa(http.StatusBadRequest),
		},
		{
			name:                  "correct",
			correctAttachmentBody: "attach",
			invalidAttachmentBody: "attach",
			expectedType:          blip.ResponseType,
			expectedErrorCode:     "",
		},
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		connectingUsername:          "user1",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // All channels
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			digest := db.Sha1DigestKey([]byte(test.correctAttachmentBody))

			// Send revision with attachment
			input := SendRevWithAttachmentInput{
				docId:            test.name,
				revId:            "1-rev1",
				attachmentName:   "myAttachment",
				attachmentLength: len(test.correctAttachmentBody),
				attachmentBody:   test.invalidAttachmentBody,
				attachmentDigest: digest,
			}
			sent, _, resp := bt.SendRevWithAttachment(input)
			assert.True(t, sent)

			// Make sure we get the expected response back
			assert.Equal(t, test.expectedType, resp.Type())
			if test.expectedErrorCode != "" {
				assert.Equal(t, test.expectedErrorCode, resp.Properties["Error-Code"])
			}

			respBody, err := resp.Body()
			assert.NoError(t, err)
			t.Logf("resp.Body: %v", string(respBody))
		})
	}
}

// Put a revision that is rejected by the sync function and assert that Sync Gateway
// returns an error code
func TestPutInvalidRevSyncFnReject(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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
	revRequest := blip.NewRequest()
	revRequest.SetCompressed(false)
	revRequest.SetProfile("rev")
	revRequest.Properties["id"] = "foo"
	revRequest.Properties["rev"] = "1-aaa"
	revRequest.Properties["deleted"] = "false"
	revRequest.SetBody([]byte(`{"key": "val", "channels": ["CNN"]}`))
	sent := bt.sender.Send(revRequest)
	goassert.True(t, sent)

	revResponse := revRequest.Response()

	// Since doc is rejected by sync function, expect a 403 error
	errorCode, hasErrorCode := revResponse.Properties["Error-Code"]
	goassert.True(t, hasErrorCode)
	goassert.Equals(t, errorCode, "403")

	// Make sure that a one-off GetChanges() returns no documents
	changes := bt.GetChanges()
	goassert.Equals(t, len(changes), 0)

}

func TestPutInvalidRevMalformedBody(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

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
	goassert.True(t, sent)

	revResponse := revRequest.Response()

	// Since doc is rejected by sync function, expect a 403 error
	errorCode, hasErrorCode := revResponse.Properties["Error-Code"]
	goassert.True(t, hasErrorCode)
	// FIXME: This used to check for error 500 -- No longer does readJSON in handleRev so fails at different point
	goassert.Equals(t, errorCode, "400")

	// Make sure that a one-off GetChanges() returns no documents
	changes := bt.GetChanges()
	goassert.Equals(t, len(changes), 0)

}

func TestPutRevNoConflictsMode(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode:    true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})
	goassert.True(t, sent)
	assert.NoError(t, err)                                // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	sent, _, resp, err = bt.SendRev("foo", "1-def", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "true"})
	goassert.True(t, sent)
	goassert.NotEquals(t, err, nil)                          // conflict error
	goassert.Equals(t, resp.Properties["Error-Code"], "409") // conflict

	sent, _, resp, err = bt.SendRev("foo", "1-ghi", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "false"})
	goassert.True(t, sent)
	goassert.NotEquals(t, err, nil)                          // conflict error
	goassert.Equals(t, resp.Properties["Error-Code"], "409") // conflict

}

func TestPutRevConflictsMode(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(t, BlipTesterSpec{
		noConflictsMode:    false,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})
	goassert.True(t, sent)
	assert.NoError(t, err)                                // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	sent, _, resp, err = bt.SendRev("foo", "1-def", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "false"})
	goassert.True(t, sent)
	assert.NoError(t, err)                                // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	sent, _, resp, err = bt.SendRev("foo", "1-ghi", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "true"})
	goassert.True(t, sent)
	goassert.NotEquals(t, err, nil)                          // conflict error
	goassert.Equals(t, resp.Properties["Error-Code"], "409") // conflict

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
//
func TestGetRemovedDoc(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	rt := NewRestTester(t, nil)
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
	assert.NoError(t, err)                         // no error
	assert.Empty(t, resp.Properties["Error-Code"]) // no error

	// Add rev-2 in channel user1
	history := []string{"1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("foo", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{"noconflicts": "true"})
	assert.True(t, sent)
	assert.NoError(t, err)                         // no error
	assert.Empty(t, resp.Properties["Error-Code"]) // no error

	require.NoError(t, rt.GetDatabase().WaitForPendingChanges(context.Background()))

	// Try to get rev 2 via BLIP API and assert that _removed == false
	resultDoc, err := bt.GetDocAtRev("foo", "2-bcd")
	assert.NoError(t, err, "Unexpected Error")
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

	require.NoError(t, rt.GetDatabase().WaitForPendingChanges(context.Background()))

	// Flush rev cache in case this prevents the bug from showing up (didn't make a difference)
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Delete any temp revisions in case this prevents the bug from showing up (didn't make a difference)
	tempRevisionDocId := base.RevPrefix + "foo:5:3-cde"
	err = rt.GetDatabase().Bucket.Delete(tempRevisionDocId)
	assert.NoError(t, err, "Unexpected Error")

	// Try to get rev 3 via BLIP API and assert that _removed == true
	resultDoc, err = bt2.GetDocAtRev("foo", "3-cde")
	assert.NoError(t, err, "Unexpected Error")
	assert.True(t, resultDoc.IsRemoved())

	// Try to get rev 3 via REST API, and assert that _removed == true
	headers := map[string]string{}
	headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(btSpec.connectingUsername+":"+btSpec.connectingPassword))
	response := rt.SendRequestWithHeaders("GET", "/db/foo?rev=3-cde", "", headers)
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
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()
	bt, err := NewBlipTesterFromSpecWithRT(t, nil, rt)
	require.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	require.NoError(t, rt.WaitForDBOnline())

	// Create 5 docs
	for i := 0; i < 5; i++ {
		docId := fmt.Sprintf("doc-%d", i)
		docRev := fmt.Sprintf("1-abc%d", i)
		sent, _, resp, err := bt.SendRev(docId, docRev, []byte(`{"key": "val", "channels": ["ABC"]}`), blip.Properties{})
		goassert.True(t, sent)
		log.Printf("resp: %v, err: %v", resp, err)
	}

	// Get a reference to the database
	targetDbContext, err := rt.ServerContext().GetDatabase("db")
	assert.NoError(t, err, "failed")
	targetDb, err := db.GetDatabase(targetDbContext, nil)
	assert.NoError(t, err, "failed")

	// Pull docs, expect to pull 5 docs since none of them has purged yet.
	docs, ok := bt.WaitForNumDocsViaChanges(5)
	assert.Len(t, docs, 5)

	// Purge one doc
	doc0Id := fmt.Sprintf("doc-%d", 0)
	err = targetDb.Purge(doc0Id)
	assert.NoError(t, err, "failed")

	// Flush rev cache
	targetDb.FlushRevisionCacheForTest()

	// Pull docs, expect to pull 4 since one was purged.  (also expect to NOT get stuck)
	docs, ok = bt.WaitForNumDocsViaChanges(4)
	assert.True(t, ok)
	assert.Len(t, docs, 4)

}

// TestBlipDeltaSyncPull tests that a simple pull replication uses deltas in EE,
// and checks that full body replication still happens in CE.
func TestBlipDeltaSyncPull(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		},
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	var deltaSentCount int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaSentCount = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 12345678901234567890}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(data))

	msg, ok := client.WaitForBlipRevMessage("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)

	// Check EE is delta, and CE is full-body replication
	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
		assert.Equal(t, deltaSentCount+1, rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value())
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(msgBody))

		var afterDeltaSyncCount int64
		if rt.GetDatabase().DbStats.DeltaSync() != nil {
			afterDeltaSyncCount = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
		}

		assert.Equal(t, deltaSentCount, afterDeltaSyncCount)
	}
}

// TestBlipDeltaSyncPullResend tests that a simple pull replication that uses a delta a client rejects will resend the revision in full.
func TestBlipDeltaSyncPullResend(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skip("Enterprise-only test for delta sync")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: base.BoolPtr(true),
			},
		},
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	// create doc1 rev 1
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)
	rev1ID := respRevID(t, resp)

	deltaSentCount := rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	// reject deltas built ontop of rev 1
	client.rejectDeltasForSrcRev = rev1ID

	client.ClientDeltas = true
	err = client.StartPull()
	assert.NoError(t, err)

	data, ok := client.WaitForRev("doc1", rev1ID)
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev="+rev1ID, `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 12345678901234567890}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)
	rev2ID := respRevID(t, resp)

	data, ok = client.WaitForRev("doc1", rev2ID)
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)

	// Check the request was initially sent with the correct deltaSrc property
	assert.Equal(t, rev1ID, msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
	assert.Equal(t, deltaSentCount+1, rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value())

	msg, ok = client.WaitForBlipRevMessage("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)

	// Check the resent request was NOT sent with a deltaSrc property
	assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was NOT the delta
	msgBody, err = msg.Body()
	assert.NoError(t, err)
	assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":12345678901234567890}]}}`, string(msgBody))
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(msgBody))
}

// TestBlipDeltaSyncPullRemoved tests a simple pull replication that drops a document out of the user's channel.
func TestBlipDeltaSyncPullRemoved(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:               "alice",
		Channels:               []string{"public"},
		ClientDeltas:           true,
		SupportedBLIPProtocols: []string{db.BlipCBMobileReplicationV2},
	})
	require.NoError(t, err)
	defer client.Close()

	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-1513b53e2738671e634d9dd111f48de0
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// create doc1 rev 2-ff91e11bc1fd12bbb4815a06571859a9
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-1513b53e2738671e634d9dd111f48de0", `{"channels": ["private"], "greetings": [{"hello": "world!"}, {"hi": "bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-ff91e11bc1fd12bbb4815a06571859a9")
	assert.True(t, ok)
	assert.Equal(t, `{"_removed":true}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"_removed":true}`, string(msgBody))
}

// TestBlipDeltaSyncPullTombstoned tests a simple pull replication that deletes a document.
//
// Sync Gateway: creates rev-1 and then tombstones it in rev-2
// Client:       continuously pulls, pulling rev-1 as normal, and then rev-2 which should be a tombstone, even though a delta was requested
// ┌──────────────┐ ┌─────────┐            ┌─────────┐
// │ Sync Gateway ├─┤ + rev-1 ├────────────┤ - rev-2 ├────■
// └──────────────┘ └─────────┤            └─────────┤
// ┌──────────────┐ ┌─────────┼──────────────────────┼──┐
// │     Client 1 ├─┤         ▼      continuous      ▼  ├─■
// └──────────────┘ └───────────────────────────────────┘
func TestBlipDeltaSyncPullTombstoned(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	var deltaCacheHitsStart int64
	var deltaCacheMissesStart int64
	var deltasRequestedStart int64
	var deltasSentStart int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedStart = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentStart = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	client, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:     "alice",
		Channels:     []string{"public"},
		ClientDeltas: true,
	})
	require.NoError(t, err)
	defer client.Close()

	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-e89945d756a1d444fa212bffbbb31941
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// tombstone doc1 at rev 2-2db70833630b396ef98a3ec75b3e90fc
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/doc1?rev=1-1513b53e2738671e634d9dd111f48de0", "")
	assert.Equal(t, http.StatusOK, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{}`, string(msgBody))
	assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted])

	var deltaCacheHitsEnd int64
	var deltaCacheMissesEnd int64
	var deltasRequestedEnd int64
	var deltasSentEnd int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	if sgUseDeltas {
		assert.Equal(t, deltaCacheHitsStart, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart+1, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart+1, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart, deltasSentEnd) // "_removed" docs are not counted as a delta
	} else {
		assert.Equal(t, deltaCacheHitsStart, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart, deltasSentEnd)
	}
}

// TestBlipDeltaSyncPullTombstonedStarChan tests two clients can perform a simple pull replication that deletes a document when the user has access to the star channel.
//
// Sync Gateway: creates rev-1 and then tombstones it in rev-2
// Client 1:     continuously pulls, and causes the tombstone delta for rev-2 to be cached
// Client 2:     runs two one-shots, once initially to pull rev-1, and finally for rev-2 after the tombstone delta has been cached
// ┌──────────────┐ ┌─────────┐            ┌─────────┐
// │ Sync Gateway ├─┤ + rev-1 ├─┬──────────┤ - rev-2 ├─┬───────────■
// └──────────────┘ └─────────┤ │          └─────────┤ │
// ┌──────────────┐ ┌─────────┼─┼────────────────────┼─┼─────────┐
// │     Client 1 ├─┤         ▼ │    continuous      ▼ │         ├─■
// └──────────────┘ └───────────┼──────────────────────┼─────────┘
// ┌──────────────┐           ┌─┼─────────┐          ┌─┼─────────┐
// │     Client 2 ├───────────┤ ▼ oneshot ├──────────┤ ▼ oneshot ├─■
// └──────────────┘           └───────────┘          └───────────┘
func TestBlipDeltaSyncPullTombstonedStarChan(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyHTTP, base.KeyCache, base.KeySync, base.KeySyncMsg)()

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	var deltaCacheHitsStart int64
	var deltaCacheMissesStart int64
	var deltasRequestedStart int64
	var deltasSentStart int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesStart = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedStart = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentStart = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	client1, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:     "client1",
		Channels:     []string{"*"},
		ClientDeltas: true,
	})
	require.NoError(t, err)
	defer client1.Close()

	client2, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:     "client2",
		Channels:     []string{"*"},
		ClientDeltas: true,
	})
	require.NoError(t, err)
	defer client2.Close()

	err = client1.StartPull()
	require.NoError(t, err)

	// create doc1 rev 1-e89945d756a1d444fa212bffbbb31941
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"channels": ["public"], "greetings": [{"hello": "world!"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client1.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// Have client2 get only rev-1 and then stop replicating
	err = client2.StartOneshotPull()
	assert.NoError(t, err)
	data, ok = client2.WaitForRev("doc1", "1-1513b53e2738671e634d9dd111f48de0")
	assert.True(t, ok)
	assert.Contains(t, string(data), `"channels":["public"]`)
	assert.Contains(t, string(data), `"greetings":[{"hello":"world!"}]`)

	// tombstone doc1 at rev 2-2db70833630b396ef98a3ec75b3e90fc
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/doc1?rev=1-1513b53e2738671e634d9dd111f48de0", `{"test": true"`)
	assert.Equal(t, http.StatusOK, resp.Code)

	data, ok = client1.WaitForRev("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	msg, ok := client1.WaitForBlipRevMessage("doc1", "2-ed278cbc310c9abeea414da15d0b2cac") // docid, revid to get the message
	assert.True(t, ok)
	if !assert.Equal(t, db.MessageRev, msg.Profile()) {
		t.Logf("unexpected profile for message %v in %v",
			msg.SerialNumber(), client1.pullReplication.GetMessages())
	}
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	if !assert.Equal(t, `{}`, string(msgBody)) {
		t.Logf("unexpected body for message %v in %v",
			msg.SerialNumber(), client1.pullReplication.GetMessages())
	}
	if !assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted]) {
		t.Logf("unexpected deleted property for message %v in %v",
			msg.SerialNumber(), client1.pullReplication.GetMessages())
	}

	// Sync Gateway will have cached the tombstone delta, so client 2 should be able to retrieve it from the cache
	err = client2.StartOneshotPull()
	assert.NoError(t, err)

	data, ok = client2.WaitForRev("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	msg, ok = client2.WaitForBlipRevMessage("doc1", "2-ed278cbc310c9abeea414da15d0b2cac")
	assert.True(t, ok)
	if !assert.Equal(t, db.MessageRev, msg.Profile()) {
		t.Logf("unexpected profile for message %v in %v",
			msg.SerialNumber(), client2.pullReplication.GetMessages())
	}
	msgBody, err = msg.Body()
	assert.NoError(t, err)
	if !assert.Equal(t, `{}`, string(msgBody)) {
		t.Logf("unexpected body for message %v in %v",
			msg.SerialNumber(), client2.pullReplication.GetMessages())
	}
	if !assert.Equal(t, "1", msg.Properties[db.RevMessageDeleted]) {
		t.Logf("unexpected deleted property for message %v in %v",
			msg.SerialNumber(), client2.pullReplication.GetMessages())
	}

	var deltaCacheHitsEnd int64
	var deltaCacheMissesEnd int64
	var deltasRequestedEnd int64
	var deltasSentEnd int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaCacheHitsEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
		deltaCacheMissesEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()
		deltasRequestedEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasRequested.Value()
		deltasSentEnd = rt.GetDatabase().DbStats.DeltaSync().DeltasSent.Value()
	}

	if sgUseDeltas {
		assert.Equal(t, deltaCacheHitsStart+1, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart+1, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart+2, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart+2, deltasSentEnd)
	} else {
		assert.Equal(t, deltaCacheHitsStart, deltaCacheHitsEnd)
		assert.Equal(t, deltaCacheMissesStart, deltaCacheMissesEnd)
		assert.Equal(t, deltasRequestedStart, deltasRequestedEnd)
		assert.Equal(t, deltasSentStart, deltasSentEnd)
	}
}

// TestBlipPullRevMessageHistory tests that a simple pull replication contains history in the rev message.
func TestBlipPullRevMessageHistory(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		},
		guestEnabled: true,
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
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": 12345678901234567890}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-26359894b20d89c97638e71c40482f28")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":12345678901234567890}]}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageHistory])
}

// TestBlipDeltaSyncPullRevCache tests that a simple pull replication uses deltas in EE,
// Second pull validates use of rev cache for previously generated deltas.
func TestBlipDeltaSyncPullRevCache(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skipf("Skipping enterprise-only delta sync test.")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		},
		guestEnabled: true,
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
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// Perform a one-shot pull as client 2 to pull down the first revision

	client2, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client2.Close()

	client2.ClientDeltas = true
	err = client2.StartOneshotPull()
	assert.NoError(t, err)

	data, ok = client2.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": "bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(data))

	msg, ok := client.WaitForBlipRevMessage("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)

	// Check EE is delta
	// Check the request was sent with the correct deltaSrc property
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

	deltaCacheHits := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
	deltaCacheMisses := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()

	// Run another one shot pull to get the 2nd revision - validate it comes as delta, and uses cached version
	client2.ClientDeltas = true
	err = client2.StartOneshotPull()
	assert.NoError(t, err)

	msg2, ok := client2.WaitForBlipRevMessage("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)

	// Check the request was sent with the correct deltaSrc property
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg2.Properties[db.RevMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody2, err := msg2.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody2))

	updatedDeltaCacheHits := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheHit.Value()
	updatedDeltaCacheMisses := rt.GetDatabase().DbStats.DeltaSync().DeltaCacheMiss.Value()

	assert.Equal(t, deltaCacheHits+1, updatedDeltaCacheHits)
	assert.Equal(t, deltaCacheMisses, updatedDeltaCacheMisses)

}

// TestBlipDeltaSyncPush tests that a simple push replication handles deltas in EE,
// and checks that full body replication is still supported in CE.
func TestBlipDeltaSyncPush(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()
	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		},
		guestEnabled: true,
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
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-abcxyz on client
	newRev, err := client.PushRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))
	assert.NoError(t, err)
	assert.Equal(t, "2-abcxyz", newRev)

	// Check EE is delta, and CE is full-body replication
	msg, ok := client.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

		// Validate that generation of a delta didn't mutate the revision body in the revision cache
		docRev, cacheErr := rt.GetDatabase().GetRevisionCacheForTest().Get("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", db.RevCacheOmitBody, db.RevCacheOmitDelta)
		assert.NoError(t, cacheErr)
		assert.NotContains(t, docRev.BodyBytes, "bob")
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
	}

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))
	assert.Equal(t, "doc1", respBody[db.BodyId])
	assert.Equal(t, "2-abcxyz", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 3)
	assert.Equal(t, map[string]interface{}{"hello": "world!"}, greetings[0])
	assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[1])
	assert.Equal(t, map[string]interface{}{"howdy": "bob"}, greetings[2])

	// tombstone doc1 (gets rev 3-f3be6c85e0362153005dae6f08fc68bb)
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)

	data, ok = client.WaitForRev("doc1", "3-f3be6c85e0362153005dae6f08fc68bb")
	assert.True(t, ok)
	assert.Equal(t, `{}`, string(data))

	var deltaPushDocCountStart int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaPushDocCountStart = rt.GetDatabase().DbStats.DeltaSync().DeltaPushDocCount.Value()
	}
	revID, err := client.PushRev("doc1", "3-f3be6c85e0362153005dae6f08fc68bb", []byte(`{"undelete":true}`))

	if base.IsEnterpriseEdition() {
		// Now make the client push up a delta that has the parent of the tombstone.
		// This is not a valid scenario, and is actively prevented on the CBL side.
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "Can't use delta. Found tombstone for doc")
		assert.Equal(t, "", revID)
	} else {
		// Pushing a full body revision on top of a tombstone is valid.
		// CBL clients should fall back to this. The test client doesn't.
		assert.NoError(t, err)
		assert.Equal(t, "4-abcxyz", revID)
	}

	var deltaPushDocCountEnd int64

	if rt.GetDatabase().DbStats.DeltaSync() != nil {
		deltaPushDocCountEnd = rt.GetDatabase().DbStats.DeltaSync().DeltaPushDocCount.Value()
	}
	assert.Equal(t, deltaPushDocCountStart, deltaPushDocCountEnd)
}

// TestBlipNonDeltaSyncPush tests that a client that doesn't support deltas can push to a SG that supports deltas (either CE or EE)
func TestBlipNonDeltaSyncPush(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		},
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	client, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = false
	err = client.StartPull()
	assert.NoError(t, err)

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-abcxyz on client
	newRev, err := client.PushRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))
	assert.NoError(t, err)
	assert.Equal(t, "2-abcxyz", newRev)

	// Check EE is delta, and CE is full-body replication
	msg, ok := client.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	// Check the request was NOT sent with a deltaSrc property
	assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
	// Check the request body was NOT the delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, resp.Body.String(), `{"howdy":"bob"}`)
}

// TestBlipDeltaSyncNewAttachmentPull tests that adding a new attachment in SG and replicated via delta sync adds the attachment
// to the temporary "allowedAttachments" map.
func TestBlipDeltaSyncNewAttachmentPull(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: &sgUseDeltas,
			},
		},
		guestEnabled: true,
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
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-10000d5ec533b29b117e60274b1e3653 on SG with the first attachment
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}], "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`)
	assert.Equal(t, http.StatusCreated, resp.Code)
	fmt.Println(resp.Body.String())

	data, ok = client.WaitForRev("doc1", "2-10000d5ec533b29b117e60274b1e3653")
	assert.True(t, ok)
	var dataMap map[string]interface{}
	assert.NoError(t, base.JSONUnmarshal(data, &dataMap))
	atts, ok := dataMap[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, atts, 1)
	hello, ok := atts["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(2), hello["revpos"])
	assert.Equal(t, true, hello["stub"])

	// message #3 is the getAttachment message that is sent in-between rev processing
	msg, ok := client.pullReplication.WaitForMessage(3)
	assert.True(t, ok)
	assert.NotEqual(t, blip.ErrorType, msg.Type(), "Expected non-error blip message type")

	// Check EE is delta, and CE is full-body replication
	// msg, ok = client.pullReplication.WaitForMessage(5)
	msg, ok = client.WaitForBlipRevMessage("doc1", "2-10000d5ec533b29b117e60274b1e3653")
	assert.True(t, ok)

	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"_attachments":[{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}}]}`, string(msgBody))
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[db.RevMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"_attachments":[{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}}]}`, string(msgBody))
		assert.Contains(t, string(msgBody), `"_attachments":{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}}`)
		assert.Contains(t, string(msgBody), `"greetings":[{"hello":"world!"},{"hi":"alice"}]`)
	}

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev=2-10000d5ec533b29b117e60274b1e3653", "")
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))
	assert.Equal(t, "doc1", respBody[db.BodyId])
	assert.Equal(t, "2-10000d5ec533b29b117e60274b1e3653", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 2)
	assert.Equal(t, map[string]interface{}{"hello": "world!"}, greetings[0])
	assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[1])
	atts = respBody[db.BodyAttachments].(map[string]interface{})
	assert.Len(t, atts, 1)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(2), hello["revpos"])
	assert.Equal(t, true, hello["stub"])

	//assert.Equal(t, `{"_attachments":{"hello.txt":{"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=","length":11,"revpos":2,"stub":true}},"_id":"doc1","_rev":"2-10000d5ec533b29b117e60274b1e3653","greetings":[{"hello":"world!"},{"hi":"alice"}]}`, resp.Body.String())
}

// Reproduces CBG-617 (a client using activeOnly for the initial replication, and then still expecting to get subsequent tombstones afterwards)
func TestActiveOnlyContinuous(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"test":true}`)
	assertStatus(t, resp, http.StatusCreated)
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
	resp = rt.SendAdminRequest(http.MethodDelete, "/db/doc1?rev="+docResp.Rev, ``)
	assertStatus(t, resp, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &docResp))

	rev, found = btc.WaitForRev("doc1", docResp.Rev)
	assert.True(t, found)
	assert.Equal(t, `{}`, string(rev))
}

// Test that exercises Sync Gateway's norev handler
func TestBlipNorev(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	norevMsg := db.NewNoRevMessage()
	norevMsg.SetId("docid")
	norevMsg.SetRev("1-a")
	norevMsg.SetError("404")
	norevMsg.SetReason("couldn't send xyz")

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

// TestBlipDeltaSyncPushAttachment tests updating a doc that has an attachment with a delta that doesn't modify the attachment.
func TestBlipDeltaSyncPushAttachment(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	if !base.IsEnterpriseEdition() {
		t.Skip("Delta test requires EE")
	}

	const docID = "pushAttachmentDoc"

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: base.BoolPtr(true),
			},
		},
		guestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	// Push first rev
	revID, err := btc.PushRev(docID, "", []byte(`{"key":"val"}`))
	require.NoError(t, err)

	// Push second rev with an attachment (no delta yet)
	attData := base64.StdEncoding.EncodeToString([]byte("attach"))
	revID, err = btc.PushRev(docID, revID, []byte(`{"key":"val","_attachments":{"myAttachment":{"data":"`+attData+`"}}}`))
	require.NoError(t, err)

	syncData, err := rt.GetDatabase().GetDocSyncData(docID)
	require.NoError(t, err)

	assert.Len(t, syncData.Attachments, 1)
	_, found := syncData.Attachments["myAttachment"]
	assert.True(t, found)

	// Turn deltas on
	btc.ClientDeltas = true

	// Get existing body with the stub attachment, insert a new property and push as delta.
	body, found := btc.GetRev(docID, revID)
	require.True(t, found)
	newBody, err := base.InjectJSONPropertiesFromBytes(body, base.KVPairBytes{Key: "update", Val: []byte(`true`)})
	require.NoError(t, err)
	revID, err = btc.PushRev(docID, revID, newBody)
	require.NoError(t, err)

	syncData, err = rt.GetDatabase().GetDocSyncData(docID)
	require.NoError(t, err)

	assert.Len(t, syncData.Attachments, 1)
	_, found = syncData.Attachments["myAttachment"]
	assert.True(t, found)
}

// Test pushing and pulling new attachments through delta sync
// 1. Create test client that have deltas enabled
// 2. Start continuous push and pull replication in client
// 3. Make sure that sync gateway is running with delta sync on, in enterprise edition
// 4. Create doc with attachment in SGW
// 5. Update doc in the test client by adding another attachment
// 6. Have that update pushed using delta sync via the continuous replication started in step 2
func TestBlipDeltaSyncPushPullNewAttachment(t *testing.T) {
	t.Skip("Skipping this test temporarily")
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	if !base.IsEnterpriseEdition() {
		t.Skip("Delta test requires EE")
	}
	rtConfig := RestTesterConfig{
		DatabaseConfig: &DbConfig{
			DeltaSync: &DeltaSyncConfig{
				Enabled: base.BoolPtr(true),
			},
		},
		guestEnabled: true,
	}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, nil)
	require.NoError(t, err)
	defer btc.Close()

	btc.ClientDeltas = true
	err = btc.StartPull()
	assert.NoError(t, err)
	const docId = "doc1"

	// Create doc1 rev 1-77d9041e49931ceef58a1eef5fd032e8 on SG with an attachment
	bodyText := `{"greetings":[{"hi": "alice"}],"_attachments":{"hello.txt":{"data":"aGVsbG8gd29ybGQ="}}}`
	response := rt.SendAdminRequest(http.MethodPut, "/db/"+docId, bodyText)
	assert.Equal(t, http.StatusCreated, response.Code)

	// Wait for the document to be replicated at the client
	revId := respRevID(t, response)
	data, ok := btc.WaitForRev(docId, revId)
	assert.True(t, ok)
	bodyTextExpected := `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`
	require.JSONEq(t, bodyTextExpected, string(data))

	// Update the replicated doc at client by adding another attachment.
	bodyText = `{"greetings":[{"hi":"alice"}],"_attachments":{"hello.txt":{"revpos":1,"length":11,"stub":true,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="},"world.txt":{"data":"bGVsbG8gd29ybGQ="}}}`
	revId, err = btc.PushRev(docId, revId, []byte(bodyText))
	require.NoError(t, err)
	assert.Equal(t, "2-abcxyz", revId)

	// Wait for the document to be replicated at SG
	_, ok = btc.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	resp := rt.SendAdminRequest(http.MethodGet, "/db/"+docId+"?rev="+revId, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	var respBody db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &respBody))

	assert.Equal(t, docId, respBody[db.BodyId])
	assert.Equal(t, "2-abcxyz", respBody[db.BodyRev])
	greetings := respBody["greetings"].([]interface{})
	assert.Len(t, greetings, 1)
	assert.Equal(t, map[string]interface{}{"hi": "alice"}, greetings[0])

	attachments, ok := respBody[db.BodyAttachments].(map[string]interface{})
	require.True(t, ok)
	assert.Len(t, attachments, 2)
	hello, ok := attachments["hello.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0=", hello["digest"])
	assert.Equal(t, float64(11), hello["length"])
	assert.Equal(t, float64(1), hello["revpos"])
	assert.Equal(t, true, hello["stub"])

	world, ok := attachments["world.txt"].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "sha1-qiF39gVoGPFzpRQkNYcY9u3wx9Y=", world["digest"])
	assert.Equal(t, float64(11), world["length"])
	assert.Equal(t, float64(2), world["revpos"])
	assert.Equal(t, true, world["stub"])
}

func TestUpdateExistingAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		guestEnabled: true,
	})
	defer rt.Close()

	btc, err := NewBlipTesterClient(t, rt)
	assert.NoError(t, err)
	defer btc.Close()

	var doc1Body db.Body
	var doc2Body db.Body

	// Add doc1 and doc2
	req := rt.SendAdminRequest("PUT", "/db/doc1", `{}`)
	assertStatus(t, req, http.StatusCreated)
	doc1Bytes := req.BodyBytes()
	req = rt.SendAdminRequest("PUT", "/db/doc2", `{}`)
	assertStatus(t, req, http.StatusCreated)
	doc2Bytes := req.BodyBytes()

	_, err = rt.WaitForChanges(2, "/db/_changes?since=0", "", true)
	require.NoError(t, err)

	err = json.Unmarshal(doc1Bytes, &doc1Body)
	assert.NoError(t, err)
	err = json.Unmarshal(doc2Bytes, &doc2Body)
	assert.NoError(t, err)

	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	_, ok := btc.WaitForRev("doc1", "1-ca9ad22802b66f662ff171f226211d5c")
	require.True(t, ok)
	_, ok = btc.WaitForRev("doc2", "1-ca9ad22802b66f662ff171f226211d5c")
	require.True(t, ok)

	attachmentAData := base64.StdEncoding.EncodeToString([]byte("attachmentA"))
	attachmentBData := base64.StdEncoding.EncodeToString([]byte("attachmentB"))

	revIDDoc1, err := btc.PushRev("doc1", doc1Body["rev"].(string), []byte(`{"key": "val", "_attachments": {"attachment": {"data": "`+attachmentAData+`"}}}`))
	require.NoError(t, err)
	revIDDoc2, err := btc.PushRev("doc2", doc2Body["rev"].(string), []byte(`{"key": "val", "_attachments": {"attachment": {"data": "`+attachmentBData+`"}}}`))
	require.NoError(t, err)

	err = rt.waitForRev("doc1", revIDDoc1)
	assert.NoError(t, err)
	err = rt.waitForRev("doc2", revIDDoc2)
	assert.NoError(t, err)

	doc1, err := rt.GetDatabase().GetDocument("doc1", db.DocUnmarshalAll)
	_, err = rt.GetDatabase().GetDocument("doc2", db.DocUnmarshalAll)

	revIDDoc1, err = btc.PushRev("doc1", revIDDoc1, []byte(`{"key": "val", "_attachments":{"attachment":{"digest":"sha1-SKk0IV40XSHW37d3H0xpv2+z9Ck=","length":11,"content_type":"","stub":true,"revpos":3}}}`))
	require.NoError(t, err)

	err = rt.waitForRev("doc1", revIDDoc1)
	assert.NoError(t, err)

	doc1, err = rt.GetDatabase().GetDocument("doc1", db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, "sha1-SKk0IV40XSHW37d3H0xpv2+z9Ck=", doc1.Attachments["attachment"].(map[string]interface{})["digest"])

	req = rt.SendAdminRequest("GET", "/db/doc1/attachment", "")
	assert.Equal(t, "attachmentB", string(req.BodyBytes()))
}

func TestRevocationMessage(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	assert.NoError(t, err)
	defer btc.Close()

	// Add channel to role and role to user
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 and then create doc in channel A
	revocationTester.fillToSeq(4)
	_ = rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	// Wait for changes to come over (user doc, and doc)
	_, err = rt.WaitForChanges(2, "/db/_changes?since=0", "user", true)
	require.NoError(t, err)

	// Start pull
	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	// Wait for doc revision to come over
	_, ok := btc.WaitForBlipRevMessage("doc", "1-ad48b5c9d9c47b98532a3d8164ec0ae7")
	require.True(t, ok)

	// Remove role from user
	revocationTester.removeRole("user", "foo")

	revID := rt.createDocReturnRev(t, "doc1", "", map[string]interface{}{"channels": "!"})

	revocationTester.fillToSeq(10)
	revID = rt.createDocReturnRev(t, "doc1", revID, map[string]interface{}{})

	// Wait for revocation message
	_, err = rt.WaitForChanges(3, "/db/_changes?since=5", "", true)
	require.NoError(t, err)

	// Start a pull since 5 to receive revocation and removal
	err = btc.StartPullSince("false", "5", "false")
	assert.NoError(t, err)

	// Wait for doc1 rev2 - This is the last rev we expect so we can be sure replication is complete here
	_, found := btc.WaitForRev("doc1", revID)
	require.True(t, found)

	messages := btc.pullReplication.GetMessages()

	testCases := []struct {
		Name            string
		DocID           string
		ExpectedDeleted int64
	}{
		{
			Name:            "Revocation",
			DocID:           "doc",
			ExpectedDeleted: int64(2),
		},
		{
			Name:            "Removed",
			DocID:           "doc1",
			ExpectedDeleted: int64(4),
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			// Verify the deleted property in the changes message is "2" this indicated a revocation
			for _, msg := range messages {
				if msg.Properties[db.BlipProfile] == db.MessageChanges {
					var changesMessages [][]interface{}
					err = msg.ReadJSONBody(&changesMessages)
					if err != nil {
						continue
					}

					if len(changesMessages) != 2 || len(changesMessages[0]) != 4 {
						continue
					}

					criteriaMet := false
					for _, changesMessage := range changesMessages {
						castedNum, ok := changesMessage[3].(json.Number)
						if !ok {
							continue
						}
						intDeleted, err := castedNum.Int64()
						if err != nil {
							continue
						}
						if docName, ok := changesMessage[1].(string); ok && docName == testCase.DocID && intDeleted == testCase.ExpectedDeleted {
							criteriaMet = true
							break
						}
					}

					assert.True(t, criteriaMet)
				}
			}
		})
	}

	assert.NoError(t, err)
}

func TestRevocationNoRev(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	revocationTester, rt := initScenario(t, nil)
	defer rt.Close()

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	assert.NoError(t, err)
	defer btc.Close()

	// Add channel to role and role to user
	revocationTester.addRoleChannel("foo", "A")
	revocationTester.addRole("user", "foo")

	// Skip to seq 4 and then create doc in channel A
	revocationTester.fillToSeq(4)
	revID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": "A"})

	changes, err := rt.WaitForChanges(3, "/db/_changes?since=0", "user", true)
	require.NoError(t, err)

	// OneShot pull to grab doc
	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	_, ok := btc.WaitForRev("doc", "1-ad48b5c9d9c47b98532a3d8164ec0ae7")
	require.True(t, ok)

	// Remove role from user
	revocationTester.removeRole("user", "foo")

	revID = rt.createDocReturnRev(t, "doc", revID, map[string]interface{}{"channels": "A", "val": "mutate"})

	waitRevID := rt.createDocReturnRev(t, "docmarker", "", map[string]interface{}{"channels": "!"})

	lastSeq := fmt.Sprintf("%v", changes.Last_Seq)
	_, err = rt.WaitForChanges(3, "/db/_changes?since="+lastSeq, "user", true)
	require.NoError(t, err)

	err = btc.StartPullSince("false", lastSeq, "false")
	assert.NoError(t, err)

	_, ok = btc.WaitForRev("docmarker", waitRevID)
	require.True(t, ok)

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
	require.NoError(t, err)
	require.Len(t, messageBody, 2)
	require.Len(t, messageBody[0], 4)

	deletedFlag, err := messageBody[0].([]interface{})[3].(json.Number).Int64()
	require.NoError(t, err)

	assert.Equal(t, deletedFlag, int64(2))
}

func TestRemovedMessageWithAlternateAccess(t *testing.T) {
	defer db.SuspendSequenceBatching()()
	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	rt := NewRestTester(t, nil)
	defer rt.Close()

	resp := rt.SendAdminRequest("PUT", "/db/_user/user", `{"admin_channels": ["A", "B"], "password": "test"}`)
	assertStatus(t, resp, http.StatusCreated)

	btc, err := NewBlipTesterClientOptsWithRT(t, rt, &BlipTesterClientOpts{
		Username:        "user",
		Channels:        []string{"*"},
		ClientDeltas:    false,
		SendRevocations: true,
	})
	assert.NoError(t, err)
	defer btc.Close()

	docRevID := rt.createDocReturnRev(t, "doc", "", map[string]interface{}{"channels": []string{"A", "B"}})

	changes, err := rt.WaitForChanges(1, "/db/_changes?since=0&revocations=true", "user", true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, "1-9b49fa26d87ad363b2b08de73ff029a9", changes.Results[0].Changes[0]["rev"])

	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	_, ok := btc.WaitForRev("doc", "1-9b49fa26d87ad363b2b08de73ff029a9")
	assert.True(t, ok)

	docRevID = rt.createDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{"B"}})

	changes, err = rt.WaitForChanges(1, fmt.Sprintf("/db/_changes?since=%s&revocations=true", changes.Last_Seq), "user", true)
	require.NoError(t, err)
	assert.Equal(t, 1, len(changes.Results))
	assert.Equal(t, "doc", changes.Results[0].ID)
	assert.Equal(t, "2-f0d4cbcdd4a9ec835799055fdba45263", changes.Results[0].Changes[0]["rev"])

	err = btc.StartOneshotPull()
	assert.NoError(t, err)

	_, ok = btc.WaitForRev("doc", "2-f0d4cbcdd4a9ec835799055fdba45263")
	assert.True(t, ok)

	docRevID = rt.createDocReturnRev(t, "doc", docRevID, map[string]interface{}{"channels": []string{}})
	_ = rt.createDocReturnRev(t, "docmarker", "", map[string]interface{}{"channels": []string{"!"}})

	changes, err = rt.WaitForChanges(2, fmt.Sprintf("/db/_changes?since=%s&revocations=true", changes.Last_Seq), "user", true)
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
	require.Len(t, messageBody, 2)
	require.Len(t, messageBody[0], 4)
	require.Len(t, messageBody[1], 3)

	deletedFlags, err := messageBody[0].([]interface{})[3].(json.Number).Int64()
	docID := messageBody[0].([]interface{})[1]
	require.NoError(t, err)
	assert.Equal(t, "doc", docID)
	assert.Equal(t, int64(4), deletedFlags)
}
