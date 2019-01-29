package rest

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	bt, err := NewBlipTester()
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
	err = json.Unmarshal(body, &changeList)
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
	goassert.Equals(t, err, nil)

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
	err = json.Unmarshal(body2, &changeList2)
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
			err = json.Unmarshal(body, &changeListReceived)
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
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg|base.KeyChanges|base.KeyCache)()

	bt, err := NewBlipTester()
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receivedChangesWg := sync.WaitGroup{}

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
			err = json.Unmarshal(body, &changeListReceived)
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

				receivedChangesWg.Done()
			}

		} else {

			receivedChangesWg.Done()

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Increment waitgroup since just the act of subscribing to continuous changes will cause
	// the callback changes handler to be invoked with an initial change w/ empty body, signaling that
	// all of the changes have been sent (eg, there are no changes to send)
	receivedChangesWg.Add(1)

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
		goassert.Equals(t, err, nil)

		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")

	}

	// Wait until all expected changes are received by change handler
	// receivedChangesWg.Wait()
	timeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*5)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	bt, err := NewBlipTester()
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
			err = json.Unmarshal(body, &changeListReceived)
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
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Increment waitgroup to account for the expected 'caught up' nil changes entry.
	receivedChangesWg.Add(1)

	// Add documents
	for docID, _ := range docIdsReceived {
		//// Add a change: Send an unsolicited doc revision in a rev request
		_, _, revResponse, err := bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		goassert.Equals(t, err, nil)
		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")
		receivedChangesWg.Add(1)
	}

	// Wait for documents to be processed and available for changes
	expectedSequence := uint64(104)
	bt.restTester.WaitForSequence(expectedSequence)

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
	goassert.True(t, numBatchesReceivedSnapshot >= 2)

	// Validate all expected documents were received.
	for docID, received := range docIdsReceived {
		if !received {
			t.Errorf("Did not receive expected doc %s in changes", docID)
		}
	}

	// Validate that the 'caught up' message was sent
	goassert.True(t, receivedCaughtUpChange)

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
		goassert.Equals(t, err, nil)
		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")
		receivedChangesWg.Add(1)
	}

	// Wait long enough to ensure the changes aren't being sent
	expectedTimeoutErr := WaitWithTimeout(&receivedChangesWg, time.Second*3)
	if expectedTimeoutErr == nil {
		t.Errorf("Received additional changes after one-shot should have been closed.")
	}

	// Validate integer sequences
	assert.False(t, nonIntegerSequenceReceived, "Unexpected non-integer sequence seen.")
}

// Test subChanges w/ docID filter
func TestBlipSubChangesDocIDFilter(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	bt, err := NewBlipTester()
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
			err = json.Unmarshal(body, &changeListReceived)
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
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Increment waitgroup to account for the expected 'caught up' nil changes entry.
	receivedChangesWg.Add(1)

	// Add documents
	for _, docID := range docIDsSent {
		//// Add a change: Send an unsolicited doc revision in a rev request
		_, _, revResponse, err := bt.SendRev(
			docID,
			"1-abc",
			[]byte(`{"key": "val"}`),
			blip.Properties{},
		)
		goassert.Equals(t, err, nil)
		_, err = revResponse.Body()
		assert.NoError(t, err, "Error unmarshalling response body")
	}
	receivedChangesWg.Add(len(docIDsExpected))

	// Wait for documents to be processed and available for changes
	// 105 docs +
	expectedSequence := uint64(100)
	bt.restTester.WaitForSequence(expectedSequence)

	// TODO: Attempt a subChanges w/ continuous=true and docID filter

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 5 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)

	body := subChangesBody{DocIDs: docIDsExpected}
	bodyBytes, err := json.Marshal(body)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noConflictsMode: true,
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
	err = json.Unmarshal(body, &changeList)
	assert.NoError(t, err, "Error getting changes response body")

	// The common case of an empty array response tells the sender to send all of the proposed revisions,
	// so the changeList returned by Sync Gateway is expected to be empty
	goassert.Equals(t, len(changeList), 0)

}

// Connect to public port with authentication
func TestPublicPortAuthentication(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create bliptester that is connected as user1, with access to the user1 channel
	btUser1, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:       true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	assert.NoError(t, err, "Error creating BlipTester")
	defer btUser1.Close()

	// Send the user1 doc
	btUser1.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["user1"]}`),
		blip.Properties{},
	)

	// Create bliptester that is connected as user2, with access to the * channel
	btUser2, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:                true,
		connectingUsername:          "user2",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"},      // user2 has access to all channels
		restTester:                  btUser1.restTester, // re-use rest tester, otherwise it will create a new underlying bucket in walrus case
	})
	assert.NoError(t, err, "Error creating BlipTester")
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

// Test send and retrieval of a doc.
//   Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetRev(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Setup
	rt := RestTester{
		noAdminParty: true,
	}
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	}
	bt, err := NewBlipTesterFromSpec(btSpec)
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Send non-deleted rev
	sent, _, resp, err := bt.SendRev("sendAndGetRev", "1-abc", []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)
	goassert.Equals(t, resp.Properties["Error-Code"], "")

	// Get non-deleted rev
	response := bt.restTester.SendAdminRequest("GET", "/db/sendAndGetRev?rev=1-abc", "")
	assertStatus(t, response, 200)
	var responseBody RestDocument
	assert.NoError(t, json.Unmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	_, ok := responseBody[db.BodyDeleted]
	goassert.False(t, ok)

	// Tombstone the document
	history := []string{"1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("sendAndGetRev", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}`), blip.Properties{"deleted": "true"})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)
	goassert.Equals(t, resp.Properties["Error-Code"], "")

	// Get the tombstoned document
	response = bt.restTester.SendAdminRequest("GET", "/db/sendAndGetRev?rev=2-bcd", "")
	assertStatus(t, response, 200)
	responseBody = RestDocument{}
	assert.NoError(t, json.Unmarshal(response.Body.Bytes(), &responseBody), "Error unmarshalling GET doc response")
	deletedValue, deletedOK := responseBody[db.BodyDeleted].(bool)
	goassert.True(t, deletedOK)
	goassert.True(t, deletedValue)
}

// Test send and retrieval of a doc with a large numeric value.  Ensure proper large number handling.
//   Validate deleted handling (includes check for https://github.com/couchbase/sync_gateway/issues/3341)
func TestBlipSendAndGetLargeNumberRev(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Setup
	rt := RestTester{
		noAdminParty: true,
	}
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	}
	bt, err := NewBlipTesterFromSpec(btSpec)
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Send non-deleted rev
	sent, _, resp, err := bt.SendRev("largeNumberRev", "1-abc", []byte(`{"key": "val", "largeNumber":9223372036854775807, "channels": ["user1"]}`), blip.Properties{})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Setup
	rt := RestTester{
		noAdminParty: true,
	}
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	}
	bt, err := NewBlipTesterFromSpec(btSpec)
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Create new checkpoint
	checkpointBody := []byte(`{"client_seq":"1000"}`)
	sent, _, resp, err := bt.SetCheckpoint("testclient", "", checkpointBody)
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)
	goassert.Equals(t, resp.Properties["Error-Code"], "")

	checkpointRev := resp.Rev()
	goassert.Equals(t, checkpointRev, "0-1")

	// Validate checkpoint existence in bucket (local file name "/" needs to be URL encoded as %252F)
	response := rt.SendAdminRequest("GET", "/db/_local/checkpoint%252Ftestclient", "")
	assertStatus(t, response, 200)
	var responseBody map[string]interface{}
	err = json.Unmarshal(response.Body.Bytes(), &responseBody)
	goassert.Equals(t, responseBody["client_seq"], "1000")

	// Attempt to update the checkpoint with previous rev
	checkpointBody = []byte(`{"client_seq":"1005"}`)
	sent, _, resp, err = bt.SetCheckpoint("testclient", checkpointRev, checkpointBody)
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

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
	rt := RestTester{
		SyncFn:       syncFn,
		noAdminParty: true,
	}
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/db/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Add a doc in the PBS channel
	_, _, addRevResponse, err := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)
	goassert.Equals(t, err, nil)

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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Setup
	rt := RestTester{
		SyncFn:       `function(doc) {channel(doc.channels); access(doc.accessUser, doc.accessChannel);}`,
		noAdminParty: true,
	}
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Add a doc in the PBS channel
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/db/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

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
	goassert.Equals(t, len(changes), 2)

}

// Grant a user access to a channel via the REST Admin API, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaAdminApi(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:       true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Add a doc in the PBS channel
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
		blip.Properties{},
	)

	// Update the user doc to grant access to PBS
	response := bt.restTester.SendAdminRequest("PUT", "/db/_user/user1", `{"admin_channels":["user1", "PBS"]}`)
	assertStatus(t, response, 200)

	// Add another doc in the PBS channel
	bt.SendRev(
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:       true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
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
	requestSetCheckpoint.SetJSONBody(checkpointBody)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:       true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	input := SendRevWithAttachmentInput{
		docId:            "doc",
		revId:            "1-rev1",
		attachmentName:   "myAttachment",
		attachmentBody:   "attach",
		attachmentDigest: "fakedigest",
	}
	bt.SendRevWithAttachment(input)

	// Try to fetch the attachment directly via getAttachment, expected to fail w/ 403 error for security reasons
	// since it's not in the context of responding to a "rev" request from the peer.
	getAttachmentRequest := blip.NewRequest()
	getAttachmentRequest.SetProfile("getAttachment")
	getAttachmentRequest.Properties["digest"] = input.attachmentDigest
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:                true,
		connectingUsername:          "user1",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // All channels
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	attachmentBody := "attach"

	digest := db.Sha1DigestKey([]byte(attachmentBody))

	// Send revision with attachment
	input := SendRevWithAttachmentInput{
		docId:            "doc",
		revId:            "1-rev1",
		attachmentName:   "myAttachment",
		attachmentBody:   attachmentBody,
		attachmentDigest: digest,
	}
	sent, _, _ := bt.SendRevWithAttachment(input)
	goassert.True(t, sent)

	// Get all docs and attachment via subChanges request
	allDocs := bt.WaitForNumDocsViaChanges(1)

	// make assertions on allDocs -- make sure attachment is present w/ expected body
	goassert.Equals(t, len(allDocs), 1)
	retrievedDoc := allDocs[input.docId]

	// doc assertions
	goassert.Equals(t, retrievedDoc.ID(), input.docId)
	goassert.Equals(t, retrievedDoc.RevID(), input.revId)

	// attachment assertions
	attachments, err := retrievedDoc.GetAttachments()
	goassert.True(t, err == nil)
	goassert.Equals(t, len(attachments), 1)
	retrievedAttachment := attachments[input.attachmentName]
	goassert.Equals(t, string(retrievedAttachment.Data), input.attachmentBody)
	goassert.Equals(t, retrievedAttachment.Length, len(attachmentBody))
	goassert.Equals(t, input.attachmentDigest, retrievedAttachment.Digest)

}

// Put a revision that is rejected by the sync function and assert that Sync Gateway
// returns an error code
func TestPutInvalidRevSyncFnReject(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	syncFn := `
		function(doc) {
			requireAccess("PBS");
			channel(doc.channels);
		}
    `

	// Setup
	rt := RestTester{
		SyncFn:       syncFn,
		noAdminParty: true,
	}
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:                true,
		connectingUsername:          "user1",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"}, // All channels
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
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
	goassert.Equals(t, errorCode, "500")

	// Make sure that a one-off GetChanges() returns no documents
	changes := bt.GetChanges()
	goassert.Equals(t, len(changes), 0)

}

func TestPutRevNoConflictsMode(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noConflictsMode: true,
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)                          // no error
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Create blip tester
	bt, err := NewBlipTesterFromSpec(BlipTesterSpec{
		noConflictsMode: false,
	})
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val"}`), blip.Properties{})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)                          // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	sent, _, resp, err = bt.SendRev("foo", "1-def", []byte(`{"key": "val"}`), blip.Properties{"noconflicts": "false"})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)                          // no error
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	// Setup
	rt := RestTester{
		noAdminParty: true,
	}
	btSpec := BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	}
	bt, err := NewBlipTesterFromSpec(btSpec)
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

	// Add rev-1 in channel user1
	sent, _, resp, err := bt.SendRev("foo", "1-abc", []byte(`{"key": "val", "channels": ["user1"]}"`), blip.Properties{})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)                          // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	// Add rev-2 in channel user1
	history := []string{"1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("foo", "2-bcd", history, []byte(`{"key": "val", "channels": ["user1"]}"`), blip.Properties{"noconflicts": "true"})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)                          // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	// Try to get rev 2 via BLIP API and assert that _removed == false
	resultDoc, err := bt.GetDocAtRev("foo", "2-bcd")
	assert.NoError(t, err, "Unexpected Error")
	goassert.False(t, resultDoc.IsRemoved())

	// Add rev-3, remove from channel user1 and put into channel another_channel
	history = []string{"2-bcd", "1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("foo", "3-cde", history, []byte(`{"key": "val", "channels": ["another_channel"]}`), blip.Properties{"noconflicts": "true"})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)                          // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	// Add rev-4, keeping it in channel another_channel
	history = []string{"3-cde", "2-bcd", "1-abc"}
	sent, _, resp, err = bt.SendRevWithHistory("foo", "4-def", history, []byte("{}"), blip.Properties{"noconflicts": "true", "deleted": "true"})
	goassert.True(t, sent)
	goassert.Equals(t, err, nil)                          // no error
	goassert.Equals(t, resp.Properties["Error-Code"], "") // no error

	// Flush rev cache in case this prevents the bug from showing up (didn't make a difference)
	rt.GetDatabase().FlushRevisionCacheForTest()

	// Delete any temp revisions in case this prevents the bug from showing up (didn't make a difference)
	tempRevisionDocId := "_sync:rev:foo:5:3-cde"
	err = rt.GetDatabase().Bucket.Delete(tempRevisionDocId)
	assert.NoError(t, err, "Unexpected Error")

	// Workaround data race (https://gist.github.com/tleyden/0ace70b8a38b76a7beee95529610b6cf) that happens because
	// there are multiple goroutines accessing the bt.blipContext.HandlerForProfile map.
	// The workaround uses a separate blipTester, and therefore a separate context.  It uses a different
	// user to avoid an error when the NewBlipTesterFromSpec tries to create the user (eg, user1 already exists error)
	btSpec2 := BlipTesterSpec{
		connectingUsername:          "user2",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"user1"}, // so it can see user1's docs
		restTester:                  &rt,
	}
	bt2, err := NewBlipTesterFromSpec(btSpec2)
	assert.NoError(t, err, "Unexpected error creating BlipTester")

	// Try to get rev 3 via BLIP API and assert that _removed == true
	resultDoc, err = bt2.GetDocAtRev("foo", "3-cde")
	assert.NoError(t, err, "Unexpected Error")
	goassert.True(t, resultDoc.IsRemoved())

	// Try to get rev 3 via REST API, and assert that _removed == true
	headers := map[string]string{}
	headers["Authorization"] = "Basic " + base64.StdEncoding.EncodeToString([]byte(btSpec.connectingUsername+":"+btSpec.connectingPassword))
	response := rt.SendRequestWithHeaders("GET", "/db/foo?rev=3-cde", "", headers)
	restDocument := response.GetRestDocument()
	goassert.True(t, restDocument.IsRemoved())

}

// Make sure that a client cannot open multiple subChanges subscriptions on a single blip context (SG #3222)
//
// - Open two continuous subChanges feeds, and asserts that it gets an error on the 2nd one.
// - Open a one-off subChanges request, assert no error
func TestMultipleOustandingChangesSubscriptions(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP|base.KeySync|base.KeySyncMsg)()

	bt, err := NewBlipTester()
	assert.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {
		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}
	}

	// Send continous subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())
	errorCode := subChangesResponse.Properties["Error-Code"]
	log.Printf("errorCode: %v", errorCode)
	goassert.True(t, errorCode == "")

	// Send a second continuous subchanges request, expect an error
	subChangesRequest2 := blip.NewRequest()
	subChangesRequest2.SetProfile("subChanges")
	subChangesRequest2.Properties["continuous"] = "true"
	subChangesRequest2.SetCompressed(false)
	sent2 := bt.sender.Send(subChangesRequest2)
	goassert.True(t, sent2)
	subChangesResponse2 := subChangesRequest2.Response()
	goassert.Equals(t, subChangesResponse2.SerialNumber(), subChangesRequest2.SerialNumber())
	errorCode2 := subChangesResponse2.Properties["Error-Code"]
	log.Printf("errorCode2: %v", errorCode2)
	goassert.True(t, errorCode2 == "500")

	// Send a thirst subChanges request, but this time continuous = false.  Should not return an error
	subChangesRequest3 := blip.NewRequest()
	subChangesRequest3.SetProfile("subChanges")
	subChangesRequest3.Properties["continuous"] = "false"
	subChangesRequest3.SetCompressed(false)
	sent3 := bt.sender.Send(subChangesRequest3)
	goassert.True(t, sent3)
	subChangesResponse3 := subChangesRequest3.Response()
	goassert.Equals(t, subChangesResponse3.SerialNumber(), subChangesRequest3.SerialNumber())
	errorCode3 := subChangesResponse3.Properties["Error-Code"]
	log.Printf("errorCode: %v", errorCode3)
	goassert.True(t, errorCode == "")

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
//   - Actual: only recieve 4 docs (4 revs)
func TestMissingNoRev(t *testing.T) {

	rt := RestTester{}
	btSpec := BlipTesterSpec{
		restTester: &rt,
	}
	bt, err := NewBlipTesterFromSpec(btSpec)
	assert.NoError(t, err, "Unexpected error creating BlipTester")
	defer bt.Close()

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

	// Purge one doc
	doc0Id := fmt.Sprintf("doc-%d", 0)
	err = targetDb.Purge(doc0Id)
	assert.NoError(t, err, "failed")

	// Flush rev cache
	targetDb.FlushRevisionCacheForTest()

	// Pull docs, expect to pull 4 since one was purged.  (also expect to NOT get stuck)
	docs := bt.WaitForNumDocsViaChanges(4)
	goassert.True(t, len(docs) == 4)

}

// TestBlipDeltaSyncPull tests that a simple pull replication uses deltas in EE,
// and checks that full body replication still happens in CE.
func TestBlipDeltaSyncPull(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	var rt = RestTester{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	defer rt.Close()

	deltaSentCount := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDeltaSync().Get(base.StatKeyDeltasSent))

	client, err := NewBlipTesterClient(&rt)
	assert.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	client.StartPull()

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": "bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(data))

	msg, ok := client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)

	// Check EE is delta, and CE is full-body replication
	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[revMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		assert.Equal(t, deltaSentCount+1, base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDeltaSync().Get(base.StatKeyDeltasSent)))
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[revMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
		assert.Equal(t, deltaSentCount, base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDeltaSync().Get(base.StatKeyDeltasSent)))
	}
}

// TestBlipDeltaSyncPullRevCache tests that a simple pull replication uses deltas in EE,
// Second pull validates use of rev cache for previously generated deltas.
func TestBlipDeltaSyncPullRevCache(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skipf("Skipping enterprise-only delta sync test.")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	sgUseDeltas := base.IsEnterpriseEdition()
	var rt = RestTester{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	defer rt.Close()

	client, err := NewBlipTesterClient(&rt)
	assert.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	client.StartPull()

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// Perform a one-shot pull as client 2 to pull down the first revision

	client2, err := NewBlipTesterClient(&rt)
	assert.NoError(t, err)
	defer client2.Close()

	client2.ClientDeltas = true
	client2.StartOneshotPull()

	msg, ok := client2.pullReplication.WaitForMessage(3)
	assert.True(t, ok)

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=1-0335a345b6ffed05707ccc4cbc1b67f4", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}, {"howdy": "bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "2-959f0e9ad32d84ff652fb91d8d0caa7e")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(data))

	msg, ok = client.pullReplication.WaitForMessage(5)
	assert.True(t, ok)

	// Check EE is delta
	// Check the request was sent with the correct deltaSrc property
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[revMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

	deltaCacheHits := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDeltaSync().Get(base.StatKeyDeltaCacheHits))
	deltaCacheMisses := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDeltaSync().Get(base.StatKeyDeltaCacheMisses))

	// Run another one shot pull to get the 2nd revision - validate it comes as delta, and uses cached version
	client2.ClientDeltas = true
	client2.StartOneshotPull()

	msg2, ok := client2.pullReplication.WaitForMessage(6)
	assert.True(t, ok)

	// Check the request was sent with the correct deltaSrc property
	assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg2.Properties[revMessageDeltaSrc])
	// Check the request body was the actual delta
	msgBody2, err := msg2.Body()
	assert.NoError(t, err)
	assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody2))

	updatedDeltaCacheHits := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDeltaSync().Get(base.StatKeyDeltaCacheHits))
	updatedDeltaCacheMisses := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDeltaSync().Get(base.StatKeyDeltaCacheMisses))

	assert.Equal(t, deltaCacheHits+1, updatedDeltaCacheHits)
	assert.Equal(t, deltaCacheMisses, updatedDeltaCacheMisses)

}

// TestBlipDeltaSyncPush tests that a simple push replication handles deltas in EE,
// and checks that full body replication is still supported in CE.
func TestBlipDeltaSyncPush(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()
	sgUseDeltas := base.IsEnterpriseEdition()
	var rt = RestTester{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	defer rt.Close()

	client, err := NewBlipTesterClient(&rt)
	assert.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	client.StartPull()

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
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[revMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

		// Validate that generation of a delta didn't mutate the revision body in the revision cache
		docRev, cacheErr := rt.GetDatabase().GetRevisionCacheForTest().Get("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
		assert.NoError(t, cacheErr)
		marshalledBody, _ := json.Marshal(docRev.Body)
		assert.Equal(t, []byte(`{"_id":"doc1","_rev":"1-0335a345b6ffed05707ccc4cbc1b67f4","greetings":[{"hello":"world!"},{"hi":"alice"}]}`), marshalledBody)

	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[revMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
	}

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, `{"_id":"doc1","_rev":"2-abcxyz","greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, resp.Body.String())
}

// TestBlipNonDeltaSyncPush tests that a client that doesn't support deltas can push to a SG that supports deltas (either CE or EE)
func TestBlipNonDeltaSyncPush(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()
	sgUseDeltas := base.IsEnterpriseEdition()
	var rt = RestTester{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	defer rt.Close()

	client, err := NewBlipTesterClient(&rt)
	assert.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = false
	client.StartPull()

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
	assert.Equal(t, "", msg.Properties[revMessageDeltaSrc])
	// Check the request body was NOT the delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))

	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, `{"_id":"doc1","_rev":"2-abcxyz","greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, resp.Body.String())
}

// TestBlipDeltaSyncWithAttachments tests pushing/pulling multiple revisions, adding/removing attachments
// on a document, attempting to use delta sync in EE, and full body replication in CE.
func TestBlipDeltaSyncWithAttachments(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()
	sgUseDeltas := base.IsEnterpriseEdition()
	var rt = RestTester{DatabaseConfig: &DbConfig{DeltaSync: &DeltaSyncConfig{Enabled: &sgUseDeltas}}}
	defer rt.Close()

	client, err := NewBlipTesterClient(&rt)
	assert.NoError(t, err)
	defer client.Close()

	client.ClientDeltas = true
	client.StartPull()

	// create doc1 rev 1-0335a345b6ffed05707ccc4cbc1b67f4 - no attachments
	resp := rt.SendAdminRequest(http.MethodPut, "/db/doc1", `{"greetings": [{"hello": "world!"}, {"hi": "alice"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok := client.WaitForRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"}]}`, string(data))

	// create doc1 rev 2-abcxyz on client - no attachments
	newRev, err := client.PushRev("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4", []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`))
	assert.NoError(t, err)
	assert.Equal(t, "2-abcxyz", newRev)

	// Check EE is delta, and CE is full-body replication
	msg, ok := client.pushReplication.WaitForMessage(2)
	assert.True(t, ok)

	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, "1-0335a345b6ffed05707ccc4cbc1b67f4", msg.Properties[revMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.Equal(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))

		// Validate that generation of a delta didn't mutate the revision body in the revision cache
		docRev, cacheErr := rt.GetDatabase().GetRevisionCacheForTest().Get("doc1", "1-0335a345b6ffed05707ccc4cbc1b67f4")
		assert.NoError(t, cacheErr)
		marshalledBody, _ := json.Marshal(docRev.Body)
		assert.Equal(t, []byte(`{"_id":"doc1","_rev":"1-0335a345b6ffed05707ccc4cbc1b67f4","greetings":[{"hello":"world!"},{"hi":"alice"}]}`), marshalledBody)
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[revMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		assert.NotEqual(t, `{"greetings":{"2-":[{"howdy":"bob"}]}}`, string(msgBody))
		assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
	}

	// Ensure rev-2 is as expected
	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, `{"_id":"doc1","_rev":"2-abcxyz","greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, resp.Body.String())

	// create doc1 rev 3-abcxyz on client - add single inline attachment
	newRev, err = client.PushRev("doc1", newRev, []byte(`{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"},{"aloha":"charlie"}],"_attachments":{"gopher":{"content_type":"image/png","data":"iVBORw0KGgoAAAANSUhEUgAAAF4AAACACAYAAACC5t4xAAAAAXNSR0IArs4c6QAAAAlwSFlzAAAuIwAALiMBeKU/dgAABCVpVFh0WE1MOmNvbS5hZG9iZS54bXAAAAAAADx4OnhtcG1ldGEgeG1sbnM6eD0iYWRvYmU6bnM6bWV0YS8iIHg6eG1wdGs9IlhNUCBDb3JlIDUuNC4wIj4KICAgPHJkZjpSREYgeG1sbnM6cmRmPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5LzAyLzIyLXJkZi1zeW50YXgtbnMjIj4KICAgICAgPHJkZjpEZXNjcmlwdGlvbiByZGY6YWJvdXQ9IiIKICAgICAgICAgICAgeG1sbnM6dGlmZj0iaHR0cDovL25zLmFkb2JlLmNvbS90aWZmLzEuMC8iCiAgICAgICAgICAgIHhtbG5zOmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIgogICAgICAgICAgICB4bWxuczpkYz0iaHR0cDovL3B1cmwub3JnL2RjL2VsZW1lbnRzLzEuMS8iCiAgICAgICAgICAgIHhtbG5zOnhtcD0iaHR0cDovL25zLmFkb2JlLmNvbS94YXAvMS4wLyI+CiAgICAgICAgIDx0aWZmOlJlc29sdXRpb25Vbml0PjI8L3RpZmY6UmVzb2x1dGlvblVuaXQ+CiAgICAgICAgIDx0aWZmOkNvbXByZXNzaW9uPjU8L3RpZmY6Q29tcHJlc3Npb24+CiAgICAgICAgIDx0aWZmOlhSZXNvbHV0aW9uPjMwMDwvdGlmZjpYUmVzb2x1dGlvbj4KICAgICAgICAgPHRpZmY6T3JpZW50YXRpb24+MTwvdGlmZjpPcmllbnRhdGlvbj4KICAgICAgICAgPHRpZmY6WVJlc29sdXRpb24+MzAwPC90aWZmOllSZXNvbHV0aW9uPgogICAgICAgICA8ZXhpZjpQaXhlbFhEaW1lbnNpb24+OTQ8L2V4aWY6UGl4ZWxYRGltZW5zaW9uPgogICAgICAgICA8ZXhpZjpDb2xvclNwYWNlPjE8L2V4aWY6Q29sb3JTcGFjZT4KICAgICAgICAgPGV4aWY6UGl4ZWxZRGltZW5zaW9uPjEyODwvZXhpZjpQaXhlbFlEaW1lbnNpb24+CiAgICAg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"}}}`))
	assert.NoError(t, err)
	assert.Equal(t, "3-abcxyz", newRev)

	msg, ok = client.pushReplication.WaitForMessage(4)
	assert.True(t, ok)

	// Check the request was NOT sent with a deltaSrc property - as it has an attachment
	assert.Equal(t, "", msg.Properties[revMessageDeltaSrc])
	// Check the request body was NOT the delta
	msgBody, err := msg.Body()
	assert.NoError(t, err)
	assert.NotEqual(t, `{"greetings":{"3-":[{"aloha":"charlie"}]}}`, string(msgBody))
	assert.Equal(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"},{"aloha":"charlie"}]}`, string(msgBody))

	// Ensure document is as expected
	resp = rt.SendAdminRequest(http.MethodGet, "/db/doc1?rev="+newRev, "")
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"_id":"doc1","_rev":"3-abcxyz","greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"},{"aloha":"charlie"}]}`, resp.Body.String())

	// create doc1 rev 4-42246765e6b1f5daaebc61e19a8c54a8 on server - remove a property - keep attachment present
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev="+newRev, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "4-42246765e6b1f5daaebc61e19a8c54a8")
	assert.True(t, ok)
	assert.Equal(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(data))

	msg, ok = client.pullReplication.WaitForMessage(7)
	assert.True(t, ok)

	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, newRev, msg.Properties[revMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		// Delta is sent with the full _attachments property stamped into it
		assert.NotEqual(t, `{"greetings":{"3-":[]}}`, string(msgBody))
		assert.Equal(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":{"3-":[]}}`, string(msgBody))
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[revMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		// Make sure the full body, and full attachment metadata is sent
		assert.NotEqual(t, `{"greetings":{"3-":[]}}`, string(msgBody))
		assert.NotEqual(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":{"3-":[]}}`, string(msgBody))
		assert.Equal(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
	}

	/* FIXME: Below is for removal of attachment - client doesn't support delete yet...

	// create doc1 rev 5-7e35ae83a546837013ec313d905dd4ae on server - remove attachment
	resp = rt.SendAdminRequest(http.MethodPut, "/db/doc1?rev=4-42246765e6b1f5daaebc61e19a8c54a8", `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`)
	assert.Equal(t, http.StatusCreated, resp.Code)

	data, ok = client.WaitForRev("doc1", "5-7e35ae83a546837013ec313d905dd4ae")
	assert.True(t, ok)
	assert.Equal(t, `{"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(data))

	msg, ok = client.pullReplication.WaitForMessage(9)
	assert.True(t, ok)

	if base.IsEnterpriseEdition() {
		// Check the request was sent with the correct deltaSrc property
		assert.Equal(t, newRev, msg.Properties[revMessageDeltaSrc])
		// Check the request body was the actual delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		// Delta is sent with the full _attachments property stamped into it
		assert.NotEqual(t, `{"greetings":{"3-":[]}}`, string(msgBody))
		assert.Equal(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":{"3-":[]}}`, string(msgBody))
	} else {
		// Check the request was NOT sent with a deltaSrc property
		assert.Equal(t, "", msg.Properties[revMessageDeltaSrc])
		// Check the request body was NOT the delta
		msgBody, err := msg.Body()
		assert.NoError(t, err)
		// Make sure the full body, and full attachment metadata is sent
		assert.NotEqual(t, `{"greetings":{"3-":[]}}`, string(msgBody))
		assert.NotEqual(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":{"3-":[]}}`, string(msgBody))
		assert.Equal(t, `{"_attachments":{"gopher":{"content_type":"image/png","digest":"sha1-kjMyuTOF0wAPc2GqUflILISzaPM=","length":10623,"revpos":3,"stub":true}},"greetings":[{"hello":"world!"},{"hi":"alice"},{"howdy":"bob"}]}`, string(msgBody))
	}

	*/

}
