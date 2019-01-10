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

	var rt RestTester
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

	var rt RestTester
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyAll)()
	var rt RestTester
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

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e on client
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
	var rt RestTester
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

	// create doc1 rev 2-959f0e9ad32d84ff652fb91d8d0caa7e on client
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
