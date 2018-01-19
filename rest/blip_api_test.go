package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
	"sync/atomic"
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

	bt := NewBlipTester()
	defer bt.Close()

	// Verify Sync Gateway will accept the doc revision that is about to be sent
	var changeList [][]interface{}
	changesRequest := blip.NewRequest()
	changesRequest.SetProfile("changes")                             // TODO: make a constant for "changes" and use it everywhere
	changesRequest.SetBody([]byte(`[["1", "foo", "1-abc", false]]`)) // [sequence, docID, revID]
	sent := bt.sender.Send(changesRequest)
	assert.True(t, sent)
	changesResponse := changesRequest.Response()
	assert.Equals(t, changesResponse.SerialNumber(), changesRequest.SerialNumber())
	body, err := changesResponse.Body()
	assertNoError(t, err, "Error reading changes response body")
	err = json.Unmarshal(body, &changeList)
	assertNoError(t, err, "Error unmarshalling response body")
	assert.True(t, len(changeList) == 1) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow := changeList[0]
	assert.True(t, len(changeRow) == 0) // Should be empty, meaning the server is saying it doesn't have the revision yet

	// Send the doc revision in a rev request
	_, _, revResponse := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val"}`),
	)
	_, err = revResponse.Body()
	assertNoError(t, err, "Error unmarshalling response body")

	// Call changes with a hypothetical new revision, assert that it returns last pushed revision
	var changeList2 [][]interface{}
	changesRequest2 := blip.NewRequest()
	changesRequest2.SetProfile("changes")
	changesRequest2.SetBody([]byte(`[["2", "foo", "2-xyz", false]]`)) // [sequence, docID, revID]
	sent2 := bt.sender.Send(changesRequest2)
	assert.True(t, sent2)
	changesResponse2 := changesRequest2.Response()
	assert.Equals(t, changesResponse2.SerialNumber(), changesRequest2.SerialNumber())
	body2, err := changesResponse2.Body()
	assertNoError(t, err, "Error reading changes response body")
	err = json.Unmarshal(body2, &changeList2)
	assertNoError(t, err, "Error unmarshalling response body")
	assert.True(t, len(changeList2) == 1) // Should be 1 row, corresponding to the single doc that was queried in changes
	changeRow2 := changeList2[0]
	assert.True(t, len(changeRow2) == 1) // Should have 1 item in row, which is the rev id of the previous revision pushed
	assert.Equals(t, changeRow2[0], "1-abc")

	// Call subChanges api and make sure we get expected changes back
	receviedChangesRequestWg := sync.WaitGroup{}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		log.Printf("got changes message: %+v", request)
		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		if string(body) != "null" {

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = json.Unmarshal(body, &changeListReceived)
			assertNoError(t, err, "Error unmarshalling changes recevied")
			assert.True(t, len(changeListReceived) == 1)
			change := changeListReceived[0] // [1,"foo","1-abc"]
			assert.True(t, len(change) == 3)
			assert.Equals(t, change[0].(float64), float64(1)) // Expect sequence to be 1, since first item in DB
			assert.Equals(t, change[1], "foo")                // Doc id of pushed rev
			assert.Equals(t, change[2], "1-abc")              // Rev id of pushed rev

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
			assertNoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

		receviedChangesRequestWg.Done()

	}

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "false"
	sent = bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	receviedChangesRequestWg.Add(1)
	subChangesResponse := subChangesRequest.Response()
	assert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

	// Also expect the "changes" profile handler above to be called back again with an empty request that
	// will be ignored since body will be "null"
	receviedChangesRequestWg.Add(1)

	// Wait until we got the expected callback on the "changes" profile handler
	receviedChangesRequestWg.Wait()

}

// Start subChanges w/ continuous=true, batchsize=20
// Make several updates
// Wait until we get the expected updates
func TestContinousChangesSubscription(t *testing.T) {

	bt := NewBlipTester()
	defer bt.Close()

	// Counter/Waitgroup to help ensure that all callbacks on continuous changes handler are received
	receviedChangesWg := sync.WaitGroup{}

	// When this test sends subChanges, Sync Gateway will send a changes request that must be handled
	lastReceivedSeq := float64(0)
	var numbatchesReceived int32
	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {

		log.Printf("got changes message: %+v", request)

		body, err := request.Body()
		log.Printf("changes body: %v, err: %v", string(body), err)

		if string(body) != "null" {

			atomic.AddInt32(&numbatchesReceived, 1)

			// Expected changes body: [[1,"foo","1-abc"]]
			changeListReceived := [][]interface{}{}
			err = json.Unmarshal(body, &changeListReceived)
			assertNoError(t, err, "Error unmarshalling changes recevied")

			for _, change := range changeListReceived {

				// The change should have three items in the array
				// [1,"foo","1-abc"]
				assert.True(t, len(change) == 3)

				// Make sure sequence numbers are monotonically increasing
				receivedSeq := change[0].(float64)
				assert.True(t, receivedSeq > lastReceivedSeq)
				lastReceivedSeq = receivedSeq

				// Verify doc id and rev id have expected vals
				docId := change[1].(string)
				assert.True(t, strings.HasPrefix(docId, "foo"))
				assert.Equals(t, change[2], "1-abc") // Rev id of pushed rev

				receviedChangesWg.Done()
			}

		} else {

			receviedChangesWg.Done()

		}

		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := json.Marshal(emptyResponseVal)
			assertNoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}

	}

	// Increment waitgroup since just the act of subscribing to continuous changes will cause
	// the callback changes handler to be invoked with an initial change w/ empty body, signaling that
	// all of the changes have been sent (eg, there are no changes to send)
	receviedChangesWg.Add(1)

	// Send subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.Properties["batch"] = "10" // default batch size is 200, lower this to 10 to make sure we get multiple batches
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	assert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	assert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())

	for i := 1; i < 1500; i++ {
		//// Add a change: Send an unsolicited doc revision in a rev request
		receviedChangesWg.Add(1)
		_, _, revResponse := bt.SendRev(
			fmt.Sprintf("foo-%d", i),
			"	1-abc",
			[]byte(`{"key": "val"}`),
		)

		_, err := revResponse.Body()
		assertNoError(t, err, "Error unmarshalling response body")

	}

	// Wait until all expected changes are received by change handler
	receviedChangesWg.Wait()

	// Since batch size was set to 10, and 15 docs were added, expect at _least_ 2 batches
	numBatchesReceivedSnapshot := atomic.LoadInt32(&numbatchesReceived)
	assert.True(t, numBatchesReceivedSnapshot >= 2)

}

// Push proposed changes and ensure that the server accepts them
//
// 1. Start sync gateway in no-conflicts mode
// 2. Send changes push request with multiple doc revisions
// 3. Make sure there are no panics
// 4. Make sure that the server responds to accept the changes (empty array)
func TestProposedChangesNoConflictsMode(t *testing.T) {

	bt := NewBlipTesterFromSpec(BlipTesterSpec{
		noConflictsMode: true,
	})
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
	assert.True(t, sent)
	proposeChangesResponse := proposeChangesRequest.Response()
	body, err := proposeChangesResponse.Body()
	assertNoError(t, err, "Error getting changes response body")

	var changeList [][]interface{}
	err = json.Unmarshal(body, &changeList)
	assertNoError(t, err, "Error getting changes response body")

	// The common case of an empty array response tells the sender to send all of the proposed revisions,
	// so the changeList returned by Sync Gateway is expected to be empty
	assert.True(t, len(changeList) == 0)

}

// Connect to public port with authentication
func TestPublicPortAuthentication(t *testing.T) {

	// Create bliptester that is connected as user1, with access to the user1 channel
	btUser1 := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:       true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	defer btUser1.Close()

	// Send the user1 doc
	btUser1.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["user1"]}`),
	)

	// Create bliptester that is connected as user2, with access to the * channel
	btUser2 := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:                true,
		connectingUsername:          "user2",
		connectingPassword:          "1234",
		connectingUserChannelGrants: []string{"*"},      // user2 has access to all channels
		restTester:                  btUser1.restTester, // re-use rest tester, otherwise it will create a new underlying bucket in walrus case
	})
	defer btUser2.Close()

	// Send the user2 doc, which is in a "random" channel, but it should be accessible due to * channel access
	btUser2.SendRev(
		"foo2",
		"1-abcd",
		[]byte(`{"key": "val", "channels": ["NBC"]}`),
	)

	// Assert that user1 received a single expected change
	changesChannelUser1 := btUser1.GetChanges()
	assert.True(t, len(changesChannelUser1) == 1)
	change := changesChannelUser1[0]
	assert.True(t, strings.Contains(change[1].(string), "foo"))
	assert.True(t, strings.Contains(change[2].(string), "1-abc"))

	// Assert that user2 received user1's change as well as it's own change
	changesChannelUser2 := btUser2.GetChanges()
	assert.True(t, len(changesChannelUser2) == 2)
	change = changesChannelUser2[0]
	assert.True(t, strings.Contains(change[1].(string), "foo"))
	assert.True(t, strings.Contains(change[2].(string), "1-abc"))
	change = changesChannelUser2[1]
	assert.True(t, strings.Contains(change[1].(string), "foo2"))
	assert.True(t, strings.Contains(change[2].(string), "1-abcd"))

}

// Test adding / retrieving attachments
func TestAttachments(t *testing.T) {

}

// Make sure it's not possible to have two outstanding subChanges w/ continuous=true.
// Expected behavior is that the first continous change subscription should get discarded in favor of 2nd.
func TestConcurrentChangesSubscriptions(t *testing.T) {

}

// Create a continous changes subscription that has docs in multiple channels, and make sure
// all docs are received
func TestMultiChannelContinousChangesSubscription(t *testing.T) {

}

// Test setting and getting checkpoints
func TestCheckpoints(t *testing.T) {

}

// Test no-conflicts mode replication (proposeChanges endpoint)
func TestNoConflictsModeReplication(t *testing.T) {

}

// Reproduce issue where ReloadUser was not being called, and so it was
// using a stale channel access grant for the user.
// Reproduces https://github.com/couchbase/sync_gateway/issues/2717
func TestReloadUser(t *testing.T) {

	base.EnableLogKey("*")
	base.EnableLogKey("Access")
	base.EnableLogKey("Access+")
	base.EnableLogKey("Changes")
	base.EnableLogKey("Changes+")

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
	bt := NewBlipTesterFromSpec(BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	})
	defer bt.Close()

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/db/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Add a doc in the PBS channel
	_, _, addRevResponse := bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
	)

	// Make assertions on response to make sure the change was accepted
	addRevResponseBody, err := addRevResponse.Body()
	assertNoError(t, err, "Unexpected error")
	errorCode, hasErrorCode := addRevResponse.Properties["Error-Code"]
	assert.False(t, hasErrorCode)
	if hasErrorCode {
		t.Fatalf("Unexpected error sending revision.  Error code: %v.  Response body: %s", errorCode, addRevResponseBody)
	}

}

// Grant a user access to a channel via the Sync Function and a doc change, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaSyncFunction(t *testing.T) {

	// Setup
	rt := RestTester{
		SyncFn:       `function(doc) {channel(doc.channels); access(doc.accessUser, doc.accessChannel);}`,
		noAdminParty: true,
	}
	bt := NewBlipTesterFromSpec(BlipTesterSpec{
		connectingUsername: "user1",
		connectingPassword: "1234",
		restTester:         &rt,
	})
	defer bt.Close()

	// Add a doc in the PBS channel
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
	)

	// Put document that triggers access grant for user to channel PBS
	response := rt.SendAdminRequest("PUT", "/db/access1", `{"accessUser":"user1", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Add another doc in the PBS channel
	bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
	)

	// Make sure we can see it by getting changes
	changes := bt.GetChanges()
	log.Printf("changes: %+v", changes)
	assert.True(t, len(changes) == 2)

}

// Grant a user access to a channel via the REST Admin API, and make sure
// it shows up in the user's changes feed
func TestAccessGrantViaAdminApi(t *testing.T) {

	// Create blip tester
	bt := NewBlipTesterFromSpec(BlipTesterSpec{
		noAdminParty:       true,
		connectingUsername: "user1",
		connectingPassword: "1234",
	})
	defer bt.Close()

	// Add a doc in the PBS channel
	bt.SendRev(
		"foo",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
	)

	// Update the user doc to grant access to PBS
	response := bt.restTester.SendAdminRequest("PUT", "/db/_user/user1", `{"admin_channels":["user1", "PBS"]}`)
	assertStatus(t, response, 200)

	// Add another doc in the PBS channel
	bt.SendRev(
		"foo2",
		"1-abc",
		[]byte(`{"key": "val", "channels": ["PBS"]}`),
	)

	// Make sure we can see both docs in the changes
	changes := bt.GetChanges()
	assert.True(t, len(changes) == 2)

}
