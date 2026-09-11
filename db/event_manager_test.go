/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

const DefaultWaitForWebhook = time.Second * 5

// Testing handler tracks received events in ResultChannel
type TestingHandler struct {
	ResultChannel chan any // channel for tracking async results
	HandledEvent  EventType
	handleDelay   int        // long running handler execution
	t             *testing.T // enclosing test instance
}

func (th *TestingHandler) HandleEvent(_ context.Context, event Event) bool {

	if th.handleDelay > 0 {
		time.Sleep(time.Duration(th.handleDelay) * time.Millisecond)
	}

	if dceEvent, ok := event.(*DocumentChangeEvent); ok {
		th.ResultChannel <- dceEvent.DocBytes
	}

	if dsceEvent, ok := event.(*DBStateChangeEvent); ok {
		doc := dsceEvent.Doc
		assert.Equal(th.t, 5, len(doc))

		state := doc["state"]
		// state must be online or offline
		assert.True(th.t, state != nil && (state == "online" || state == "offline"))

		// admin interface must resolve to a a valis tcp address
		adminInterface := (doc["admininterface"]).(string)
		_, err := net.ResolveTCPAddr("tcp", adminInterface)
		assert.NoError(th.t, err)

		// localtime must parse from an ISO8601 Format string
		localtime := (doc["localtime"]).(string)
		_, err = time.Parse(base.ISO8601Format, localtime)
		assert.NoError(th.t, err)

		th.ResultChannel <- dsceEvent.Doc
	}
	return true
}

func (th *TestingHandler) SetChannel(channel chan any) {
	th.ResultChannel = channel
}

func (th *TestingHandler) String() string {
	return "Testing Handler"
}

// docChangeEventBody returns the body, doc ID and channel set for the i'th test event.  value is stored in the body, so
// that webhook filter functions can select on it.
func docChangeEventBody(i int, value any) (Body, string, base.Set) {
	docID := strconv.Itoa(i)
	channel := "Odd"
	if i%2 == 0 {
		channel = "Even"
	}
	body := Body{
		BodyId:  docID,
		"value": value,
	}
	return body, docID, base.SetFromArray([]string{channel})
}

// raiseDocChangeEvents raises document change events for doc IDs in the range [from, to), and returns the number of
// events dropped because the event queue was full.  Events are raised with a short pause between them, so that they
// reach the queue in a predictable order when the queue is under pressure.
func raiseDocChangeEvents(ctx context.Context, t *testing.T, em *EventManager, from, to int) (droppedCount int) {
	t.Helper()
	for i := from; i < to; i++ {
		body, docID, channels := docChangeEventBody(i, i)
		bodyBytes := base.MustJSONMarshal(t, body)
		if err := em.RaiseDocumentChangeEvent(ctx, bodyBytes, docID, "", channels, false); err != nil {
			droppedCount++
		}
		time.Sleep(2 * time.Millisecond)
	}
	return droppedCount
}

// raiseDocChangeEventsWithOldDoc raises document change events for doc IDs in the range [from, to), each carrying an
// old revision whose value is the negation of the new value.
func raiseDocChangeEventsWithOldDoc(ctx context.Context, t *testing.T, em *EventManager, from, to int) {
	t.Helper()
	for i := from; i < to; i++ {
		oldBody, _, _ := docChangeEventBody(i, -i)
		oldBodyBytes := base.MustJSONMarshal(t, oldBody)
		body, docID, channels := docChangeEventBody(i, i)
		bodyBytes := base.MustJSONMarshal(t, body)
		require.NoError(t, em.RaiseDocumentChangeEvent(ctx, bodyBytes, docID, string(oldBodyBytes), channels, false))
	}
}

func TestDocumentChangeEvent(t *testing.T) {
	ctx := base.TestCtx(t)
	terminator := make(chan bool)
	defer close(terminator)

	em := NewEventManager(terminator)
	em.Start(ctx, 0, -1)

	resultChannel := make(chan any, 10)
	testHandler := &TestingHandler{HandledEvent: DocumentChange}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(ctx, testHandler, DocumentChange)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))

	assertChannelLengthWithTimeout(t, resultChannel, 10, 10*time.Second)
}

func TestDBStateChangeEvent(t *testing.T) {
	ctx := base.TestCtx(t)
	terminator := make(chan bool)
	defer close(terminator)

	em := NewEventManager(terminator)
	em.Start(ctx, 0, -1)

	// Setup test data
	ids := make([]string, 20)
	for i := range 20 {
		ids[i] = fmt.Sprintf("db%d", i)
	}

	resultChannel := make(chan any, 20)
	// Setup test handler
	testHandler := &TestingHandler{HandledEvent: DBStateChange, t: t}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(ctx, testHandler, DBStateChange)
	// Raise online events
	for i := range 10 {
		err := em.RaiseDBStateChangeEvent(ctx, ids[i], "online", "DB started from config", base.Ptr("0.0.0.0:0000"))
		assert.NoError(t, err)
	}
	// Raise offline events
	for i := 10; i < 20; i++ {
		err := em.RaiseDBStateChangeEvent(ctx, ids[i], "offline", "Sync Gateway context closed", base.Ptr("0.0.0.0:0000"))
		assert.NoError(t, err)
	}

	assertChannelLengthWithTimeout(t, resultChannel, 20, 10*time.Second)
}

// Test sending many events with slow-running execution to validate they get dropped after hitting
// the max concurrent goroutines
func TestSlowExecutionProcessing(t *testing.T) {
	base.LongRunningTest(t)

	ctx := base.TestCtx(t)
	terminator := make(chan bool)
	defer close(terminator)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyEvents)

	em := NewEventManager(terminator)
	em.Start(ctx, 0, -1)

	resultChannel := make(chan any, 100)
	testHandler := &TestingHandler{HandledEvent: DocumentChange, handleDelay: 500}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(ctx, testHandler, DocumentChange)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 20))

	assertChannelLengthWithTimeout(t, resultChannel, 20, 10*time.Second)
}

func TestCustomHandler(t *testing.T) {
	ctx := base.TestCtx(t)
	terminator := make(chan bool)
	defer close(terminator)

	em := NewEventManager(terminator)
	em.Start(ctx, 0, -1)

	resultChannel := make(chan any, 20)
	testHandler := &TestingHandler{HandledEvent: DocumentChange}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(ctx, testHandler, DocumentChange)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))

	assertChannelLengthWithTimeout(t, resultChannel, 10, 10*time.Second)
}

func TestUnhandledEvent(t *testing.T) {
	ctx := base.TestCtx(t)
	terminator := make(chan bool)
	defer close(terminator)

	em := NewEventManager(terminator)
	em.Start(ctx, 0, -1)

	resultChannel := make(chan any, 10)

	// create handler for an unhandled event
	testHandler := &TestingHandler{HandledEvent: math.MaxUint8}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(ctx, testHandler, math.MaxUint8)

	// send DocumentChange events to handler
	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))

	// Validate that no events were handled
	assertChannelLengthWithTimeout(t, resultChannel, 0, 10*time.Second)
}

// Uses WebhookRequest for simplified tracking of POST requests received by HTTP.
// A mutex has been embedded in WebhookRequest to avoid race conditions.
type WebhookRequest struct {
	mutex    sync.Mutex
	count    int
	payloads [][]byte
}

func (wr *WebhookRequest) GetCount() int {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()
	return wr.count
}

func (wr *WebhookRequest) IncrementCount() int {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()
	wr.count++
	return wr.count
}

func (wr *WebhookRequest) GetPayloads() [][]byte {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()
	return wr.payloads
}

func (wr *WebhookRequest) AddPayload(payload []byte) {
	wr.mutex.Lock()
	defer wr.mutex.Unlock()
	wr.payloads = append(wr.payloads, payload)
}

func (em *EventManager) waitForProcessedTotal(t testing.TB, waitCount int, maxWaitTime time.Duration) {
	t.Helper()
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.GreaterOrEqual(c, em.GetEventsProcessedSuccess()+em.GetEventsProcessedFail(), int64(waitCount))
	}, maxWaitTime, 10*time.Millisecond)
}

func GetRouterWithHandler(wr *WebhookRequest) http.Handler {
	r := http.NewServeMux()
	r.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1 * time.Second)
		wr.IncrementCount()
		_, _ = fmt.Fprintf(w, "OK")
	})
	r.HandleFunc("/slow_2s", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		wr.IncrementCount()
		_, _ = fmt.Fprintf(w, "OK")
	})
	r.HandleFunc("/slow_5s", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		wr.IncrementCount()
		_, _ = fmt.Fprintf(w, "OK")
	})
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error trying to read body: %s", err)
		}
		if len(body) > 0 {
			wr.AddPayload(body)
		}
		wr.IncrementCount()
		_, _ = fmt.Fprintf(w, "OK")
	})
	return r
}

func InitWebhookTest() (*httptest.Server, *WebhookRequest) {
	wr := &WebhookRequest{}
	ts := httptest.NewServer(GetRouterWithHandler(wr))
	return ts, wr
}

// newWebhookTest starts a webhook test server and an event manager terminator, both torn down when the test ends.
func newWebhookTest(t *testing.T) (url string, wr *WebhookRequest, terminator chan bool) {
	ts, wr := InitWebhookTest()
	t.Cleanup(ts.Close)
	terminator = make(chan bool)
	t.Cleanup(func() { close(terminator) })
	return ts.URL, wr, terminator
}

// newWebhookEventManager returns a started event manager with a single webhook handler registered for document change
// events.
func newWebhookEventManager(ctx context.Context, t *testing.T, terminator chan bool, url string, filterFunction string, timeout *uint64, maxProcesses uint, waitTime int) *EventManager {
	t.Helper()
	em := NewEventManager(terminator)
	em.Start(ctx, maxProcesses, waitTime)
	webhookHandler, err := NewWebhook(ctx, url, filterFunction, timeout, nil)
	require.NoError(t, err)
	em.RegisterEventHandler(ctx, webhookHandler, DocumentChange)
	return em
}

// Test that all events are posted to a webhook with no filter function.
func TestWebhookBasic(t *testing.T) {
	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), "", nil, 0, -1)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))
	em.waitForProcessedTotal(t, 10, DefaultWaitForWebhook)
	assert.Equal(t, int64(10), em.GetEventsProcessedSuccess())
}

// Test that a filter function stops the events it rejects from being posted.  Only the four events with a value of 6
// or more are posted.
func TestWebhookFilterFunction(t *testing.T) {
	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	filterFunction := `function(doc) {
							if (doc.value < 6) {
								return false;
							} else {
								return true;
							}
							}`
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), filterFunction, nil, 0, -1)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))
	em.waitForProcessedTotal(t, 10, DefaultWaitForWebhook)
	assert.Equal(t, int64(4), em.GetEventsProcessedSuccess())
}

// Test the payload posted to a webhook.
func TestWebhookPayload(t *testing.T) {
	url, wr, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), "", nil, 0, -1)

	body, docID, channels := docChangeEventBody(0, 0)
	bodyBytes, err := base.JSONMarshalCanonical(body)
	require.NoError(t, err)
	require.NoError(t, em.RaiseDocumentChangeEvent(ctx, bodyBytes, docID, "", channels, false))

	em.waitForProcessedTotal(t, 1, DefaultWaitForWebhook)
	require.Len(t, wr.GetPayloads(), 1)
	assert.Equal(t, `{"_id":"0","value":0}`, string(wr.GetPayloads()[0]))
}

// Test that a fast webhook keeps up with events raised as fast as possible.
func TestWebhookOverflowFastWebhook(t *testing.T) {
	t.Skip("Test skipped, re-enable once CBG-2281 is fixed")

	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	timeout := uint64(60)
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), "", &timeout, 5, -1)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 100))
	em.waitForProcessedTotal(t, 100, DefaultWaitForWebhook)
	assert.Equal(t, int64(100), em.GetEventsProcessedSuccess())
}

// Test that a slow webhook with a short queue wait time drops the events that don't fit in the queues.  Five events
// get goroutines immediately, fifteen get queued, one is blocked waiting for a goroutine, and the rest are dropped.
func TestWebhookOverflowQueueFull(t *testing.T) {
	t.Skip("Test skipped, re-enable once CBG-2281 is fixed")

	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/slow", url), "", nil, 5, 1)

	require.Equal(t, 79, raiseDocChangeEvents(ctx, t, em, 0, 100))
	em.waitForProcessedTotal(t, 21, 10*time.Second)
	assert.Equal(t, int64(21), em.GetEventsProcessedSuccess())
}

// Test that a slow webhook with a long queue wait time throttles events instead of dropping them.
func TestWebhookOverflowQueueFullLongWait(t *testing.T) {
	t.Skip("Test skipped, re-enable once CBG-2281 is fixed")

	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/slow", url), "", nil, 5, 1500)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 100))
	em.waitForProcessedTotal(t, 100, 10*time.Second)
	assert.Equal(t, int64(100), em.GetEventsProcessedSuccess())
}

// Test that an old doc is accepted by a webhook with no filter function.
func TestWebhookOldDocNoFilter(t *testing.T) {
	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), "", nil, 0, -1)

	raiseDocChangeEventsWithOldDoc(ctx, t, em, 0, 10)
	em.waitForProcessedTotal(t, 10, DefaultWaitForWebhook)
	assert.Equal(t, int64(10), em.GetEventsProcessedSuccess())
}

// Test that an old doc is accepted when the filter function doesn't reference it.
func TestWebhookOldDocFilterWithoutOldDoc(t *testing.T) {
	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	filterFunction := `function(doc) {
							if (doc.value < 6) {
								return false;
							} else {
								return true;
							}
							}`
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), filterFunction, nil, 0, -1)

	raiseDocChangeEventsWithOldDoc(ctx, t, em, 0, 10)
	em.waitForProcessedTotal(t, 10, DefaultWaitForWebhook)
	assert.Equal(t, int64(4), em.GetEventsProcessedSuccess())
}

// Test a filter function that selects on the old doc as well as the new one.
func TestWebhookOldDocFilterWithOldDoc(t *testing.T) {
	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	filterFunction := `function(doc, oldDoc) {
							if (doc.value < 6 && doc.value == -oldDoc.value) {
								return false;
							} else {
								return true;
							}
							}`
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), filterFunction, nil, 0, -1)

	raiseDocChangeEventsWithOldDoc(ctx, t, em, 0, 10)
	em.waitForProcessedTotal(t, 10, DefaultWaitForWebhook)
	assert.Equal(t, int64(4), em.GetEventsProcessedSuccess())
}

// Test a filter function that references an old doc, for events that don't all carry one.  Only the ten events with an
// old doc are posted.
func TestWebhookOldDocMissingOldDoc(t *testing.T) {
	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	filterFunction := `function(doc, oldDoc) {
							if (oldDoc) {
								return true;
							} else {
								return false;
							}
							}`
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), filterFunction, nil, 0, -1)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))
	raiseDocChangeEventsWithOldDoc(ctx, t, em, 10, 20)
	em.waitForProcessedTotal(t, 20, DefaultWaitForWebhook)
	assert.Equal(t, int64(10), em.GetEventsProcessedSuccess())
}

// Test fast webhook execution with a short timeout.  All events are processed successfully.
func TestWebhookTimeoutFastWebhook(t *testing.T) {
	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	timeout := uint64(2)
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/echo", url), "", &timeout, 0, -1)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))
	em.waitForProcessedTotal(t, 10, DefaultWaitForWebhook)
	assert.Equal(t, int64(10), em.GetEventsProcessedSuccess())
}

// Test a webhook that is slower than its timeout, with a single processing slot.  Every event fails, but none are
// dropped, because the 1s webhook timeout frees the slot well inside the queue wait time.
func TestWebhookTimeoutSlowWebhook(t *testing.T) {
	base.LongRunningTest(t)

	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	timeout := uint64(1)
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/slow_2s", url), "", &timeout, 1, 5000)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))
	em.waitForProcessedTotal(t, 10, 30*time.Second)
	assert.Equal(t, int64(0), em.GetEventsProcessedSuccess())
	assert.Equal(t, int64(10), em.GetEventsProcessedFail())
}

// Test a webhook that is slower than the queue wait time, with a single processing slot.  Only the events that fit in
// the queues are processed - one in progress, one held by the queue worker and three buffered - and the other five are
// dropped.
func TestWebhookTimeoutQueueFull(t *testing.T) {
	base.LongRunningTest(t)

	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	timeout := uint64(9)
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/slow_5s", url), "", &timeout, 1, 100)

	require.Equal(t, 5, raiseDocChangeEvents(ctx, t, em, 0, 10))
	em.waitForProcessedTotal(t, 5, 30*time.Second)
	assert.Equal(t, int64(5), em.GetEventsProcessedSuccess())
}

// Test a slow webhook with no timeout, with a single processing slot.  The queue wait time is well above the 1s the
// webhook takes to free the slot, so events are throttled rather than dropped, and all of them are processed.
func TestWebhookTimeoutNoTimeout(t *testing.T) {
	base.LongRunningTest(t)

	url, _, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	timeout := uint64(0)
	em := newWebhookEventManager(ctx, t, terminator, fmt.Sprintf("%s/slow", url), "", &timeout, 1, 5000)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))
	em.waitForProcessedTotal(t, 10, 20*time.Second)
	assert.Equal(t, int64(10), em.GetEventsProcessedSuccess())
}

func TestUnavailableWebhook(t *testing.T) {
	_, wr, terminator := newWebhookTest(t)
	ctx := base.TestCtx(t)

	em := newWebhookEventManager(ctx, t, terminator, "http://badhost:1000/echo", "", nil, 0, -1)

	require.Equal(t, 0, raiseDocChangeEvents(ctx, t, em, 0, 10))
	// Each event fails on name resolution, which can be slow, so allow well over the time this takes locally.
	em.waitForProcessedTotal(t, 10, 30*time.Second)
	assert.Equal(t, int64(10), em.GetEventsProcessedFail())
	assert.Equal(t, 0, wr.GetCount())
}

// asserts that the number of items seen in the channel within the specified time limit is the same as the expected value.
// WARNING: This function will drain the channel of items!
func assertChannelLengthWithTimeout(t *testing.T, c chan any, expectedLength int, timeout time.Duration) {
	t.Helper()
	count := 0
	for {
		if count >= expectedLength {
			// Make sure there are no additional items on the channel after a short wait.
			// This avoids relying on the longer timeout value for the final check.
			time.Sleep(timeout / 100)
			assert.Equal(t, expectedLength, count+len(c))
			return
		}

		select {
		case _ = <-c:
			count++
		case <-time.After(timeout):
			t.Fatalf("timed out waiting for items on channel... got: %d, expected: %d", count, expectedLength)
		}
	}
}

func mockDBStateChangeEvent(dbName string, state string, reason string, adminInterface string) *DBStateChangeEvent {
	body := make(Body, 5)
	body["dbname"] = dbName
	body["admininterface"] = adminInterface
	body["state"] = state
	body["reason"] = reason
	body["localtime"] = time.Now().Format(base.ISO8601Format)
	event := &DBStateChangeEvent{Doc: body}
	return event
}

type UnsupportedEvent struct {
	AsyncEvent
}

func (event *UnsupportedEvent) String() string {
	return "Couchbase Sync Gateway doesn't support this kind of events!"
}

func (event *UnsupportedEvent) EventType() EventType {
	return EventType(255)
}

// Simulate the scenario for handling unsupported events.
func TestWebhookHandleUnsupportedEventType(t *testing.T) {
	ts, _ := InitWebhookTest()
	defer ts.Close()
	wh := &Webhook{url: ts.URL}
	event := &UnsupportedEvent{}
	success := wh.HandleEvent(base.TestCtx(t), event)
	assert.False(t, success, "Event shouldn't get posted to webhook; event type is not supported")
}

// Simulate the filter function processing abort scenario.
func TestWebhookHandleEventDBStateChangeFilterFuncError(t *testing.T) {
	ts, _ := InitWebhookTest()
	defer ts.Close()
	wh := &Webhook{url: ts.URL}
	event := mockDBStateChangeEvent("db", "online", "Index service is listening", "127.0.0.1:4985")
	source := `function (doc) { invalidKeyword if (doc.state == "online") { return true; } else { return false; } }`
	ctx := base.TestCtx(t)
	wh.filter = NewJSEventFunction(ctx, source)
	success := wh.HandleEvent(ctx, event)
	assert.False(t, success, "Filter function processing should be aborted and warnings should be logged")
}

// Simulate marshalling doc error for webhook post against DBStateChangeEvent
func TestWebhookHandleEventDBStateChangeMarshalDocError(t *testing.T) {
	ts, _ := InitWebhookTest()
	defer ts.Close()
	wh := &Webhook{url: ts.URL}
	body := make(Body, 1)
	body["key"] = make(chan int)
	event := &DBStateChangeEvent{Doc: body}
	success := wh.HandleEvent(base.TestCtx(t), event)
	assert.False(t, success, "It should throw marshalling doc error and log warnings")
}
