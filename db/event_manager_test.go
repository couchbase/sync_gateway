package db

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	goassert "github.com/couchbaselabs/go.assert"
)

// Webhook tests use an HTTP listener.  Use of this listener is disabled by default, to avoid
// port conflicts/permission issues when running tests on a non-dev machine.
const testLiveHTTP = true

// Testing handler tracks received events in ResultChannel
type TestingHandler struct {
	receivedType  EventType
	payload       Body
	ResultChannel chan interface{} // channel for tracking async results
	HandledEvent  EventType
	handleDelay   int        // long running handler execution
	t             *testing.T //enclosing test instance
}

func (th *TestingHandler) HandleEvent(event Event) {

	if th.handleDelay > 0 {
		time.Sleep(time.Duration(th.handleDelay) * time.Millisecond)
	}
	if dceEvent, ok := event.(*DocumentChangeEvent); ok {
		th.ResultChannel <- dceEvent.DocBytes
	}

	if dsceEvent, ok := event.(*DBStateChangeEvent); ok {

		doc := dsceEvent.Doc

		goassert.Equals(th.t, len(doc), 5)

		state := doc["state"]

		//state must be online or offline
		goassert.True(th.t, state != nil && (state == "online" || state == "offline"))

		//admin interface must resolve to a a valis tcp address
		adminInterface := (doc["admininterface"]).(string)

		_, err := net.ResolveTCPAddr("tcp", adminInterface)

		goassert.Equals(th.t, err, nil)

		//localtime must parse from an ISO8601 Format string
		localtime := (doc["localtime"]).(string)

		_, err = time.Parse(base.ISO8601Format, localtime)

		goassert.Equals(th.t, err, nil)

		th.ResultChannel <- dsceEvent.Doc
	}
	return
}

func (th *TestingHandler) SetChannel(channel chan interface{}) {
	th.ResultChannel = channel
}

func (th *TestingHandler) String() string {
	return "Testing Handler"
}

func TestDocumentChangeEvent(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	// Setup test data
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}
	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[i], channelSet
	}
	resultChannel := make(chan interface{}, 10)
	//Setup test handler
	testHandler := &TestingHandler{HandledEvent: DocumentChange}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)
	//Raise events
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}

	assertChannelLengthWithTimeout(t, resultChannel, 10, 10*time.Second)

}

func TestDBStateChangeEvent(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	// Setup test data
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("db%d", i)
	}

	resultChannel := make(chan interface{}, 20)
	//Setup test handler
	testHandler := &TestingHandler{HandledEvent: DBStateChange, t: t}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DBStateChange)
	//Raise online events
	for i := 0; i < 10; i++ {
		em.RaiseDBStateChangeEvent(ids[i], "online", "DB started from config", "0.0.0.0:0000")
	}
	//Raise offline events
	for i := 10; i < 20; i++ {
		em.RaiseDBStateChangeEvent(ids[i], "offline", "Sync Gateway context closed", "0.0.0.0:0000")
	}

	for i := 0; i < 25; i++ {
		if len(resultChannel) == 20 {
			break
		}
	}

	assertChannelLengthWithTimeout(t, resultChannel, 20, 10*time.Second)

}

// Test sending many events with slow-running execution to validate they get dropped after hitting
// the max concurrent goroutines
func TestSlowExecutionProcessing(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyEvents)()

	em := NewEventManager()
	em.Start(0, -1)

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[i], channelSet
	}

	resultChannel := make(chan interface{}, 100)
	testHandler := &TestingHandler{HandledEvent: DocumentChange, handleDelay: 500}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)

	for i := 0; i < 20; i++ {
		body, docid, channels := eventForTest(i % 10)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}

	assertChannelLengthWithTimeout(t, resultChannel, 20, 10*time.Second)

}

func TestCustomHandler(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[i], channelSet
	}

	resultChannel := make(chan interface{}, 20)

	testHandler := &TestingHandler{HandledEvent: DocumentChange}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)

	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}

	assertChannelLengthWithTimeout(t, resultChannel, 10, 10*time.Second)

}

func TestUnhandledEvent(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[i], channelSet
	}

	resultChannel := make(chan interface{}, 10)

	// create handler for UserAdd events
	testHandler := &TestingHandler{HandledEvent: UserAdd}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, UserAdd)

	// send DocumentChange events to handler
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}

	// Validate that no events were handled
	assertChannelLengthWithTimeout(t, resultChannel, 0, 10*time.Second)

}

func GetRouterWithHandler(counter *int, sum *float64, payloads *[][]byte) http.Handler {
	r := http.NewServeMux()
	r.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(1000 * time.Millisecond)
		*counter++
		fmt.Fprintf(w, "OK")
	})
	r.HandleFunc("/slow_2s", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		*counter++
		fmt.Fprintf(w, "OK")
	})
	r.HandleFunc("/slow_5s", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Second)
		*counter++
		fmt.Fprintf(w, "OK")
	})
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Printf("Error trying to read body: %s", err)
		}
		if len(body) > 0 {
			*payloads = append(*payloads, body)
			var payload map[string]interface{}
			json.Unmarshal(body, &payload)
			floatValue, ok := payload["value"].(float64)
			if ok {
				*sum = *sum + floatValue
			}
		}
		if len(r.Form) > 0 {
			log.Printf("Handled request with form: %v", r.Form)
			floatValue, err := strconv.ParseFloat(r.Form.Get("value"), 64)
			if err == nil {
				*sum = *sum + floatValue
			}
		}
		fmt.Fprintf(w, "OK")
		*counter++
	})
	return r
}

func InitWebhookTest() (*int, *float64, *[][]byte, *httptest.Server) {

	// Uses counter and sum values for simplified tracking of POST requests recieved by HTTP
	// TODO:  enhance by adding listener for /count, /sum, /reset endpoints, and leave
	// all management within the function, instead of sharing pointer references around
	counter := 0
	sum := 0.0
	var payloads [][]byte
	// Start HTTP listener for webhook calls
	ts := httptest.NewServer(GetRouterWithHandler(&counter, &sum, &payloads))
	return &counter, &sum, &payloads, ts

}

func TestWebhookBasic(t *testing.T) {

	if !testLiveHTTP {
		return
	}
	count, sum, payloads, ts := InitWebhookTest()
	defer ts.Close()
	url := ts.URL

	ids := make([]string, 200)
	for i := 0; i < 200; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	time.Sleep(1 * time.Second)
	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[i], channelSet
	}

	// Test basic webhook
	em := NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ := NewWebhook(fmt.Sprintf("%s/echo", url), "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}
	time.Sleep(50 * time.Millisecond)
	goassert.Equals(t, *count, 10)

	// Test webhook filter function
	log.Println("Test filter function")
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(0, -1)
	filterFunction := `function(doc) {
							if (doc.value < 6) {
								return false;
							} else {
								return true;
							}
							}`
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/echo", url), filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}
	time.Sleep(50 * time.Millisecond)
	goassert.Equals(t, *count, 4)

	// Validate payload
	*count, *sum, *payloads = 0, 0.0, nil
	em = NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/echo", url), "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	body, docid, channels := eventForTest(0)
	bodyBytes, _ := base.JSONMarshal(body)
	em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	time.Sleep(50 * time.Millisecond)
	receivedPayload := string((*payloads)[0])
	fmt.Println("payload:", receivedPayload)
	goassert.Equals(t, string((*payloads)[0]), `{"_id":"0","value":0}`)
	goassert.Equals(t, *count, 1)

	// Test fast fill, fast webhook
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(5, -1)
	timeout := uint64(60)
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/echo", url), "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, docid, channels := eventForTest(i % 10)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}
	time.Sleep(500 * time.Millisecond)
	goassert.Equals(t, *count, 100)

	// Test queue full, slow webhook.  Drops events
	log.Println("Test queue full, slow webhook")
	*count, *sum = 0, 0.0
	errCount := 0
	em = NewEventManager()
	em.Start(5, -1)
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/slow", url), "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		err := em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	time.Sleep(5 * time.Second)
	// Expect 21 to complete.  5 get goroutines immediately, 15 get queued, and one is blocked waiting
	// for a goroutine.  The rest get discarded because the queue is full.
	goassert.Equals(t, *count, 21)
	goassert.Equals(t, errCount, 79)

	// Test queue full, slow webhook, long wait time.  Throttles events
	log.Println("Test queue full, slow webhook, long wait")
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(5, 1100)
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/slow", url), "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, docid, channels := eventForTest(i % 10)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}
	time.Sleep(5 * time.Second)
	goassert.Equals(t, *count, 100)

}

// Abs returns the absolute value of x.
func Abs(x int64) int64 {
	y := x >> 63
	return (x ^ y) - y
}

/*
 * Test Webhook where there is an old doc revision and where the filter function
 * is expecting an old doc revision
 */
func TestWebhookOldDoc(t *testing.T) {

	if !testLiveHTTP {
		return
	}
	count, sum, _, ts := InitWebhookTest()
	defer ts.Close()
	url := ts.URL

	ids := make([]string, 200)
	for i := 0; i < 200; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	time.Sleep(1 * time.Second)
	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[Abs(int64(i))],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[Abs(int64(i))], channelSet
	}

	// Test basic webhook where an old doc is passed but not filtered
	em := NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ := NewWebhook(fmt.Sprintf("%s/echo", url), "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		oldBody, olddocid, _ := eventForTest(-i)
		oldBody[BodyId] = olddocid
		oldBodyBytes, _ := base.JSONMarshal(oldBody)
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, string(oldBodyBytes), channels)

	}
	time.Sleep(50 * time.Millisecond)
	goassert.Equals(t, *count, 10)

	// Test webhook where an old doc is passed and is not used by the filter
	log.Println("Test filter function with old doc which is not referenced")
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(0, -1)
	filterFunction := `function(doc) {
							if (doc.value < 6) {
								return false;
							} else {
								return true;
							}
							}`
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/echo", url), filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		oldBody, olddocid, _ := eventForTest(-i)
		oldBody[BodyId] = olddocid
		oldBodyBytes, _ := base.JSONMarshal(oldBody)
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, string(oldBodyBytes), channels)
	}
	time.Sleep(50 * time.Millisecond)
	goassert.Equals(t, *count, 4)

	// Test webhook where an old doc is passed and is validated by the filter
	log.Println("Test filter function with old doc")
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(0, -1)
	filterFunction = `function(doc, oldDoc) {
							if (doc.value > 6 && doc.value = -oldDoc.value) {
								return false;
							} else {
								return true;
							}
							}`
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/echo", url), filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		oldBody, olddocid, _ := eventForTest(-i)
		oldBody[BodyId] = olddocid
		oldBodyBytes, _ := base.JSONMarshal(oldBody)
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, string(oldBodyBytes), channels)
	}
	time.Sleep(50 * time.Millisecond)
	goassert.Equals(t, *count, 4)

	// Test webhook where an old doc is not passed but is referenced in the filter function args
	log.Println("Test filter function with old doc")
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(0, -1)
	filterFunction = `function(doc, oldDoc) {
							if (oldDoc) {
								return true;
							} else {
								return false;
							}
							}`
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/echo", url), filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}
	for i := 10; i < 20; i++ {
		oldBody, olddocid, _ := eventForTest(-i)
		oldBody[BodyId] = olddocid
		oldBodyBytes, _ := base.JSONMarshal(oldBody)
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, string(oldBodyBytes), channels)
	}
	time.Sleep(50 * time.Millisecond)
	goassert.Equals(t, *count, 10)

}

func TestWebhookTimeout(t *testing.T) {
	if !testLiveHTTP {
		return
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyEvents)()

	count, sum, _, ts := InitWebhookTest()
	defer ts.Close()
	url := ts.URL

	ids := make([]string, 200)
	for i := 0; i < 200; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	time.Sleep(1 * time.Second)
	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[i], channelSet
	}

	// Test fast execution, short timeout.  All events processed
	log.Println("Test fast webhook, short timeout")
	em := NewEventManager()
	em.Start(0, -1)
	timeout := uint64(2)
	webhookHandler, _ := NewWebhook(fmt.Sprintf("%s/echo", url), "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}
	time.Sleep(50 * time.Millisecond)
	goassert.Equals(t, *count, 10)

	// Test slow webhook, short timeout, numProcess=1, waitForProcess > timeout.  All events should get processed.
	log.Println("Test slow webhook, short timeout")
	*count, *sum = 0, 0.0
	errCount := 0
	em = NewEventManager()
	em.Start(1, 1100)
	timeout = uint64(1)
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/slow_2s", url), "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		err := em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	time.Sleep(15 * time.Second)
	// Even though we timed out waiting for response on the SG side, POST still completed on target side.
	goassert.Equals(t, *count, 10)

	// Test slow webhook, short timeout, numProcess=1, waitForProcess << timeout.  Events that don't fit in queues
	// should get dropped (1 immediately processed, 1 in normal queue, 3 in overflow queue, 5 dropped)
	log.Println("Test very slow webhook, short timeout")
	*count, *sum = 0, 0.0
	errCount = 0
	em = NewEventManager()
	em.Start(1, 100)
	timeout = uint64(9)
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/slow_5s", url), "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		err := em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	// wait for slow webhook to finish processing
	time.Sleep(25 * time.Second)
	goassert.Equals(t, *count, 5)

	// Test slow webhook, no timeout, numProcess=1, waitForProcess=1s.  All events should complete.
	log.Println("Test slow webhook, no timeout, wait for process ")
	*count, *sum = 0, 0.0
	errCount = 0
	em = NewEventManager()
	em.Start(1, 1100)
	timeout = uint64(0)
	webhookHandler, _ = NewWebhook(fmt.Sprintf("%s/slow", url), "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		err := em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	// wait for slow webhook to finish processing
	time.Sleep(15 * time.Second)
	goassert.Equals(t, *count, 10)

}

func TestUnavailableWebhook(t *testing.T) {

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, string, base.Set) {
		testBody := Body{
			BodyId:  ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, ids[i], channelSet
	}

	// Test unreachable webhook
	em := NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ := NewWebhook("http://badhost:1000/echo", "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, docid, channels := eventForTest(i)
		bodyBytes, _ := base.JSONMarshal(body)
		em.RaiseDocumentChangeEvent(bodyBytes, docid, "", channels)
	}

}

// asserts that the number of items seen in the channel within the specified time limit is the same as the expected value.
// WARNING: This function will drain the channel of items!
func assertChannelLengthWithTimeout(t *testing.T, c chan interface{}, expectedLength int, timeout time.Duration) {
	count := 0
	for {
		if count >= expectedLength {
			// Make sure there are no additional items on the channel after a short wait.
			// This avoids relying on the longer timeout value for the final check.
			time.Sleep(timeout / 100)
			goassert.Equals(t, count+len(c), expectedLength)
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
