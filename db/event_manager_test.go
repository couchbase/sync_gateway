package db

import (
	"encoding/json"
	"fmt"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/go.assert"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// Webhook tests use an HTTP listener.  Use of this listener is disabled by default, to avoid
// port conflicts/permission issues when running tests on a non-dev machine.
const testLiveHTTP = false

// Testing handler tracks received events in ResultChannel
type TestingHandler struct {
	receivedType  EventType
	payload       Body
	ResultChannel chan Body // channel for tracking async results
	HandledEvent  EventType
	handleDelay   int        // long running handler execution
	t             *testing.T //enclosing test instance
}

func (th *TestingHandler) HandleEvent(event Event) {

	if th.handleDelay > 0 {
		time.Sleep(time.Duration(th.handleDelay) * time.Millisecond)
	}
	if dceEvent, ok := event.(*DocumentChangeEvent); ok {
		th.ResultChannel <- dceEvent.Doc
	}

	if dsceEvent, ok := event.(*DBStateChangeEvent); ok {

		doc := dsceEvent.Doc

		assert.True(th.t, len(doc) == 5)

		state := doc["state"]

		//state must be online or offline
		assert.True(th.t, state != nil && (state == "online" || state == "offline"))

		//admin interface must resolve to a a valis tcp address
		adminInterface := (doc["admininterface"]).(string)

		_, err := net.ResolveTCPAddr("tcp", adminInterface)

		assert.True(th.t, err == nil)

		//localtime must parse from an ISO8601 Format string
		localtime := (doc["localtime"]).(string)

		_, err = time.Parse(base.ISO8601Format, localtime)

		assert.True(th.t, err == nil)

		th.ResultChannel <- dsceEvent.Doc
	}
	return
}

func (th *TestingHandler) SetChannel(channel chan Body) {
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
	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}
	resultChannel := make(chan Body, 10)
	//Setup test handler
	testHandler := &TestingHandler{HandledEvent: DocumentChange}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)
	//Raise events
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	// wait for Event Manager queue worker to process
	time.Sleep(100 * time.Millisecond)

	// Diagnostics for failures
	channelSize := len(resultChannel)
	if channelSize != 10 {
		log.Printf("Expected 10 change events, got %v", channelSize)
		for {
			select {
			case result := <-resultChannel:
				log.Printf("Change event: %v", result)
			default:
				break
			}
		}
	}

	assert.True(t, channelSize == 10)

}

func TestDBStateChangeEvent(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	// Setup test data
	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("db%d", i)
	}

	resultChannel := make(chan Body, 20)
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

	// Give the Event Manager queue worker some time to process
	for i := 0; i < 25; i++ {
		if len(resultChannel) == 20 {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	assert.True(t, len(resultChannel) == 20)

}

// Test sending many events with slow-running execution to validate they get dropped after hitting
// the max concurrent goroutines
func TestSlowExecutionProcessing(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	var logKeys = map[string]bool{
		"Events": true,
	}

	base.UpdateLogKeys(logKeys, true)

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}

	resultChannel := make(chan Body, 100)
	testHandler := &TestingHandler{HandledEvent: DocumentChange, handleDelay: 500}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)

	for i := 0; i < 20; i++ {
		body, channels := eventForTest(i % 10)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	// wait for Event Manager queue worker to process
	time.Sleep(2 * time.Second)
	fmt.Println("resultChannel:", len(resultChannel))

	assert.True(t, len(resultChannel) == 20)

}

func TestCustomHandler(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}

	resultChannel := make(chan Body, 20)

	testHandler := &TestingHandler{HandledEvent: DocumentChange}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)

	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	// wait for Event Manager queue worker to process
	time.Sleep(50 * time.Millisecond)

	assert.True(t, len(resultChannel) == 10)

}

func TestUnhandledEvent(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}

	resultChannel := make(chan Body, 10)

	// create handler for UserAdd events
	testHandler := &TestingHandler{HandledEvent: UserAdd}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, UserAdd)

	// send DocumentChange events to handler
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	// Wait for Event Manager queue worker to process
	time.Sleep(50 * time.Millisecond)

	// Validate that no events were handled
	assert.True(t, len(resultChannel) == 0)

}

func InitWebhookTest() (*int, *float64, *[][]byte) {

	// Uses counter and sum values for simplified tracking of POST requests recieved by HTTP
	// TODO:  enhance by adding listener for /count, /sum, /reset endpoints, and leave
	// all management within the function, instead of sharing pointer references around
	counter := 0
	sum := 0.0
	var payloads [][]byte
	// Start HTTP listener for webhook calls
	go func() {
		http.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(1000 * time.Millisecond)
			counter++
			fmt.Fprintf(w, "OK")
		})
		http.HandleFunc("/slow_2s", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(2 * time.Second)
			counter++
			fmt.Fprintf(w, "OK")
		})
		http.HandleFunc("/slow_5s", func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(5 * time.Second)
			counter++
			fmt.Fprintf(w, "OK")
		})
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm()

			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("Error trying to read body: %s", err)
			}

			if len(body) > 0 {
				payloads = append(payloads, body)
				var payload map[string]interface{}
				json.Unmarshal(body, &payload)
				floatValue, ok := payload["value"].(float64)
				if ok {
					sum = sum + floatValue
				}
			}

			if len(r.Form) > 0 {
				log.Printf("Handled request with form: %v", r.Form)
				floatValue, err := strconv.ParseFloat(r.Form.Get("value"), 64)
				if err == nil {
					sum = sum + floatValue
				}
			}

			fmt.Fprintf(w, "OK")
			counter++

		})
		http.ListenAndServe(":8081", nil)
	}()
	return &counter, &sum, &payloads
}

func TestWebhookBasic(t *testing.T) {

	if !testLiveHTTP {
		return
	}
	count, sum, payloads := InitWebhookTest()
	ids := make([]string, 200)
	for i := 0; i < 200; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	time.Sleep(1 * time.Second)
	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}

	// Test basic webhook
	em := NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ := NewWebhook("http://localhost:8081/echo", "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 10)

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
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 4)

	// Validate payload
	*count, *sum, *payloads = 0, 0.0, nil
	em = NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	body, channels := eventForTest(0)
	em.RaiseDocumentChangeEvent(body, "", channels)
	time.Sleep(50 * time.Millisecond)
	receivedPayload := string((*payloads)[0])
	fmt.Println("payload:", receivedPayload)
	assert.Equals(t, string((*payloads)[0]), `{"_id":"0","value":0}`)
	assert.Equals(t, *count, 1)

	// Test fast fill, fast webhook
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(5, -1)
	timeout := uint64(60)
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, channels := eventForTest(i % 10)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	time.Sleep(500 * time.Millisecond)
	assert.Equals(t, *count, 100)

	// Test queue full, slow webhook.  Drops events
	log.Println("Test queue full, slow webhook")
	*count, *sum = 0, 0.0
	errCount := 0
	em = NewEventManager()
	em.Start(5, -1)
	webhookHandler, _ = NewWebhook("http://localhost:8081/slow", "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, channels := eventForTest(i)
		err := em.RaiseDocumentChangeEvent(body, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	time.Sleep(5 * time.Second)
	// Expect 21 to complete.  5 get goroutines immediately, 15 get queued, and one is blocked waiting
	// for a goroutine.  The rest get discarded because the queue is full.
	assert.Equals(t, *count, 21)
	assert.Equals(t, errCount, 79)

	// Test queue full, slow webhook, long wait time.  Throttles events
	log.Println("Test queue full, slow webhook, long wait")
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(5, 1100)
	webhookHandler, _ = NewWebhook("http://localhost:8081/slow", "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, channels := eventForTest(i % 10)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	time.Sleep(5 * time.Second)
	assert.Equals(t, *count, 100)

}

/*
 * Test Webhook where there is an old doc revision and where the filter function
 * is expecting an old doc revision
 */
func TestWebhookOldDoc(t *testing.T) {

	if !testLiveHTTP {
		return
	}
	count, sum, _ := InitWebhookTest()
	ids := make([]string, 200)
	for i := 0; i < 200; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	time.Sleep(1 * time.Second)
	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}

	// Test basic webhook where an old doc is passed but not filtered
	em := NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ := NewWebhook("http://localhost:8081/echo", "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		oldBody, _ := eventForTest(-i)
		oldBodyBytes, _ := json.Marshal(oldBody)
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, string(oldBodyBytes), channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 10)

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
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		oldBody, _ := eventForTest(-i)
		oldBodyBytes, _ := json.Marshal(oldBody)
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, string(oldBodyBytes), channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 4)

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
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		oldBody, _ := eventForTest(-i)
		oldBodyBytes, _ := json.Marshal(oldBody)
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, string(oldBodyBytes), channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 4)

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
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", filterFunction, nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	for i := 10; i < 20; i++ {
		body, channels := eventForTest(i)
		oldBody, _ := eventForTest(-i)
		oldBodyBytes, _ := json.Marshal(oldBody)
		em.RaiseDocumentChangeEvent(body, string(oldBodyBytes), channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 10)

}

func TestWebhookTimeout(t *testing.T) {

	if !testLiveHTTP {
		return
	}

	var logKeys = map[string]bool{
		"Events+": true,
	}

	base.UpdateLogKeys(logKeys, true)

	count, sum, _ := InitWebhookTest()
	ids := make([]string, 200)
	for i := 0; i < 200; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	time.Sleep(1 * time.Second)
	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}

	// Test fast execution, short timeout.  All events processed
	log.Println("Test fast webhook, short timeout")
	em := NewEventManager()
	em.Start(0, -1)
	timeout := uint64(2)
	webhookHandler, _ := NewWebhook("http://localhost:8081/echo", "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 10)

	// Test slow webhook, short timeout, numProcess=1, waitForProcess > timeout.  All events should get processed.
	log.Println("Test slow webhook, short timeout")
	*count, *sum = 0, 0.0
	errCount := 0
	em = NewEventManager()
	em.Start(1, 1100)
	timeout = uint64(1)
	webhookHandler, _ = NewWebhook("http://localhost:8081/slow_2s", "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		err := em.RaiseDocumentChangeEvent(body, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	time.Sleep(15 * time.Second)
	// Even though we timed out waiting for response on the SG side, POST still completed on target side.
	assert.Equals(t, *count, 10)

	// Test slow webhook, short timeout, numProcess=1, waitForProcess << timeout.  Events that don't fit in queues
	// should get dropped (1 immediately processed, 1 in normal queue, 3 in overflow queue, 5 dropped)
	log.Println("Test very slow webhook, short timeout")
	*count, *sum = 0, 0.0
	errCount = 0
	em = NewEventManager()
	em.Start(1, 100)
	timeout = uint64(9)
	webhookHandler, _ = NewWebhook("http://localhost:8081/slow_5s", "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		err := em.RaiseDocumentChangeEvent(body, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	// wait for slow webhook to finish processing
	time.Sleep(25 * time.Second)
	assert.Equals(t, *count, 5)

	// Test slow webhook, no timeout, numProcess=1, waitForProcess=1s.  All events should complete.
	log.Println("Test slow webhook, no timeout, wait for process ")
	*count, *sum = 0, 0.0
	errCount = 0
	em = NewEventManager()
	em.Start(1, 1100)
	timeout = uint64(0)
	webhookHandler, _ = NewWebhook("http://localhost:8081/slow", "", &timeout)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		err := em.RaiseDocumentChangeEvent(body, "", channels)
		time.Sleep(2 * time.Millisecond)
		if err != nil {
			errCount++
		}
	}
	// wait for slow webhook to finish processing
	time.Sleep(15 * time.Second)
	assert.Equals(t, *count, 10)

}

func TestUnavailableWebhook(t *testing.T) {

	ids := make([]string, 20)
	for i := 0; i < 20; i++ {
		ids[i] = fmt.Sprintf("%d", i)
	}

	eventForTest := func(i int) (Body, base.Set) {
		testBody := Body{
			"_id":   ids[i],
			"value": i,
		}
		var channelSet base.Set
		if i%2 == 0 {
			channelSet = base.SetFromArray([]string{"Even"})
		} else {
			channelSet = base.SetFromArray([]string{"Odd"})
		}
		return testBody, channelSet
	}

	// Test unreachable webhook
	em := NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ := NewWebhook("http://badhost:1000/echo", "", nil)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, "", channels)
	}

	time.Sleep(50 * time.Millisecond)
}
