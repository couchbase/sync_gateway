package db

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbase/sync_gateway/base"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"testing"
	"time"
)

// Webhook tests use an HTTP listener.  Use of this listener is disabled by default, to avoid
// port conflicts/permission issues when running tests on a non-dev machine.
const testLiveHTTP = false

// Testing handler tracks recieved events in ResultChannel
type TestingHandler struct {
	receivedType  EventType
	payload       Body
	ResultChannel chan Body // channel for tracking async results
	HandledEvent  EventType
	handleDelay   int // long running handler execution
}

func (th *TestingHandler) HandleEvent(event Event) {

	if th.handleDelay > 0 {
		time.Sleep(time.Duration(th.handleDelay) * time.Millisecond)
	}
	if dceEvent, ok := event.(*DocumentChangeEvent); ok {
		th.ResultChannel <- dceEvent.Doc
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
		em.RaiseDocumentChangeEvent(body, channels)
	}
	// wait for Event Manager queue worker to process
	time.Sleep(10 * time.Millisecond)

	assert.True(t, len(resultChannel) == 10)

}

// Test sending many events with slow-running execution to validate they get dropped after hitting
// the max concurrent goroutines
func TestSlowExecutionProcessing(t *testing.T) {

	em := NewEventManager()
	em.Start(0, -1)

	base.LogKeys["Events"] = true

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
		em.RaiseDocumentChangeEvent(body, channels)
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
		em.RaiseDocumentChangeEvent(body, channels)
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
		em.RaiseDocumentChangeEvent(body, channels)
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

func TestWebhook(t *testing.T) {

	if !testLiveHTTP {
		return
	}
	count, sum, payloads := InitWebhookTest()
	ids := make([]string, 200)
	for i := 0; i < 200; i++ {
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

	// Test basic webhook
	em := NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ := NewWebhook("http://localhost:8081/echo", "", 0)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 10)

	// Test webhook filter function
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
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", filterFunction, 0)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 4)

	// Validate payload
	fmt.Println("RESET HERE")
	*count, *sum, *payloads = 0, 0.0, nil
	em = NewEventManager()
	em.Start(0, -1)
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "", 0)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	body, channels := eventForTest(0)
	em.RaiseDocumentChangeEvent(body, channels)
	time.Sleep(50 * time.Millisecond)
	receivedPayload := string((*payloads)[0])
	fmt.Println("payload:", receivedPayload)
	assert.Equals(t, string((*payloads)[0]), `{"_id":"0","value":0}`)
	assert.Equals(t, *count, 1)

	// Test fast fill, fast webhook
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(5, -1)
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "", 0)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, channels := eventForTest(i % 10)
		em.RaiseDocumentChangeEvent(body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 100)

	// Test queue full, slow webhook.  Drops events
	*count, *sum = 0, 0.0
	errCount := 0
	em = NewEventManager()
	em.Start(5, -1)
	webhookHandler, _ = NewWebhook("http://localhost:8081/slow", "", 0)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, channels := eventForTest(i)
		err := em.RaiseDocumentChangeEvent(body, channels)
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
	*count, *sum = 0, 0.0
	em = NewEventManager()
	em.Start(5, 1100)
	webhookHandler, _ = NewWebhook("http://localhost:8081/slow", "", 0)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 100; i++ {
		body, channels := eventForTest(i % 10)
		em.RaiseDocumentChangeEvent(body, channels)
	}
	time.Sleep(5 * time.Second)
	assert.Equals(t, *count, 100)

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
	webhookHandler, _ := NewWebhook("http://badhost:1000/echo", "", 0)
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, channels)
	}

	time.Sleep(50 * time.Millisecond)
}
