package db

import (
	"encoding/json"
	"fmt"
	"github.com/couchbaselabs/go.assert"
	"github.com/couchbaselabs/sync_gateway/base"
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

func TestDocumentChangeEvent(t *testing.T) {

	em := NewEventManager()

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

// Test sending many events with slow-running execution to validate they get processed in parallel
func TestSlowExecutionProcessing(t *testing.T) {

	em := NewEventManager()

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

	resultChannel := make(chan Body, 101000)
	testHandler := &TestingHandler{HandledEvent: DocumentChange, handleDelay: 500}
	testHandler.SetChannel(resultChannel)
	em.RegisterEventHandler(testHandler, DocumentChange)

	for i := 0; i < 100000; i++ {
		body, channels := eventForTest(i % 10)
		em.RaiseDocumentChangeEvent(body, channels)
	}
	// wait for Event Manager queue worker to process
	time.Sleep(750 * time.Millisecond)

	assert.True(t, len(resultChannel) == 100000)

}

func TestCustomHandler(t *testing.T) {

	em := NewEventManager()

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
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm()

			fmt.Println("Handling event")
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

	// Test basic webhook
	em := NewEventManager()
	webhookHandler, _ := NewWebhook("http://localhost:8081/echo", "")
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
	filterFunction := `function(doc) {
							if (doc.value < 6) {
								return false;
							} else {
								return true;
							}
							}`
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", filterFunction)
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
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "")
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	body, channels := eventForTest(0)
	em.RaiseDocumentChangeEvent(body, channels)
	time.Sleep(50 * time.Millisecond)
	receivedPayload := string((*payloads)[0])
	fmt.Println("payload:", receivedPayload)
	assert.Equals(t, string((*payloads)[0]), `{"_id":"0","value":0}`)
	assert.Equals(t, *count, 1)

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
	webhookHandler, _ := NewWebhook("http://badhost:1000/echo", "")
	em.RegisterEventHandler(webhookHandler, DocumentChange)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentChangeEvent(body, channels)
	}

	time.Sleep(50 * time.Millisecond)
}
