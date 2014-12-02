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

const testLiveHTTP = true

type OfflineHook struct {
	webhook *Webhook
}

type TestingHandler struct {
	receivedType  EventType
	payload       Body
	ResultChannel chan Body
	HandledEvent  EventType
}

func (th *TestingHandler) HandleEvent(event Event) error {

	if dceEvent, ok := event.(*DocumentCommitEvent); ok {
		th.ResultChannel <- dceEvent.Doc
	}
	return nil
}

func (th *TestingHandler) SetChannel(channel chan Body) {
	th.ResultChannel = channel
}

func TestDocumentCommitEvent(t *testing.T) {

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

	testHandler := &TestingHandler{HandledEvent: DocumentCommit}
	testHandler.SetChannel(resultChannel)
	fmt.Println("channel: %s", testHandler.ResultChannel)
	em.RegisterEventHandler(testHandler, DocumentCommit)

	fmt.Println("Registered Event handler, raising 10 events...")
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	// wait for Event Manager queue worker to process
	time.Sleep(10 * time.Millisecond)

	fmt.Println("Channel size after adds: %d", len(resultChannel))
	assert.True(t, len(resultChannel) == 10)

}

func TestCustomEvent(t *testing.T) {

	fmt.Println("Creating event manager")
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

	testHandler := &TestingHandler{HandledEvent: DocumentCommit}
	testHandler.SetChannel(resultChannel)
	fmt.Println("channel: %s", testHandler.ResultChannel)
	em.RegisterEventHandler(testHandler, DocumentCommit)

	fmt.Println("Registered Event handler")
	for i := 0; i < 10; i++ {
		fmt.Println("raising event %d", i)
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	// wait for Event Manager queue worker to process
	time.Sleep(50 * time.Millisecond)

	fmt.Println("Channel size after adds: %d", len(resultChannel))
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

	// send DocumentCommit events to handler
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	// Wait for Event Manager queue worker to process
	time.Sleep(50 * time.Millisecond)

	fmt.Println("Channel size after adds: %d", len(resultChannel))
	// Validate that no events were handled
	assert.True(t, len(resultChannel) == 0)

}

func InitWebhookTest() (*int, *float64) {

	// Uses counter and sum values for simplified tracking of POST requests recieved by HTTP
	// TODO:  enhance by adding listener for /count, /sum, /reset endpoints, and leave
	// all management within the function, instead of sharing pointer references around
	counter := 0
	sum := 0.0
	// Start HTTP listener for webhook calls
	go func() {
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm()

			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				log.Printf("Error trying to read body: %s", err)
			}

			if len(body) > 0 {
				log.Printf("Handled request with body: %s", body)
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
	return &counter, &sum
}

func TestWebhook(t *testing.T) {

	if !testLiveHTTP {
		return
	}

	count, sum := InitWebhookTest()

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

	// Test basic webhook (no channels, no transform)
	em := NewEventManager()
	webhookHandler, _ := NewWebhook("http://localhost:8081/echo", "", "")
	em.RegisterEventHandler(webhookHandler, DocumentCommit)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 10)

	// Test webhook filtering by channels
	*count, *sum = 0, 0.0
	em = NewEventManager()
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "Even", "")
	em.RegisterEventHandler(webhookHandler, DocumentCommit)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 5)

	// Test webhook filtering by channels (regex)
	*count, *sum = 0, 0.0
	em = NewEventManager()
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "E.", "")
	em.RegisterEventHandler(webhookHandler, DocumentCommit)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 5)
	assert.Equals(t, *sum, 20.0)

	// Test webhook filtering by transform
	*count, *sum = 0, 0.0
	em = NewEventManager()
	transformFunction := `function(doc) {
							if (doc.value < 6) {
								return '';
							} else {
								return doc;
							}
							}`
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "", transformFunction)
	em.RegisterEventHandler(webhookHandler, DocumentCommit)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 4)

	// Test webhook transform that returns string, verify HTTP post as form data
	*count, *sum = 0, 0.0
	em = NewEventManager()
	transformFunction = `function(doc) {
							output = "value=" + doc.value;
							return output;
							}`
	webhookHandler, _ = NewWebhook("http://localhost:8081/echo", "", transformFunction)
	em.RegisterEventHandler(webhookHandler, DocumentCommit)
	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	time.Sleep(50 * time.Millisecond)
	assert.Equals(t, *count, 10)
	assert.Equals(t, *sum, 45.0)

}

// Test custom event handler
func TestCustomHandler(t *testing.T) {
	em := NewEventManager()

	// write handler that does simple logging
	customFn := `function(doc) {
						log("Received doc with id" + doc._id)
						return;
						}`

	customHandler, _ := NewCustomEventHandler(customFn)
	em.RegisterEventHandler(customHandler, DocumentCommit)

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

	for i := 0; i < 10; i++ {
		body, channels := eventForTest(i)
		em.RaiseDocumentCommitEvent(body, body, channels)
	}
	time.Sleep(50 * time.Millisecond)

}

// Test transform returning string
func TestTransformFunctionReturningString(t *testing.T) {
	eventFunction := `function(doc) {
						return 'hello';}`
	transformer := NewJSEventFunction(eventFunction)
	dce := &DocumentCommitEvent{
		Doc: Body{"input": "a1"},
	}
	returnString, responseType, err := transformer.CallTransformFunction(dce)
	assert.Equals(t, err, nil)
	assert.Equals(t, responseType, StringResponse)
	assert.Equals(t, returnString, "hello")
}

// Test transform returning js object (as JSON string)
func TestTransformFunctionReturningObject(t *testing.T) {
	eventFunction := `function(doc) {
						var jsobj = new Object();
						jsobj.val = "hello";
						return jsobj;}`
	transformer := NewJSEventFunction(eventFunction)
	dce := &DocumentCommitEvent{
		Doc: Body{"input": "a1"},
	}
	returnString, responseType, err := transformer.CallTransformFunction(dce)

	assert.Equals(t, responseType, JSObjectResponse)
	assert.Equals(t, err, nil)
	assert.Equals(t, returnString, `{"val":"hello"}`)
}

// Test that input parameters are available for transformation by function
func TestTransformFunctionFromInput(t *testing.T) {
	eventFunction := `function(doc, oldDoc) {
						var jsobj = new Object();
						jsobj.new = doc.value;
						jsobj.old = oldDoc.value;
						return jsobj;}`
	transformer := NewJSEventFunction(eventFunction)
	dce := &DocumentCommitEvent{
		Doc:    Body{"new": "123", "value": "a1"},
		OldDoc: Body{"old": "123", "value": "a0"},
	}
	returnString, responseType, err := transformer.CallTransformFunction(dce)

	assert.Equals(t, responseType, JSObjectResponse)
	assert.Equals(t, err, nil)
	assert.Equals(t, returnString, `{"new":"a1","old":"a0"}`)
}

// Test that function runner supports functionswith subset of parameters
func TestTransformFunctionWithSingleParamFunction(t *testing.T) {
	eventFunction := `function(doc) {
						var jsobj = new Object();
						jsobj.id = doc.id;
						return jsobj;}`
	transformer := NewJSEventFunction(eventFunction)
	dce := &DocumentCommitEvent{
		Doc:    Body{"id": "123", "input": "a1"},
		OldDoc: Body{"id": "456", "input": "a0"},
	}
	returnString, responseType, err := transformer.CallTransformFunction(dce)

	assert.Equals(t, responseType, JSObjectResponse)
	assert.Equals(t, err, nil)
	assert.Equals(t, returnString, `{"id":"123"}`)
}

// Test that a malformed function returns error without panic
func TestTransformFunctionMalformedFunction(t *testing.T) {

	eventFunction := `function(doc) {
		var jsobj =`
	transformer := NewJSEventFunction(eventFunction)

	dce := &DocumentCommitEvent{
		Doc: Body{"id": "123", "input": "a1"},
	}
	returnString, _, err := transformer.CallTransformFunction(dce)

	assert.Equals(t, err != nil, true)
	assert.Equals(t, returnString, "")
}
