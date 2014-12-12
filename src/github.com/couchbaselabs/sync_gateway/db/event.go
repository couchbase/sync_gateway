package db

import (
	"errors"
	"fmt"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/walrus"
	"github.com/robertkrimen/otto"
	"strconv"
)

// Event type
type EventType uint8

const (
	DocumentChange EventType = iota
	UserAdd
)

// An event that can be raised during SG processing.
type Event interface {
	Synchronous() bool
	EventType() EventType
	String() string
}

type EventImpl struct {
	eventType EventType
}

func (e *EventImpl) EventType() EventType {
	return e.eventType
}

// Currently the AsyncEvent type only manages the Synchronous() check.  Future enhancements
// around async processing would leverage this type.
type AsyncEvent struct {
	EventImpl
}

func (ae *AsyncEvent) Synchronous() bool {
	return false
}

// DocumentChangeEvent is raised when a document has been successfully written to the backing
// data store.  Event has the document body and channel set as properties.
type DocumentChangeEvent struct {
	AsyncEvent
	Doc      Body
	Channels base.Set
}

func (dce *DocumentChangeEvent) String() string {
	return fmt.Sprintf("Document change event for doc id: %s", dce.Doc["_id"])
}

// Javascript function handling for events
const kTaskCacheSize = 4

type ResponseType uint8

const (
	StringResponse ResponseType = iota
	JSObjectResponse
)

// A compiled JavaScript event function.
type jsEventTask struct {
	walrus.JSRunner
	responseType ResponseType
}

// Compiles a JavaScript event function to a jsEventTask object.
func newJsEventTask(funcSource string) (walrus.JSServerTask, error) {
	eventTask := &jsEventTask{}
	err := eventTask.Init(funcSource)
	if err != nil {
		return nil, err
	}

	eventTask.After = func(result otto.Value, err error) (interface{}, error) {
		nativeValue, _ := result.Export()
		/*
			switch nativeValue := nativeValue.(type) {
			case string:
				stringResult = nativeValue
				eventTask.responseType = StringResponse
			case interface{}:
				resultBytes, marshErr := json.Marshal(nativeValue)
				if marshErr != nil {
					err = marshErr
				} else {
					stringResult = string(resultBytes)
					eventTask.responseType = JSObjectResponse
				}
			}
		*/
		return nativeValue, err
	}

	return eventTask, nil
}

//////// JSEventFunction

// A thread-safe wrapper around a jsEventTask, i.e. an event function.
type JSEventFunction struct {
	*walrus.JSServer
}

func NewJSEventFunction(fnSource string) *JSEventFunction {

	base.LogTo("Events", "Creating new JSEventFunction")
	return &JSEventFunction{
		JSServer: walrus.NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (walrus.JSServerTask, error) {
				return newJsEventTask(fnSource)
			}),
	}
}

// Calls a jsEventFunction returning an interface{}
func (ef *JSEventFunction) CallFunction(event Event) (interface{}, error) {

	var err error
	var result interface{}

	// Different events send different parameters
	switch event := event.(type) {

	case *DocumentChangeEvent:
		result, err = ef.Call(event.Doc)
	}

	if err != nil {
		base.Warn("Error calling function - function processing aborted: %v", err)
		return "", err
	}

	return result, err
}

// Calls a jsEventFunction returning bool.
func (ef *JSEventFunction) CallValidateFunction(event Event) (bool, error) {

	result, err := ef.CallFunction(event)
	if err != nil {
		return false, err
	}

	switch result := result.(type) {
	case bool:
		return result, nil
	case string:
		boolResult, err := strconv.ParseBool(result)
		if err != nil {
			return false, err
		}
		return boolResult, nil
	default:
		base.Warn("Event validate function returned non-boolean result %v", result)
		return false, errors.New("Validate function returned non-boolean value.")
	}

}
