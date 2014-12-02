package db

import (
	"encoding/json"
	"errors"
	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/walrus"
	"github.com/robertkrimen/otto"
	"strconv"
)

// Event type
type EventType uint8

const (
	DocumentCommit EventType = iota
	UserAdd
)

// Event
type Event interface {
	Synchronous() bool
	EventType() EventType
}

type EventImpl struct {
	eventType EventType
}

func (e *EventImpl) EventType() EventType {
	return e.eventType
}

type AsyncEvent struct {
	EventImpl
}

func (ae *AsyncEvent) Synchronous() bool {
	return false
}

type DocumentCommitEvent struct {
	AsyncEvent
	Doc      Body     `json:"doc,omitempty"`
	OldDoc   string   `json:"oldDoc,omitempty"`
	Channels base.Set `json:"channels,omitempty"`
}

/*
func (d DocumentCommitEvent) EventType() EventType {
	return DocumentCommit
}
*/
// Transform function handling
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

	return &JSEventFunction{
		JSServer: walrus.NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (walrus.JSServerTask, error) {
				return newJsEventTask(fnSource)
			}),
	}
}

// Calls a jsMapTask.
func (ef *JSEventFunction) CallFunction(event Event) (interface{}, error) {

	var err error
	var result interface{}

	// Different events send different parameters
	switch event := event.(type) {

	case *DocumentCommitEvent:
		result, err = ef.Call(event.Doc, event.OldDoc)
	}

	if err != nil {
		base.Warn("Error calling function - function processing aborted: %v", err)
		return "", err
	}

	return result, err
}

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

func (ef *JSEventFunction) CallTransformFunction(event Event) (string, ResponseType, error) {

	result, err := ef.CallFunction(event)

	var stringResult string
	var responseType ResponseType
	switch result := result.(type) {
	case string:
		stringResult = result
		responseType = StringResponse
	case interface{}:
		resultBytes, marshErr := json.Marshal(result)
		if marshErr != nil {
			err = marshErr
		} else {
			stringResult = string(resultBytes)
			responseType = JSObjectResponse
		}
	}

	return stringResult, responseType, err
}
