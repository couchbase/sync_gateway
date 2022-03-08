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
	"errors"
	"fmt"
	"strconv"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/robertkrimen/otto"
)

// EventType is an enum for each unique event type.
type EventType uint8

const (
	DocumentChange EventType = iota // fires whenever a document is updated (even if the change did not cause the winning rev to change)
	DBStateChange                   // fires when the database is created or is taken offline/online

	eventTypeCount
)

var eventTypeNames = []string{"DocumentChange", "DBStateChange"}

// String returns the string representation of an event type (e.g. "DBStateChange")
func (et EventType) String() string {
	if et >= eventTypeCount {
		return fmt.Sprintf("EventType(%d)", et)

	}
	return eventTypeNames[et]
}

// An event that can be raised during SG processing.
type Event interface {
	Synchronous() bool
	EventType() EventType
	String() string
}

// Currently the AsyncEvent type only manages the Synchronous() check.  Future enhancements
// around async processing would leverage this type.
type AsyncEvent struct {
	//EventImpl
}

func (ae AsyncEvent) Synchronous() bool {
	return false
}

// DocumentChangeEvent is raised when a document has been successfully written to the backing
// data store.  Event has the document body and channel set as properties.
type DocumentChangeEvent struct {
	AsyncEvent
	DocBytes         []byte
	DocID            string
	OldDoc           string
	Channels         base.Set
	WinningRevChange bool // whether this event is a change to the winning revision
}

func (dce *DocumentChangeEvent) String() string {
	return fmt.Sprintf("Document change event for doc id: %s", dce.DocID)
}

func (dce *DocumentChangeEvent) EventType() EventType {
	return DocumentChange
}

// DBStateChangeEvent is raised when a DB goes online or offline.
// Event has name of DB that is firing event, the admin interface address for interacting with the db.
// The new state, the reason for the state change, the local system time of the change
type DBStateChangeEvent struct {
	AsyncEvent
	Doc Body
}

func (dsce *DBStateChangeEvent) String() string {
	return fmt.Sprintf("DB state change event for db name: %s", dsce.Doc["dbname"])
}

func (dsce *DBStateChangeEvent) EventType() EventType {
	return DBStateChange
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
	sgbucket.JSRunner
	responseType ResponseType
}

// Compiles a JavaScript event function to a jsEventTask object.
func newJsEventTask(funcSource string) (sgbucket.JSServerTask, error) {
	eventTask := &jsEventTask{}
	err := eventTask.InitWithLogging(funcSource,
		func(s string) {
			base.ErrorfCtx(context.Background(), base.KeyJavascript.String()+": Webhook %s", base.UD(s))
		},
		func(s string) { base.InfofCtx(context.Background(), base.KeyJavascript, "Webhook %s", base.UD(s)) })
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
				resultBytes, marshErr := base.JSONMarshal(nativeValue)
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
	*sgbucket.JSServer
}

func NewJSEventFunction(fnSource string) *JSEventFunction {

	base.InfofCtx(context.Background(), base.KeyEvents, "Creating new JSEventFunction")
	return &JSEventFunction{
		JSServer: sgbucket.NewJSServer(fnSource, kTaskCacheSize,
			func(fnSource string) (sgbucket.JSServerTask, error) {
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
		result, err = ef.Call(sgbucket.JSONString(event.DocBytes), sgbucket.JSONString(event.OldDoc))
	case *DBStateChangeEvent:
		result, err = ef.Call(event.Doc)
	default:
		base.WarnfCtx(context.TODO(), "unknown event %v tried to call function", event.EventType())
		return "", fmt.Errorf("unknown event %v tried to call function", event.EventType())
	}

	if err != nil {
		base.WarnfCtx(context.TODO(), "Error calling function - function processing aborted: %v", err)
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
		base.WarnfCtx(context.TODO(), "Event validate function returned non-boolean result %T %v", result, result)
		return false, errors.New("Validate function returned non-boolean value.")
	}

}
