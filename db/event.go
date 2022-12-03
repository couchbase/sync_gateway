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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/js"
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
	// EventImpl
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

// type ResponseType uint8

// const (
// 	StringResponse ResponseType = iota
// 	JSObjectResponse
// )

//////// JSEventFunction

// A compiled event function.
type JSEventFunction struct {
	*js.Service
}

func NewJSEventFunction(host js.ServiceHost, fnSource string) *JSEventFunction {
	base.InfofCtx(context.Background(), base.KeyEvents, "Creating new JSEventFunction")
	return &JSEventFunction{
		Service: js.NewService(host, "event", fnSource),
	}
}

// Calls a jsEventFunction returning an interface{}
func (ef *JSEventFunction) CallFunction(event Event) (interface{}, error) {
	ctx := context.TODO()
	var err error
	var result interface{}

	// Different events send different parameters
	switch event := event.(type) {

	case *DocumentChangeEvent:
		result, err = ef.Run(ctx, js.JSONString(event.DocBytes), js.JSONString(event.OldDoc))
	case *DBStateChangeEvent:
		result, err = ef.Run(ctx, event.Doc)
	default:
		base.WarnfCtx(context.TODO(), "unknown event %v tried to call function", event.EventType())
		return "", fmt.Errorf("unknown event %v tried to call function", event.EventType())
	}

	if err != nil {
		base.WarnfCtx(context.TODO(), "Error calling function - function processing aborted: %+v", err)
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
		return strconv.ParseBool(result)
	default:
		base.WarnfCtx(context.TODO(), "Event validate function returned non-boolean result %T %v", result, result)
		return false, errors.New("validate function returned non-boolean value.")
	}

}
