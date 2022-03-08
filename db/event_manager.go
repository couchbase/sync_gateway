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
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// EventManager routes raised events to corresponding event handlers.  Incoming events are just dumped in the
// eventChannel to minimize time spent blocking whatever process is raising the event.
// The event queue worker goroutine works the event channel and sends events to the appropriate handlers
type EventManager struct {
	activeEventTypes       map[EventType]bool
	eventHandlers          map[EventType][]EventHandler
	asyncEventChannel      chan Event
	activeCountChannel     chan bool
	waitTime               int
	eventsProcessedSuccess int64
	eventsProcessedFail    int64
}

func (em *EventManager) GetEventsProcessedSuccess() int64 {
	return atomic.LoadInt64(&em.eventsProcessedSuccess)
}

func (em *EventManager) IncrementEventsProcessedSuccess(delta int64) int64 {
	return atomic.AddInt64(&em.eventsProcessedSuccess, delta)
}

func (em *EventManager) GetEventsProcessedFail() int64 {
	return atomic.LoadInt64(&em.eventsProcessedFail)
}

func (em *EventManager) IncrementEventsProcessedFail(delta int64) int64 {
	return atomic.AddInt64(&em.eventsProcessedFail, delta)
}

const kMaxActiveEvents = 500 // number of events that are processed concurrently
const kEventWaitTime = 100   // time (ms) to wait before dropping event, when event queue is full

// Creates a new event manager.  Sets up the event channel for async events, and the goroutine to
// monitor and process that channel.
func NewEventManager() *EventManager {
	return &EventManager{
		eventHandlers:    make(map[EventType][]EventHandler, 0),
		activeEventTypes: make(map[EventType]bool),
	}
}

// Starts the listener queue for the event manager
func (em *EventManager) Start(maxProcesses uint, waitTime int) {

	if maxProcesses == 0 {
		maxProcesses = kMaxActiveEvents
	}
	if waitTime < 0 {
		em.waitTime = kEventWaitTime
	} else {
		em.waitTime = waitTime
	}

	base.InfofCtx(context.TODO(), base.KeyEvents, "Starting event manager with max processes:%d, wait time:%d ms", maxProcesses, em.waitTime)
	// activeCountChannel limits the number of concurrent events being processed
	em.activeCountChannel = make(chan bool, maxProcesses)

	// asyncEventChannel stores the incoming events.  It's set to 3x activeCountChannel, to
	// handle temporary spikes in event inflow
	em.asyncEventChannel = make(chan Event, 3*maxProcesses)

	// Start the event channel worker go routine, which will work the event queue and spawn goroutines to process the
	// event.  Blocks if the activeCountChannel is full, to prevent spawning more than cap(activeCountChannel)
	// goroutines.
	go func() {
		for event := range em.asyncEventChannel {
			em.activeCountChannel <- true
			go em.ProcessEvent(event)
		}
	}()

}

// Concurrent processing of all async event handlers registered for the event type
func (em *EventManager) ProcessEvent(event Event) {
	defer func() { <-em.activeCountChannel }()
	logCtx := context.TODO()
	// Send event to all registered handlers concurrently.  WaitGroup blocks
	// until all are finished
	var wg sync.WaitGroup
	for _, handler := range em.eventHandlers[event.EventType()] {
		base.DebugfCtx(logCtx, base.KeyEvents, "Event queue worker sending event %s to: %s", base.UD(event.String()), handler)
		wg.Add(1)
		go func(event Event, handler EventHandler) {
			defer wg.Done()
			//TODO: Currently we're not tracking success/fail from event handlers.  When this
			// is needed, could pass a channel to HandleEvent for tracking results
			if handler.HandleEvent(event) {
				em.IncrementEventsProcessedSuccess(1)
			} else {
				em.IncrementEventsProcessedFail(1)
			}
			base.TracefCtx(logCtx, base.KeyAll, "Webhook event processed %s", event)

		}(event, handler)
	}
	wg.Wait()
}

// Register a new event handler to the EventManager.  The event manager will route events of
// type eventType to the handler.
func (em *EventManager) RegisterEventHandler(handler EventHandler, eventType EventType) {
	em.eventHandlers[eventType] = append(em.eventHandlers[eventType], handler)
	em.activeEventTypes[eventType] = true
	base.InfofCtx(context.Background(), base.KeyEvents, "Registered event handler: %v, for event type %v", handler, eventType)
}

// Checks whether a handler of the given type has been registered to the event manager.
func (em *EventManager) HasHandlerForEvent(eventType EventType) bool {
	return em.activeEventTypes[eventType]
}

// Adds async events to the channel for processing
func (em *EventManager) raiseEvent(event Event) error {
	if !event.Synchronous() {
		// When asyncEventChannel is full, the raiseEvent method will block for (waitTime).
		// Default value of (waitTime) is 5 ms.
		timer := time.NewTimer(time.Duration(em.waitTime) * time.Millisecond)
		defer timer.Stop()
		select {
		case em.asyncEventChannel <- event:
			base.TracefCtx(context.TODO(), base.KeyAll, "Event sent to channel %s", event.String())
		case <-timer.C:
			// Event queue channel is full - ignore event and log error
			base.WarnfCtx(context.TODO(), "Event queue full - discarding event: %s", base.UD(event.String()))
			return errors.New("Event queue full")
		}
	}
	// TODO: handling for synchronous events
	return nil
}

// Raises a document change event based on the the document body and channel set.  If the
// event manager doesn't have a listener for this event, ignores.
func (em *EventManager) RaiseDocumentChangeEvent(docBytes []byte, docID string, oldBodyJSON string, channels base.Set, winningRevChange bool) error {

	if !em.activeEventTypes[DocumentChange] {
		return nil
	}
	event := &DocumentChangeEvent{
		DocID:            docID,
		DocBytes:         docBytes,
		OldDoc:           oldBodyJSON,
		Channels:         channels,
		WinningRevChange: winningRevChange,
	}

	return em.raiseEvent(event)

}

// Raises a DB state change event based on the db name, admininterface, new state, reason and local system time.
// If the event manager doesn't have a listener for this event, ignores.
func (em *EventManager) RaiseDBStateChangeEvent(dbName string, state string, reason string, adminInterface *string) error {

	if !em.activeEventTypes[DBStateChange] {
		return nil
	}

	adminInterfaceStr := ""
	if adminInterface != nil {
		adminInterfaceStr = *adminInterface
	}

	body := make(Body, 5)
	body["dbname"] = dbName
	body["admininterface"] = adminInterfaceStr
	body["state"] = state
	body["reason"] = reason
	body["localtime"] = time.Now().Format(base.ISO8601Format)

	event := &DBStateChangeEvent{
		Doc: body,
	}

	return em.raiseEvent(event)
}
