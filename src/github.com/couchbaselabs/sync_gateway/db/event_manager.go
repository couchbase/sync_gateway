package db

import (
	"github.com/couchbaselabs/sync_gateway/base"
)

// EventManager manages the configured set of event listeners.  Incoming events are just dumped in the
// eventChannel to minimize time spent blocking whatever process is raising the event.
// The event queue worker goroutine works the event channel and sends events to the appropriate listener channels
type EventManager struct {
	activeEventTypes  map[EventType]bool
	eventHandlers     map[EventType][]EventHandler
	asyncEventChannel chan Event
}

func NewEventManager() *EventManager {

	em := &EventManager{
		eventHandlers: make(map[EventType][]EventHandler, 0),
	}

	// create channel for incoming asynchronous events
	em.asyncEventChannel = make(chan Event)

	em.activeEventTypes = make(map[EventType]bool)

	// Start event queue worker go routine
	go func() {
		for event := range em.asyncEventChannel {
			// send event to all registered handlers
			for _, handler := range em.eventHandlers[event.EventType()] {
				base.LogTo("Events", "Event queue worker sending event to: %s", handler)
				handler.HandleEvent(event)
			}
		}
	}()

	return em
}

func (em *EventManager) RegisterEventHandler(handler EventHandler, eventType EventType) {
	em.eventHandlers[eventType] = append(em.eventHandlers[eventType], handler)
	em.activeEventTypes[eventType] = true
	base.LogTo("Events", "Registered event handler: %v", handler)
}

func (em *EventManager) HasHandlerForEvent(eventType EventType) bool {

	// TODO EVENT: activeEvents might be getting stored somewhere less redundant
	return em.activeEventTypes[eventType]

}

func (em *EventManager) raiseEvent(event Event) bool {
	if !event.Synchronous() {
		em.asyncEventChannel <- event
	}
	// TODO: handling for synchronous events
	return true
}

func (em *EventManager) RaiseDocumentCommitEvent(body Body, oldBody string, channels base.Set) {

	if !em.activeEventTypes[DocumentCommit] {
		return
	}

	event := &DocumentCommitEvent{
		Doc:      body,
		OldDoc:   oldBody,
		Channels: channels}
	em.raiseEvent(event)

}
