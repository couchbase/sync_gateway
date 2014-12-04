package db

import (
	"github.com/couchbaselabs/sync_gateway/base"
)

// EventManager routes raised events to corresponding event handlers.  Incoming events are just dumped in the
// eventChannel to minimize time spent blocking whatever process is raising the event.
// The event queue worker goroutine works the event channel and sends events to the appropriate handlers
type EventManager struct {
	activeEventTypes  map[EventType]bool
	eventHandlers     map[EventType][]EventHandler
	asyncEventChannel chan Event
}

// Creates a new event manager.  Sets up the event channel for async events, and the goroutine to
// monitor and process that channel.
func NewEventManager() *EventManager {

	em := &EventManager{
		eventHandlers: make(map[EventType][]EventHandler, 0),
	}

	// Create channel for queued asynchronous events.
	em.asyncEventChannel = make(chan Event, 500)

	em.activeEventTypes = make(map[EventType]bool)

	// Start event channel worker go routine
	go func() {
		for event := range em.asyncEventChannel {
			// send event to all registered handlers
			for _, handler := range em.eventHandlers[event.EventType()] {
				base.LogTo("Events+", "Event queue worker sending event to: %s", handler)
				//TODO: currently we're not tracking success/fail from event handlers.  When this
				// is needed, could pass a channel to HandleEvent for tracking results
				go handler.HandleEvent(event)
			}
		}
	}()

	return em
}

// Register a new event handler to the EventManger.  The event manager will route events of
// type eventType to the handler.
func (em *EventManager) RegisterEventHandler(handler EventHandler, eventType EventType) {
	em.eventHandlers[eventType] = append(em.eventHandlers[eventType], handler)
	em.activeEventTypes[eventType] = true
	base.LogTo("Events", "Registered event handler: %v", handler)
}

// Checks whether a handler of the given type has been registered to the event manager.
func (em *EventManager) HasHandlerForEvent(eventType EventType) bool {
	return em.activeEventTypes[eventType]
}

// Adds async events to the channel for processing
func (em *EventManager) raiseEvent(event Event) {
	if !event.Synchronous() {
		em.asyncEventChannel <- event
	}
	// TODO: handling for synchronous events
}

// Raises a document change event based on the the document body and channel set.  If the
// event manager doesn't have a listener for this event, ignores.
func (em *EventManager) RaiseDocumentChangeEvent(body Body, channels base.Set) {

	if !em.activeEventTypes[DocumentChange] {
		return
	}
	event := &DocumentChangeEvent{
		Doc:      body,
		Channels: channels}

	em.raiseEvent(event)

}
