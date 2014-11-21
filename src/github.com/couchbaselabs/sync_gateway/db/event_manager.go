package db

import (
	"bytes"
	"encoding/json"
	"github.com/couchbaselabs/sync_gateway/base"
	"net/http"
	"regexp"
)

// Event type
type EventType uint8

const (
	DocumentCommit EventType = iota
	UserAdd
)

// EventManager
// EventManager manages the configured set of event listeners.  Incoming events are just dumped in the
// eventChannel to minimize time spent blocking whatever process is raising the event.
//
// EventCoordinator goroutine works the event channel and sends events to the appropriate listener channels

type EventManager struct {
	activeEventTypes  map[EventType]bool
	eventHandlers     []EventHandler
	asyncEventChannel chan Event
}

func NewEventManager() *EventManager {

	em := &EventManager{
		eventHandlers: make([]EventHandler, 0),
	}

	// create channel for incoming asynchronous events
	em.eventChannel = make(chan Event)

	// TODO: retrieve handlers from config
	/*
		myWebHook, err := NewWebhook("http://localhost:8081/echo", "")
		if err == nil {
			em.eventHandlers = append(em.eventHandlers, myWebHook)
		}
		myOtherWebHook, err := NewWebhook("http://localhost:8081/test", "")
		if err == nil {
			em.eventHandlers = append(em.eventHandlers, myOtherWebHook)
		}
	*/

	em.activeEventTypes = make(map[EventType]bool)
	em.activeEventTypes[DocumentCommit] = true

	// Start incoming event queue worker go routine
	go func() {
		for event := range em.asyncEventChannel {

			// send event to all registered handlers
			for _, handler := range em.eventHandlers {

				base.LogTo("Events", "Event queue worker sending event to: %s", handler)

				if handler.HandlesEvent(event) {
					handler.HandleEvent(event)
				}
			}
		}
	}()

	return em
}

func (em *EventManager) RegisterEventHandler(handler EventHandler) {
	em.eventHandlers = append(em.eventHandlers, handler)

	base.LogTo("Events", "Registered event handler")
}

func (em *EventManager) HasHandlerForEvent(eventType EventType) bool {

	// TODO EVENT: activeEvents might be getting stored somewhere less redundant
	return em.activeEventTypes[eventType]

}

func (em *EventManager) raiseAsyncEvent(event Event) {
	em.asyncEventChannel <- event
	base.LogTo("Events", "%s added to event channel", event.EventType())
}

func (em *EventManager) RaiseDocumentCommitEvent(body Body, channels base.Set) {

	if !em.activeEventTypes[DocumentCommit] {
		return
	}
	event := &DocumentCommitEvent{
		body:     body,
		channels: channels}
	em.raiseAsyncEvent(event)

}

// Event
// TODO: move to event.go?
type Event interface {
	// TODO EVENT: get rid of EventType and just use type check
	EventType() EventType
}

type DocumentCommitEvent struct {
	eventType EventType
	body      Body
	channels  base.Set
}

func (d DocumentCommitEvent) EventType() EventType {
	return DocumentCommit
}

// EventHandler interface represents an instance of an event handler defined in the database config
type EventHandler interface {
	HandleEvent(event Event) error
}

// Webhook is an implementation of EventHandler that sends an asynchronous HTTP POST
type Webhook struct {
	url          string
	channelRegex *regexp.Regexp
}

func NewWebhook(url string, channelRegexString string) (Webhook, error) {

	base.LogTo("Events", "Creating webhook for %s", url)

	var err error
	wh := Webhook{
		url: url,
	}

	if channelRegexString != "" {
		wh.channelRegex, err = regexp.Compile(channelRegexString)
		if err != nil {
			base.LogTo("Events", "Invalid channel expression: %s - webhook not loaded", channelRegexString)
			return wh, err
		}
	}
	return wh, err
}

func (wh Webhook) HandleEvent(event Event) error {

	switch event := event.(type) {
	case *DocumentCommitEvent:
		// start a goroutine to perform the HTTP POST

		// TODO: consider just returning this function, and let the EventManager coordinate
		// go routine execution, to support throttling if needed
		go func() {

			// transform body if map function provided

			// post to URL
			jsonOut, err := json.Marshal(event.body)
			if err != nil {
				base.Warn("Couldn't serialize event JSON for %v", event.body)
				return err
			}
			resp, err := http.Post(wh.url, "application/json", bytes.NewBuffer(jsonOut))
			if err != nil {
				base.LogTo("Events", "Error posting to webhook %s", err)
				return err
			}

			base.LogTo("Events", "Webhook handler ran for event.  Doc with id %s posted to URL %s, got status %s",
				event.body["_id"], wh.url, resp.Status)
		}()
	}

	return nil

}

// HandlesEvent checks whether the incoming event type is handled by the webhook listener, and
// if so, that the set of channels on the document associated with the event match the channel regex
// in the webhook definition.
func (wh Webhook) HandlesEvent(event Event) bool {

	dceEvent := event.(*DocumentCommitEvent)

	base.LogTo("Events", "Type assertion to DocumentCommitEvent for %s, %s", event, dceEvent)

	if event, ok := event.(*DocumentCommitEvent); !ok {

		base.LogTo("Events", "Webhook only supports DocumentCommitEvent")
		return false
	} else {
		matchesChannel := false
		// If no regex is specified, no channel matching required
		if wh.channelRegex == nil {
			matchesChannel = true
		} else {
			for channel, _ := range event.channels {
				if wh.channelRegex.MatchString(channel) {
					matchesChannel = true
					break
				}
			}
		}
		return matchesChannel
	}

}
