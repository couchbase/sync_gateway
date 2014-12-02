package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbaselabs/sync_gateway/base"
	"net/http"
	"regexp"
)

// EventHandler interface represents an instance of an event handler defined in the database config
type EventHandler interface {
	HandleEvent(event Event) error
}

type AsyncEventHandler struct{}

func (a *AsyncEventHandler) MapPayload(body Body, mapFunction string) (Body, error) {
	var err error
	return body, err
}

// Webhook is an implementation of EventHandler that sends an asynchronous HTTP POST
type Webhook struct {
	AsyncEventHandler
	url          string
	channelRegex *regexp.Regexp
	transformFn  *JSEventFunction
}

func NewWebhook(url string, channelRegexString string, transformFnString string) (*Webhook, error) {

	base.LogTo("Events", "Creating webhook for %s", url)

	var err error
	wh := &Webhook{
		url: url,
	}

	if channelRegexString != "" {
		wh.channelRegex, err = regexp.Compile(channelRegexString)
		if err != nil {
			base.LogTo("Events", "Invalid channel expression: %s - webhook not loaded", channelRegexString)
			return wh, err
		}
	}

	if transformFnString != "" {
		wh.transformFn = NewJSEventFunction(transformFnString)
	}
	return wh, err
}

func (wh *Webhook) HandleEvent(event Event) error {

	var err error

	// Check whether the webhook should handle this event
	if !wh.handlesEvent(event) {
		return nil
	}

	// start a goroutine to perform the HTTP POST
	// TODO: consider just returning this function, and let the EventManager coordinate
	// go routine execution, to support throttling if needed

	go func() {

		var payload *bytes.Buffer
		var contentType string

		if wh.transformFn != nil {
			// If transform function is defined, use it to build the response
			result, responseType, err := wh.transformFn.CallTransformFunction(event)
			if err != nil {
				base.Warn("Error running wehbook transform function: %v", err)
			}
			switch responseType {
			case StringResponse:
				// If function returns string, send as-is.  If empty string, cancel http POST.
				if result == "" {
					return
				}
				//TODO: do we need to handle urlencoding of the payload here, in case
				// function hasn't?
				contentType = "application/x-www-form-urlencoded"
				payload = bytes.NewBufferString(result)
			case JSObjectResponse:
				contentType = "application/json"
				payload = bytes.NewBufferString(result)
			}
		} else {
			// If no transform function defined, the response depends on event type
			switch event := event.(type) {
			case *DocumentCommitEvent:
				// for DocumentCommitEvent, return new document body
				jsonOut, err := json.Marshal(event.Doc)
				if err != nil {
					base.Warn("Error marshalling JSON for event, cancelled webhook POST: %v", err)
					return
				}
				contentType = "application/json"
				payload = bytes.NewBuffer(jsonOut)
			default:
				err = errors.New("Webhook invoked for unsupported event type.")
				return
			}
		}

		resp, err := http.Post(wh.url, contentType, payload)
		if err != nil {
			base.LogTo("Events", "Error posting to webhook %s", err)
			return
		}

		base.LogTo("Events+", "Webhook handler ran for event.  Payload %s posted to URL %s, got status %s",
			payload, wh.url, resp.Status)
	}()

	return err

}

// HandlesEvent checks whether the incoming event type is handled by the webhook listener, and
// if so, that the set of channels on the document associated with the event match the channel regex
// in the webhook definition.
func (wh *Webhook) handlesEvent(event Event) bool {

	if event, ok := event.(*DocumentCommitEvent); !ok {
		base.LogTo("Events", "Webhook only supports DocumentCommitEvent")
		return false
	} else {
		matchesChannel := false
		// If no regex is specified, no channel matching required
		if wh.channelRegex == nil {
			matchesChannel = true
		} else {
			for channel, _ := range event.Channels {
				if wh.channelRegex.MatchString(channel) {
					matchesChannel = true
					break
				}
			}
		}
		return matchesChannel
	}

}

func (wh *Webhook) String() string {
	return fmt.Sprintf("Webhook handler [%s]", wh.url)
}

// Webhook is an implementation of EventHandler that sends an asynchronous HTTP POST
type CustomEventHandler struct {
	AsyncEventHandler
	handleEventFn *JSEventFunction
}

func NewCustomEventHandler(handleEventFn string) (*CustomEventHandler, error) {

	base.LogTo("Events", "Creating custom handler")

	handleFunction := NewJSEventFunction(handleEventFn)

	handler := &CustomEventHandler{
		handleEventFn: handleFunction,
	}

	return handler, nil
}

func (ch *CustomEventHandler) HandleEvent(event Event) error {

	var err error
	switch event := event.(type) {
	case *DocumentCommitEvent:
		// start a goroutine to execute the custom function
		// TODO: consider just returning this function, and let the EventManager coordinate
		// go routine execution, to support throttling if needed
		go func() {

			ch.handleEventFn.CallFunction(event)

		}()
	}

	return err

}

func (ch *CustomEventHandler) String() string {
	//TODO: add a 'name' property to handlers for better diagnostics?
	return fmt.Sprintf("Custom event handler")
}
