package db

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/sync_gateway/base"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
)

// EventHandler interface represents an instance of an event handler defined in the database config
type EventHandler interface {
	HandleEvent(event Event)
	String() string
}

type AsyncEventHandler struct{}

// Webhook is an implementation of EventHandler that sends an asynchronous HTTP POST
type Webhook struct {
	AsyncEventHandler
	url        string
	filter     *JSEventFunction
	n1qlFilter *N1QLEventFunction
	timeout    time.Duration
	client     *http.Client
}

// default HTTP post timeout
const kDefaultWebhookTimeout = 60

// Creates a new webhook handler based on the url and filter function.
func NewWebhook(url string, filterFnString string, timeout *uint64) (*Webhook, error) {

	var err error

	if url == "" {
		err = errors.New("url parameter must be defined for webhook events.")
		return nil, err
	}

	wh := &Webhook{
		url: url,
	}
	if strings.HasPrefix(filterFnString, "WHERE ") {
		filter, err := NewN1QLEventFunction(filterFnString)
		if err != nil {
			return nil, err
		}
		wh.n1qlFilter = filter
	} else if filterFnString != "" {
		wh.filter = NewJSEventFunction(filterFnString)
	}

	if timeout != nil {
		wh.timeout = time.Duration(*timeout) * time.Second
	} else {
		wh.timeout = time.Duration(kDefaultWebhookTimeout) * time.Second
	}

	// Initialize transport and client
	transport := &http.Transport{DisableKeepAlives: false}
	wh.client = &http.Client{Transport: transport, Timeout: wh.timeout}

	return wh, err
}

// Performs an HTTP POST to the url defined for the handler.  If a filter function is defined,
// calls it to determine whether to POST.  The payload for the POST is depends
// on the event type.
func (wh *Webhook) HandleEvent(event Event) {

	var payload *bytes.Buffer
	var contentType string
	if wh.n1qlFilter != nil {
		success, err := wh.n1qlFilter.CallFilterFunction(event)
		if err != nil {
			base.Warn("Error calling webhook N1QL filter: %v", err)
		}

		// If filter returns false, cancel webhook post
		if !success {
			return
		}
	} else if wh.filter != nil {
		// If filter function is defined, use it to determine whether to post
		success, err := wh.filter.CallValidateFunction(event)
		if err != nil {
			base.Warn("Error calling webhook filter function: %v", err)
		}

		// If filter returns false, cancel webhook post
		if !success {
			return
		}
	}

	// Different events post different content by default
	switch event := event.(type) {
	case *DocumentChangeEvent:
		// for DocumentChangeEvent, post document body
		jsonOut, err := json.Marshal(event.Doc)
		if err != nil {
			base.Warn("Error marshalling doc for webhook post")
			return
		}
		contentType = "application/json"
		payload = bytes.NewBuffer(jsonOut)
	default:
		base.Warn("Webhook invoked for unsupported event type.")
		return
	}
	func() {
		resp, err := wh.client.Post(wh.url, contentType, payload)
		defer func() {
			// Ensure we're closing the response, so it can be reused
			if resp != nil && resp.Body != nil {
				io.Copy(ioutil.Discard, resp.Body)
				resp.Body.Close()
			}
		}()

		if err != nil {
			base.Warn("Error attempting to post to url %s: %s -- %+v", wh.url, err)
			return
		}

		base.LogTo("Events+", "Webhook handler ran for event.  Payload %s posted to URL %s, got status %s",
			payload, wh.url, resp.Status)
	}()

}

func (wh *Webhook) String() string {
	return fmt.Sprintf("Webhook handler [%s]", wh.url)
}
