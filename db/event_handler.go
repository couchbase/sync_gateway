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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/couchbase/sync_gateway/base"
)

// EventHandler interface represents an instance of an event handler defined in the database config
type EventHandler interface {
	HandleEvent(event Event) bool
	String() string
}

type AsyncEventHandler struct{}

// Webhook is an implementation of EventHandler that sends an asynchronous HTTP POST
type Webhook struct {
	AsyncEventHandler
	url     string
	filter  *JSEventFunction
	timeout time.Duration
	client  *http.Client
	options struct {
		DocumentChangedWinningRevOnly bool
	}
}

const (
	// default HTTP post timeout
	kDefaultWebhookTimeout = 60
	// EventOptionDocumentChangedWinningRevOnly controls whether a document_changed event is processed for winning revs only.
	EventOptionDocumentChangedWinningRevOnly = "winning_rev_only"
)

// Creates a new webhook handler based on the url and filter function.
func NewWebhook(url string, filterFnString string, timeout *uint64, options map[string]interface{}) (*Webhook, error) {

	var err error

	if url == "" {
		err = errors.New("url parameter must be defined for webhook events.")
		return nil, err
	}

	wh := &Webhook{
		url: url,
	}
	if filterFnString != "" {
		wh.filter = NewJSEventFunction(filterFnString)
	}

	if timeout != nil {
		wh.timeout = time.Duration(*timeout) * time.Second
	} else {
		wh.timeout = time.Duration(kDefaultWebhookTimeout) * time.Second
	}

	// Initialize transport and client
	transport := base.DefaultHTTPTransport()
	transport.DisableKeepAlives = false
	wh.client = &http.Client{Transport: transport, Timeout: wh.timeout}

	if options != nil {
		wh.options.DocumentChangedWinningRevOnly, _ = options[EventOptionDocumentChangedWinningRevOnly].(bool)
	}

	return wh, err
}

// Performs an HTTP POST to the url defined for the handler.  If a filter function is defined,
// calls it to determine whether to POST.  The payload for the POST is depends
// on the event type.
func (wh *Webhook) HandleEvent(event Event) bool {

	const contentType = "application/json"
	var payload []byte
	logCtx := context.TODO()

	// Different events post different content by default
	switch event := event.(type) {
	case *DocumentChangeEvent:
		// skip event if this is for a non-winning rev and the winning rev only option is enabled
		if !event.WinningRevChange && wh.options.DocumentChangedWinningRevOnly {
			return false
		}
		payload = event.DocBytes
	case *DBStateChangeEvent:
		// for DBStateChangeEvent, post JSON document with the following format
		//{
		//	“admininterface":"127.0.0.1:4985",
		//	“dbname":"db",
		//	“localtime":"2015-10-07T11:20:29.138+01:00",
		//	"reason":"DB started from config”,
		//	“state”:"online"
		//}
		jsonOut, err := base.JSONMarshal(event.Doc)
		if err != nil {
			base.WarnfCtx(logCtx, "Error marshalling doc for webhook post")
			return false
		}
		payload = jsonOut
	default:
		base.WarnfCtx(logCtx, "Webhook invoked for unsupported event type.")
		return false
	}

	if wh.filter != nil {
		// If filter function is defined, use it to determine whether to post
		success, err := wh.filter.CallValidateFunction(event)
		if err != nil {
			base.WarnfCtx(logCtx, "Error calling webhook filter function: %v", err)
		}

		// If filter returns false, cancel webhook post
		if !success {
			return false
		}
	}

	success := func() bool {
		resp, err := wh.client.Post(wh.url, contentType, bytes.NewBuffer(payload))
		defer func() {
			// Ensure we're closing the response, so it can be reused
			if resp != nil && resp.Body != nil {
				_, err := io.Copy(ioutil.Discard, resp.Body)
				if err != nil {
					base.DebugfCtx(logCtx, base.KeyEvents, "Error copying response body: %v", err)
				}
				err = resp.Body.Close()
				if err != nil {
					base.DebugfCtx(logCtx, base.KeyEvents, "Error closing response body: %v", err)
				}
			}
		}()

		if err != nil {
			base.WarnfCtx(logCtx, "Error attempting to post %s to url %s: %s", base.UD(event.String()), base.UD(wh.SanitizedUrl()), err)
			return false
		}

		// Check Log Level first, as SanitizedUrl is expensive to evaluate.
		if base.LogDebugEnabled(base.KeyEvents) {
			base.DebugfCtx(logCtx, base.KeyEvents, "Webhook handler ran for event.  Payload %s posted to URL %s, got status %s",
				base.UD(string(payload)), base.UD(wh.SanitizedUrl()), resp.Status)
		}
		return true
	}()
	return success
}

func (wh *Webhook) String() string {
	return fmt.Sprintf("Webhook handler [%s]", wh.SanitizedUrl())
}

func (wh *Webhook) SanitizedUrl() string {
	// Basic auth credentials may have been included in the URL, in which case obscure them
	return base.RedactBasicAuthURLUserAndPassword(wh.url)
}
