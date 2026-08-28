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
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

func TestWebhookString(t *testing.T) {
	var wh *Webhook

	wh = &Webhook{
		url: "http://username:password@example.com/foo",
	}
	assert.Equal(t, "Webhook handler [http://xxxxx:xxxxx@example.com/foo]", wh.String())

	wh = &Webhook{
		url: "http://example.com:9000/baz",
	}
	assert.Equal(t, "Webhook handler [http://example.com:9000/baz]", wh.String())
}

func TestSanitizedUrl(t *testing.T) {
	var wh *Webhook
	ctx := base.TestCtx(t)
	wh = &Webhook{
		url: "https://foo%40bar.baz:my-%24ecret-p%40%25%24w0rd@example.com:8888/bar",
	}
	assert.Equal(t, "https://xxxxx:xxxxx@example.com:8888/bar", wh.SanitizedUrl(ctx))

	wh = &Webhook{
		url: "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux",
	}
	assert.Equal(t, "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux", wh.SanitizedUrl(ctx))
}

func TestCallValidateFunction(t *testing.T) {
	// Boolean return type handling of CallValidateFunction; Mock up a document change event and
	// filter function which returns a bool value while calling CallValidateFunction.
	channels := base.SetFromArray([]string{"Netflix"})
	docId, body, oldBodyJSON := "doc1", Body{BodyId: "doc1", "key1": "value1"}, ""
	bodyBytes := base.MustJSONMarshal(t, body)
	event := &DocumentChangeEvent{DocID: docId, DocBytes: bodyBytes, OldDoc: oldBodyJSON, Channels: channels}

	ctx := base.TestCtx(t)
	// Boolean return type handling of CallValidateFunction; bool true value.
	source := `function(doc) { if (doc.key1 == "value1") { return true; } else { return false; } }`
	filterFunc := NewJSEventFunction(ctx, source)
	result, err := filterFunc.CallValidateFunction(ctx, event)
	assert.True(t, result, "It should return true since doc.key1 is value1")
	assert.NoError(t, err, "It should return boolean result")

	// Boolean return type handling of CallValidateFunction; bool false value.
	source = `function(doc) { if (doc.key1 == "value2") { return true; } else { return false; } }`
	filterFunc = NewJSEventFunction(ctx, source)
	result, err = filterFunc.CallValidateFunction(ctx, event)
	assert.False(t, result, "It should return false since doc.key1 is not value2")
	assert.NoError(t, err, "It should return boolean result")

	// Parsable boolean string return type handling of CallValidateFunction.
	source = `function(doc) { if (doc.key1 == "value1") { return "true"; } else { return "false"; } }`
	filterFunc = NewJSEventFunction(ctx, source)
	result, err = filterFunc.CallValidateFunction(ctx, event)
	assert.True(t, result, "It should return true since doc.key1 is value1")
	assert.NoError(t, err, "It should return parsable boolean result")

	// Non parsable boolean string return type handling of CallValidateFunction.
	source = `function(doc) { if (doc.key1 == "value1") { return "TrUe"; } else { return "false"; } }`
	filterFunc = NewJSEventFunction(ctx, source)
	result, err = filterFunc.CallValidateFunction(ctx, event)
	assert.False(t, result, "It should return false since 'TrUe' is non parsable boolean string")
	assert.Error(t, err, "It should return parsable throw ParseBool error")
	assert.Contains(t, err.Error(), `invalid syntax`)

	// Not boolean and not parsable boolean string return type handling of CallValidateFunction.
	source = `function(doc) { if (doc.key1 == "Pi") { return 3.14; } else { return 0.0; } }`
	filterFunc = NewJSEventFunction(ctx, source)
	result, err = filterFunc.CallValidateFunction(ctx, event)
	assert.False(t, result, "It should return not boolean and not parsable boolean string value")
	assert.Error(t, err, "It should throw Validate function returned non-boolean value error")
	assert.Contains(t, err.Error(), "Validate function returned non-boolean value.")

	// Simulate CallFunction failure by making syntax error in filter function.
	source = `function(doc) { invalidKeyword if (doc.key1 == "value1") { return true; } else { return false; } }`
	filterFunc = NewJSEventFunction(ctx, source)
	result, err = filterFunc.CallValidateFunction(ctx, event)
	assert.False(t, result, "It should return false due to the syntax error in filter function")
	assert.Error(t, err, "It should throw an error due to syntax error")
	assert.Contains(t, err.Error(), "Unexpected token")
}

// TestWebhookClientTimeouts asserts the webhook client's transport timeouts are non-zero, so a webhook
// endpoint that stalls can't block an event handler.  The configured timeout governs the header wait, so a
// long timeout isn't capped; a zero timeout leaves Client.Timeout unbounded and falls back to the default.
func TestWebhookClientTimeouts(t *testing.T) {
	tests := []struct {
		name                          string
		timeout                       *uint64
		expectedClientTimeout         time.Duration
		expectedResponseHeaderTimeout time.Duration
	}{
		{
			name:                          "default timeout",
			timeout:                       nil,
			expectedClientTimeout:         kDefaultWebhookTimeout * time.Second,
			expectedResponseHeaderTimeout: kDefaultWebhookTimeout * time.Second,
		},
		{
			name:                          "timeout longer than the transport default",
			timeout:                       base.Ptr(uint64(300)),
			expectedClientTimeout:         300 * time.Second,
			expectedResponseHeaderTimeout: 300 * time.Second,
		},
		{
			name:                          "timeout shorter than the transport default",
			timeout:                       base.Ptr(uint64(5)),
			expectedClientTimeout:         5 * time.Second,
			expectedResponseHeaderTimeout: 5 * time.Second,
		},
		{
			// Client.Timeout is unbounded here, so the transport default has to bound the header wait
			name:                          "zero timeout",
			timeout:                       base.Ptr(uint64(0)),
			expectedClientTimeout:         0,
			expectedResponseHeaderTimeout: base.DefaultHttpResponseHeaderTimeout,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := base.TestCtx(t)
			wh, err := NewWebhook(ctx, "http://example.com/webhook", "", test.timeout, nil)
			require.NoError(t, err)

			transport, ok := wh.client.Transport.(*http.Transport)
			require.True(t, ok, "expected webhook client to carry an *http.Transport, got %T", wh.client.Transport)
			sgtest.RequireNonZeroHTTPTimeouts(t, transport)

			assert.Equal(t, test.expectedClientTimeout, wh.client.Timeout)
			assert.Equal(t, test.expectedResponseHeaderTimeout, transport.ResponseHeaderTimeout)
			assert.Equal(t, base.DefaultHttpExpectContinueTimeout, transport.ExpectContinueTimeout)
			assert.Equal(t, base.DefaultHttpIdleConnTimeout, transport.IdleConnTimeout)
		})
	}
}

// TestWebhookStalledEndpoint asserts a webhook endpoint that accepts the connection and then never
// responds fails the post rather than pinning the event handler.  Two bounds have to hold for that: the
// configured timeout when there is one, and the transport default when the configured timeout is zero and
// Client.Timeout therefore isn't bounding anything.
func TestWebhookStalledEndpoint(t *testing.T) {
	tests := []struct {
		name    string
		timeout *uint64
		// shortenTransport is set where the bound under test is the transport default, which is too long
		// to wait out - shortening it keeps what's being exercised the transport rather than Client.Timeout
		shortenTransport bool
	}{
		{
			name:    "configured timeout",
			timeout: base.Ptr(uint64(1)),
		},
		{
			name:             "zero timeout",
			timeout:          base.Ptr(uint64(0)),
			shortenTransport: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			listener := sgtest.NewStallingListener(t)

			ctx := base.TestCtx(t)
			wh, err := NewWebhook(ctx, "http://"+listener.Addr()+"/webhook", "", test.timeout, nil)
			require.NoError(t, err)
			if test.shortenTransport {
				require.Equal(t, time.Duration(0), wh.client.Timeout, "Client.Timeout must be unbounded for this case to test the transport")
				wh.client.Transport.(*http.Transport).ResponseHeaderTimeout = 500 * time.Millisecond
			}

			event := &DBStateChangeEvent{Doc: Body{"dbname": "db", "state": "online"}}

			// run in a goroutine so a regression reports as this test failing, rather than hanging the package
			handled := make(chan bool, 1)
			go func() { handled <- wh.HandleEvent(ctx, event) }()
			select {
			case success := <-handled:
				assert.False(t, success, "expected the post to a stalled endpoint to fail")
			case <-time.After(30 * time.Second):
				t.Errorf("HandleEvent did not give up on a stalled webhook endpoint")
			}

			listener.RequireAcceptedConnections(t, 1)
			listener.RequireClosedConnections(t, 1)
		})
	}
}
