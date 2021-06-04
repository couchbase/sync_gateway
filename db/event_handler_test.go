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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
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

	wh = &Webhook{
		url: "https://foo%40bar.baz:my-%24ecret-p%40%25%24w0rd@example.com:8888/bar",
	}
	assert.Equal(t, "https://xxxxx:xxxxx@example.com:8888/bar", wh.SanitizedUrl())

	wh = &Webhook{
		url: "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux",
	}
	assert.Equal(t, "https://example.com/does-not-count-as-url-embedded:basic-auth-credentials@qux", wh.SanitizedUrl())
}

func TestCallValidateFunction(t *testing.T) {
	// Boolean return type handling of CallValidateFunction; Mock up a document change event and
	// filter function which returns a bool value while calling CallValidateFunction.
	channels := base.SetFromArray([]string{"Netflix"})
	docId, body, oldBodyJSON := "doc1", Body{BodyId: "doc1", "key1": "value1"}, ""
	bodyBytes, _ := base.JSONMarshal(body)
	event := &DocumentChangeEvent{DocID: docId, DocBytes: bodyBytes, OldDoc: oldBodyJSON, Channels: channels}

	// Boolean return type handling of CallValidateFunction; bool true value.
	source := `function(doc) { if (doc.key1 == "value1") { return true; } else { return false; } }`
	filterFunc := NewJSEventFunction(source)
	result, err := filterFunc.CallValidateFunction(event)
	assert.True(t, result, "It should return true since doc.key1 is value1")
	assert.NoError(t, err, "It should return boolean result")

	// Boolean return type handling of CallValidateFunction; bool false value.
	source = `function(doc) { if (doc.key1 == "value2") { return true; } else { return false; } }`
	filterFunc = NewJSEventFunction(source)
	result, err = filterFunc.CallValidateFunction(event)
	assert.False(t, result, "It should return false since doc.key1 is not value2")
	assert.NoError(t, err, "It should return boolean result")

	// Parsable boolean string return type handling of CallValidateFunction.
	source = `function(doc) { if (doc.key1 == "value1") { return "true"; } else { return "false"; } }`
	filterFunc = NewJSEventFunction(source)
	result, err = filterFunc.CallValidateFunction(event)
	assert.True(t, result, "It should return true since doc.key1 is value1")
	assert.NoError(t, err, "It should return parsable boolean result")

	// Non parsable boolean string return type handling of CallValidateFunction.
	source = `function(doc) { if (doc.key1 == "value1") { return "TrUe"; } else { return "false"; } }`
	filterFunc = NewJSEventFunction(source)
	result, err = filterFunc.CallValidateFunction(event)
	assert.False(t, result, "It should return false since 'TrUe' is non parsable boolean string")
	assert.Error(t, err, "It should return parsable throw ParseBool error")
	assert.Contains(t, err.Error(), `invalid syntax`)

	// Not boolean and not parsable boolean string return type handling of CallValidateFunction.
	source = `function(doc) { if (doc.key1 == "Pi") { return 3.14; } else { return 0.0; } }`
	filterFunc = NewJSEventFunction(source)
	result, err = filterFunc.CallValidateFunction(event)
	assert.False(t, result, "It should return not boolean and not parsable boolean string value")
	assert.Error(t, err, "It should throw Validate function returned non-boolean value error")
	assert.Contains(t, err.Error(), "Validate function returned non-boolean value.")

	// Simulate CallFunction failure by making syntax error in filter function.
	source = `function(doc) { invalidKeyword if (doc.key1 == "value1") { return true; } else { return false; } }`
	filterFunc = NewJSEventFunction(source)
	result, err = filterFunc.CallValidateFunction(event)
	assert.False(t, result, "It should return false due to the syntax error in filter function")
	assert.Error(t, err, "It should throw an error due to syntax error")
	assert.Contains(t, err.Error(), "Unexpected token")
}
