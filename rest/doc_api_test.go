/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests ConvertJSONNumber handling for the sync function, and preservation of large numbers in the
// stored document.
func TestDocumentNumbers(t *testing.T) {

	tests := []struct {
		name                  string
		body                  string
		expectedString        string
		expectedFormatChannel string
	}{
		{"maxInt64",
			`{"number": 9223372036854775807}`,
			"9223372036854775807",
			"number"},
		{"minInt64",
			`{"number": -9223372036854775808}`,
			"-9223372036854775808",
			"number"},
		{"greaterThanMaxInt64",
			`{"number": 9223372036854775808}`,
			"9223372036854775808",
			"number"},
		{"lessThanMinInt64",
			`{"number": -9223372036854775809}`,
			"9223372036854775809",
			"number"},
		{"javascriptMaxSafeInt",
			`{"number": 9007199254740991}`,
			"9007199254740991",
			"number"},
		{"javascriptMinSafeInt",
			`{"number": -9007199254740991}`,
			"-9007199254740991",
			"number"},
		{"javascriptGreaterThanMaxSafeInt",
			`{"number": 9007199254740992}`,
			"9007199254740992",
			"number"},
		{"javascriptLessThanMinSafeInt",
			`{"number": -9007199254740992}`,
			"-9007199254740992",
			"number"},
		{"simpleFloat",
			`{"number": -234.56}`,
			"-234.56",
			"number"},
		{"highPrecisionFloat",
			`{"number": 9223.372036854775807}`,
			"9223.372036854775807",
			"number"},
		{"scientificFloat",
			`{"number": 6.0221409e+23}`,
			"6.0221409e+23",
			"number"},
		{"integerArray",
			`{"array":[1234]}`,
			`"array":[1234]`,
			"number",
		},
		{"floatArray",
			`{"array":[12.34]}`,
			`"array":[12.34]`,
			"number",
		},
		{"bigInt",
			`{"number": 123456789012345678901234567890}`,
			"123456789012345678901234567890",
			"number"},
		{"bigIntArray",
			`{"array": [123456789012345678901234567890]}`,
			`"array":[123456789012345678901234567890]`,
			"number"},
	}

	// Use channels to ensure numbers are making it to sync function with expected formats
	syncFn := `
	function(doc) {
		if (doc.number) {
			channel(typeof(doc.number))
		}
		if (doc.array) {
			channel(typeof doc.array[0])
		}
	}`
	rtConfig := RestTesterConfig{SyncFn: syncFn}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	for _, test := range tests {
		t.Run(test.name, func(ts *testing.T) {
			// Create document
			response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+test.name, test.body)
			RequireStatus(ts, response, 201)

			// Get document, validate number value
			getResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+test.name, "")
			RequireStatus(ts, getResponse, 200)

			// Check the raw bytes, because unmarshalling the response would be another opportunity for the number to get modified
			responseString := getResponse.Body.String()
			if !strings.Contains(responseString, test.expectedString) {
				ts.Errorf("Response does not contain the expected number format (%s).  Response: %s", test.name, responseString)
			}

			// Check channel assignment
			// NOTE: Now that we use V8 instead of Otto, this is kind of pointless: V8 supports
			// JavaScript "bigints" -- arbitrary-size integers -- so it's no longer necessary to
			// marshal large integers to strings before passing them to the sync function.
			getRawResponse := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", test.name), "")
			var rawResponse RawResponse
			require.NoError(ts, base.JSONUnmarshal(getRawResponse.Body.Bytes(), &rawResponse))
			log.Printf("raw response: %s", getRawResponse.Body.Bytes())
			assert.Equal(ts, 1, len(rawResponse.Sync.Channels))
			assert.True(ts, HasActiveChannel(rawResponse.Sync.Channels, test.expectedFormatChannel), fmt.Sprintf("Expected channel %s was not found in document channels %s (%s)", test.expectedFormatChannel, rawResponse.Sync.Channels, test.name))

		})
	}

}

func TestGuestReadOnly(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		GuestEnabled: true,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Unsupported: &db.UnsupportedOptions{
				GuestReadOnly: true,
			},
		},
		}},
	)

	defer rt.Close()

	rt.GetDatabase()
	// Write a document as admin
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc", "{}")
	RequireStatus(t, response, http.StatusCreated)

	// Attempt to read as guest
	response = rt.SendRequest("GET", "/{{.keyspace}}/doc", "")
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, `{"_id":"doc","_rev":"1-ca9ad22802b66f662ff171f226211d5c"}`, string(response.BodyBytes()))

	// Attempt to write as guest
	response = rt.SendRequest("PUT", "/{{.keyspace}}/doc?rev=1-ca9ad22802b66f662ff171f226211d5c", `{"val": "newval"}`)
	RequireStatus(t, response, http.StatusForbidden)

	// Attempt to access _blipsync as guest - blip sync handling for read-only GUEST is applied at replication level (to allow pull-only replications).
	// Should succeed permission check, and only fail on websocket upgrade
	response = rt.SendRequest("GET", "/{{.db}}/_blipsync", "")
	RequireStatus(t, response, http.StatusUpgradeRequired)

	// Verify matching on _blipsync path doesn't incorrectly match docs, attachments
	response = rt.SendRequest("PUT", "/{{.keyspace}}/doc_named_blipsync", "")
	RequireStatus(t, response, http.StatusForbidden)
	response = rt.SendRequest("PUT", "/{{.keyspace}}/doc/_blipsync", "")
	RequireStatus(t, response, http.StatusForbidden)

}
