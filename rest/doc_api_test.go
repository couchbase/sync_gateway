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
	"io"
	"mime"
	"mime/multipart"
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
			"string"},
		{"minInt64",
			`{"number": -9223372036854775808}`,
			"-9223372036854775808",
			"string"},
		{"greaterThanMaxInt64",
			`{"number": 9223372036854775808}`,
			"9223372036854775808",
			"string"},
		{"lessThanMinInt64",
			`{"number": -9223372036854775809}`,
			"9223372036854775809",
			"string"},
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
			"string"},
		{"javascriptLessThanMinSafeInt",
			`{"number": -9007199254740992}`,
			"-9007199254740992",
			"string"},
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
		rt.Run(test.name, func(ts *testing.T) {
			// Create document
			response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+test.name, test.body)
			RequireStatus(ts, response, 201)

			// Get document, validate number value
			getResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+test.name, "")
			RequireStatus(ts, getResponse, 200)

			// Check the raw bytes, because unmarshalling the response would be another opportunity for the number to get modified
			responseString := string(getResponse.Body.Bytes())
			if !strings.Contains(responseString, test.expectedString) {
				ts.Errorf("Response does not contain the expected number format (%s).  Response: %s", test.name, responseString)
			}

			// Check channel assignment
			getRawResponse := rt.SendAdminRequest("GET", fmt.Sprintf("/{{.keyspace}}/_raw/%s?redact=false", test.name), "")
			RequireStatus(t, getRawResponse, 200)
			var rawResponse RawDocResponse
			require.NoError(ts, base.JSONUnmarshal(getRawResponse.Body.Bytes(), &rawResponse))
			assert.Equal(ts, 1, len(rawResponse.Xattrs.Sync.Channels))
			assert.Containsf(ts, rawResponse.Xattrs.Sync.Channels, test.expectedFormatChannel, "Expected channel %s was not found in document channels (%s)", test.expectedFormatChannel, test.name)
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

	const docID = "doc"

	// Write a document as admin
	response := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"val": "test"}`)
	RequireStatus(t, response, http.StatusCreated)

	// Attempt to read as guest
	body := rt.GetDocBody(docID)
	assert.Equal(t, "test", body["val"].(string))
	rev := body["_rev"].(string)

	// Attempt to write as guest
	response = rt.SendRequest(http.MethodPut, "/{{.keyspace}}/doc?rev="+rev, `{"val": "newval"}`)
	RequireStatus(t, response, http.StatusForbidden)

	// Attempt to access _blipsync as guest - blip sync handling for read-only GUEST is applied at replication level (to allow pull-only replications).
	// Should succeed permission check, and only fail on websocket upgrade
	response = rt.SendRequest(http.MethodGet, "/{{.db}}/_blipsync", "")
	RequireStatus(t, response, http.StatusUpgradeRequired)

	// Verify matching on _blipsync path doesn't incorrectly match docs, attachments
	response = rt.SendRequest(http.MethodPut, "/{{.keyspace}}/doc_named_blipsync", "")
	RequireStatus(t, response, http.StatusForbidden)
	response = rt.SendRequest(http.MethodPut, "/{{.keyspace}}/doc/_blipsync", "")
	RequireStatus(t, response, http.StatusForbidden)

}

func TestGetDocWithCV(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	docID := "doc1"
	docVersion := rt.PutDoc(docID, `{"foo": "bar"}`)
	testCases := []struct {
		name      string
		url       string
		output    string
		headers   map[string]string
		multipart bool
	}{
		{
			name:   "get doc with cv",
			url:    "/{{.keyspace}}/doc1",
			output: fmt.Sprintf(`{"_id":"%s","_rev":"%s","_cv":"%s","foo":"bar"}`, docID, docVersion.RevTreeID, docVersion.CV),
		},
		{
			name:   "get doc with open_revs=all and cv no multipart",
			url:    "/{{.keyspace}}/doc1?open_revs=all",
			output: fmt.Sprintf(`[{"ok": {"_id":"%s","_rev":"%s","_cv":"%s","foo":"bar"}}]`, docID, docVersion.RevTreeID, docVersion.CV),
			headers: map[string]string{
				"Accept": "application/json",
			},
		},

		{
			name:      "get doc with open_revs=all and cv",
			url:       "/{{.keyspace}}/doc1?open_revs=all",
			output:    fmt.Sprintf(`{"_id":"%s","_rev":"%s","_cv":"%s","foo":"bar"}`, docID, docVersion.RevTreeID, docVersion.CV),
			multipart: true,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response := rt.SendAdminRequestWithHeaders("GET", testCase.url, "", testCase.headers)
			RequireStatus(t, response, http.StatusOK)
			output := response.BodyString()
			if testCase.multipart {
				multipartOutput := readMultiPartBody(t, response)
				require.Len(t, multipartOutput, 1)
				output = multipartOutput[0]
			}
			assert.JSONEq(t, testCase.output, output)
		})
	}

}

func TestBulkGetWithCV(t *testing.T) {
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	doc1ID := "doc1"
	doc2ID := "doc2"
	doc1Version := rt.PutDoc(doc1ID, `{"foo": "bar"}`)
	doc2Version := rt.PutDoc(doc2ID, `{"foo": "baz"}`)
	testCases := []struct {
		name   string
		url    string
		input  string
		output []string
	}{
		{
			name:  "get doc multipart",
			url:   "/{{.keyspace}}/_bulk_get",
			input: fmt.Sprintf(`{"docs":[{"id":"%s"},{"id":"%s"}]}`, doc1ID, doc2ID),
			output: []string{
				fmt.Sprintf(`{"_id":"%s","_rev":"%s","foo":"bar"}`, doc1ID, doc1Version.RevTreeID),
				fmt.Sprintf(`{"_id":"%s","_rev":"%s","foo":"baz"}`, doc2ID, doc2Version.RevTreeID),
			},
		},
		{
			name:  "get doc multipart",
			url:   "/{{.keyspace}}/_bulk_get?show_cv=true",
			input: fmt.Sprintf(`{"docs":[{"id":"%s"},{"id":"%s"}]}`, doc1ID, doc2ID),
			output: []string{
				fmt.Sprintf(`{"_id":"%s","_rev":"%s","foo":"bar", "_cv": "%s"}`, doc1ID, doc1Version.RevTreeID, doc1Version.CV),
				fmt.Sprintf(`{"_id":"%s","_rev":"%s","foo":"baz", "_cv": "%s"}`, doc2ID, doc2Version.RevTreeID, doc2Version.CV),
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response := rt.SendAdminRequest(http.MethodPost, testCase.url, testCase.input)
			RequireStatus(t, response, http.StatusOK)
			bodies := readMultiPartBody(t, response)
			require.Len(t, bodies, len(testCase.output))
			for i, body := range bodies {
				assert.JSONEq(t, testCase.output[i], body)
			}
		})
	}

}

// readMultiPartBody reads a multipart response body and returns the parts as strings
func readMultiPartBody(t *testing.T, response *TestResponse) []string {
	_, params, err := mime.ParseMediaType(response.Header().Get("Content-Type"))
	require.NoError(t, err)
	mr := multipart.NewReader(response.Body, params["boundary"])
	var output []string
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		bodyBytes, err := io.ReadAll(p)
		require.NoError(t, err)
		output = append(output, string(bodyBytes))
	}
	return output
}

func TestCVUnescapedRevQueryParam(t *testing.T) {
	tests := []struct {
		revValue    string
		expectError bool
	}{
		{revValue: "1-abc"}, // Normal Rev ID (doesn't need escaping)
		{revValue: "185dd4a2b4490000%404EZjEgl1AKEj8qh%2BvXS7OQ"},                // CV escaped
		{revValue: "185dd4a2b4490000@4EZjEgl1AKEj8qh+vXS7OQ", expectError: true}, // CV unescaped
	}

	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	for _, test := range tests {
		t.Run(test.revValue, func(t *testing.T) {
			resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/testdoc?rev="+test.revValue, `{"foo":"bar"}`)
			if test.expectError {
				RequireStatus(t, resp, http.StatusBadRequest)
				assert.Contains(t, resp.BodyString(), "Bad rev query parameter")
			} else {
				// this is "successful" since there isn't a doc that exists with that rev but the request made it through
				RequireStatus(t, resp, http.StatusConflict)
				assert.Contains(t, resp.BodyString(), `Document revision conflict`)
			}
		})
	}
}
