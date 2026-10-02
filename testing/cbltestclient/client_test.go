// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package cbltestclient

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// stubServer is a stand-in for the Couchbase Lite test server, recording what a client sent and
// replying with whatever the test set up.
type stubServer struct {
	*httptest.Server
	// requests records the body of every request, keyed by path.  GET / has no body and so records
	// an empty entry.
	requests map[string]json.RawMessage
	// headers records the headers of the last request to each path.
	headers map[string]http.Header
	// responses maps a path to the raw body to reply with.
	responses map[string]string
	// statuses maps a path to a non-200 status to reply with.
	statuses map[string]int
}

const stubAPIVersion = 1

func newStubServer(t *testing.T) *stubServer {
	stub := &stubServer{
		requests:  map[string]json.RawMessage{},
		headers:   map[string]http.Header{},
		responses: map[string]string{"/": `{"version":"4.1.2-2","apiVersion":1,"cbl":"couchbase-lite-c","additionalInfo":"Edition: Enterprise, Build: 2"}`},
		statuses:  map[string]int{},
	}
	stub.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		stub.requests[r.URL.Path] = body
		stub.headers[r.URL.Path] = r.Header.Clone()

		w.Header().Set("Content-Type", "application/json")
		if status, failing := stub.statuses[r.URL.Path]; failing {
			w.WriteHeader(status)
		}
		_, err = io.WriteString(w, stub.responses[r.URL.Path])
		assert.NoError(t, err)
	}))
	t.Cleanup(stub.Close)
	return stub
}

// requestBody decodes the body the client sent to path into v.
func (s *stubServer) requestBody(t *testing.T, path string, v any) {
	t.Helper()
	body, found := s.requests[path]
	require.True(t, found, "no request was made to %s", path)
	require.NoError(t, base.JSONUnmarshal(body, v))
}

func newStubClient(t *testing.T) (*Client, *stubServer) {
	t.Helper()
	stub := newStubServer(t)
	client, err := NewClient(base.TestCtx(t), stub.URL)
	require.NoError(t, err)
	return client, stub
}

func TestClientNegotiatesAPIVersion(t *testing.T) {
	client, stub := newStubClient(t)
	// The spec is at API version 2 while the C server still reports 1, so the version has to come
	// from the server rather than from a constant.
	assert.Equal(t, stubAPIVersion, client.APIVersion())
	assert.NotEmpty(t, client.ClientID())
	// GET / is reachable before a session exists, and carries neither header.
	assert.Empty(t, stub.headers["/"].Get(apiVersionHeader))
	assert.Empty(t, stub.headers["/"].Get(clientIDHeader))
}

func TestClientSendsSessionHeaders(t *testing.T) {
	client, stub := newStubClient(t)
	// The test server rejects a request whose API version doesn't match its own exactly, and one
	// with no client ID, so both headers go on every request but GET /.
	require.NoError(t, client.Reset(base.TestCtx(t), t.Name(), nil))

	assert.Equal(t, "1", stub.headers["/reset"].Get(apiVersionHeader))
	assert.Equal(t, client.ClientID(), stub.headers["/reset"].Get(clientIDHeader))
	assert.Equal(t, "application/json", stub.headers["/reset"].Get("Content-Type"))
}

func TestClientNewSession(t *testing.T) {
	client, stub := newStubClient(t)
	// The C test server requires dataset_version even though the spec doesn't list it, and the
	// session ID has to match the header every later request sends.
	stub.responses["/newSession"] = "null"
	require.NoError(t, client.NewSession(base.TestCtx(t), ""))

	var request struct {
		ID             string `json:"id"`
		DatasetVersion string `json:"dataset_version"`
	}
	stub.requestBody(t, "/newSession", &request)
	assert.Equal(t, client.ClientID(), request.ID)
	assert.Equal(t, DefaultDatasetVersion, request.DatasetVersion)
}

func TestClientReset(t *testing.T) {
	client, stub := newStubClient(t)
	databases := map[string]DatabaseSpec{"db1": {Collections: []string{"scope.collection"}}}
	require.NoError(t, client.Reset(base.TestCtx(t), "TestSomething", databases))

	var request struct {
		Test      string                  `json:"test"`
		Databases map[string]DatabaseSpec `json:"databases"`
	}
	stub.requestBody(t, "/reset", &request)
	assert.Equal(t, "TestSomething", request.Test)
	assert.Equal(t, databases, request.Databases)
}

func TestClientGetDocument(t *testing.T) {
	client, stub := newStubClient(t)
	stub.responses["/getDocument"] = `{"_id":"doc1","_revs":"1000@src","foo":"bar"}`

	doc, err := client.GetDocument(base.TestCtx(t), "db1", DocumentRef{Collection: "scope.collection", ID: "doc1"})
	require.NoError(t, err)
	assert.Equal(t, "doc1", doc.ID())
	assert.Equal(t, "1000@src", doc.Revs())
	assert.Equal(t, map[string]any{"foo": "bar"}, doc.Properties())
}

func TestClientGetDocumentNotFound(t *testing.T) {
	// The spec says an absent document is a 404, but the C test server reports it as a 400 whose
	// message is the only thing distinguishing it from a malformed request.  Both have to be
	// recognised, or a missing document looks like a broken client.
	testCases := []struct {
		name     string
		status   int
		body     string
		notFound bool
	}{
		{
			name:     "400 as older C servers report it",
			status:   http.StatusBadRequest,
			body:     `{"code":400,"domain":"TESTSERVER","message":"Document '_default._default.doc1' not found"}`,
			notFound: true,
		},
		{
			name:     "404 as the spec and newer C servers report it",
			status:   http.StatusNotFound,
			body:     `{"code":404,"domain":"TESTSERVER","message":"Document '_default._default.doc1' not found"}`,
			notFound: true,
		},
		{
			name:   "an unrelated 400 is not a missing document",
			status: http.StatusBadRequest,
			body:   `{"code":400,"domain":"TESTSERVER","message":"Collection 'scope.collection' Not Found"}`,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			client, stub := newStubClient(t)
			stub.statuses["/getDocument"] = testCase.status
			stub.responses["/getDocument"] = testCase.body

			_, err := client.GetDocument(base.TestCtx(t), "db1", DocumentRef{Collection: "scope.collection", ID: "doc1"})
			require.Error(t, err)
			assert.Equal(t, testCase.notFound, IsDocumentNotFound(err))
		})
	}
}

func TestClientStartReplicator(t *testing.T) {
	client, stub := newStubClient(t)
	stub.responses["/startReplicator"] = `{"id":"repl-1"}`

	config := ReplicatorConfig{
		Database:       "db1",
		Collections:    []ReplicationCollection{{Names: []string{"scope.collection"}}},
		Endpoint:       "ws://127.0.0.1:4984/db",
		ReplicatorType: ReplicatorTypePush,
		Continuous:     true,
		Authenticator:  &Authenticator{Type: AuthenticatorTypeBasic, Username: "user", Password: "letmein"},
	}
	id, err := client.StartReplicator(base.TestCtx(t), config, true)
	require.NoError(t, err)
	assert.Equal(t, "repl-1", id)

	// The config is nested under "config", alongside the reset flag.
	var request struct {
		Config ReplicatorConfig `json:"config"`
		Reset  bool             `json:"reset"`
	}
	stub.requestBody(t, "/startReplicator", &request)
	assert.Equal(t, config, request.Config)
	assert.True(t, request.Reset)

	// enableAutoPurge defaults to true on the test server, so a false has to be sent rather than
	// omitted.
	var raw struct {
		Config map[string]any `json:"config"`
	}
	stub.requestBody(t, "/startReplicator", &raw)
	_, sent := raw.Config["enableAutoPurge"]
	assert.True(t, sent, "enableAutoPurge must be sent explicitly, since the test server defaults it to true")
}

func TestClientReplicatorStatus(t *testing.T) {
	client, stub := newStubClient(t)
	stub.responses["/getReplicatorStatus"] = `{"activity":"IDLE","progress":{"completed":true},"documents":[{"collection":"scope.collection","documentID":"doc1","isPush":true,"flags":[]}]}`

	status, err := client.ReplicatorStatus(base.TestCtx(t), "repl-1")
	require.NoError(t, err)
	assert.Equal(t, ReplicatorActivityIdle, status.Activity)
	assert.True(t, status.Progress.Completed)
	require.Len(t, status.Documents, 1)
	assert.Equal(t, "doc1", status.Documents[0].DocumentID)
	assert.Nil(t, status.Error)
}

func TestClientReplicatorStatusError(t *testing.T) {
	client, stub := newStubClient(t)
	// A replicator that failed reports the error inside a successful response, so it must not be
	// mistaken for a request failure.
	stub.responses["/getReplicatorStatus"] = `{"activity":"STOPPED","progress":{"completed":false},"error":{"domain":"CBL","code":10401,"message":"Unauthorized"}}`

	status, err := client.ReplicatorStatus(base.TestCtx(t), "repl-1")
	require.NoError(t, err)
	assert.Equal(t, ReplicatorActivityStopped, status.Activity)
	require.NotNil(t, status.Error)
	assert.Equal(t, "Unauthorized", status.Error.Message)
	assert.Equal(t, ErrorDomainCBL, status.Error.Domain)
}

func TestClientAPIError(t *testing.T) {
	client, stub := newStubClient(t)
	stub.statuses["/updateDatabase"] = http.StatusInternalServerError
	stub.responses["/updateDatabase"] = `{"code":7,"domain":"CBL","message":"Database is closed"}`

	err := client.UpdateDatabase(base.TestCtx(t), "db1", nil)
	require.Error(t, err)
	var apiErr *APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, http.StatusInternalServerError, apiErr.StatusCode)
	assert.Equal(t, ErrorDomainCBL, apiErr.Domain)
	assert.Equal(t, 7, apiErr.Code)
}

func TestClientEmptyResponseBody(t *testing.T) {
	// /reset answers 200 with no body at all, and /newSession with a literal null.
	client, stub := newStubClient(t)
	for path, body := range map[string]string{"/reset": "", "/newSession": "null"} {
		stub.responses[path] = body
	}
	ctx := base.TestCtx(t)
	require.NoError(t, client.Reset(ctx, "TestSomething", nil))
	require.NoError(t, client.NewSession(ctx, "4.0"))
}

func TestServerInfoEdition(t *testing.T) {
	testCases := []struct {
		additionalInfo string
		enterprise     bool
	}{
		{additionalInfo: "Edition: Enterprise, Build: 2", enterprise: true},
		{additionalInfo: "Edition: enterprise, Build: 0", enterprise: true},
		{additionalInfo: "Edition: Community, Build: 2"},
		{additionalInfo: ""},
	}
	for _, testCase := range testCases {
		t.Run(testCase.additionalInfo, func(t *testing.T) {
			assert.Equal(t, testCase.enterprise, ServerInfo{AdditionalInfo: testCase.additionalInfo}.IsEnterprise())
		})
	}
}

func TestServerInfoMajorVersion(t *testing.T) {
	assert.Equal(t, 4, ServerInfo{Version: "4.1.2"}.MajorVersion())
	assert.Equal(t, 4, ServerInfo{Version: "4.1.2-2"}.MajorVersion())
	assert.Equal(t, 0, ServerInfo{Version: "not-a-version"}.MajorVersion())
}
