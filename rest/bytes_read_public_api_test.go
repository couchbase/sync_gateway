// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBytesReadDcoOperations(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// create a user to authenticate as for public api calls and assert the stat hasn't incremented as a result
	rt.CreateUser("greg", []string{"ABC"})
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, 0)
	require.True(t, ok)

	// use public api to put a doc through SGW then assert the stat has increased
	input := `{"foo":"bar", "channels":["ABC"]}`
	inputBytes := []byte(input)
	resp := rt.SendUserRequest(http.MethodPut, "/{{.keyspace}}/doc1", input, "greg")
	RequireStatus(t, resp, http.StatusCreated)

	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

	// send admin request assert that the public rest count doesn't increase
	resp = rt.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/doc1", "")
	RequireStatus(t, resp, http.StatusOK)
	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

	// send user request that has empty body, asser the stat doesn't increase
	resp = rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/doc1", "", "greg")
	RequireStatus(t, resp, http.StatusOK)
	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

	// assert blipsync connection doesn't increment stat
	resp = rt.SendUserRequest(http.MethodGet, "/{{.db}}/_blipsync", "", "greg")
	RequireStatus(t, resp, http.StatusUpgradeRequired)
	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

	srv := httptest.NewServer(rt.TestMetricsHandler())
	defer srv.Close()
	httpClient := http.DefaultClient

	// test metrics endpoint, assert the stat doesn't increment
	response, err := httpClient.Get(srv.URL + "/_metrics")
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, response.StatusCode)

	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

	// test public endpoint but one that doesn't access a db and assert that doesn't increment stat
	resp = rt.SendUserRequest(http.MethodGet, "/", "", "greg")
	RequireStatus(t, resp, http.StatusOK)
	// assert the stat doesn't increment
	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

	// send another public request (this time POST) to check stat increments, but check it increments by correct bytes value
	input = fmt.Sprint(`{"foo":"bar", "channels":["ABC"]}`)
	inputBytes2 := []byte(input)

	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/", input, "greg")
	RequireStatus(t, resp, http.StatusOK)

	cumulativeBytes := len(inputBytes) + len(inputBytes2)
	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(cumulativeBytes))
	require.True(t, ok)
}

func TestBytesReadChanges(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", nil)
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, 0)
	require.True(t, ok)

	changesJSON := `{"style":"all_docs", "timeout":6000, "feed":"longpoll", "limit":50, "since":"0"}`
	byteArrayChangesBody := []byte(changesJSON)
	resp := rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", changesJSON, "alice")
	RequireStatus(t, resp, http.StatusOK)

	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(byteArrayChangesBody)))
	require.True(t, ok)

}

func TestBytesReadPutAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	response := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), "doc1"), `{"channels":["ABC"]}`)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.Equal(t, true, body["ok"])
	revid := body["rev"].(string)

	response = rt.SendUserRequest(http.MethodGet, fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), "doc1"), ``, "alice")
	RequireStatus(t, response, 200)

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	byteArrayAttachmentBody := []byte(attachmentBody)

	// attach to existing document created above
	resp := rt.SendUserRequestWithHeaders("PUT", "/{{.keyspace}}/doc1/attach1?rev="+revid, attachmentBody, reqHeaders, "alice", "letmein")
	RequireStatus(t, resp, 201)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(byteArrayAttachmentBody)))
	require.True(t, ok)

}

func TestBytesReadRevDiff(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", nil)

	// Create some docs:
	input := `{"new_edits":false, "docs": [
                    {"_id": "rd1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "rd2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	resp := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, resp, 201)

	// Now call _revs_diff:
	input = `{"rd1": ["13-def", "12-xyz"],
              "rd2": ["34-def"],
              "rd9": ["1-a", "2-b", "3-c"],
              "_design/ddoc": ["1-woo"]
             }`
	inputBytes := []byte(input)
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_revs_diff", input, "alice")
	RequireStatus(t, resp, 200)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)
}

func TestBytesReadAllDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/doc5", `{"channels":"ABC"}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc4", `{"channels":["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc3", `{"channels":["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc2", `{"channels":["ABC"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"channels":[]}`)
	RequireStatus(t, resp, http.StatusCreated)

	input := `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	inputBytes := []byte(input)
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_all_docs", input, "alice")
	RequireStatus(t, resp, 200)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}

func TestBytesReadBulkDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}, {"_id": "_local/bulk3", "n": 3}]}`
	inputBytes := []byte(input)
	response := rt.SendUserRequest("POST", "/{{.keyspace}}/_bulk_docs", input, "alice")
	RequireStatus(t, response, 201)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}

func TestBytesReadBulkGet(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/expNumericTTL", `{"bulk":"docs"}`)
	RequireStatus(t, resp, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
	revId := body["rev"].(string)

	input := fmt.Sprintf(`{"docs": [{"id": "expNumericTTL", "rev": "%s"}]}`, revId)
	inputBytes := []byte(input)
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_bulk_get", input, "alice")
	RequireStatus(t, resp, 200)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}

func TestBytesReadLocalDocPut(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	input := `{"local": "doc"}`
	inputBytes := []byte(input)
	resp := rt.SendUserRequest(http.MethodPut, "/{{.keyspace}}/_local/doc1", input, "alice")
	RequireStatus(t, resp, 201)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)
}

func TestBytesReadPOSTSession(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	input := `{"name":"alice","password":"letmein"}`
	inputBytes := []byte(input)
	resp := rt.SendUserRequest(http.MethodPost, "/{{.db}}/_session", input, "alice")
	RequireStatus(t, resp, http.StatusOK)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)
}
