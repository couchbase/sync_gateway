// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"compress/gzip"
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

func TestBytesReadDocOperations(t *testing.T) {
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

	// create a user and assert this doesn't increase the bytes read stat
	rt.CreateUser("alice", nil)
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, 0)
	require.True(t, ok)

	// take the bytes of the body we will pass into request and perform changes POST request
	changesJSON := `{"style":"all_docs", "timeout":6000, "feed":"longpoll", "limit":50, "since":"0"}`
	byteArrayChangesBody := []byte(changesJSON)
	resp := rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", changesJSON, "alice")
	RequireStatus(t, resp, http.StatusOK)

	// assert the stat has increased by the number of bytes passed into request
	_, ok = base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(byteArrayChangesBody)))
	require.True(t, ok)

}

func TestBytesReadPutAttachment(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// create user
	rt.CreateUser("alice", []string{"ABC"})

	// add a doc for an attachment to be added to
	resp := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), "doc1"), `{"channels":["ABC"]}`)
	RequireStatus(t, resp, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
	require.Equal(t, true, body["ok"])
	revid := body["rev"].(string)

	resp = rt.SendUserRequest(http.MethodGet, fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), "doc1"), ``, "alice")
	RequireStatus(t, resp, 200)

	attachmentBody := "this is the body of attachment"
	attachmentContentType := "content/type"
	reqHeaders := map[string]string{
		"Content-Type": attachmentContentType,
	}
	byteArrayAttachmentBody := []byte(attachmentBody)

	// attach to existing document created above
	resp = rt.SendUserRequestWithHeaders("PUT", "/{{.keyspace}}/doc1/attach1?rev="+revid, attachmentBody, reqHeaders, "alice", "letmein")
	RequireStatus(t, resp, 201)

	// assert the stat has increased by the attachment endpoint input
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(byteArrayAttachmentBody)))
	require.True(t, ok)

}

func TestBytesReadRevDiff(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", nil)

	// Create some docs
	input := `{"new_edits":false, "docs": [
                    {"_id": "rd1", "_rev": "12-abc", "n": 1,
                     "_revisions": {"start": 12, "ids": ["abc", "eleven", "ten", "nine"]}},
                    {"_id": "rd2", "_rev": "34-def", "n": 2,
                     "_revisions": {"start": 34, "ids": ["def", "three", "two", "one"]}}
              ]}`
	resp := rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, resp, 201)

	// Now call _revs_diff as the user and take the bytes length of the endpoint input
	input = `{"rd1": ["13-def", "12-xyz"],
              "rd2": ["34-def"],
              "rd9": ["1-a", "2-b", "3-c"],
              "_design/ddoc": ["1-woo"]
             }`
	inputBytes := []byte(input)
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_revs_diff", input, "alice")
	RequireStatus(t, resp, 200)

	// assert the stat has increased by the bytes above
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)
}

func TestBytesReadAllDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	// add a load of docs
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

	// send user request to _all_docs an take the bytes length of input for the endpoint
	input := `{"keys": ["doc4", "doc1", "doc3", "b0gus"]}`
	inputBytes := []byte(input)
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_all_docs", input, "alice")
	RequireStatus(t, resp, 200)

	// assert the stat has increased by the bytes length
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}

func TestBytesReadBulkDocs(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	// call bulk_docs as user. Take bytes of the input string and asser the stat has increased by that length
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}, {"_id": "_local/bulk3", "n": 3}]}`
	inputBytes := []byte(input)
	resp := rt.SendUserRequest("POST", "/{{.keyspace}}/_bulk_docs", input, "alice")
	RequireStatus(t, resp, 201)

	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}

func TestBytesReadBulkGet(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	// add single doc
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/testdoc", `{"bulk":"docs"}`)
	RequireStatus(t, resp, 201)
	var body db.Body
	assert.NoError(t, base.JSONUnmarshal(resp.Body.Bytes(), &body))
	revId := body["rev"].(string)

	// construct input to the endpoint and take the byte array opf the string input
	input := fmt.Sprintf(`{"docs": [{"id": "testdoc", "rev": "%s"}]}`, revId)
	inputBytes := []byte(input)
	resp = rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_bulk_get", input, "alice")
	RequireStatus(t, resp, 200)

	// assert the stat has increased by the length of byte array
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}

func TestBytesReadLocalDocPut(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	// create a local doc using the public endpoint as the user created above
	input := `{"local": "doc"}`
	inputBytes := []byte(input)
	resp := rt.SendUserRequest(http.MethodPut, "/{{.keyspace}}/_local/doc1", input, "alice")
	RequireStatus(t, resp, 201)

	// assert the stat is increased by the correct amount
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)
}

func TestBytesReadPOSTSession(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})

	// create session
	input := `{"name":"alice","password":"letmein"}`
	inputBytes := []byte(input)
	resp := rt.SendUserRequest(http.MethodPost, "/{{.db}}/_session", input, "alice")
	RequireStatus(t, resp, http.StatusOK)

	// assert the stat is increased by the correct amount
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)
}

func TestBytesReadAuthFailed(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// create a user with different password to the default one
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", GetUserPayload(t, "alice", "pass", "", rt.GetSingleTestDatabaseCollection(), []string{"ABC"}, nil))
	RequireStatus(t, resp, http.StatusCreated)

	// make a request that will fail on auth
	input := `{"docs": [{"_id": "bulk1", "n": 1}, {"_id": "bulk2", "n": 2}, {"_id": "_local/bulk3", "n": 3}]}`
	inputBytes := []byte(input)
	resp = rt.SendUserRequest("POST", "/{{.keyspace}}/_bulk_docs", input, "alice")
	RequireStatus(t, resp, http.StatusUnauthorized)

	// assert the stat has still increased by the bytes of the body passed into request
	_, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}

func TestBytesReadGzipRequest(t *testing.T) {
	// Need default collection as request below doesn't work with {{.keyspace}}
	rt := NewRestTesterDefaultCollection(t, &RestTesterConfig{
		GuestEnabled: true,
	})
	defer rt.Close()

	rt.CreateUser("alice", []string{"ABC"})
	input := `{"channel":["ABC"]}`

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	_, err := gz.Write([]byte(input))
	require.NoError(t, err)
	err = gz.Close()
	require.NoError(t, err)

	inputBytes := []byte(input)

	// {{.keyspace}} isn't supported so use default collection
	rq, err := http.NewRequest("PUT", "/db/doc", &buf)
	require.NoError(t, err)
	rq.Header.Set("Content-Encoding", "gzip")
	resp := rt.Send(rq)
	RequireStatus(t, resp, http.StatusCreated)

	_, ok := base.WaitForStat(func() int64 {
		fmt.Println(rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value())
		return rt.GetDatabase().DbStats.DatabaseStats.PublicRestBytesRead.Value()
	}, int64(len(inputBytes)))
	require.True(t, ok)

}
