/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"fmt"
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/require"
)

type putDocResponse struct {
	ID  string
	Ok  bool
	Rev string
}

func (rt *RestTester) getDoc(docID string) (body db.Body) {
	rawResponse := rt.SendAdminRequest("GET", "/db/"+docID, "")
	assertStatus(rt.tb, rawResponse, 200)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
	return body
}

func (rt *RestTester) requireDocNotFound(docID string) {
	rawResponse := rt.SendAdminRequest(http.MethodGet, "/db/"+docID, "")
	assertStatus(rt.tb, rawResponse, http.StatusNotFound)
}

func (rt *RestTester) putDoc(docID string, body string) (response putDocResponse) {
	rawResponse := rt.SendAdminRequest("PUT", "/db/"+docID, body)
	assertStatus(rt.tb, rawResponse, 201)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.tb, response.Ok)
	require.NotEmpty(rt.tb, response.Rev)
	return response
}

func (rt *RestTester) updateDoc(docID, revID, body string) (response putDocResponse) {
	resource := fmt.Sprintf("/db/%s?rev=%s", docID, revID)
	rawResponse := rt.SendAdminRequest(http.MethodPut, resource, body)
	assertStatus(rt.tb, rawResponse, http.StatusCreated)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.tb, response.Ok)
	require.NotEmpty(rt.tb, response.Rev)
	return response
}

func (rt *RestTester) tombstoneDoc(docID string, revID string) {
	rawResponse := rt.SendAdminRequest("DELETE", "/db/"+docID+"?rev="+revID, "")
	assertStatus(rt.tb, rawResponse, 200)
}

func (rt *RestTester) upsertDoc(docID string, body string) (response putDocResponse) {

	getResponse := rt.SendAdminRequest("GET", "/db/"+docID, "")
	if getResponse.Code == 404 {
		return rt.putDoc(docID, body)
	}
	var getBody db.Body
	require.NoError(rt.tb, base.JSONUnmarshal(getResponse.Body.Bytes(), &getBody))
	revID, ok := getBody["revID"].(string)
	require.True(rt.tb, ok)

	rawResponse := rt.SendAdminRequest("PUT", "/db/"+docID+"?rev="+revID, body)
	assertStatus(rt.tb, rawResponse, 200)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.tb, response.Ok)
	require.NotEmpty(rt.tb, response.Rev)
	return response
}

func (rt *RestTester) deleteDoc(docID, revID string) {
	assertStatus(rt.tb, rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/db/%s?rev=%s", docID, revID), ""), http.StatusOK)
}

// prugeDoc removes all the revisions (active and tombstones) of the specified document.
func (rt *RestTester) purgeDoc(docID string) {
	response := rt.SendAdminRequest(http.MethodPost, "/db/_purge", fmt.Sprintf(`{"%s":["*"]}`, docID))
	assertStatus(rt.tb, response, http.StatusOK)
	var body map[string]interface{}
	require.NoError(rt.tb, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.Equal(rt.tb, body, map[string]interface{}{"purged": map[string]interface{}{docID: []interface{}{"*"}}})
}

// PutDocumentWithRevID builds a new_edits=false style put to create a revision with the specified revID.
// If parentRevID is not specified, treated as insert
func (rt *RestTester) putNewEditsFalse(docID string, newRevID string, parentRevID string, bodyString string) (response putDocResponse) {

	var body db.Body
	marshalErr := base.JSONUnmarshal([]byte(bodyString), &body)
	require.NoError(rt.tb, marshalErr)

	rawResponse, err := rt.PutDocumentWithRevID(docID, newRevID, parentRevID, body)
	require.NoError(rt.tb, err)
	assertStatus(rt.tb, rawResponse, 201)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.tb, response.Ok)
	require.NotEmpty(rt.tb, response.Rev)

	require.NoError(rt.tb, rt.WaitForPendingChanges())

	return response
}

func (rt *RestTester) RequireWaitChanges(numChangesExpected int, since string) changesResults {
	changesResults, err := rt.WaitForChanges(numChangesExpected, "/db/_changes?since="+since, "", true)
	require.NoError(rt.tb, err)
	require.Len(rt.tb, changesResults.Results, numChangesExpected)
	return changesResults
}

func (rt *RestTester) waitForRev(docID string, revID string) error {
	return rt.WaitForCondition(func() bool {
		rawResponse := rt.SendAdminRequest("GET", "/db/"+docID, "")
		if rawResponse.Code != 200 && rawResponse.Code != 201 {
			return false
		}
		var body db.Body
		require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
		return body.ExtractRev() == revID
	})
}
