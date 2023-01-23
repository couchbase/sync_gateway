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

func (rt *RestTester) RequireDocNotFound(docID string) {
	rawResponse := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), docID), "")
	RequireStatus(rt.TB, rawResponse, http.StatusNotFound)
}

func (rt *RestTester) TombstoneDoc(docID string, revID string) {
	rawResponse := rt.SendAdminRequest("DELETE", "/db/"+docID+"?rev="+revID, "")
	RequireStatus(rt.TB, rawResponse, 200)
}

// prugeDoc removes all the revisions (active and tombstones) of the specified document.
func (rt *RestTester) PurgeDoc(docID string) {
	response := rt.SendAdminRequest(http.MethodPost, fmt.Sprintf("/%s/_purge", rt.GetSingleKeyspace()), fmt.Sprintf(`{"%s":["*"]}`, docID))
	RequireStatus(rt.TB, response, http.StatusOK)
	var body map[string]interface{}
	require.NoError(rt.TB, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.Equal(rt.TB, body, map[string]interface{}{"purged": map[string]interface{}{docID: []interface{}{"*"}}})
}

// PutDocumentWithRevID builds a new_edits=false style put to create a revision with the specified revID.
// If parentRevID is not specified, treated as insert
func (rt *RestTester) PutNewEditsFalse(docID string, newRevID string, parentRevID string, bodyString string) (response PutDocResponse) {

	var body db.Body
	marshalErr := base.JSONUnmarshal([]byte(bodyString), &body)
	require.NoError(rt.TB, marshalErr)

	rawResponse, err := rt.PutDocumentWithRevID(docID, newRevID, parentRevID, body)
	require.NoError(rt.TB, err)
	RequireStatus(rt.TB, rawResponse, 201)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.TB, response.Ok)
	require.NotEmpty(rt.TB, response.Rev)

	require.NoError(rt.TB, rt.WaitForPendingChanges())

	return response
}

func (rt *RestTester) RequireWaitChanges(numChangesExpected int, since string) ChangesResults {
	changesResults, err := rt.WaitForChanges(numChangesExpected, "/{{.keyspace}}/_changes?since="+since, "", true)
	require.NoError(rt.TB, err)
	require.Len(rt.TB, changesResults.Results, numChangesExpected)
	return changesResults
}
