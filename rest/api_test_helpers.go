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
	"encoding/json"
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

// prugeDoc removes all the revisions (active and tombstones) of the specified document.
func (rt *RestTester) PurgeDoc(docID string) {
	response := rt.SendAdminRequest(http.MethodPost, fmt.Sprintf("/%s/_purge", rt.GetSingleKeyspace()), fmt.Sprintf(`{"%s":["*"]}`, docID))
	RequireStatus(rt.TB, response, http.StatusOK)
	var body map[string]interface{}
	require.NoError(rt.TB, base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.Equal(rt.TB, body, map[string]interface{}{"purged": map[string]interface{}{docID: []interface{}{"*"}}})
}

// PutDocResponse should be replaced with functions that return DocVersion.
type PutDocResponse struct {
	ID  string
	Ok  bool
	Rev string
}

// PutNewEditsFalse builds a new_edits=false style put to create a revision with the specified revID.
// If parentRevID is not specified, treated as insert
func (rt *RestTester) PutNewEditsFalse(docID string, newVersion DocVersion, parentVersion DocVersion, bodyString string) DocVersion {

	var body db.Body
	marshalErr := base.JSONUnmarshal([]byte(bodyString), &body)
	require.NoError(rt.TB, marshalErr)

	requestBody := body.ShallowCopy()
	newRevGeneration, newRevDigest := db.ParseRevID(base.TestCtx(rt.TB), newVersion.RevID)

	revisions := make(map[string]interface{})
	revisions["start"] = newRevGeneration
	ids := []string{newRevDigest}
	if parentVersion.RevID != "" {
		_, parentDigest := db.ParseRevID(base.TestCtx(rt.TB), parentVersion.RevID)
		ids = append(ids, parentDigest)
	}
	revisions["ids"] = ids

	requestBody[db.BodyRevisions] = revisions
	requestBytes, err := json.Marshal(requestBody)
	require.NoError(rt.TB, err)
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?new_edits=false", string(requestBytes))
	RequireStatus(rt.TB, resp, 201)

	require.NoError(rt.TB, rt.WaitForPendingChanges())

	return DocVersionFromPutResponse(rt.TB, resp)
}

func (rt *RestTester) RequireWaitChanges(numChangesExpected int, since string) ChangesResults {
	changesResults, err := rt.WaitForChanges(numChangesExpected, "/{{.keyspace}}/_changes?since="+since, "", true)
	require.NoError(rt.TB, err)
	require.Len(rt.TB, changesResults.Results, numChangesExpected)
	return changesResults
}
