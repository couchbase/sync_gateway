package rest

import (
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

func (rt *RestTester) putDoc(docID string, body string) (response putDocResponse) {
	rawResponse := rt.SendAdminRequest("PUT", "/db/"+docID, body)
	assertStatus(rt.tb, rawResponse, 201)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.tb, response.Ok)
	require.NotEmpty(rt.tb, response.Rev)
	return response
}

func (rt *RestTester) RequireWaitChanges(numChangesExpected int, since string) changesResults {
	changesResults, err := rt.WaitForChanges(numChangesExpected, "/db/_changes?since="+since, "", true)
	require.NoError(rt.tb, err)
	require.Len(rt.tb, changesResults.Results, numChangesExpected)
	return changesResults
}
