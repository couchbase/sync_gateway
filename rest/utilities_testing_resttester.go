package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type PutDocResponse struct {
	ID  string
	Ok  bool
	Rev string
}

func (rt *RestTester) GetDoc(docID string) (body db.Body) {
	rawResponse := rt.SendAdminRequest("GET", "/db/"+docID, "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
	return body
}

func (rt *RestTester) CreateDoc(t *testing.T, docid string) string {
	response := rt.SendAdminRequest("PUT", "/db/"+docid, `{"prop":true}`)
	RequireStatus(t, response, 201)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, true, body["ok"])
	revid := body["rev"].(string)
	if revid == "" {
		t.Fatalf("No revid in response for PUT doc")
	}
	return revid
}

func (rt *RestTester) PutDoc(docID string, body string) (response PutDocResponse) {
	rawResponse := rt.SendAdminRequest("PUT", "/db/"+docID, body)
	RequireStatus(rt.TB, rawResponse, 201)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.TB, response.Ok)
	require.NotEmpty(rt.TB, response.Rev)
	return response
}

func (rt *RestTester) UpdateDoc(docID, revID, body string) (response PutDocResponse) {
	resource := fmt.Sprintf("/db/%s?rev=%s", docID, revID)
	rawResponse := rt.SendAdminRequest(http.MethodPut, resource, body)
	RequireStatus(rt.TB, rawResponse, http.StatusCreated)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.TB, response.Ok)
	require.NotEmpty(rt.TB, response.Rev)
	return response
}

func (rt *RestTester) upsertDoc(docID string, body string) (response PutDocResponse) {

	getResponse := rt.SendAdminRequest("GET", "/db/"+docID, "")
	if getResponse.Code == 404 {
		return rt.PutDoc(docID, body)
	}
	var getBody db.Body
	require.NoError(rt.TB, base.JSONUnmarshal(getResponse.Body.Bytes(), &getBody))
	revID, ok := getBody["revID"].(string)
	require.True(rt.TB, ok)

	rawResponse := rt.SendAdminRequest("PUT", "/db/"+docID+"?rev="+revID, body)
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.TB, response.Ok)
	require.NotEmpty(rt.TB, response.Rev)
	return response
}

func (rt *RestTester) DeleteDoc(docID, revID string) {
	RequireStatus(rt.TB, rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/db/%s?rev=%s", docID, revID), ""), http.StatusOK)
}

func (rt *RestTester) WaitForRev(docID string, revID string) error {
	return rt.WaitForCondition(func() bool {
		rawResponse := rt.SendAdminRequest("GET", "/db/"+docID, "")
		if rawResponse.Code != 200 && rawResponse.Code != 201 {
			return false
		}
		var body db.Body
		require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
		return body.ExtractRev() == revID
	})
}

// createReplication creates a replication via the REST API with the specified ID, remoteURL, direction and channel filter
func (rt *RestTester) createReplication(replicationID string, remoteURLString string, direction db.ActiveReplicatorDirection, channels []string, continuous bool, conflictResolver db.ConflictResolverType) {
	replicationConfig := &db.ReplicationConfig{
		ID:                     replicationID,
		Direction:              direction,
		Remote:                 remoteURLString,
		Continuous:             continuous,
		ConflictResolutionType: conflictResolver,
	}
	if len(channels) > 0 {
		replicationConfig.Filter = base.ByChannelFilter
		replicationConfig.QueryParams = map[string]interface{}{"channels": channels}
	}
	payload, err := json.Marshal(replicationConfig)
	require.NoError(rt.TB, err)
	resp := rt.SendAdminRequest(http.MethodPost, "/db/_replication/", string(payload))
	RequireStatus(rt.TB, resp, http.StatusCreated)
}

func (rt *RestTester) waitForAssignedReplications(count int) {
	successFunc := func() bool {
		replicationStatuses := rt.GetReplicationStatuses("?localOnly=true")
		return len(replicationStatuses) == count
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc))
}

func (rt *RestTester) WaitForReplicationStatus(replicationID string, targetStatus string) {
	successFunc := func() bool {
		status := rt.GetReplicationStatus(replicationID)
		return status.Status == targetStatus
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc))
}

func (rt *RestTester) GetReplications() (replications map[string]db.ReplicationCfg) {
	rawResponse := rt.SendAdminRequest("GET", "/db/_replication/", "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &replications))
	return replications
}

func (rt *RestTester) GetReplicationStatus(replicationID string) (status db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/db/_replicationStatus/"+replicationID, "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &status))
	return status
}

func (rt *RestTester) GetReplicationStatuses(queryString string) (statuses []db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/db/_replicationStatus/"+queryString, "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &statuses))
	return statuses
}
