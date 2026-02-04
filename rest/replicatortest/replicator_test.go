//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package replicatortest

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/gocb/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
)

func TestReplicationAPI(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	replicationConfig := db.ReplicationConfig{
		ID:                 "replication1",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		Adhoc:              true,
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}

	// PUT replication
	response := rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", rest.MarshalConfig(t, replicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	// GET replication for PUT
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var configResponse db.ReplicationConfig
	err := json.Unmarshal(response.BodyBytes(), &configResponse)
	t.Logf("configResponse direction type: %T", configResponse.Direction)
	require.NoError(t, err)
	assert.Equal(t, "replication1", configResponse.ID)
	assert.Equal(t, "http://remote:4984/db", configResponse.Remote)
	assert.Equal(t, true, configResponse.Adhoc)
	assert.Equal(t, db.ActiveReplicatorTypePull, configResponse.Direction)

	// POST replication
	replicationConfig.ID = "replication2"
	response = rt.SendAdminRequest("POST", "/{{.db}}/_replication/", rest.MarshalConfig(t, replicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	// GET replication for POST
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replication/replication2", "")
	rest.RequireStatus(t, response, http.StatusOK)
	configResponse = db.ReplicationConfig{}
	err = json.Unmarshal(response.BodyBytes(), &configResponse)
	require.NoError(t, err)
	assert.Equal(t, "replication2", configResponse.ID)
	assert.Equal(t, "http://remote:4984/db", configResponse.Remote)
	assert.Equal(t, db.ActiveReplicatorTypePull, configResponse.Direction)

	// GET all replications
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replication/", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var replicationsResponse map[string]db.ReplicationConfig
	t.Logf("response: %s", response.BodyBytes())
	err = json.Unmarshal(response.BodyBytes(), &replicationsResponse)
	require.NoError(t, err)
	assert.Len(t, replicationsResponse, 2)
	_, ok := replicationsResponse["replication1"]
	assert.True(t, ok)
	_, ok = replicationsResponse["replication2"]
	assert.True(t, ok)

	// DELETE replication
	response = rt.SendAdminRequest("DELETE", "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Verify delete was successful
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusNotFound)

	// DELETE non-existent replication
	response = rt.SendAdminRequest("DELETE", "/{{.db}}/_replication/replication3", "")
	rest.RequireStatus(t, response, http.StatusNotFound)

}
func TestValidateReplicationAPI(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	tests := []struct {
		name                  string
		ID                    string
		config                db.ReplicationConfig
		expectedResponseCode  int
		expectedErrorContains string
	}{
		{
			name:                  "ID Mismatch",
			ID:                    "ID_Mismatch",
			config:                db.ReplicationConfig{ID: "ID_Mismatch_foo"},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "does not match request URI",
		},
		{
			name:                  "Missing Remote",
			ID:                    "Missing_Remote",
			config:                db.ReplicationConfig{ID: "Missing_Remote"},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "remote must be specified",
		},
		{
			name:                  "Missing Direction",
			ID:                    "Missing_Direction",
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db"},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "direction must be specified",
		},
		{
			name:                  "Valid Replication",
			ID:                    "Valid_Replication",
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db", Direction: "pull"},
			expectedResponseCode:  http.StatusCreated,
			expectedErrorContains: "",
		},
		{
			name:                  "Started adhoc",
			ID:                    "Started_adhoc",
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db", Direction: "pull", Adhoc: true, InitialState: db.ReplicationStateRunning},
			expectedResponseCode:  http.StatusCreated,
			expectedErrorContains: "",
		},
		{
			name:                  "Stopped adhoc",
			ID:                    "Stopped_adhoc",
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db", Direction: "pull", Adhoc: true, InitialState: db.ReplicationStateStopped},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "state=stopped is not valid for replications specifying adhoc=true",
		},
		{
			name:                  "Stopped non-adhoc",
			ID:                    "Stopped_non_adhoc",
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db", Direction: "pull", InitialState: db.ReplicationStateStopped},
			expectedResponseCode:  http.StatusCreated,
			expectedErrorContains: "",
		},
	}

	for _, test := range tests {
		rt.Run(test.name, func(t *testing.T) {
			test.config.CollectionsEnabled = !rt.GetDatabase().OnlyDefaultCollection()
			response := rt.SendAdminRequest("PUT", fmt.Sprintf("/{{.db}}/_replication/%s", test.ID), rest.MarshalConfig(t, test.config))
			rest.RequireStatus(t, response, test.expectedResponseCode)
			if test.expectedErrorContains != "" {
				assert.Contains(t, response.Body.String(), test.expectedErrorContains)
			}
		})
	}

}

func TestReplicationStatusAPI(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// GET replication status for non-existent replication ID
	response := rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/replication1", "")
	rest.RequireStatus(t, response, http.StatusNotFound)

	replicationConfig := db.ReplicationConfig{
		ID:                 "replication1",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}

	// PUT replication1
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", rest.MarshalConfig(t, replicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	// GET replication status for replication1
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/replication1", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var statusResponse db.ReplicationStatus
	err := json.Unmarshal(response.BodyBytes(), &statusResponse)
	require.NoError(t, err)
	assert.Equal(t, "replication1", statusResponse.ID)
	assert.True(t, statusResponse.Config == nil)

	// PUT replication2
	replication2Config := db.ReplicationConfig{
		ID:                 "replication2",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication2", rest.MarshalConfig(t, replication2Config))
	rest.RequireStatus(t, response, http.StatusCreated)

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &allStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(allStatusResponse), 2)
	assert.True(t, allStatusResponse[0].Config == nil)
	assert.True(t, allStatusResponse[1].Config == nil)

	// PUT replication status, no action
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	// PUT replication status with action
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)
}

func TestReplicationStatusStopAdhoc(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// GET replication status for non-existent replication ID
	response := rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/replication1", "")
	rest.RequireStatus(t, response, http.StatusNotFound)

	permanentReplicationConfig := db.ReplicationConfig{
		ID:                 "replication1",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		Continuous:         true,
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}

	adhocReplicationConfig := db.ReplicationConfig{
		ID:                 "replication2",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		Continuous:         true,
		Adhoc:              true,
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}

	// PUT non-adhoc replication
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", rest.MarshalConfig(t, permanentReplicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	// PUT adhoc replication
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication2", rest.MarshalConfig(t, adhocReplicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	err := json.Unmarshal(response.BodyBytes(), &allStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(allStatusResponse), 2)
	t.Logf("All status response: %v", allStatusResponse)

	// PUT _replicationStatus to stop non-adhoc replication
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var stopResponse *db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &stopResponse)
	require.NoError(t, err)
	assert.True(t, stopResponse.Status == "stopping" || stopResponse.Status == "stopped")

	// PUT _replicationStatus to stop adhoc replication
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication2?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)

	var stopAdhocResponse *db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &stopAdhocResponse)
	require.NoError(t, err)
	assert.True(t, stopAdhocResponse.Status == "removed")

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var updatedStatusResponse []*db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &updatedStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(updatedStatusResponse), 1)
	assert.Equal(t, "replication1", updatedStatusResponse[0].ID)
}

func TestReplicationStatusAPIIncludeConfig(t *testing.T) {

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// GET replication status for non-existent replication ID
	response := rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/replication1?includeConfig=true", "")
	rest.RequireStatus(t, response, http.StatusNotFound)

	replicationConfig := db.ReplicationConfig{
		ID:                 "replication1",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}

	// PUT replication1
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", rest.MarshalConfig(t, replicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	// GET replication status for replication1
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/replication1?includeConfig=true", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var statusResponse db.ReplicationStatus
	err := json.Unmarshal(response.BodyBytes(), &statusResponse)
	require.NoError(t, err)
	assert.Equal(t, "replication1", statusResponse.ID)
	assert.True(t, statusResponse.Config != nil)

	// PUT replication2
	replication2Config := db.ReplicationConfig{
		ID:                 "replication2",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication2", rest.MarshalConfig(t, replication2Config))
	rest.RequireStatus(t, response, http.StatusCreated)

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/?includeConfig=true", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &allStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(allStatusResponse), 2)
	assert.True(t, allStatusResponse[0].Config != nil)
	assert.True(t, allStatusResponse[1].Config != nil)

	// PUT replication status, no action
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1", "")
	rest.RequireStatus(t, response, http.StatusBadRequest)

	// PUT replication status with action
	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)

}

// Upserts replications via config, validates using _replication response
func TestReplicationsFromConfig(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for some config properties")
	}
	replicationConfig1String := `{
		"replication_id": "replication1",
		"remote": "http://remote:4985/db",
		"direction":"pull",
		"continuous":true,
		"conflict_resolution_type":"` + string(db.ConflictResolverCustom) + `",
		"custom_conflict_resolver":"func()",
		"purge_on_removal":true,
		"delta_sync_enabled":true,
		"max_backoff":100,
		"state":"stopped",
		"filter":"` + base.ByChannelFilter + `",
		"query_params":["ABC"],
		"cancel":false,
		"collections_enabled": ` + strconv.FormatBool(!base.TestsUseNamedCollections()) + `
	}`
	replicationConfig2String := `{
		"replication_id": "replication2",
		"remote": "http://remote:4985/db",
		"direction":"pull",
		"continuous":true,
		"conflict_resolution_type":"` + string(db.ConflictResolverCustom) + `",
		"custom_conflict_resolver":"func()",
		"purge_on_removal":true,
		"delta_sync_enabled":true,
		"max_backoff":100,
		"state":"stopped",
		"filter":"` + base.ByChannelFilter + `",
		"query_params":["ABC"],
		"cancel":false,
		"collections_enabled": ` + strconv.FormatBool(!base.TestsUseNamedCollections()) + `
	}`

	replicationConfig1 := &db.ReplicationConfig{}
	err := base.JSONUnmarshal([]byte(replicationConfig1String), replicationConfig1)
	require.NoError(t, err)
	replicationConfig2 := &db.ReplicationConfig{}
	err = base.JSONUnmarshal([]byte(replicationConfig2String), replicationConfig2)
	require.NoError(t, err)

	testCases := []struct {
		name           string
		replicationSet []*db.ReplicationConfig
	}{
		{
			name:           "Single replication",
			replicationSet: []*db.ReplicationConfig{replicationConfig1},
		},
		{
			name:           "Multiple replications",
			replicationSet: []*db.ReplicationConfig{replicationConfig1, replicationConfig2},
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			dbConfig := &rest.DatabaseConfig{}
			dbConfig.Replications = make(map[string]*db.ReplicationConfig)
			for _, rc := range test.replicationSet {
				dbConfig.Replications[rc.ID] = rc
			}

			rt := rest.NewRestTester(t,
				&rest.RestTesterConfig{DatabaseConfig: dbConfig})
			defer rt.Close()

			// Retrieve replications
			response := rt.SendAdminRequest("GET", "/{{.db}}/_replication/", "")
			rest.RequireStatus(t, response, http.StatusOK)
			var configResponse map[string]*db.ReplicationConfig
			err := json.Unmarshal(response.BodyBytes(), &configResponse)
			require.NoError(t, err)
			assert.Equal(t, len(test.replicationSet), len(configResponse))
			for _, replication := range test.replicationSet {
				loadedReplication, ok := configResponse[replication.ID]
				assert.True(t, ok)
				equals, equalsErr := loadedReplication.Equals(replication)
				assert.True(t, equals)
				assert.NoError(t, equalsErr)
			}
		})
	}

}

// - Starts 2 RestTesters, one active, and one passive.
// - Creates documents on rt1.
// - Creates a continuous push replication on rt1 via the REST API
// - Validates documents are replicated to rt2
func TestPushReplicationAPI(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)

	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create doc1 on rt1
		docID1 := rest.SafeDocumentName(t, t.Name()+"rt1doc")
		_ = rt1.PutDoc(docID1, `{"source":"rt1","channels":["alice"]}`)

		// Create push replication, verify running
		replicationID := rest.SafeDocumentName(t, t.Name())
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		// wait for document originally written to rt1 to arrive at rt2
		changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID1, changesResults.Results[0].ID)

		// Validate doc1 contents on remote
		doc1Body := rt2.GetDocBody(docID1)
		assert.Equal(t, "rt1", doc1Body["source"])

		// Create doc2 on rt1
		docID2 := rest.SafeDocumentName(t, t.Name()+"rt1doc2")
		_ = rt2.PutDoc(docID2, `{"source":"rt1","channels":["alice"]}`)

		// wait for doc2 to arrive at rt2
		changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		assert.Equal(t, docID2, changesResults.Results[0].ID)

		// Validate doc2 contents
		doc2Body := rt2.GetDocBody(docID2)
		assert.Equal(t, "rt1", doc2Body["source"])
	})
}

// TestPullReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt2.
//   - Creates a continuous pull replication on rt1 via the REST API
//   - Validates documents are replicated to rt1
func TestPullReplicationAPI(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create doc1 on rt2
		docID1 := rest.SafeDocumentName(t, t.Name()+"rt2doc")
		_ = rt2.PutDoc(docID1, `{"source":"rt2","channels":["alice"]}`)

		// Create pull replication, verify running
		replicationID := rest.SafeDocumentName(t, t.Name())
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		// wait for document originally written to rt2 to arrive at rt1
		changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		changesResults.RequireDocIDs(t, []string{docID1})

		// Validate doc1 contents
		doc1Body := rt1.GetDocBody(docID1)
		assert.Equal(t, "rt2", doc1Body["source"])

		// Create doc2 on rt2
		docID2 := rest.SafeDocumentName(t, t.Name()+"rt2doc2")
		_ = rt2.PutDoc(docID2, `{"source":"rt2","channels":["alice"]}`)

		// wait for new document to arrive at rt1
		changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		changesResults.RequireDocIDs(t, []string{docID2})

		// Validate doc2 contents
		doc2Body := rt1.GetDocBody(docID2)
		assert.Equal(t, "rt2", doc2Body["source"])
	})
}

func TestStopServerlessConnectionLimitingDuringReplications(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		resp := rt2.SendAdminRequest(http.MethodPut, "/_config", `{"max_concurrent_replications" : 2}`)
		rest.RequireStatus(t, resp, http.StatusOK)

		for i := range 10 {
			_ = rt2.PutDoc(fmt.Sprint(i), `{"source":"rt2","channels":["alice"]}`)
		}

		// create two replications to take us to the limit
		replicationID := rest.SafeDocumentName(t, t.Name())
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
		replicationID = rest.SafeDocumentName(t, t.Name()+"1")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
		rt1.WaitForActiveReplicatorInitialization(2)

		// try create a new replication to take it beyond the threshold set by runtime config call
		// assert it enter error state
		replicationID = rest.SafeDocumentName(t, t.Name()+"2")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateReconnecting)

		// change limit to 0 (turning limiting off) and assert that the replications currently running continue as normal and reject any new ones being added
		resp = rt2.SendAdminRequest(http.MethodPut, "/_config", `{"max_concurrent_replications" : 0}`)
		rest.RequireStatus(t, resp, http.StatusOK)

		// assert the replications aren't killed as result of change in limit
		rt2.WaitForActiveReplicatorCount(2)
		// assert we still can create a new replication given that originally the limit was 2 replications
		replicationID = rest.SafeDocumentName(t, t.Name()+"3")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
	})
}

func TestServerlessConnectionLimitingOneshotFeed(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// update runtime config to limit to 2 concurrent replication connections
		resp := rt2.SendAdminRequest(http.MethodPut, "/_config", `{"max_concurrent_replications" : 2}`)
		rest.RequireStatus(t, resp, http.StatusOK)

		for i := range 200 {
			_ = rt2.PutDoc(fmt.Sprint(i), `{"source":"rt2","channels":["alice"]}`)
		}

		replicationID := rest.SafeDocumentName(t, t.Name())
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, false, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
		replicationID = rest.SafeDocumentName(t, t.Name()+"1")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, false, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		rt1.WaitForActiveReplicatorInitialization(2)
		// assert the active replicator count has increased by 2
		rt2.WaitForActiveReplicatorCount(2)
		replicationID = rest.SafeDocumentName(t, t.Name())
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)
		replicationID = rest.SafeDocumentName(t, t.Name()+"1")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

		// assert that the count for active replicators has decreased by 2 as both replications have finished
		rt2.WaitForActiveReplicatorCount(0)

		// assert we can create a new replication as count has decreased below threshold
		replicationID = rest.SafeDocumentName(t, t.Name()+"2")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, false, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
	})
}

func TestServerlessConnectionLimitingContinuous(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// update runtime config to limit to 2 concurrent replication connections
		resp := rt2.SendAdminRequest(http.MethodPut, "/_config", `{"max_concurrent_replications" : 2}`)
		rest.RequireStatus(t, resp, http.StatusOK)

		for i := range 200 {
			_ = rt2.PutDoc(fmt.Sprint(i), `{"source":"rt2","channels":["alice"]}`)
		}

		// create two replications to take us to the limit
		replicationID := rest.SafeDocumentName(t, t.Name())
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
		replicationID = rest.SafeDocumentName(t, t.Name()+"1")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)
		rt1.WaitForActiveReplicatorInitialization(2)

		// try create a new replication to take it beyond the threshold set by runtime config call
		// assert it enter error state
		replicationID = rest.SafeDocumentName(t, t.Name()+"2")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateReconnecting)

		// change limit to 1 and assert that the replications currently running continue as normal and reject any new ones being added
		resp = rt2.SendAdminRequest(http.MethodPut, "/_config", `{"max_concurrent_replications" : 1}`)
		rest.RequireStatus(t, resp, http.StatusOK)

		// assert the replications aren't killed as result of change in limit
		rt2.WaitForActiveReplicatorCount(2)
		// assert we still can't create a new replication
		replicationID = rest.SafeDocumentName(t, t.Name()+"3")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateReconnecting)

		// stop one of the replicators currently running
		replicationID = rest.SafeDocumentName(t, t.Name()+"1")
		resp = rt1.SendAdminRequest(http.MethodPut, "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		rt1.WaitForReplicationStatus(rest.SafeDocumentName(t, t.Name()+"1"), db.ReplicationStateStopped)
		// assert the count has been decremented
		rt2.WaitForActiveReplicatorCount(1)

		// assert we still can't create new replication (new limit is 1)
		replicationID = rest.SafeDocumentName(t, t.Name()+"4")
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateReconnecting)
	})
}

// TestPullReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a continuous pull replication on rt1 via the REST API
//   - Validates stop/start/reset actions on the replicationStatus endpoint
func TestReplicationStatusActions(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		// Increase checkpoint persistence frequency for cross-node status verification
		reduceCheckpointInterval := reduceTestCheckpointInterval(50 * time.Millisecond)
		t.Cleanup(reduceCheckpointInterval)
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create doc1 on rt2
		docID1 := rest.SafeDocumentName(t, t.Name()+"rt2doc")
		_ = rt2.PutDoc(docID1, `{"source":"rt2","channels":["alice"]}`)

		// Create pull replication, verify running
		replicationID := rest.SafeDocumentName(t, t.Name())
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		// Start goroutine to continuously poll for status of replication on rt1 to detect race conditions
		doneChan := make(chan struct{})
		var statusWg sync.WaitGroup
		statusWg.Add(1)
		go func() {
			for {
				select {
				case <-doneChan:
					statusWg.Done()
					return
				default:
				}
				_ = rt1.GetReplicationStatus(replicationID)
			}
		}()

		// wait for document originally written to rt2 to arrive at rt1
		changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		changesResults.RequireDocIDs(t, []string{docID1})

		// Validate doc1 contents
		doc1Body := rt1.GetDocBody(docID1)
		assert.Equal(t, "rt2", doc1Body["source"])

		// Create doc2 on rt2
		docID2 := rest.SafeDocumentName(t, t.Name()+"rt2doc2")
		_ = rt2.PutDoc(docID2, `{"source":"rt2","channels":["alice"]}`)

		// wait for new document to arrive at rt1
		changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		changesResults.RequireDocIDs(t, []string{docID2})

		// Validate doc2 contents
		doc2Body := rt1.GetDocBody(docID2)
		assert.Equal(t, "rt2", doc2Body["source"])

		// Stop replication
		response := rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
		rest.RequireStatus(t, response, http.StatusOK)

		// Wait for stopped.  Non-instant as config change needs to arrive over DCP
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

		// Reset replication
		response = rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=reset", "")
		rest.RequireStatus(t, response, http.StatusOK)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status := rt1.GetReplicationStatus(replicationID)
			assert.Equal(c, db.ReplicationStateStopped, status.Status)
			assert.Equal(c, "", status.LastSeqPull)
		}, 10*time.Second, 100*time.Millisecond)

		// Restart the replication
		response = rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=start", "")
		rest.RequireStatus(t, response, http.StatusOK)

		// Verify replication has restarted from zero. Since docs have already been replicated,
		// expect no docs read, two docs checked.
		statError := rt1.WaitForCondition(func() bool {
			status := rt1.GetReplicationStatus(replicationID)
			return status.DocsCheckedPull == 2 && status.DocsRead == 0
		})
		assert.NoError(t, statError)

		// Terminate status goroutine
		close(doneChan)
		statusWg.Wait()
	})
}

// TestReplicationRebalanceToZeroNodes checks that the replication goes into an unassigned state when there are no nodes available to run replications.
func TestReplicationRebalanceToZeroNodes(t *testing.T) {
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, remoteRT, _ := sgrRunner.SetupSGRPeers(t)

		// Build connection string for active RT
		srv := httptest.NewServer(activeRT.TestPublicHandler())
		activeDBURL, _ := url.Parse(srv.URL + "/" + activeRT.GetDatabase().Name)
		activeDBURL.User = url.UserPassword("alice", rest.RestTesterDefaultUserPassword)
		defer srv.Close()

		// Put replication on remote RT where sg replicate is off, so will not get assigned a node
		replicationID := rest.SafeDocumentName(t, t.Name())
		remoteRT.CreateReplication(replicationID, activeDBURL.String(), db.ActiveReplicatorTypePush, nil, false, db.ConflictResolverDefault, "")

		remoteRT.WaitForAssignedReplications(0)

		// assert that the replication state is error after the replication is failed to be assigned a node
		remoteRT.WaitForReplicationStatus(replicationID, db.ReplicationStateUnassigned)
	})
}

// TestReplicationRebalancePull
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt1 in two channels
//   - Creates two continuous pull replications on rt1 via the REST API
//   - adds another active node
//   - Creates more documents, validates they are replicated
func TestReplicationRebalancePull(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only (replication rebalance)")
	}
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		// Increase checkpoint persistence frequency for cross-node status verification
		reduceCheckpointInterval := reduceTestCheckpointInterval(50 * time.Millisecond)
		t.Cleanup(reduceCheckpointInterval)
		activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create docs on remote
		docABC1 := rest.SafeDocumentName(t, t.Name()+"ABC1")
		docDEF1 := rest.SafeDocumentName(t, t.Name()+"DEF1")
		_ = remoteRT.PutDoc(docABC1, `{"source":"remoteRT","channels":["ABC"]}`)
		_ = remoteRT.PutDoc(docDEF1, `{"source":"remoteRT","channels":["DEF"]}`)

		// Create pull replications, verify running
		activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePull, []string{"ABC"}, true, db.ConflictResolverDefault, "")
		activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePull, []string{"DEF"}, true, db.ConflictResolverDefault, "")
		activeRT.WaitForAssignedReplications(2)
		activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
		activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

		// wait for documents originally written to remoteRT to arrive at activeRT
		changesResults := activeRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
		changesResults.RequireDocIDs(t, []string{docABC1, docDEF1})

		// Validate doc contents
		docABC1Body := activeRT.GetDocBody(docABC1)
		assert.Equal(t, "remoteRT", docABC1Body["source"])
		docDEF1Body := activeRT.GetDocBody(docDEF1)
		assert.Equal(t, "remoteRT", docDEF1Body["source"])

		// Add another node to the active cluster
		activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
		defer activeRT2.Close()

		// Wait for replication to be rebalanced to activeRT2
		activeRT.WaitForAssignedReplications(1)
		activeRT2.WaitForAssignedReplications(1)

		t.Logf("==============replication rebalance is done================")

		// Create additional docs on remoteRT
		docABC2 := rest.SafeDocumentName(t, t.Name()+"ABC2")
		_ = remoteRT.PutDoc(docABC2, `{"source":"remoteRT","channels":["ABC"]}`)
		docDEF2 := rest.SafeDocumentName(t, t.Name()+"DEF2")
		_ = remoteRT.PutDoc(docDEF2, `{"source":"remoteRT","channels":["DEF"]}`)

		// wait for new documents to arrive at activeRT
		changesResults = activeRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		changesResults.RequireDocIDs(t, []string{docABC2, docDEF2})

		// Validate doc contents
		docABC2Body := activeRT.GetDocBody(docABC2)
		assert.Equal(t, "remoteRT", docABC2Body["source"])
		docDEF2Body := activeRT.GetDocBody(docDEF2)
		assert.Equal(t, "remoteRT", docDEF2Body["source"])
		docABC2Body2 := activeRT2.GetDocBody(docABC2)
		assert.Equal(t, "remoteRT", docABC2Body2["source"])
		docDEF2Body2 := activeRT2.GetDocBody(docDEF2)
		assert.Equal(t, "remoteRT", docDEF2Body2["source"])

		// Validate replication stats across rebalance, on both active nodes
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT.GetReplicationStatus("rep_ABC").DocsRead
			t.Logf("activeRT rep_ABC DocsRead: %d", actual)
			return actual == 2
		})
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT.GetReplicationStatus("rep_DEF").DocsRead
			t.Logf("activeRT rep_DEF DocsRead: %d", actual)
			return actual == 2
		})
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT2.GetReplicationStatus("rep_ABC").DocsRead
			t.Logf("activeRT2 rep_ABC DocsRead: %d", actual)
			return actual == 2
		})
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT2.GetReplicationStatus("rep_DEF").DocsRead
			t.Logf("activeRT2 rep_DEF DocsRead: %d", actual)
			return actual == 2
		})

		// explicitly stop the SGReplicateMgrs on the active nodes, to prevent a node rebalance during test teardown.
		activeRT.GetDatabase().SGReplicateMgr.Stop()
		activeRT.GetDatabase().SGReplicateMgr = nil
		activeRT2.GetDatabase().SGReplicateMgr.Stop()
		activeRT2.GetDatabase().SGReplicateMgr = nil
	})
}

// TestReplicationRebalancePush
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt1 in two channels
//   - Creates two continuous pull replications on rt1 via the REST API
//   - adds another active node
//   - Creates more documents, validates they are replicated
func TestReplicationRebalancePush(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only (replication rebalance)")
	}

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		// Increase checkpoint persistence frequency for cross-node status verification
		reduceCheckpointInterval := reduceTestCheckpointInterval(50 * time.Millisecond)
		t.Cleanup(reduceCheckpointInterval)
		activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create docs on active
		docABC1 := rest.SafeDocumentName(t, t.Name()+"ABC1")
		docDEF1 := rest.SafeDocumentName(t, t.Name()+"DEF1")
		_ = activeRT.PutDoc(docABC1, `{"source":"activeRT","channels":["ABC"]}`)
		_ = activeRT.PutDoc(docDEF1, `{"source":"activeRT","channels":["DEF"]}`)

		// This seems to fix the flaking. Wait until the change-cache has caught up with the latest writes to the database.
		activeRT.WaitForPendingChanges()

		// Create push replications, verify running
		activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePush, []string{"ABC"}, true, db.ConflictResolverDefault, "")
		activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePush, []string{"DEF"}, true, db.ConflictResolverDefault, "")
		activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
		activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

		// wait for documents to be pushed to remote
		changesResults := remoteRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
		changesResults.RequireDocIDs(t, []string{docABC1, docDEF1})

		// Validate doc contents
		docABC1Body := remoteRT.GetDocBody(docABC1)
		assert.Equal(t, "activeRT", docABC1Body["source"])
		docDEF1Body := remoteRT.GetDocBody(docDEF1)
		assert.Equal(t, "activeRT", docDEF1Body["source"])

		// Add another node to the active cluster
		activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
		defer activeRT2.Close()

		// Wait for replication to be rebalanced to activeRT2
		activeRT.WaitForAssignedReplications(1)
		activeRT2.WaitForAssignedReplications(1)

		// Create additional docs on local
		docABC2 := rest.SafeDocumentName(t, t.Name()+"ABC2")
		_ = activeRT.PutDoc(docABC2, `{"source":"activeRT","channels":["ABC"]}`)
		docDEF2 := rest.SafeDocumentName(t, t.Name()+"DEF2")
		_ = activeRT.PutDoc(docDEF2, `{"source":"activeRT","channels":["DEF"]}`)

		// wait for new documents to arrive at remote
		changesResults = remoteRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		changesResults.RequireDocIDs(t, []string{docABC2, docDEF2})

		// Validate doc contents
		docABC2Body := remoteRT.GetDocBody(docABC2)
		assert.Equal(t, "activeRT", docABC2Body["source"])
		docDEF2Body := remoteRT.GetDocBody(docDEF2)
		assert.Equal(t, "activeRT", docDEF2Body["source"])

		// Validate replication stats across rebalance, on both active nodes
		// Checking DocsCheckedPush here, as DocsWritten isn't necessarily going to be 2, due to a
		// potential for race updating status during replication rebalance:
		//     1. active node 1 writes document 1 to passive
		//     2. replication is rebalanced prior to checkpoint being persisted
		//     3. active node 2 is assigned replication, starts from zero (since checkpoint wasn't persisted)
		//     4. active node 2 attempts to write document 1, passive already has it.  DocsCheckedPush is incremented, but not DocsWritten
		// Note that we can't wait for checkpoint persistence prior to rebalance, as the node initiating the rebalance
		// isn't necessarily the one running the replication.
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT.GetReplicationStatus("rep_ABC").DocsCheckedPush
			t.Logf("activeRT rep_ABC DocsCheckedPush: %d", actual)
			return actual == 2
		})
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT.GetReplicationStatus("rep_DEF").DocsCheckedPush
			t.Logf("activeRT rep_DEF DocsCheckedPush: %d", actual)
			return actual == 2
		})
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT2.GetReplicationStatus("rep_ABC").DocsCheckedPush
			t.Logf("activeRT2 rep_ABC DocsCheckedPush: %d", actual)
			return actual == 2
		})
		rest.WaitAndAssertCondition(t, func() bool {
			actual := activeRT2.GetReplicationStatus("rep_DEF").DocsCheckedPush
			t.Logf("activeRT2 rep_DEF DocsCheckedPush: %d", actual)
			return actual == 2
		})

		// explicitly stop the SGReplicateMgrs on the active nodes, to prevent a node rebalance during test teardown.
		activeRT.GetDatabase().SGReplicateMgr.Stop()
		activeRT.GetDatabase().SGReplicateMgr = nil
		activeRT2.GetDatabase().SGReplicateMgr.Stop()
		activeRT2.GetDatabase().SGReplicateMgr = nil
	})
}

// TestPullReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt2.
//   - Creates a one-shot pull replication on rt1 via the REST API
//   - Validates documents are replicated to rt1
//   - Validates replication status count when replication is local and non-local
func TestPullOneshotReplicationAPI(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create 20 docs on rt2
		docCount := 20
		docIDs := make([]string, 20)
		prefix := rest.SafeDocumentName(t, t.Name())
		for i := range 20 {
			docID := fmt.Sprintf("%s%s%d", prefix, "rt2doc", i)
			_ = remoteRT.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
			docIDs[i] = docID
		}

		remoteRT.WaitForPendingChanges()

		// Create oneshot replication, verify running
		replicationID := rest.SafeDocumentName(t, t.Name())
		activeRT.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, false, db.ConflictResolverDefault, "")
		activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		// wait for documents originally written to rt2 to arrive at rt1
		changesResults := activeRT.WaitForChanges(docCount, "/{{.keyspace}}/_changes?since=0", "", true)
		changesResults.RequireDocIDs(t, docIDs)

		// Validate sample doc contents
		doc1Body := activeRT.GetDocBody(docIDs[0])
		assert.Equal(t, "rt2", doc1Body["source"])

		// Wait for replication to stop
		activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

		// Validate docs read from active
		status := activeRT.GetReplicationStatus(replicationID)
		assert.Equal(t, int64(docCount), status.DocsRead)

		// Add another node to the active cluster
		activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
		defer activeRT2.Close()

		// Get replication status for non-local replication
		remoteStatus := activeRT2.GetReplicationStatus(replicationID)
		assert.Equal(t, int64(docCount), remoteStatus.DocsRead)
	})
}

// TestReplicationConcurrentPush
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates two continuous push replications on rt1 via the REST API for two channels
//   - Write documents to rt1 belonging to both channels
//   - Write documents to rt1, each belonging to one of the channels (verifies replications are still running)
//   - Validate replications do not report errors, all docs are replicated
//
// Note: This test intermittently reproduced CBG-998 under -race when a 1s sleep was added post-callback to
//
//	WriteUpdateWithXattr.  Have been unable to reproduce the same with a leaky bucket UpdateCallback.
func TestReplicationConcurrentPush(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)
		// Create push replications, verify running, also verify active replicators are created
		activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePush, []string{"ABC"}, true, db.ConflictResolverDefault, "")
		activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePush, []string{"DEF"}, true, db.ConflictResolverDefault, "")
		activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
		activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)
		activeRT.WaitForActiveReplicatorInitialization(2)

		// Create docs on active
		docAllChannels1 := rest.SafeDocumentName(t, t.Name()+"All1")
		docAllChannels2 := rest.SafeDocumentName(t, t.Name()+"All2")
		_ = activeRT.PutDoc(docAllChannels1, `{"source":"activeRT1","channels":["ABC","DEF"]}`)
		_ = activeRT.PutDoc(docAllChannels2, `{"source":"activeRT2","channels":["ABC","DEF"]}`)

		// wait for documents to be pushed to remote
		changesResults := remoteRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
		changesResults.RequireDocIDs(t, []string{docAllChannels1, docAllChannels2})

		// wait for both replications to have pushed, and total pushed to equal 2
		assert.NoError(t, activeRT.WaitForCondition(func() bool {
			abcStatus := activeRT.GetReplicationStatus("rep_ABC")
			if abcStatus.DocsCheckedPush != 2 {
				t.Logf("abcStatus.DocsCheckedPush not 2, is %v", abcStatus.DocsCheckedPush)
				t.Logf("abcStatus=%+v", abcStatus)
				return false
			}
			defStatus := activeRT.GetReplicationStatus("rep_DEF")
			if defStatus.DocsCheckedPush != 2 {
				t.Logf("defStatus.DocsCheckedPush not 2, is %v", defStatus.DocsCheckedPush)
				t.Logf("defStatus=%+v", defStatus)
				return false
			}

			// DocsWritten is incremented on a successful write, but ALSO in the race scenario where the remote responds
			// to the changes message to say it needs the rev, but then receives the rev from another source. This means that
			// in this test, DocsWritten can be any value between 0 and 2 for each replication, but should be at least 2
			// for both replications
			totalDocsWritten := abcStatus.DocsWritten + defStatus.DocsWritten
			if totalDocsWritten < 2 || totalDocsWritten > 4 {
				t.Logf("Total docs written is not between 2 and 4, is abc=%v, def=%v", abcStatus.DocsWritten, defStatus.DocsWritten)
				return false
			}
			return true
		}))

		// Validate doc contents
		docAll1Body := remoteRT.GetDocBody(docAllChannels1)
		assert.Equal(t, "activeRT1", docAll1Body["source"])
		docAll2Body := remoteRT.GetDocBody(docAllChannels2)
		assert.Equal(t, "activeRT2", docAll2Body["source"])
	})

}
func TestReplicationAPIWithAuthCredentials(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create replication with explicitly defined auth credentials in replication config
	replication1Config := db.ReplicationConfig{
		ID:                 "replication1",
		Remote:             "http://remote:4984/db",
		RemoteUsername:     "alice",
		RemotePassword:     "pass",
		Direction:          db.ActiveReplicatorTypePull,
		Adhoc:              true,
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", rest.MarshalConfig(t, replication1Config))
	rest.RequireStatus(t, response, http.StatusCreated)

	// Check whether auth are credentials redacted from replication response
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var configResponse db.ReplicationConfig
	err := json.Unmarshal(response.BodyBytes(), &configResponse)
	require.NoError(t, err, "Error un-marshalling replication response")

	// Assert actual replication config response against expected
	checkReplicationConfig := func(expected, actual *db.ReplicationConfig) {
		require.NotNil(t, actual, "Actual replication config is not available")
		assert.Equal(t, expected.ID, actual.ID, "Replication ID mismatch")
		assert.Equal(t, expected.Adhoc, actual.Adhoc, "Replication type mismatch")
		assert.Equal(t, expected.Direction, actual.Direction, "Replication direction mismatch")
		assert.Equal(t, expected.Remote, actual.Remote, "Couldn't redact auth credentials")
		assert.Equal(t, expected.RemoteUsername, actual.RemoteUsername, "Couldn't redact username")
		assert.Equal(t, expected.RemotePassword, actual.RemotePassword, "Couldn't redact password")
	}
	replication1Config.RemotePassword = base.RedactedStr
	checkReplicationConfig(&replication1Config, &configResponse)

	// Create another replication with auth credentials defined in Remote URL
	replication2Config := db.ReplicationConfig{
		ID:        "replication2",
		Remote:    "http://bob:pass@remote:4984/db",
		Direction: db.ActiveReplicatorTypePull,
		Adhoc:     true,
	}
	response = rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_replication/", rest.MarshalConfig(t, replication2Config))
	rest.RequireStatus(t, response, http.StatusCreated)

	// Check whether auth are credentials redacted from replication response
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication2", "")
	rest.RequireStatus(t, response, http.StatusOK)
	configResponse = db.ReplicationConfig{}
	err = json.Unmarshal(response.BodyBytes(), &configResponse)
	require.NoError(t, err, "Error un-marshalling replication response")
	replication2Config.Remote = "http://bob:xxxxx@remote:4984/db"

	// Check whether auth are credentials redacted from all replications response
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/", "")
	rest.RequireStatus(t, response, http.StatusOK)
	t.Logf("response: %s", response.BodyBytes())

	var replicationsResponse map[string]db.ReplicationConfig
	err = json.Unmarshal(response.BodyBytes(), &replicationsResponse)
	require.NoError(t, err, "Error un-marshalling replication response")
	assert.Lenf(t, replicationsResponse, 2, "Replication count mismatch")

	replication1, ok := replicationsResponse[replication1Config.ID]
	assert.True(t, ok, "Error getting replication")
	checkReplicationConfig(&replication1Config, &replication1)

	replication2, ok := replicationsResponse[replication2Config.ID]
	assert.True(t, ok, "Error getting replication")
	checkReplicationConfig(&replication2Config, &replication2)

	// Check whether auth are credentials redacted replication status for all replications
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replicationStatus/?includeConfig=true", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	require.NoError(t, json.Unmarshal(response.BodyBytes(), &allStatusResponse))
	require.Lenf(t, allStatusResponse, 2, "Replication count mismatch")

	// Sort replications by replication ID before assertion
	sort.Slice(allStatusResponse, func(i, j int) bool {
		return allStatusResponse[i].Config.ID < allStatusResponse[j].Config.ID
	})
	checkReplicationConfig(&replication1Config, allStatusResponse[0].Config)
	checkReplicationConfig(&replication2Config, allStatusResponse[1].Config)

	// Delete both replications
	response = rt.SendAdminRequest(http.MethodDelete, "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodDelete, "/{{.db}}/_replication/replication2", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Verify deletes were successful
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusNotFound)
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication2", "")
	rest.RequireStatus(t, response, http.StatusNotFound)
}

func TestValidateReplication(t *testing.T) {
	testCases := []struct {
		name              string
		replicationConfig db.ReplicationConfig
		fromConfig        bool
		expectedErrorMsg  string
		eeOnly            bool
	}{
		{
			name: "replication config unsupported Adhoc option",
			replicationConfig: db.ReplicationConfig{
				Adhoc: true,
			},
			fromConfig:       true,
			expectedErrorMsg: db.ConfigErrorConfigBasedAdhoc,
		},
		{
			name: "replication config with no remote URL specified",
			replicationConfig: db.ReplicationConfig{
				Remote: "",
			},
			expectedErrorMsg: db.ConfigErrorMissingRemote,
		},
		{
			name: "auth credentials specified in both replication config and remote URL",
			replicationConfig: db.ReplicationConfig{
				Remote:         "http://bob:pass@remote:4984/db",
				RemoteUsername: "alice",
				RemotePassword: "pass",
			},
			expectedErrorMsg: db.ConfigErrorDuplicateCredentials,
		},
		{
			name: "auth credentials specified in replication config",
			replicationConfig: db.ReplicationConfig{
				Remote:         "http://remote:4984/db",
				RemoteUsername: "alice",
				RemotePassword: "pass",
				Filter:         base.ByChannelFilter,
				QueryParams:    map[string]any{"channels": []any{"E", "A", "D", "G", "B", "e"}},
				Direction:      db.ActiveReplicatorTypePull,
			},
		},
		{
			name: "auth credentials specified in remote URL",
			replicationConfig: db.ReplicationConfig{
				Remote:      "http://bob:pass@remote:4984/db",
				Filter:      base.ByChannelFilter,
				QueryParams: map[string]any{"channels": []any{"E", "A", "D", "G", "B", "e"}},
				Direction:   db.ActiveReplicatorTypePull,
			},
		},
		{
			name: "replication config with no direction",
			replicationConfig: db.ReplicationConfig{
				Remote: "http://bob:pass@remote:4984/db",
			},
			expectedErrorMsg: db.ConfigErrorMissingDirection,
		},
		{
			name: "replication config with invalid direction",
			replicationConfig: db.ReplicationConfig{
				Remote:    "http://bob:pass@remote:4984/db",
				Direction: "UpAndDown",
			},
			expectedErrorMsg: fmt.Sprintf(db.ConfigErrorInvalidDirectionFmt,
				"UpAndDown", db.ActiveReplicatorTypePush, db.ActiveReplicatorTypePull, db.ActiveReplicatorTypePushAndPull),
		},
		{
			name: "replication config with unknown filter",
			replicationConfig: db.ReplicationConfig{
				Remote:      "http://bob:pass@remote:4984/db",
				QueryParams: map[string]any{"channels": []any{"E", "A", "D", "G", "B", "e"}},
				Direction:   db.ActiveReplicatorTypePull,
				Filter:      "unknownFilter",
			},
			expectedErrorMsg: db.ConfigErrorUnknownFilter,
		},
		{
			name: "replication config with channel filter but no query params",
			replicationConfig: db.ReplicationConfig{
				Remote:    "http://bob:pass@remote:4984/db",
				Filter:    base.ByChannelFilter,
				Direction: db.ActiveReplicatorTypePull,
			},
			expectedErrorMsg: db.ConfigErrorMissingQueryParams,
		},
		{
			name: "replication config with channel filter and invalid query params",
			replicationConfig: db.ReplicationConfig{
				Remote:      "http://bob:pass@remote:4984/db",
				Filter:      base.ByChannelFilter,
				QueryParams: []string{"E", "A", "D", "G", "B", "e"},
				Direction:   db.ActiveReplicatorTypePull,
			},
			expectedErrorMsg: db.ConfigErrorBadChannelsArray,
		},
		{
			name: "replication config replicationID too long",
			replicationConfig: db.ReplicationConfig{
				ID: "0123456789012345678901234567890123456789012345678901234567890123456789" +
					"0123456789012345678901234567890123456789012345678901234567890123456789" +
					"0123456789012345678901234567890123456789012345678901234567890123456789",
				Remote: "http://bob:pass@remote:4984/db",
			},
			expectedErrorMsg: db.ConfigErrorIDTooLong,
		},
		{
			name: "custom conflict resolution without func",
			replicationConfig: db.ReplicationConfig{
				ID:                     "replication1",
				Remote:                 "http://remote:4984/db",
				Direction:              "pull",
				Adhoc:                  true,
				ConflictResolutionType: db.ConflictResolverCustom,
			},
			expectedErrorMsg: "Custom conflict resolution type has been set but no conflict resolution function has been defined",
			eeOnly:           true,
		},
		{
			name: "custom conflict resolution with func",
			replicationConfig: db.ReplicationConfig{
				ID:                     "replication2",
				Remote:                 "http://remote:4984/db",
				Direction:              "pull",
				Adhoc:                  true,
				ConflictResolutionType: db.ConflictResolverCustom,
				ConflictResolutionFn:   "func(){}",
			},
			eeOnly: true,
		},
		{
			name: "bad conflict resolution type",
			replicationConfig: db.ReplicationConfig{
				ID:                     "replication2",
				Remote:                 "http://remote:4984/db",
				Direction:              "pull",
				Adhoc:                  true,
				ConflictResolutionType: "random",
				ConflictResolutionFn:   "func(){}",
			},
			eeOnly: true,
			expectedErrorMsg: fmt.Sprintf(db.ConfigErrorInvalidConflictResolutionTypeFmt, db.ConflictResolverLocalWins,
				db.ConflictResolverRemoteWins, db.ConflictResolverDefault, db.ConflictResolverCustom),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.eeOnly && !base.IsEnterpriseEdition() {
				return
			}
			err := tc.replicationConfig.ValidateReplication(tc.fromConfig)
			if tc.expectedErrorMsg != "" {
				expectedError := &base.HTTPError{
					Status:  http.StatusBadRequest,
					Message: tc.expectedErrorMsg,
				}
				assert.Equal(t, expectedError, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}

}

func TestValidateReplicationWithInvalidURL(t *testing.T) {
	// Replication config with credentials in an invalid remote URL
	replicationConfig := db.ReplicationConfig{Remote: "http://user:foo{bar=pass@remote:4984/db"}
	err := replicationConfig.ValidateReplication(false)
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
	assert.NotContains(t, err.Error(), "user:foo{bar=pass")

	// Replication config with no credentials in an invalid remote URL
	replicationConfig = db.ReplicationConfig{Remote: "http://{unknown@remote:4984/db"}
	err = replicationConfig.ValidateReplication(false)
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
}

func TestGetStatusWithReplication(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Create a replication
	config1 := db.ReplicationConfig{
		ID:             "replication1",
		Remote:         "http://remote:4984/db",
		RemoteUsername: "alice",
		RemotePassword: "pass",
		Direction:      db.ActiveReplicatorTypePull,
		Adhoc:          true,
	}
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", rest.MarshalConfig(t, config1))
	rest.RequireStatus(t, response, http.StatusCreated)

	// Create another replication
	config2 := db.ReplicationConfig{
		ID:        "replication2",
		Remote:    "http://bob:pass@remote:4984/db",
		Direction: db.ActiveReplicatorTypePull,
		Adhoc:     true,
	}
	response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication2", rest.MarshalConfig(t, config2))
	rest.RequireStatus(t, response, http.StatusCreated)

	// Check _status response
	response = rt.SendAdminRequest(http.MethodGet, "/_status", "")
	rest.RequireStatus(t, response, http.StatusOK)
	var status rest.Status
	err := json.Unmarshal(response.BodyBytes(), &status)
	require.NoError(t, err, "Error un-marshalling replication response")
	database := status.Databases["db"]
	require.Lenf(t, database.ReplicationStatus, 2, "Replication count mismatch")

	// Sort replications by replication ID before asserting replication status
	sort.Slice(database.ReplicationStatus, func(i, j int) bool {
		return database.ReplicationStatus[i].ID < database.ReplicationStatus[j].ID
	})
	assert.Equal(t, config1.ID, database.ReplicationStatus[0].ID)
	assert.Equal(t, config2.ID, database.ReplicationStatus[1].ID)
	assert.Equal(t, "unassigned", database.ReplicationStatus[0].Status)
	assert.Equal(t, "unassigned", database.ReplicationStatus[1].Status)

	assert.Lenf(t, database.SGRCluster.Replications, 2, "Replication count mismatch")
	assert.Lenf(t, database.SGRCluster.Nodes, 0, "Replication node count mismatch")
	assertReplication := func(expected db.ReplicationConfig, actual *db.ReplicationCfg) {
		assert.Equal(t, expected.ID, actual.ID)
		assert.Equal(t, expected.Remote, actual.Remote)
		assert.Equal(t, expected.RemoteUsername, actual.RemoteUsername)
		assert.Equal(t, expected.RemotePassword, actual.RemotePassword)
		assert.Equal(t, expected.Direction, actual.Direction)
		assert.Equal(t, expected.Adhoc, actual.Adhoc)
	}

	// Check replication1 details in cluster response
	repl, ok := database.SGRCluster.Replications[config1.ID]
	assert.True(t, ok, "Error getting replication")
	config1.RemotePassword = base.RedactedStr
	assertReplication(config1, repl)

	// Check replication2 details in cluster response
	repl, ok = database.SGRCluster.Replications[config2.ID]
	assert.True(t, ok, "Error getting replication")
	config2.Remote = "http://bob:xxxxx@remote:4984/db"
	assertReplication(config2, repl)

	// Delete both replications
	response = rt.SendAdminRequest(http.MethodDelete, "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodDelete, "/{{.db}}/_replication/replication2", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Verify deletes were successful
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, response, http.StatusNotFound)
	response = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication2", "")
	rest.RequireStatus(t, response, http.StatusNotFound)

	// Check _cluster response after replications are removed
	status = rest.Status{}
	err = json.Unmarshal(response.BodyBytes(), &status)
	require.NoError(t, err, "Error un-marshalling replication response")
	require.Len(t, status.Databases["db"].ReplicationStatus, 0)
}

func TestRequireReplicatorStoppedBeforeUpsert(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SgReplicateEnabled: true})
	defer rt.Close()

	const username = "alice"
	rt.CreateUser("alice", []string{})

	DBURL := userDBURL(rt, username)

	replicationConfig := fmt.Sprintf(`{
		"replication_id": "replication1",
		"remote": "%s",
		"direction": "pushAndPull",
		"conflict_resolution_type":"default",
		"max_backoff":100,
        "continuous":true
	}`, DBURL.String())

	response := rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", replicationConfig)
	rest.RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/", "")
	rest.RequireStatus(t, response, http.StatusOK)

	var body []map[string]any
	err := base.JSONUnmarshal(response.BodyBytes(), &body)
	fmt.Println(string(response.BodyBytes()))
	assert.NoError(t, err)
	assert.Equal(t, "running", body[0]["status"])

	replicationConfigUpdate := fmt.Sprintf(`{
		"replication_id": "replication1",
		"remote": "%s",
		"direction": "push",
		"conflict_resolution_type":"default",
		"max_backoff":100,
        "continuous":true
	}`, DBURL.String())

	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", replicationConfigUpdate)
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)

	rt.WaitForReplicationStatus("replication1", db.ReplicationStateStopped)

	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", replicationConfigUpdate)
	rest.RequireStatus(t, response, http.StatusOK)
}

func TestReplicationMultiCollectionChannelFilter(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Add docs to two channels
		bulkDocs := `
	{
	"docs":
		[
			{"channels": ["ChannelOne"], "_id": "doc_1"},
			{"channels": ["ChannelOne"], "_id": "doc_2"},
			{"channels": ["ChannelOne"], "_id": "doc_3"},
			{"channels": ["ChannelOne"], "_id": "doc_4"},
			{"channels": ["ChannelTwo"], "_id": "doc_5"},
			{"channels": ["ChannelTwo"], "_id": "doc_6"},
			{"channels": ["ChannelTwo"], "_id": "doc_7"},
			{"channels": ["ChannelTwo"], "_id": "doc_8"}
		]
	}
	`
		resp := rt1.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", bulkDocs)
		rest.RequireStatus(t, resp, http.StatusCreated)

		rt1Keyspace := rt1.GetSingleDataStore().ScopeName() + "." + rt1.GetSingleDataStore().CollectionName()

		replicationID := "testRepl"

		replConf := `
	{
		"replication_id": "` + replicationID + `",
		"remote": "` + remoteURLString + `",
		"direction": "push",
		"continuous": true,
		"filter":"sync_gateway/bychannel",
		"query_params": {
			"collections_channels": {
				"` + rt1Keyspace + `": ["ChannelOne"]
			}
		},
		"collections_enabled": true,
		"collections_local": ["` + rt1Keyspace + `"]
	}`

		// Create replication for first channel
		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replication/"+replicationID, replConf)
		rest.RequireStatus(t, resp, http.StatusCreated)

		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		rt2.WaitForChanges(4, "/{{.keyspace}}/_changes?since=0", "", true)

		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

		// Upsert replication to use second channel
		replConfUpdate := `
	{
		"replication_id": "` + replicationID + `",
		"query_params": {
			"collections_channels": {
				"` + rt1Keyspace + `": ["ChannelTwo"]
			}
		}
	}`

		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replication/"+replicationID, replConfUpdate)
		rest.RequireStatus(t, resp, http.StatusOK)

		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=start", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		rt2.WaitForChanges(8, "/{{.keyspace}}/_changes?since=0", "", true)
	})
}

func TestReplicationConfigChange(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Add docs to two channels
		bulkDocs := `
	{
	"docs":
		[
			{"channels": ["ChannelOne"], "_id": "doc_1"},
			{"channels": ["ChannelOne"], "_id": "doc_2"},
			{"channels": ["ChannelOne"], "_id": "doc_3"},
			{"channels": ["ChannelOne"], "_id": "doc_4"},
			{"channels": ["ChannelTwo"], "_id": "doc_5"},
			{"channels": ["ChannelTwo"], "_id": "doc_6"},
			{"channels": ["ChannelTwo"], "_id": "doc_7"},
			{"channels": ["ChannelTwo"], "_id": "doc_8"}
		]
	}
	`
		resp := rt1.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", bulkDocs)
		rest.RequireStatus(t, resp, http.StatusCreated)

		replicationID := "testRepl"

		replConf := `
	{
		"replication_id": "` + replicationID + `",
		"remote": "` + remoteURLString + `",
		"direction": "push",
		"continuous": true,
		"filter":"sync_gateway/bychannel",
		"query_params": {
			"channels":["ChannelOne"]
		},
		"collections_enabled": ` + strconv.FormatBool(!rt1.GetDatabase().OnlyDefaultCollection()) + `
	}`

		// Create replication for first channel
		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replication/"+replicationID, replConf)
		rest.RequireStatus(t, resp, http.StatusCreated)

		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		rt2.WaitForChanges(4, "/{{.keyspace}}/_changes?since=0", "", true)

		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

		// Upsert replication to use second channel
		replConfUpdate := `
	{
		"replication_id": "` + replicationID + `",
		"query_params": {
			"channels":["ChannelTwo"]
		},
		"collections_enabled": ` + strconv.FormatBool(!rt1.GetDatabase().OnlyDefaultCollection()) + `
	}`

		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replication/"+replicationID, replConfUpdate)
		rest.RequireStatus(t, resp, http.StatusOK)

		resp = rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=start", "")
		rest.RequireStatus(t, resp, http.StatusOK)
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		rt2.WaitForChanges(8, "/{{.keyspace}}/_changes?since=0", "", true)
	})
}

// TestReplicationHeartbeatRemoval
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates two continuous pull replications on rt1 via the REST API
//   - adds another active RT
//   - simulates heartbeat-based removal of first RT by second RT
//   - validates that active RT adds itself back to the node set and is reassigned a replication
//   - Creates more documents, validates they are replicated
func TestReplicationHeartbeatRemoval(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only (replication rebalance)")
	}

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		// Increase checkpoint persistence frequency for cross-node status verification
		reduceCheckpointInterval := reduceTestCheckpointInterval(50 * time.Millisecond)
		t.Cleanup(reduceCheckpointInterval)
		restartBatching := db.SuspendSequenceBatching()
		t.Cleanup(restartBatching)

		activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create docs on remote
		docABC1 := rest.SafeDocumentName(t, t.Name()+"ABC1")
		docDEF1 := rest.SafeDocumentName(t, t.Name()+"DEF1")
		_ = remoteRT.PutDoc(docABC1, `{"source":"remoteRT","channels":["ABC"]}`)
		_ = remoteRT.PutDoc(docDEF1, `{"source":"remoteRT","channels":["DEF"]}`)

		// Create pull replications, verify running
		activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePull, []string{"ABC"}, true, db.ConflictResolverDefault, "")
		activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePull, []string{"DEF"}, true, db.ConflictResolverDefault, "")
		activeRT.WaitForAssignedReplications(2)
		activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
		activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

		// wait for documents originally written to remoteRT to arrive at activeRT
		changesResults := activeRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)

		// Validate doc replication
		_ = activeRT.GetDocBody(docABC1)
		_ = activeRT.GetDocBody(docDEF1)

		// Add another node to the active cluster
		activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
		defer activeRT2.Close()

		// Wait for replication to be rebalanced to activeRT2
		activeRT.WaitForAssignedReplications(1)
		activeRT2.WaitForAssignedReplications(1)

		// Create additional docs on remoteRT
		docABC2 := rest.SafeDocumentName(t, t.Name()+"ABC2")
		_ = remoteRT.PutDoc(docABC2, `{"source":"remoteRT","channels":["ABC"]}`)
		docDEF2 := rest.SafeDocumentName(t, t.Name()+"DEF2")
		_ = remoteRT.PutDoc(docDEF2, `{"source":"remoteRT","channels":["DEF"]}`)

		// wait for new documents to arrive at activeRT
		changesResults = activeRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		changesResults.RequireDocIDs(t, []string{docABC2, docDEF2})

		// Validate doc contents via both active nodes
		_ = activeRT.GetDocBody(docABC2)
		_ = activeRT.GetDocBody(docDEF2)
		_ = activeRT2.GetDocBody(docABC2)
		_ = activeRT2.GetDocBody(docDEF2)

		activeRTUUID := activeRT.GetDatabase().UUID
		activeRT2UUID := activeRT2.GetDatabase().UUID
		activeRTMgr := activeRT2.GetDatabase().SGReplicateMgr
		activeRT2Mgr := activeRT2.GetDatabase().SGReplicateMgr

		// Have each RT remove the other node (simulates behaviour on heartbeat expiry)
		assert.NoError(t, activeRTMgr.RemoveNode(activeRT2UUID))
		assert.NoError(t, activeRT2Mgr.RemoveNode(activeRTUUID))

		// Wait for nodes to add themselves back to cluster
		err := activeRT.WaitForCondition(func() bool {
			clusterDef, err := activeRTMgr.GetSGRCluster()
			if err != nil {
				return false
			}
			return len(clusterDef.Nodes) == 2
		})
		assert.NoError(t, err, "Nodes did not re-register after removal")

		// Wait and validate replications are rebalanced
		activeRT.WaitForAssignedReplications(1)
		activeRT2.WaitForAssignedReplications(1)

		// Add more docs to remote, to validate rebalanced replications are running
		docABC3 := rest.SafeDocumentName(t, t.Name()+"ABC3")
		_ = remoteRT.PutDoc(docABC3, `{"source":"remoteRT","channels":["ABC"]}`)
		docDEF3 := rest.SafeDocumentName(t, t.Name()+"DEF3")
		_ = remoteRT.PutDoc(docDEF3, `{"source":"remoteRT","channels":["DEF"]}`)

		changesResults = activeRT.WaitForChanges(2, "/{{.keyspace}}/_changes?since="+changesResults.Last_Seq.String(), "", true)
		changesResults.RequireDocIDs(t, []string{docABC3, docDEF3})

		// explicitly stop the SGReplicateMgrs on the active nodes, to prevent a node rebalance during test teardown.
		activeRT.GetDatabase().SGReplicateMgr.Stop()
		activeRT.GetDatabase().SGReplicateMgr = nil
		activeRT2.GetDatabase().SGReplicateMgr.Stop()
		activeRT2.GetDatabase().SGReplicateMgr = nil
	})
}

// Repros CBG-2416
func TestDBReplicationStatsTeardown(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	// Test tests Prometheus stat registration
	base.SkipPrometheusStatsRegistration = false
	defer func() {
		base.SkipPrometheusStatsRegistration = true
	}()
	ctx := base.TestCtx(t)
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			PersistentConfig: true,
			CustomTestBucket: tb,
		})
	defer rt.Close()

	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()
	db2Url, err := url.Parse(srv.URL + "/db2")
	require.NoError(t, err)

	const (
		db2 = "db2"
		db1 = "db1"
	)
	db2Config := rt.NewDbConfig()
	db2Config.Bucket = base.Ptr(tb.GetName())
	rest.RequireStatus(t, rt.CreateDatabase(db2, db2Config), http.StatusCreated)

	tb2 := base.GetTestBucket(t)
	defer tb2.Close(ctx)
	db1Config := rt.NewDbConfig()
	db1Config.Bucket = base.Ptr(tb2.GetName())
	rest.RequireStatus(t, rt.CreateDatabase(db1, db1Config), http.StatusCreated)

	rt.CreateReplicationForDB("{{.db1}}", "repl1", db2Url.String(), db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault, "")
	rt.WaitForReplicationStatusForDB("{{.db1}}", "repl1", db.ReplicationStateRunning)

	// Wait for document to replicate from db to db2 to confirm replication start
	marker1 := "marker1"
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.db1keyspace}}/"+marker1, `{"prop":true}`), http.StatusCreated)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest("GET", "/{{.db2keyspace}}/"+marker1, "")
		assert.Equal(c, http.StatusOK, resp.Code)
	}, 5*time.Second, 100*time.Millisecond)

	// Force DB reload by modifying config
	resp := rt.SendAdminRequest(http.MethodPost, "/"+db1+"/_config", `{"import_docs": false}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// If CE, recreate the replication
	if !base.IsEnterpriseEdition() {
		rt.CreateReplicationForDB(db1, "repl1", db2Url.String(), db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault, "")
		rt.WaitForReplicationStatusForDB(db1, "repl1", db.ReplicationStateRunning)
	}

	// Wait for second document to replicate to confirm replication restart
	marker2 := "marker2"
	rest.RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.db1keyspace}}/"+marker2, `{"prop":true}`), http.StatusCreated)
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		resp := rt.SendAdminRequest("GET", "/{{.db2keyspace}}/"+marker2, "")
		assert.Equal(c, http.StatusOK, resp.Code)
	}, 5*time.Second, 100*time.Millisecond)
}

func TestTakeDbOfflineOngoingPushReplication(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create doc1 on rt1
		docID1 := rest.SafeDocumentName(t, t.Name()+"rt1doc")
		_ = rt1.PutDoc(docID1, `{"source":"rt1","channels":["alice"]}`)

		// Create push replication, verify running
		replicationID := rest.SafeDocumentName(t, t.Name())
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		// wait for document originally written to rt1 to arrive at rt2
		changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID1, changesResults.Results[0].ID)

		resp := rt2.SendAdminRequest("POST", "/{{.db}}/_offline", "")
		assert.Equal(t, resp.Code, 200)
	})
}

// TestPushReplicationAPIUpdateDatabase starts a push replication and updates the passive database underneath the replication.
// Expect to see the connection closed with an error, instead of continuously panicking.
// This is the ISGR version of TestBlipPusherUpdateDatabase
//
// This test causes the race detector to flag the bucket=nil operation and any in-flight requests being made using that bucket, prior to the replication being reset.
// TODO CBG-1903: Can be fixed by draining in-flight requests before fully closing the database.
func TestPushReplicationAPIUpdateDatabase(t *testing.T) {

	t.Skip("Skipping test - revisit in CBG-1908")

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// Create initial doc on rt1
		docID := rest.SafeDocumentName(t, t.Name()+"rt1doc")
		_ = rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)

		// Create push replication, verify running
		replicationID := t.Name()
		rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault, "")
		rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		// wait for document originally written to rt1 to arrive at rt2
		changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		require.Equal(t, docID, changesResults.Results[0].ID)

		var lastDocID atomic.Value

		// Wait for the background updates to finish at the end of the test
		shouldCreateDocs := base.NewAtomicBool(true)
		wg := sync.WaitGroup{}
		wg.Add(1)
		defer func() {
			shouldCreateDocs.Set(false)
			wg.Wait()
		}()

		// Start creating documents in the background on rt1 for the replicator to push to rt2
		go func() {
			for i := 0; shouldCreateDocs.IsTrue(); i++ {
				docID := fmt.Sprintf("%s-doc%d", t.Name(), i)
				_ = rt1.PutDoc(docID, fmt.Sprintf(`{"i":%d,"channels":["alice"]}`, i))
				lastDocID.Store(docID)
			}
			rt1.WaitForPendingChanges()
			wg.Done()
		}()

		// and wait for a few to be done before we proceed with updating database config underneath replication
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			changes := rt2.GetChanges("/{{.keyspace}}/_changes", "")
			assert.GreaterOrEqual(c, 5, changes.Results)
		}, time.Second*5, time.Millisecond*100)

		// just change the sync function to cause the database to reload
		dbConfig := *rt2.ServerContext().GetDbConfig("db")
		dbConfig.Sync = base.Ptr(`function(doc){channel(doc.channels);}`)
		resp := rt2.ReplaceDbConfig("db", dbConfig)
		rest.RequireStatus(t, resp, http.StatusCreated)

		shouldCreateDocs.Set(false)

		lastDocIDString, ok := lastDocID.Load().(string)
		require.True(t, ok)

		// wait for the last document written to rt1 to arrive at rt2
		rest.WaitAndAssertCondition(t, func() bool {
			collection, ctx := rt2.GetSingleTestDatabaseCollection()
			_, err := collection.GetDocument(ctx, lastDocIDString, db.DocUnmarshalSync)
			return err == nil
		})
	})
}

// TestActiveReplicatorHeartbeats uses an ActiveReplicator with another RestTester instance to connect, and waits for several websocket ping/pongs.
func TestActiveReplicatorHeartbeats(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyWebSocket, base.KeyWebSocketFrame)

	username := "alice"
	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					username: {Password: base.Ptr(rest.RestTesterDefaultUserPassword)},
				},
			}},
		})
	defer rt.Close()
	ctx := rt.Context()

	ar, err := db.NewActiveReplicator(ctx, &db.ActiveReplicatorConfig{
		ID:                    t.Name(),
		Direction:             db.ActiveReplicatorTypePush,
		ActiveDB:              &db.Database{DatabaseContext: rt.GetDatabase()},
		RemoteDBURL:           userDBURL(rt, username),
		WebsocketPingInterval: time.Millisecond * 10,
		Continuous:            true,
		ReplicationStatsMap:   dbReplicatorStats(t),
		CollectionsEnabled:    !rt.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)

	pingCountStart := base.ExpvarVar2Int(ctx, expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count"))
	pingGoroutinesStart := base.ExpvarVar2Int(ctx, expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))

	assert.NoError(t, ar.Start(ctx))

	// let some pings happen
	time.Sleep(time.Millisecond * 500)

	pingGoroutines := base.ExpvarVar2Int(ctx, expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))
	assert.Equal(t, 1+pingGoroutinesStart, pingGoroutines, "Expected ping sender goroutine to be 1 more than start")

	pingCount := base.ExpvarVar2Int(ctx, expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count"))
	assert.Greaterf(t, pingCount, pingCountStart, "Expected ping count to increase since start")
	assert.NoError(t, ar.Stop())

	pingGoroutines = base.ExpvarVar2Int(ctx, expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))
	assert.Equal(t, pingGoroutinesStart, pingGoroutines, "Expected ping sender goroutine to return to start count after stop")
}

// TestActiveReplicatorPullBasic:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullBasic(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {

		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)

		const (
			// test url encoding of username/password with these
			username = "AL_1c.e-@"
			password = rest.RestTesterDefaultUserPassword
		)

		docID := rest.SafeDocumentName(t, t.Name()) + "rt2doc1"
		version := rt2.PutDoc(docID, `{"source":"rt2","channels":["`+username+`"]}`)

		rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
		remoteDoc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		ctx1 := rt1.Context()

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()

		assert.Equal(t, "", ar.GetStatus(ctx1).LastSeqPull)

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt2 to arrive at rt1
		sgrRunner.WaitForVersion(docID, rt1, version)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])

		// replication status updates just after the document has been written in a blip handler callback, so the
		// document can exist before stat is updated
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			status := ar.GetStatus(ctx1)
			assert.Equal(c, strconv.FormatUint(remoteDoc.Sequence, 10), status.LastSeqPull, "status=%#+v", status)
		}, 5*time.Second, 10*time.Millisecond)
	})
}

// TestActiveReplicatorPullSkippedSequence ensures that ISGR and the checkpointer are able to handle the compound sequence format appropriately.
// - Creates several documents on rt2, separated by a skipped sequence, and rt1 pulls them.
//   - rt2 seq 1 _user    rt1 seq n/a
//   - rt2 seq 2 doc1     rt1 seq 1
//   - rt2 seq 3 doc2     rt1 seq 2
//   - rt2 seq 4 skipped  rt1 seq n/a
//   - rt2 seq 5 doc3     rt1 seq 3
//   - rt2 seq 6 doc4     rt1 seq 4
//
// - Issues a few pulls to ensure the replicator is resuming correctly from a compound sequence checkpoint, and that we're emptying the expected/processed lists appropriately.
func TestActiveReplicatorPullSkippedSequence(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyCRUD, base.KeyChanges, base.KeyReplicate)

	// Passive
	const (
		username = "alice"
		password = rest.RestTesterDefaultUserPassword
	)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		restartBatching := db.SuspendSequenceBatching()
		t.Cleanup(restartBatching)

		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			PassiveRestTesterConfig: &rest.RestTesterConfig{
				DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
					CacheConfig: &rest.CacheConfig{
						// shorten pending sequence handling to speed up test
						ChannelCacheConfig: &rest.ChannelCacheConfig{
							MaxWaitPending: base.Ptr(uint32(1)),
						},
					},
				}},
			},
		})
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)
		ctx1 := rt1.Context()

		dbstats := dbReplicatorStats(t)

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()

		docIDPrefix := rest.SafeDocumentName(t, t.Name()+"rt2doc")

		docID1 := docIDPrefix + "1"
		doc1Version := rt2.PutDoc(docID1, `{"source":"rt2","channels":["`+username+`"]}`)
		rt2.WaitForPendingChanges()

		// Start the replicator (implicit connect)
		assert.NoError(t, ar.Start(ctx1))

		pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

		// wait for the documents originally written to rt2 to arrive at rt1
		sgrRunner.WaitForVersion(docID1, rt1, doc1Version)

		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, 1)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, 1)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().AlreadyKnownSequenceCount)
		require.NoError(t, ar.Stop())

		assert.Equal(t, int64(1), dbstats.ExpectedSequenceLen.Value())
		assert.Equal(t, int64(1), dbstats.ProcessedSequenceLen.Value())
		assert.Equal(t, int64(0), dbstats.ExpectedSequenceLenPostCleanup.Value())
		assert.Equal(t, int64(0), dbstats.ProcessedSequenceLenPostCleanup.Value())

		docID2 := docIDPrefix + "2"
		rt2.PutDoc(docID2, `{"source":"rt2","channels":["`+username+`"]}`)

		// allocate a fake sequence to trigger skipped sequence handling - this never arrives at rt1 - we could think about creating the doc afterwards to let the replicator recover, but not necessary for the test.
		_, err = rt2.MetadataStore().Incr(rt2.GetDatabase().MetadataKeys.SyncSeqKey(), 1, 1, 0)
		require.NoError(t, err)

		docID3 := docIDPrefix + "3"
		rt2.PutDoc(docID3, `{"source":"rt2","channels":["`+username+`"]}`)
		rt2.WaitForPendingChanges()

		// Start the replicator (implicit connect)
		assert.NoError(t, ar.Start(ctx1))

		// restarted replicator has a new checkpointer
		pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

		rt1.WaitForChanges(3, "/{{.keyspace}}/_changes?since=0", "", true)

		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, 2)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, 2)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().AlreadyKnownSequenceCount)
		require.NoError(t, ar.Stop())

		assert.Equal(t, int64(2), dbstats.ExpectedSequenceLen.Value())
		assert.Equal(t, int64(2), dbstats.ProcessedSequenceLen.Value())
		assert.Equal(t, int64(0), dbstats.ExpectedSequenceLenPostCleanup.Value())
		assert.Equal(t, int64(0), dbstats.ProcessedSequenceLenPostCleanup.Value())

		docID4 := docIDPrefix + "4"
		rt2.PutDoc(docID4, `{"source":"rt2","channels":["`+username+`"]}`)
		rt2.WaitForPendingChanges()

		require.NoError(t, ar.Start(ctx1))

		// restarted replicator has a new checkpointer
		pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

		rt1.WaitForChanges(4, "/{{.keyspace}}/_changes?since=0", "", true)

		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, 1)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, 1)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().AlreadyKnownSequenceCount)
		require.NoError(t, ar.Stop())

		assert.Equal(t, int64(1), dbstats.ExpectedSequenceLen.Value())
		assert.Equal(t, int64(1), dbstats.ProcessedSequenceLen.Value())
		assert.Equal(t, int64(0), dbstats.ExpectedSequenceLenPostCleanup.Value())
		assert.Equal(t, int64(0), dbstats.ProcessedSequenceLenPostCleanup.Value())
	})
}

// TestReplicatorReconnectBehaviour tests the interactive values that configure replicator reconnection behaviour
func TestReplicatorReconnectBehaviour(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		name                 string
		maxBackoff           int
		specified            bool
		reconnectTimeout     time.Duration
		maxReconnectInterval time.Duration
	}{
		{
			name:                 "maxbackoff 0",
			specified:            true,
			maxBackoff:           0,
			reconnectTimeout:     10 * time.Minute,
			maxReconnectInterval: 5 * time.Minute,
		},
		{
			name:                 "max backoff not specified",
			specified:            false,
			reconnectTimeout:     0 * time.Minute,
			maxReconnectInterval: 5 * time.Minute,
		},
		{
			name:                 "maxbackoff 1",
			specified:            true,
			maxBackoff:           1,
			reconnectTimeout:     0 * time.Minute,
			maxReconnectInterval: 1 * time.Minute,
		},
	}

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				activeRT, _, remoteURL := sgrRunner.SetupSGRPeers(t)
				var resp *rest.TestResponse

				if test.specified {
					resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", fmt.Sprintf(`{
    					"replication_id": "replication1", "remote": "%s", "direction": "pull",
						"collections_enabled": %t, "continuous": true, "max_backoff_time": %d}`, remoteURL, base.TestsUseNamedCollections(), test.maxBackoff))
					rest.RequireStatus(t, resp, http.StatusCreated)
				} else {
					resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", fmt.Sprintf(`{
    					"replication_id": "replication1", "remote": "%s", "direction": "pull",
						"collections_enabled": %t, "continuous": true}`, remoteURL, base.TestsUseNamedCollections()))
					rest.RequireStatus(t, resp, http.StatusCreated)
				}
				activeRT.WaitForReplicationStatus("replication1", db.ReplicationStateRunning)
				activeRT.WaitForActiveReplicatorInitialization(1)

				activeReplicator := activeRT.GetDatabase().SGReplicateMgr.GetActiveReplicator("replication1")
				config := activeReplicator.GetActiveReplicatorConfig()

				assert.Equal(t, test.reconnectTimeout, config.TotalReconnectTimeout)
				assert.Equal(t, test.maxReconnectInterval, config.MaxReconnectInterval)
			})
		}
	})

}

// TestReconnectReplicator:
//   - Starts 2 RestTesters, one active, and one remote.
//   - creates a pull replication from remote to active rest tester
//   - kills the blip sender to simulate a disconnect that was not initiated by the user
//   - asserts the replicator enters a reconnecting state and eventually enters a running state again
//   - puts some docs on the remote rest tester and assert the replicator pulls these docs to prove reconnect was successful
func TestReconnectReplicator(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		name       string
		maxBackoff int
		specified  bool
	}{
		{
			name:       "maxbackoff 0",
			specified:  true,
			maxBackoff: 0,
		},
		{
			name:      "max backoff not specified",
			specified: false,
		},
		{
			name:       "maxbackoff 1",
			specified:  true,
			maxBackoff: 1,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				activeRT, remoteRT, remoteURL := sgrRunner.SetupSGRPeers(t)
				var resp *rest.TestResponse
				const replicationName = "replication1"

				if test.specified {
					resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", fmt.Sprintf(`{
    					"replication_id": "%s", "remote": "%s", "direction": "pull",
						"collections_enabled": %t, "continuous": true, "max_backoff_time": %d}`, replicationName, remoteURL, base.TestsUseNamedCollections(), test.maxBackoff))
					rest.RequireStatus(t, resp, http.StatusCreated)
				} else {
					resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", fmt.Sprintf(`{
    					"replication_id": "%s", "remote": "%s", "direction": "pull",
						"collections_enabled": %t, "continuous": true}`, replicationName, remoteURL, base.TestsUseNamedCollections()))
					rest.RequireStatus(t, resp, http.StatusCreated)
				}
				activeRT.WaitForReplicationStatus("replication1", db.ReplicationStateRunning)

				activeRT.WaitForActiveReplicatorInitialization(1)
				ar := activeRT.GetDatabase().SGReplicateMgr.GetActiveReplicator("replication1")
				numConnectionAttempts := ar.Pull.GetStats().NumConnectAttempts.Value()
				// race between stopping the blip sender here and the initialization of it on the replicator so need this assertion in here to avoid panic
				activeRT.WaitForPullBlipSenderInitialisation(replicationName)
				ar.Pull.GetBlipSender().Stop()

				// assert on replicator reconnecting and getting back to running state once reconnected
				// due to race in jenkins we assert on connection stat increasing instead of asserting on replication state hitting reconnecting state
				ar = activeRT.GetDatabase().SGReplicateMgr.GetActiveReplicator("replication1")
				assert.Greater(t, ar.Pull.GetStats().NumConnectAttempts.Value(), numConnectionAttempts)
				activeRT.WaitForReplicationStatus(replicationName, db.ReplicationStateRunning)

				// assert the replicator works and we replicate docs still after replicator reconnects
				for i := range 10 {
					response := remoteRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+fmt.Sprint(i), `{"source": "remote"}`)
					rest.RequireStatus(t, response, http.StatusCreated)
				}
				activeRT.WaitForChanges(10, "/{{.keyspace}}/_changes", "", true)
			})
		}
	})

}

func TestReplicatorReconnectTimeout(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, passiveRT, _ := sgrRunner.SetupSGRPeers(t)

		id, err := base.GenerateRandomID()
		require.NoError(t, err)
		arConfig := db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: userDBURL(passiveRT, "bob"),
			ActiveDB: &db.Database{
				DatabaseContext: activeRT.GetDatabase(),
			},
			Continuous: true,
			// aggressive reconnect intervals for testing purposes
			TotalReconnectTimeout:  time.Millisecond * 10,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !activeRT.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(activeRT.Context(), &arConfig)
		require.NoError(t, err)
		require.Equal(t, int64(0), ar.Push.GetStats().NumConnectAttempts.Value())

		expectedErrMsg := "unexpected status code 401 from target database"
		require.ErrorContains(t, ar.Start(activeRT.Context()), expectedErrMsg)
		activeRT.WaitForReplicationStatus(id, db.ReplicationStateError)

		status, err := activeRT.GetDatabase().SGReplicateMgr.GetReplicationStatus(activeRT.Context(), id, db.DefaultReplicationStatusOptions())
		require.NoError(t, err)
		require.Equal(t, db.ReplicationStateError, status.Status)
		require.Equal(t, expectedErrMsg, status.ErrorMessage)
		// state is updated by the reconnect loop slightly before NumReconnectsAborted is set
		base.RequireWaitForStat(t, ar.Push.GetStats().NumReconnectsAborted.Value, 1)
		firstNumConnectAttempts := ar.Push.GetStats().NumConnectAttempts.Value()
		require.GreaterOrEqual(t, firstNumConnectAttempts, int64(1))

		// restart replicator to make sure we'll retry a reconnection, so the state can go back to reconnecting
		require.ErrorContains(t, ar.Start(activeRT.Context()), expectedErrMsg)
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state, errMsg := ar.State(activeRT.Context())
			assert.Equal(c, db.ReplicationStateError, state)
			assert.Equal(c, expectedErrMsg, errMsg)
			assert.Equal(c, int64(2), ar.Push.GetStats().NumReconnectsAborted.Value())
		}, time.Second*10, time.Millisecond*100)
		require.GreaterOrEqual(t, ar.Push.GetStats().NumConnectAttempts.Value(), firstNumConnectAttempts+1)
	})
}

// TestTotalSyncTimeStat:
//   - starts a replicator to simulate a long lived websocket connection on a sync gateway
//   - wait for this replication connection to be picked up on stats (NumReplicationsActive)
//   - wait some time for the background task to increment TotalSyncTime stat
//   - assert on the TotalSyncTime stat being incremented
func TestTotalSyncTimeStat(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, passiveRT, remoteURL := sgrRunner.SetupSGRPeers(t)
		const repName = "replication1"

		startValue := passiveRT.GetDatabase().DbStats.DatabaseStats.TotalSyncTime.Value()
		require.Equal(t, int64(0), startValue)

		// create a replication to just make a long lived websocket connection between two rest testers
		activeRT.CreateReplication(repName, remoteURL, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault, "")
		activeRT.WaitForReplicationStatus(repName, db.ReplicationStateRunning)

		// wait for active replication stat to pick up the replication connection
		base.RequireWaitForStat(passiveRT.TB(), func() int64 {
			return passiveRT.GetDatabase().DbStats.DatabaseStats.NumReplicationsActive.Value()
		}, 1)

		// wait some time to wait for the stat to increment
		base.RequireWaitForStat(passiveRT.TB(), func() int64 {
			return passiveRT.GetDatabase().DbStats.DatabaseStats.TotalSyncTime.Value()
		}, 2)

		syncTimeStat := passiveRT.GetDatabase().DbStats.DatabaseStats.TotalSyncTime.Value()
		// we can't be certain how long has passed since grabbing the stat so to avoid flake here just assert the stat has incremented
		require.Greater(t, syncTimeStat, startValue)
	})
}

// TestChangesEndpointTotalSyncTime:
//   - add a user to run the changes endpoint as
//   - start a changes feed request with user (simulating CBL replication)
//   - wait for CBL stat NumPullReplActiveContinuous to pick up the replication connection
//   - assert on the TotalSyncTime stat being incremented
//   - put doc to end changes feed connection
func TestChangesEndpointTotalSyncTime(t *testing.T) {
	base.LongRunningTest(t)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channel);}`,
	})
	defer rt.Close()

	// to run changes feed as
	rt.CreateUser("alice", []string{"ABC"})

	// assert stat is zero value to begin with
	startValue := rt.GetDatabase().DbStats.DatabaseStats.TotalSyncTime.Value()
	require.Equal(t, int64(0), startValue)

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs1", `{"value":1, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs2", `{"value":2, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/pbs3", `{"value":3, "channel":["PBS"]}`)
	rest.RequireStatus(t, response, 201)

	changesJSON := `{"style":"all_docs",
					 "heartbeat":300000,
					 "feed":"longpoll",
					 "limit":50,
					 "since":"1",
					 "filter":"` + base.ByChannelFilter + `",
					 "channels":"ABC,PBS"}`
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		resp1 := rt.SendUserRequest(http.MethodPost, "/{{.keyspace}}/_changes", changesJSON, "alice")
		rest.RequireStatus(t, resp1, http.StatusOK)
	}()

	// wait for active replication stat for CBL to pick up the replication connection
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.CBLReplicationPullStats.NumPullReplActiveContinuous.Value()
	}, 1)

	// wait some time to wait for the stat to increment
	base.RequireWaitForStat(t, func() int64 {
		return rt.GetDatabase().DbStats.DatabaseStats.TotalSyncTime.Value()
	}, 2)

	syncTimeStat := rt.GetDatabase().DbStats.DatabaseStats.TotalSyncTime.Value()
	// we can't be certain how long has passed since grabbing the stat so to avoid flake here just assert the stat has incremented
	require.Greater(t, syncTimeStat, startValue)

	// put doc to end changes feed
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/abc1", `{"value":3, "channel":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	wg.Wait()

}

// TestActiveReplicatorPullAttachments:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document with an attachment on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
//   - Creates a second doc which references the same attachment.
func TestActiveReplicatorPullAttachments(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	const (
		username = "alice"
	)

	sgrRuunner := rest.NewSGRTestRunner(t)
	sgrRuunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRuunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})

		attachment := `"_attachments":{"hi.txt":{"data":"aGk=","content_type":"text/plain"}}`

		docID := rest.SafeDocumentName(t, t.Name()+"rt2doc1")
		version := rt2.PutDoc(docID, `{"source":"rt2","doc_num":1,`+attachment+`,"channels":["alice"]}`)

		ctx1 := rt1.Context()

		replicationID := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRuunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()

		assert.Equal(t, int64(0), ar.Pull.GetStats().GetAttachment.Value())

		// Start the replicator (implicit connect)
		assert.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt2 to arrive at rt1
		sgrRuunner.WaitForVersion(docID, rt1, version)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])

		assert.Equal(t, int64(1), ar.Pull.GetStats().GetAttachment.Value())

		docID = rest.SafeDocumentName(t, t.Name()+"rt2doc2")
		version = rt2.PutDoc(docID, `{"source":"rt2","doc_num":2,`+attachment+`,"channels":["alice"]}`)

		// wait for the new document written to rt2 to arrive at rt1
		sgrRuunner.WaitForVersion(docID, rt1, version)

		doc2, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err = doc2.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])

		// When targeting a Hydrogen node that supports proveAttachments, we typically end up sending
		// the attachment only once. However, targeting a Lithium node sends the attachment twice like
		// the pre-Hydrogen node, GetAttachment would be 2. The reason is that a Hydrogen node uses a
		// new storage model for attachment storage and retrieval.
		assert.Equal(t, int64(2), ar.Pull.GetStats().GetAttachment.Value())
		assert.Equal(t, int64(0), ar.Pull.GetStats().ProveAttachment.Value())
	})
}

// TestActiveReplicatorPullMergeConflictingAttachments:
//   - Creates an initial revision on rt2 which is replicated to rt1.
//   - Stops the replicator, and adds different attachments to the doc on both rt1 and rt2 at conflicting revisions.
//   - Starts the replicator to trigger conflict resolution to merge both attachments in the conflict.
func TestActiveReplicatorPullMergeConflictingAttachments(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skip("Test uses EE-only features for custom conflict resolution")
	}

	base.RequireNumTestBuckets(t, 2)

	tests := []struct {
		name                     string
		initialRevBody           string
		localConflictingRevBody  string
		remoteConflictingRevBody string
		expectedAttachments      int
	}{
		{
			name:                     "merge new conflicting atts",
			initialRevBody:           `{"channels":["alice"]}`,
			localConflictingRevBody:  `{"source":"rt1","_attachments":{"localAtt.txt":{"data":"cmVtb3Rl"}},"channels":["alice"]}`,
			remoteConflictingRevBody: `{"source":"rt2","_attachments":{"remoteAtt.txt":{"data":"bG9jYWw="}},"channels":["alice"]}`,
			expectedAttachments:      2,
		},
		{
			name:                     "remove initial attachment",
			initialRevBody:           `{"_attachments":{"initialAtt.txt":{"data":"aW5pdGlhbA=="}},"channels":["alice"]}`,
			localConflictingRevBody:  `{"source":"rt1","channels":["alice"]}`,
			remoteConflictingRevBody: `{"source":"rt2","channels":["alice"]}`,
			expectedAttachments:      0,
		},
		{
			name:                     "preserve initial attachment with local",
			initialRevBody:           `{"_attachments":{"initialAtt.txt":{"data":"aW5pdGlhbA=="}},"channels":["alice"]}`,
			localConflictingRevBody:  `{"source":"rt1","_attachments":{"initialAtt.txt":{"stub":true,"revpos":1}},"channels":["alice"]}`,
			remoteConflictingRevBody: `{"source":"rt2","channels":["alice"]}`,
			expectedAttachments:      1,
		},
		{
			name:                     "preserve initial attachment with remote",
			initialRevBody:           `{"_attachments":{"initialAtt.txt":{"data":"aW5pdGlhbA=="}},"channels":["alice"]}`,
			localConflictingRevBody:  `{"source":"rt1","channels":["alice"]}`,
			remoteConflictingRevBody: `{"source":"rt2","_attachments":{"initialAtt.txt":{"stub":true,"revpos":1}},"channels":["alice"]}`,
			expectedAttachments:      1,
		},
		{
			name:                     "preserve initial attachment with new local att",
			initialRevBody:           `{"_attachments":{"initialAtt.txt":{"data":"aW5pdGlhbA=="}},"channels":["alice"]}`,
			localConflictingRevBody:  `{"source":"rt1","_attachments":{"initialAtt.txt":{"stub":true,"revpos":1},"localAtt.txt":{"data":"cmVtb3Rl"}},"channels":["alice"]}`,
			remoteConflictingRevBody: `{"source":"rt2","channels":["alice"]}`,
			expectedAttachments:      2,
		},
		{
			name:                     "preserve initial attachment with new remote att",
			initialRevBody:           `{"_attachments":{"initialAtt.txt":{"data":"aW5pdGlhbA=="}},"channels":["alice"]}`,
			localConflictingRevBody:  `{"source":"rt1","_attachments":{"initialAtt.txt":{"stub":true,"revpos":1}},"channels":["alice"]}`,
			remoteConflictingRevBody: `{"source":"rt2","_attachments":{"remoteAtt.txt":{"data":"bG9jYWw="}},"channels":["alice"]}`,
			expectedAttachments:      2,
		},
		{
			name:                     "preserve initial attachment with new conflicting atts",
			initialRevBody:           `{"_attachments":{"initialAtt.txt":{"data":"aW5pdGlhbA=="}},"channels":["alice"]}`,
			localConflictingRevBody:  `{"source":"rt1","_attachments":{"initialAtt.txt":{"stub":true,"revpos":1},"localAtt.txt":{"data":"cmVtb3Rl"}},"channels":["alice"]}`,
			remoteConflictingRevBody: `{"source":"rt2","_attachments":{"remoteAtt.txt":{"data":"bG9jYWw="}},"channels":["alice"]}`,
			expectedAttachments:      3,
		},
	}

	const username = "alice"

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				// Increase checkpoint persistence frequency for cross-node status verification
				reduceCheckpoint := reduceTestCheckpointInterval(50 * time.Millisecond)
				t.Cleanup(reduceCheckpoint)

				rt1, rt2, remoteURL := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
					UserChannelAccess: []string{username},
				})

				resolverCode := `
					function(conflict) {
						var mergedDoc = new Object();
						mergedDoc.source = "merged";

						var mergedAttachments = new Object();
						dst = conflict.RemoteDocument._attachments;
						for (var key in dst) {
							mergedAttachments[key] = dst[key];
						}
						src = conflict.LocalDocument._attachments;
						for (var key in src) {
							mergedAttachments[key] = src[key];
						}
						mergedDoc._attachments = mergedAttachments;

						mergedDoc.channels = ["alice"];

						return mergedDoc;
					}`

				rt1.CreateReplication("repl1", remoteURL, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverCustom, resolverCode)

				rt1.WaitForReplicationStatus("repl1", db.ReplicationStateRunning)

				docID := test.name + "doc1"
				version1 := rt2.PutDoc(docID, test.initialRevBody)

				// wait for the document originally written to rt2 to arrive at rt1
				changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
				assert.Equal(t, docID, changesResults.Results[0].ID)
				lastSeq := changesResults.Last_Seq.String()

				resp := rt1.SendAdminRequest(http.MethodPut, "/{{.db}}/_replicationStatus/repl1?action=stop", "")
				rest.RequireStatus(t, resp, http.StatusOK)

				rt1.WaitForReplicationStatus("repl1", db.ReplicationStateStopped)

				resp = rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+version1.RevTreeID, test.localConflictingRevBody)
				rest.RequireStatus(t, resp, http.StatusCreated)

				rt1DocConflictVersion, _ := rt1.GetDoc(docID)

				changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
				assert.Equal(t, docID, changesResults.Results[0].ID)
				lastSeq = changesResults.Last_Seq.String()

				resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+version1.RevTreeID, test.remoteConflictingRevBody)
				rest.RequireStatus(t, resp, http.StatusCreated)

				rt2DocConflictVersion, _ := rt2.GetDoc(docID)

				resp = rt1.SendAdminRequest(http.MethodPut, "/{{.db}}/_replicationStatus/repl1?action=start", "")
				rest.RequireStatus(t, resp, http.StatusOK)

				rt1.WaitForReplicationStatus("repl1", db.ReplicationStateRunning)

				changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
				assert.Equal(t, docID, changesResults.Results[0].ID)
				_ = changesResults.Last_Seq.String()

				rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
				doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				ctx := base.TestCtx(t)
				revGen, _ := db.ParseRevID(ctx, doc.SyncData.GetRevTreeID())

				assert.Equal(t, 3, revGen)
				assert.Equal(t, "merged", doc.Body(ctx)["source"].(string))

				if sgrRunner.IsV4Protocol() {
					// assert on hlv
					require.Len(t, doc.HLV.MergeVersions, 2)
					val1, ok := doc.HLV.MergeVersions[rt1DocConflictVersion.CV.SourceID]
					require.True(t, ok)
					assert.Equal(t, rt1DocConflictVersion.CV.Value, val1)
					val2, ok := doc.HLV.MergeVersions[rt2DocConflictVersion.CV.SourceID]
					require.True(t, ok)
					assert.Equal(t, rt2DocConflictVersion.CV.Value, val2)
					assert.Equal(t, rt1.GetDatabase().EncodedSourceID, doc.HLV.SourceID)
					assert.Equal(t, doc.Cas, doc.HLV.Version)
				}

				assert.Nil(t, doc.Body(ctx)[db.BodyAttachments], "_attachments property should not be in resolved doc body")

				assert.Len(t, doc.Attachments(), test.expectedAttachments, "mismatch in expected number of attachments in sync data of resolved doc, %#+v", doc.Attachments())
				for attName, att := range doc.Attachments() {
					attMap := att.(map[string]any)
					assert.Equal(t, true, attMap["stub"].(bool), "attachment %q should be a stub", attName)
					assert.NotEmpty(t, attMap["digest"].(string), "attachment %q should have digest", attName)
					assert.True(t, attMap["revpos"].(float64) >= 1, "attachment %q revpos should be at least 1", attName)
					assert.True(t, attMap["length"].(float64) >= 1, "attachment %q length should be at least 1 byte", attName)
				}
			})
		}
	})
}

// TestActiveReplicatorPullFromCheckpoint:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt2 which can be pulled by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt2.
//   - Starts the pull replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPullFromCheckpoint(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT2DocsInitial = 13 // 2 batches of changes
		numRT2DocsTotal   = 24 // 2 more batches
		username          = "alice"
	)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		// Create first batch of docs
		docIDPrefix := rest.SafeDocumentName(t, t.Name()) + "rt2doc"
		for i := range numRT2DocsInitial {
			resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
			rest.RequireStatus(t, resp, http.StatusCreated)
		}
		ctx1 := rt1.Context()

		arConfig := db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       changesBatchSize,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

		require.NoError(t, ar.Start(ctx1))

		// wait for all of the documents originally written to rt2 to arrive at rt1
		changesResults := rt1.WaitForChanges(numRT2DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
		docIDsSeen := make(map[string]bool, numRT2DocsInitial)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}
		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		for i := range numRT2DocsInitial {
			docID := fmt.Sprintf("%s%d", docIDPrefix, i)
			assert.True(t, docIDsSeen[docID])

			doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			assert.NoError(t, err)

			body, err := doc.GetDeepMutableBody()
			require.NoError(t, err)
			assert.Equal(t, "rt2", body["source"])
		}

		// one _changes from seq:0 with initial number of docs sent
		numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

		pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

		// rev assertions
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, startNumRevsSentTotal+numRT2DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, numRT2DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, numRT2DocsInitial)

		// checkpoint assertions
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)

		// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, ar.Stop())

		// Second batch of docs
		for i := numRT2DocsInitial; i < numRT2DocsTotal; i++ {
			resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
			rest.RequireStatus(t, resp, http.StatusCreated)
		}

		// Create a new replicator using the same config, which should use the checkpoint set from the first.
		ar, err = db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, ar.Stop())
		}()
		require.NoError(t, ar.Start(ctx1))

		// new replicator - new checkpointer
		pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

		// wait for all of the documents originally written to rt2 to arrive at rt1
		changesResults = rt1.WaitForChanges(numRT2DocsTotal, "/{{.keyspace}}/_changes?since=0", "", true)

		docIDsSeen = make(map[string]bool, numRT2DocsTotal)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}

		rt1collection, rt1ctx = rt1.GetSingleTestDatabaseCollection()
		for i := range numRT2DocsTotal {
			docID := fmt.Sprintf("%s%d", docIDPrefix, i)
			assert.True(t, docIDsSeen[docID])

			doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			assert.NoError(t, err)

			body, err := doc.GetDeepMutableBody()
			require.NoError(t, err)
			assert.Equal(t, "rt2", body["source"])
		}

		// Make sure we've not started any more since:0 replications on rt2 since the first one
		endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

		// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, startNumRevsSentTotal+numRT2DocsTotal)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, numRT2DocsTotal-numRT2DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, numRT2DocsTotal-numRT2DocsInitial)

		// assert the second active replicator stats
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointMissCount)

		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
	})
}

// TestActiveReplicatorPullFromCheckpointIgnored:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates identical documents on rt1 and rt2.
//   - Starts a pull replication to ensure that even ignored revisions are checkpointed.
func TestActiveReplicatorPullFromCheckpointIgnored(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT2DocsInitial = 13 // 2 batches of changes
		numRT2DocsTotal   = 24 // 2 more batches
	)

	sgrRunner := rest.NewSGRTestRunner(t)
	// This test is not applicable for > 3 protocol versions given the CV's each side will be generated differently
	// and thus pull replication will request changes.
	sgrRunner.RunSubprotocolV3(func(t *testing.T) {
		// Passive
		rt2 := rest.NewRestTester(t,
			&rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
			})
		defer rt2.Close()
		const username = "alice"

		rt2.CreateUser(username, []string{username})

		// Active
		rt1 := rest.NewRestTester(t, nil)
		defer rt1.Close()
		ctx1 := rt1.Context()

		// Create first batch of docs
		docIDPrefix := rest.SafeDocumentName(t, t.Name()) + "doc"
		for i := range numRT2DocsInitial {
			rt1Version := rt1.PutDoc(fmt.Sprintf("%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
			rt2Version := rt2.PutDoc(fmt.Sprintf("%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
			rest.RequireDocRevTreeEqual(t, rt1Version, rt2Version)
		}

		// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
		srv := httptest.NewServer(rt2.TestPublicHandler())
		defer srv.Close()

		arConfig := db.ActiveReplicatorConfig{
			ID:          t.Name(),
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       changesBatchSize,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()

		assert.NoError(t, ar.Start(ctx1))

		pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

		base.RequireWaitForStat(t, func() int64 {
			return pullCheckpointer.Stats().AlreadyKnownSequenceCount
		}, numRT2DocsInitial)

		// wait for all of the documents originally written to rt2 to arrive at rt1
		changesResults := rt1.WaitForChanges(numRT2DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
		docIDsSeen := make(map[string]bool, numRT2DocsInitial)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}
		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		for i := range numRT2DocsInitial {
			docID := fmt.Sprintf("%s%d", docIDPrefix, i)
			assert.True(t, docIDsSeen[docID])

			_, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			assert.NoError(t, err)
		}

		// one _changes from seq:0 with initial number of docs sent
		numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

		// rev assertions
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, 0)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, 0)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, 0)

		// checkpoint assertions
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)

		// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

		assert.NoError(t, ar.Stop())

		// Second batch of docs
		for i := numRT2DocsInitial; i < numRT2DocsTotal; i++ {
			rt1Version := rt1.PutDoc(fmt.Sprintf("%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
			rt2Version := rt2.PutDoc(fmt.Sprintf("%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
			// above docs CV's won't be equal, revIDs will though
			rest.RequireDocRevTreeEqual(t, rt1Version, rt2Version)
		}

		// Create a new replicator using the same config, which should use the checkpoint set from the first.
		ar, err = db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()
		assert.NoError(t, ar.Start(ctx1))

		// new replicator - new checkpointer
		pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

		base.RequireWaitForStat(t, func() int64 {
			return pullCheckpointer.Stats().AlreadyKnownSequenceCount
		}, numRT2DocsTotal-numRT2DocsInitial)

		// Make sure we've not started any more since:0 replications on rt2 since the first one
		endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

		// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, 0)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, 0)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, 0)

		// assert the second active replicator stats
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointMissCount)

		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
	})
}

// TestActiveReplicatorPullOneshot:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullOneshot(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

	const username = "alice"

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})

		docID := rest.SafeDocumentName(t, t.Name()) + "rt2doc1"
		rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
		ctx1 := rt1.Context()

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		assert.Equal(t, "", ar.GetStatus(ctx1).LastSeqPull)

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
	})
}

// TestActiveReplicatorPushBasic:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which can be pushed by the replicator.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasic(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	const username = "alice"

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		ctx1 := rt1.Context()

		docID := rest.SafeDocumentName(t, t.Name()) + "rt1doc1"
		version := rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		localDoc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		assert.Equal(t, "", ar.GetStatus(ctx1).LastSeqPush)

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt1 to arrive at rt2
		sgrRunner.WaitForVersion(docID, rt2, version)

		rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
		doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])

		assert.Equal(t, strconv.FormatUint(localDoc.Sequence, 10), ar.GetStatus(ctx1).LastSeqPush)
	})
}

// TestActiveReplicatorPushAttachments:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document with an attachment on rt1 which can be pushed by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pushing changes to rt2.
//   - Creates a second doc which references the same attachment.
func TestActiveReplicatorPushAttachments(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	const username = "alice"

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})

		ctx1 := rt1.Context()
		attachment := `"_attachments":{"hi.txt":{"data":"aGk=","content_type":"text/plain"}}`

		docID := rest.SafeDocumentName(t, t.Name()+"rt1doc1")
		version := rt1.PutDoc(docID, `{"source":"rt1","doc_num":1,`+attachment+`,"channels":["alice"]}`)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		assert.Equal(t, int64(0), ar.Push.GetStats().HandleGetAttachment.Value())

		// Start the replicator (implicit connect)
		assert.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt1 to arrive at rt2
		sgrRunner.WaitForVersion(docID, rt2, version)

		rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
		doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])
		assert.Equal(t, json.Number("1"), body["doc_num"])

		assert.Equal(t, int64(1), ar.Push.GetStats().HandleGetAttachment.Value())

		docID = rest.SafeDocumentName(t, t.Name()+"rt1doc2")
		version = rt1.PutDoc(docID, `{"source":"rt1","doc_num":2,`+attachment+`,"channels":["alice"]}`)

		// wait for the new document written to rt1 to arrive at rt2
		sgrRunner.WaitForVersion(docID, rt2, version)

		doc2, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err = doc2.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])
		assert.Equal(t, json.Number("2"), body["doc_num"])

		// When targeting a Hydrogen node that supports proveAttachments, we typically end up sending
		// the attachment only once. However, targeting a Lithium node sends the attachment twice like
		// the pre-Hydrogen node, GetAttachment would be 2. The reason is that a Hydrogen node uses a
		// new storage model for attachment storage and retrieval.
		assert.Equal(t, int64(2), ar.Push.GetStats().HandleGetAttachment.Value())
		assert.Equal(t, int64(0), ar.Push.GetStats().HandleProveAttachment.Value())
	})
}

// TestActiveReplicatorPushFromCheckpoint:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt1 which can be pushed by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt1.
//   - Starts the push replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPushFromCheckpoint(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT1DocsInitial = 13 // 2 batches of changes
		numRT1DocsTotal   = 24 // 2 more batches
		username          = "alice"
	)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		ctx1 := rt1.Context()

		// Create first batch of docs
		docIDPrefix := rest.SafeDocumentName(t, t.Name()) + "rt2doc"
		for i := range numRT1DocsInitial {
			rt1.PutDoc(fmt.Sprintf("%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		}

		arConfig := db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       changesBatchSize,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		stats, err := base.SyncGatewayStats.NewDBStats(t.Name()+"1", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)
		arConfig.ReplicationStatsMap = dbstats
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		startNumRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()

		require.NoError(t, ar.Start(ctx1))

		// wait for all of the documents originally written to rt1 to arrive at rt2
		changesResults := rt2.WaitForChanges(numRT1DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
		docIDsSeen := make(map[string]bool, numRT1DocsInitial)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}
		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		for i := range numRT1DocsInitial {
			docID := fmt.Sprintf("%s%d", docIDPrefix, i)
			assert.True(t, docIDsSeen[docID])

			doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			assert.NoError(t, err)

			body, err := doc.GetDeepMutableBody()
			require.NoError(t, err)
			assert.Equal(t, "rt1", body["source"])
		}

		// one _changes from seq:0 with initial number of docs sent
		numChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

		pushCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer

		// rev assertions
		base.RequireWaitForStat(t, ar.Push.GetStats().SendRevCount.Value, startNumRevsSentTotal+numRT1DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ProcessedSequenceCount }, numRT1DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ExpectedSequenceCount }, numRT1DocsInitial)

		// checkpoint assertions
		assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointMissCount)

		assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
		require.NoError(t, ar.Stop())

		// Second batch of docs
		for i := numRT1DocsInitial; i < numRT1DocsTotal; i++ {
			rt1.PutDoc(fmt.Sprintf("%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		}

		// Create a new replicator using the same config, which should use the checkpoint set from the first.
		stats, err = base.SyncGatewayStats.NewDBStats(t.Name()+"2", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err = stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)
		arConfig.ReplicationStatsMap = dbstats
		ar, err = db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)
		require.NoError(t, ar.Start(ctx1))
		defer func() { assert.NoError(t, ar.Stop()) }()

		// new replicator - new checkpointer
		pushCheckpointer = ar.Push.GetSingleCollection(t).Checkpointer

		// wait for all of the documents originally written to rt1 to arrive at rt2
		changesResults = rt2.WaitForChanges(numRT1DocsTotal, "/{{.keyspace}}/_changes?since=0", "", true)

		docIDsSeen = make(map[string]bool, numRT1DocsTotal)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}

		rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
		for i := range numRT1DocsTotal {
			docID := fmt.Sprintf("%s%d", docIDPrefix, i)
			assert.True(t, docIDsSeen[docID])

			doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
			assert.NoError(t, err)

			body, err := doc.GetDeepMutableBody()
			require.NoError(t, err)
			assert.Equal(t, "rt1", body["source"])
		}

		// Make sure we've not started any more since:0 replications on rt1 since the first one
		endNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

		// make sure the new replicator has only sent new mutations
		base.RequireWaitForStat(t, ar.Push.GetStats().SendRevCount.Value, numRT1DocsTotal-numRT1DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ProcessedSequenceCount }, numRT1DocsTotal-numRT1DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ExpectedSequenceCount }, numRT1DocsTotal-numRT1DocsInitial)

		// assert the second active replicator stats
		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointMissCount)

		assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
		pushCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)
	})
}

// TestActiveReplicatorEdgeCheckpointNameCollisions:
//   - Starts 3 RestTesters, one to create documents, and two running pull replications from the central cluster
//   - Replicators running on the edges have identical IDs (e.g. edge-repl)
func TestActiveReplicatorEdgeCheckpointNameCollisions(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 3)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT1DocsInitial = 13 // 2 batches of changes
		username          = "alice"
	)
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		_, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		// Create first batch of docs
		docIDPrefix := rest.SafeDocumentName(t, t.Name()) + "rt1doc"
		for i := range numRT1DocsInitial {
			rt2.PutDoc(fmt.Sprintf("%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		}

		// Edge 1
		edge1Bucket := base.GetTestBucket(t)
		edge1 := rest.NewRestTester(t,
			&rest.RestTesterConfig{
				CustomTestBucket: edge1Bucket,
			})
		defer edge1.Close()
		ctx1 := edge1.Context()

		arConfig := db.ActiveReplicatorConfig{
			ID:          "edge-repl",
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: edge1.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       changesBatchSize,
			CollectionsEnabled:     !rt2.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}
		arConfig.SetCheckpointPrefix(t, "cluster1:")

		// Create the first active replicator to pull from seq:0
		stats, err := base.SyncGatewayStats.NewDBStats(t.Name()+"edge1", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)
		arConfig.ReplicationStatsMap = dbstats
		edge1Replicator, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		startNumRevsHandledTotal := edge1Replicator.Pull.GetStats().HandleRevCount.Value()

		require.NoError(t, edge1Replicator.Start(ctx1))

		// wait for all of the documents originally written to rt1 to arrive at edge1
		changesResults := edge1.WaitForChanges(numRT1DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
		edge1LastSeq := changesResults.Last_Seq
		require.Len(t, changesResults.Results, numRT1DocsInitial)
		docIDsSeen := make(map[string]bool, numRT1DocsInitial)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}
		edge1collection, edge1ctx := edge1.GetSingleTestDatabaseCollection()
		for i := range numRT1DocsInitial {
			docID := fmt.Sprintf("%s%d", docIDPrefix, i)
			assert.True(t, docIDsSeen[docID])

			doc, err := edge1collection.GetDocument(edge1ctx, docID, db.DocUnmarshalAll)
			assert.NoError(t, err)

			body, err := doc.GetDeepMutableBody()
			require.NoError(t, err)
			assert.Equal(t, "rt1", body["source"])
		}

		edge1PullCheckpointer := edge1Replicator.Pull.GetSingleCollection(t).Checkpointer
		edge1PullCheckpointer.CheckpointNow()

		// one _changes from seq:0 with initial number of docs sent
		numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

		// rev assertions
		base.RequireWaitForStat(t, edge1Replicator.Pull.GetStats().HandleRevCount.Value, startNumRevsHandledTotal+numRT1DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return edge1PullCheckpointer.Stats().ProcessedSequenceCount }, numRT1DocsInitial)
		base.RequireWaitForStat(t, func() int64 { return edge1PullCheckpointer.Stats().ExpectedSequenceCount }, numRT1DocsInitial)

		// checkpoint assertions
		assert.Equal(t, int64(0), edge1PullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), edge1PullCheckpointer.Stats().GetCheckpointMissCount)

		assert.Equal(t, int64(1), edge1PullCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, edge1Replicator.Stop())

		// Edge 2
		edge2Bucket := base.GetTestBucket(t)
		edge2 := rest.NewRestTester(t,
			&rest.RestTesterConfig{
				CustomTestBucket: edge2Bucket,
			})
		defer edge2.Close()
		ctx2 := edge2.Context()

		// Create a new replicator using the same ID, which should NOT use the checkpoint set by the first edge.
		stats, err = base.SyncGatewayStats.NewDBStats(t.Name()+"edge2", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err = stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)
		arConfig.ReplicationStatsMap = dbstats
		arConfig.ActiveDB = &db.Database{
			DatabaseContext: edge2.GetDatabase(),
		}
		arConfig.SetCheckpointPrefix(t, "cluster2:")
		edge2Replicator, err := db.NewActiveReplicator(ctx2, &arConfig)
		require.NoError(t, err)
		require.NoError(t, edge2Replicator.Start(ctx2))

		changesResults = edge2.WaitForChanges(numRT1DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)

		edge2PullCheckpointer := edge2Replicator.Pull.GetSingleCollection(t).Checkpointer
		edge2PullCheckpointer.CheckpointNow()

		// make sure that edge 2 didn't use a checkpoint
		assert.Equal(t, int64(0), edge2PullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), edge2PullCheckpointer.Stats().GetCheckpointMissCount)

		assert.Equal(t, int64(1), edge2PullCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, edge2Replicator.Stop())

		resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, numRT1DocsInitial), `{"source":"rt1","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		rt2.WaitForPendingChanges()

		// run a replicator on edge1 again to make sure that edge2 didn't blow away its checkpoint
		stats, err = base.SyncGatewayStats.NewDBStats(t.Name()+"edge1", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err = stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)
		arConfig.ReplicationStatsMap = dbstats
		arConfig.ActiveDB = &db.Database{
			DatabaseContext: edge1.GetDatabase(),
		}
		arConfig.SetCheckpointPrefix(t, "cluster1:")

		edge1Replicator2, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)
		require.NoError(t, edge1Replicator2.Start(ctx1))

		changesResults = edge1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", edge1LastSeq), "", true)
		changesResults.RequireDocIDs(t, []string{fmt.Sprintf("%s%d", docIDPrefix, numRT1DocsInitial)})

		edge1Checkpointer2 := edge1Replicator2.Pull.GetSingleCollection(t).Checkpointer
		edge1Checkpointer2.CheckpointNow()
		if rt2.GetDatabase().OnlyDefaultCollection() {
			assert.Equal(t, int64(1), edge1Checkpointer2.Stats().GetCheckpointHitCount)
			assert.Equal(t, int64(0), edge1Checkpointer2.Stats().GetCheckpointMissCount)
		}

		assert.Equal(t, int64(1), edge1Checkpointer2.Stats().SetCheckpointCount)

		require.NoError(t, edge1Replicator2.Stop())
	})
}

// TestActiveReplicatorPushOneshot:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which can be pushed by the replicator.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushOneshot(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeers(t)

		docID := rest.SafeDocumentName(t, t.Name()) + "rt1doc1"
		version := rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		localDoc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		ctx1 := rt1.Context()
		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		assert.Equal(t, "", ar.GetStatus(ctx1).LastSeqPush)

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		rt1.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

		rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollectionWithUser()
		doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)

		sgrRunner.WaitForVersion(docID, rt2, version)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])

		assert.Equal(t, strconv.FormatUint(localDoc.Sequence, 10), ar.GetStatus(ctx1).LastSeqPush)
	})
}

// TestActiveReplicatorPullTombstone:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
//   - Deletes the document in rt2, and waits for the tombstone to get to rt1.
func TestActiveReplicatorPullTombstone(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})

		docID := rest.SafeDocumentName(t, t.Name()) + "rt2doc1"
		version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
		ctx1 := rt1.Context()

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt2 to arrive at rt1
		sgrRunner.WaitForVersion(docID, rt1, version)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])

		// Tombstone the doc in rt2
		deletedVersion := rt2.DeleteDoc(docID, version)

		// wait for the tombstone written to rt2 to arrive at rt1
		sgrRunner.WaitForTombstone(docID, rt1, deletedVersion)
		doc, err = rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		assert.True(t, doc.IsDeleted())
	})
}

// TestActiveReplicatorPullPurgeOnRemoval:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
//   - Drops the document out of the channel so the replicator in rt1 pulls a _removed revision.
func TestActiveReplicatorPullPurgeOnRemoval(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyReplicate)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		docID := rest.SafeDocumentName(t, t.Name()+"rt2doc1")
		version := rt2.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)

		ctx1 := rt1.Context()

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: userDBURL(rt2, username),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			Continuous:             true,
			PurgeOnRemoval:         true,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt2 to arrive at rt1
		sgrRunner.WaitForVersion(docID, rt1, version)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])

		_ = rt2.UpdateDoc(docID, version, `{"source":"rt2","channels":["bob"]}`)

		// wait for the channel removal written to rt2 to arrive at rt1 - we can't monitor _changes, because we've purged, not removed. But we can monitor the associated stat.
		base.RequireWaitForStat(t, func() int64 {
			stats := ar.GetStatus(ctx1)
			return stats.DocsPurged
		}, 1)

		doc, err = rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.Error(t, err)
		assert.True(t, base.IsDocNotFoundError(err), "Error returned wasn't a DocNotFound error")
		assert.Nil(t, doc)
	})
}

// TestActiveReplicatorPullConflict:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullConflict(t *testing.T) {
	base.LongRunningTest(t)

	// scenarios
	conflictResolutionTests := []struct {
		name                      string
		localRevisionBody         string
		localVersion              rest.DocVersion
		remoteRevisionBody        string
		remoteVersion             rest.DocVersion
		conflictResolver          string
		expectedLocalBody         string
		expectedLocalVersion      rest.DocVersion
		expectedTombstonedVersion rest.DocVersion
		expectedResolutionType    db.ConflictResolutionType
		skipActiveLeafAssertion   bool
		skipBodyAssertion         bool
		newCVGenerated            bool
		localWinsHLV              bool
		tombstoneCase             bool
	}{
		{
			name:                   "remoteWins",
			localRevisionBody:      `{"source": "local"}`,
			localVersion:           rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody:     `{"source": "remote"}`,
			remoteVersion:          rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver:       `function(conflict) {return conflict.RemoteDocument;}`,
			expectedLocalBody:      `{"source": "remote"}`,
			expectedLocalVersion:   rest.NewDocVersionFromFakeRev("1-b"),
			expectedResolutionType: db.ConflictResolutionRemote,
		},
		{
			name:               "merge",
			localRevisionBody:  `{"source": "local"}`,
			localVersion:       rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody: `{"source": "remote"}`,
			remoteVersion:      rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver: `function(conflict) {
					var mergedDoc = new Object();
					mergedDoc.source = "merged";
					return mergedDoc;
				}`,
			expectedLocalBody:      `{"source": "merged"}`,
			expectedLocalVersion:   rest.NewDocVersionFromFakeRev(db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"merged"}`))), // rev for merged body, with parent 1-b
			expectedResolutionType: db.ConflictResolutionMerge,
			newCVGenerated:         true,
		},
		{
			name:                   "localWins",
			localRevisionBody:      `{"source": "local"}`,
			localVersion:           rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody:     `{"source": "remote"}`,
			remoteVersion:          rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver:       `function(conflict) {return conflict.LocalDocument;}`,
			expectedLocalBody:      `{"source": "local"}`,
			expectedLocalVersion:   rest.NewDocVersionFromFakeRev(db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"local"}`))), // rev for local body, transposed under parent 1-b
			expectedResolutionType: db.ConflictResolutionLocal,
			localWinsHLV:           true,
		},
		{
			name:                    "twoTombstonesRemoteWin",
			localRevisionBody:       `{"_deleted": true, "source": "local"}`,
			localVersion:            rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody:      `{"_deleted": true, "source": "remote"}`,
			remoteVersion:           rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver:        `function(conflict){}`,
			expectedLocalBody:       `{"source": "remote"}`,
			expectedLocalVersion:    rest.NewDocVersionFromFakeRev("1-b"),
			skipActiveLeafAssertion: true,
			skipBodyAssertion:       base.TestUseXattrs(),
			tombstoneCase:           true,
		},
		{
			name:                    "twoTombstonesLocalWin",
			localRevisionBody:       `{"_deleted": true, "source": "local"}`,
			localVersion:            rest.NewDocVersionFromFakeRev("1-b"),
			remoteRevisionBody:      `{"_deleted": true, "source": "remote"}`,
			remoteVersion:           rest.NewDocVersionFromFakeRev("1-a"),
			conflictResolver:        `function(conflict){}`,
			expectedLocalBody:       `{"source": "local"}`,
			expectedLocalVersion:    rest.NewDocVersionFromFakeRev("1-b"),
			skipActiveLeafAssertion: true,
			skipBodyAssertion:       base.TestUseXattrs(),
			tombstoneCase:           true,
		},
	}

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range conflictResolutionTests {
			t.Run(test.name, func(t *testing.T) {
				base.RequireNumTestBuckets(t, 2)
				base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD)

				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)

				// Create revision on rt2 (remote)
				docID := test.name
				rt2Version := rt2.PutNewEditsFalse(docID, test.remoteVersion, rest.EmptyDocVersion(), test.remoteRevisionBody)
				rest.RequireDocRevTreeEqual(t, test.remoteVersion, *rt2Version)

				ctx1 := rt1.Context()

				// Create revision on rt1 (local)
				rt1version := rt1.PutNewEditsFalse(docID, test.localVersion, rest.EmptyDocVersion(), test.localRevisionBody)
				rest.RequireDocRevTreeEqual(t, test.localVersion, *rt1version)

				var rt1CVVersion rest.DocVersion
				if !test.tombstoneCase {
					rt1CVVersion, _ = rt1.GetDoc(docID)
				}

				customConflictResolver, err := db.NewCustomConflictResolver(ctx1, test.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)
				stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
				require.NoError(t, err)
				replicationStats, err := stats.DBReplicatorStats(t.Name())
				require.NoError(t, err)

				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          rest.SafeDocumentName(t, t.Name()),
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ConflictResolverFunc:       customConflictResolver,
					ConflictResolverFuncForHLV: customConflictResolver,
					Continuous:                 true,
					ReplicationStatsMap:        replicationStats,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
				})
				require.NoError(t, err)
				defer func() { require.NoError(t, ar.Stop()) }()

				// Start the replicator (implicit connect)
				require.NoError(t, ar.Start(ctx1))

				require.EventuallyWithTf(t, func(c *assert.CollectT) {
					assert.Equal(c, 1, int(ar.GetStatus(ctx1).DocsRead))
				}, 10*time.Second, 100*time.Millisecond, "Expecting DocsRead == 1: %+v", ar.GetStatus(ctx1))

				switch test.expectedResolutionType {
				case db.ConflictResolutionLocal:
					assert.Equal(t, 1, int(replicationStats.ConflictResolvedLocalCount.Value()))
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedMergedCount.Value()))
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedRemoteCount.Value()))
				case db.ConflictResolutionMerge:
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedLocalCount.Value()))
					assert.Equal(t, 1, int(replicationStats.ConflictResolvedMergedCount.Value()))
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedRemoteCount.Value()))
				case db.ConflictResolutionRemote:
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedLocalCount.Value()))
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedMergedCount.Value()))
					assert.Equal(t, 1, int(replicationStats.ConflictResolvedRemoteCount.Value()))
				default:
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedLocalCount.Value()))
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedMergedCount.Value()))
					assert.Equal(t, 0, int(replicationStats.ConflictResolvedRemoteCount.Value()))
				}
				// wait for the document originally written to rt2 to arrive at rt1.  Should end up as winner under default conflict resolution

				changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
				assert.Equal(t, docID, changesResults.Results[0].ID)
				rest.RequireChangeRev(t, test.expectedLocalVersion, changesResults.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)
				t.Logf("Changes response is %+v", changesResults)

				rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
				doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				var expValue uint64
				if sgrRunner.IsV4Protocol() {
					if test.localWinsHLV {
						expValue = rt1CVVersion.CV.Value
					} else {
						expValue = doc.Cas
					}
					if test.newCVGenerated || test.localWinsHLV {
						test.expectedLocalVersion.CV = db.Version{
							SourceID: rt1.GetDatabase().EncodedSourceID,
							Value:    expValue,
						}
					} else {
						test.expectedLocalVersion.CV = rt2Version.CV
					}
				}
				if !test.skipBodyAssertion {
					sgrRunner.WaitForVersion(docID, rt1, test.expectedLocalVersion)
					// This is skipped for tombstone tests running with xattr as xattr tombstones don't have a body to assert
					// against
					requireBodyEqual(t, test.expectedLocalBody, doc)
				} else {
					sgrRunner.WaitForTombstone(docID, rt1, test.expectedLocalVersion)
				}
				t.Logf("Doc %s is %+v", docID, doc)
				for revID, revInfo := range doc.SyncData.History {
					t.Logf("doc revision [%s]: %+v", revID, revInfo)
				}

				if !test.skipActiveLeafAssertion {
					// Validate only one active leaf node remains after conflict resolution, and that all parents
					// of leaves have empty bodies
					activeCount := 0
					for _, revID := range doc.SyncData.History.GetLeaves() {
						revInfo, ok := doc.SyncData.History[revID]
						require.True(t, ok)
						if !revInfo.Deleted {
							activeCount++
						}
						if revInfo.Parent != "" {
							parentRevInfo, ok := doc.SyncData.History[revInfo.Parent]
							require.True(t, ok)
							assert.True(t, parentRevInfo.Body == nil)
						}
					}
					assert.Equal(t, 1, activeCount)
				}
			})
		}
	})
}

// TestActiveReplicatorPushAndPullConflict:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pushAndPull from rt2.
//   - verifies expected conflict resolution, and that expected result is replicated to both peers
func TestActiveReplicatorPushAndPullConflict(t *testing.T) {
	base.LongRunningTest(t)

	type conflictWinner int
	const (
		local conflictWinner = iota
		remote
		merge
	)
	// scenarios
	conflictResolutionTests := []struct {
		name                  string
		localRevisionBody     string
		localVersion          rest.DocVersion
		remoteRevisionBody    string
		remoteVersion         rest.DocVersion
		commonAncestorVersion *rest.DocVersion
		conflictResolver      string
		expectedBody          string
		expectedVersion       rest.DocVersion
		expectedPushResolved  bool
		newCVGenerated        bool
		winner                conflictWinner
	}{
		{
			name:                 "remoteWins",
			localRevisionBody:    `{"source": "local"}`,
			localVersion:         rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody:   `{"source": "remote"}`,
			remoteVersion:        rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver:     `function(conflict) {return conflict.RemoteDocument;}`,
			expectedBody:         `{"source": "remote"}`,
			expectedVersion:      rest.NewDocVersionFromFakeRev("1-b"),
			expectedPushResolved: false,
			winner:               remote,
		},
		{
			name:               "merge",
			localRevisionBody:  `{"source": "local"}`,
			localVersion:       rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody: `{"source": "remote"}`,
			remoteVersion:      rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver: `function(conflict) {
							var mergedDoc = new Object();
							mergedDoc.source = "merged";
							return mergedDoc;
						}`,
			expectedBody:         `{"source": "merged"}`,
			expectedVersion:      rest.NewDocVersionFromFakeRev(db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"merged"}`))), // rev for merged body, with parent 1-b
			expectedPushResolved: true,
			winner:               merge,
		},
		{
			name:                 "localWins",
			localRevisionBody:    `{"source": "local"}`,
			localVersion:         rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody:   `{"source": "remote"}`,
			remoteVersion:        rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver:     `function(conflict) {return conflict.LocalDocument;}`,
			expectedBody:         `{"source": "local"}`,
			expectedVersion:      rest.NewDocVersionFromFakeRev(db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"local"}`))), // rev for local body, transposed under parent 1-b
			expectedPushResolved: true,
			winner:               local,
		},
		{
			name:                  "localWinsRemoteTombstone",
			localRevisionBody:     `{"source": "local"}`,
			localVersion:          rest.NewDocVersionFromFakeRev("2-a"),
			remoteRevisionBody:    `{"_deleted": true}`,
			remoteVersion:         rest.NewDocVersionFromFakeRev("2-b"),
			commonAncestorVersion: base.Ptr(rest.NewDocVersionFromFakeRev("1-a")),
			conflictResolver:      `function(conflict) {return conflict.LocalDocument;}`,
			expectedBody:          `{"source": "local"}`,
			expectedVersion:       rest.NewDocVersionFromFakeRev(db.CreateRevIDWithBytes(3, "2-b", []byte(`{"source":"local"}`))), // rev for local body, transposed under parent 2-b
			expectedPushResolved:  true,
			winner:                local,
		},
	}

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range conflictResolutionTests {
			t.Run(test.name, func(t *testing.T) {
				base.RequireNumTestBuckets(t, 2)
				base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeySyncMsg, base.KeyChanges, base.KeyCRUD)

				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				remoteURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)

				// Create revision on rt2 (remote)
				docID := test.name
				if test.commonAncestorVersion != nil {
					t.Logf("Creating common ancestor revision on rt2")
					rt2Version := rt2.PutNewEditsFalse(docID, *test.commonAncestorVersion, nil, test.remoteRevisionBody)
					rest.RequireDocRevTreeEqual(t, *test.commonAncestorVersion, *rt2Version)
				}

				t.Logf("Creating remote revision on rt2")
				rt2Version := rt2.PutNewEditsFalse(docID, test.remoteVersion, test.commonAncestorVersion, test.remoteRevisionBody)
				rest.RequireDocRevTreeEqual(t, test.remoteVersion, *rt2Version)

				rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
				remoteDoc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalSync)
				require.NoError(t, err)

				ctx1 := rt1.Context()
				// Create revision on rt1 (local)
				if test.commonAncestorVersion != nil {
					t.Logf("Creating common ancestor revision on rt1")
					rt1version := rt1.PutNewEditsFalse(docID, *test.commonAncestorVersion, nil, test.localRevisionBody)
					rest.RequireDocRevTreeEqual(t, *test.commonAncestorVersion, *rt1version)
				}

				t.Logf("Creating local revision on rt1")
				rt1Version := rt1.PutNewEditsFalse(docID, test.localVersion, test.commonAncestorVersion, test.localRevisionBody)
				rest.RequireDocRevTreeEqual(t, test.localVersion, *rt1Version)

				rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
				localDoc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalSync)
				require.NoError(t, err)

				customConflictResolver, err := db.NewCustomConflictResolver(ctx1, test.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)

				replicationID := rest.SafeDocumentName(t, t.Name())
				stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
				require.NoError(t, err)
				dbstats, err := stats.DBReplicatorStats(replicationID)
				require.NoError(t, err)

				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          replicationID,
					Direction:   db.ActiveReplicatorTypePushAndPull,
					RemoteDBURL: remoteURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ConflictResolverFunc:       customConflictResolver,
					ConflictResolverFuncForHLV: customConflictResolver,
					Continuous:                 true,
					ReplicationStatsMap:        dbstats,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
				})
				require.NoError(t, err)
				defer func() { assert.NoError(t, ar.Stop()) }()

				// Start the replicator (implicit connect)
				t.Logf("Starting replicator")
				require.NoError(t, ar.Start(ctx1))
				t.Logf("Replicator started")

				// wait for both push and pull to complete:
				// - the document originally written to rt2 to arrive at rt1
				// - the document originally written to rt1 to get a conflict on push
				// - if applicable: the resolved rev to be pushed up to rt2
				require.EventuallyWithTf(t, func(c *assert.CollectT) {
					status := ar.GetStatus(ctx1)
					assert.Equal(c, 1, int(status.PullReplicationStatus.DocsRead))
					assert.Equal(c, 1, int(status.PushReplicationStatus.DocWriteConflict))
					if test.expectedPushResolved {
						assert.Equal(c, 1, int(status.PushReplicationStatus.DocsWritten))
					}
				}, 10*time.Second, 100*time.Millisecond, "Expected both push and pull to be completed: %+v", ar.GetStatus(ctx1))
				t.Logf("========================Replication should be done, checking with changes")

				// Validate results on the local (rt1)
				changesResults := rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", localDoc.Sequence), "", true)
				assert.Equal(t, docID, changesResults.Results[0].ID)
				rest.RequireChangeRev(t, test.expectedVersion, changesResults.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)
				t.Logf("Changes response is %+v", changesResults)

				rawDocResponse := rt1.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, "")
				t.Logf("Raw response: %s", rawDocResponse.Body.Bytes())

				docResponse := rt1.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID, "")
				t.Logf("Non-raw response: %s", docResponse.Body.Bytes())

				doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				switch test.winner {
				case merge:
					test.expectedVersion.CV = db.Version{
						SourceID: rt1.GetDatabase().EncodedSourceID,
						Value:    doc.Cas,
					}
				case local:
					test.expectedVersion.CV = rt1Version.CV
				case remote:
					test.expectedVersion.CV = rt2Version.CV
				}
				sgrRunner.WaitForVersion(docID, rt1, test.expectedVersion)
				requireBodyEqual(t, test.expectedBody, doc)
				t.Logf("Doc %s is %+v", docID, doc)
				t.Logf("Doc %s attachments are %+v", docID, doc.Attachments())
				for revID, revInfo := range doc.SyncData.History {
					t.Logf("doc revision [%s]: %+v", revID, revInfo)
				}

				// Validate only one active leaf node remains after conflict resolution, and that all parents
				// of leaves have empty bodies
				activeCount := 0
				for _, revID := range doc.SyncData.History.GetLeaves() {
					revInfo, ok := doc.SyncData.History[revID]
					require.True(t, ok)
					if !revInfo.Deleted {
						activeCount++
					}
					if revInfo.Parent != "" {
						parentRevInfo, ok := doc.SyncData.History[revInfo.Parent]
						require.True(t, ok)
						assert.True(t, parentRevInfo.Body == nil)
					}
				}
				assert.Equal(t, 1, activeCount)

				// Validate results on the remote (rt2)
				rt2Since := remoteDoc.Sequence
				if test.expectedVersion.RevTreeID == test.remoteVersion.RevTreeID {
					// no changes should have been pushed back up to rt2, because this rev won.
					rt2Since = 0
				}
				changesResults = rt2.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", rt2Since), "", true)
				assert.Equal(t, docID, changesResults.Results[0].ID)
				rest.RequireChangeRev(t, test.expectedVersion, changesResults.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)
				t.Logf("Changes response is %+v", changesResults)

				doc, err = rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				sgrRunner.WaitForVersion(docID, rt2, test.expectedVersion)
				requireBodyEqual(t, test.expectedBody, doc)
				t.Logf("Remote Doc %s is %+v", docID, doc)
				t.Logf("Remote Doc %s attachments are %+v", docID, doc.Attachments())
				for revID, revInfo := range doc.SyncData.History {
					t.Logf("doc revision [%s]: %+v", revID, revInfo)
				}

				// Validate only one active leaf node remains after conflict resolution, and that all parents
				// of leaves have empty bodies
				activeCount = 0
				for _, revID := range doc.SyncData.History.GetLeaves() {
					revInfo, ok := doc.SyncData.History[revID]
					require.True(t, ok)
					if !revInfo.Deleted {
						activeCount++
					}
					if revInfo.Parent != "" {
						parentRevInfo, ok := doc.SyncData.History[revInfo.Parent]
						require.True(t, ok)
						assert.True(t, parentRevInfo.Body == nil)
					}
				}
				assert.Equal(t, 1, activeCount)
			})
		}
	})
}

// TestActiveReplicatorPushBasicWithInsecureSkipVerify:
//   - Starts 2 RestTesters, one active (with InsecureSkipVerify), and one passive
//   - Creates a document on rt1 which can be pushed by the replicator to rt2.
//   - rt2 served using a self-signed TLS cert (via httptest)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasicWithInsecureSkipVerifyEnabled(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		ctx1 := rt1.Context()

		docID := rest.SafeDocumentName(t, t.Name()+"rt1doc1")
		version := rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)

		replicationID := rest.SafeDocumentName(t, t.Name())
		stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
		srv := httptest.NewTLSServer(rt2.TestPublicHandler())
		defer srv.Close()

		passiveDBURL, err := url.Parse(srv.URL + "/passivedb")
		require.NoError(t, err)

		// Add basic auth creds to target db URL
		passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: passiveDBURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			InsecureSkipVerify:     true,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt1 to arrive at rt2
		sgrRunner.WaitForVersion(docID, rt2, version)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])
	})
}

// TestActiveReplicatorPushBasicWithInsecureSkipVerifyDisabled:
//   - Starts 2 RestTesters, one active, and one passive
//   - Creates a document on rt1 which can be pushed by the replicator to rt2.
//   - rt2 served using a self-signed TLS cert (via httptest)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasicWithInsecureSkipVerifyDisabled(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		ctx1 := rt1.Context()

		docID := rest.SafeDocumentName(t, t.Name()+"rt1doc1")
		resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
		srv := httptest.NewTLSServer(rt2.TestPublicHandler())
		defer srv.Close()

		passiveDBURL, err := url.Parse(srv.URL + "/passivedb")
		require.NoError(t, err)

		// Add basic auth creds to target db URL
		passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

		replicationID := rest.SafeDocumentName(t, t.Name())
		stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: passiveDBURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			ChangesBatchSize:       200,
			InsecureSkipVerify:     false,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()

		// Start the replicator (implicit connect)
		require.Error(t, ar.Start(ctx1), "Error certificate signed by unknown authority")
	})
}

// TestActiveReplicatorRecoverFromLocalFlush:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which is pulled to rt1.
//   - Checkpoints once finished.
//   - Recreates rt1 with a new bucket (to simulate a flush).
//   - Starts the replication again, and ensures that documents are re-replicated to it.
func TestActiveReplicatorRecoverFromLocalFlush(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 3)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	// test will setup its own rest testers given we close one middle of the test
	sgrRunner.Run(func(t *testing.T) {
		// Passive
		rt2 := rest.NewRestTester(t,
			&rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
			})
		defer rt2.Close()

		rt2.CreateUser(username, []string{username})

		// Create doc on rt2
		docID := rest.SafeDocumentName(t, t.Name()) + "rt2doc"
		resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		rt2.WaitForPendingChanges()

		// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
		srv := httptest.NewServer(rt2.TestPublicHandler())
		defer srv.Close()

		// Build passiveDBURL with basic auth creds
		passiveDBURL, err := url.Parse(srv.URL + "/db")
		require.NoError(t, err)
		passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

		// Active
		rt1 := rest.NewRestTester(t, nil)
		ctx1 := rt1.Context()

		replicationID := rest.SafeDocumentName(t, t.Name())
		stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		arConfig := db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: passiveDBURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

		require.NoError(t, ar.Start(ctx1))

		// wait for document originally written to rt2 to arrive at rt1
		changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID, changesResults.Results[0].ID)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])

		// one _changes from seq:0 with initial number of docs sent
		numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

		pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

		// rev assertions
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, startNumRevsSentTotal+1)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, 1)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, 1)

		// checkpoint assertions
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)

		// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
		require.NoError(t, ar.Stop())

		// close rt1, and release the underlying bucket back to the pool.
		rt1.Close()

		// recreate rt1 with a new bucket
		rt1 = rest.NewRestTester(t, nil)
		defer rt1.Close()
		ctx1 = rt1.Context()

		// Create a new replicator using the same config, which should use the checkpoint set from the first.
		// Have to re-set ActiveDB because we recreated it with the new rt1.
		arConfig.ActiveDB = &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		}
		ar, err = db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		require.NoError(t, ar.Start(ctx1))

		// new replicator - new checkpointer
		pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

		// we pulled the remote checkpoint, but the local checkpoint wasn't there to match it.
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)

		// wait for document originally written to rt2 to arrive at rt1
		changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID, changesResults.Results[0].ID)

		rt1collection, rt1ctx = rt1.GetSingleTestDatabaseCollection()
		doc, err = rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)

		body, err = doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])

		// one _changes from seq:0 with initial number of docs sent
		endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, numChangesRequestedFromZeroTotal+1, endNumChangesRequestedFromZeroTotal)

		// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, startNumRevsSentTotal+2)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, 1)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, 1)

		// assert the second active replicator stats
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, ar.Stop())
	})
}

// TestActiveReplicatorRecoverFromRemoteFlush:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Checkpoints once finished.
//   - Recreates rt2 with a new bucket (to simulate a flush).
//   - Starts the replication again, and ensures that post-flush, documents are re-replicated to it.
func TestActiveReplicatorRecoverFromRemoteFlush(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 3)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	// test will setup its own rest testers given we close one middle of the test
	sgrRunner.Run(func(t *testing.T) {
		// Passive
		rt2 := rest.NewRestTester(t, nil)
		rt2.CreateUser(username, []string{username})

		// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
		srv := httptest.NewServer(rt2.TestPublicHandler())
		defer srv.Close()

		// Build passiveDBURL with basic auth creds
		passiveDBURL, err := url.Parse(srv.URL + "/db")
		require.NoError(t, err)
		passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

		// Active
		rt1 := rest.NewRestTester(t,
			&rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
			})
		defer rt1.Close()
		ctx1 := rt1.Context()

		// Create doc on rt1
		docID := rest.SafeDocumentName(t, t.Name()+"rt1doc")
		resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		rt1.WaitForPendingChanges()

		arConfig := db.ActiveReplicatorConfig{
			ID:          rest.SafeDocumentName(t, t.Name()),
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: passiveDBURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		stats, err := base.SyncGatewayStats.NewDBStats(t.Name()+"1", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)
		arConfig.ReplicationStatsMap = dbstats
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		// startNumRevsSentTotal := ar.Pull.GetStats().SendRevCount.Value()
		startNumRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()

		require.NoError(t, ar.Start(ctx1))

		pushCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer

		// wait for document originally written to rt1 to arrive at rt2
		changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID, changesResults.Results[0].ID)

		rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
		doc, err := rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])

		// one _changes from seq:0 with initial number of docs sent
		numChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

		// rev assertions
		base.RequireWaitForStat(t, ar.Push.GetStats().SendRevCount.Value, startNumRevsSentTotal+1)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ProcessedSequenceCount }, 1)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ExpectedSequenceCount }, 1)

		// checkpoint assertions
		assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointMissCount)

		// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
		assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
		pushCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, ar.Stop())

		// close rt2, and release the underlying bucket back to the pool.
		rt2.Close()

		// recreate rt2 with a new bucket, http server and update target URL in the replicator
		rt2 = rest.NewRestTester(t, nil)
		defer rt2.Close()

		rt2.CreateUser(username, []string{username})

		srv.Config.Handler = rt2.TestPublicHandler()

		passiveDBURL, err = url.Parse(srv.URL + "/db")
		require.NoError(t, err)
		passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
		arConfig.RemoteDBURL = passiveDBURL
		stats, err = base.SyncGatewayStats.NewDBStats(t.Name()+"2", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err = stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)
		arConfig.ReplicationStatsMap = dbstats

		ar, err = db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		require.NoError(t, ar.Start(ctx1))

		pushCheckpointer = ar.Push.GetSingleCollection(t).Checkpointer

		// we pulled the remote checkpoint, but the local checkpoint wasn't there to match it.
		assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointHitCount)

		// wait for document originally written to rt1 to arrive at rt2
		changesResults = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID, changesResults.Results[0].ID)

		rt2collection, rt2ctx = rt2.GetSingleTestDatabaseCollection()
		doc, err = rt2collection.GetDocument(rt2ctx, docID, db.DocUnmarshalAll)
		require.NoError(t, err)

		body, err = doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])

		// one _changes from seq:0 with initial number of docs sent
		endNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, numChangesRequestedFromZeroTotal+1, endNumChangesRequestedFromZeroTotal)

		// make sure the replicator has resent the rev
		base.RequireWaitForStat(t, ar.Push.GetStats().SendRevCount.Value, startNumRevsSentTotal+1)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ProcessedSequenceCount }, 1)
		base.RequireWaitForStat(t, func() int64 { return pushCheckpointer.Stats().ExpectedSequenceCount }, 1)

		// assert the second active replicator stats
		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointMissCount)

		assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
		pushCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, ar.Stop())
	})
}

// TestActiveReplicatorRecoverFromRemoteRollback:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Checkpoints.
//   - Creates another document on rt1 which is again pushed to rt2.
//   - Manually rolls back the bucket to the first document.
//   - Starts the replication again, and ensures that documents are re-replicated to it.
func TestActiveReplicatorRecoverFromRemoteRollback(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyBucket, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, passiveRT, remoteURLString := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)

		ctx2 := passiveRT.Context()
		ctx1 := activeRT.Context()

		// Create doc1 on activeRT
		docID := rest.SafeDocumentName(t, t.Name()+"rt1doc")
		docID2 := docID + "2"
		resp := activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"activeRT","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		activeRT.WaitForPendingChanges()
		replicationID := rest.SafeDocumentName(t, t.Name())
		stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		arConfig := db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: activeRT.GetDatabase(),
			},
			Continuous:          true,
			ReplicationStatsMap: dbstats,
			CollectionsEnabled:  !activeRT.GetDatabase().OnlyDefaultCollection(),
			// CBG-4786: remove this protocol line in this ticket
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		require.NoError(t, ar.Start(ctx1))

		activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		pushCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer

		base.RequireWaitForStat(t, func() int64 {
			return ar.Push.GetStats().SendRevCount.Value()
		}, 1)

		// wait for document originally written to activeRT to arrive at passiveRT
		changesResults := passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID, changesResults.Results[0].ID)
		lastSeq := changesResults.Last_Seq.String()

		_, doc1Body := activeRT.GetDoc(docID)
		assert.Equal(t, "activeRT", doc1Body["source"])

		// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
		assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
		pushCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

		cID := ar.Push.CheckpointID
		checkpointDocID := base.SyncDocPrefix + "local:checkpoint/" + cID

		var firstCheckpoint any
		_, err = passiveRT.GetSingleDataStore().Get(checkpointDocID, &firstCheckpoint)
		require.NoError(t, err)

		// Create doc2 on activeRT
		resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID2, `{"source":"activeRT","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		activeRT.WaitForPendingChanges()

		base.RequireWaitForStat(t, func() int64 {
			return ar.Push.GetStats().SendRevCount.Value()
		}, 2)

		// wait for new document to arrive at passiveRT
		changesResults = passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
		assert.Equal(t, docID2, changesResults.Results[0].ID)

		_, doc2Body := passiveRT.GetDoc(docID2)
		assert.Equal(t, "activeRT", doc2Body["source"])

		assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)
		pushCheckpointer.CheckpointNow()
		assert.Equal(t, int64(2), pushCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, ar.Stop())

		// roll back checkpoint value to first one and remove the associated doc
		err = passiveRT.GetSingleDataStore().Set(checkpointDocID, 0, nil, firstCheckpoint)
		assert.NoError(t, err)

		passiveRTCollection, passiveRTCtx := passiveRT.GetSingleTestDatabaseCollectionWithUser()
		if !sgrRunner.IsV4Protocol() {
			err = passiveRTCollection.Purge(passiveRTCtx, docID2, true)
			assert.NoError(t, err)
		} else {
			// we need to remove the _vv xattr for the doc to be re-replicated successfully, otherwise SG sees the existing _vv
			// and incoming vv and determines no new version to add
			err = passiveRTCollection.GetCollectionDatastore().DeleteWithXattrs(passiveRTCtx, docID2, []string{base.SyncXattrName, base.VvXattrName})
			require.NoError(t, err)
		}

		require.NoError(t, passiveRTCollection.FlushChannelCache(ctx2))
		passiveRT.GetDatabase().FlushRevisionCacheForTest()

		require.NoError(t, ar.Start(ctx1))

		activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		pushCheckpointer = ar.Push.GetSingleCollection(t).Checkpointer

		// wait for new document to arrive at passiveRT again
		changesResults = passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
		assert.Equal(t, docID2, changesResults.Results[0].ID)

		_, doc2Body = passiveRT.GetDoc(docID2)
		assert.Equal(t, "activeRT", doc2Body["source"])

		assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
		// wait for checkpoint set count to reach 1, there is small window between rev being sent to passive and awaiting
		// response before adding sequence to processed sequence list. So calling CheckpointNow before this sent rev is
		// added to processed sequences will mean CheckpointNow is a no-op. So we should wait for checkpoint count to increment.
		base.RequireWaitForStat(t, func() int64 {
			pushCheckpointer.CheckpointNow()
			return pushCheckpointer.Stats().SetCheckpointCount
		}, 1)
		assert.NoError(t, ar.Stop())
	})
}

// TestActiveReplicatorRecoverFromMismatchedRev:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Modifies the checkpoint rev ID in the target bucket.
//   - Checkpoints again to ensure it is retried on error.
func TestActiveReplicatorRecoverFromMismatchedRev(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyBucket, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	sgrRunner := rest.NewSGRTestRunner(t)
	const username = "alice"
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)

		ctx1 := rt1.Context()
		replicationID := rest.SafeDocumentName(t, t.Name())
		stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		arConfig := db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		require.NoError(t, ar.Start(ctx1))

		defer func() {
			assert.NoError(t, ar.Stop())
		}()

		pushCheckpointID := ar.Push.CheckpointID
		pushCheckpointDocID := base.SyncDocPrefix + "local:checkpoint/" + pushCheckpointID
		err = rt2.GetSingleDataStore().Set(pushCheckpointDocID, 0, nil, map[string]any{"last_sequence": "0", "_rev": "abc"})
		require.NoError(t, err)

		pullCheckpointID := ar.Pull.CheckpointID
		require.NoError(t, err)
		pullCheckpointDocID := base.SyncDocPrefix + "local:checkpoint/" + pullCheckpointID
		err = rt1.GetSingleDataStore().Set(pullCheckpointDocID, 0, nil, map[string]any{"last_sequence": "0", "_rev": "abc"})
		require.NoError(t, err)

		// Create doc1 on rt1
		docID := rest.SafeDocumentName(t, t.Name()+"rt1doc")
		resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		rt1.WaitForPendingChanges()

		// wait for document originally written to rt1 to arrive at rt2
		changesResults := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, docID, changesResults.Results[0].ID)

		// Create doc2 on rt2
		docID = rest.SafeDocumentName(t, t.Name()+"rt2doc")
		resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		rt2.WaitForPendingChanges()

		// wait for document originally written to rt2 to arrive at rt1
		changesResults = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=1", "", true)
		assert.Equal(t, docID, changesResults.Results[0].ID)

		pushCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer
		assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
		pushCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

		pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer
		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
	})
}

// TestActiveReplicatorIgnoreNoConflicts ensures the IgnoreNoConflicts flag allows Hydrogen<-->Hydrogen replication with no_conflicts set.
func TestActiveReplicatorIgnoreNoConflicts(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	const username = "alice"
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{username},
		})
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)
		ctx1 := rt1.Context()

		rt1docID := rest.SafeDocumentName(t, t.Name()) + "rt1doc1"
		rt1Version := rt1.PutDoc(rt1docID, `{"source":"rt1","channels":["alice"]}`)

		replicationID := rest.SafeDocumentName(t, t.Name())
		stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       200,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()

		assert.Equal(t, "", ar.GetStatus(ctx1).LastSeqPush)

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx1))

		// wait for the document originally written to rt1 to arrive at rt2
		sgrRunner.WaitForVersion(rt1docID, rt2, rt1Version)

		rt2collection, rt2ctx := rt2.GetSingleTestDatabaseCollection()
		doc, err := rt2collection.GetDocument(rt2ctx, rt1docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])

		// write a doc on rt2 ...
		rt2docID := rest.SafeDocumentName(t, t.Name()+"rt2doc1")
		rt2Version := rt2.PutDoc(rt2docID, `{"source":"rt2","channels":["alice"]}`)

		// ... and wait to arrive at rt1
		sgrRunner.WaitForVersion(rt2docID, rt1, rt2Version)
		changesResults := rt1.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
		assert.Equal(t, rt1docID, changesResults.Results[0].ID)
		assert.Equal(t, rt2docID, changesResults.Results[1].ID)

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		doc, err = rt1collection.GetDocument(rt1ctx, rt2docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err = doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])
	})

}

// TestActiveReplicatorPullFromCheckpointModifiedHash:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt2 which can be pulled by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt2.
//   - Starts the pull replication again with a config change, validate checkpoint is reset
func TestActiveReplicatorPullModifiedHash(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize         = 10
		numDocsPerChannelInitial = 13 // 2 batches of changes
		numDocsPerChannelTotal   = 24 // 2 more batches
		numChannels              = 2  // two channels
	)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			UserChannelAccess: []string{"chan1", "chan2"},
		})
		remoteURL, err := url.Parse(remoteURLString)
		require.NoError(t, err)

		// Create first batch of docs, creating numRT2DocsInitial in each channel
		docIDPrefix := rest.SafeDocumentName(t, t.Name()+"rt2doc")
		for i := range numDocsPerChannelInitial {
			rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i), `{"source":"rt2","channels":["chan1"]}`)
			rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i), `{"source":"rt2","channels":["chan2"]}`)
		}

		ctx1 := rt1.Context()
		replicationID := rest.SafeDocumentName(t, t.Name())
		stats, err := base.SyncGatewayStats.NewDBStats(replicationID, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(replicationID)
		require.NoError(t, err)

		arConfig := db.ActiveReplicatorConfig{
			ID:          replicationID,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: remoteURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       changesBatchSize,
			Filter:                 base.ByChannelFilter,
			FilterChannels:         []string{"chan1"},
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull chan1 from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

		require.NoError(t, ar.Start(ctx1))

		// wait for all of the documents originally written to rt2 to arrive at rt1
		changesResults := rt1.WaitForChanges(numDocsPerChannelInitial, "/{{.keyspace}}/_changes?since=0", "", true)
		docIDsSeen := make(map[string]bool, numDocsPerChannelInitial)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}
		for i := range numDocsPerChannelInitial {
			docID := fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i)
			assert.True(t, docIDsSeen[docID])
			doc := rt1.GetDocBody(docID)
			assert.Equal(t, "rt2", doc["source"])
		}

		// one _changes from seq:0 with initial number of docs sent
		numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

		pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

		// rev assertions
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, startNumRevsSentTotal+numDocsPerChannelInitial)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, numDocsPerChannelInitial)
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, numDocsPerChannelInitial)

		// checkpoint assertions
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)

		// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

		require.NoError(t, ar.Stop())

		// Second batch of docs, both channels
		for i := numDocsPerChannelInitial; i < numDocsPerChannelTotal; i++ {
			rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i), `{"source":"rt2","channels":["chan1"]}`)
			rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i), `{"source":"rt2","channels":["chan2"]}`)
		}

		// Create a new replicator using the same replicationID but different channel filter, which should reset the checkpoint
		arConfig.FilterChannels = []string{"chan2"}
		ar, err = db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)
		defer func() { assert.NoError(t, ar.Stop()) }()
		require.NoError(t, ar.Start(ctx1))

		// new replicator - new checkpointer
		pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

		// wait for all of the documents originally written to rt2 to arrive at rt1
		expectedChan1Docs := numDocsPerChannelInitial
		expectedChan2Docs := numDocsPerChannelTotal
		expectedTotalDocs := expectedChan1Docs + expectedChan2Docs
		changesResults = rt1.WaitForChanges(expectedTotalDocs, "/{{.keyspace}}/_changes?since=0", "", true)

		docIDsSeen = make(map[string]bool, expectedTotalDocs)
		for _, result := range changesResults.Results {
			docIDsSeen[result.ID] = true
		}

		rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
		for i := range numDocsPerChannelTotal {
			docID := fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i)
			assert.True(t, docIDsSeen[docID])

			doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
			assert.NoError(t, err)

			body, err := doc.GetDeepMutableBody()
			require.NoError(t, err)
			assert.Equal(t, "rt2", body["source"])
		}

		// Should have two replications since zero
		endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
		assert.Equal(t, startNumChangesRequestedFromZeroTotal+2, endNumChangesRequestedFromZeroTotal)

		// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
		base.RequireWaitForStat(t, rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value, startNumRevsSentTotal+int64(expectedTotalDocs))
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ProcessedSequenceCount }, int64(expectedChan2Docs))
		base.RequireWaitForStat(t, func() int64 { return pullCheckpointer.Stats().ExpectedSequenceCount }, int64(expectedChan2Docs))

		// assert the second active replicator stats
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
		pullCheckpointer.CheckpointNow()
		assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
	})
}

// TestActiveReplicatorReconnectOnStart ensures ActiveReplicators retry their initial connection for cases like:
// - Incorrect credentials
// - Unroutable remote address
// Will test both indefinite retry, and a timeout.
func TestActiveReplicatorReconnectOnStart(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	if testing.Short() {
		t.Skipf("Test skipped in short mode")
	}

	tests := []struct {
		name                             string
		usernameOverride                 string
		remoteURLHostOverride            string
		expectedErrorContains            string
		expectedErrorIsConnectionRefused bool
	}{
		{
			name:                  "wrong user",
			usernameOverride:      "bob",
			expectedErrorContains: "unexpected status code 401 from target database",
		},
		{
			name:                             "invalid port", // fails faster than unroutable address (connection refused vs. connect timeout)
			remoteURLHostOverride:            "127.0.0.1:1234",
			expectedErrorIsConnectionRefused: true,
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {

				var abortTimeout = time.Millisecond * 500
				if runtime.GOOS == "windows" {
					// A longer timeout is required on Windows as connection refused errors take approx 2 seconds vs. instantaneous on Linux.
					abortTimeout = time.Second * 5
				}
				// test cases with and without a timeout. Ensure replicator retry loop is stopped in both cases.
				timeoutVals := []time.Duration{
					0,
					abortTimeout,
				}

				for _, timeoutVal := range timeoutVals {
					t.Run(test.name+" with timeout "+timeoutVal.String(), func(t *testing.T) {
						username := "alice"
						rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
							UserChannelAccess: []string{username},
						})

						// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
						srv := httptest.NewServer(rt2.TestPublicHandler())
						defer srv.Close()

						// Build remoteDBURL with basic auth creds
						remoteDBURL, err := url.Parse(srv.URL + "/db")
						require.NoError(t, err)

						// Add basic auth creds to target db URL
						if test.usernameOverride != "" {
							username = test.usernameOverride
						}
						remoteDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

						if test.remoteURLHostOverride != "" {
							remoteDBURL.Host = test.remoteURLHostOverride
						}
						ctx1 := rt1.Context()

						id, err := base.GenerateRandomID()
						require.NoError(t, err)
						sgwStats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
						require.NoError(t, err)
						dbstats, err := sgwStats.DBReplicatorStats(id)
						require.NoError(t, err)

						arConfig := db.ActiveReplicatorConfig{
							ID:          id,
							Direction:   db.ActiveReplicatorTypePush,
							RemoteDBURL: remoteDBURL,
							ActiveDB: &db.Database{
								DatabaseContext: rt1.GetDatabase(),
							},
							Continuous: true,
							// aggressive reconnect intervals for testing purposes
							InitialReconnectInterval: time.Millisecond,
							MaxReconnectInterval:     time.Millisecond * 50,
							TotalReconnectTimeout:    timeoutVal,
							ReplicationStatsMap:      dbstats,
							CollectionsEnabled:       !rt1.GetDatabase().OnlyDefaultCollection(),
							SupportedBLIPProtocols:   sgrRunner.SupportedSubprotocols,
						}

						// Create the first active replicator to pull from seq:0
						ar, err := db.NewActiveReplicator(ctx1, &arConfig)
						require.NoError(t, err)

						assert.Equal(t, int64(0), ar.Push.GetStats().NumConnectAttempts.Value())

						err = ar.Start(ctx1)
						assert.Error(t, err, "expecting ar.Start() to return error, but it didn't")
						defer func() { assert.NoError(t, ar.Stop()) }()

						if test.expectedErrorIsConnectionRefused {
							assert.True(t, base.IsConnectionRefusedError(err))
						}

						if test.expectedErrorContains != "" {
							assert.True(t, strings.Contains(err.Error(), test.expectedErrorContains))
						}

						if timeoutVal > 0 {
							// wait for an arbitrary number of reconnect attempts
							require.EventuallyWithT(t, func(c *assert.CollectT) {
								assert.Greaterf(c, ar.Push.GetStats().NumConnectAttempts.Value(), int64(2), "Expecting NumConnectAttempts > 2")
							}, time.Second*5, time.Millisecond*100)

							time.Sleep(timeoutVal + time.Millisecond*250)

							// wait for the retry loop to hit the TotalReconnectTimeout and give up retrying
							require.EventuallyWithT(t, func(c *assert.CollectT) {
								assert.Greaterf(c, ar.Push.GetStats().NumReconnectsAborted.Value(), int64(0), "Expecting NumReconnectsAborted > 0")
							}, time.Second*5, time.Millisecond*100)
						}
					})
				}
			})
		}
	})
}

// TestActiveReplicatorReconnectOnStartEventualSuccess ensures an active replicator with invalid creds retries,
// but succeeds once the user is created on the remote.
func TestActiveReplicatorReconnectOnStartEventualSuccess(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			AvoidUserCreation: true,
		})
		// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
		srv := httptest.NewServer(rt2.TestPublicHandler())
		defer srv.Close()

		// Build remoteDBURL with basic auth creds
		remoteDBURL, err := url.Parse(srv.URL + "/passivedb")
		require.NoError(t, err)

		// Add basic auth creds to target db URL
		remoteDBURL.User = url.UserPassword("alice", "pass")
		ctx1 := rt1.Context()

		id, err := base.GenerateRandomID()
		require.NoError(t, err)
		stats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(id)
		require.NoError(t, err)

		arConfig := db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePushAndPull,
			RemoteDBURL: remoteDBURL,
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous: true,
			// aggressive reconnect intervals for testing purposes
			InitialReconnectInterval: time.Millisecond,
			MaxReconnectInterval:     time.Millisecond * 50,
			TotalReconnectTimeout:    time.Second * 30,
			ReplicationStatsMap:      dbstats,
			CollectionsEnabled:       !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols:   sgrRunner.SupportedSubprotocols,
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		assert.Equal(t, int64(0), ar.Push.GetStats().NumConnectAttempts.Value())

		// expected error
		msg401 := "unexpected status code 401 from target database"

		err = ar.Start(ctx1)
		defer func() { require.NoError(t, ar.Stop()) }() // prevents panic if waiting for ar state running fails
		require.Error(t, err)
		assert.True(t, strings.Contains(err.Error(), msg401))

		// wait for an arbitrary number of reconnect attempts
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Greaterf(c, ar.Push.GetStats().NumConnectAttempts.Value(), int64(3), "Expecting NumConnectAttempts > 3")
		}, time.Second*5, time.Millisecond*100)

		resp := rt2.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", `{"password":"pass"}`)
		rest.RequireStatus(t, resp, http.StatusCreated)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			state, errMsg := ar.State(ctx1)
			if strings.TrimSpace(errMsg) != "" && !strings.Contains(errMsg, msg401) {
				log.Println("unexpected replicator error:", errMsg)
			}
			assert.Equal(c, db.ReplicationStateRunning, state, "Expecting replication state to be running")
		}, time.Second*5, time.Millisecond*100)
	})
}

// TestActiveReplicatorReconnectSendActions ensures ActiveReplicator reconnect retry loops exit when the replicator is stopped
func TestActiveReplicatorReconnectSendActions(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		rt1, rt2, _ := sgrRunner.SetupSGRPeers(t)
		ctx1 := rt1.Context()

		id, err := base.GenerateRandomID()
		require.NoError(t, err)
		arConfig := db.ActiveReplicatorConfig{
			ID:        id,
			Direction: db.ActiveReplicatorTypePull,
			// Add incorrect basic auth creds to target db URL
			RemoteDBURL: userDBURL(rt2, "bob"),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous: true,
			// aggressive reconnect intervals for testing purposes
			InitialReconnectInterval: time.Millisecond,
			MaxReconnectInterval:     time.Millisecond * 50,
			TotalReconnectTimeout:    time.Second * 5,
			ReplicationStatsMap:      dbReplicatorStats(t),
			CollectionsEnabled:       !rt1.GetDatabase().OnlyDefaultCollection(),
		}

		// Create the first active replicator to pull from seq:0
		ar, err := db.NewActiveReplicator(ctx1, &arConfig)
		require.NoError(t, err)

		defer func() {
			assert.NoError(t, ar.Stop())
		}()

		assert.Equal(t, int64(0), ar.Pull.GetStats().NumConnectAttempts.Value())

		err = ar.Start(ctx1)
		assert.Error(t, err)
		assert.ErrorContains(t, err, "unexpected status code 401 from target database")

		// wait for an arbitrary number of reconnect attempts
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Greater(c, ar.Pull.GetStats().NumConnectAttempts.Value(), int64(3))
		}, time.Second*20, time.Millisecond*100)

		assert.NoError(t, ar.Stop())
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Equal(c, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
		}, time.Second*20, time.Millisecond*100)

		// wait for a bit to see if the reconnect loop has stopped
		reconnectAttempts := ar.Pull.GetStats().NumConnectAttempts.Value()
		time.Sleep(time.Millisecond * 250)
		assert.Equal(t, reconnectAttempts, ar.Pull.GetStats().NumConnectAttempts.Value())

		assert.NoError(t, ar.Reset())
		assert.Equal(t, reconnectAttempts, ar.Pull.GetStats().NumConnectAttempts.Value())

		err = ar.Start(ctx1)
		assert.ErrorContains(t, err, "unexpected status code 401 from target database")

		// wait for another set of reconnect attempts
		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Greater(c, ar.Pull.GetStats().NumConnectAttempts.Value(), reconnectAttempts+int64(3))
		}, time.Second*20, time.Millisecond*100)
	})
}

// TestActiveReplicatorPullConflictReadWriteIntlProps:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullConflictReadWriteIntlProps(t *testing.T) {
	base.LongRunningTest(t)

	createVersion := func(generation int, parentRevID string, body db.Body) rest.DocVersion {
		rev, err := db.CreateRevID(generation, parentRevID, body)
		require.NoError(t, err, "Error creating revision")
		return rest.DocVersion{RevTreeID: rev}
	}
	docExpiry := time.Now().Local().Add(time.Hour * time.Duration(4)).Format(time.RFC3339)

	// scenarios
	conflictResolutionTests := []struct {
		name                  string
		commonAncestorVersion *rest.DocVersion
		localRevisionBody     string
		localVersion          rest.DocVersion
		remoteRevisionBody    string
		remoteVersion         rest.DocVersion
		conflictResolver      string
		expectedLocalBody     db.Body
		expectedLocalVersion  rest.DocVersion
	}{
		{
			name:               "mergeReadWriteIntlProps",
			localRevisionBody:  `{"source": "local"}`,
			localVersion:       rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody: `{ "source": "remote"}`,
			remoteVersion:      rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc.remoteDocId = conflict.RemoteDocument._id;
				mergedDoc.remoteRevId = conflict.RemoteDocument._rev;
				mergedDoc.localDocId = conflict.LocalDocument._id;
				mergedDoc.localRevId = conflict.LocalDocument._rev;
				mergedDoc._id = "foo";
				mergedDoc._rev = "2-c";
				mergedDoc._exp = 100;
				return mergedDoc;
			}`,
			expectedLocalBody: db.Body{
				db.BodyId:     "foo",
				db.BodyRev:    "2-c",
				db.BodyExpiry: json.Number("100"),
				"localDocId":  "mergeReadWriteIntlProps",
				"localRevId":  "1-a",
				"remoteDocId": "mergeReadWriteIntlProps",
				"remoteRevId": "1-b",
				"source":      "merged",
			},
			expectedLocalVersion: createVersion(2, "1-b", db.Body{
				db.BodyId:     "foo",
				db.BodyRev:    "2-c",
				db.BodyExpiry: json.Number("100"),
				"localDocId":  "mergeReadWriteIntlProps",
				"localRevId":  "1-a",
				"remoteDocId": "mergeReadWriteIntlProps",
				"remoteRevId": "1-b",
				"source":      "merged",
			}),
		},
		{
			name:               "mergeReadWriteAttachments",
			localRevisionBody:  `{"_attachments": {"A": {"data": "QQo="}}, "source": "local"}`,
			localVersion:       rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody: `{"_attachments": {"B": {"data": "Qgo="}}, "source": "remote"}`,
			remoteVersion:      rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				var mergedAttachments = new Object();

				dst = conflict.RemoteDocument._attachments;
				for (var key in dst) {
					mergedAttachments[key] = dst[key];
				}
				src = conflict.LocalDocument._attachments;
				for (var key in src) {
					mergedAttachments[key] = src[key];
				}
				mergedDoc._attachments = mergedAttachments;
				return mergedDoc;
			}`,
			expectedLocalBody: map[string]any{
				"source": "merged",
			},
			expectedLocalVersion: createVersion(2, "1-b", db.Body{
				"source": "merged",
			}),
		},
		{
			name:               "mergeReadIntlPropsLocalExpiry",
			localRevisionBody:  fmt.Sprintf(`{"source": "local", "_exp": "%s"}`, docExpiry),
			localVersion:       rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody: `{"source": "remote"}`,
			remoteVersion:      rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc.localDocExp = conflict.LocalDocument._exp;
				return mergedDoc;
			}`,
			expectedLocalBody: db.Body{
				"localDocExp": docExpiry,
				"source":      "merged",
			},
			expectedLocalVersion: createVersion(2, "1-b", db.Body{
				"localDocExp": docExpiry,
				"source":      "merged",
			}),
		},
		{
			name:               "mergeWriteIntlPropsExpiry",
			localRevisionBody:  fmt.Sprintf(`{"source": "local", "_exp": "%s"}`, docExpiry),
			localVersion:       rest.NewDocVersionFromFakeRev("1-a"),
			remoteRevisionBody: `{"source": "remote"}`,
			remoteVersion:      rest.NewDocVersionFromFakeRev("1-b"),
			conflictResolver: fmt.Sprintf(`function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc._exp = %q;
				return mergedDoc;
			}`, docExpiry),
			expectedLocalBody: db.Body{
				db.BodyExpiry: docExpiry,
				"source":      "merged",
			},
			expectedLocalVersion: createVersion(2, "1-b", db.Body{
				db.BodyExpiry: docExpiry,
				"source":      "merged",
			}),
		},
		{
			name:                  "mergeReadIntlPropsDeletedWithLocalTombstone",
			localRevisionBody:     `{"source": "local", "_deleted": true}`,
			commonAncestorVersion: base.Ptr(rest.NewDocVersionFromFakeRev("1-a")),
			localVersion:          rest.NewDocVersionFromFakeRev("2-a"),
			remoteRevisionBody:    `{"source": "remote"}`,
			remoteVersion:         rest.NewDocVersionFromFakeRev("2-b"),
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc.localDeleted = conflict.LocalDocument._deleted;
				return mergedDoc;
			}`,
			expectedLocalBody: db.Body{
				"localDeleted": true,
				"source":       "merged",
			},
			expectedLocalVersion: createVersion(3, "2-b", db.Body{
				"localDeleted": true,
				"source":       "merged",
			}),
		},
	}

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range conflictResolutionTests {
			t.Run(test.name, func(t *testing.T) {
				base.RequireNumTestBuckets(t, 2)
				rt1, rt2, remoteURLString := sgrRunner.SetupSGRPeers(t)
				passiveDBURL, err := url.Parse(remoteURLString)
				require.NoError(t, err)

				// Create revision on rt2 (remote)
				docID := test.name
				if test.commonAncestorVersion != nil {
					_ = rt2.PutNewEditsFalse(docID, *test.commonAncestorVersion, nil, test.remoteRevisionBody)
				}
				fmt.Println("remoteRevisionBody:", test.remoteRevisionBody)
				rt2Version := rt2.PutNewEditsFalse(docID, test.remoteVersion, test.commonAncestorVersion, test.remoteRevisionBody)
				rest.RequireDocRevTreeEqual(t, test.remoteVersion, *rt2Version)
				ctx1 := rt1.Context()

				// Create revision on rt1 (local)
				if test.commonAncestorVersion != nil {
					_ = rt1.PutNewEditsFalse(docID, *test.commonAncestorVersion, nil, test.remoteRevisionBody)
				}
				fmt.Println("localRevisionBody:", test.localRevisionBody)
				rt1Version := rt1.PutNewEditsFalse(docID, test.localVersion, test.commonAncestorVersion, test.localRevisionBody)
				rest.RequireDocRevTreeEqual(t, test.localVersion, *rt1Version)

				id := rest.SafeDocumentName(t, t.Name())
				customConflictResolver, err := db.NewCustomConflictResolver(ctx1, test.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
				require.NoError(t, err)
				dbstats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
				require.NoError(t, err)
				replicationStats, err := dbstats.DBReplicatorStats(id)
				require.NoError(t, err)

				ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
					ID:          id,
					Direction:   db.ActiveReplicatorTypePull,
					RemoteDBURL: passiveDBURL,
					ActiveDB: &db.Database{
						DatabaseContext: rt1.GetDatabase(),
					},
					ChangesBatchSize:           200,
					ConflictResolverFunc:       customConflictResolver,
					ConflictResolverFuncForHLV: customConflictResolver,
					Continuous:                 true,
					ReplicationStatsMap:        replicationStats,
					CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
					SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
				})
				require.NoError(t, err)
				defer func() { assert.NoError(t, ar.Stop()) }()

				// Start the replicator (implicit connect)
				require.NoError(t, ar.Start(ctx1))
				require.EventuallyWithT(t, func(c *assert.CollectT) {
					assert.Equal(c, 1, int(ar.GetStatus(ctx1).DocsRead))
				}, time.Second*5, time.Millisecond*100)
				assert.Equal(t, 1, int(replicationStats.ConflictResolvedMergedCount.Value()))

				// Wait for the document originally written to rt2 to arrive at rt1.
				// Should end up as winner under default conflict resolution.
				changesResults := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?&since=0", "", true)
				assert.Equal(t, docID, changesResults.Results[0].ID)
				rest.RequireChangeRev(t, test.expectedLocalVersion, changesResults.Results[0].Changes[0], db.ChangesVersionTypeRevTreeID)
				t.Logf("Changes response is %+v", changesResults)

				rt1collection, rt1ctx := rt1.GetSingleTestDatabaseCollection()
				doc, err := rt1collection.GetDocument(rt1ctx, docID, db.DocUnmarshalAll)
				require.NoError(t, err)
				test.expectedLocalVersion.CV = db.Version{
					SourceID: rt1.GetDatabase().EncodedSourceID,
					Value:    doc.Cas,
				}
				sgrRunner.WaitForVersion(docID, rt1, test.expectedLocalVersion)
				ctx := base.TestCtx(t)
				t.Logf("doc.Body(): %v", doc.Body(ctx))
				assert.Equal(t, test.expectedLocalBody, doc.Body(ctx))
				t.Logf("Doc %s is %+v", docID, doc)
				for revID, revInfo := range doc.SyncData.History {
					t.Logf("doc revision [%s]: %+v", revID, revInfo)
				}

				// Validate only one active leaf node remains after conflict resolution, and that all parents
				// of leaves have empty bodies
				activeCount := 0
				for _, revID := range doc.SyncData.History.GetLeaves() {
					revInfo, ok := doc.SyncData.History[revID]
					require.True(t, ok)
					if !revInfo.Deleted {
						activeCount++
					}
					if revInfo.Parent != "" {
						parentRevInfo, ok := doc.SyncData.History[revInfo.Parent]
						require.True(t, ok)
						assert.True(t, parentRevInfo.Body == nil)
					}
				}
				assert.Equal(t, 1, activeCount)
			})
		}
	})
}
func TestSGR2TombstoneConflictHandling(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	tombstoneTests := []struct {
		name               string
		longestBranchLocal bool
		resurrectLocal     bool
		sdkResurrect       bool
	}{

		{
			name:               "RemoteLongResurrectLocal",
			longestBranchLocal: false,
			resurrectLocal:     true,
			sdkResurrect:       false,
		},
		{
			name:               "LocalLongResurrectLocal",
			longestBranchLocal: true,
			resurrectLocal:     true,
			sdkResurrect:       false,
		},
		{
			name:               "RemoteLongResurrectRemote",
			longestBranchLocal: false,
			resurrectLocal:     false,
			sdkResurrect:       false,
		},
		{
			name:               "LocalLongResurrectRemote",
			longestBranchLocal: true,
			resurrectLocal:     false,
			sdkResurrect:       false,
		},

		{
			name:               "RemoteLongSDKResurrectLocal",
			longestBranchLocal: false,
			resurrectLocal:     true,
			sdkResurrect:       true,
		},
		{
			name:               "RemoteLongSDKResurrectRemote",
			longestBranchLocal: false,
			resurrectLocal:     false,
			sdkResurrect:       true,
		},
		{
			name:               "LocalLongSDKResurrectLocal",
			longestBranchLocal: true,
			resurrectLocal:     true,
			sdkResurrect:       true,
		},
		{
			name:               "LocalLongSDKResurrectRemote",
			longestBranchLocal: true,
			resurrectLocal:     false,
			sdkResurrect:       true,
		},
	}

	// requireTombstone validates tombstoned revision.
	requireTombstone := func(t *testing.T, dataStore base.DataStore, docID string) {
		var rawBody db.Body
		_, err := dataStore.Get(docID, &rawBody)
		if base.TestUseXattrs() {
			require.True(t, base.IsDocNotFoundError(err))
			require.Len(t, rawBody, 0)
		} else {
			require.NoError(t, err)
			require.Len(t, rawBody, 1)
			rawSyncData, ok := rawBody[base.SyncPropertyName].(map[string]any)
			require.True(t, ok)
			val, ok := rawSyncData["flags"].(float64)
			require.True(t, ok)
			require.NotEqual(t, 0, int(val)&channels.Deleted)
		}
	}

	sgrRunner := rest.NewSGRTestRunner(t)
	// tests is putting document rev trees in certain states, not fully relevant for v4 replication so keep in v3 mode
	sgrRunner.RunSubprotocolV3(func(t *testing.T) {
		for _, test := range tombstoneTests {
			t.Run(test.name, func(t *testing.T) {
				if test.sdkResurrect && !base.TestUseXattrs() {
					t.Skip("SDK resurrect test cases require xattrs to be enabled")
				}
				localActiveRT, remotePassiveRT, _ := sgrRunner.SetupSGRPeers(t)

				replConf := `
			{
				"replication_id": "replication",
				"remote": "` + adminDBURL(remotePassiveRT).String() + `",
				"direction": "pushAndPull",
				"continuous": true,
				"collections_enabled": ` + strconv.FormatBool(!localActiveRT.GetDatabase().OnlyDefaultCollection()) + `
			}`

				// Send up replication
				resp := localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replication/replication", replConf)
				rest.RequireStatus(t, resp, http.StatusCreated)

				// Create a doc with 3-revs
				resp = localActiveRT.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", `{"docs":[{"_id": "docid2", "_rev": "1-abc"}, {"_id": "docid2", "_rev": "2-abc", "_revisions": {"start": 2, "ids": ["abc", "abc"]}}, {"_id": "docid2", "_rev": "3-abc", "val":"test", "_revisions": {"start": 3, "ids": ["abc", "abc", "abc"]}}], "new_edits":false}`)
				rest.RequireStatus(t, resp, http.StatusCreated)

				// Wait for the replication to be started
				localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

				const doc2ID = "docid2"
				doc2Version := rest.DocVersion{RevTreeID: "3-abc"}
				sgrRunner.WaitForVersion(doc2ID, localActiveRT, doc2Version)
				sgrRunner.WaitForVersion(doc2ID, remotePassiveRT, doc2Version)

				// Stop the replication
				rest.RequireStatus(t, localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=stop", ""), http.StatusOK)
				localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateStopped)

				// Delete on the short branch and make another doc on the longer branch before deleting it
				var deleteVersion rest.DocVersion
				if test.longestBranchLocal {
					// Delete doc on remote
					deletedVersion := remotePassiveRT.DeleteDoc(doc2ID, doc2Version)
					require.Equal(t, "4-cc0337d9d38c8e5fc930ae3deda62bf8", deletedVersion.RevTreeID)

					// Create another rev and then delete doc on local - ie tree is longer
					version := localActiveRT.UpdateDoc(doc2ID, doc2Version, `{"foo":"bar"}`)
					deleteVersion = localActiveRT.DeleteDoc(doc2ID, version)

					// Validate local is CBS tombstone, expect not found error
					// Expect KeyNotFound error retrieving local tombstone pre-replication
					requireTombstone(t, localActiveRT.GetSingleDataStore(), "docid2")

				} else {
					// Delete doc on localActiveRT (active / local)
					deletedVersion := localActiveRT.DeleteDoc(doc2ID, doc2Version)
					require.Equal(t, "4-cc0337d9d38c8e5fc930ae3deda62bf8", deletedVersion.RevTreeID)

					// Create another rev and then delete doc on remotePassiveRT (passive) - ie, tree is longer
					version := remotePassiveRT.UpdateDoc(doc2ID, rest.DocVersion{RevTreeID: "3-abc"}, `{"foo":"bar"}`)
					deleteVersion = remotePassiveRT.DeleteDoc(doc2ID, version)

					// Validate local is CBS tombstone, expect not found error
					// Expect KeyNotFound error retrieving remote tombstone pre-replication
					requireTombstone(t, remotePassiveRT.GetSingleDataStore(), doc2ID)
				}

				// Start up repl again
				rest.RequireStatus(t, localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=start", ""), http.StatusOK)
				localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

				// Wait for the recently longest branch to show up on both sides
				sgrRunner.WaitForTombstone(doc2ID, localActiveRT, deleteVersion)
				sgrRunner.WaitForTombstone(doc2ID, remotePassiveRT, deleteVersion)

				// Stop the replication
				rest.RequireStatus(t, localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=stop", ""), http.StatusOK)
				localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateStopped)

				// Resurrect Doc
				updatedBody := make(map[string]any)
				updatedBody["resurrection"] = true
				if test.resurrectLocal {
					if test.sdkResurrect {
						// resurrect doc via SDK on local
						err := localActiveRT.GetSingleDataStore().Set(doc2ID, 0, nil, updatedBody)
						assert.NoError(t, err, "Unable to resurrect doc docid2")
						collection, ctx := localActiveRT.GetSingleTestDatabaseCollection()
						// force on-demand import
						_, getErr := collection.GetDocument(ctx, "docid2", db.DocUnmarshalSync)
						require.NoError(t, getErr, "Unable to retrieve resurrected doc docid2")
					} else {
						localActiveRT.PutDoc("docid2", `{"resurrection": true}`)
					}
				} else {
					if test.sdkResurrect {
						// resurrect doc via SDK on remote
						err := remotePassiveRT.GetSingleDataStore().Set(doc2ID, 0, nil, updatedBody)
						assert.NoError(t, err, "Unable to resurrect doc docid2")
						// force on-demand import
						collection, ctx := remotePassiveRT.GetSingleTestDatabaseCollection()
						_, getErr := collection.GetDocument(ctx, doc2ID, db.DocUnmarshalSync)
						assert.NoError(t, getErr, "Unable to retrieve resurrected doc docid2")
					} else {
						remotePassiveRT.PutDoc("docid2", `{"resurrection": true}`)
					}
				}

				// For SG resurrect, rev history is preserved, expect rev 6-...
				expectedRevID := "6-bf187e11c1f8913769dca26e56621036"
				if test.sdkResurrect {
					// For SDK resurrect, rev history is not preserved, expect rev 1-...
					expectedRevID = "1-e5d43a9cdc4a2d4e258800dfc37e9d77"
				}

				expectedVersion := rest.DocVersion{RevTreeID: expectedRevID}
				// Wait for doc to show up on side that the resurrection was done
				if test.resurrectLocal {
					localActiveRT.WaitForVersion(doc2ID, expectedVersion)
				} else {
					remotePassiveRT.WaitForVersion(doc2ID, expectedVersion)
				}

				// Start the replication
				rest.RequireStatus(t, localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=start", ""), http.StatusOK)
				localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

				// Wait for doc to replicate from side resurrection was done on to the other side
				if test.resurrectLocal {
					remotePassiveRT.WaitForVersion(doc2ID, expectedVersion)
				} else {
					localActiveRT.WaitForVersion(doc2ID, expectedVersion)
				}
			})
		}
	})
}

// This test ensures that the local tombstone revision wins over non-tombstone revision
// whilst applying default conflict resolution policy through pushAndPull replication.
func TestDefaultConflictResolverWithTombstoneLocal(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	defaultConflictResolverWithTombstoneTests := []struct {
		name             string   // A unique name to identify the unit test.
		remoteBodyValues []string // Controls the remote revision generation.
		expectedRevID    string   // Expected document revision ID.
	}{
		{
			// Revision tie with local digest is lower than the remote digest.
			// local generation = remote generation:
			//	- e.g. local is 3-a(T), remote is 3-b
			name:             "revGenTieLocalDigestLower",
			remoteBodyValues: []string{"baz", "EADGBE"},
			expectedRevID:    "4-c6fe7cde8f7187705f9e048322a9c350",
		},
		{
			// Revision tie with local digest is higher than the remote digest.
			// local generation = remote generation:
			//	- e.g. local is 3-c(T), remote is 3-b
			name:             "revGenTieLocalDigestHigher",
			remoteBodyValues: []string{"baz", "qux"},
			expectedRevID:    "4-a210e8a790415d7e842e78e1d051cb3d",
		},
		{
			// Local revision generation is lower than remote revision generation.
			// local generation < remote generation:
			//  - e.g. local is 3-a(T), remote is 4-b
			name:             "revGenLocalLower",
			remoteBodyValues: []string{"baz", "qux", "grunt"},
			expectedRevID:    "5-fe3ac95144be01e9b455bfa163687f0e",
		},
		{
			// Local revision generation is higher than remote revision generation.
			// local generation > remote generation:
			//	- e.g. local is 3-a(T), remote is 2-b
			name:             "revGenLocalHigher",
			remoteBodyValues: []string{"baz"},
			expectedRevID:    "4-232b1f34f6b9341c54435eaf5447d85d",
		},
	}
	for _, test := range defaultConflictResolverWithTombstoneTests {
		t.Run(test.name, func(t *testing.T) {
			passiveRT := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
			})

			defer passiveRT.Close()

			username := "alice"
			passiveRT.CreateUser(username, []string{username})

			// Active
			activeRT := rest.NewRestTester(t, nil)
			defer activeRT.Close()
			activeRTCtx := activeRT.Context()

			defaultConflictResolver, err := db.NewCustomConflictResolver(
				activeRTCtx, `function(conflict) { return defaultPolicy(conflict); }`, activeRT.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err, "Error creating custom conflict resolver")

			config := db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: userDBURL(passiveRT, username),
				ActiveDB: &db.Database{
					DatabaseContext: activeRT.GetDatabase(),
				},
				Continuous:             true,
				ConflictResolverFunc:   defaultConflictResolver,
				ReplicationStatsMap:    dbReplicatorStats(t),
				CollectionsEnabled:     !activeRT.GetDatabase().OnlyDefaultCollection(),
				SupportedBLIPProtocols: []string{db.CBMobileReplicationV3.SubprotocolString()}, // only relevant for v3 and below replications
			}

			docID := "doc"
			// Create the first revision of the document on activeRT.
			activeRTVersionCreated := createDoc(activeRT, docID, "foo")

			// Create active replicator and start replication.
			ar, err := db.NewActiveReplicator(activeRTCtx, &config)
			require.NoError(t, err)
			require.NoError(t, ar.Start(activeRTCtx), "Error starting replication")
			defer func() { require.NoError(t, ar.Stop(), "Error stopping replication") }()

			// Wait for the original document revision written to activeRT to arrive at passiveRT.
			passiveRT.WaitForVersionRevIDOnly(docID, activeRTVersionCreated) // < v4 replication only sends revID

			// Stop replication.
			require.NoError(t, ar.Stop(), "Error stopping replication")

			// Update the document on activeRT to build a revision history.
			activeRTVersionCreated.CV = db.Version{} // need to clear cv to update with revID not CV
			activeRTVersionUpdated := updateDoc(activeRT, docID, activeRTVersionCreated, "bar")

			// Tombstone the document on activeRT to mark the tip of the revision history for deletion.
			tombstoneVersion := activeRT.DeleteDoc(docID, activeRTVersionUpdated)

			activeRT.WaitForTombstoneRevIDOnly(docID, tombstoneVersion) // < v4 replication only sends revID

			// Update the document on passiveRT with the specified body values.
			passiveRTVersion := activeRTVersionCreated
			for _, bodyValue := range test.remoteBodyValues {
				passiveRTVersion = updateDoc(passiveRT, docID, passiveRTVersion, bodyValue)
				passiveRTVersion.CV = db.Version{} // need to clear cv to update with revID not CV
			}

			// Start replication.
			require.NoError(t, ar.Start(activeRTCtx), "Error starting replication")
			// Wait for default conflict resolution policy to be applied through replication and
			// the winning revision to be written to both activeRT and passiveRT buckets. Check whether the
			// winning revision is a tombstone; tombstone revision wins over non-tombstone revision.
			expectedVersion := rest.NewDocVersionFromFakeRev(test.expectedRevID)
			activeRT.WaitForTombstoneRevIDOnly(docID, expectedVersion)
			passiveRT.WaitForTombstoneRevIDOnly(docID, expectedVersion)
		})
	}
}

// This test ensures that the remote tombstone revision wins over non-tombstone revision
// whilst applying default conflict resolution policy through pushAndPull replication.
func TestDefaultConflictResolverWithTombstoneRemote(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	defaultConflictResolverWithTombstoneTests := []struct {
		name            string   // A unique name to identify the unit test.
		localBodyValues []string // Controls the local revision generation.
		expectedRevID   string   // Expected document revision ID.
	}{
		{
			// Revision tie with remote digest is lower than the local digest.
			// local generation = remote generation:
			//	- e.g. local is 3-b, remote is 3-a(T)
			name:            "revGenTieRemoteDigestLower",
			localBodyValues: []string{"baz", "EADGBE"},
			expectedRevID:   "4-0748692c1535b62f59b2c276cc2a8bda",
		},
		{
			// Revision tie with remote digest is higher than the local digest.
			// local generation = remote generation:
			//	- e.g. local is 3-b, remote is 3-c(T)
			name:            "revGenTieRemoteDigestHigher",
			localBodyValues: []string{"baz", "qux"},
			expectedRevID:   "4-5afdb61ba968c9eaa7599e727c4c1b53",
		},
		{
			// Local revision generation is higher than remote revision generation.
			// local generation > remote generation:
			//  - e.g. local is 4-b, remote is 3-a(T)
			name:            "revGenRemoteLower",
			localBodyValues: []string{"baz", "qux", "grunt"},
			expectedRevID:   "5-962dc965fd8e7fd2bc3ffbcab85d53ba",
		},
		{
			// Local revision generation is lower than remote revision generation.
			// local generation < remote generation:
			//	- e.g. local is 2-b, remote is 3-a(T)
			name:            "revGenRemoteHigher",
			localBodyValues: []string{"grunt"},
			expectedRevID:   "3-cd4c29d9c84fc8b2a51c50e1234252c9",
		},
	}

	for _, test := range defaultConflictResolverWithTombstoneTests {
		t.Run(test.name, func(t *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: channels.DocChannelsSyncFunction,
				})
			defer rt2.Close()

			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Active
			rt1 := rest.NewRestTester(t, nil)
			defer rt1.Close()
			ctx1 := rt1.Context()

			defaultConflictResolver, err := db.NewCustomConflictResolver(
				ctx1, `function(conflict) { return defaultPolicy(conflict); }`, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err, "Error creating custom conflict resolver")
			config := db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: userDBURL(rt2, username),
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				Continuous:             true,
				ConflictResolverFunc:   defaultConflictResolver,
				ReplicationStatsMap:    dbReplicatorStats(t),
				CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
				SupportedBLIPProtocols: []string{db.CBMobileReplicationV3.SubprotocolString()}, // only relevant for v3 and below replications
			}

			// Create the first revision of the document on rt2.
			docID := test.name + "foo"
			rt2VersionCreated := createDoc(rt2, docID, "foo")

			// Create active replicator and start replication.
			ar, err := db.NewActiveReplicator(ctx1, &config)
			require.NoError(t, err)
			require.NoError(t, ar.Start(ctx1), "Error starting replication")
			defer func() { require.NoError(t, ar.Stop(), "Error stopping replication") }()

			rt1.WaitForVersionRevIDOnly(docID, rt2VersionCreated)

			// Stop replication.
			require.NoError(t, ar.Stop(), "Error stopping replication")

			// Update the document on rt2 to build a revision history.
			rt2VersionCreated.CV = db.Version{} // need to clear cv to update with revID not CV
			rt2VersionUpdated := updateDoc(rt2, docID, rt2VersionCreated, "bar")

			// Tombstone the document on rt2 to mark the tip of the revision history for deletion.
			rt2VersionUpdated.CV = db.Version{} // need to clear cv to update with revID not CV
			tombstoneVersion := rt2.DeleteDoc(docID, rt2VersionUpdated)

			// Ensure that the tombstone revision is written to rt2 bucket with an empty body.
			rt2.WaitForTombstoneRevIDOnly(docID, tombstoneVersion)

			// Update the document on rt1 with the specified body values.
			rt1Version := rt2VersionCreated
			for _, bodyValue := range test.localBodyValues {
				rt1Version = updateDoc(rt1, docID, rt1Version, bodyValue)
				rt1Version.CV = db.Version{} // need to clear cv to update with revID not CV
			}

			// Start replication.
			require.NoError(t, ar.Start(ctx1), "Error starting replication")

			// Wait for default conflict resolution policy to be applied through replication and
			// the winning revision to be written to both rt1 and rt2 buckets. Check whether the
			// winning revision is a tombstone; tombstone revision wins over non-tombstone revision.

			expectedVersion := rest.NewDocVersionFromFakeRev(test.expectedRevID)
			rt1.WaitForTombstoneRevIDOnly(docID, expectedVersion)
			rt2.WaitForTombstoneRevIDOnly(docID, expectedVersion)
		})
	}
}

// TestLocalWinsConflictResolution:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Validates document metadata (deleted, attachments) are preserved during LocalWins conflict
//     resolution, when local rev is rewritten as child of remote
func TestLocalWinsConflictResolution(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only (non-default conflict resolver)")
	}

	type revisionState struct {
		generation       int
		propertyValue    string // test creates documents with body like {"prop": propertyValue}
		deleted          bool
		attachmentRevPos int
	}

	newRevisionState := func(generation int, propertyValue string, deleted bool, attachmentRevPos int) revisionState {
		return revisionState{
			generation:       generation,
			deleted:          deleted,
			attachmentRevPos: attachmentRevPos,
			propertyValue:    propertyValue,
		}
	}

	// makeRevBody creates a revision body with a value "prop" equal to property value, with an attachment
	// if attachmentRevPos is specified.
	makeRevBody := func(propertyValue string, attachmentRevPos, generation int) string {
		// No attachment if revpos==0 or is greater than current generation
		if attachmentRevPos == 0 || generation < attachmentRevPos {
			return fmt.Sprintf(`{"prop": %q}`, propertyValue)
		}

		// Create as new attachment if revpos matches generation
		if attachmentRevPos == generation {
			return fmt.Sprintf(`{"prop": %q, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`, propertyValue)
		}

		// Otherwise include attachment as digest/stub/revpos
		return fmt.Sprintf(`{"prop": %q, "_attachments": {"hello.txt": {"stub":true,"revpos":%d,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`, propertyValue, attachmentRevPos)
	}

	conflictResolutionTests := []struct {
		name           string
		initialState   revisionState // Initial revision state on both nodes before conflict is introduced
		localMutation  revisionState // Revision state post-mutation on local node
		remoteMutation revisionState // Revision state post-mutation on remote node
		expectedResult revisionState // Expected revision state after conflict resolution and replication
	}{
		{
			// simpleMutation mutates remote and local
			name:           "simpleMutation",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(4, "b", false, 0),
			remoteMutation: newRevisionState(4, "c", false, 0),
			expectedResult: newRevisionState(5, "b", false, 0),
		},
		{
			// simpleMutation mutates local and tombstones remote, validates _deleted is applied
			name:           "mutateLocal_tombstoneRemote",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(4, "b", false, 0),
			remoteMutation: newRevisionState(4, "c", true, 0),
			expectedResult: newRevisionState(5, "b", false, 0),
		},
		{
			// simpleMutation includes an attachment prior to conflict, validates it's preserved
			name:           "localAttachment",
			initialState:   newRevisionState(3, "a", false, 2),
			localMutation:  newRevisionState(4, "b", false, 0),
			remoteMutation: newRevisionState(4, "c", false, 0),
			expectedResult: newRevisionState(5, "b", false, 3), // revpos==3 here because the revision isn't replicated until rev 3
		},
		{
			// localAttachmentPostConflict adds a local attachment on a conflicting branch
			name:           "localAttachmentPostConflict",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(6, "b", false, 5),
			remoteMutation: newRevisionState(6, "c", false, 0),
			expectedResult: newRevisionState(7, "b", false, 7),
		},
		{
			// remoteAttachmentPostConflict adds a remote attachment on a conflicting branch
			name:           "remoteAttachmentPostConflict",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(6, "b", false, 0),
			remoteMutation: newRevisionState(6, "c", false, 5),
			expectedResult: newRevisionState(7, "b", false, 0),
		},
		{
			// remoteAttachmentPostConflict adds the same attachment to local and remote conflicting branches
			name:           "conflictingDocMatchingAttachmentPostConflict",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(6, "b", false, 4),
			remoteMutation: newRevisionState(6, "c", false, 5),
			expectedResult: newRevisionState(7, "b", false, 5),
		},
	}

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range conflictResolutionTests {
			t.Run(test.name, func(t *testing.T) {
				base.RequireNumTestBuckets(t, 2)

				activeRT, remoteRT, remoteURLString := sgrRunner.SetupSGRPeers(t)

				// Create initial revision(s) on local
				docID := test.name

				var newVersion rest.DocVersion
				var parentVersion *rest.DocVersion
				for gen := 1; gen <= test.initialState.generation; gen++ {
					newVersion = rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-initial", gen))
					parentVersion = activeRT.PutNewEditsFalse(docID, newVersion, parentVersion,
						makeRevBody(test.initialState.propertyValue, test.initialState.attachmentRevPos, gen))
				}

				// Create replication, wait for initial revision to be replicated
				replicationID := test.name
				activeRT.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePushAndPull, nil, true, db.ConflictResolverLocalWins, "")
				activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

				sgrRunner.WaitForVersion(docID, remoteRT, newVersion)

				// Stop the replication
				response := activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
				rest.RequireStatus(t, response, http.StatusOK)
				activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

				rawResponse := activeRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
				t.Logf("-- local raw pre-update: %s", rawResponse.Body.Bytes())
				rawResponse = remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
				t.Logf("-- remote raw pre-update: %s", rawResponse.Body.Bytes())

				// Update local and remote revisions
				localParentVersion := &newVersion
				var newLocalVersion rest.DocVersion
				for localGen := test.initialState.generation + 1; localGen <= test.localMutation.generation; localGen++ {
					// If deleted=true, tombstone on the last mutation
					if test.localMutation.deleted == true && localGen == test.localMutation.generation {
						activeRT.DeleteDoc(docID, newVersion)
						continue
					}

					newLocalVersion = rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-local", localGen))
					// Local rev pos is greater of initial state revpos and localMutation rev pos
					localRevPos := test.initialState.attachmentRevPos
					if test.localMutation.attachmentRevPos > 0 {
						localRevPos = test.localMutation.attachmentRevPos
					}
					localParentVersion = activeRT.PutNewEditsFalse(docID, newLocalVersion, localParentVersion, makeRevBody(test.localMutation.propertyValue, localRevPos, localGen))
				}

				remoteParentVersion := &newVersion
				var newRemoteVersion rest.DocVersion
				for remoteGen := test.initialState.generation + 1; remoteGen <= test.remoteMutation.generation; remoteGen++ {
					// If deleted=true, tombstone on the last mutation
					if test.remoteMutation.deleted == true && remoteGen == test.remoteMutation.generation {
						remoteRT.DeleteDoc(docID, newVersion)
						continue
					}
					newRemoteVersion = rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-remote", remoteGen))

					// Local rev pos is greater of initial state revpos and remoteMutation rev pos
					remoteRevPos := test.initialState.attachmentRevPos
					if test.remoteMutation.attachmentRevPos > 0 {
						remoteRevPos = test.remoteMutation.attachmentRevPos
					}
					remoteParentVersion = remoteRT.PutNewEditsFalse(docID, newRemoteVersion, remoteParentVersion, makeRevBody(test.remoteMutation.propertyValue, remoteRevPos, remoteGen))
				}

				rawResponse = activeRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
				t.Logf("-- local raw pre-replication: %s", rawResponse.Body.Bytes())
				rawResponse = remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
				t.Logf("-- remote raw pre-replication: %s", rawResponse.Body.Bytes())

				// Restart the replication
				response = activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=start", "")
				rest.RequireStatus(t, response, http.StatusOK)

				// Wait for expected property value on remote to determine replication complete
				waitErr := remoteRT.WaitForCondition(func() bool {
					var remoteDoc db.Body
					rawResponse := remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
					require.NoError(t, base.JSONUnmarshal(rawResponse.Body.Bytes(), &remoteDoc))
					prop, ok := remoteDoc["prop"].(string)
					t.Logf("-- Waiting for property: %v, got property: %v", test.expectedResult.propertyValue, prop)
					return ok && prop == test.expectedResult.propertyValue
				})
				require.NoError(t, waitErr)

				localDoc := activeRT.GetDocBody(docID)
				localRevID := localDoc.ExtractRev()
				remoteDoc := remoteRT.GetDocBody(docID)
				remoteRevID := remoteDoc.ExtractRev()

				assert.Equal(t, localRevID, remoteRevID) // local and remote rev IDs must match
				localGeneration, _ := db.ParseRevID(activeRT.Context(), localRevID)
				assert.Equal(t, test.expectedResult.generation, localGeneration)               // validate expected generation
				assert.Equal(t, test.expectedResult.propertyValue, remoteDoc["prop"].(string)) // validate expected body
				assert.Equal(t, test.expectedResult.propertyValue, localDoc["prop"].(string))  // validate expected body

				remoteRevpos := getTestRevpos(t, remoteDoc, "hello.txt")
				assert.Equal(t, test.expectedResult.attachmentRevPos, remoteRevpos) // validate expected revpos

				rawResponse = activeRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
				t.Logf("-- local raw post-replication: %s", rawResponse.Body.Bytes())

				rawResponse = remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
				t.Logf("-- remote raw post-replication: %s", rawResponse.Body.Bytes())
			})
		}
	})
}

// This test can be used for testing replication to a pre-hydrogen SGR target. The test itself simply has a passive and
// active node and attempts to replicate and expects the replicator to enter an error state. The intention is that the
// passive side emulates a pre-hydrogen target by having ignoreNoConflicts set to false. In order to use this test this
// flag should be hardcoded during development. This can be set inside the sendChangesOptions struct under the _connect
// method in active_replicator_push.go
func TestSendChangesToNoConflictPreHydrogenTarget(t *testing.T) {
	t.Skip("Test is only for development purposes")

	base.RequireNumTestBuckets(t, 2)

	errorCountBefore := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value()

	// Passive
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				AllowConflicts: base.Ptr(false),
			}},
		})
	defer rt2.Close()

	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          "test",
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: adminDBURL(rt2),
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		InsecureSkipVerify:  true,
		ReplicationStatsMap: dbReplicatorStats(t),
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)

	defer func() {
		require.NoError(t, ar.Stop())
	}()
	require.NoError(t, ar.Start(ctx1))

	assert.Equal(t, errorCountBefore, base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value())

	response := rt1.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", "{}")
	rest.RequireStatus(t, response, http.StatusCreated)

	err = rt2.WaitForCondition(func() bool {
		return base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value() == errorCountBefore+1
	})
	assert.NoError(t, err)

	assert.Equal(t, db.ReplicationStateStopped, ar.GetStatus(ctx1).Status)
	assert.Equal(t, db.PreHydrogenTargetAllowConflictsError.Error(), ar.GetStatus(ctx1).ErrorMessage)
}
func TestReplicatorConflictAttachment(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	if !base.IsEnterpriseEdition() {
		t.Skipf("requires enterprise edition")
	}

	testCases := []struct {
		name                      string
		conflictResolution        db.ConflictResolverType
		expectedFinalVersion      rest.DocVersion
		expectedRevPos            int
		expectedAttachmentContent string
	}{
		{
			name:                      "local",
			conflictResolution:        db.ConflictResolverLocalWins,
			expectedFinalVersion:      rest.NewDocVersionFromFakeRev("6-3545745ab68aec5b00e745f9e0e3277c"),
			expectedRevPos:            6,
			expectedAttachmentContent: "hello world",
		},
		{
			name:                      "remote",
			conflictResolution:        db.ConflictResolverRemoteWins,
			expectedFinalVersion:      rest.NewDocVersionFromFakeRev("5-remote"),
			expectedRevPos:            4,
			expectedAttachmentContent: "goodbye cruel world",
		},
	}
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		for _, test := range testCases {
			t.Run(test.name, func(t *testing.T) {
				activeRT, passiveRT, passiveDBURL := sgrRunner.SetupSGRPeers(t)

				docID := test.name

				var newVersion rest.DocVersion
				var parentVersion *rest.DocVersion
				for gen := 1; gen <= 3; gen++ {
					newVersion = rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-initial", gen))
					parentVersion = activeRT.PutNewEditsFalse(docID, newVersion, parentVersion, "{}")
				}

				replicationID := "replication_" + rest.SafeDocumentName(t, t.Name())
				activeRT.CreateReplication(replicationID, passiveDBURL, db.ActiveReplicatorTypePushAndPull, nil, true, test.conflictResolution, "")
				defer activeRT.DeleteReplication(replicationID)
				activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

				passiveRT.WaitForVersion(docID, newVersion)

				response := activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
				rest.RequireStatus(t, response, http.StatusOK)
				activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

				nextGen := 4

				localGen := nextGen
				localParentVersion := newVersion
				newLocalVersion := rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-local", localGen))
				_ = activeRT.PutNewEditsFalse(docID, newLocalVersion, &localParentVersion, `{"_attachments": {"attach": {"data":"aGVsbG8gd29ybGQ="}}}`)
				localParentVersion = newLocalVersion

				localGen++
				newLocalVersion = rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-local", localGen))
				_ = activeRT.PutNewEditsFalse(docID, newLocalVersion, &localParentVersion, fmt.Sprintf(`{"_attachments": {"attach": {"stub": true, "revpos": %d, "digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`, localGen-1))

				remoteGen := nextGen
				remoteParentVersion := newVersion
				newRemoteVersion := rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-remote", remoteGen))
				_ = passiveRT.PutNewEditsFalse(docID, newRemoteVersion, &remoteParentVersion, `{"_attachments": {"attach": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`)
				remoteParentVersion = newRemoteVersion

				remoteGen++
				newRemoteVersion = rest.NewDocVersionFromFakeRev(fmt.Sprintf("%d-remote", remoteGen))
				remoteWinsVersion := passiveRT.PutNewEditsFalse(docID, newRemoteVersion, &remoteParentVersion, fmt.Sprintf(`{"_attachments": {"attach": {"stub": true, "revpos": %d, "digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`, remoteGen-1))

				response = activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=start", "")
				rest.RequireStatus(t, response, http.StatusOK)

				expVersion := test.expectedFinalVersion
				if sgrRunner.IsV4Protocol() {
					if test.conflictResolution == db.ConflictResolverRemoteWins {
						expVersion = *remoteWinsVersion
					} else {
						activeCollection, activeCtx := activeRT.GetSingleTestDatabaseCollectionWithUser()
						doc, err := activeCollection.GetDocument(activeCtx, docID, db.DocUnmarshalAll)
						require.NoError(t, err)
						expVersion.CV = db.Version{
							SourceID: activeRT.GetDatabase().EncodedSourceID,
							Value:    doc.Cas,
						}
					}
				}
				activeRT.WaitForVersion(docID, expVersion)
				passiveRT.WaitForVersion(docID, expVersion)

				localDoc := activeRT.GetDocBody(docID)
				localVersion := localDoc.ExtractRev()

				remoteDoc := passiveRT.GetDocBody(docID)
				remoteVersion := remoteDoc.ExtractRev()

				assert.Equal(t, localVersion, remoteVersion)
				remoteRevpos := getTestRevpos(t, remoteDoc, "attach")
				assert.Equal(t, test.expectedRevPos, remoteRevpos)

				response = activeRT.SendAdminRequest("GET", "/{{.keyspace}}/"+docID+"/attach", "")
				assert.Equal(t, test.expectedAttachmentContent, string(response.BodyBytes()))
			})
		}
	})
}

func TestConflictResolveMergeWithMutatedRev(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		// Passive
		rt1, rt2, _ := sgrRunner.SetupSGRPeers(t)
		ctx1 := rt1.Context()

		customConflictResolver, err := db.NewCustomConflictResolver(ctx1, `function(conflict){
			var mutatedLocal = conflict.LocalDocument;
			mutatedLocal.source = "merged";
			mutatedLocal["_deleted"] = true;
			mutatedLocal["_rev"] = "";
			return mutatedLocal;
		}`, rt1.GetDatabase().Options.JavascriptTimeout)
		require.NoError(t, err)

		id := rest.SafeDocumentName(t, t.Name())
		sgwStats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := sgwStats.DBReplicatorStats(id)
		require.NoError(t, err)

		ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePull,
			RemoteDBURL: adminDBURL(rt2),
			ActiveDB: &db.Database{
				DatabaseContext: rt1.GetDatabase(),
			},
			Continuous:                 false,
			ReplicationStatsMap:        dbstats,
			ConflictResolutionType:     db.ConflictResolverCustom,
			ConflictResolverFunc:       customConflictResolver,
			ConflictResolverFuncForHLV: customConflictResolver,
			CollectionsEnabled:         !rt1.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols:     sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)

		rt2.PutDoc("doc", "{}")
		rt2.WaitForPendingChanges()
		rt1.PutDoc("doc", `{"some_val": "val"}`)
		rt1.WaitForPendingChanges()

		require.NoError(t, ar.Start(ctx1))

		base.RequireWaitForStat(t, func() int64 {
			dbRepStats, err := base.SyncGatewayStats.DbStats[id].DBReplicatorStats(ar.ID)
			require.NoError(t, err)
			return dbRepStats.PulledCount.Value()
		}, 1)

		rt1.WaitForReplicationStatus(id, db.ReplicationStateStopped)
	})
}

// CBG-1427 - ISGR should not try sending a delta when deltaSrc is a tombstone
func TestReplicatorDoNotSendDeltaWhenSrcIsTombstone(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for delta sync")
	}

	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {

		activeRT, passiveRT, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			ActiveRestTesterConfig: &rest.RestTesterConfig{
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						DeltaSync: &rest.DeltaSyncConfig{
							Enabled: base.Ptr(true),
						},
					},
				},
			},
			PassiveRestTesterConfig: &rest.RestTesterConfig{
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						DeltaSync: &rest.DeltaSyncConfig{
							Enabled: base.Ptr(true),
						},
					},
				},
			},
		})
		activeCtx := activeRT.Context()

		// Create a document //
		version := activeRT.PutDoc("test", `{"field1":"f1_1","field2":"f2_1"}`)
		activeRT.WaitForVersion("test", version)

		// Set-up replicator //
		id := rest.SafeDocumentName(t, t.Name())
		sgwStats, err := base.SyncGatewayStats.NewDBStats(id, false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := sgwStats.DBReplicatorStats(id)
		require.NoError(t, err)

		ar, err := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: adminDBURL(passiveRT),
			ActiveDB: &db.Database{
				DatabaseContext: activeRT.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       1,
			DeltasEnabled:          true,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !activeRT.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		assert.Equal(t, "", ar.GetStatus(activeCtx).LastSeqPush)
		require.NoError(t, ar.Start(activeCtx))

		// Wait for active to replicate to passive
		sgrRunner.WaitForVersion("test", passiveRT, version)

		// Delete active document
		deletedVersion := activeRT.DeleteDoc("test", version)

		// Assert that the tombstone is replicated to passive
		// Get revision 2 on passive peer to assert it has been (a) replicated and (b) deleted
		sgrRunner.WaitForTombstone("test", passiveRT, deletedVersion)

		// Resurrect tombstoned document
		resurrectedVersion := activeRT.UpdateDoc("test", deletedVersion, `{"field2":"f2_2"}`)

		// Replicate resurrection to passive
		sgrRunner.WaitForVersion("test", passiveRT, resurrectedVersion)

		dbRepStats, err := base.SyncGatewayStats.DbStats[id].DBReplicatorStats(ar.ID)
		require.NoError(t, err)
		base.RequireWaitForStat(t, func() int64 {
			// should be 1 given delta is sent for delete, but not for resurrection
			return dbRepStats.PushDeltaSentCount.Value()
		}, 1)

		// Shutdown replicator to close out
		require.NoError(t, ar.Stop())
		activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
	})
}

// CBG-1672 - Return 422 status for unprocessable deltas instead of 404 to use non-delta retry handling
// Should log "422 Unable to unmarshal mutable body for doc test deltaSrc=1-dbc7919edc9ec2576d527880186f8e8a"
// then fall back to full body replication
func TestUnprocessableDeltas(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for some delta sync")
	}
	if base.TestDisableRevCache() {
		t.Skipf("Test requires altering of rev cache values")
	}

	// need Sync debugging due to AssertLogContains below
	base.SetUpTestLogging(t, base.LevelDebug, base.KeySync)
	base.RequireNumTestBuckets(t, 2)
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		restartBatching := db.SuspendSequenceBatching()
		t.Cleanup(restartBatching)
		activeRT, passiveRT, _ := sgrRunner.SetupSGRPeersWithOptions(t, rest.TestISGRPeerOpts{
			ActiveRestTesterConfig: &rest.RestTesterConfig{
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						DeltaSync: &rest.DeltaSyncConfig{
							Enabled: base.Ptr(true),
						},
					},
				},
			},
			PassiveRestTesterConfig: &rest.RestTesterConfig{
				DatabaseConfig: &rest.DatabaseConfig{
					DbConfig: rest.DbConfig{
						DeltaSync: &rest.DeltaSyncConfig{
							Enabled: base.Ptr(true),
						},
					},
				},
			},
		})

		activeCtx := activeRT.Context()

		// Create a document //
		version := activeRT.PutDoc("test", `{"field1":"f1_1","field2":"f2_1"}`)
		activeRT.WaitForVersion("test", version)

		id := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: adminDBURL(passiveRT),
			ActiveDB: &db.Database{
				DatabaseContext: activeRT.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       200,
			DeltasEnabled:          true,
			ReplicationStatsMap:    dbReplicatorStats(t),
			CollectionsEnabled:     !activeRT.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		assert.Equal(t, "", ar.GetStatus(activeCtx).LastSeqPush)

		require.NoError(t, ar.Start(activeCtx))

		sgrRunner.WaitForVersion("test", passiveRT, version)

		require.NoError(t, ar.Stop())

		// Make 2nd revision
		version2 := activeRT.UpdateDoc("test", version, `{"field1":"f1_2","field2":"f2_2"}`)
		activeRT.WaitForPendingChanges()

		passiveRTCollection, passiveRTCtx := passiveRT.GetSingleTestDatabaseCollection()
		rev, err := passiveRTCollection.GetRevisionCacheForTest().GetActive(passiveRTCtx, "test")
		require.NoError(t, err)
		// Making body invalid to trigger log "Unable to unmarshal mutable body for doc" in handleRev
		// Which should give a HTTP 422
		rev.BodyBytes = []byte("{invalid}")
		passiveRTCollection.GetRevisionCacheForTest().Upsert(base.TestCtx(t), rev)

		base.AssertLogContains(t, "Unable to unmarshal mutable body for doc test", func() {
			require.NoError(t, ar.Start(activeCtx))
			// Check if it replicated
			sgrRunner.WaitForVersion("test", passiveRT, version2)
			require.NoError(t, ar.Stop())
		})
	})
}

// CBG-1428 - check for regression of ISGR not ignoring _removed:true bodies when purgeOnRemoval is disabled
func TestReplicatorIgnoreRemovalBodies(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	// Copies the behaviour of TestGetRemovedAsUser but with replication and no user
	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		restartBatching := db.SuspendSequenceBatching()
		t.Cleanup(restartBatching)

		activeRT, passiveRT, _ := sgrRunner.SetupSGRPeers(t)
		activeCtx := activeRT.Context()
		collection, _ := activeRT.GetSingleTestDatabaseCollection()

		docID := rest.SafeDocumentName(t, t.Name())
		// Create the docs //
		// Doc rev 1
		version1 := activeRT.PutDoc(docID, `{"key":"12","channels": ["rev1chan"]}`)
		sgrRunner.WaitForVersion(docID, activeRT, version1)

		// doc rev 2
		version2 := activeRT.UpdateDoc(docID, version1, `{"key":"12","channels":["rev2+3chan"]}`)
		sgrRunner.WaitForVersion(docID, activeRT, version2)

		// Doc rev 3
		version3 := activeRT.UpdateDoc(docID, version2, `{"key":"3","channels":["rev2+3chan"]}`)
		sgrRunner.WaitForVersion(docID, activeRT, version3)

		activeRT.GetDatabase().FlushRevisionCacheForTest()
		err := collection.PurgeOldRevisionJSON(activeCtx, docID, version2.RevTreeID)
		require.NoError(t, err)

		id := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: adminDBURL(passiveRT),
			ActiveDB: &db.Database{
				DatabaseContext: activeRT.GetDatabase(),
			},
			Continuous:             false,
			ChangesBatchSize:       200,
			ReplicationStatsMap:    dbReplicatorStats(t),
			PurgeOnRemoval:         false,
			Filter:                 base.ByChannelFilter,
			FilterChannels:         []string{"rev1chan"},
			CollectionsEnabled:     !activeRT.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		docWriteFailuresBefore := ar.GetStatus(activeCtx).DocWriteFailures

		require.NoError(t, ar.Start(activeCtx))
		activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

		assert.Equal(t, docWriteFailuresBefore, ar.GetStatus(activeCtx).DocWriteFailures, "ISGR should ignore _remove:true bodies when purgeOnRemoval is disabled. CBG-1428 regression.")
	})
}

// CBG-1995: Test the support for using an underscore prefix in the top-level body of a document
// Tests replication and Rest API
func TestUnderscorePrefixSupport(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, passiveRT, _ := sgrRunner.SetupSGRPeers(t)

		activeCtx := activeRT.Context()

		// Create the document
		docID := rest.SafeDocumentName(t, t.Name())
		rawDoc := `{"_foo": true, "_exp": 120, "true": false, "_attachments": {"bar": {"data": "Zm9vYmFy"}}}`
		_ = activeRT.PutDoc(docID, rawDoc)

		// Set-up replicator
		id := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
			ID:          id,
			Direction:   db.ActiveReplicatorTypePush,
			RemoteDBURL: adminDBURL(passiveRT),
			ActiveDB: &db.Database{
				DatabaseContext: activeRT.GetDatabase(),
			},
			Continuous:             true,
			ChangesBatchSize:       200,
			ReplicationStatsMap:    dbReplicatorStats(t),
			PurgeOnRemoval:         false,
			CollectionsEnabled:     !activeRT.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)
		defer func() { require.NoError(t, ar.Stop()) }()

		require.NoError(t, ar.Start(activeCtx))
		activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

		// Confirm document is replicated
		changesResults := passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)

		passiveRT.WaitForPendingChanges()

		require.NoError(t, ar.Stop())

		// Assert document was replicated successfully
		doc := passiveRT.GetDocBody(docID)
		assert.EqualValues(t, true, doc["_foo"])  // Confirm user defined value got created
		assert.EqualValues(t, nil, doc["_exp"])   // Confirm expiry was consumed
		assert.EqualValues(t, false, doc["true"]) // Sanity check normal keys
		// Confirm attachment was created successfully
		resp := passiveRT.SendAdminRequest("GET", "/{{.keyspace}}/"+docID+"/bar", "")
		rest.RequireStatus(t, resp, 200)

		// Edit existing document
		rev := doc["_rev"]
		require.NotNil(t, rev)
		rawDoc = fmt.Sprintf(`{"_rev": "%s","_foo": false, "test": true}`, rev)
		_ = activeRT.PutDoc(docID, rawDoc)

		// Replicate modified document
		require.NoError(t, ar.Start(activeCtx))
		activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

		changesResults = passiveRT.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", changesResults.Last_Seq), "", true)

		passiveRT.WaitForPendingChanges()

		// Verify document replicated successfully
		doc = passiveRT.GetDocBody(docID)
		assert.NotEqualValues(t, doc["_rev"], rev) // Confirm rev got replaced with new rev
		assert.EqualValues(t, false, doc["_foo"])  // Confirm user defined value got created
		assert.EqualValues(t, true, doc["test"])
		// Confirm attachment was removed successfully in latest revision
		resp = passiveRT.SendAdminRequest("GET", "/{{.keyspace}}/"+docID+"/bar", "")
		rest.RequireStatus(t, resp, 404)

		// Add disallowed _removed tag in document
		rawDoc = fmt.Sprintf(`{"_rev": "%s","_removed": false}`, doc["_rev"])
		resp = activeRT.SendAdminRequest("PUT", "/{{.keyspace}}/"+docID, rawDoc)
		rest.RequireStatus(t, resp, 404)

		// Add disallowed _purged tag in document
		rawDoc = fmt.Sprintf(`{"_rev": "%s","_purged": true}`, doc["_rev"])
		resp = activeRT.SendAdminRequest("PUT", "/{{.keyspace}}/"+docID, rawDoc)
		rest.RequireStatus(t, resp, 400)
	})
}

// TestActiveReplicatorBlipsync uses an ActiveReplicator with another RestTester instance to connect and cleanly disconnect.
func TestActiveReplicatorBlipsync(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		_, rt, passiveDBURL := sgrRunner.SetupSGRPeers(t)
		remoteURL, err := url.Parse(passiveDBURL)
		require.NoError(t, err)
		ctx := rt.Context()

		stats, err := base.SyncGatewayStats.NewDBStats("test", false, false, false, nil, nil)
		require.NoError(t, err)
		dbstats, err := stats.DBReplicatorStats(t.Name())
		require.NoError(t, err)

		id := rest.SafeDocumentName(t, t.Name())
		ar, err := db.NewActiveReplicator(ctx, &db.ActiveReplicatorConfig{
			ID:                     id,
			Direction:              db.ActiveReplicatorTypePushAndPull,
			ActiveDB:               &db.Database{DatabaseContext: rt.GetDatabase()},
			RemoteDBURL:            remoteURL,
			Continuous:             true,
			ReplicationStatsMap:    dbstats,
			CollectionsEnabled:     !rt.GetDatabase().OnlyDefaultCollection(),
			SupportedBLIPProtocols: sgrRunner.SupportedSubprotocols,
		})
		require.NoError(t, err)

		startNumReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
		startNumReplicationsActive := rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()

		// Start the replicator (implicit connect)
		require.NoError(t, ar.Start(ctx))

		// Check total stat
		numReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
		assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)

		// Check active stat
		assert.Equal(t, startNumReplicationsActive+2, rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value())

		// Close the replicator (implicit disconnect)
		require.NoError(t, ar.Stop())

		// Wait for active stat to drop to original value
		base.RequireWaitForStat(t, func() int64 {
			return rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()
		}, startNumReplicationsActive)

		// Verify total stat has not been decremented
		numReplicationsTotal = rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
		assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)
	})
}

func TestBlipSyncNonUpgradableConnection(t *testing.T) {
	base.LongRunningTest(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {Password: base.Ptr("pass")},
			},
		}},
	})
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	server := httptest.NewServer(rt.TestPublicHandler())
	defer server.Close()
	dbURL, err := url.Parse(server.URL + "/db/_blipsync")
	require.NoError(t, err)

	// Add basic auth credentials to target db URL
	dbURL.User = url.UserPassword("alice", "pass")
	request, err := http.NewRequest(http.MethodGet, dbURL.String(), nil)
	require.NoError(t, err, "Error creating new request")

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err, "Error sending request")
	defer func() {
		assert.NoError(t, response.Body.Close())
	}()
	require.Equal(t, http.StatusUpgradeRequired, response.StatusCode)
}

// Test that the username and password fields in the replicator still work and get redacted appropriately.
// This should log a deprecation notice.
func TestReplicatorDeprecatedCredentials(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	passiveRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					"alice": {
						Password: base.Ptr("pass"),
					},
				},
			},
		},
		})
	defer passiveRT.Close()

	adminSrv := httptest.NewServer(passiveRT.TestPublicHandler())
	defer adminSrv.Close()

	activeRT := rest.NewRestTester(t, nil)
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
	require.NoError(t, err)

	docID := "test"
	version := activeRT.PutDoc(docID, `{"prop":true}`)

	replConfig := `
{
	"replication_id": "` + t.Name() + `",
	"remote": "` + adminSrv.URL + `/db",
	"direction": "push",
	"continuous": true,
	"username": "alice",
	"password": "pass",
	"collections_enabled": ` + strconv.FormatBool(!activeRT.GetDatabase().OnlyDefaultCollection()) + `
}
`
	resp := activeRT.SendAdminRequest("POST", "/{{.db}}/_replication/", replConfig)
	rest.RequireStatus(t, resp, 201)

	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateRunning)

	passiveRT.WaitForVersion(docID, version)

	resp = activeRT.SendAdminRequest("GET", "/{{.db}}/_replication/"+t.Name(), "")
	rest.RequireStatus(t, resp, 200)

	var config db.ReplicationConfig
	err = json.Unmarshal(resp.BodyBytes(), &config)
	require.NoError(t, err)
	assert.Equal(t, "alice", config.Username)
	assert.Equal(t, base.RedactedStr, config.Password)
	assert.Equal(t, "", config.RemoteUsername)
	assert.Equal(t, "", config.RemotePassword)

	_, _, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(activeRT.Context(), t.Name(), "stop")
	require.NoError(t, err)
	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)
	err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(t.Name())
	require.NoError(t, err)
}

// CBG-1581: Ensure activeReplicatorCommon does final checkpoint on stop/disconnect
func TestReplicatorCheckpointOnStop(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, passiveRT, remoteURL := sgrRunner.SetupSGRPeers(t)

		replicationID := rest.SafeDocumentName(t, t.Name())

		// increase checkpointing interval temporarily to ensure the checkpointer doesn't fire on an
		// interval during the running of the test
		reduceCheckpointInterval := reduceTestCheckpointInterval(9999 * time.Hour)
		t.Cleanup(reduceCheckpointInterval)

		collection, ctx := activeRT.GetSingleTestDatabaseCollectionWithUser()
		revID, doc, err := collection.Put(ctx, "test", db.Body{})
		require.NoError(t, err)
		seq := strconv.FormatUint(doc.Sequence, 10)

		activeRT.CreateReplication(replicationID, remoteURL, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault, "")
		activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

		sgrRunner.WaitForVersion("test", passiveRT, rest.DocVersion{RevTreeID: revID, CV: *doc.HLV.ExtractCurrentVersionFromHLV()})

		// assert on the processed seq list being updated before stopping the active replicator
		ar, ok := activeRT.GetDatabase().SGReplicateMgr.GetLocalActiveReplicatorForTest(t, replicationID)
		assert.True(t, ok)
		pullCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer
		base.RequireWaitForStat(t, func() int64 {
			return pullCheckpointer.Stats().ProcessedSequenceCount
		}, 1)

		// stop active replicator explicitly
		require.NoError(t, ar.Stop())

		// Check checkpoint document was wrote to bucket with correct status
		// _sync:local:checkpoint/sgr2cp:push:TestReplicatorCheckpointOnStop
		expectedCheckpointName := base.SyncDocPrefix + "local:checkpoint/" + db.PushCheckpointID(replicationID)
		lastSeq, err := activeRT.WaitForCheckpointLastSequence(expectedCheckpointName)
		require.NoError(t, err)
		assert.Equal(t, seq, lastSeq)

		err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(replicationID)
		require.NoError(t, err)
	})
}

// Tests replications to make sure they are namespaced by group ID
func TestGroupIDReplications(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test requires xattrs")
	}
	if !base.IsEnterpriseEdition() {
		t.Skip("Requires EE to use GroupID")
	}
	base.RequireNumTestBuckets(t, 2)

	ctx := base.TestCtx(t)
	// Create test buckets to replicate between
	activeBucket := base.GetTestBucket(t)
	defer activeBucket.Close(ctx)

	// Set up passive bucket RT
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	passiveDBURL := adminDBURL(rt)

	// Start SG nodes for default group, group A and group B
	groupIDs := []string{"", "GroupA", "GroupB"}
	serverContexts := make([]*rest.ServerContext, 0, len(groupIDs))
	for _, group := range groupIDs {

		config := rest.BootstrapStartupConfigForTest(t)
		uniqueUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		config.Bootstrap.ConfigGroupID = group + uniqueUUID.String()
		sc, closeFn := rest.StartServerWithConfig(t, &config)
		defer closeFn()

		serverContexts = append(serverContexts, sc)

		dbConfig := rest.DbConfig{
			AutoImport: true,
			BucketConfig: rest.BucketConfig{
				Bucket: base.Ptr(activeBucket.GetName()),
			},
			EnableXattrs: base.Ptr(base.TestUseXattrs()),
		}
		if !base.UnitTestUrlIsWalrus() {
			dbConfig.UseViews = base.Ptr(base.TestsDisableGSI())
		}

		if rt.GetDatabase().OnlyDefaultCollection() {
			dbConfig.Sync = base.Ptr(channels.DocChannelsSyncFunction)
		} else {
			dbConfig.Scopes = rest.GetCollectionsConfigWithFiltering(rt.TB(), rt.TestBucket, 1, base.Ptr(channels.DocChannelsSyncFunction), nil)
		}
		dbcJSON, err := base.JSONMarshal(dbConfig)
		require.NoError(t, err)

		resp := rest.BootstrapAdminRequest(t, sc, http.MethodPut, "/db/", string(dbcJSON))
		resp.RequireStatus(http.StatusCreated)
	}
	// Start replicators
	for i, group := range groupIDs {
		channelFilter := []string{"chan" + group}
		replicationConfig := db.ReplicationConfig{
			ID:                     "repl",
			Remote:                 passiveDBURL.String(),
			Direction:              db.ActiveReplicatorTypePush,
			Filter:                 base.ByChannelFilter,
			QueryParams:            map[string]any{"channels": channelFilter},
			Continuous:             true,
			InitialState:           db.ReplicationStateRunning,
			ConflictResolutionType: db.ConflictResolverDefault,
			CollectionsEnabled:     !rt.GetDatabase().OnlyDefaultCollection(),
		}
		resp := rest.BootstrapAdminRequest(t, serverContexts[i], http.MethodPost, "/db/_replication/", rest.MarshalConfig(t, replicationConfig))
		resp.RequireStatus(http.StatusCreated)
	}

	dataStore := activeBucket.DefaultDataStore()
	keyspace := "/db/"
	if !rt.GetDatabase().OnlyDefaultCollection() {
		var err error
		dataStore, err = activeBucket.GetNamedDataStore(0)
		require.NoError(t, err)
		keyspace = fmt.Sprintf("/db.%s.%s/", dataStore.ScopeName(), dataStore.CollectionName())
	}
	for groupNum, group := range groupIDs {
		channel := "chan" + group
		key := "doc" + group
		body := fmt.Sprintf(`{"channels":["%s"]}`, channel)
		added, err := dataStore.Add(key, 0, []byte(body))
		require.NoError(t, err)
		require.True(t, added)

		// Force on-demand import and cache
		for _, sc := range serverContexts {
			resp := rest.BootstrapAdminRequest(t, sc, http.MethodGet, keyspace+key, "")
			resp.RequireStatus(http.StatusOK)
		}

		for scNum, sc := range serverContexts {
			var expectedPushed int64
			// If replicated doc to db already (including this loop iteration) then expect 1
			if scNum <= groupNum {
				expectedPushed = 1
			}

			ctx := sc.SetContextLogID(base.TestCtx(t), sc.Config.Bootstrap.ConfigGroupID)
			dbContext, err := sc.GetDatabase(ctx, "db")
			require.NoError(t, err)
			dbstats, err := dbContext.DbStats.DBReplicatorStats("repl")
			require.NoError(t, err)
			base.RequireWaitForStat(t, dbstats.NumDocPushed.Value, expectedPushed)
		}
	}
}

// Reproduces panic seen in CBG-1053
func TestAdhocReplicationStatus(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{SgReplicateEnabled: true})
	defer rt.Close()

	replConf := `
	{
	  "replication_id": "pushandpull-with-target-oneshot-adhoc",
	  "remote": "` + adminDBURL(rt).String() + `",
	  "direction": "pushAndPull",
	  "adhoc": true,
	  "collections_enabled": ` + strconv.FormatBool(!rt.GetDatabase().OnlyDefaultCollection()) + `
	}`

	resp := rt.SendAdminRequest("PUT", "/{{.db}}/_replication/pushandpull-with-target-oneshot-adhoc", replConf)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// With the error hitting the replicationStatus endpoint will either return running, if not completed, and once
	// completed panics. With the fix after running it'll return a 404 as replication no longer exists.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, http.StatusNotFound, rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/pushandpull-with-target-oneshot-adhoc", "").Code)
	}, 10*time.Second, 10*time.Millisecond)
}

// CBG-1046: Add ability to specify user for active peer in sg-replicate2
func TestSpecifyUserDocsToReplicate(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	testCases := []struct {
		direction string
	}{
		{
			direction: "push",
		},
		{
			direction: "pull",
		},
	}
	for _, test := range testCases {
		t.Run(test.direction, func(t *testing.T) {
			replName := test.direction
			syncFunc := `
function (doc) {
	if (doc.owner) {
		requireUser(doc.owner);
	}
	channel(doc.channels);
	requireAccess(doc.channels);
}`
			rtConfig := &rest.RestTesterConfig{
				SyncFn: syncFunc,
			}
			// Set up buckets, rest testers, and set up servers
			passiveRT := rest.NewRestTester(t, rtConfig)

			defer passiveRT.Close()

			publicSrv := httptest.NewServer(passiveRT.TestPublicHandler())
			defer publicSrv.Close()

			adminSrv := httptest.NewServer(passiveRT.TestAdminHandler())
			defer adminSrv.Close()

			activeRT := rest.NewRestTester(t, rtConfig)
			defer activeRT.Close()

			for _, rt := range []*rest.RestTester{passiveRT, activeRT} {
				rt.CreateUser("alice", []string{"chanAlpha", "chanBeta", "chanCharlie", "chanHotel", "chanIndia"})
				rt.CreateUser("bob", []string{"chanDelta", "chanEcho"})

			}
			// Change RT depending on direction
			var senderRT *rest.RestTester   // RT that has the initial docs that get replicated to the other bucket
			var receiverRT *rest.RestTester // RT that gets the docs replicated to it
			if test.direction == "push" {
				senderRT = activeRT
				receiverRT = passiveRT
			} else if test.direction == "pull" {
				senderRT = passiveRT
				receiverRT = activeRT
			}

			// Create docs to replicate
			bulkDocsBody := `
{
  "docs": [
	{"channels":["chanAlpha"], "access":"alice"},
	{"channels":["chanBeta","chanFoxtrot"], "access":"alice"},
	{"channels":["chanCharlie","chanEcho"], "access":"alice,bob"},
	{"channels":["chanDelta"], "access":"bob"},
	{"channels":["chanGolf"], "access":""},
	{"channels":["!"], "access":"alice,bob"},
	{"channels":["!"], "access":"bob", "owner":"bob"},
	{"channels":["!"], "access":"alice", "owner":"alice"},
	{"channels":["chanHotel"], "access":"", "owner":"mike"},
	{"channels":["chanIndia"], "access":"alice", "owner":"alice"}
  ]
}
`
			resp := senderRT.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", bulkDocsBody)
			rest.RequireStatus(t, resp, http.StatusCreated)

			senderRT.WaitForPendingChanges()

			// Replicate just alices docs
			replConf := `
				{
					"replication_id": "` + replName + `",
					"remote": "` + publicSrv.URL + `/db",
					"direction": "` + test.direction + `",
					"continuous": true,
					"batch": 200,
					"run_as": "alice",
					"remote_username": "alice",
					"remote_password": "letmein",
					"collections_enabled": ` + strconv.FormatBool(!activeRT.GetDatabase().OnlyDefaultCollection()) + `
				}`

			resp = activeRT.SendAdminRequest("PUT", "/{{.db}}/_replication/"+replName, replConf)
			rest.RequireStatus(t, resp, http.StatusCreated)

			activeCtx := activeRT.Context()
			err := activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateRunning)

			base.RequireWaitForStat(t, receiverRT.GetDatabase().DbStats.Database().NumDocWrites.Value, 6)

			changesResults := receiverRT.WaitForChanges(6, "/{{.keyspace}}/_changes?since=0&include_docs=true", "", true)
			// Check the docs are alices docs
			for _, result := range changesResults.Results {
				body, err := result.Doc.MarshalJSON()
				require.NoError(t, err)
				assert.Contains(t, string(body), "alice")
			}

			// Stop and remove replicator (to stop checkpointing after teardown causing panic)
			_, _, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(activeRT.Context(), replName, "stop")
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateStopped)
			err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(replName)
			require.NoError(t, err)

			// Replicate all docs
			// Run as admin should default to true
			replConf = `
					{
						"replication_id": "` + replName + `",
						"remote": "` + adminSrv.URL + `/db",
						"direction": "` + test.direction + `",
						"continuous": true,
						"batch": 200,
						"collections_enabled": ` + strconv.FormatBool(!activeRT.GetDatabase().OnlyDefaultCollection()) + `
					}`

			resp = activeRT.SendAdminRequest("PUT", "/{{.db}}/_replication/"+replName, replConf)
			rest.RequireStatus(t, resp, http.StatusCreated)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateRunning)

			base.RequireWaitForStat(t, receiverRT.GetDatabase().DbStats.Database().NumDocWrites.Value, 10)

			// Stop and remove replicator
			_, _, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(activeRT.Context(), replName, "stop")
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateStopped)
			err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(replName)
			require.NoError(t, err)
		})
	}
}
func TestBasicGetReplicator2(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	// Put document as usual
	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", `{"foo": "bar"}`)
	rest.RequireStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)

	// Get a document with rev using replicator2
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1?replicator2=true&rev="+revID, ``)
	if base.IsEnterpriseEdition() {
		rest.RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		rest.RequireStatus(t, response, http.StatusNotImplemented)
	}

	// Get a document without specifying rev using replicator2
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1?replicator2=true", ``)
	if base.IsEnterpriseEdition() {
		rest.RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		rest.RequireStatus(t, response, http.StatusNotImplemented)
	}
}
func TestBasicPutReplicator2(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	var (
		body  db.Body
		revID string
		err   error
	)

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?replicator2=true", `{}`)
	if base.IsEnterpriseEdition() {
		rest.RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		rest.RequireStatus(t, response, http.StatusNotImplemented)
	}

	// Put basic doc with replicator2 flag and ensure it saves correctly
	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?replicator2=true&rev="+revID, `{"foo": "bar"}`)
	if base.IsEnterpriseEdition() {
		rest.RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		rest.RequireStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", ``)
	if base.IsEnterpriseEdition() {
		rest.RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		rest.RequireStatus(t, response, http.StatusNotFound)
	}
}

func TestDeletedPutReplicator2(t *testing.T) {
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	var body db.Body

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1", "{}")
	rest.RequireStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().NumDocWrites.Value())

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?replicator2=true&rev="+revID+"&deleted=true", "{}")
	if base.IsEnterpriseEdition() {
		rest.RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", ``)
		rest.RequireStatus(t, response, http.StatusNotFound)
		assert.Equal(t, 0, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		rest.RequireStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("PUT", "/{{.keyspace}}/doc1?replicator2=true&rev="+revID+"&deleted=false", `{}`)
	if base.IsEnterpriseEdition() {
		rest.RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 3, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc1", ``)
		rest.RequireStatus(t, response, http.StatusOK)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		rest.RequireStatus(t, response, http.StatusNotImplemented)
	}
}

// TestReplicatorWithCollectionsFailWithoutCollectionsEnabled makes sure not enabling collections causes an error.
func TestReplicatorWithCollectionsFailWithoutCollectionsEnabled(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	url, err := url.Parse("http://example.com")
	require.NoError(t, err)

	for _, direction := range []db.ActiveReplicatorDirection{db.ActiveReplicatorTypePush, db.ActiveReplicatorTypePull} {
		ar, err := db.NewActiveReplicator(base.TestCtx(t), &db.ActiveReplicatorConfig{
			ID:                  t.Name(),
			Direction:           direction,
			ActiveDB:            &db.Database{DatabaseContext: rt.GetDatabase()},
			RemoteDBURL:         url,
			ReplicationStatsMap: dbReplicatorStats(t),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "default collection is not configured")
		require.Nil(t, ar)
	}

}

var emptyReplicationTestCases = []struct {
	name         string
	replications map[string]*db.ReplicationConfig
	errorMessage string
}{
	{
		name: "empty name and empty id",
		replications: map[string]*db.ReplicationConfig{
			"": {
				ID: "",
			},
		},
		errorMessage: "replication name cannot be empty, id is also empty",
	},
	{
		name: "empty name and populated id",
		replications: map[string]*db.ReplicationConfig{
			"": {
				ID: "foo",
			},
		},
		errorMessage: `replication name cannot be empty, id: \"foo\"`,
	},
	{
		name: "populated name and empty id",
		replications: map[string]*db.ReplicationConfig{
			"foo": {
				ID: "",
			},
		},
		errorMessage: `replication id cannot be empty, name: \"foo\"`,
	},
}

func TestBanEmptyReplicationID(t *testing.T) {
	rt := rest.NewRestTesterPersistentConfig(t)
	defer rt.Close()

	resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_replication/", `{"remote": "fakeremote", "direction": "pull", "initial_state": "stopped"}`)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	require.Contains(t, resp.BodyString(), "Replication ID is required")

	for _, testCase := range emptyReplicationTestCases {
		rt.Run(testCase.name, func(t *testing.T) {
			// legacy config pathway, no errors, just warning
			dbConfig := rt.NewDbConfig()
			dbConfig.Replications = testCase.replications
			require.NoError(t, rest.SetupAndValidateDatabases(rt.Context(), map[string]*rest.DbConfig{"db": &dbConfig}))
			for _, method := range []string{http.MethodPut, http.MethodPost} {
				t.Run(method, func(t *testing.T) {
					configEndpoint := "/{{.db}}/_config"
					newDBEndpoint := "/newdb/"
					for _, endpoint := range []string{configEndpoint, newDBEndpoint} {
						// only PUT is valid for a new endpoint
						if endpoint == newDBEndpoint && method == http.MethodPost {
							continue
						}
						t.Run(endpoint, func(t *testing.T) {
							dbConfig := rt.NewDbConfig()
							dbConfig.Replications = testCase.replications

							resp := rt.SendAdminRequest(method, endpoint, string(base.MustJSONMarshal(t, dbConfig)))
							rest.RequireStatus(t, resp, http.StatusBadRequest)
							require.Contains(t, resp.BodyString(), testCase.errorMessage)

						})
					}
				})
			}
		})
	}
}

func TestExistingConfigEmptyReplicationID(t *testing.T) {
	bucket := base.GetTestBucket(t)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		CustomTestBucket: bucket,
	})
	defer rt.Close()

	username, password, _ := bucket.BucketSpec.Auth.GetCredentials()
	// this pathway is used for reading legacy config and also fetchAndLoadConfigs (bootstrap polling). There should be no errors, just warnings.
	for i, testCase := range emptyReplicationTestCases {
		rt.Run(testCase.name, func(t *testing.T) {
			dbName := fmt.Sprintf("db%d", i)
			ctx := rt.Context()
			defer func() {
				require.True(t, rt.ServerContext().RemoveDatabase(ctx, dbName, fmt.Sprintf("Removing database for %s", testCase.name)))
			}()
			dbConfig := rt.NewDbConfig()
			dbConfig.Name = dbName
			dbConfig.Username = username
			dbConfig.Password = password
			dbConfig.Replications = testCase.replications
			_, err := rt.ServerContext().AddDatabaseFromConfig(rt.Context(), rest.DatabaseConfig{DbConfig: dbConfig})
			require.NoError(t, err)
			rest.RequireStatus(t, rt.SendAdminRequest(http.MethodGet, "/"+dbName+"/", ""), http.StatusOK)
		})
	}
}

// TestNoDBInCheckpointHash:
//   - Create two rest testers
//   - Add active replicator for rt1 to push to rt2
//   - Remove database context off os rt1
//   - Call start on active replicator, this would normally hit panic in ticket CBG-4070, should now error instead
func TestNoDBInCheckpointHash(t *testing.T) {

	// Create two rest testers
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()

	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	ar, err := db.NewActiveReplicator(base.TestCtx(t), &db.ActiveReplicatorConfig{
		ID:                  t.Name(),
		Direction:           db.ActiveReplicatorTypePush,
		ActiveDB:            &db.Database{DatabaseContext: rt1.GetDatabase()},
		RemoteDBURL:         userDBURL(rt2, username),
		ReplicationStatsMap: dbReplicatorStats(t),
		Continuous:          true,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()
	// remove the db context for rt1 off the server context
	ok := rt1.ServerContext().RemoveDatabase(base.TestCtx(t), "db", "Removing database context for test")
	require.True(t, ok)

	// assert that the db context has been removed
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, 0, len(rt1.ServerContext().AllDatabases()))
	}, time.Second*10, time.Millisecond*10)

	// attempt to start active replicator, this will hit panic in CBG-4070 pre this work due to the bucket being nil
	// on the active db context
	replicatorErr := ar.Start(base.TestCtx(t))
	assert.Error(t, replicatorErr)
	assert.ErrorContains(t, replicatorErr, "cannot fetch bucket UUID")

}

func TestDbConfigNoOverwriteReplications(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE since this tests persistence of replication configuration in CfgSg")
	}
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	startReplicationConfig := db.ReplicationConfig{
		ID:                 "replication1",
		Remote:             "http://remote:4984/db",
		Direction:          "pull",
		CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
	}

	// PUT replication
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", string(base.MustJSONMarshal(t, startReplicationConfig)))
	rest.RequireStatus(t, resp, http.StatusCreated)

	dbConfig := rt.NewDbConfig()
	dbConfig.Replications = map[string]*db.ReplicationConfig{
		"replication1": {
			ID:                 "replication1",
			Remote:             "http://remote:4984/db",
			Direction:          "push",
			CollectionsEnabled: !rt.GetDatabase().OnlyDefaultCollection(),
		},
	}
	rt.UpsertDbConfig("db", dbConfig)

	resp = rt.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication1", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	var config *db.ReplicationConfig
	require.NoError(t, base.JSONUnmarshal(resp.BodyBytes(), &config))
	require.Equal(t, startReplicationConfig.Direction, config.Direction)
}

func TestActiveReplicatorChangesFeedExit(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	var shouldChannelQueryError atomic.Bool
	activeRT := rest.NewRestTester(t, &rest.RestTesterConfig{
		LeakyBucketConfig: &base.LeakyBucketConfig{
			QueryCallback: func(ddoc, viewname string, params map[string]any) error {
				if viewname == "channels" && shouldChannelQueryError.Load() {
					shouldChannelQueryError.Store(false)
					return gocb.ErrTimeout
				}
				return nil
			},
			N1QLQueryCallback: func(_ context.Context, statement string, params map[string]any, consistency base.ConsistencyMode, adhoc bool) error {
				// * channel query uses all docs index
				if strings.Contains(statement, "sg_allDocs") && shouldChannelQueryError.Load() {
					shouldChannelQueryError.Store(false)
					return gocb.ErrTimeout
				}
				return nil
			},
		},
	})
	t.Cleanup(activeRT.Close)
	_ = activeRT.Bucket()

	passiveRT := rest.NewRestTesterPersistentConfig(t)
	t.Cleanup(passiveRT.Close)

	username := "alice"
	passiveRT.CreateUser(username, []string{"*"})
	passiveDBURL := userDBURL(passiveRT, username)
	stats := dbReplicatorStats(t)
	ar, err := db.NewActiveReplicator(activeRT.Context(), &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          false,
		ReplicationStatsMap: stats,
		CollectionsEnabled:  !activeRT.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, ar.Stop()) })

	docID := "doc1"
	_ = activeRT.CreateTestDoc(docID)

	shouldChannelQueryError.Store(true)
	require.NoError(t, ar.Start(activeRT.Context()))

	changesResults := passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.Equal(t, docID, changesResults.Results[0].ID)
	require.Equal(t, int64(2), stats.NumConnectAttemptsPush.Value())
}
func requireBodyEqual(t *testing.T, expected string, doc *db.Document) {
	var expectedBody db.Body
	require.NoError(t, base.JSONUnmarshal([]byte(expected), &expectedBody))
	require.Equal(t, expectedBody, doc.Body(base.TestCtx(t)))
}

func dbReplicatorStats(t *testing.T) *base.DbReplicatorStats {
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)
	return dbstats
}

// userDBURL creates a public server for the passive RT and returns the URL for the given user, e.g. http://alice:password@localhost:1234/dbname. The webserver will be closed by testing.T.Cleanup.
func userDBURL(rt *rest.RestTester, username string) *url.URL {
	srv := httptest.NewServer(rt.TestPublicHandler())
	rt.TB().Cleanup(srv.Close)

	passiveDBURL, err := url.Parse(srv.URL + "/" + rt.GetDatabase().Name)
	require.NoError(rt.TB(), err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	return passiveDBURL
}

// adminDBURL creates a admin server for RestTester returns the URL, e.g. http://localhost:1234/dbname. The webserver will be closed by testing.T.Cleanup.
func adminDBURL(rt *rest.RestTester) *url.URL {
	srv := httptest.NewServer(rt.TestAdminHandler())
	rt.TB().Cleanup(srv.Close)

	passiveDBURL, err := url.Parse(srv.URL + "/" + rt.GetDatabase().Name)
	require.NoError(rt.TB(), err)

	return passiveDBURL
}
func TestReplicationConfigUpdatedAt(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	sgrRunner := rest.NewSGRTestRunner(t)
	sgrRunner.Run(func(t *testing.T) {
		activeRT, _, remoteURLString := sgrRunner.SetupSGRPeers(t)

		// create a replication and assert the updated at field is present in the config
		activeRT.CreateReplication("replication1", remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault, "")

		activeRT.WaitForReplicationStatus("replication1", db.ReplicationStateRunning)

		resp := activeRT.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication1", "")
		var configResponse db.ReplicationConfig
		require.NoError(t, json.Unmarshal(resp.BodyBytes(), &configResponse))

		// Check that the config has an updated_at field
		require.NotNil(t, configResponse.UpdatedAt)
		require.NotNil(t, configResponse.CreatedAt)
		currTime := configResponse.UpdatedAt
		createdAtTime := configResponse.CreatedAt

		// avoid flake where update at seems to be the same (possibly running to fast)
		time.Sleep(10 * time.Millisecond)

		resp = activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1?action=stop", "")
		rest.RequireStatus(t, resp, http.StatusOK)

		activeRT.WaitForReplicationStatus("replication1", db.ReplicationStateStopped)

		// update the config
		resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_replication/replication1", fmt.Sprintf(`{"name":"replication1","source":"%s","type":"push", "continuous":true}`, remoteURLString))
		rest.RequireStatus(t, resp, http.StatusOK)

		// Check that the updated_at field is updated when the config is updated
		resp = activeRT.SendAdminRequest(http.MethodGet, "/{{.db}}/_replication/replication1", "")
		configResponse = db.ReplicationConfig{}
		require.NoError(t, json.Unmarshal(resp.BodyBytes(), &configResponse))

		base.AssertTimeGreaterThan(t, *configResponse.UpdatedAt, *currTime)
		assert.Equal(t, configResponse.CreatedAt.UnixNano(), createdAtTime.UnixNano())
	})
}
