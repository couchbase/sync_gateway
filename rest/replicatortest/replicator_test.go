//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package replicatortest

import (
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

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/document"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	log.Printf("configResponse direction type: %T", configResponse.Direction)
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
	log.Printf("response: %s", response.BodyBytes())
	err = json.Unmarshal(response.BodyBytes(), &replicationsResponse)
	require.NoError(t, err)
	assert.Equal(t, 2, len(replicationsResponse))
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
		t.Run(test.name, func(t *testing.T) {
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
	log.Printf("All status response: %v", allStatusResponse)

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
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	rt1, rt2, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
	defer teardown()

	// Create doc1 on rt1
	docID1 := t.Name() + "rt1doc"
	_ = rt1.PutDoc(docID1, `{"source":"rt1","channels":["alice"]}`)

	// Create push replication, verify running
	replicationID := t.Name()
	rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
	rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults := rt2.RequireWaitChanges(1, "0")
	assert.Equal(t, docID1, changesResults.Results[0].ID)

	// Validate doc1 contents on remote
	doc1Body := rt2.GetDoc(docID1)
	assert.Equal(t, "rt1", doc1Body["source"])

	// Create doc2 on rt1
	docID2 := t.Name() + "rt1doc2"
	_ = rt2.PutDoc(docID2, `{"source":"rt1","channels":["alice"]}`)

	// wait for doc2 to arrive at rt2
	changesResults = rt2.RequireWaitChanges(1, changesResults.Last_Seq.(string))
	assert.Equal(t, docID2, changesResults.Results[0].ID)

	// Validate doc2 contents
	doc2Body := rt2.GetDoc(docID2)
	assert.Equal(t, "rt1", doc2Body["source"])
}

// TestPullReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt2.
//   - Creates a continuous pull replication on rt1 via the REST API
//   - Validates documents are replicated to rt1
func TestPullReplicationAPI(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	rt1, rt2, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
	defer teardown()

	// Create doc1 on rt2
	docID1 := t.Name() + "rt2doc"
	_ = rt2.PutDoc(docID1, `{"source":"rt2","channels":["alice"]}`)

	// Create pull replication, verify running
	replicationID := t.Name()
	rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault)
	rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

	// wait for document originally written to rt2 to arrive at rt1
	changesResults := rt1.RequireWaitChanges(1, "0")
	changesResults.RequireDocIDs(t, []string{docID1})

	// Validate doc1 contents
	doc1Body := rt1.GetDoc(docID1)
	assert.Equal(t, "rt2", doc1Body["source"])

	// Create doc2 on rt2
	docID2 := t.Name() + "rt2doc2"
	_ = rt2.PutDoc(docID2, `{"source":"rt2","channels":["alice"]}`)

	// wait for new document to arrive at rt1
	changesResults = rt1.RequireWaitChanges(1, changesResults.Last_Seq.(string))
	changesResults.RequireDocIDs(t, []string{docID2})

	// Validate doc2 contents
	doc2Body := rt1.GetDoc(docID2)
	assert.Equal(t, "rt2", doc2Body["source"])
}

// TestPullReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a continuous pull replication on rt1 via the REST API
//   - Validates stop/start/reset actions on the replicationStatus endpoint
func TestReplicationStatusActions(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	// CBG-2766 blocks using non default collection
	rt1, rt2, remoteURLString, teardown := rest.SetupSGRPeers(t, false)
	defer teardown()

	// Create doc1 on rt2
	docID1 := t.Name() + "rt2doc"
	_ = rt2.PutDoc(docID1, `{"source":"rt2","channels":["alice"]}`)

	// Create pull replication, verify running
	replicationID := t.Name()
	rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true, db.ConflictResolverDefault)
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
	changesResults := rt1.RequireWaitChanges(1, "0")
	changesResults.RequireDocIDs(t, []string{docID1})

	// Validate doc1 contents
	doc1Body := rt1.GetDoc(docID1)
	assert.Equal(t, "rt2", doc1Body["source"])

	// Create doc2 on rt2
	docID2 := t.Name() + "rt2doc2"
	_ = rt2.PutDoc(docID2, `{"source":"rt2","channels":["alice"]}`)

	// wait for new document to arrive at rt1
	changesResults = rt1.RequireWaitChanges(1, changesResults.Last_Seq.(string))
	changesResults.RequireDocIDs(t, []string{docID2})

	// Validate doc2 contents
	doc2Body := rt1.GetDoc(docID2)
	assert.Equal(t, "rt2", doc2Body["source"])

	// Stop replication
	response := rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)

	// Wait for stopped.  Non-instant as config change needs to arrive over DCP
	stateError := rt1.WaitForCondition(func() bool {
		status := rt1.GetReplicationStatus(replicationID)
		return status.Status == db.ReplicationStateStopped
	})
	assert.NoError(t, stateError)

	// Reset replication
	response = rt1.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=reset", "")
	rest.RequireStatus(t, response, http.StatusOK)

	resetErr := rt1.WaitForCondition(func() bool {
		status := rt1.GetReplicationStatus(replicationID)
		return status.Status == db.ReplicationStateStopped && status.LastSeqPull == ""
	})
	assert.NoError(t, resetErr)

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

	// Increase checkpoint persistence frequency for cross-node status verification
	defer reduceTestCheckpointInterval(50 * time.Millisecond)()

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	defer db.SuspendSequenceBatching()()

	// CBG-2766 blocks using non default collection
	activeRT, remoteRT, remoteURLString, teardown := rest.SetupSGRPeers(t, false)
	defer teardown()

	// Create docs on remote
	docABC1 := t.Name() + "ABC1"
	docDEF1 := t.Name() + "DEF1"
	_ = remoteRT.PutDoc(docABC1, `{"source":"remoteRT","channels":["ABC"]}`)
	_ = remoteRT.PutDoc(docDEF1, `{"source":"remoteRT","channels":["DEF"]}`)

	// Create pull replications, verify running
	activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePull, []string{"ABC"}, true, db.ConflictResolverDefault)
	activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePull, []string{"DEF"}, true, db.ConflictResolverDefault)
	activeRT.WaitForAssignedReplications(2)
	activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
	activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

	// wait for documents originally written to remoteRT to arrive at activeRT
	changesResults := activeRT.RequireWaitChanges(2, "0")
	changesResults.RequireDocIDs(t, []string{docABC1, docDEF1})

	// Validate doc contents
	docABC1Body := activeRT.GetDoc(docABC1)
	assert.Equal(t, "remoteRT", docABC1Body["source"])
	docDEF1Body := activeRT.GetDoc(docDEF1)
	assert.Equal(t, "remoteRT", docDEF1Body["source"])

	// Add another node to the active cluster
	activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
	defer activeRT2.Close()

	// Wait for replication to be rebalanced to activeRT2
	activeRT.WaitForAssignedReplications(1)
	activeRT2.WaitForAssignedReplications(1)

	log.Printf("==============replication rebalance is done================")

	// Create additional docs on remoteRT
	docABC2 := t.Name() + "ABC2"
	_ = remoteRT.PutDoc(docABC2, `{"source":"remoteRT","channels":["ABC"]}`)
	docDEF2 := t.Name() + "DEF2"
	_ = remoteRT.PutDoc(docDEF2, `{"source":"remoteRT","channels":["DEF"]}`)

	// wait for new documents to arrive at activeRT
	changesResults = activeRT.RequireWaitChanges(2, changesResults.Last_Seq.(string))
	changesResults.RequireDocIDs(t, []string{docABC2, docDEF2})

	// Validate doc contents
	docABC2Body := activeRT.GetDoc(docABC2)
	assert.Equal(t, "remoteRT", docABC2Body["source"])
	docDEF2Body := activeRT.GetDoc(docDEF2)
	assert.Equal(t, "remoteRT", docDEF2Body["source"])
	docABC2Body2 := activeRT2.GetDoc(docABC2)
	assert.Equal(t, "remoteRT", docABC2Body2["source"])
	docDEF2Body2 := activeRT2.GetDoc(docDEF2)
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

	// Increase checkpoint persistence frequency for cross-node status verification
	defer reduceTestCheckpointInterval(50 * time.Millisecond)()

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	defer db.SuspendSequenceBatching()()

	// CBG-2766 blocks using non default collection
	activeRT, remoteRT, remoteURLString, teardown := rest.SetupSGRPeers(t, false)
	defer teardown()

	// Create docs on active
	docABC1 := t.Name() + "ABC1"
	docDEF1 := t.Name() + "DEF1"
	_ = activeRT.PutDoc(docABC1, `{"source":"activeRT","channels":["ABC"]}`)
	_ = activeRT.PutDoc(docDEF1, `{"source":"activeRT","channels":["DEF"]}`)

	// This seems to fix the flaking. Wait until the change-cache has caught up with the latest writes to the database.
	require.NoError(t, activeRT.WaitForPendingChanges())

	// Create push replications, verify running
	activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePush, []string{"ABC"}, true, db.ConflictResolverDefault)
	activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePush, []string{"DEF"}, true, db.ConflictResolverDefault)
	activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
	activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

	// wait for documents to be pushed to remote
	changesResults := remoteRT.RequireWaitChanges(2, "0")
	changesResults.RequireDocIDs(t, []string{docABC1, docDEF1})

	// Validate doc contents
	docABC1Body := remoteRT.GetDoc(docABC1)
	assert.Equal(t, "activeRT", docABC1Body["source"])
	docDEF1Body := remoteRT.GetDoc(docDEF1)
	assert.Equal(t, "activeRT", docDEF1Body["source"])

	// Add another node to the active cluster
	activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
	defer activeRT2.Close()

	// Wait for replication to be rebalanced to activeRT2
	activeRT.WaitForAssignedReplications(1)
	activeRT2.WaitForAssignedReplications(1)

	// Create additional docs on local
	docABC2 := t.Name() + "ABC2"
	_ = activeRT.PutDoc(docABC2, `{"source":"activeRT","channels":["ABC"]}`)
	docDEF2 := t.Name() + "DEF2"
	_ = activeRT.PutDoc(docDEF2, `{"source":"activeRT","channels":["DEF"]}`)

	// wait for new documents to arrive at remote
	changesResults = remoteRT.RequireWaitChanges(2, changesResults.Last_Seq.(string))
	changesResults.RequireDocIDs(t, []string{docABC2, docDEF2})

	// Validate doc contents
	docABC2Body := remoteRT.GetDoc(docABC2)
	assert.Equal(t, "activeRT", docABC2Body["source"])
	docDEF2Body := remoteRT.GetDoc(docDEF2)
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

	// CBG-2766 blocks using non default collection, since stats are not propogated across rebalance
	activeRT, remoteRT, remoteURLString, teardown := rest.SetupSGRPeers(t, false)
	defer teardown()

	// Create 20 docs on rt2
	docCount := 20
	docIDs := make([]string, 20)
	for i := 0; i < 20; i++ {
		docID := fmt.Sprintf("%s%s%d", t.Name(), "rt2doc", i)
		_ = remoteRT.PutDoc(docID, `{"source":"rt2","channels":["alice"]}`)
		docIDs[i] = docID
	}

	require.NoError(t, remoteRT.WaitForPendingChanges())

	// Create oneshot replication, verify running
	replicationID := t.Name()
	activeRT.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, false, db.ConflictResolverDefault)
	activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

	// wait for documents originally written to rt2 to arrive at rt1
	changesResults := activeRT.RequireWaitChanges(docCount, "0")
	changesResults.RequireDocIDs(t, docIDs)

	// Validate sample doc contents
	doc1Body := activeRT.GetDoc(docIDs[0])
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
	fmt.Println("HONK remoteStatus=", remoteStatus)
	assert.Equal(t, int64(docCount), remoteStatus.DocsRead)

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
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	defer db.SuspendSequenceBatching()()

	// Increase checkpoint persistence frequency for cross-node status verification
	defer reduceTestCheckpointInterval(50 * time.Millisecond)()

	activeRT, remoteRT, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
	defer teardown()
	// Create push replications, verify running
	activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePush, []string{"ABC"}, true, db.ConflictResolverDefault)
	activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePush, []string{"DEF"}, true, db.ConflictResolverDefault)
	activeRT.WaitForAssignedReplications(2)
	activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
	activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

	// Create docs on active
	docAllChannels1 := t.Name() + "All1"
	docAllChannels2 := t.Name() + "All2"
	_ = activeRT.PutDoc(docAllChannels1, `{"source":"activeRT1","channels":["ABC","DEF"]}`)
	_ = activeRT.PutDoc(docAllChannels2, `{"source":"activeRT2","channels":["ABC","DEF"]}`)

	// wait for documents to be pushed to remote
	changesResults := remoteRT.RequireWaitChanges(2, "0")
	changesResults.RequireDocIDs(t, []string{docAllChannels1, docAllChannels2})

	// wait for both replications to have pushed, and total pushed to equal 2
	assert.NoError(t, activeRT.WaitForCondition(func() bool {
		abcStatus := activeRT.GetReplicationStatus("rep_ABC")
		if abcStatus.DocsCheckedPush != 2 {
			log.Printf("abcStatus.DocsCheckedPush not 2, is %v", abcStatus.DocsCheckedPush)
			log.Printf("abcStatus=%+v", abcStatus)
			return false
		}
		defStatus := activeRT.GetReplicationStatus("rep_DEF")
		if defStatus.DocsCheckedPush != 2 {
			log.Printf("defStatus.DocsCheckedPush not 2, is %v", defStatus.DocsCheckedPush)
			log.Printf("defStatus=%+v", defStatus)
			return false
		}

		// DocsWritten is incremented on a successful write, but ALSO in the race scenario where the remote responds
		// to the changes message to say it needs the rev, but then receives the rev from another source. This means that
		// in this test, DocsWritten can be any value between 0 and 2 for each replication, but should be at least 2
		// for both replications
		totalDocsWritten := abcStatus.DocsWritten + defStatus.DocsWritten
		if totalDocsWritten < 2 || totalDocsWritten > 4 {
			log.Printf("Total docs written is not between 2 and 4, is abc=%v, def=%v", abcStatus.DocsWritten, defStatus.DocsWritten)
			return false
		}
		return true
	}))

	// Validate doc contents
	docAll1Body := remoteRT.GetDoc(docAllChannels1)
	assert.Equal(t, "activeRT1", docAll1Body["source"])
	docAll2Body := remoteRT.GetDoc(docAllChannels2)
	assert.Equal(t, "activeRT2", docAll2Body["source"])

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
	log.Printf("response: %s", response.BodyBytes())

	var replicationsResponse map[string]db.ReplicationConfig
	err = json.Unmarshal(response.BodyBytes(), &replicationsResponse)
	require.NoError(t, err, "Error un-marshalling replication response")
	assert.Equal(t, 2, len(replicationsResponse), "Replication count mismatch")

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
	require.Equal(t, 2, len(allStatusResponse), "Replication count mismatch")

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
				QueryParams:    map[string]interface{}{"channels": []interface{}{"E", "A", "D", "G", "B", "e"}},
				Direction:      db.ActiveReplicatorTypePull,
			},
		},
		{
			name: "auth credentials specified in remote URL",
			replicationConfig: db.ReplicationConfig{
				Remote:      "http://bob:pass@remote:4984/db",
				Filter:      base.ByChannelFilter,
				QueryParams: map[string]interface{}{"channels": []interface{}{"E", "A", "D", "G", "B", "e"}},
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
				QueryParams: map[string]interface{}{"channels": []interface{}{"E", "A", "D", "G", "B", "e"}},
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
	require.Equal(t, 2, len(database.ReplicationStatus), "Replication count mismatch")

	// Sort replications by replication ID before asserting replication status
	sort.Slice(database.ReplicationStatus, func(i, j int) bool {
		return database.ReplicationStatus[i].ID < database.ReplicationStatus[j].ID
	})
	assert.Equal(t, config1.ID, database.ReplicationStatus[0].ID)
	assert.Equal(t, config2.ID, database.ReplicationStatus[1].ID)
	assert.Equal(t, "running", database.ReplicationStatus[0].Status)
	assert.Equal(t, "running", database.ReplicationStatus[1].Status)

	assert.Equal(t, 2, len(database.SGRCluster.Replications), "Replication count mismatch")
	assert.Equal(t, 0, len(database.SGRCluster.Nodes), "Replication node count mismatch")
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
	require.Equal(t, 0, len(status.Databases["db"].ReplicationStatus))
}

func TestRequireReplicatorStoppedBeforeUpsert(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)

	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	replicationConfig := `{
		"replication_id": "replication1",
		"remote": "http://remote:4985/db",
		"direction":"` + db.ActiveReplicatorTypePushAndPull + `",
		"conflict_resolution_type":"default",
		"max_backoff":100
	}`

	response := rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", string(replicationConfig))
	rest.RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/", "")
	rest.RequireStatus(t, response, http.StatusOK)

	var body []map[string]interface{}
	err := base.JSONUnmarshal(response.BodyBytes(), &body)
	fmt.Println(string(response.BodyBytes()))
	assert.NoError(t, err)
	assert.Equal(t, "running", body[0]["status"])

	replicationConfigUpdate := `{
		"replication_id": "replication1",
		"remote": "http://remote:4985/db",
		"direction":"` + db.ActiveReplicatorTypePush + `",
		"conflict_resolution_type":"default",
		"max_backoff":100
	}`

	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", string(replicationConfigUpdate))
	rest.RequireStatus(t, response, http.StatusBadRequest)

	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication1?action=stop", "")
	rest.RequireStatus(t, response, http.StatusOK)

	response = rt.SendAdminRequest("PUT", "/{{.db}}/_replication/replication1", string(replicationConfigUpdate))
	rest.RequireStatus(t, response, http.StatusOK)

}

func TestReplicationConfigChange(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	rt1, rt2, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
	defer teardown()

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

	changesResults, err := rt2.WaitForChanges(4, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 4)

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

	changesResults, err = rt2.WaitForChanges(8, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 8)
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

	// Increase checkpoint persistence frequency for cross-node status verification
	defer reduceTestCheckpointInterval(50 * time.Millisecond)()

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	defer db.SuspendSequenceBatching()()

	// CBG-2766 blocks using non default collection
	activeRT, remoteRT, remoteURLString, teardown := rest.SetupSGRPeers(t, false)
	defer teardown()

	// Create docs on remote
	docABC1 := t.Name() + "ABC1"
	docDEF1 := t.Name() + "DEF1"
	_ = remoteRT.PutDoc(docABC1, `{"source":"remoteRT","channels":["ABC"]}`)
	_ = remoteRT.PutDoc(docDEF1, `{"source":"remoteRT","channels":["DEF"]}`)

	// Create pull replications, verify running
	activeRT.CreateReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePull, []string{"ABC"}, true, db.ConflictResolverDefault)
	activeRT.CreateReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePull, []string{"DEF"}, true, db.ConflictResolverDefault)
	activeRT.WaitForAssignedReplications(2)
	activeRT.WaitForReplicationStatus("rep_ABC", db.ReplicationStateRunning)
	activeRT.WaitForReplicationStatus("rep_DEF", db.ReplicationStateRunning)

	// wait for documents originally written to remoteRT to arrive at activeRT
	changesResults := activeRT.RequireWaitChanges(2, "0")
	changesResults.RequireDocIDs(t, []string{docABC1, docDEF1})

	// Validate doc replication
	_ = activeRT.GetDoc(docABC1)
	_ = activeRT.GetDoc(docDEF1)

	// Add another node to the active cluster
	activeRT2 := addActiveRT(t, activeRT.GetDatabase().Name, activeRT.TestBucket)
	defer activeRT2.Close()

	// Wait for replication to be rebalanced to activeRT2
	activeRT.WaitForAssignedReplications(1)
	activeRT2.WaitForAssignedReplications(1)

	// Create additional docs on remoteRT
	docABC2 := t.Name() + "ABC2"
	_ = remoteRT.PutDoc(docABC2, `{"source":"remoteRT","channels":["ABC"]}`)
	docDEF2 := t.Name() + "DEF2"
	_ = remoteRT.PutDoc(docDEF2, `{"source":"remoteRT","channels":["DEF"]}`)

	// wait for new documents to arrive at activeRT
	changesResults = activeRT.RequireWaitChanges(2, changesResults.Last_Seq.(string))
	changesResults.RequireDocIDs(t, []string{docABC2, docDEF2})

	// Validate doc contents via both active nodes
	_ = activeRT.GetDoc(docABC2)
	_ = activeRT.GetDoc(docDEF2)
	_ = activeRT2.GetDoc(docABC2)
	_ = activeRT2.GetDoc(docDEF2)

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
	docABC3 := t.Name() + "ABC3"
	_ = remoteRT.PutDoc(docABC3, `{"source":"remoteRT","channels":["ABC"]}`)
	docDEF3 := t.Name() + "DEF3"
	_ = remoteRT.PutDoc(docDEF3, `{"source":"remoteRT","channels":["DEF"]}`)

	changesResults = activeRT.RequireWaitChanges(2, changesResults.Last_Seq.(string))
	changesResults.RequireDocIDs(t, []string{docABC3, docDEF3})

	// explicitly stop the SGReplicateMgrs on the active nodes, to prevent a node rebalance during test teardown.
	activeRT.GetDatabase().SGReplicateMgr.Stop()
	activeRT.GetDatabase().SGReplicateMgr = nil
	activeRT2.GetDatabase().SGReplicateMgr.Stop()
	activeRT2.GetDatabase().SGReplicateMgr = nil
}

// Repros CBG-2416
func TestDBReplicationStatsTeardown(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.RequireNumTestBuckets(t, 2)
	// Test tests Prometheus stat registration
	base.SkipPrometheusStatsRegistration = false
	defer func() {
		base.SkipPrometheusStatsRegistration = true
	}()

	tb := base.GetTestBucket(t)
	defer tb.Close()
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

	resp := rt.SendAdminRequest(http.MethodPut, "/db2/", fmt.Sprintf(`{
				"bucket": "%s",
				"use_views": %t,
				"num_index_replicas": 0
	}`, tb.GetName(), base.TestsDisableGSI()))
	rest.RequireStatus(t, resp, http.StatusCreated)

	tb2 := base.GetTestBucket(t)
	defer tb2.Close()
	resp = rt.SendAdminRequest(http.MethodPut, "/db/", fmt.Sprintf(`{
				"bucket": "%s",
				"use_views": %t,
				"num_index_replicas": 0
	}`, tb2.GetName(), base.TestsDisableGSI()))
	rest.RequireStatus(t, resp, http.StatusCreated)

	rt.CreateReplicationForDB("{{.db1}}", "repl1", db2Url.String(), db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
	rt.WaitForReplicationStatusForDB("{{.db1}}", "repl1", db.ReplicationStateRunning)

	// Wait for document to replicate from db to db2 to confirm replication start
	rt.CreateDoc(t, "marker1")
	err = rt.WaitForCondition(func() bool {
		getResp := rt.SendAdminRequest("GET", "/db2/marker1", "")
		return getResp.Code == http.StatusOK
	})
	require.NoError(t, err)

	// Force DB reload by modifying config
	resp = rt.SendAdminRequest(http.MethodPost, "/db/_config", `{"import_docs": false}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// If CE, recreate the replication
	if !base.IsEnterpriseEdition() {
		rt.CreateReplication("repl1", db2Url.String(), db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
		rt.WaitForReplicationStatus("repl1", db.ReplicationStateRunning)
	}

	// Wait for second document to replicate to confirm replication restart
	rt.CreateDoc(t, "marker2")
	err = rt.WaitForCondition(func() bool {
		getResp := rt.SendAdminRequest("GET", "/db2/marker2", "")
		return getResp.Code == http.StatusOK
	})
	require.NoError(t, err)

}

func TestTakeDbOfflineOngoingPushReplication(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	rt1, rt2, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
	defer teardown()

	// Create doc1 on rt1
	docID1 := t.Name() + "rt1doc"
	_ = rt1.PutDoc(docID1, `{"source":"rt1","channels":["alice"]}`)

	// Create push replication, verify running
	replicationID := t.Name()
	rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
	rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults := rt2.RequireWaitChanges(1, "0")
	assert.Equal(t, docID1, changesResults.Results[0].ID)

	resp := rt2.SendAdminRequest("POST", "/{{.db}}/_offline", "")
	assert.Equal(t, resp.Code, 200)
}

// TestPushReplicationAPIUpdateDatabase starts a push replication and updates the passive database underneath the replication.
// Expect to see the connection closed with an error, instead of continuously panicking.
// This is the ISGR version of TestBlipPusherUpdateDatabase
//
// This test causes the race detector to flag the bucket=nil operation and any in-flight requests being made using that bucket, prior to the replication being reset.
// TODO CBG-1903: Can be fixed by draining in-flight requests before fully closing the database.
func TestPushReplicationAPIUpdateDatabase(t *testing.T) {

	t.Skip("Skipping test - revisit in CBG-1908")

	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test does not support Walrus - depends on closing and re-opening persistent bucket")
	}

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	rt1, rt2, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
	defer teardown()

	// Create initial doc on rt1
	docID := t.Name() + "rt1doc"
	_ = rt1.PutDoc(docID, `{"source":"rt1","channels":["alice"]}`)

	// Create push replication, verify running
	replicationID := t.Name()
	rt1.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true, db.ConflictResolverDefault)
	rt1.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults := rt2.RequireWaitChanges(1, "0")
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
		// for i := 0; i < 10; i++ {
		for i := 0; shouldCreateDocs.IsTrue(); i++ {
			resp := rt1.PutDoc(fmt.Sprintf("%s-doc%d", t.Name(), i), fmt.Sprintf(`{"i":%d,"channels":["alice"]}`, i))
			lastDocID.Store(resp.ID)
		}
		_ = rt1.WaitForPendingChanges()
		wg.Done()
	}()

	// and wait for a few to be done before we proceed with updating database config underneath replication
	_, err := rt2.WaitForChanges(5, "/db/_changes", "", true)
	require.NoError(t, err)

	// just change the sync function to cause the database to reload
	dbConfig := *rt2.ServerContext().GetDbConfig("db")
	dbConfig.Sync = base.StringPtr(`function(doc){channel(doc.channels);}`)
	resp := rt2.ReplaceDbConfig("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	shouldCreateDocs.Set(false)

	lastDocIDString, ok := lastDocID.Load().(string)
	require.True(t, ok)

	// wait for the last document written to rt1 to arrive at rt2
	rest.WaitAndAssertCondition(t, func() bool {
		_, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), lastDocIDString, db.DocUnmarshalNone)
		return err == nil
	})
}

// TestActiveReplicatorHeartbeats uses an ActiveReplicator with another RestTester instance to connect, and waits for several websocket ping/pongs.
func TestActiveReplicatorHeartbeats(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyWebSocket, base.KeyWebSocketFrame)

	username := "alice"
	rt := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					username: {Password: base.StringPtr(rest.RestTesterDefaultUserPassword)},
				},
			}},
		})
	defer rt.Close()
	ctx := rt.Context()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx, &db.ActiveReplicatorConfig{
		ID:                    t.Name(),
		Direction:             db.ActiveReplicatorTypePush,
		ActiveDB:              &db.Database{DatabaseContext: rt.GetDatabase()},
		RemoteDBURL:           passiveDBURL,
		WebsocketPingInterval: time.Millisecond * 10,
		Continuous:            true,
		ReplicationStatsMap:   dbstats,
		CollectionsEnabled:    !rt.GetDatabase().OnlyDefaultCollection(),
	})

	pingCountStart := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count"))
	pingGoroutinesStart := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))

	assert.NoError(t, ar.Start(ctx))

	// let some pings happen
	time.Sleep(time.Millisecond * 500)

	pingGoroutines := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))
	assert.Equal(t, 1+pingGoroutinesStart, pingGoroutines, "Expected ping sender goroutine to be 1 more than start")

	pingCount := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count"))
	assert.Greaterf(t, pingCount, pingCountStart, "Expected ping count to increase since start")
	assert.NoError(t, ar.Stop())

	pingGoroutines = base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))
	assert.Equal(t, pingGoroutinesStart, pingGoroutines, "Expected ping sender goroutine to return to start count after stop")
}

// TestActiveReplicatorPullBasic:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullBasic(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	// Passive
	const (
		// test url encoding of username/password with these
		username = "AL_1c.e-@"
		password = rest.RestTesterDefaultUserPassword
	)

	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt2.Close()

	rt2.CreateUser(username, []string{username})

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["`+username+`"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	remoteDoc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, password)

	// Active
	rt1 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPull)

	// Start the replicator (implicit connect)
	require.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])

	assert.Equal(t, strconv.FormatUint(remoteDoc.Sequence, 10), ar.GetStatus().LastSeqPull)
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
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelTrace, base.KeyCRUD, base.KeyChanges, base.KeyReplicate)

	defer db.SuspendSequenceBatching()()

	// Passive
	const (
		username = "alice"
		password = rest.RestTesterDefaultUserPassword
	)

	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				CacheConfig: &rest.CacheConfig{
					// shorten pending sequence handling to speed up test
					ChannelCacheConfig: &rest.ChannelCacheConfig{
						MaxWaitPending: base.Uint32Ptr(1),
					},
				},
			}},
		})
	defer rt2.Close()

	response := rt2.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+username, rest.GetUserPayload(t, "", password, "", rt2.GetSingleTestDatabaseCollection(), []string{"*"}, nil))
	rest.RequireStatus(t, response, http.StatusCreated)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	docIDPrefix := t.Name() + "rt2doc"

	docID1 := docIDPrefix + "1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID1, `{"source":"rt2","channels":["`+username+`"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	require.NoError(t, rt2.WaitForPendingChanges())

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

	// wait for the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)

	require.NoError(t, ar.Stop())
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ExpectedSequenceCount)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().AlreadyKnownSequenceCount)
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ProcessedSequenceCount)

	assert.Equal(t, int64(1), dbstats.ExpectedSequenceLen.Value())
	assert.Equal(t, int64(1), dbstats.ProcessedSequenceLen.Value())
	assert.Equal(t, int64(0), dbstats.ExpectedSequenceLenPostCleanup.Value())
	assert.Equal(t, int64(0), dbstats.ProcessedSequenceLenPostCleanup.Value())

	docID2 := docIDPrefix + "2"
	resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID2, `{"source":"rt2","channels":["`+username+`"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// allocate a fake sequence to trigger skipped sequence handling - this never arrives at rt1 - we could think about creating the doc afterwards to let the replicator recover, but not necessary for the test.
	_, err = rt2.MetadataStore().Incr(rt2.GetDatabase().MetadataKeys.SyncSeqKey(), 1, 1, 0)
	require.NoError(t, err)

	docID3 := docIDPrefix + "3"
	resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID3, `{"source":"rt2","channels":["`+username+`"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	require.NoError(t, rt2.WaitForPendingChanges())

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// restarted replicator has a new checkpointer
	pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

	changesResults, err = rt1.WaitForChanges(3, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 3)

	require.NoError(t, ar.Stop())
	assert.Equal(t, int64(2), pullCheckpointer.Stats().ExpectedSequenceCount)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().AlreadyKnownSequenceCount)
	assert.Equal(t, int64(2), pullCheckpointer.Stats().ProcessedSequenceCount)

	assert.Equal(t, int64(2), dbstats.ExpectedSequenceLen.Value())
	assert.Equal(t, int64(2), dbstats.ProcessedSequenceLen.Value())
	assert.Equal(t, int64(0), dbstats.ExpectedSequenceLenPostCleanup.Value())
	assert.Equal(t, int64(0), dbstats.ProcessedSequenceLenPostCleanup.Value())

	docID4 := docIDPrefix + "4"
	resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID4, `{"source":"rt2","channels":["`+username+`"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt2.WaitForPendingChanges())

	require.NoError(t, ar.Start(ctx1))

	// restarted replicator has a new checkpointer
	pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

	changesResults, err = rt1.WaitForChanges(4, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 4)

	require.NoError(t, ar.Stop())
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ExpectedSequenceCount)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().AlreadyKnownSequenceCount)
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ProcessedSequenceCount)

	assert.Equal(t, int64(1), dbstats.ExpectedSequenceLen.Value())
	assert.Equal(t, int64(1), dbstats.ExpectedSequenceLen.Value())
	assert.Equal(t, int64(0), dbstats.ExpectedSequenceLenPostCleanup.Value())
	assert.Equal(t, int64(0), dbstats.ProcessedSequenceLenPostCleanup.Value())
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

	// Passive
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})

	defer rt2.Close()

	const (
		username = "alice"
	)
	rt2.CreateUser(username, []string{username})

	attachment := `"_attachments":{"hi.txt":{"data":"aGk=","content_type":"text/plain"}}`

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","doc_num":1,`+attachment+`,"channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, int64(0), ar.Pull.GetStats().GetAttachment.Value())

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])

	assert.Equal(t, int64(1), ar.Pull.GetStats().GetAttachment.Value())

	docID = t.Name() + "rt2doc2"
	resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","doc_num":2,`+attachment+`,"channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID = rest.RespRevID(t, resp)

	// wait for the new document written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 2)
	assert.Equal(t, docID, changesResults.Results[1].ID)

	doc2, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc2.SyncData.CurrentRev)

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])

	// When targeting a Hydrogen node that supports proveAttachments, we typically end up sending
	// the attachment only once. However, targeting a Lithium node sends the attachment twice like
	// the pre-Hydrogen node, GetAttachment would be 2. The reason is that a Hydrogen node uses a
	// new storage model for attachment storage and retrieval.
	assert.Equal(t, int64(2), ar.Pull.GetStats().GetAttachment.Value())
	assert.Equal(t, int64(0), ar.Pull.GetStats().ProveAttachment.Value())
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

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

			// Increase checkpoint persistence frequency for cross-node status verification
			defer reduceTestCheckpointInterval(50 * time.Millisecond)()

			// Disable sequence batching for multi-RT tests (pending CBG-1000)
			defer db.SuspendSequenceBatching()()

			// Passive
			rt2 := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: channels.DocChannelsSyncFunction,
				})
			defer rt2.Close()
			const username = "alice"
			rt2.CreateUser(username, []string{username})

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

			// Active
			rt1 := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SyncFn: channels.DocChannelsSyncFunction,
					DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
						Replications: map[string]*db.ReplicationConfig{
							"repl1": {
								Remote:                 passiveDBURL.String(),
								Direction:              db.ActiveReplicatorTypePull,
								CollectionsEnabled:     !rt2.GetDatabase().OnlyDefaultCollection(),
								Continuous:             true,
								ConflictResolutionType: db.ConflictResolverCustom,
								ConflictResolutionFn: `
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
					}`},
						},
					}},
					SgReplicateEnabled: true,
				})
			defer rt1.Close()

			rt1.WaitForAssignedReplications(1)

			docID := test.name + "doc1"
			putDocResp := rt2.PutDoc(docID, test.initialRevBody)
			require.True(t, putDocResp.Ok)
			rev1 := putDocResp.Rev

			// wait for the document originally written to rt2 to arrive at rt1
			changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			lastSeq := changesResults.Last_Seq.(string)

			resp := rt1.SendAdminRequest(http.MethodPut, "/{{.db}}/_replicationStatus/repl1?action=stop", "")
			rest.RequireStatus(t, resp, http.StatusOK)

			rt1.WaitForReplicationStatus("repl1", db.ReplicationStateStopped)

			resp = rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+rev1, test.localConflictingRevBody)
			rest.RequireStatus(t, resp, http.StatusCreated)

			changesResults, err = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
			require.NoError(t, err)
			assert.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			lastSeq = changesResults.Last_Seq.(string)

			resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+rev1, test.remoteConflictingRevBody)
			rest.RequireStatus(t, resp, http.StatusCreated)

			resp = rt1.SendAdminRequest(http.MethodPut, "/{{.db}}/_replicationStatus/repl1?action=start", "")
			rest.RequireStatus(t, resp, http.StatusOK)

			rt1.WaitForReplicationStatus("repl1", db.ReplicationStateRunning)

			changesResults, err = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
			require.NoError(t, err)
			assert.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			_ = changesResults.Last_Seq.(string)

			doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			revGen, _ := db.ParseRevID(doc.SyncData.CurrentRev)

			assert.Equal(t, 3, revGen)
			assert.Equal(t, "merged", doc.UnmarshalBody()["source"].(string))

			assert.Nil(t, doc.UnmarshalBody()[db.BodyAttachments], "_attachments property should not be in resolved doc body")

			assert.Len(t, doc.SyncData.Attachments, test.expectedAttachments, "mismatch in expected number of attachments in sync data of resolved doc")
			for attName, attMap := range doc.SyncData.Attachments {
				assert.Equal(t, true, attMap.Stub, "attachment %q should be a stub", attName)
				assert.NotEmpty(t, attMap.Digest, "attachment %q should have digest", attName)
				assert.True(t, attMap.Revpos >= 1, "attachment %q revpos should be at least 1", attName)
				assert.True(t, attMap.Length >= 1, "attachment %q length should be at least 1 byte", attName)
			}
		})
	}
}

// TestActiveReplicatorPullFromCheckpoint:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt2 which can be pulled by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt2.
//   - Starts the pull replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPullFromCheckpoint(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT2DocsInitial = 13 // 2 batches of changes
		numRT2DocsTotal   = 24 // 2 more batches
	)

	// Passive
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt2.Close()

	const username = "alice"
	rt2.CreateUser(username, []string{username})
	// Create first batch of docs
	docIDPrefix := t.Name() + "rt2doc"
	for i := 0; i < numRT2DocsInitial; i++ {
		resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    changesBatchSize,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

	assert.NoError(t, ar.Start(ctx1))

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(numRT2DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT2DocsInitial)
	docIDsSeen := make(map[string]bool, numRT2DocsInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numRT2DocsInitial; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
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
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+numRT2DocsInitial, numRevsSentTotal)
	assert.Equal(t, int64(numRT2DocsInitial), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT2DocsInitial), pullCheckpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions

	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt2.GetDatabase().OnlyDefaultCollection() {
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
	}
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs
	for i := numRT2DocsInitial; i < numRT2DocsTotal; i++ {
		resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	ar = db.NewActiveReplicator(ctx1, &arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start(ctx1))

	// new replicator - new checkpointer
	pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(numRT2DocsTotal, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT2DocsTotal)

	docIDsSeen = make(map[string]bool, numRT2DocsTotal)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}

	for i := 0; i < numRT2DocsTotal; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])
	}

	// Make sure we've not started any more since:0 replications on rt2 since the first one
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+numRT2DocsTotal, numRevsSentTotal)
	assert.Equal(t, int64(numRT2DocsTotal-numRT2DocsInitial), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT2DocsTotal-numRT2DocsInitial), pullCheckpointer.Stats().ExpectedSequenceCount)

	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt2.GetDatabase().OnlyDefaultCollection() {
		// assert the second active replicator stats

		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorPullFromCheckpointIgnored:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates identical documents on rt1 and rt2.
//   - Starts a pull replication to ensure that even ignored revisions are checkpointed.
func TestActiveReplicatorPullFromCheckpointIgnored(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT2DocsInitial = 13 // 2 batches of changes
		numRT2DocsTotal   = 24 // 2 more batches
	)

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
	docIDPrefix := t.Name() + "doc"
	for i := 0; i < numRT2DocsInitial; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		rt1RevID := rest.RespRevID(t, resp)
		resp = rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		rt2RevID := rest.RespRevID(t, resp)
		require.Equal(t, rt1RevID, rt2RevID)
	}

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    changesBatchSize,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()

	assert.NoError(t, ar.Start(ctx1))

	pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

	_, ok := base.WaitForStat(func() int64 {
		return pullCheckpointer.Stats().AlreadyKnownSequenceCount
	}, numRT2DocsInitial)
	assert.True(t, ok)

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(numRT2DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT2DocsInitial)
	docIDsSeen := make(map[string]bool, numRT2DocsInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numRT2DocsInitial; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		_, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
	}

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, int64(0), numRevsSentTotal)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt2.GetDatabase().OnlyDefaultCollection() {
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
	}
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs
	for i := numRT2DocsInitial; i < numRT2DocsTotal; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		rt1RevID := rest.RespRevID(t, resp)
		resp = rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
		rt2RevID := rest.RespRevID(t, resp)
		require.Equal(t, rt1RevID, rt2RevID)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	ar = db.NewActiveReplicator(ctx1, &arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start(ctx1))

	// new replicator - new checkpointer
	pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

	_, ok = base.WaitForStat(func() int64 {
		return pullCheckpointer.Stats().AlreadyKnownSequenceCount
	}, numRT2DocsTotal-numRT2DocsInitial)
	assert.True(t, ok)

	// Make sure we've not started any more since:0 replications on rt2 since the first one
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, int64(0), numRevsSentTotal)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().ExpectedSequenceCount)

	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt2.GetDatabase().OnlyDefaultCollection() {
		// assert the second active replicator stats

		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorPullOneshot:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullOneshot(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()
	const username = "alice"
	rt2.CreateUser(username, []string{username})

	docID := t.Name() + "rt2doc1"

	resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	_, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active

	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPull)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the replication to stop
	replicationStopped := false
	for i := 0; i < 100; i++ {
		status := ar.GetStatus()
		if status.Status == db.ReplicationStateStopped {
			replicationStopped = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, replicationStopped, "One-shot replication status should go to stopped on completion")

	/*doc, err := rt1.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])
	assert.Equal(t, strconv.FormatUint(remoteDoc.Sequence, 10), ar.GetStatus().LastSeqPull)
	*/
}

// TestActiveReplicatorPushBasic:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which can be pushed by the replicator.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasic(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()
	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	localDoc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	assert.Equal(t, strconv.FormatUint(localDoc.Sequence, 10), ar.GetStatus().LastSeqPush)
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

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	attachment := `"_attachments":{"hi.txt":{"data":"aGk=","content_type":"text/plain"}}`

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","doc_num":1,`+attachment+`,"channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, int64(0), ar.Push.GetStats().HandleGetAttachment.Value())

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	assert.Equal(t, int64(1), ar.Push.GetStats().HandleGetAttachment.Value())

	docID = t.Name() + "rt1doc2"
	resp = rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","doc_num":2,`+attachment+`,"channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID = rest.RespRevID(t, resp)

	// wait for the new document written to rt1 to arrive at rt2
	changesResults, err = rt2.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 2)
	assert.Equal(t, docID, changesResults.Results[1].ID)

	doc2, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc2.SyncData.CurrentRev)

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	// When targeting a Hydrogen node that supports proveAttachments, we typically end up sending
	// the attachment only once. However, targeting a Lithium node sends the attachment twice like
	// the pre-Hydrogen node, GetAttachment would be 2. The reason is that a Hydrogen node uses a
	// new storage model for attachment storage and retrieval.
	assert.Equal(t, int64(2), ar.Push.GetStats().HandleGetAttachment.Value())
	assert.Equal(t, int64(0), ar.Push.GetStats().HandleProveAttachment.Value())
}

// TestActiveReplicatorPushFromCheckpoint:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt1 which can be pushed by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt1.
//   - Starts the push replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPushFromCheckpoint(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT1DocsInitial = 13 // 2 batches of changes
		numRT1DocsTotal   = 24 // 2 more batches
	)

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	// Create first batch of docs
	docIDPrefix := t.Name() + "rt2doc"
	for i := 0; i < numRT1DocsInitial; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:         true,
		ChangesBatchSize:   changesBatchSize,
		CollectionsEnabled: !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name()+"1", false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)
	arConfig.ReplicationStatsMap = dbstats
	ar := db.NewActiveReplicator(ctx1, &arConfig)

	startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()

	require.NoError(t, ar.Start(ctx1))

	// wait for all of the documents originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(numRT1DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT1DocsInitial)
	docIDsSeen := make(map[string]bool, numRT1DocsInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numRT1DocsInitial; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
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
	numRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, startNumRevsSentTotal+numRT1DocsInitial, numRevsSentTotal)
	assert.Equal(t, int64(numRT1DocsInitial), pushCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT1DocsInitial), pushCheckpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt2.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
	require.NoError(t, ar.Stop())

	// Second batch of docs
	for i := numRT1DocsInitial; i < numRT1DocsTotal; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	stats, err = base.SyncGatewayStats.NewDBStats(t.Name()+"2", false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err = stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)
	arConfig.ReplicationStatsMap = dbstats
	ar = db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, ar.Start(ctx1))
	defer func() { assert.NoError(t, ar.Stop()) }()

	// new replicator - new checkpointer
	pushCheckpointer = ar.Push.GetSingleCollection(t).Checkpointer

	// wait for all of the documents originally written to rt1 to arrive at rt2
	changesResults, err = rt2.WaitForChanges(numRT1DocsTotal, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT1DocsTotal)

	docIDsSeen = make(map[string]bool, numRT1DocsTotal)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}

	for i := 0; i < numRT1DocsTotal; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])
	}

	// Make sure we've not started any more since:0 replications on rt1 since the first one
	endNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure the new replicator has only sent new mutations
	numRevsSentNewReplicator := ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, numRT1DocsTotal-numRT1DocsInitial, int(numRevsSentNewReplicator))
	assert.Equal(t, int64(numRT1DocsTotal-numRT1DocsInitial), pushCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT1DocsTotal-numRT1DocsInitial), pushCheckpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt2.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
	pushCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorEdgeCheckpointNameCollisions:
//   - Starts 3 RestTesters, one to create documents, and two running pull replications from the central cluster
//   - Replicators running on the edges have identical IDs (e.g. edge-repl)
func TestActiveReplicatorEdgeCheckpointNameCollisions(t *testing.T) {
	base.RequireNumTestBuckets(t, 3)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize  = 10
		numRT1DocsInitial = 13 // 2 batches of changes
	)

	// Central cluster
	rt1 := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: channels.DocChannelsSyncFunction,
	})

	defer rt1.Close()

	username := "alice"
	rt1.CreateUser(username, []string{username})

	// Create first batch of docs
	docIDPrefix := t.Name() + "rt1doc"
	for i := 0; i < numRT1DocsInitial; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	// Make rt1 listen on an actual HTTP port, so it can receive the blipsync request from edges
	srv := httptest.NewServer(rt1.TestPublicHandler())
	defer srv.Close()

	// Build rt1DBURL with basic auth creds
	rt1DBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	rt1DBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

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
		RemoteDBURL: rt1DBURL,
		ActiveDB: &db.Database{
			DatabaseContext: edge1.GetDatabase(),
		},
		Continuous:         true,
		ChangesBatchSize:   changesBatchSize,
		CollectionsEnabled: !rt1.GetDatabase().OnlyDefaultCollection(),
	}
	arConfig.SetCheckpointPrefix(t, "cluster1:")

	// Create the first active replicator to pull from seq:0
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name()+"edge1", false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)
	arConfig.ReplicationStatsMap = dbstats
	edge1Replicator := db.NewActiveReplicator(ctx1, &arConfig)

	startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsHandledTotal := edge1Replicator.Pull.GetStats().HandleRevCount.Value()

	assert.NoError(t, edge1Replicator.Start(ctx1))

	// wait for all of the documents originally written to rt1 to arrive at edge1
	changesResults, err := edge1.WaitForChanges(numRT1DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	edge1LastSeq := changesResults.Last_Seq
	require.Len(t, changesResults.Results, numRT1DocsInitial)
	docIDsSeen := make(map[string]bool, numRT1DocsInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numRT1DocsInitial; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := edge1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt1", body["source"])
	}

	edge1PullCheckpointer := edge1Replicator.Pull.GetSingleCollection(t).Checkpointer
	edge1PullCheckpointer.CheckpointNow()

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsHandledTotal := edge1Replicator.Pull.GetStats().HandleRevCount.Value()
	assert.Equal(t, startNumRevsHandledTotal+numRT1DocsInitial, numRevsHandledTotal)
	assert.Equal(t, int64(numRT1DocsInitial), edge1PullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT1DocsInitial), edge1PullCheckpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt1.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(0), edge1PullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), edge1PullCheckpointer.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(1), edge1PullCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, edge1Replicator.Stop())

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
	edge2Replicator := db.NewActiveReplicator(ctx2, &arConfig)
	assert.NoError(t, edge2Replicator.Start(ctx2))

	changesResults, err = edge2.WaitForChanges(numRT1DocsInitial, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)

	edge2PullCheckpointer := edge2Replicator.Pull.GetSingleCollection(t).Checkpointer
	edge2PullCheckpointer.CheckpointNow()

	// make sure that edge 2 didn't use a checkpoint
	// CBG-2767 skip assertions for GetCheckpoint stats
	if edge2.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(0), edge2PullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), edge2PullCheckpointer.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(1), edge2PullCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, edge2Replicator.Stop())

	resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s%d", docIDPrefix, numRT1DocsInitial), `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt1.WaitForPendingChanges())

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

	edge1Replicator2 := db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, edge1Replicator2.Start(ctx1))

	changesResults, err = edge1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", edge1LastSeq), "", true)
	require.NoErrorf(t, err, "changesResults: %v", changesResults)
	changesResults.RequireDocIDs(t, []string{fmt.Sprintf("%s%d", docIDPrefix, numRT1DocsInitial)})

	edge1Checkpointer2 := edge1Replicator2.Pull.GetSingleCollection(t).Checkpointer
	edge1Checkpointer2.CheckpointNow()
	// CBG-2767 skip assertions for GetCheckpoint stats
	if edge1.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(1), edge1Checkpointer2.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(0), edge1Checkpointer2.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(1), edge1Checkpointer2.Stats().SetCheckpointCount)

	require.NoError(t, edge1Replicator2.Stop())
}

// TestActiveReplicatorPushOneshot:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which can be pushed by the replicator.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushOneshot(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	localDoc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the replication to stop
	replicationStopped := false
	for i := 0; i < 100; i++ {
		status := ar.GetStatus()
		if status.Status == db.ReplicationStateStopped {
			replicationStopped = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	assert.True(t, replicationStopped, "One-shot replication status should go to stopped on completion")

	doc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	assert.Equal(t, strconv.FormatUint(localDoc.Sequence, 10), ar.GetStatus().LastSeqPush)
}

// TestActiveReplicatorPullTombstone:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
//   - Deletes the document in rt2, and waits for the tombstone to get to rt1.
func TestActiveReplicatorPullTombstone(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	// Passive
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active

	rt1 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])

	// Tombstone the doc in rt2
	resp = rt2.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/"+docID+"?rev="+revID, ``)
	rest.RequireStatus(t, resp, http.StatusOK)
	revID = rest.RespRevID(t, resp)

	// wait for the tombstone written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+strconv.FormatUint(doc.Sequence, 10), "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err = rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.True(t, doc.IsDeleted())
	assert.Equal(t, revID, doc.SyncData.CurrentRev)
}

// TestActiveReplicatorPullPurgeOnRemoval:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
//   - Drops the document out of the channel so the replicator in rt1 pulls a _removed revision.
func TestActiveReplicatorPullPurgeOnRemoval(t *testing.T) {

	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyReplicate)

	// Passive
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])

	resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"?rev="+revID, `{"source":"rt2","channels":["bob"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// wait for the channel removal written to rt2 to arrive at rt1 - we can't monitor _changes, because we've purged, not removed. But we can monitor the associated stat.
	base.WaitForStat(func() int64 {
		stats := ar.GetStatus()
		return stats.DocsPurged
	}, 1)

	doc, err = rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err), "Error returned wasn't a DocNotFound error")
	assert.Nil(t, doc)
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
		name                    string
		localRevisionBody       db.Body
		localRevID              string
		remoteRevisionBody      db.Body
		remoteRevID             string
		conflictResolver        string
		expectedLocalBody       db.Body
		expectedLocalRevID      string
		expectedTombstonedRevID string
		expectedResolutionType  db.ConflictResolutionType
		skipActiveLeafAssertion bool
		skipBodyAssertion       bool
	}{
		{
			name:                   "remoteWins",
			localRevisionBody:      db.Body{"source": "local"},
			localRevID:             "1-a",
			remoteRevisionBody:     db.Body{"source": "remote"},
			remoteRevID:            "1-b",
			conflictResolver:       `function(conflict) {return conflict.RemoteDocument;}`,
			expectedLocalBody:      db.Body{"source": "remote"},
			expectedLocalRevID:     "1-b",
			expectedResolutionType: db.ConflictResolutionRemote,
		},
		{
			name:               "merge",
			localRevisionBody:  db.Body{"source": "local"},
			localRevID:         "1-a",
			remoteRevisionBody: db.Body{"source": "remote"},
			remoteRevID:        "1-b",
			conflictResolver: `function(conflict) {
					var mergedDoc = new Object();
					mergedDoc.source = "merged";
					return mergedDoc;
				}`,
			expectedLocalBody:      db.Body{"source": "merged"},
			expectedLocalRevID:     document.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"merged"}`)), // rev for merged body, with parent 1-b
			expectedResolutionType: db.ConflictResolutionMerge,
		},
		{
			name:                   "localWins",
			localRevisionBody:      db.Body{"source": "local"},
			localRevID:             "1-a",
			remoteRevisionBody:     db.Body{"source": "remote"},
			remoteRevID:            "1-b",
			conflictResolver:       `function(conflict) {return conflict.LocalDocument;}`,
			expectedLocalBody:      db.Body{"source": "local"},
			expectedLocalRevID:     document.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"local"}`)), // rev for local body, transposed under parent 1-b
			expectedResolutionType: db.ConflictResolutionLocal,
		},
		{
			name:                    "twoTombstonesRemoteWin",
			localRevisionBody:       db.Body{"_deleted": true, "source": "local"},
			localRevID:              "1-a",
			remoteRevisionBody:      db.Body{"_deleted": true, "source": "remote"},
			remoteRevID:             "1-b",
			conflictResolver:        `function(conflict){}`,
			expectedLocalBody:       db.Body{"source": "remote"},
			expectedLocalRevID:      "1-b",
			skipActiveLeafAssertion: true,
			skipBodyAssertion:       base.TestUseXattrs(),
		},
		{
			name:                    "twoTombstonesLocalWin",
			localRevisionBody:       db.Body{"_deleted": true, "source": "local"},
			localRevID:              "1-b",
			remoteRevisionBody:      db.Body{"_deleted": true, "source": "remote"},
			remoteRevID:             "1-a",
			conflictResolver:        `function(conflict){}`,
			expectedLocalBody:       db.Body{"source": "local"},
			expectedLocalRevID:      "1-b",
			skipActiveLeafAssertion: true,
			skipBodyAssertion:       base.TestUseXattrs(),
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			base.RequireNumTestBuckets(t, 2)
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD)

			// Passive
			rt2 := rest.NewRestTester(t, nil)
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{"*"})

			// Create revision on rt2 (remote)
			docID := test.name
			resp, err := rt2.PutDocumentWithRevID(docID, test.remoteRevID, "", test.remoteRevisionBody)
			assert.NoError(t, err)
			rest.RequireStatus(t, resp, http.StatusCreated)
			rt2revID := rest.RespRevID(t, resp)
			assert.Equal(t, test.remoteRevID, rt2revID)

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

			// Active
			rt1 := rest.NewRestTester(t, nil)
			defer rt1.Close()
			ctx1 := rt1.Context()

			// Create revision on rt1 (local)
			resp, err = rt1.PutDocumentWithRevID(docID, test.localRevID, "", test.localRevisionBody)
			assert.NoError(t, err)
			rest.RequireStatus(t, resp, http.StatusCreated)
			rt1revID := rest.RespRevID(t, resp)
			assert.Equal(t, test.localRevID, rt1revID)

			customConflictResolver, err := db.NewCustomConflictResolver(test.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)
			stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
			require.NoError(t, err)
			replicationStats, err := stats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:     200,
				ConflictResolverFunc: customConflictResolver,
				Continuous:           true,
				ReplicationStatsMap:  replicationStats,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
			})
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start(ctx1))

			rest.WaitAndRequireCondition(t, func() bool { return ar.GetStatus().DocsRead == 1 }, "Expecting DocsRead == 1")
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

			changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedLocalRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLocalRevID, doc.SyncData.CurrentRev)

			// This is skipped for tombstone tests running with xattr as xattr tombstones don't have a body to assert
			// against
			if !test.skipBodyAssertion {
				assert.Equal(t, test.expectedLocalBody, doc.UnmarshalBody())
			}

			log.Printf("Doc %s is %+v", docID, doc)
			for revID, revInfo := range doc.SyncData.History {
				log.Printf("doc revision [%s]: %+v", revID, revInfo)
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
}

// TestActiveReplicatorPushAndPullConflict:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pushAndPull from rt2.
//   - verifies expected conflict resolution, and that expected result is replicated to both peers
func TestActiveReplicatorPushAndPullConflict(t *testing.T) {

	base.LongRunningTest(t)

	// scenarios
	conflictResolutionTests := []struct {
		name                    string
		localRevisionBody       []byte
		localRevID              string
		remoteRevisionBody      []byte
		remoteRevID             string
		commonAncestorRevID     string
		conflictResolver        string
		expectedBody            []byte
		expectedRevID           string
		expectedTombstonedRevID string
	}{
		{
			name:               "remoteWins",
			localRevisionBody:  []byte(`{"source": "local"}`),
			localRevID:         "1-a",
			remoteRevisionBody: []byte(`{"source": "remote"}`),
			remoteRevID:        "1-b",
			conflictResolver:   `function(conflict) {return conflict.RemoteDocument;}`,
			expectedBody:       []byte(`{"source": "remote"}`),
			expectedRevID:      "1-b",
		},
		{
			name:               "merge",
			localRevisionBody:  []byte(`{"source": "local"}`),
			localRevID:         "1-a",
			remoteRevisionBody: []byte(`{"source": "remote"}`),
			remoteRevID:        "1-b",
			conflictResolver: `function(conflict) {
							var mergedDoc = new Object();
							mergedDoc.source = "merged";
							return mergedDoc;
						}`,
			expectedBody:  []byte(`{"source": "merged"}`),
			expectedRevID: document.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"merged"}`)), // rev for merged body, with parent 1-b
		},
		{
			name:               "localWins",
			localRevisionBody:  []byte(`{"source": "local"}`),
			localRevID:         "1-a",
			remoteRevisionBody: []byte(`{"source": "remote"}`),
			remoteRevID:        "1-b",
			conflictResolver:   `function(conflict) {return conflict.LocalDocument;}`,
			expectedBody:       []byte(`{"source": "local"}`),
			expectedRevID:      document.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"local"}`)), // rev for local body, transposed under parent 1-b
		},
		{
			name:                "localWinsRemoteTombstone",
			localRevisionBody:   []byte(`{"source": "local"}`),
			localRevID:          "2-a",
			remoteRevisionBody:  []byte(`{"_deleted": true}`),
			remoteRevID:         "2-b",
			commonAncestorRevID: "1-a",
			conflictResolver:    `function(conflict) {return conflict.LocalDocument;}`,
			expectedBody:        []byte(`{"source": "local"}`),
			expectedRevID:       document.CreateRevIDWithBytes(3, "2-b", []byte(`{"source":"local"}`)), // rev for local body, transposed under parent 2-b
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			base.RequireNumTestBuckets(t, 2)
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD)

			// Passive
			rt2 := rest.NewRestTester(t, nil)
			defer rt2.Close()
			username := "alice"
			rt2.CreateUser(username, []string{"*"})

			var localRevisionBody db.Body
			assert.NoError(t, json.Unmarshal(test.localRevisionBody, &localRevisionBody))

			var remoteRevisionBody db.Body
			assert.NoError(t, json.Unmarshal(test.remoteRevisionBody, &remoteRevisionBody))

			var expectedLocalBody db.Body
			assert.NoError(t, json.Unmarshal(test.expectedBody, &expectedLocalBody))

			// Create revision on rt2 (remote)
			docID := test.name

			if test.commonAncestorRevID != "" {
				resp, err := rt2.PutDocumentWithRevID(docID, test.commonAncestorRevID, "", remoteRevisionBody)
				assert.NoError(t, err)
				rest.RequireStatus(t, resp, http.StatusCreated)
				rt2revID := rest.RespRevID(t, resp)
				assert.Equal(t, test.commonAncestorRevID, rt2revID)
			}

			resp, err := rt2.PutDocumentWithRevID(docID, test.remoteRevID, test.commonAncestorRevID, remoteRevisionBody)
			assert.NoError(t, err)
			rest.RequireStatus(t, resp, http.StatusCreated)
			rt2revID := rest.RespRevID(t, resp)
			assert.Equal(t, test.remoteRevID, rt2revID)

			remoteDoc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalSync)
			require.NoError(t, err)

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

			// Active
			rt1 := rest.NewRestTester(t, nil)
			defer rt1.Close()
			ctx1 := rt1.Context()

			// Create revision on rt1 (local)
			if test.commonAncestorRevID != "" {
				resp, err = rt1.PutDocumentWithRevID(docID, test.commonAncestorRevID, "", localRevisionBody)
				assert.NoError(t, err)
				rest.RequireStatus(t, resp, http.StatusCreated)
				rt1revID := rest.RespRevID(t, resp)
				assert.Equal(t, test.commonAncestorRevID, rt1revID)
			}

			resp, err = rt1.PutDocumentWithRevID(docID, test.localRevID, test.commonAncestorRevID, localRevisionBody)
			assert.NoError(t, err)
			rest.RequireStatus(t, resp, http.StatusCreated)
			rt1revID := rest.RespRevID(t, resp)
			assert.Equal(t, test.localRevID, rt1revID)

			localDoc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalSync)
			require.NoError(t, err)

			customConflictResolver, err := db.NewCustomConflictResolver(test.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)

			stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
			require.NoError(t, err)
			dbstats, err := stats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:     200,
				ConflictResolverFunc: customConflictResolver,
				Continuous:           true,
				ReplicationStatsMap:  dbstats,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
			})
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start(ctx1))
			// wait for the document originally written to rt2 to arrive at rt1.  Should end up as winner under default conflict resolution
			base.WaitForStat(func() int64 {
				return ar.GetStatus().DocsWritten
			}, 1)
			log.Printf("========================Replication should be done, checking with changes")

			// Validate results on the local (rt1)
			changesResults, err := rt1.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", localDoc.Sequence), "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			rawDocResponse := rt1.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/_raw/"+docID, "")
			log.Printf("Raw response: %s", rawDocResponse.Body.Bytes())

			docResponse := rt1.SendAdminRequest(http.MethodGet, "/{{.keyspace}}/"+docID, "")
			log.Printf("Non-raw response: %s", docResponse.Body.Bytes())

			doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedRevID, doc.SyncData.CurrentRev)
			assert.Equal(t, db.Body(expectedLocalBody), doc.UnmarshalBody())
			log.Printf("Doc %s is %+v", docID, doc)
			log.Printf("Doc %s attachments are %+v", docID, doc.Attachments)
			for revID, revInfo := range doc.SyncData.History {
				log.Printf("doc revision [%s]: %+v", revID, revInfo)
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
			if test.expectedRevID == test.remoteRevID {
				// no changes should have been pushed back up to rt2, because this rev won.
				rt2Since = 0
			}
			changesResults, err = rt2.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%d", rt2Since), "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err = rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedRevID, doc.SyncData.CurrentRev)
			assert.Equal(t, db.Body(expectedLocalBody), doc.UnmarshalBody())
			log.Printf("Remote Doc %s is %+v", docID, doc)
			log.Printf("Remote Doc %s attachments are %+v", docID, doc.Attachments)
			for revID, revInfo := range doc.SyncData.History {
				log.Printf("doc revision [%s]: %+v", revID, revInfo)
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
}

// TestActiveReplicatorPushBasicWithInsecureSkipVerify:
//   - Starts 2 RestTesters, one active (with InsecureSkipVerify), and one passive
//   - Creates a document on rt1 which can be pushed by the replicator to rt2.
//   - rt2 served using a self-signed TLS cert (via httptest)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasicWithInsecureSkipVerifyEnabled(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()
	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewTLSServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		InsecureSkipVerify:  true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	require.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])
}

// TestActiveReplicatorPushBasicWithInsecureSkipVerifyDisabled:
//   - Starts 2 RestTesters, one active, and one passive
//   - Creates a document on rt1 which can be pushed by the replicator to rt2.
//   - rt2 served using a self-signed TLS cert (via httptest)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasicWithInsecureSkipVerifyDisabled(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewTLSServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		InsecureSkipVerify:  false,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	assert.Error(t, ar.Start(ctx1), "Error certificate signed by unknown authority")
}

// TestActiveReplicatorRecoverFromLocalFlush:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which is pulled to rt1.
//   - Checkpoints once finished.
//   - Recreates rt1 with a new bucket (to simulate a flush).
//   - Starts the replication again, and ensures that documents are re-replicated to it.
func TestActiveReplicatorRecoverFromLocalFlush(t *testing.T) {

	base.RequireNumTestBuckets(t, 3)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	// Passive
	rt2 := rest.NewRestTesterDefaultCollection(t, // CBG-2379 test requires default collection
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Create doc on rt2
	docID := t.Name() + "rt2doc"
	resp := rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt2.WaitForPendingChanges())

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active
	rt1 := rest.NewRestTesterDefaultCollection(t, nil) // CBG-2379 test requires default collection
	ctx1 := rt1.Context()
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

	assert.NoError(t, ar.Start(ctx1))

	// wait for document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

	// rev assertions
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+1, numRevsSentTotal)
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt2.GetDatabase().OnlyDefaultCollection() {
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
	}
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
	assert.NoError(t, ar.Stop())

	// close rt1, and release the underlying bucket back to the pool.
	rt1.Close()

	// recreate rt1 with a new bucket
	rt1 = rest.NewRestTesterDefaultCollection(t, nil) // CBG-2379 test requires default collection
	defer rt1.Close()
	ctx1 = rt1.Context()

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	// Have to re-set ActiveDB because we recreated it with the new rt1.
	arConfig.ActiveDB = &db.Database{
		DatabaseContext: rt1.GetDatabase(),
	}
	ar = db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start(ctx1))

	// new replicator - new checkpointer
	pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

	// we pulled the remote checkpoint, but the local checkpoint wasn't there to match it.
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt1.GetDatabase().OnlyDefaultCollection() {
		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
	}
	// wait for document originally written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err = rt1.GetSingleTestDatabaseCollectionWithUser().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])

	// one _changes from seq:0 with initial number of docs sent
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal+1, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+2, numRevsSentTotal)
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), pullCheckpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorRecoverFromRemoteFlush:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Checkpoints once finished.
//   - Recreates rt2 with a new bucket (to simulate a flush).
//   - Starts the replication again, and ensures that post-flush, documents are re-replicated to it.
func TestActiveReplicatorRecoverFromRemoteFlush(t *testing.T) {

	base.RequireNumTestBuckets(t, 3)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	// Passive
	rt2 := rest.NewRestTester(t, nil)

	username := "alice"
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
	docID := t.Name() + "rt1doc"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt1.WaitForPendingChanges())

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:         true,
		CollectionsEnabled: !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name()+"1", false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)
	arConfig.ReplicationStatsMap = dbstats
	ar := db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	// startNumRevsSentTotal := ar.Pull.GetStats().SendRevCount.Value()
	startNumRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()

	assert.NoError(t, ar.Start(ctx1))

	pushCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	_, ok := base.WaitForStat(func() int64 {
		return ar.Push.GetStats().SendRevCount.Value()
	}, startNumRevsSentTotal+1)
	assert.True(t, ok)
	assert.Equal(t, int64(1), pushCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), pushCheckpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt1.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointMissCount)
	}
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
	pushCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

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

	ar = db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start(ctx1))

	pushCheckpointer = ar.Push.GetSingleCollection(t).Checkpointer

	// we pulled the remote checkpoint, but the local checkpoint wasn't there to match it.
	assert.Equal(t, int64(0), pushCheckpointer.Stats().GetCheckpointHitCount)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err = rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	// one _changes from seq:0 with initial number of docs sent
	endNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal+1, endNumChangesRequestedFromZeroTotal)

	// make sure the replicator has resent the rev
	_, ok = base.WaitForStat(func() int64 {
		return ar.Push.GetStats().SendRevCount.Value()
	}, startNumRevsSentTotal+1)
	assert.True(t, ok)
	assert.Equal(t, int64(1), pushCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), pushCheckpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt1.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(1), pushCheckpointer.Stats().GetCheckpointMissCount)
	}
	assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
	pushCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorRecoverFromRemoteRollback:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Checkpoints.
//   - Creates another document on rt1 which is again pushed to rt2.
//   - Manually rolls back the bucket to the first document.
//   - Starts the replication again, and ensures that documents are re-replicated to it.
func TestActiveReplicatorRecoverFromRemoteRollback(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyBucket, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	ctx2 := rt2.Context()

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

	// Create doc1 on rt1
	docID := t.Name() + "rt1doc"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt1.WaitForPendingChanges())
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start(ctx1))

	pushCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer

	base.WaitForStat(func() int64 {
		return ar.Push.GetStats().SendRevCount.Value()
	}, 1)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)
	lastSeq := changesResults.Last_Seq.(string)

	doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
	pushCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

	cID := ar.Push.CheckpointID
	checkpointDocID := base.SyncDocPrefix + "local:checkpoint/" + cID

	var firstCheckpoint interface{}
	_, err = rt2.GetSingleDataStore().Get(checkpointDocID, &firstCheckpoint)
	require.NoError(t, err)

	// Create doc2 on rt1
	resp = rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID+"2", `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt1.WaitForPendingChanges())

	base.WaitForStat(func() int64 {
		return ar.Push.GetStats().SendRevCount.Value()
	}, 2)

	// wait for new document to arrive at rt2
	changesResults, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID+"2", changesResults.Results[0].ID)

	doc, err = rt2.GetSingleTestDatabaseCollection().GetDocument(ctx2, docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)
	pushCheckpointer.CheckpointNow()
	assert.Equal(t, int64(2), pushCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// roll back checkpoint value to first one and remove the associated doc
	err = rt2.GetSingleDataStore().Set(checkpointDocID, 0, nil, firstCheckpoint)
	assert.NoError(t, err)

	rt2collection := rt2.GetSingleTestDatabaseCollectionWithUser()
	err = rt2collection.Purge(ctx2, docID+"2")
	assert.NoError(t, err)

	require.NoError(t, rt2collection.FlushChannelCache(ctx2))
	rt2collection.FlushRevisionCacheForTest()

	assert.NoError(t, ar.Start(ctx1))

	pushCheckpointer = ar.Push.GetSingleCollection(t).Checkpointer

	// wait for new document to arrive at rt2 again
	changesResults, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since="+lastSeq, "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID+"2", changesResults.Results[0].ID)

	doc, err = rt2collection.GetDocument(ctx2, docID, db.DocUnmarshalAll)
	require.NoError(t, err)

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
	pushCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)
	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorRecoverFromMismatchedRev:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Modifies the checkpoint rev ID in the target bucket.
//   - Checkpoints again to ensure it is retried on error.
func TestActiveReplicatorRecoverFromMismatchedRev(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyBucket, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	// Passive
	rt2 := rest.NewRestTesterDefaultCollection(t, nil) // CBG-2775 doesn't work yet with default collection
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active
	rt1 := rest.NewRestTesterDefaultCollection(t, nil) // CBG-2775 doesn't work yet with default collection
	defer rt1.Close()
	ctx1 := rt1.Context()
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start(ctx1))

	defer func() {
		assert.NoError(t, ar.Stop())
	}()

	pushCheckpointID := ar.Push.CheckpointID
	pushCheckpointDocID := base.SyncDocPrefix + "local:checkpoint/" + pushCheckpointID
	err = rt2.GetSingleDataStore().Set(pushCheckpointDocID, 0, nil, map[string]interface{}{"last_sequence": "0", "_rev": "abc"})
	require.NoError(t, err)

	pullCheckpointID := ar.Pull.CheckpointID
	require.NoError(t, err)
	pullCheckpointDocID := base.SyncDocPrefix + "local:checkpoint/" + pullCheckpointID
	err = rt1.GetSingleDataStore().Set(pullCheckpointDocID, 0, nil, map[string]interface{}{"last_sequence": "0", "_rev": "abc"})
	require.NoError(t, err)

	// Create doc1 on rt1
	docID := t.Name() + "rt1doc"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.NoError(t, rt1.WaitForPendingChanges())

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	// Create doc2 on rt2
	docID = t.Name() + "rt2doc"
	resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+docID, `{"source":"rt2","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	assert.NoError(t, rt2.WaitForPendingChanges())

	// wait for document originally written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?since=1", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	pushCheckpointer := ar.Push.GetSingleCollection(t).Checkpointer
	assert.Equal(t, int64(0), pushCheckpointer.Stats().SetCheckpointCount)
	pushCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pushCheckpointer.Stats().SetCheckpointCount)

	pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

}

// TestActiveReplicatorIgnoreNoConflicts ensures the IgnoreNoConflicts flag allows Hydrogen<-->Hydrogen replication with no_conflicts set.
func TestActiveReplicatorIgnoreNoConflicts(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)

	// Passive
	rt2 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				AllowConflicts: base.BoolPtr(false),
			}},
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{username})

	// Active
	rt1 := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
				AllowConflicts: base.BoolPtr(false),
			}},
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt1.Close()
	ctx1 := rt1.Context()

	rt1docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+rt1docID, `{"source":"rt1","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rt1revID := rest.RespRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx1))

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, rt1docID, changesResults.Results[0].ID)

	doc, err := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), rt1docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, rt1revID, doc.SyncData.CurrentRev)

	body, err := doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt1", body["source"])

	// write a doc on rt2 ...
	rt2docID := t.Name() + "rt2doc1"
	resp = rt2.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+rt2docID, `{"source":"rt2","channels":["alice"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rt2revID := rest.RespRevID(t, resp)

	// ... and wait to arrive at rt1
	changesResults, err = rt1.WaitForChanges(2, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 2)
	assert.Equal(t, rt1docID, changesResults.Results[0].ID)
	assert.Equal(t, rt2docID, changesResults.Results[1].ID)

	doc, err = rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), rt2docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, rt2revID, doc.SyncData.CurrentRev)

	body, err = doc.GetDeepMutableBody()
	require.NoError(t, err)
	assert.Equal(t, "rt2", body["source"])
}

// TestActiveReplicatorPullFromCheckpointModifiedHash:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt2 which can be pulled by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt2.
//   - Starts the pull replication again with a config change, validate checkpoint is reset
func TestActiveReplicatorPullModifiedHash(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		changesBatchSize         = 10
		numDocsPerChannelInitial = 13 // 2 batches of changes
		numDocsPerChannelTotal   = 24 // 2 more batches
		numChannels              = 2  // two channels
	)

	// Passive
	// CBG-2759 needs channel filtering to use non-default collection
	rt2 := rest.NewRestTesterDefaultCollection(t,
		&rest.RestTesterConfig{
			SyncFn: channels.DocChannelsSyncFunction,
		})
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{"chan1", "chan2"})

	// Create first batch of docs, creating numRT2DocsInitial in each channel
	docIDPrefix := t.Name() + "rt2doc"
	for i := 0; i < numDocsPerChannelInitial; i++ {
		rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i), `{"source":"rt2","channels":["chan1"]}`)
		rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i), `{"source":"rt2","channels":["chan2"]}`)
	}

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

	// Active
	// CBG-2759 needs channel filtering to use non-default collection
	rt1 := rest.NewRestTesterDefaultCollection(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    changesBatchSize,
		Filter:              base.ByChannelFilter,
		FilterChannels:      []string{"chan1"},
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull chan1 from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

	assert.NoError(t, ar.Start(ctx1))

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(numDocsPerChannelInitial, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numDocsPerChannelInitial)
	docIDsSeen := make(map[string]bool, numDocsPerChannelInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numDocsPerChannelInitial; i++ {
		docID := fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i)
		assert.True(t, docIDsSeen[docID])
		doc := rt1.GetDoc(docID)
		assert.Equal(t, "rt2", doc["source"])
	}

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	pullCheckpointer := ar.Pull.GetSingleCollection(t).Checkpointer

	// rev assertions
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+int64(numDocsPerChannelInitial), numRevsSentTotal)
	assert.Equal(t, int64(numDocsPerChannelInitial), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numDocsPerChannelInitial), pullCheckpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	// CBG-2767 skip assertions for GetCheckpoint stats
	if rt1.GetDatabase().OnlyDefaultCollection() {

		assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
		assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
	}
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs, both channels
	for i := numDocsPerChannelInitial; i < numDocsPerChannelTotal; i++ {
		rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i), `{"source":"rt2","channels":["chan1"]}`)
		rt2.PutDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i), `{"source":"rt2","channels":["chan2"]}`)
	}

	// Create a new replicator using the same replicationID but different channel filter, which should reset the checkpoint
	arConfig.FilterChannels = []string{"chan2"}
	ar = db.NewActiveReplicator(ctx1, &arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start(ctx1))

	// new replicator - new checkpointer
	pullCheckpointer = ar.Pull.GetSingleCollection(t).Checkpointer

	// wait for all of the documents originally written to rt2 to arrive at rt1
	expectedChan1Docs := numDocsPerChannelInitial
	expectedChan2Docs := numDocsPerChannelTotal
	expectedTotalDocs := expectedChan1Docs + expectedChan2Docs
	changesResults, err = rt1.WaitForChanges(expectedTotalDocs, "/{{.keyspace}}/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, expectedTotalDocs)

	docIDsSeen = make(map[string]bool, expectedTotalDocs)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}

	for i := 0; i < numDocsPerChannelTotal; i++ {
		docID := fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		assert.NoError(t, err)

		body, err := doc.GetDeepMutableBody()
		require.NoError(t, err)
		assert.Equal(t, "rt2", body["source"])
	}

	// Should have two replications since zero
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+2, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+int64(expectedTotalDocs), numRevsSentTotal)
	assert.Equal(t, int64(expectedChan2Docs), pullCheckpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(expectedChan2Docs), pullCheckpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(0), pullCheckpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), pullCheckpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), pullCheckpointer.Stats().SetCheckpointCount)
	pullCheckpointer.CheckpointNow()
	assert.Equal(t, int64(1), pullCheckpointer.Stats().SetCheckpointCount)
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

					base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

					// Passive
					rt2 := rest.NewRestTester(t, nil)
					defer rt2.Close()
					username := "alice"
					rt2.CreateUser(username, []string{username})

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

					// Active
					rt1 := rest.NewRestTester(t, nil)
					defer rt1.Close()
					ctx1 := rt1.Context()

					sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
					require.NoError(t, err)
					dbstats, err := sgwStats.DBReplicatorStats(t.Name())
					require.NoError(t, err)

					id, err := base.GenerateRandomID()
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
					}

					// Create the first active replicator to pull from seq:0
					ar := db.NewActiveReplicator(ctx1, &arConfig)
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
						rest.WaitAndRequireCondition(t, func() bool {
							return ar.Push.GetStats().NumConnectAttempts.Value() > 2
						}, "Expecting NumConnectAttempts > 2")

						time.Sleep(timeoutVal + time.Millisecond*250)
						// wait for the retry loop to hit the TotalReconnectTimeout and give up retrying
						rest.WaitAndRequireCondition(t, func() bool {
							return ar.Push.GetStats().NumReconnectsAborted.Value() > 0
						}, "Expecting NumReconnectsAborted > 0")
					}
				})
			}
		})
	}
}

// TestActiveReplicatorReconnectOnStartEventualSuccess ensures an active replicator with invalid creds retries,
// but succeeds once the user is created on the remote.
func TestActiveReplicatorReconnectOnStartEventualSuccess(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build remoteDBURL with basic auth creds
	remoteDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	remoteDBURL.User = url.UserPassword("alice", "pass")

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	id, err := base.GenerateRandomID()
	require.NoError(t, err)
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
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
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	assert.Equal(t, int64(0), ar.Push.GetStats().NumConnectAttempts.Value())

	// expected error
	msg401 := "unexpected status code 401 from target database"

	err = ar.Start(ctx1)
	defer func() { assert.NoError(t, ar.Stop()) }() // prevents panic if waiting for ar state running fails
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), msg401))

	// wait for an arbitrary number of reconnect attempts
	rest.WaitAndRequireCondition(t, func() bool {
		return ar.Push.GetStats().NumConnectAttempts.Value() > 3
	}, "Expecting NumConnectAttempts > 3")

	resp := rt2.SendAdminRequest(http.MethodPut, "/db/_user/alice", `{"password":"pass"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	rest.WaitAndRequireCondition(t, func() bool {
		state, errMsg := ar.State()
		if strings.TrimSpace(errMsg) != "" && !strings.Contains(errMsg, msg401) {
			log.Println("unexpected replicator error:", errMsg)
		}
		return state == db.ReplicationStateRunning
	}, "Expecting replication state to be running")
}

// TestActiveReplicatorReconnectSendActions ensures ActiveReplicator reconnect retry loops exit when the replicator is stopped
func TestActiveReplicatorReconnectSendActions(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	username := "alice"
	rt2.CreateUser(username, []string{"*"})

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build remoteDBURL with basic auth creds
	remoteDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add incorrect basic auth creds to target db URL
	remoteDBURL.User = url.UserPassword("bob", "pass")

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()
	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	id, err := base.GenerateRandomID()
	require.NoError(t, err)
	arConfig := db.ActiveReplicatorConfig{
		ID:          id,
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: remoteDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous: true,
		// aggressive reconnect intervals for testing purposes
		InitialReconnectInterval: time.Millisecond,
		MaxReconnectInterval:     time.Millisecond * 50,
		TotalReconnectTimeout:    time.Second * 5,
		ReplicationStatsMap:      dbstats,
		CollectionsEnabled:       !rt1.GetDatabase().OnlyDefaultCollection(),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(ctx1, &arConfig)
	require.NoError(t, err)

	assert.Equal(t, int64(0), ar.Pull.GetStats().NumConnectAttempts.Value())

	err = ar.Start(ctx1)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unexpected status code 401 from target database"))

	// wait for an arbitrary number of reconnect attempts
	err = rt1.WaitForCondition(func() bool {
		return ar.Pull.GetStats().NumConnectAttempts.Value() > 3
	})
	assert.NoError(t, err, "Expecting NumConnectAttempts > 3")

	assert.NoError(t, ar.Stop())
	err = rt1.WaitForCondition(func() bool {
		return ar.GetStatus().Status == db.ReplicationStateStopped
	})
	require.NoError(t, err)

	// wait for a bit to see if the reconnect loop has stopped
	reconnectAttempts := ar.Pull.GetStats().NumConnectAttempts.Value()
	time.Sleep(time.Millisecond * 250)
	assert.Equal(t, reconnectAttempts, ar.Pull.GetStats().NumConnectAttempts.Value())

	assert.NoError(t, ar.Reset())

	err = ar.Start(ctx1)
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unexpected status code 401 from target database"))

	// wait for another set of reconnect attempts
	err = rt1.WaitForCondition(func() bool {
		return ar.Pull.GetStats().NumConnectAttempts.Value() > 3
	})
	assert.NoError(t, err, "Expecting NumConnectAttempts > 3")

	require.NoError(t, ar.Stop())
}

// TestActiveReplicatorPullConflictReadWriteIntlProps:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullConflictReadWriteIntlProps(t *testing.T) {

	base.LongRunningTest(t)

	createRevID := func(generation int, parentRevID string, body db.Body) string {
		rev, err := document.CreateRevID(generation, parentRevID, db.Body(body))
		require.NoError(t, err, "Error creating revision")
		return rev
	}
	docExpiry := time.Now().Local().Add(time.Hour * time.Duration(4)).Format(time.RFC3339)

	// scenarios
	conflictResolutionTests := []struct {
		name                string
		commonAncestorRevID string
		localRevisionBody   db.Body
		localRevID          string
		remoteRevisionBody  db.Body
		remoteRevID         string
		conflictResolver    string
		expectedLocalBody   db.Body
		expectedLocalRevID  string
	}{
		{
			name: "mergeReadWriteIntlProps",
			localRevisionBody: db.Body{
				"source": "local",
			},
			localRevID: "1-a",
			remoteRevisionBody: db.Body{
				"source": "remote",
			},
			remoteRevID: "1-b",
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
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
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
			name: "mergeReadWriteAttachments",
			localRevisionBody: map[string]interface{}{
				db.BodyAttachments: map[string]interface{}{
					"A": map[string]interface{}{
						"data": "QQo=",
					}},
				"source": "local",
			},
			localRevID: "1-a",
			remoteRevisionBody: map[string]interface{}{
				db.BodyAttachments: map[string]interface{}{
					"B": map[string]interface{}{
						"data": "Qgo=",
					}},
				"source": "remote",
			},
			remoteRevID: "1-b",
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
			expectedLocalBody: map[string]interface{}{
				"source": "merged",
			},
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				"source": "merged",
			}),
		},
		{
			name: "mergeReadIntlPropsLocalExpiry",
			localRevisionBody: db.Body{
				"source":      "local",
				db.BodyExpiry: docExpiry,
			},
			localRevID:         "1-a",
			remoteRevisionBody: db.Body{"source": "remote"},
			remoteRevID:        "1-b",
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
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				"localDocExp": docExpiry,
				"source":      "merged",
			}),
		},
		{
			name: "mergeWriteIntlPropsExpiry",
			localRevisionBody: db.Body{
				"source":      "local",
				db.BodyExpiry: docExpiry,
			},
			localRevID: "1-a",
			remoteRevisionBody: db.Body{
				"source": "remote",
			},
			remoteRevID: "1-b",
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
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				db.BodyExpiry: docExpiry,
				"source":      "merged",
			}),
		},
		{
			name: "mergeReadIntlPropsDeletedWithLocalTombstone",
			localRevisionBody: db.Body{
				"source":       "local",
				db.BodyDeleted: true,
			},
			commonAncestorRevID: "1-a",
			localRevID:          "2-a",
			remoteRevisionBody: db.Body{
				"source": "remote",
			},
			remoteRevID: "2-b",
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
			expectedLocalRevID: createRevID(3, "2-b", db.Body{
				"localDeleted": true,
				"source":       "merged",
			}),
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			base.RequireNumTestBuckets(t, 2)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

			// Passive
			rt2 := rest.NewRestTester(t, nil)
			defer rt2.Close()

			username := "alice"
			rt2.CreateUser(username, []string{"*"})

			// Create revision on rt2 (remote)
			docID := test.name
			if test.commonAncestorRevID != "" {
				_, err := rt2.PutDocumentWithRevID(docID, test.commonAncestorRevID, "", test.remoteRevisionBody)
				assert.NoError(t, err)
			}
			resp, err := rt2.PutDocumentWithRevID(docID, test.remoteRevID, test.commonAncestorRevID, test.remoteRevisionBody)
			assert.NoError(t, err)
			rest.RequireStatus(t, resp, http.StatusCreated)
			rt2revID := rest.RespRevID(t, resp)
			assert.Equal(t, test.remoteRevID, rt2revID)

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

			// Active
			rt1 := rest.NewRestTester(t, nil)
			defer rt1.Close()
			ctx1 := rt1.Context()

			// Create revision on rt1 (local)
			if test.commonAncestorRevID != "" {
				_, err := rt1.PutDocumentWithRevID(docID, test.commonAncestorRevID, "", test.remoteRevisionBody)
				assert.NoError(t, err)
			}
			resp, err = rt1.PutDocumentWithRevID(docID, test.localRevID, test.commonAncestorRevID, test.localRevisionBody)
			assert.NoError(t, err)
			rest.RequireStatus(t, resp, http.StatusCreated)
			rt1revID := rest.RespRevID(t, resp)
			assert.Equal(t, test.localRevID, rt1revID)

			customConflictResolver, err := db.NewCustomConflictResolver(test.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)
			dbstats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
			require.NoError(t, err)
			replicationStats, err := dbstats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:     200,
				ConflictResolverFunc: customConflictResolver,
				Continuous:           true,
				ReplicationStatsMap:  replicationStats,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
			})
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start(ctx1))
			rest.WaitAndRequireCondition(t, func() bool { return ar.GetStatus().DocsRead == 1 })
			assert.Equal(t, 1, int(replicationStats.ConflictResolvedMergedCount.Value()))

			// Wait for the document originally written to rt2 to arrive at rt1.
			// Should end up as winner under default conflict resolution.
			changesResults, err := rt1.WaitForChanges(1, "/{{.keyspace}}/_changes?&since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedLocalRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLocalRevID, doc.SyncData.CurrentRev)
			log.Printf("doc.Body(): %v", doc.UnmarshalBody())
			assert.Equal(t, db.Body(test.expectedLocalBody), doc.UnmarshalBody())
			log.Printf("Doc %s is %+v", docID, doc)
			for revID, revInfo := range doc.SyncData.History {
				log.Printf("doc revision [%s]: %+v", revID, revInfo)
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
}
func TestSGR2TombstoneConflictHandling(t *testing.T) {
	base.LongRunningTest(t)
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
			rawSyncData, ok := rawBody[base.SyncPropertyName].(map[string]interface{})
			require.True(t, ok)
			val, ok := rawSyncData["flags"].(float64)
			require.True(t, ok)
			require.NotEqual(t, 0, int(val)&channels.Deleted)
		}
	}

	compareDocRev := func(docRev, cmpRev string) (shouldRetry bool, err error, value interface{}) {
		docGen, docHash := db.ParseRevID(docRev)
		cmpGen, cmpHash := db.ParseRevID(cmpRev)
		if docGen == cmpGen {
			if docHash != cmpHash {
				return false, fmt.Errorf("rev generations match but hashes are different: %v, %v", docRev, cmpRev), nil
			}
			return false, nil, docRev
		}
		return true, nil, nil
	}

	maxAttempts := 200
	attemptSleepMs := 100

	for _, test := range tombstoneTests {

		t.Run(test.name, func(t *testing.T) {
			if test.sdkResurrect && !base.TestUseXattrs() {
				t.Skip("SDK resurrect test cases require xattrs to be enabled")
			}

			makeDoc := func(rt *rest.RestTester, docid string, rev string, value string) string {
				var body db.Body
				resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docid+"?rev="+rev, value)
				rest.RequireStatus(t, resp, http.StatusCreated)
				err := json.Unmarshal(resp.BodyBytes(), &body)
				assert.NoError(t, err)
				return body["rev"].(string)
			}

			remotePassiveRT := rest.NewRestTester(t, nil)
			defer remotePassiveRT.Close()

			srv := httptest.NewServer(remotePassiveRT.TestAdminHandler())
			defer srv.Close()

			// Active
			localActiveRT := rest.NewRestTester(t,
				&rest.RestTesterConfig{
					SgReplicateEnabled: true,
				})
			defer localActiveRT.Close()

			replConf := `
			{
				"replication_id": "replication",
				"remote": "` + srv.URL + `/db",
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

			// Wait for document to arrive on the doc is was put on
			err := localActiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, _ := localActiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if doc != nil {
					return compareDocRev(doc.SyncData.CurrentRev, "3-abc")
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			// Wait for document to be replicated
			err = remotePassiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, _ := remotePassiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if doc != nil {
					return compareDocRev(doc.SyncData.CurrentRev, "3-abc")
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			// Stop the replication
			_ = localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=stop", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateStopped)

			// Delete on the short branch and make another doc on the longer branch before deleting it
			if test.longestBranchLocal {
				// Delete doc on remote
				resp = remotePassiveRT.SendAdminRequest("PUT", "/{{.keyspace}}/docid2?rev=3-abc", `{"_deleted": true}`)
				rest.RequireStatus(t, resp, http.StatusCreated)

				// Validate document revision created to prevent race conditions
				err = remotePassiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
					doc, docErr := remotePassiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					if assert.NoError(t, docErr) {
						if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "4-cc0337d9d38c8e5fc930ae3deda62bf8"); value != nil {
							requireTombstone(t, remotePassiveRT.GetSingleDataStore(), "docid2")
						}
						return
					}
					return true, nil, nil
				}, maxAttempts, attemptSleepMs)
				assert.NoError(t, err)

				// Create another rev and then delete doc on local - ie tree is longer
				revid := makeDoc(localActiveRT, "docid2", "3-abc", `{"foo":"bar"}`)
				localActiveRT.DeleteDoc("docid2", revid)

				// Validate local is CBS tombstone, expect not found error
				// Expect KeyNotFound error retrieving local tombstone pre-replication
				requireTombstone(t, localActiveRT.GetSingleDataStore(), "docid2")

			} else {
				// Delete doc on localActiveRT (active / local)
				resp = localActiveRT.SendAdminRequest("PUT", "/{{.keyspace}}/docid2?rev=3-abc", `{"_deleted": true}`)
				rest.RequireStatus(t, resp, http.StatusCreated)

				// Validate document revision created to prevent race conditions
				err = localActiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
					doc, docErr := localActiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					if assert.NoError(t, docErr) {
						if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "4-cc0337d9d38c8e5fc930ae3deda62bf8"); value != nil {
							requireTombstone(t, localActiveRT.GetSingleDataStore(), "docid2")
						}
						return
					}
					return true, nil, nil
				}, maxAttempts, attemptSleepMs)
				assert.NoError(t, err)

				// Create another rev and then delete doc on remotePassiveRT (passive) - ie, tree is longer
				revid := makeDoc(remotePassiveRT, "docid2", "3-abc", `{"foo":"bar"}`)
				remotePassiveRT.DeleteDoc("docid2", revid)

				// Validate local is CBS tombstone, expect not found error
				// Expect KeyNotFound error retrieving remote tombstone pre-replication
				requireTombstone(t, remotePassiveRT.GetSingleDataStore(), "docid2")
			}

			// Start up repl again
			_ = localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=start", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

			// Wait for the recently longest branch to show up on both sides
			err = localActiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, docErr := localActiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if assert.NoError(t, docErr) {
					if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "5-4a5f5a35196c37c117737afd5be1fc9b"); value != nil {
						// Validate local is CBS tombstone, expect not found error
						// Expect KeyNotFound error retrieving local tombstone post-replication
						requireTombstone(t, localActiveRT.GetSingleDataStore(), "docid2")
					}
					return
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			err = remotePassiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, docErr := remotePassiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if assert.NoError(t, docErr) {
					if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "5-4a5f5a35196c37c117737afd5be1fc9b"); value != nil {
						// Validate remote is CBS tombstone
						// Expect KeyNotFound error retrieving remote tombstone post-replication
						requireTombstone(t, remotePassiveRT.GetSingleDataStore(), "docid2")
					}
					return
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			// Stop the replication
			_ = localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=stop", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateStopped)

			// Resurrect Doc
			updatedBody := make(map[string]interface{})
			updatedBody["resurrection"] = true
			if test.resurrectLocal {
				if test.sdkResurrect {
					// resurrect doc via SDK on local
					err = localActiveRT.GetSingleDataStore().Set("docid2", 0, nil, updatedBody)
					assert.NoError(t, err, "Unable to resurrect doc docid2")
					// force on-demand import
					_, getErr := localActiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					assert.NoError(t, getErr, "Unable to retrieve resurrected doc docid2")
				} else {
					resp = localActiveRT.SendAdminRequest("PUT", "/{{.keyspace}}/docid2", `{"resurrection": true}`)
					rest.RequireStatus(t, resp, http.StatusCreated)
				}
			} else {
				if test.sdkResurrect {
					// resurrect doc via SDK on remote
					err = remotePassiveRT.GetSingleDataStore().Set("docid2", 0, nil, updatedBody)
					assert.NoError(t, err, "Unable to resurrect doc docid2")
					// force on-demand import
					_, getErr := remotePassiveRT.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					assert.NoError(t, getErr, "Unable to retrieve resurrected doc docid2")
				} else {
					resp = remotePassiveRT.SendAdminRequest("PUT", "/{{.keyspace}}/docid2", `{"resurrection": true}`)
					rest.RequireStatus(t, resp, http.StatusCreated)
				}
			}

			// For SG resurrect, rev history is preserved, expect rev 6-...
			expectedRevID := "6-bf187e11c1f8913769dca26e56621036"
			if test.sdkResurrect {
				// For SDK resurrect, rev history is not preserved, expect rev 1-...
				expectedRevID = "1-e5d43a9cdc4a2d4e258800dfc37e9d77"
			}

			// Wait for doc to show up on side that the resurrection was done
			if test.resurrectLocal {
				err = localActiveRT.WaitForRev("docid2", expectedRevID)
			} else {
				err = remotePassiveRT.WaitForRev("docid2", expectedRevID)
			}
			require.NoError(t, err)

			// Start the replication
			_ = localActiveRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/replication?action=start", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

			// Wait for doc to replicate from side resurrection was done on to the other side
			if test.resurrectLocal {
				err = remotePassiveRT.WaitForRev("docid2", expectedRevID)
			} else {
				err = localActiveRT.WaitForRev("docid2", expectedRevID)
			}
			assert.NoError(t, err)
		})
	}
}

// This test ensures that the local tombstone revision wins over non-tombstone revision
// whilst applying default conflict resolution policy through pushAndPull replication.
func TestDefaultConflictResolverWithTombstoneLocal(t *testing.T) {

	base.LongRunningTest(t)
	base.RequireNumTestBuckets(t, 2)
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

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
		t.Run(test.name, func(tt *testing.T) {
			// Passive
			rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
				SyncFn: channels.DocChannelsSyncFunction,
			})

			defer rt2.Close()

			username := "alice"
			rt2.CreateUser(username, []string{username})

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			// Build passiveDBURL with basic auth creds
			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)
			passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

			// Active
			rt1 := rest.NewRestTester(t, nil)
			defer rt1.Close()
			ctx1 := rt1.Context()

			defaultConflictResolver, err := db.NewCustomConflictResolver(
				`function(conflict) { return defaultPolicy(conflict); }`, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err, "Error creating custom conflict resolver")
			sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
			require.NoError(t, err)
			dbstats, err := sgwStats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			config := db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				Continuous:           true,
				ConflictResolverFunc: defaultConflictResolver,
				ReplicationStatsMap:  dbstats,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
			}

			// Create the first revision of the document on rt1.
			docID := t.Name() + "foo"
			rt1RevIDCreated := createOrUpdateDoc(t, rt1, docID, "", "foo")

			// Create active replicator and start replication.
			ar := db.NewActiveReplicator(ctx1, &config)
			require.NoError(t, ar.Start(ctx1), "Error starting replication")
			defer func() { require.NoError(t, ar.Stop(), "Error stopping replication") }()

			// Wait for the original document revision written to rt1 to arrive at rt2.
			rt2RevIDCreated := rt1RevIDCreated
			require.NoError(t, rt2.WaitForCondition(func() bool {
				doc, _ := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
				return doc != nil && doc.HasBody()
			}))
			requireRevID(t, rt2, docID, rt2RevIDCreated)

			// Stop replication.
			require.NoError(t, ar.Stop(), "Error stopping replication")

			// Update the document on rt1 to build a revision history.
			rt1RevIDUpdated := createOrUpdateDoc(t, rt1, docID, rt1RevIDCreated, "bar")

			// Tombstone the document on rt1 to mark the tip of the revision history for deletion.
			resp := rt1.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/"+docID+"?rev="+rt1RevIDUpdated, ``)
			rest.RequireStatus(t, resp, http.StatusOK)

			// Ensure that the tombstone revision is written to rt1 bucket with an empty body.
			waitForTombstone(t, rt1, docID)

			// Update the document on rt2 with the specified body values.
			rt2RevID := rt2RevIDCreated
			for _, bodyValue := range test.remoteBodyValues {
				rt2RevID = createOrUpdateDoc(t, rt2, docID, rt2RevID, bodyValue)
			}

			// Start replication.
			require.NoError(t, ar.Start(ctx1), "Error starting replication")

			// Wait for default conflict resolution policy to be applied through replication and
			// the winning revision to be written to both rt1 and rt2 buckets. Check whether the
			// winning revision is a tombstone; tombstone revision wins over non-tombstone revision.
			waitForTombstone(t, rt2, docID)
			waitForTombstone(t, rt1, docID)

			requireRevID(t, rt2, docID, test.expectedRevID)
			requireRevID(t, rt1, docID, test.expectedRevID)

			// Ensure that the document body of the winning tombstone revision written to both
			// rt1 and rt2 is empty, i.e., An attempt to read the document body of a tombstone
			// revision via SDK should return a "key not found" error.
			requireErrorKeyNotFound(t, rt2, docID)
			requireErrorKeyNotFound(t, rt1, docID)
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
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

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

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			// Build passiveDBURL with basic auth creds
			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)
			passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

			// Active
			rt1 := rest.NewRestTester(t, nil)
			defer rt1.Close()
			ctx1 := rt1.Context()

			defaultConflictResolver, err := db.NewCustomConflictResolver(
				`function(conflict) { return defaultPolicy(conflict); }`, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err, "Error creating custom conflict resolver")
			sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
			require.NoError(t, err)
			dbstats, err := sgwStats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			config := db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				Continuous:           true,
				ConflictResolverFunc: defaultConflictResolver,
				ReplicationStatsMap:  dbstats,
				CollectionsEnabled:   !rt1.GetDatabase().OnlyDefaultCollection(),
			}

			// Create the first revision of the document on rt2.
			docID := test.name + "foo"
			rt2RevIDCreated := createOrUpdateDoc(t, rt2, docID, "", "foo")

			// Create active replicator and start replication.
			ar := db.NewActiveReplicator(ctx1, &config)
			require.NoError(t, ar.Start(ctx1), "Error starting replication")
			defer func() { require.NoError(t, ar.Stop(), "Error stopping replication") }()

			// Wait for the original document revision written to rt2 to arrive at rt1.
			rt1RevIDCreated := rt2RevIDCreated
			require.NoError(t, rt1.WaitForCondition(func() bool {
				doc, _ := rt1.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
				return doc != nil && doc.HasBody()
			}))
			requireRevID(t, rt1, docID, rt1RevIDCreated)

			// Stop replication.
			require.NoError(t, ar.Stop(), "Error stopping replication")

			// Update the document on rt2 to build a revision history.
			rt2RevIDUpdated := createOrUpdateDoc(t, rt2, docID, rt2RevIDCreated, "bar")

			// Tombstone the document on rt2 to mark the tip of the revision history for deletion.
			resp := rt2.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/"+docID+"?rev="+rt2RevIDUpdated, ``)
			rest.RequireStatus(t, resp, http.StatusOK)
			rt2RevID := rest.RespRevID(t, resp)
			log.Printf("rt2RevID: %s", rt2RevID)

			// Ensure that the tombstone revision is written to rt2 bucket with an empty body.
			waitForTombstone(t, rt2, docID)

			// Update the document on rt1 with the specified body values.
			rt1RevID := rt1RevIDCreated
			for _, bodyValue := range test.localBodyValues {
				rt1RevID = createOrUpdateDoc(t, rt1, docID, rt1RevID, bodyValue)
			}

			// Start replication.
			require.NoError(t, ar.Start(ctx1), "Error starting replication")

			// Wait for default conflict resolution policy to be applied through replication and
			// the winning revision to be written to both rt1 and rt2 buckets. Check whether the
			// winning revision is a tombstone; tombstone revision wins over non-tombstone revision.
			waitForTombstone(t, rt1, docID)
			waitForTombstone(t, rt2, docID)

			requireRevID(t, rt1, docID, test.expectedRevID)
			// Wait for conflict resolved doc (tombstone) to be pulled to passive bucket
			// Then require it is the expected rev
			require.NoError(t, rt2.WaitForCondition(func() bool {
				doc, _ := rt2.GetSingleTestDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
				return doc != nil && doc.SyncData.CurrentRev == test.expectedRevID
			}))

			// Ensure that the document body of the winning tombstone revision written to both
			// rt1 and rt2 is empty, i.e., An attempt to read the document body of a tombstone
			// revision via SDK should return a "key not found" error.
			requireErrorKeyNotFound(t, rt2, docID)
			requireErrorKeyNotFound(t, rt1, docID)
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

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			base.RequireNumTestBuckets(t, 2)
			base.SetUpTestLogging(t, base.LevelTrace, base.KeyAll)

			activeRT, remoteRT, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
			defer teardown()

			// Create initial revision(s) on local
			docID := test.name

			var parentRevID, newRevID string
			for gen := 1; gen <= test.initialState.generation; gen++ {
				newRevID = fmt.Sprintf("%d-initial", gen)
				resp := activeRT.PutNewEditsFalse(docID, newRevID, parentRevID,
					makeRevBody(test.initialState.propertyValue, test.initialState.attachmentRevPos, gen))
				log.Printf("-- Added initial revision: %s", resp.Rev)
				parentRevID = newRevID
			}

			// Create replication, wait for initial revision to be replicated
			replicationID := test.name
			activeRT.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePushAndPull, nil, true, db.ConflictResolverLocalWins)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

			assert.NoError(t, remoteRT.WaitForRev(docID, newRevID))

			// Stop the replication
			response := activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
			rest.RequireStatus(t, response, http.StatusOK)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

			rawResponse := activeRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
			log.Printf("-- local raw pre-update: %s", rawResponse.Body.Bytes())
			rawResponse = remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
			log.Printf("-- remote raw pre-update: %s", rawResponse.Body.Bytes())

			// Update local and remote revisions
			localParentRevID := newRevID
			var newLocalRevID string
			for localGen := test.initialState.generation + 1; localGen <= test.localMutation.generation; localGen++ {
				// If deleted=true, tombstone on the last mutation
				if test.localMutation.deleted == true && localGen == test.localMutation.generation {
					activeRT.TombstoneDoc(docID, localParentRevID)
					continue
				}

				newLocalRevID = fmt.Sprintf("%d-local", localGen)
				// Local rev pos is greater of initial state revpos and localMutation rev pos
				localRevPos := test.initialState.attachmentRevPos
				if test.localMutation.attachmentRevPos > 0 {
					localRevPos = test.localMutation.attachmentRevPos
				}
				resp := activeRT.PutNewEditsFalse(docID, newLocalRevID, localParentRevID, makeRevBody(test.localMutation.propertyValue, localRevPos, localGen))
				log.Printf("-- Added local revision: %s", resp.Rev)
				localParentRevID = newLocalRevID
			}

			remoteParentRevID := newRevID
			var newRemoteRevID string
			for remoteGen := test.initialState.generation + 1; remoteGen <= test.remoteMutation.generation; remoteGen++ {
				// If deleted=true, tombstone on the last mutation
				if test.remoteMutation.deleted == true && remoteGen == test.remoteMutation.generation {
					remoteRT.TombstoneDoc(docID, remoteParentRevID)
					continue
				}
				newRemoteRevID = fmt.Sprintf("%d-remote", remoteGen)

				// Local rev pos is greater of initial state revpos and remoteMutation rev pos
				remoteRevPos := test.initialState.attachmentRevPos
				if test.remoteMutation.attachmentRevPos > 0 {
					remoteRevPos = test.remoteMutation.attachmentRevPos
				}
				resp := remoteRT.PutNewEditsFalse(docID, newRemoteRevID, remoteParentRevID, makeRevBody(test.remoteMutation.propertyValue, remoteRevPos, remoteGen))
				log.Printf("-- Added remote revision: %s", resp.Rev)
				remoteParentRevID = newRemoteRevID
			}

			rawResponse = activeRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
			log.Printf("-- local raw pre-replication: %s", rawResponse.Body.Bytes())
			rawResponse = remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
			log.Printf("-- remote raw pre-replication: %s", rawResponse.Body.Bytes())

			// Restart the replication
			response = activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=start", "")
			rest.RequireStatus(t, response, http.StatusOK)

			// Wait for expected property value on remote to determine replication complete
			waitErr := remoteRT.WaitForCondition(func() bool {
				var remoteDoc db.Body
				rawResponse := remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
				require.NoError(t, base.JSONUnmarshal(rawResponse.Body.Bytes(), &remoteDoc))
				prop, ok := remoteDoc["prop"].(string)
				log.Printf("-- Waiting for property: %v, got property: %v", test.expectedResult.propertyValue, prop)
				return ok && prop == test.expectedResult.propertyValue
			})
			require.NoError(t, waitErr)

			localDoc := activeRT.GetDoc(docID)
			localRevID := localDoc.ExtractRev()
			remoteDoc := remoteRT.GetDoc(docID)
			remoteRevID := remoteDoc.ExtractRev()

			assert.Equal(t, localRevID, remoteRevID) // local and remote rev IDs must match
			localGeneration, _ := db.ParseRevID(localRevID)
			assert.Equal(t, test.expectedResult.generation, localGeneration)               // validate expected generation
			assert.Equal(t, test.expectedResult.propertyValue, remoteDoc["prop"].(string)) // validate expected body
			assert.Equal(t, test.expectedResult.propertyValue, localDoc["prop"].(string))  // validate expected body

			remoteRevpos := getTestRevpos(t, remoteDoc, "hello.txt")
			assert.Equal(t, test.expectedResult.attachmentRevPos, remoteRevpos) // validate expected revpos

			rawResponse = activeRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
			log.Printf("-- local raw post-replication: %s", rawResponse.Body.Bytes())

			rawResponse = remoteRT.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docID, "")
			log.Printf("-- remote raw post-replication: %s", rawResponse.Body.Bytes())
		})
	}
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
				AllowConflicts: base.BoolPtr(false),
			}},
		})
	defer rt2.Close()

	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewTLSServer(rt2.TestAdminHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          "test",
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		InsecureSkipVerify:  true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt1.GetDatabase().OnlyDefaultCollection(),
	})

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

	assert.Equal(t, db.ReplicationStateStopped, ar.GetStatus().Status)
	assert.Equal(t, db.PreHydrogenTargetAllowConflictsError.Error(), ar.GetStatus().ErrorMessage)
}
func TestReplicatorConflictAttachment(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	if !base.IsEnterpriseEdition() {
		t.Skipf("requires enterprise edition")
	}

	testCases := []struct {
		name                      string
		conflictResolution        db.ConflictResolverType
		expectedFinalRev          string
		expectedRevPos            int
		expectedAttachmentContent string
	}{
		{
			name:                      "local",
			conflictResolution:        db.ConflictResolverLocalWins,
			expectedFinalRev:          "6-3545745ab68aec5b00e745f9e0e3277c",
			expectedRevPos:            6,
			expectedAttachmentContent: "hello world",
		},
		{
			name:                      "remote",
			conflictResolution:        db.ConflictResolverRemoteWins,
			expectedFinalRev:          "5-remote",
			expectedRevPos:            4,
			expectedAttachmentContent: "goodbye cruel world",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			activeRT, remoteRT, remoteURLString, teardown := rest.SetupSGRPeers(t, true)
			defer teardown()

			docID := test.name

			var parentRevID, newRevID string
			for gen := 1; gen <= 3; gen++ {
				newRevID = fmt.Sprintf("%d-initial", gen)
				resp := activeRT.PutNewEditsFalse(docID, newRevID, parentRevID, "{}")
				parentRevID = newRevID
				assert.True(t, resp.Ok)
			}

			replicationID := "replication"
			activeRT.CreateReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePushAndPull, nil, true, test.conflictResolution)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

			assert.NoError(t, remoteRT.WaitForRev(docID, newRevID))

			response := activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=stop", "")
			rest.RequireStatus(t, response, http.StatusOK)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

			nextGen := 4

			localGen := nextGen
			localParentRevID := newRevID
			newLocalRevID := fmt.Sprintf("%d-local", localGen)
			resp := activeRT.PutNewEditsFalse(docID, newLocalRevID, localParentRevID, `{"_attachments": {"attach": {"data":"aGVsbG8gd29ybGQ="}}}`)
			assert.True(t, resp.Ok)
			localParentRevID = newLocalRevID

			localGen++
			newLocalRevID = fmt.Sprintf("%d-local", localGen)
			resp = activeRT.PutNewEditsFalse(docID, newLocalRevID, localParentRevID, fmt.Sprintf(`{"_attachments": {"attach": {"stub": true, "revpos": %d, "digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`, localGen-1))
			assert.True(t, resp.Ok)

			remoteGen := nextGen
			remoteParentRevID := newRevID
			newRemoteRevID := fmt.Sprintf("%d-remote", remoteGen)
			resp = remoteRT.PutNewEditsFalse(docID, newRemoteRevID, remoteParentRevID, `{"_attachments": {"attach": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`)
			assert.True(t, resp.Ok)
			remoteParentRevID = newRemoteRevID

			remoteGen++
			newRemoteRevID = fmt.Sprintf("%d-remote", remoteGen)
			resp = remoteRT.PutNewEditsFalse(docID, newRemoteRevID, remoteParentRevID, fmt.Sprintf(`{"_attachments": {"attach": {"stub": true, "revpos": %d, "digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`, remoteGen-1))

			response = activeRT.SendAdminRequest("PUT", "/{{.db}}/_replicationStatus/"+replicationID+"?action=start", "")
			rest.RequireStatus(t, response, http.StatusOK)

			waitErr := activeRT.WaitForRev(docID, test.expectedFinalRev)
			assert.NoError(t, waitErr)
			waitErr = remoteRT.WaitForRev(docID, test.expectedFinalRev)
			require.NoError(t, waitErr)

			localDoc := activeRT.GetDoc(docID)
			localRevID := localDoc.ExtractRev()

			remoteDoc := remoteRT.GetDoc(docID)
			remoteRevID := remoteDoc.ExtractRev()

			assert.Equal(t, localRevID, remoteRevID)
			remoteRevpos := getTestRevpos(t, remoteDoc, "attach")
			assert.Equal(t, test.expectedRevPos, remoteRevpos)

			response = activeRT.SendAdminRequest("GET", "/{{.keyspace}}/"+docID+"/attach", "")
			assert.Equal(t, test.expectedAttachmentContent, string(response.BodyBytes()))
		})
	}
}
func TestConflictResolveMergeWithMutatedRev(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	base.RequireNumTestBuckets(t, 2)

	// Passive
	rt2 := rest.NewRestTester(t, nil)
	defer rt2.Close()

	// Active
	rt1 := rest.NewRestTester(t, nil)
	defer rt1.Close()
	ctx1 := rt1.Context()

	srv := httptest.NewServer(rt2.TestAdminHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	customConflictResolver, err := db.NewCustomConflictResolver(`function(conflict){
			var mutatedLocal = conflict.LocalDocument;
			mutatedLocal.source = "merged";
			mutatedLocal["_deleted"] = true;
			mutatedLocal["_rev"] = "";
			return mutatedLocal;
		}`, rt1.GetDatabase().Options.JavascriptTimeout)
	require.NoError(t, err)

	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:             false,
		ReplicationStatsMap:    dbstats,
		ConflictResolutionType: db.ConflictResolverCustom,
		ConflictResolverFunc:   customConflictResolver,
		CollectionsEnabled:     !rt1.GetDatabase().OnlyDefaultCollection(),
	})

	resp := rt2.SendAdminRequest("PUT", "/{{.keyspace}}/doc", "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt2.WaitForPendingChanges())

	resp = rt1.SendAdminRequest("PUT", "/{{.keyspace}}/doc", `{"some_val": "val"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt1.WaitForPendingChanges())

	require.NoError(t, ar.Start(ctx1))

	val, found := base.WaitForStat(func() int64 {
		dbRepStats, err := base.SyncGatewayStats.DbStats[t.Name()].DBReplicatorStats(ar.ID)
		require.NoError(t, err)
		return dbRepStats.PulledCount.Value()
	}, 1)
	assert.True(t, found)
	assert.Equal(t, int64(1), val)

	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)
}

// CBG-1427 - ISGR should not try sending a delta when deltaSrc is a tombstone
func TestReplicatorDoNotSendDeltaWhenSrcIsTombstone(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for delta sync")
	}

	base.RequireNumTestBuckets(t, 2)

	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	// Passive //
	passiveBucket := base.GetTestBucket(t)
	passiveRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			CustomTestBucket: passiveBucket,
			DatabaseConfig: &rest.DatabaseConfig{
				DbConfig: rest.DbConfig{
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.BoolPtr(true),
					},
				},
			},
		})
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator.
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeBucket := base.GetTestBucket(t)
	activeRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			CustomTestBucket: activeBucket,
			DatabaseConfig: &rest.DatabaseConfig{
				DbConfig: rest.DbConfig{
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.BoolPtr(true),
					},
				},
			},
		})
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create a document //
	resp := activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/test", `{"field1":"f1_1","field2":"f2_1"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)
	err := activeRT.WaitForRev("test", revID)
	require.NoError(t, err)

	// Set-up replicator //
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), true, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    1,
		DeltasEnabled:       true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !activeRT.GetDatabase().OnlyDefaultCollection(),
	})
	assert.Equal(t, "", ar.GetStatus().LastSeqPush)
	assert.NoError(t, ar.Start(activeCtx))

	// Wait for active to replicate to passive
	err = passiveRT.WaitForRev("test", revID)
	require.NoError(t, err)

	// Delete active document
	resp = activeRT.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/test?rev="+revID, "")
	rest.RequireStatus(t, resp, http.StatusOK)
	revID = rest.RespRevID(t, resp)

	// Replicate tombstone to passive
	err = passiveRT.WaitForCondition(func() bool {
		rawResponse := passiveRT.SendAdminRequest("GET", "/{{.keyspace}}/test?rev="+revID, "")
		return rawResponse.Code == 404
	})
	require.NoError(t, err)

	// Resurrect tombstoned document
	resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/test?rev="+revID, `{"field2":"f2_2"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID = rest.RespRevID(t, resp)

	// Replicate resurrection to passive
	err = passiveRT.WaitForRev("test", revID)
	assert.NoError(t, err) // If error, problem not fixed

	// Shutdown replicator to close out
	require.NoError(t, ar.Stop())
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
}

// CBG-1672 - Return 422 status for unprocessable deltas instead of 404 to use non-delta retry handling
// Should log "422 Unable to unmarshal mutable body for doc test deltaSrc=1-dbc7919edc9ec2576d527880186f8e8a"
// then fall back to full body replication
func TestUnprocessableDeltas(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for some delta sync")
	}

	base.RequireNumTestBuckets(t, 2)

	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Passive //
	passiveBucket := base.GetTestBucket(t)
	passiveRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			CustomTestBucket: passiveBucket,
			DatabaseConfig: &rest.DatabaseConfig{
				DbConfig: rest.DbConfig{
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.BoolPtr(true),
					},
				},
			},
		})
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator.
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeBucket := base.GetTestBucket(t)
	activeRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			CustomTestBucket: activeBucket,
			DatabaseConfig: &rest.DatabaseConfig{
				DbConfig: rest.DbConfig{
					DeltaSync: &rest.DeltaSyncConfig{
						Enabled: base.BoolPtr(true),
					},
				},
			},
		})
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create a document //
	resp := activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/test", `{"field1":"f1_1","field2":"f2_1"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID := rest.RespRevID(t, resp)
	err := activeRT.WaitForRev("test", revID)
	require.NoError(t, err)

	// Set-up replicator //
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), true, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    200,
		DeltasEnabled:       true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !activeRT.GetDatabase().OnlyDefaultCollection(),
	})
	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	assert.NoError(t, ar.Start(activeCtx))

	err = passiveRT.WaitForRev("test", revID)
	require.NoError(t, err)

	assert.NoError(t, ar.Stop())

	// Make 2nd revision
	resp = activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/test?rev="+revID, `{"field1":"f1_2","field2":"f2_2"}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	revID = rest.RespRevID(t, resp)
	err = activeRT.WaitForPendingChanges()
	require.NoError(t, err)

	rev, err := passiveRT.GetSingleTestDatabaseCollection().GetRevisionCacheForTest().GetActive(base.TestCtx(t), "test")
	require.NoError(t, err)
	// Making body invalid to trigger log "Unable to unmarshal mutable body for doc" in handleRev
	// Which should give a HTTP 422
	rev.SetBodyBytes([]byte("{invalid}"))
	passiveRT.GetSingleTestDatabaseCollection().GetRevisionCacheForTest().Upsert(base.TestCtx(t), rev)

	assert.NoError(t, ar.Start(activeCtx))
	// Check if it replicated
	err = passiveRT.WaitForRev("test", revID)
	assert.NoError(t, err)

	assert.NoError(t, ar.Stop())
}

// CBG-1428 - check for regression of ISGR not ignoring _removed:true bodies when purgeOnRemoval is disabled
func TestReplicatorIgnoreRemovalBodies(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	// Copies the behaviour of TestGetRemovedAsUser but with replication and no user
	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Passive //
	passiveBucket := base.GetTestBucket(t)
	passiveRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			CustomTestBucket: passiveBucket,
		})
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeBucket := base.GetTestBucket(t)
	activeRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{
			CustomTestBucket: activeBucket,
		})
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create the docs //
	// Doc rev 1
	resp := activeRT.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+t.Name(), `{"key":"12","channels": ["rev1chan"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rev1ID := rest.RespRevID(t, resp)
	err := activeRT.WaitForRev(t.Name(), rev1ID)
	require.NoError(t, err)

	// doc rev 2
	resp = activeRT.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", t.Name(), rev1ID), `{"key":"12","channels":["rev2+3chan"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rev2ID := rest.RespRevID(t, resp)
	err = activeRT.WaitForRev(t.Name(), rev2ID)
	require.NoError(t, err)

	// Doc rev 3
	resp = activeRT.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s?rev=%s", t.Name(), rev2ID), `{"key":"3","channels":["rev2+3chan"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rev3ID := rest.RespRevID(t, resp)
	err = activeRT.WaitForRev(t.Name(), rev3ID)
	require.NoError(t, err)

	activeRT.GetSingleTestDatabaseCollection().FlushRevisionCacheForTest()
	err = activeRT.GetSingleDataStore().Delete(fmt.Sprintf("_sync:rev:%s:%d:%s", t.Name(), len(rev2ID), rev2ID))
	require.NoError(t, err)
	// Set-up replicator //
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          false,
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		PurgeOnRemoval:      false,
		Filter:              base.ByChannelFilter,
		FilterChannels:      []string{"rev1chan"},
		CollectionsEnabled:  !activeRT.GetDatabase().OnlyDefaultCollection(),
	})
	docWriteFailuresBefore := ar.GetStatus().DocWriteFailures

	assert.NoError(t, ar.Start(activeCtx))
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	assert.Equal(t, docWriteFailuresBefore, ar.GetStatus().DocWriteFailures, "ISGR should ignore _remove:true bodies when purgeOnRemoval is disabled. CBG-1428 regression.")
}

// CBG-1995: Test the support for using an underscore prefix in the top-level body of a document
// Tests replication and Rest API
func TestUnderscorePrefixSupport(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	// Passive //
	passiveRT := rest.NewRestTester(t, nil)
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeRT := rest.NewRestTester(t, nil)
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create the document
	docID := t.Name()
	rawDoc := `{"_foo": true, "_exp": 120, "true": false, "_attachments": {"bar": {"data": "Zm9vYmFy"}}}`
	_ = activeRT.PutDoc(docID, rawDoc)

	// Set-up replicator
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		PurgeOnRemoval:      false,
		CollectionsEnabled:  !activeRT.GetDatabase().OnlyDefaultCollection(),
	})
	defer func() { require.NoError(t, ar.Stop()) }()

	require.NoError(t, ar.Start(activeCtx))
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	// Confirm document is replicated
	changesResults, err := passiveRT.WaitForChanges(1, "/{{.keyspace}}/_changes?since=0", "", true)
	assert.NoError(t, err)
	assert.Len(t, changesResults.Results, 1)

	err = passiveRT.WaitForPendingChanges()
	require.NoError(t, err)

	require.NoError(t, ar.Stop())

	// Assert document was replicated successfully
	doc := passiveRT.GetDoc(docID)
	assert.EqualValues(t, true, doc["_foo"])  // Confirm user defined value got created
	assert.EqualValues(t, nil, doc["_exp"])   // Confirm expiry was consumed
	assert.EqualValues(t, false, doc["true"]) // Sanity check normal keys
	// Confirm attachment was created successfully
	resp := passiveRT.SendAdminRequest("GET", "/{{.keyspace}}/"+t.Name()+"/bar", "")
	rest.RequireStatus(t, resp, 200)

	// Edit existing document
	rev := doc["_rev"]
	require.NotNil(t, rev)
	rawDoc = fmt.Sprintf(`{"_rev": "%s","_foo": false, "test": true}`, rev)
	_ = activeRT.PutDoc(docID, rawDoc)

	// Replicate modified document
	require.NoError(t, ar.Start(activeCtx))
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	changesResults, err = passiveRT.WaitForChanges(1, fmt.Sprintf("/{{.keyspace}}/_changes?since=%v", changesResults.Last_Seq), "", true)
	assert.NoError(t, err)
	assert.Len(t, changesResults.Results, 1)

	err = passiveRT.WaitForPendingChanges()
	require.NoError(t, err)

	// Verify document replicated successfully
	doc = passiveRT.GetDoc(docID)
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
}

// TestActiveReplicatorBlipsync uses an ActiveReplicator with another RestTester instance to connect and cleanly disconnect.
func TestActiveReplicatorBlipsync(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)

	passiveDBName := "passivedb"
	username := "alice"
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			Name: passiveDBName,
			Users: map[string]*auth.PrincipalConfig{
				username: {Password: base.StringPtr(rest.RestTesterDefaultUserPassword)},
			},
		}},
	})
	defer rt.Close()
	ctx := rt.Context()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/" + passiveDBName)
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)
	stats, err := base.SyncGatewayStats.NewDBStats("test", false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx, &db.ActiveReplicatorConfig{
		ID:                  t.Name(),
		Direction:           db.ActiveReplicatorTypePushAndPull,
		ActiveDB:            &db.Database{DatabaseContext: rt.GetDatabase()},
		RemoteDBURL:         passiveDBURL,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  !rt.GetDatabase().OnlyDefaultCollection(),
	})

	startNumReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	startNumReplicationsActive := rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx))

	// Check total stat
	numReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)

	// Check active stat
	assert.Equal(t, startNumReplicationsActive+2, rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value())

	// Close the replicator (implicit disconnect)
	assert.NoError(t, ar.Stop())

	// Wait for active stat to drop to original value
	numReplicationsActive, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()
	}, startNumReplicationsActive)
	assert.True(t, ok)
	assert.Equal(t, startNumReplicationsActive, numReplicationsActive)

	// Verify total stat has not been decremented
	numReplicationsTotal = rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)
}

func TestBlipSyncNonUpgradableConnection(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
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
	base.RequireNumTestBuckets(t, 2)

	passiveRT := rest.NewRestTester(t,
		&rest.RestTesterConfig{DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Users: map[string]*auth.PrincipalConfig{
					"alice": {
						Password: base.StringPtr("pass"),
					},
				},
			},
		},
		})
	defer passiveRT.Close()

	adminSrv := httptest.NewServer(passiveRT.TestPublicHandler())
	defer adminSrv.Close()

	activeRT := rest.NewRestTester(t, nil) //  CBG-2319: replicator currently requires default collection
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
	require.NoError(t, err)

	rev := activeRT.CreateDoc(t, "test")

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

	err = passiveRT.WaitForRev("test", rev)
	require.NoError(t, err)

	resp = activeRT.SendAdminRequest("GET", "/{{.db}}/_replication/"+t.Name(), "")
	rest.RequireStatus(t, resp, 200)

	var config db.ReplicationConfig
	err = json.Unmarshal(resp.BodyBytes(), &config)
	require.NoError(t, err)
	assert.Equal(t, "alice", config.Username)
	assert.Equal(t, base.RedactedStr, config.Password)
	assert.Equal(t, "", config.RemoteUsername)
	assert.Equal(t, "", config.RemotePassword)

	_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(t.Name(), "stop")
	require.NoError(t, err)
	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)
	err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(t.Name())
	require.NoError(t, err)
}

// CBG-1581: Ensure activeReplicatorCommon does final checkpoint on stop/disconnect
func TestReplicatorCheckpointOnStop(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	passiveRT := rest.NewRestTester(t, nil)
	defer passiveRT.Close()

	adminSrv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer adminSrv.Close()

	activeRT := rest.NewRestTester(t, nil)
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Disable checkpointing at an interval
	activeRT.GetDatabase().SGReplicateMgr.CheckpointInterval = 0
	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
	require.NoError(t, err)

	rev, doc, err := activeRT.GetSingleTestDatabaseCollectionWithUser().Put(activeCtx, "test", db.Body{})
	require.NoError(t, err)
	seq := strconv.FormatUint(doc.Sequence, 10)

	replConfig := `
{
	"replication_id": "` + t.Name() + `",
	"remote": "` + adminSrv.URL + `/db",
	"direction": "push",
	"continuous": true,
	"collections_enabled": ` + strconv.FormatBool(!activeRT.GetDatabase().OnlyDefaultCollection()) + `
}
`
	resp := activeRT.SendAdminRequest("POST", "/{{.db}}/_replication/", replConfig)
	rest.RequireStatus(t, resp, 201)

	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateRunning)

	err = passiveRT.WaitForRev("test", rev)
	require.NoError(t, err)

	_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(t.Name(), "stop")
	require.NoError(t, err)
	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)

	// Check checkpoint document was wrote to bucket with correct status
	// _sync:local:checkpoint/sgr2cp:push:TestReplicatorCheckpointOnStop
	expectedCheckpointName := base.SyncDocPrefix + "local:checkpoint/" + db.PushCheckpointID(t.Name())
	lastSeq, err := activeRT.WaitForCheckpointLastSequence(expectedCheckpointName)
	require.NoError(t, err)
	assert.Equal(t, seq, lastSeq)

	err = activeRT.GetDatabase().SGReplicateMgr.DeleteReplication(t.Name())
	require.NoError(t, err)
}

// Tests replications to make sure they are namespaced by group ID
func TestGroupIDReplications(t *testing.T) {
	if base.UnitTestUrlIsWalrus() || !base.TestUseXattrs() {
		t.Skip("This test only works against Couchbase Server with xattrs enabled")
	}
	base.RequireNumTestBuckets(t, 2)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Create test buckets to replicate between
	activeBucket := base.GetTestBucket(t)
	defer activeBucket.Close()

	// Set up passive bucket RT
	rt := rest.NewRestTester(t, nil)
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive replications
	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Start SG nodes for default group, group A and group B
	groupIDs := []string{"", "GroupA", "GroupB"}
	adminHosts := make([]string, 0, len(groupIDs))
	serverContexts := make([]*rest.ServerContext, 0, len(groupIDs))
	for i, group := range groupIDs {
		serverErr := make(chan error)

		config := rest.BootstrapStartupConfigForTest(t)
		portOffset := i * 10
		adminInterface := fmt.Sprintf("127.0.0.1:%d", 4985+rest.BootstrapTestPortOffset+portOffset)
		adminHosts = append(adminHosts, "http://"+adminInterface)
		config.API.PublicInterface = fmt.Sprintf("127.0.0.1:%d", 4984+rest.BootstrapTestPortOffset+portOffset)
		config.API.AdminInterface = adminInterface
		config.API.MetricsInterface = fmt.Sprintf("127.0.0.1:%d", 4986+rest.BootstrapTestPortOffset+portOffset)
		uniqueUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		config.Bootstrap.ConfigGroupID = group + uniqueUUID.String()

		ctx := base.TestCtx(t)
		sc, err := rest.SetupServerContext(ctx, &config, true)
		require.NoError(t, err)
		serverContexts = append(serverContexts, sc)
		ctx = sc.SetContextLogID(ctx, config.Bootstrap.ConfigGroupID)
		defer func() {
			sc.Close(ctx)
			require.NoError(t, <-serverErr)
		}()
		go func() {
			serverErr <- rest.StartServer(ctx, &config, sc)
		}()
		require.NoError(t, sc.WaitForRESTAPIs())

		dbConfig := rest.DbConfig{
			AutoImport: true,
			BucketConfig: rest.BucketConfig{
				Bucket: base.StringPtr(activeBucket.GetName()),
			},
		}
		if rt.GetDatabase().OnlyDefaultCollection() {
			dbConfig.Sync = base.StringPtr(channels.DocChannelsSyncFunction)
		} else {
			dbConfig.Scopes = rest.GetCollectionsConfigWithSyncFn(rt.TB, rt.TestBucket, base.StringPtr(channels.DocChannelsSyncFunction), 1)
		}
		dbcJSON, err := base.JSONMarshal(dbConfig)
		require.NoError(t, err)

		resp := rest.BootstrapAdminRequestCustomHost(t, http.MethodPut, adminHosts[i], "/db/", string(dbcJSON))
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
			QueryParams:            map[string]interface{}{"channels": channelFilter},
			Continuous:             true,
			InitialState:           db.ReplicationStateRunning,
			ConflictResolutionType: db.ConflictResolverDefault,
			CollectionsEnabled:     !rt.GetDatabase().OnlyDefaultCollection(),
		}
		resp := rest.BootstrapAdminRequestCustomHost(t, http.MethodPost, adminHosts[i], "/db/_replication/", rest.MarshalConfig(t, replicationConfig))
		resp.RequireStatus(http.StatusCreated)
	}

	dataStore := activeBucket.DefaultDataStore()
	keyspace := "/db/"
	if !rt.GetDatabase().OnlyDefaultCollection() {
		dataStore, err = activeBucket.GetNamedDataStore(0)
		require.NoError(t, err)
		dsName, ok := base.AsDataStoreName(dataStore)
		require.True(t, ok)
		keyspace = fmt.Sprintf("/db.%s.%s/", dsName.ScopeName(), dsName.CollectionName())
	}
	for groupNum, group := range groupIDs {
		channel := "chan" + group
		key := "doc" + group
		body := fmt.Sprintf(`{"channels":["%s"]}`, channel)
		added, err := dataStore.Add(key, 0, []byte(body))
		require.NoError(t, err)
		require.True(t, added)

		// Force on-demand import and cache
		for _, host := range adminHosts {
			resp := rest.BootstrapAdminRequestCustomHost(t, http.MethodGet, host, keyspace+key, "")
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
			actualPushed, _ := base.WaitForStat(dbstats.NumDocPushed.Value, expectedPushed)
			assert.Equal(t, expectedPushed, actualPushed)
		}
	}
}

// Reproduces panic seen in CBG-1053
func TestAdhocReplicationStatus(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll, base.KeyReplicate)
	// CBG-2770 does not work with non default collections
	rt := rest.NewRestTesterDefaultCollection(t, &rest.RestTesterConfig{SgReplicateEnabled: true})
	defer rt.Close()

	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()

	replConf := `
	{
	  "replication_id": "pushandpull-with-target-oneshot-adhoc",
	  "remote": "` + srv.URL + `/db",
	  "direction": "pushAndPull",
	  "adhoc": true,
	  "collections_enabled": ` + strconv.FormatBool(!rt.GetDatabase().OnlyDefaultCollection()) + `
	}`

	resp := rt.SendAdminRequest("PUT", "/{{.db}}/_replication/pushandpull-with-target-oneshot-adhoc", replConf)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// With the error hitting the replicationStatus endpoint will either return running, if not completed, and once
	// completed panics. With the fix after running it'll return a 404 as replication no longer exists.
	stateError := rt.WaitForCondition(func() bool {
		resp = rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/pushandpull-with-target-oneshot-adhoc", "")
		return resp.Code == http.StatusNotFound
	})
	assert.NoError(t, stateError)
}

// CBG-1046: Add ability to specify user for active peer in sg-replicate2
func TestSpecifyUserDocsToReplicate(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

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
			passiveRT := rest.NewRestTesterDefaultCollection(t, rtConfig) //  CBG-2772: replicator currently requires default collection

			defer passiveRT.Close()

			publicSrv := httptest.NewServer(passiveRT.TestPublicHandler())
			defer publicSrv.Close()

			adminSrv := httptest.NewServer(passiveRT.TestAdminHandler())
			defer adminSrv.Close()

			activeRT := rest.NewRestTesterDefaultCollection(t, rtConfig) //  CBG-2772: replicator currently requires default collection
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

			err := senderRT.WaitForPendingChanges()
			require.NoError(t, err)

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
			err = activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateRunning)

			value, _ := base.WaitForStat(receiverRT.GetDatabase().DbStats.Database().NumDocWrites.Value, 6)
			assert.EqualValues(t, 6, value)

			changesResults, err := receiverRT.WaitForChanges(6, "/{{.keyspace}}/_changes?since=0&include_docs=true", "", true)
			assert.NoError(t, err)
			assert.Len(t, changesResults.Results, 6)
			// Check the docs are alices docs
			for _, result := range changesResults.Results {
				body, err := result.Doc.MarshalJSON()
				require.NoError(t, err)
				assert.Contains(t, string(body), "alice")
			}

			// Stop and remove replicator (to stop checkpointing after teardown causing panic)
			_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(replName, "stop")
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

			value, _ = base.WaitForStat(receiverRT.GetDatabase().DbStats.Database().NumDocWrites.Value, 10)
			assert.EqualValues(t, 10, value)

			// Stop and remove replicator
			_, err = activeRT.GetDatabase().SGReplicateMgr.PutReplicationStatus(replName, "stop")
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
