package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/couchbaselabs/walrus"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicationAPI(t *testing.T) {

	var rt = NewRestTester(t, nil)
	defer rt.Close()

	replicationConfig := db.ReplicationConfig{
		ID:        "replication1",
		Remote:    "http://remote:4984/db",
		Direction: "pull",
		Adhoc:     true,
	}

	// PUT replication
	response := rt.SendAdminRequest("PUT", "/db/_replication/replication1", marshalConfig(t, replicationConfig))
	assertStatus(t, response, http.StatusCreated)

	// GET replication for PUT
	response = rt.SendAdminRequest("GET", "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusOK)
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
	response = rt.SendAdminRequest("POST", "/db/_replication/", marshalConfig(t, replicationConfig))
	assertStatus(t, response, http.StatusCreated)

	// GET replication for POST
	response = rt.SendAdminRequest("GET", "/db/_replication/replication2", "")
	assertStatus(t, response, http.StatusOK)
	configResponse = db.ReplicationConfig{}
	err = json.Unmarshal(response.BodyBytes(), &configResponse)
	require.NoError(t, err)
	assert.Equal(t, "replication2", configResponse.ID)
	assert.Equal(t, "http://remote:4984/db", configResponse.Remote)
	assert.Equal(t, db.ActiveReplicatorTypePull, configResponse.Direction)

	// GET all replications
	response = rt.SendAdminRequest("GET", "/db/_replication/", "")
	assertStatus(t, response, http.StatusOK)
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
	response = rt.SendAdminRequest("DELETE", "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusOK)

	// Verify delete was successful
	response = rt.SendAdminRequest("GET", "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusNotFound)

	// DELETE non-existent replication
	response = rt.SendAdminRequest("DELETE", "/db/_replication/replication3", "")
	assertStatus(t, response, http.StatusNotFound)

}

func TestValidateReplicationAPI(t *testing.T) {

	var rt = NewRestTester(t, nil)
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
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db", Direction: "pull", Adhoc: true, State: "started"},
			expectedResponseCode:  http.StatusCreated,
			expectedErrorContains: "",
		},
		{
			name:                  "Stopped adhoc",
			ID:                    "Stopped_adhoc",
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db", Direction: "pull", Adhoc: true, State: "stopped"},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "state=stopped is not valid for replications specifying adhoc=true",
		},
		{
			name:                  "Stopped non-adhoc",
			ID:                    "Stopped_non_adhoc",
			config:                db.ReplicationConfig{Remote: "http://remote:4985/db", Direction: "pull", State: "stopped"},
			expectedResponseCode:  http.StatusCreated,
			expectedErrorContains: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_replication/%s", test.ID), marshalConfig(t, test.config))
			assertStatus(t, response, test.expectedResponseCode)
			if test.expectedErrorContains != "" {
				assert.Contains(t, string(response.Body.Bytes()), test.expectedErrorContains)
			}
		})
	}

}

func TestReplicationStatusAPI(t *testing.T) {

	var rt = NewRestTester(t, nil)
	defer rt.Close()

	// GET replication status for non-existent replication ID
	response := rt.SendAdminRequest("GET", "/db/_replicationStatus/replication1", "")
	assertStatus(t, response, http.StatusNotFound)

	replicationConfig := db.ReplicationConfig{
		ID:        "replication1",
		Remote:    "http://remote:4984/db",
		Direction: "pull",
	}

	// PUT replication1
	response = rt.SendAdminRequest("PUT", "/db/_replication/replication1", marshalConfig(t, replicationConfig))
	assertStatus(t, response, http.StatusCreated)

	// GET replication status for replication1
	response = rt.SendAdminRequest("GET", "/db/_replicationStatus/replication1", "")
	assertStatus(t, response, http.StatusOK)
	var statusResponse db.ReplicationStatus
	err := json.Unmarshal(response.BodyBytes(), &statusResponse)
	require.NoError(t, err)
	assert.Equal(t, "replication1", statusResponse.ID)
	assert.True(t, statusResponse.Config == nil)

	// PUT replication2
	replication2Config := db.ReplicationConfig{
		ID:        "replication2",
		Remote:    "http://remote:4984/db",
		Direction: "pull",
	}
	response = rt.SendAdminRequest("PUT", "/db/_replication/replication2", marshalConfig(t, replication2Config))
	assertStatus(t, response, http.StatusCreated)

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/db/_replicationStatus/", "")
	assertStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &allStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(allStatusResponse), 2)
	assert.True(t, allStatusResponse[0].Config == nil)
	assert.True(t, allStatusResponse[1].Config == nil)

	// PUT replication status, no action
	response = rt.SendAdminRequest("PUT", "/db/_replicationStatus/replication1", "")
	assertStatus(t, response, http.StatusBadRequest)

	// PUT replication status with action
	response = rt.SendAdminRequest("PUT", "/db/_replicationStatus/replication1?action=start", "")
	assertStatus(t, response, http.StatusOK)
}

func TestReplicationStatusStopAdhoc(t *testing.T) {

	var rt = NewRestTester(t, nil)
	defer rt.Close()

	// GET replication status for non-existent replication ID
	response := rt.SendAdminRequest("GET", "/db/_replicationStatus/replication1", "")
	assertStatus(t, response, http.StatusNotFound)

	permanentReplicationConfig := db.ReplicationConfig{
		ID:         "replication1",
		Remote:     "http://remote:4984/db",
		Direction:  "pull",
		Continuous: true,
	}

	adhocReplicationConfig := db.ReplicationConfig{
		ID:         "replication2",
		Remote:     "http://remote:4984/db",
		Direction:  "pull",
		Continuous: true,
		Adhoc:      true,
	}

	// PUT non-adhoc replication
	response = rt.SendAdminRequest("PUT", "/db/_replication/replication1", marshalConfig(t, permanentReplicationConfig))
	assertStatus(t, response, http.StatusCreated)

	// PUT adhoc replication
	response = rt.SendAdminRequest("PUT", "/db/_replication/replication2", marshalConfig(t, adhocReplicationConfig))
	assertStatus(t, response, http.StatusCreated)

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/db/_replicationStatus/", "")
	assertStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	err := json.Unmarshal(response.BodyBytes(), &allStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(allStatusResponse), 2)
	log.Printf("All status response: %v", allStatusResponse)

	// PUT _replicationStatus to stop non-adhoc replication
	response = rt.SendAdminRequest("PUT", "/db/_replicationStatus/replication1?action=stop", "")
	assertStatus(t, response, http.StatusOK)
	var stopResponse *db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &stopResponse)
	require.NoError(t, err)
	assert.True(t, stopResponse.Status == "stopping" || stopResponse.Status == "stopped")

	// PUT _replicationStatus to stop adhoc replication
	response = rt.SendAdminRequest("PUT", "/db/_replicationStatus/replication2?action=stop", "")
	assertStatus(t, response, http.StatusOK)
	var stopAdhocResponse *db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &stopAdhocResponse)
	require.NoError(t, err)
	assert.True(t, stopAdhocResponse.Status == "removed")

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/db/_replicationStatus/", "")
	assertStatus(t, response, http.StatusOK)
	var updatedStatusResponse []*db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &updatedStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(updatedStatusResponse), 1)
	assert.Equal(t, "replication1", updatedStatusResponse[0].ID)

}

func TestReplicationStatusAPIIncludeConfig(t *testing.T) {

	var rt = NewRestTester(t, nil)
	defer rt.Close()

	// GET replication status for non-existent replication ID
	response := rt.SendAdminRequest("GET", "/db/_replicationStatus/replication1?includeConfig=true", "")
	assertStatus(t, response, http.StatusNotFound)

	replicationConfig := db.ReplicationConfig{
		ID:        "replication1",
		Remote:    "http://remote:4984/db",
		Direction: "pull",
	}

	// PUT replication1
	response = rt.SendAdminRequest("PUT", "/db/_replication/replication1", marshalConfig(t, replicationConfig))
	assertStatus(t, response, http.StatusCreated)

	// GET replication status for replication1
	response = rt.SendAdminRequest("GET", "/db/_replicationStatus/replication1?includeConfig=true", "")
	assertStatus(t, response, http.StatusOK)
	var statusResponse db.ReplicationStatus
	err := json.Unmarshal(response.BodyBytes(), &statusResponse)
	require.NoError(t, err)
	assert.Equal(t, "replication1", statusResponse.ID)
	assert.True(t, statusResponse.Config != nil)

	// PUT replication2
	replication2Config := db.ReplicationConfig{
		ID:        "replication2",
		Remote:    "http://remote:4984/db",
		Direction: "pull",
	}
	response = rt.SendAdminRequest("PUT", "/db/_replication/replication2", marshalConfig(t, replication2Config))
	assertStatus(t, response, http.StatusCreated)

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/db/_replicationStatus/?includeConfig=true", "")
	assertStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &allStatusResponse)
	require.NoError(t, err)
	require.Equal(t, len(allStatusResponse), 2)
	assert.True(t, allStatusResponse[0].Config != nil)
	assert.True(t, allStatusResponse[1].Config != nil)

	// PUT replication status, no action
	response = rt.SendAdminRequest("PUT", "/db/_replicationStatus/replication1", "")
	assertStatus(t, response, http.StatusBadRequest)

	// PUT replication status with action
	response = rt.SendAdminRequest("PUT", "/db/_replicationStatus/replication1?action=start", "")
	assertStatus(t, response, http.StatusOK)

}

func marshalConfig(t *testing.T, config db.ReplicationConfig) string {
	replicationPayload, err := json.Marshal(config)
	require.NoError(t, err)
	return string(replicationPayload)
}

// Upserts replications via config, validates using _replication response
func TestReplicationsFromConfig(t *testing.T) {

	replicationConfig1String := `{
		"replication_id": "replication1",
		"remote": "http://remote:4985/db",
		"direction":"pull",
		"continuous":true,
		"conflict_resolution_type":"foo",
		"conflict_resolution_fn":"func()",
		"purge_on_removal":true,
		"delta_sync_enabled":true,
		"max_backoff":100,
		"state":"stopped",
		"filter":"` + base.ByChannelFilter + `",
		"query_params":["ABC"],
		"cancel":false
	}`
	replicationConfig2String := `{
		"replication_id": "replication2",
		"remote": "http://remote:4985/db",
		"direction":"pull",
		"continuous":true,
		"conflict_resolution_type":"foo",
		"conflict_resolution_fn":"func()",
		"purge_on_removal":true,
		"delta_sync_enabled":true,
		"max_backoff":100,
		"state":"stopped",
		"filter":"` + base.ByChannelFilter + `",
		"query_params":["ABC"],
		"cancel":false
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
			dbConfig := &DbConfig{}
			dbConfig.Replications = make(map[string]*db.ReplicationConfig)
			for _, rc := range test.replicationSet {
				dbConfig.Replications[rc.ID] = rc
			}

			rt := NewRestTester(t, &RestTesterConfig{DatabaseConfig: dbConfig})
			defer rt.Close()

			// Retrieve replications
			response := rt.SendAdminRequest("GET", "/db/_replication/", "")
			assertStatus(t, response, http.StatusOK)
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

// TestPushReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt1.
//   - Creates a continuous push replication on rt1 via the REST API
//   - Validates documents are replicated to rt2
func TestPushReplicationAPI(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	rt1, rt2, remoteURLString, teardown := setupSGRPeers(t)
	defer teardown()

	// Create doc1 on rt1
	docID1 := t.Name() + "rt1doc"
	_ = rt1.putDoc(docID1, `{"source":"rt1","channels":["alice"]}`)

	// Create push replication, verify running
	replicationID := t.Name()
	rt1.createReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePush, nil, true)
	rt1.assertReplicationState(replicationID, db.ReplicationStateRunning)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults := rt2.RequireWaitChanges(1, "0")
	assert.Equal(t, docID1, changesResults.Results[0].ID)

	// Validate doc1 contents on remote
	doc1Body := rt2.getDoc(docID1)
	assert.Equal(t, "rt1", doc1Body["source"])

	// Create doc2 on rt1
	docID2 := t.Name() + "rt1doc2"
	_ = rt2.putDoc(docID2, `{"source":"rt1","channels":["alice"]}`)

	// wait for doc2 to arrive at rt2
	changesResults = rt2.RequireWaitChanges(1, changesResults.Last_Seq.(string))
	assert.Equal(t, docID2, changesResults.Results[0].ID)

	// Validate doc2 contents
	doc2Body := rt2.getDoc(docID2)
	assert.Equal(t, "rt1", doc2Body["source"])
}

// TestPullReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt2.
//   - Creates a continuous pull replication on rt1 via the REST API
//   - Validates documents are replicated to rt1
func TestPullReplicationAPI(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	rt1, rt2, remoteURLString, teardown := setupSGRPeers(t)
	defer teardown()

	// Create doc1 on rt2
	docID1 := t.Name() + "rt2doc"
	_ = rt2.putDoc(docID1, `{"source":"rt2","channels":["alice"]}`)

	// Create pull replication, verify running
	replicationID := t.Name()
	rt1.createReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, true)
	rt1.assertReplicationState(replicationID, db.ReplicationStateRunning)

	// wait for document originally written to rt2 to arrive at rt1
	changesResults := rt1.RequireWaitChanges(1, "0")
	changesResults.requireDocIDs(t, []string{docID1})

	// Validate doc1 contents
	doc1Body := rt1.getDoc(docID1)
	assert.Equal(t, "rt2", doc1Body["source"])

	// Create doc2 on rt2
	docID2 := t.Name() + "rt2doc2"
	_ = rt2.putDoc(docID2, `{"source":"rt2","channels":["alice"]}`)

	// wait for new document to arrive at rt1
	changesResults = rt1.RequireWaitChanges(1, changesResults.Last_Seq.(string))
	changesResults.requireDocIDs(t, []string{docID2})

	// Validate doc2 contents
	doc2Body := rt1.getDoc(docID2)
	assert.Equal(t, "rt2", doc2Body["source"])
}

// TestReplicationRebalancePull
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt1 in two channels
//   - Creates two continuous pull replications on rt1 via the REST API
//   - adds another active node
//   - Creates more documents, validates they are replicated
func TestReplicationRebalancePull(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	oldFrequency := db.MaxSequenceIncrFrequency
	defer func() { db.MaxSequenceIncrFrequency = oldFrequency }()
	db.MaxSequenceIncrFrequency = 0 * time.Millisecond

	activeRT, remoteRT, remoteURLString, teardown := setupSGRPeers(t)
	defer teardown()

	// Create docs on remote
	docABC1 := t.Name() + "ABC1"
	docDEF1 := t.Name() + "DEF1"
	_ = remoteRT.putDoc(docABC1, `{"source":"remoteRT","channels":["ABC"]}`)
	_ = remoteRT.putDoc(docDEF1, `{"source":"remoteRT","channels":["DEF"]}`)

	// Create pull replications, verify running
	activeRT.createReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePull, []string{"ABC"}, true)
	activeRT.assertReplicationState("rep_ABC", db.ReplicationStateRunning)
	activeRT.createReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePull, []string{"DEF"}, true)
	activeRT.assertReplicationState("rep_DEF", db.ReplicationStateRunning)

	// wait for documents originally written to remoteRT to arrive at activeRT
	changesResults := activeRT.RequireWaitChanges(2, "0")
	changesResults.requireDocIDs(t, []string{docABC1, docDEF1})

	// Validate doc contents
	docABC1Body := activeRT.getDoc(docABC1)
	assert.Equal(t, "remoteRT", docABC1Body["source"])
	docDEF1Body := activeRT.getDoc(docDEF1)
	assert.Equal(t, "remoteRT", docDEF1Body["source"])

	// Add another node to the active cluster
	activeRT2 := addActiveRT(t, activeRT.TestBucket)
	defer activeRT2.Close()

	// Wait for replication to be rebalanced to activeRT2
	activeRT.waitForAssignedReplications(1)
	activeRT2.waitForAssignedReplications(1)

	// Create additional docs on remoteRT
	docABC2 := t.Name() + "ABC2"
	_ = remoteRT.putDoc(docABC2, `{"source":"remoteRT","channels":["ABC"]}`)
	docDEF2 := t.Name() + "DEF2"
	_ = remoteRT.putDoc(docDEF2, `{"source":"remoteRT","channels":["DEF"]}`)

	// wait for new documents to arrive at activeRT
	changesResults = activeRT.RequireWaitChanges(2, changesResults.Last_Seq.(string))
	changesResults.requireDocIDs(t, []string{docABC2, docDEF2})

	// Validate doc contents
	docABC2Body := activeRT.getDoc(docABC2)
	assert.Equal(t, "remoteRT", docABC2Body["source"])
	docDEF2Body := activeRT.getDoc(docDEF2)
	assert.Equal(t, "remoteRT", docDEF2Body["source"])
	docABC2Body2 := activeRT2.getDoc(docABC2)
	assert.Equal(t, "remoteRT", docABC2Body2["source"])
	docDEF2Body2 := activeRT2.getDoc(docDEF2)
	assert.Equal(t, "remoteRT", docDEF2Body2["source"])
}

// TestReplicationRebalancePush
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt1 in two channels
//   - Creates two continuous pull replications on rt1 via the REST API
//   - adds another active node
//   - Creates more documents, validates they are replicated
func TestReplicationRebalancePush(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	// Disable sequence batching for multi-RT tests (pending CBG-1000)
	oldFrequency := db.MaxSequenceIncrFrequency
	defer func() { db.MaxSequenceIncrFrequency = oldFrequency }()
	db.MaxSequenceIncrFrequency = 0 * time.Millisecond

	activeRT, remoteRT, remoteURLString, teardown := setupSGRPeers(t)
	defer teardown()

	// Create docs on active
	docABC1 := t.Name() + "ABC1"
	docDEF1 := t.Name() + "DEF1"
	_ = activeRT.putDoc(docABC1, `{"source":"activeRT","channels":["ABC"]}`)
	_ = activeRT.putDoc(docDEF1, `{"source":"activeRT","channels":["DEF"]}`)

	// Create push replications, verify running
	activeRT.createReplication("rep_ABC", remoteURLString, db.ActiveReplicatorTypePush, []string{"ABC"}, true)
	activeRT.assertReplicationState("rep_ABC", db.ReplicationStateRunning)
	activeRT.createReplication("rep_DEF", remoteURLString, db.ActiveReplicatorTypePush, []string{"DEF"}, true)
	activeRT.assertReplicationState("rep_DEF", db.ReplicationStateRunning)

	// wait for documents to be pushed to remote
	changesResults := remoteRT.RequireWaitChanges(2, "0")
	changesResults.requireDocIDs(t, []string{docABC1, docDEF1})

	// Validate doc contents
	docABC1Body := remoteRT.getDoc(docABC1)
	assert.Equal(t, "activeRT", docABC1Body["source"])
	docDEF1Body := remoteRT.getDoc(docDEF1)
	assert.Equal(t, "activeRT", docDEF1Body["source"])

	// Add another node to the active cluster
	activeRT2 := addActiveRT(t, activeRT.TestBucket)
	defer activeRT2.Close()

	// Wait for replication to be rebalanced to activeRT2
	activeRT.waitForAssignedReplications(1)
	activeRT2.waitForAssignedReplications(1)

	// Create additional docs on local
	docABC2 := t.Name() + "ABC2"
	_ = activeRT.putDoc(docABC2, `{"source":"activeRT","channels":["ABC"]}`)
	docDEF2 := t.Name() + "DEF2"
	_ = activeRT.putDoc(docDEF2, `{"source":"activeRT","channels":["DEF"]}`)

	// wait for new documents to arrive at remote
	changesResults = remoteRT.RequireWaitChanges(2, changesResults.Last_Seq.(string))
	changesResults.requireDocIDs(t, []string{docABC2, docDEF2})

	// Validate doc contents
	docABC2Body := remoteRT.getDoc(docABC2)
	assert.Equal(t, "activeRT", docABC2Body["source"])
	docDEF2Body := remoteRT.getDoc(docDEF2)
	assert.Equal(t, "activeRT", docDEF2Body["source"])
}

// TestPullReplicationAPI
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates documents on rt2.
//   - Creates a one-shot pull replication on rt1 via the REST API
//   - Validates documents are replicated to rt1
//   - Validates replication status count when replication is local and non-local
func TestPullOneshotReplicationAPI(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	activeRT, remoteRT, remoteURLString, teardown := setupSGRPeers(t)
	defer teardown()

	// Create 20 docs on rt2
	docCount := 20
	docIDs := make([]string, 20)
	for i := 0; i < 20; i++ {
		docID := fmt.Sprintf("%s%s%d", t.Name(), "rt2doc", i)
		_ = remoteRT.putDoc(docID, `{"source":"rt2","channels":["alice"]}`)
		docIDs[i] = docID
	}

	// Create oneshot replication, verify running
	replicationID := t.Name()
	activeRT.createReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePull, nil, false)
	activeRT.assertReplicationState(replicationID, db.ReplicationStateRunning)

	// wait for documents originally written to rt2 to arrive at rt1
	changesResults := activeRT.RequireWaitChanges(docCount, "0")
	changesResults.requireDocIDs(t, docIDs)

	// Validate sample doc contents
	doc1Body := activeRT.getDoc(docIDs[0])
	assert.Equal(t, "rt2", doc1Body["source"])

	// Get replication status from active
	status := activeRT.GetReplicationStatus(replicationID)
	assert.Equal(t, int64(docCount), status.DocsRead)

	// Add another node to the active cluster
	activeRT2 := addActiveRT(t, activeRT.TestBucket)
	defer activeRT2.Close()

	// Get replication status for non-local replication
	remoteStatus := activeRT2.GetReplicationStatus(replicationID)
	assert.Equal(t, int64(docCount), remoteStatus.DocsRead)

}

// Helper functions for SGR testing

// setupSGRPeers sets up two rest testers to be used for sg-replicate testing with the following configuration:
//   activeRT:
//     - backed by test bucket
//     - SGReplicationMgr.StartReplications() has been called
//   passiveRT:
//     - backed by different test bucket
//     - user 'alice' created with star channel access
//     - http server wrapping the public API, remoteDBURLString targets the rt2 database as user alice (e.g. http://alice:pass@host/db)
//   returned teardown function closes activeRT, passiveRT and the http server, should be invoked with defer
func setupSGRPeers(t *testing.T) (activeRT *RestTester, passiveRT *RestTester, remoteDBURLString string, teardown func()) {
	// Set up passive RestTester (rt2)
	passiveRT = NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("*"),
				},
			},
		},
		noAdminParty: true,
	})

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(passiveRT.TestPublicHandler())

	// Build passiveDBURL with basic auth creds
	passiveDBURL, _ := url.Parse(srv.URL + "/db")
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Set up active RestTester (rt1)
	activeRT = NewRestTester(t, &RestTesterConfig{
		TestBucket:         base.GetTestBucket(t),
		sgReplicateEnabled: true,
	})

	// Start replication manager on rt1
	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications()
	require.NoError(t, err)

	teardown = func() {
		activeRT.Close()
		srv.Close()
		passiveRT.Close()
	}
	return activeRT, passiveRT, passiveDBURL.String(), teardown
}

// AddActiveRT returns a new RestTester backed by a no-close clone of TestBucket
func addActiveRT(t *testing.T, testBucket *base.TestBucket) (activeRT *RestTester) {

	// Create a new rest tester, using a NoCloseClone of testBucket, which disables the TestBucketPool teardown
	activeRT = NewRestTester(t, &RestTesterConfig{
		TestBucket:         testBucket.NoCloseClone(),
		sgReplicateEnabled: true,
	})

	// If this is a walrus bucket, we need to jump through some hoops to ensure the shared in-memory walrus bucket isn't
	// deleted when bucket.Close() is called during DatabaseContext.Close().
	// Using IgnoreClose in leakyBucket to no-op the close operation.
	// Because RestTester has Sync Gateway create the database context and bucket based on the bucketSpec, we can't
	// set up the leakyBucket wrapper prior to bucket creation.
	// Instead, we need to modify the leaky bucket config (created for vbno handling) after the fact.
	leakyBucket, ok := activeRT.GetDatabase().Bucket.(*base.LeakyBucket)
	if ok {
		underlyingBucket := leakyBucket.GetUnderlyingBucket()
		if _, ok := underlyingBucket.(*walrus.WalrusBucket); ok {
			leakyBucket.SetIgnoreClose(true)
		}
	}

	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications()
	require.NoError(t, err)
	return activeRT
}

// assertReplicationState retrieves _replicationStatus via the REST API for the specified replicationID, and
// validates the expected state
func (rt *RestTester) assertReplicationState(replicationID string, expectedState string) {
	resp := rt.SendAdminRequest(http.MethodGet, "/db/_replicationStatus/"+replicationID, "")
	assertStatus(rt.tb, resp, http.StatusOK)
	var status db.ReplicationStatus
	require.NoError(rt.tb, json.Unmarshal(resp.Body.Bytes(), &status))
	assert.Equal(rt.tb, expectedState, status.Status)
}

// createReplication creates a replication via the REST API with the specified ID, remoteURL, direction and channel filter
func (rt *RestTester) createReplication(replicationID string, remoteURLString string,
	direction db.ActiveReplicatorDirection, channels []string, continuous bool) {
	replicationConfig := &db.ReplicationConfig{
		ID:         replicationID,
		Direction:  direction,
		Remote:     remoteURLString,
		Continuous: continuous,
	}
	if len(channels) > 0 {
		replicationConfig.Filter = base.ByChannelFilter
		replicationConfig.QueryParams = map[string]interface{}{"channels": channels}
	}
	payload, err := json.Marshal(replicationConfig)
	require.NoError(rt.tb, err)
	resp := rt.SendAdminRequest(http.MethodPost, "/db/_replication/", string(payload))
	assertStatus(rt.tb, resp, http.StatusCreated)
}

func (rt *RestTester) waitForAssignedReplications(count int) {
	successFunc := func() bool {
		replicationStatuses := rt.GetReplicationStatuses("?localOnly=true")
		return len(replicationStatuses) == count
	}
	require.NoError(rt.tb, rt.WaitForCondition(successFunc))
}

func (rt *RestTester) GetReplications() (replications map[string]db.ReplicationCfg) {
	rawResponse := rt.SendAdminRequest("GET", "/db/_replication/", "")
	assertStatus(rt.tb, rawResponse, 200)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &replications))
	return replications
}

func (rt *RestTester) GetReplicationStatus(replicationID string) (status db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/db/_replicationStatus/"+replicationID, "")
	assertStatus(rt.tb, rawResponse, 200)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &status))
	return status
}

func (rt *RestTester) GetReplicationStatuses(queryString string) (statuses []db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/db/_replicationStatus/"+queryString, "")
	assertStatus(rt.tb, rawResponse, 200)
	require.NoError(rt.tb, base.JSONUnmarshal(rawResponse.Body.Bytes(), &statuses))
	return statuses
}

func TestReplicationAPIWithAuthCredentials(t *testing.T) {
	var rt = NewRestTester(t, nil)
	defer rt.Close()

	// Create replication with explicitly defined auth credentials in replication config
	replication1Config := db.ReplicationConfig{
		ID:        "replication1",
		Remote:    "http://remote:4984/db",
		Username:  "alice",
		Password:  "pass",
		Direction: db.ActiveReplicatorTypePull,
		Adhoc:     true,
	}
	response := rt.SendAdminRequest(http.MethodPut, "/db/_replication/replication1", marshalConfig(t, replication1Config))
	assertStatus(t, response, http.StatusCreated)

	// Check whether auth are credentials redacted from replication response
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusOK)
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
		assert.Equal(t, expected.Username, actual.Username, "Couldn't redact username")
		assert.Equal(t, expected.Password, actual.Password, "Couldn't redact password")
	}
	replication1Config.Username = "****"
	replication1Config.Password = "****"
	checkReplicationConfig(&replication1Config, &configResponse)

	// Create another replication with auth credentials defined in Remote URL
	replication2Config := db.ReplicationConfig{
		ID:        "replication2",
		Remote:    "http://bob:pass@remote:4984/db",
		Direction: db.ActiveReplicatorTypePull,
		Adhoc:     true,
	}
	response = rt.SendAdminRequest(http.MethodPost, "/db/_replication/", marshalConfig(t, replication2Config))
	assertStatus(t, response, http.StatusCreated)

	// Check whether auth are credentials redacted from replication response
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replication/replication2", "")
	assertStatus(t, response, http.StatusOK)
	configResponse = db.ReplicationConfig{}
	err = json.Unmarshal(response.BodyBytes(), &configResponse)
	require.NoError(t, err, "Error un-marshalling replication response")
	replication2Config.Remote = "http://****:****@remote:4984/db"
	//checkReplicationConfig(&replication2Config, &configResponse)

	// Check whether auth are credentials redacted from all replications response
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replication/", "")
	assertStatus(t, response, http.StatusOK)
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
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replicationStatus/?includeConfig=true", "")
	assertStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	require.NoError(t, json.Unmarshal(response.BodyBytes(), &allStatusResponse))
	require.Equal(t, 2, len(allStatusResponse), "Replication count mismatch")

	// Sort replications by replication ID before assertion
	sort.Slice(allStatusResponse[:], func(i, j int) bool {
		return allStatusResponse[i].Config.ID < allStatusResponse[j].Config.ID
	})
	checkReplicationConfig(&replication1Config, allStatusResponse[0].Config)
	checkReplicationConfig(&replication2Config, allStatusResponse[1].Config)

	// Delete both replications
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_replication/replication2", "")
	assertStatus(t, response, http.StatusOK)

	// Verify deletes were successful
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusNotFound)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replication/replication2", "")
	assertStatus(t, response, http.StatusNotFound)
}

func TestValidateReplication(t *testing.T) {
	testCases := []struct {
		name              string
		replicationConfig db.ReplicationConfig
		fromConfig        bool
		errExpected       error
	}{
		{
			name: "replication config unsupported Cancel option",
			replicationConfig: db.ReplicationConfig{
				Cancel: true,
			},
			fromConfig: true,
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "cancel=true is invalid for replication in Sync Gateway configuration",
			},
		},
		{
			name: "replication config unsupported Adhoc option",
			replicationConfig: db.ReplicationConfig{
				Adhoc: true,
			},
			fromConfig: true,
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "adhoc=true is invalid for replication in Sync Gateway configuration",
			},
		},
		{
			name: "replication config with no remote URL specified",
			replicationConfig: db.ReplicationConfig{
				Remote: "",
			},
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "Replication remote must be specified.",
			},
		},
		{
			name: "auth credentials specified in both replication config and remote URL",
			replicationConfig: db.ReplicationConfig{
				Remote:   "http://bob:pass@remote:4984/db",
				Username: "alice",
				Password: "pass",
			},
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "Auth credentials can be specified either in replication config or remote URL but not allowed in both",
			},
		},
		{
			name: "auth credentials specified in replication config",
			replicationConfig: db.ReplicationConfig{
				Remote:      "http://remote:4984/db",
				Username:    "alice",
				Password:    "pass",
				Filter:      base.ByChannelFilter,
				QueryParams: map[string]interface{}{"channels": []interface{}{"E", "A", "D", "G", "B", "e"}},
				Direction:   db.ActiveReplicatorTypePull,
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
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "Replication direction must be specified",
			},
		},
		{
			name: "replication config with invalid direction",
			replicationConfig: db.ReplicationConfig{
				Remote:    "http://bob:pass@remote:4984/db",
				Direction: "UpAndDown",
			},
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "Invalid replication direction \"UpAndDown\", valid values are push/pull/pushAndPull",
			},
		},
		{
			name: "replication config with unknown filter",
			replicationConfig: db.ReplicationConfig{
				Remote:      "http://bob:pass@remote:4984/db",
				QueryParams: map[string]interface{}{"channels": []interface{}{"E", "A", "D", "G", "B", "e"}},
				Direction:   db.ActiveReplicatorTypePull,
				Filter:      "unknownFilter",
			},
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "Unknown replication filter; try sync_gateway/bychannel",
			},
		},
		{
			name: "replication config with channel filter but no query params",
			replicationConfig: db.ReplicationConfig{
				Remote:    "http://bob:pass@remote:4984/db",
				Filter:    base.ByChannelFilter,
				Direction: db.ActiveReplicatorTypePull,
			},
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "Replication specifies sync_gateway/bychannel filter but is missing query_params",
			},
		},
		{
			name: "replication config with channel filter and invalid query params",
			replicationConfig: db.ReplicationConfig{
				Remote:      "http://bob:pass@remote:4984/db",
				Filter:      base.ByChannelFilter,
				QueryParams: []string{"E", "A", "D", "G", "B", "e"},
				Direction:   db.ActiveReplicatorTypePull,
			},
			errExpected: &base.HTTPError{
				Status:  http.StatusBadRequest,
				Message: "Bad channels array in query_params for sync_gateway/bychannel filter",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.replicationConfig.ValidateReplication(tc.fromConfig)
			assert.Equal(t, tc.errExpected, err)
		})
	}

}

func TestValidateReplicationWithInvalidURL(t *testing.T) {
	// Replication config with credentials in an invalid remote URL
	replicationConfig := db.ReplicationConfig{Remote: "http://user:foo{bar=pass@remote:4984/db"}
	err := replicationConfig.ValidateReplication(false)
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
	assert.Contains(t, err.Error(), "http://****:****@remote:4984/db")
	assert.NotContains(t, err.Error(), "user:foo{bar=pass")

	// Replication config with no credentials in an invalid remote URL
	replicationConfig = db.ReplicationConfig{Remote: "http://{unknown@remote:4984/db"}
	err = replicationConfig.ValidateReplication(false)
	assert.Contains(t, err.Error(), strconv.Itoa(http.StatusBadRequest))
	assert.Contains(t, err.Error(), "http://{unknown@remote:4984/db")
}

func TestGetStatusWithReplication(t *testing.T) {
	var rt = NewRestTester(t, nil)
	defer rt.Close()

	// Create a replication
	config1 := db.ReplicationConfig{
		ID:        "replication1",
		Remote:    "http://remote:4984/db",
		Username:  "alice",
		Password:  "pass",
		Direction: db.ActiveReplicatorTypePull,
		Adhoc:     true,
	}
	response := rt.SendAdminRequest(http.MethodPut, "/db/_replication/replication1", marshalConfig(t, config1))
	assertStatus(t, response, http.StatusCreated)

	// Create another replication
	config2 := db.ReplicationConfig{
		ID:        "replication2",
		Remote:    "http://bob:pass@remote:4984/db",
		Direction: db.ActiveReplicatorTypePull,
		Adhoc:     true,
	}
	response = rt.SendAdminRequest(http.MethodPut, "/db/_replication/replication2", marshalConfig(t, config2))
	assertStatus(t, response, http.StatusCreated)

	// Check _status response
	response = rt.SendAdminRequest(http.MethodGet, "/_status", "")
	assertStatus(t, response, http.StatusOK)
	var status Status
	err := json.Unmarshal(response.BodyBytes(), &status)
	require.NoError(t, err, "Error un-marshalling replication response")
	database := status.Databases["db"]
	require.Equal(t, 2, len(database.ReplicationStatus), "Replication count mismatch")

	// Sort replications by replication ID before asserting replication status
	sort.Slice(database.ReplicationStatus[:], func(i, j int) bool {
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
		assert.Equal(t, expected.Username, actual.Username)
		assert.Equal(t, expected.Password, actual.Password)
		assert.Equal(t, expected.Direction, actual.Direction)
		assert.Equal(t, expected.Adhoc, actual.Adhoc)
	}

	// Check replication1 details in cluster response
	repl, ok := database.SGRCluster.Replications[config1.ID]
	assert.True(t, ok, "Error getting replication")
	config1.Username = "****"
	config1.Password = "****"
	assertReplication(config1, repl)

	// Check replication2 details in cluster response
	repl, ok = database.SGRCluster.Replications[config2.ID]
	assert.True(t, ok, "Error getting replication")
	config2.Remote = "http://****:****@remote:4984/db"
	assertReplication(config2, repl)

	// Delete both replications
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusOK)
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_replication/replication2", "")
	assertStatus(t, response, http.StatusOK)

	// Verify deletes were successful
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusNotFound)
	response = rt.SendAdminRequest(http.MethodGet, "/db/_replication/replication2", "")
	assertStatus(t, response, http.StatusNotFound)

	// Check _cluster response after replications are removed
	status = Status{}
	err = json.Unmarshal(response.BodyBytes(), &status)
	require.NoError(t, err, "Error un-marshalling replication response")
	require.Equal(t, 0, len(status.Databases["db"].ReplicationStatus))
}
