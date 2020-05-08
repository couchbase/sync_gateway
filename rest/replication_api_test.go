package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"testing"

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
		Direction: "Pull",
	}

	// PUT replication
	response := rt.SendAdminRequest("PUT", "/db/_replication/replication1", marshalConfig(t, replicationConfig))
	assertStatus(t, response, http.StatusCreated)

	// GET replication for PUT
	response = rt.SendAdminRequest("GET", "/db/_replication/replication1", "")
	assertStatus(t, response, http.StatusOK)
	var configResponse db.ReplicationConfig
	err := json.Unmarshal(response.BodyBytes(), &configResponse)
	require.NoError(t, err)
	assert.Equal(t, configResponse.ID, "replication1")
	assert.Equal(t, configResponse.Remote, "http://remote:4984/db")
	assert.Equal(t, configResponse.Direction, "Pull")

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
	assert.Equal(t, configResponse.ID, "replication2")
	assert.Equal(t, configResponse.Remote, "http://remote:4984/db")
	assert.Equal(t, configResponse.Direction, "Pull")

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
}

func TestValidateReplicationAPI(t *testing.T) {

	var rt = NewRestTester(t, nil)
	defer rt.Close()

	replicationID := "replication1"

	tests := []struct {
		name                  string
		config                db.ReplicationConfig
		expectedResponseCode  int
		expectedErrorContains string
	}{
		{
			name:                  "ID Mismatch",
			config:                db.ReplicationConfig{ID: "replication2"},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "does not match request URI",
		},
		{
			name:                  "Missing Remote",
			config:                db.ReplicationConfig{ID: "replication1"},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "remote must be specified",
		},
		{
			name:                  "Missing Direction",
			config:                db.ReplicationConfig{ID: "replication1", Remote: "http://remote:4985/db"},
			expectedResponseCode:  http.StatusBadRequest,
			expectedErrorContains: "direction must be specified",
		},
		{
			name:                  "Valid Replication",
			config:                db.ReplicationConfig{ID: "replication1", Remote: "http://remote:4985/db", Direction: "pull"},
			expectedResponseCode:  http.StatusCreated,
			expectedErrorContains: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_replication/%s", replicationID), marshalConfig(t, test.config))
			assertStatus(t, response, test.expectedResponseCode)
			if test.expectedErrorContains != "" {
				assert.Contains(t, string(response.Body.Bytes()), test.expectedErrorContains)
			}
		})
	}

}

// TODO: Pending CBG-768, test should be updated to validate response data
func TestReplicationStatusAPI(t *testing.T) {

	var rt = NewRestTester(t, nil)
	defer rt.Close()

	// GET replication status for replication ID
	response := rt.SendAdminRequest("GET", "/db/_replicationStatus/replication1", "")
	assertStatus(t, response, http.StatusOK)
	var statusResponse db.ReplicationStatus
	err := json.Unmarshal(response.BodyBytes(), &statusResponse)
	require.NoError(t, err)
	assert.Equal(t, statusResponse.ID, "replication1")

	// GET replication status for all replications
	response = rt.SendAdminRequest("GET", "/db/_replicationStatus/", "")
	assertStatus(t, response, http.StatusOK)
	var allStatusResponse []*db.ReplicationStatus
	err = json.Unmarshal(response.BodyBytes(), &allStatusResponse)
	require.NoError(t, err)
	assert.Equal(t, allStatusResponse[0].ID, "sampleReplication1")
	assert.Equal(t, allStatusResponse[1].ID, "sampleReplication2")

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
