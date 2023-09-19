// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type PutDocResponse struct {
	ID  string
	Ok  bool
	Rev string
}

// Run is equivalent to testing.T.Run() but updates the RestTester's TB to the new testing.T
// so that checks are made against the right instance (otherwise the outer test complains
// "subtest may have called FailNow on a parent test")
func (rt *RestTester) Run(name string, test func(*testing.T)) {
	mainT := rt.TB.(*testing.T)
	mainT.Run(name, func(t *testing.T) {
		rt.TB = t
		defer func() { rt.TB = mainT }()
		test(t)
	})
}

func (rt *RestTester) GetDoc(docID string) (body db.Body) {
	rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
	return body
}

func (rt *RestTester) CreateDoc(t *testing.T, docid string) string {
	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), docid), `{"prop":true}`)
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
	rawResponse := rt.SendAdminRequest("PUT", fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), docID), body)
	RequireStatus(rt.TB, rawResponse, 201)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.TB, response.Ok)
	require.NotEmpty(rt.TB, response.Rev)
	return response
}

func (rt *RestTester) UpdateDoc(docID, revID, body string) (response PutDocResponse) {
	resource := fmt.Sprintf("/%s/%s?rev=%s", rt.GetSingleKeyspace(), docID, revID)
	rawResponse := rt.SendAdminRequest(http.MethodPut, resource, body)
	RequireStatus(rt.TB, rawResponse, http.StatusCreated)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.TB, response.Ok)
	require.NotEmpty(rt.TB, response.Rev)
	return response
}

func (rt *RestTester) upsertDoc(docID string, body string) (response PutDocResponse) {

	getResponse := rt.SendAdminRequest("GET", "/{{.db}}/"+docID, "")
	if getResponse.Code == 404 {
		return rt.PutDoc(docID, body)
	}
	var getBody db.Body
	require.NoError(rt.TB, base.JSONUnmarshal(getResponse.Body.Bytes(), &getBody))
	revID, ok := getBody["revID"].(string)
	require.True(rt.TB, ok)

	rawResponse := rt.SendAdminRequest("PUT", "/{{.db}}/"+docID+"?rev="+revID, body)
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &response))
	require.True(rt.TB, response.Ok)
	require.NotEmpty(rt.TB, response.Rev)
	return response
}

func (rt *RestTester) DeleteDoc(docID, revID string) {
	RequireStatus(rt.TB, rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/%s/%s?rev=%s", rt.GetSingleKeyspace(), docID, revID), ""), http.StatusOK)
}

func (rt *RestTester) WaitForRev(docID string, revID string) error {
	return rt.WaitForCondition(func() bool {
		rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
		if rawResponse.Code != 200 && rawResponse.Code != 201 {
			return false
		}
		var body db.Body
		require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
		return body.ExtractRev() == revID
	})
}

func (rt *RestTester) WaitForCheckpointLastSequence(expectedName string) (string, error) {
	var lastSeq string
	successFunc := func() bool {
		val, _, err := rt.GetSingleDataStore().GetRaw(expectedName)
		if err != nil {
			rt.TB.Logf("Error getting checkpoint: %v - will retry", err)
			return false
		}
		var config struct { // db.replicationCheckpoint
			LastSeq string `json:"last_sequence"`
		}
		err = json.Unmarshal(val, &config)
		if err != nil {
			rt.TB.Logf("Error unmarshalling checkpoint: %v - will retry", err)
			return false
		}
		lastSeq = config.LastSeq
		return lastSeq != ""
	}
	return lastSeq, rt.WaitForCondition(successFunc)
}

func (rt *RestTester) WaitForActiveReplicatorInitialization(count int) {
	successFunc := func() bool {
		ar := rt.GetDatabase().SGReplicateMgr.GetNumberActiveReplicators()
		return ar == count
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc), "mismatch on number of active replicators")
}

func (rt *RestTester) WaitForPullBlipSenderInitialisation(name string) {
	successFunc := func() bool {
		bs := rt.GetDatabase().SGReplicateMgr.GetActiveReplicator(name).Pull.GetBlipSender()
		return bs != nil
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc), "blip sender on active replicator not initialized")
}

// createReplication creates a replication via the REST API with the specified ID, remoteURL, direction and channel filter
func (rt *RestTester) CreateReplication(replicationID string, remoteURLString string, direction db.ActiveReplicatorDirection, channels []string, continuous bool, conflictResolver db.ConflictResolverType) {
	rt.CreateReplicationForDB("{{.db}}", replicationID, remoteURLString, direction, channels, continuous, conflictResolver)
}

func (rt *RestTester) CreateReplicationForDB(dbName string, replicationID string, remoteURLString string, direction db.ActiveReplicatorDirection, channels []string, continuous bool, conflictResolver db.ConflictResolverType) {
	replicationConfig := &db.ReplicationConfig{
		ID:                     replicationID,
		Direction:              direction,
		Remote:                 remoteURLString,
		Continuous:             continuous,
		ConflictResolutionType: conflictResolver,
		CollectionsEnabled:     base.TestsUseNamedCollections(),
	}

	if len(channels) > 0 {
		replicationConfig.Filter = base.ByChannelFilter
		replicationConfig.QueryParams = map[string]interface{}{"channels": channels}
	}
	payload, err := json.Marshal(replicationConfig)
	require.NoError(rt.TB, err)
	resp := rt.SendAdminRequest(http.MethodPost, "/"+dbName+"/_replication/", string(payload))
	RequireStatus(rt.TB, resp, http.StatusCreated)
}

func (rt *RestTester) WaitForAssignedReplications(count int) {
	successFunc := func() bool {
		replicationStatuses := rt.GetReplicationStatuses("?localOnly=true")
		return len(replicationStatuses) == count
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc))
}

func (rt *RestTester) GetActiveReplicatorCount() int {
	rt.ServerContext().ActiveReplicationsCounter.lock.Lock()
	defer rt.ServerContext().ActiveReplicationsCounter.lock.Unlock()
	return rt.ServerContext().ActiveReplicationsCounter.activeReplicatorCount
}

func (rt *RestTester) WaitForActiveReplicatorCount(expCount int) {
	var count int
	successFunc := func() bool {
		count = rt.GetActiveReplicatorCount()
		return count == expCount
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc), "Mismatch in active replicator count, expected count %d actual %d", expCount, count)
}

func (rt *RestTester) WaitForReplicationStatusForDB(dbName string, replicationID string, targetStatus string) {
	var status db.ReplicationStatus
	successFunc := func() bool {
		status = rt.GetReplicationStatusForDB(dbName, replicationID)
		return status.Status == targetStatus
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc), "Expected status: %s, actual status: %s", targetStatus, status.Status)
}

func (rt *RestTester) WaitForReplicationStatus(replicationID string, targetStatus string) {
	rt.WaitForReplicationStatusForDB("{{.db}}", replicationID, targetStatus)
}

func (rt *RestTester) GetReplications() (replications map[string]db.ReplicationCfg) {
	rawResponse := rt.SendAdminRequest("GET", "/{{.db}}/_replication/", "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &replications))
	return replications
}

func (rt *RestTester) GetReplicationStatus(replicationID string) (status db.ReplicationStatus) {
	return rt.GetReplicationStatusForDB("{{.db}}", replicationID)
}

func (rt *RestTester) GetReplicationStatusForDB(dbName string, replicationID string) (status db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/"+dbName+"/_replicationStatus/"+replicationID, "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &status))
	return status
}

func (rt *RestTester) GetReplicationStatuses(queryString string) (statuses []db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/"+queryString, "")
	RequireStatus(rt.TB, rawResponse, 200)
	require.NoError(rt.TB, base.JSONUnmarshal(rawResponse.Body.Bytes(), &statuses))
	return statuses
}

func (rt *RestTester) WaitForResyncStatus(status db.BackgroundProcessState) db.ResyncManagerResponse {
	var resyncStatus db.ResyncManagerResponse
	successFunc := func() bool {
		response := rt.SendAdminRequest("GET", "/{{.db}}/_resync", "")
		err := json.Unmarshal(response.BodyBytes(), &resyncStatus)
		require.NoError(rt.TB, err)

		var val interface{}
		_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(rt.TB), &val)

		if status == db.BackgroundProcessStateCompleted {
			return resyncStatus.State == status && base.IsDocNotFoundError(err)
		} else {
			return resyncStatus.State == status
		}
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc), "Expected status: %s, actual status: %s", status, resyncStatus.State)
	return resyncStatus
}

func (rt *RestTester) WaitForResyncDCPStatus(status db.BackgroundProcessState) db.ResyncManagerResponseDCP {
	var resyncStatus db.ResyncManagerResponseDCP
	successFunc := func() bool {
		response := rt.SendAdminRequest("GET", "/{{.db}}/_resync", "")
		err := json.Unmarshal(response.BodyBytes(), &resyncStatus)
		require.NoError(rt.TB, err)

		var val interface{}
		_, err = rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(rt.TB), &val)

		if status == db.BackgroundProcessStateCompleted {
			return resyncStatus.State == status && base.IsDocNotFoundError(err)
		} else {
			return resyncStatus.State == status
		}
	}
	require.NoError(rt.TB, rt.WaitForCondition(successFunc), "Expected status: %s, actual status: %s", status, resyncStatus.State)
	return resyncStatus
}

// UpdatePersistedBucketName will update the persisted config bucket name to name specified in parameters
func (rt *RestTester) UpdatePersistedBucketName(dbConfig *DatabaseConfig, newBucketName *string) (*DatabaseConfig, error) {
	updatedDbConfig := DatabaseConfig{}
	_, err := rt.ServerContext().BootstrapContext.UpdateConfig(base.TestCtx(rt.TB), *dbConfig.Bucket, rt.ServerContext().Config.Bootstrap.ConfigGroupID, dbConfig.Name, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {

		bucketDbConfig = dbConfig
		bucketDbConfig.Bucket = newBucketName

		return bucketDbConfig, nil
	})
	return &updatedDbConfig, err
}

func (rt *RestTester) InsertDbConfigToBucket(config *DatabaseConfig, bucketName string) {
	_, insertErr := rt.ServerContext().BootstrapContext.InsertConfig(base.TestCtx(rt.TB), bucketName, rt.ServerContext().Config.Bootstrap.ConfigGroupID, config)
	require.NoError(rt.TB, insertErr)
}

func (rt *RestTester) PersistDbConfigToBucket(dbConfig DbConfig, bucketName string) {
	version, err := GenerateDatabaseConfigVersionID(rt.Context(), "", &dbConfig)
	require.NoError(rt.TB, err)

	metadataID, metadataIDError := rt.ServerContext().BootstrapContext.ComputeMetadataIDForDbConfig(base.TestCtx(rt.TB), &dbConfig)
	require.NoError(rt.TB, metadataIDError)

	dbConfig.Bucket = &bucketName
	persistedConfig := DatabaseConfig{
		Version:    version,
		MetadataID: metadataID,
		DbConfig:   dbConfig,
		SGVersion:  base.ProductVersion.String(),
	}
	rt.InsertDbConfigToBucket(&persistedConfig, rt.CustomTestBucket.GetName())
}

// setupSGRPeers sets up two rest testers to be used for sg-replicate testing with the following configuration:
//
//	activeRT:
//	  - backed by test bucket
//	  - has sgreplicate enabled
//	passiveRT:
//	  - backed by different test bucket
//	  - user 'alice' created with star channel access
//	  - http server wrapping the public API, remoteDBURLString targets the rt2 database as user alice (e.g. http://alice:pass@host/db)
//	returned teardown function closes activeRT, passiveRT and the http server, should be invoked with defer
func SetupSGRPeers(t *testing.T) (activeRT *RestTester, passiveRT *RestTester, remoteDBURLString string, teardown func()) {
	// Set up passive RestTester (rt2)
	passiveTestBucket := base.GetTestBucket(t)

	passiveRTConfig := &RestTesterConfig{
		CustomTestBucket: passiveTestBucket.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Name: "passivedb",
		}},
		SyncFn: channels.DocChannelsSyncFunction,
	}
	passiveRT = NewRestTester(t, passiveRTConfig)
	response := passiveRT.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/alice", GetUserPayload(t, "", RestTesterDefaultUserPassword, "", passiveRT.GetSingleTestDatabaseCollection(), []string{"*"}, nil))
	RequireStatus(t, response, http.StatusCreated)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(passiveRT.TestPublicHandler())

	// Build passiveDBURL with basic auth creds
	passiveDBURL, _ := url.Parse(srv.URL + "/" + passiveRT.GetDatabase().Name)
	passiveDBURL.User = url.UserPassword("alice", RestTesterDefaultUserPassword)

	// Set up active RestTester (rt1)
	activeTestBucket := base.GetTestBucket(t)
	activeRTConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Name: "activedb",
		}},
		CustomTestBucket:   activeTestBucket.NoCloseClone(),
		SgReplicateEnabled: true,
		SyncFn:             channels.DocChannelsSyncFunction,
	}
	activeRT = NewRestTester(t, activeRTConfig)
	// Initalize RT and bucket
	_ = activeRT.Bucket()

	teardown = func() {
		ctx := base.TestCtx(t)
		activeRT.Close()
		activeTestBucket.Close(ctx)
		srv.Close()
		passiveRT.Close()
		passiveTestBucket.Close(ctx)
	}
	return activeRT, passiveRT, passiveDBURL.String(), teardown
}
