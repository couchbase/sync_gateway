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
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Run is equivalent to testing.T.Run() but updates the RestTester's TB to the new testing.T
// so that checks are made against the right instance (otherwise the outer test complains
// "subtest may have called FailNow on a parent test")
func (rt *RestTester) Run(name string, test func(*testing.T)) {
	mainT := rt.TB().(*testing.T)
	mainT.Run(name, func(t *testing.T) {
		var tb testing.TB = t
		old := rt.testingTB.Swap(&tb)
		defer func() { rt.testingTB.Store(old) }()
		test(t)
	})
}

func (rt *RestTester) UpdateTB(t *testing.T) {
	var tb testing.TB = t
	rt.testingTB.Store(&tb)
}

// GetDocBody returns the doc body for the given docID. If the document is not found, t.Fail will be called.
func (rt *RestTester) GetDocBody(docID string) db.Body {
	return rt.GetDocBodyFromKeyspace("{{.keyspace}}", docID)
}

// GetDocBodyFromKeyspace returns the doc body for the given docID in the specified keyspace. If the document is not found, t.Fail will be called.
func (rt *RestTester) GetDocBodyFromKeyspace(keyspace, docID string) db.Body {
	rawResponse := rt.SendAdminRequest("GET", "/"+keyspace+"/"+docID, "")
	RequireStatus(rt.TB(), rawResponse, http.StatusOK)
	var body db.Body
	require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
	require.NotNil(rt.TB(), body)
	return body
}

// GetDoc returns the doc body and version for the given docID. If the document is not found, t.Fail will be called.
func (rt *RestTester) GetDoc(docID string) (DocVersion, db.Body) {
	rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
	RequireStatus(rt.TB(), rawResponse, http.StatusOK)
	var body db.Body
	require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
	version, err := db.ParseVersion(body[db.BodyCV].(string))
	require.NoError(rt.TB(), err)
	return DocVersion{RevTreeID: body[db.BodyRev].(string), CV: version}, body
}

// TriggerOnDemandImport will use the REST API to trigger on an demand import via GET. This function intentionally does not check error codes in case the document does not exist or is invalid to be imported.
func (rt *RestTester) TriggerOnDemandImport(docID string) {
	_ = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/{{.keyspace}}/%s", docID), "")
}

// GetDocVersion returns the doc body and version for the given docID and version. If the document is not found, t.Fail will be called.
func (rt *RestTester) GetDocVersion(docID string, version DocVersion) db.Body {
	rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID+"?rev="+version.RevTreeID, "")
	RequireStatus(rt.TB(), rawResponse, http.StatusOK)
	var body db.Body
	require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
	return body
}

// CreateTestDoc creates a document with an arbitrary body.
func (rt *RestTester) CreateTestDoc(docid string) DocVersion {
	response := rt.SendAdminRequest("PUT", fmt.Sprintf("/%s/%s", rt.GetSingleKeyspace(), docid), `{"prop":true}`)
	RequireStatus(rt.TB(), response, 201)
	return DocVersionFromPutResponse(rt.TB(), response)
}

// PutDoc will upsert the document with a given contents.
func (rt *RestTester) PutDoc(docID string, body string) DocVersion {
	return rt.PutDocWithKeyspace(rt.GetSingleKeyspace(), docID, body)
}

// PutDocWithKeyspace will upsert the document with a given contents in the specified keyspace.
func (rt *RestTester) PutDocWithKeyspace(keyspace, docID string, body string) DocVersion {
	rawResponse := rt.SendAdminRequest("PUT", fmt.Sprintf("/%s/%s", keyspace, docID), body)
	RequireStatus(rt.TB(), rawResponse, 201)
	return DocVersionFromPutResponse(rt.TB(), rawResponse)
}

// UpdateDocRev updates a document at a specific revision and returns the new version. Deprecated for UpdateDoc.
func (rt *RestTester) UpdateDocRev(docID, revID string, body string) string {
	version := rt.UpdateDoc(docID, DocVersion{RevTreeID: revID}, body)
	return version.RevTreeID
}

// UpdateDoc updates a document at a specific version and returns the new version.
func (rt *RestTester) UpdateDoc(docID string, version DocVersion, body string) DocVersion {
	resource := fmt.Sprintf("/%s/%s?rev=%s", rt.GetSingleKeyspace(), docID, version.RevTreeID)
	rawResponse := rt.SendAdminRequest(http.MethodPut, resource, body)
	RequireStatus(rt.TB(), rawResponse, http.StatusCreated)
	return DocVersionFromPutResponse(rt.TB(), rawResponse)
}

// DeleteDoc deletes a document at a specific version. The test will fail if the revision does not exist.
func (rt *RestTester) DeleteDoc(docID string, docVersion DocVersion) DocVersion {
	resp := rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/%s/%s?rev=%s", rt.GetSingleKeyspace(), docID, docVersion.RevTreeID), "")
	RequireStatus(rt.TB(), resp, http.StatusOK)
	return DocVersionFromPutResponse(rt.TB(), resp)
}

// DeleteDocRev removes a document at a specific revision. Deprecated for DeleteDoc.
func (rt *RestTester) DeleteDocRev(docID, revID string) {
	rt.DeleteDoc(docID, DocVersion{RevTreeID: revID})
}

func (rt *RestTester) GetDatabaseRoot(dbname string) DatabaseRoot {
	var dbroot DatabaseRoot
	resp := rt.SendAdminRequest("GET", "/"+dbname+"/", "")
	RequireStatus(rt.TB(), resp, 200)
	require.NoError(rt.TB(), base.JSONUnmarshal(resp.BodyBytes(), &dbroot))
	return dbroot
}

// WaitForVersion retries a GET for a given document version until it returns 200 or 201 for a given document and revision. If version is not found, the test will fail.
func (rt *RestTester) WaitForVersion(docID string, version DocVersion) {
	if version.RevTreeID == "" {
		require.NotEqual(rt.TB(), "", version.CV.String(), "Expected CV if RevTreeID in WaitForVersion")
	}
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
		if !assert.Contains(c, []int{200, 201}, rawResponse.Code, "Unexpected status code for %s", rawResponse.Body.String()) {
			return
		}
		var body db.Body
		require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
		if version.RevTreeID != "" {
			assert.Equal(c, version.RevTreeID, body.ExtractRev())
		}
		if !version.CV.IsEmpty() {
			assert.Equal(c, version.CV.String(), body[db.BodyCV].(string))
		}
	}, 10*time.Second, 50*time.Millisecond)
}

// WaitForRevTreeVersion retries a GET for a given document version until it returns 200 or 201 for a given document and revtree version. If version is not found, the test will fail.
//
// This is a stub for until ISGR supports CV.
func (rt *RestTester) WaitForRevTreeVersion(docID string, version DocVersion) {
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		rawResponse := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docID, "")
		if !assert.Contains(c, []int{200, 201}, rawResponse.Code, "Unexpected status code for %s", rawResponse.Body.String()) {
			return
		}
		var body db.Body
		require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &body))
		assert.Equal(c, version.RevTreeID, body.ExtractRev())
	}, 10*time.Second, 50*time.Millisecond)
}

// WaitForTombstone waits for a the document version to exist and be tombstoned. If the document is not found, the test will fail.
func (rt *RestTester) WaitForTombstone(docID string, deleteVersion DocVersion) {
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		if !assert.NoError(c, err) {
			return
		}
		assert.NotEqual(c, int64(0), doc.TombstonedAt)
		if deleteVersion.RevTreeID != "" {
			assert.Equal(c, deleteVersion.RevTreeID, doc.SyncData.CurrentRev)
		}
		if !deleteVersion.CV.IsEmpty() {
			assert.Equal(c, deleteVersion.CV.String(), doc.SyncData.HLV.GetCurrentVersionString())
		}
	}, time.Second*10, time.Millisecond*100)
}

// WaitForRevTreeTombstone waits for a the document version to exist and be tombstoned. If the document is not found, the test will fail.
//
// This is a stub for until ISGR supports CV.
func (rt *RestTester) WaitForRevTreeTombstone(docID string, deleteVersion DocVersion) {
	collection, ctx := rt.GetSingleTestDatabaseCollectionWithUser()
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		doc, err := collection.GetDocument(ctx, docID, db.DocUnmarshalAll)
		if !assert.NoError(c, err) {
			return
		}
		assert.NotEqual(c, int64(0), doc.TombstonedAt)
		assert.Equal(c, deleteVersion.RevTreeID, doc.SyncData.CurrentRev)
	}, time.Second*10, time.Millisecond*100)
}
func (rt *RestTester) WaitForCheckpointLastSequence(expectedName string) (string, error) {
	var lastSeq string
	successFunc := func() bool {
		val, _, err := rt.GetSingleDataStore().GetRaw(expectedName)
		if err != nil {
			rt.TB().Logf("Error getting checkpoint: %v - will retry", err)
			return false
		}
		var config struct { // db.replicationCheckpoint
			LastSeq string `json:"last_sequence"`
		}
		err = json.Unmarshal(val, &config)
		if err != nil {
			rt.TB().Logf("Error unmarshalling checkpoint: %v - will retry", err)
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
	require.NoError(rt.TB(), rt.WaitForCondition(successFunc), "mismatch on number of active replicators")
}

func (rt *RestTester) WaitForPullBlipSenderInitialisation(name string) {
	successFunc := func() bool {
		bs := rt.GetDatabase().SGReplicateMgr.GetActiveReplicator(name).Pull.GetBlipSender()
		return bs != nil
	}
	require.NoError(rt.TB(), rt.WaitForCondition(successFunc), "blip sender on active replicator not initialized")
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
	require.NoError(rt.TB(), err)
	resp := rt.SendAdminRequest(http.MethodPost, "/"+dbName+"/_replication/", string(payload))
	RequireStatus(rt.TB(), resp, http.StatusCreated)
}

func (rt *RestTester) WaitForAssignedReplications(count int) {
	successFunc := func() bool {
		replicationStatuses := rt.GetReplicationStatuses("?localOnly=true")
		return len(replicationStatuses) == count
	}
	require.NoError(rt.TB(), rt.WaitForCondition(successFunc))
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
	require.NoError(rt.TB(), rt.WaitForCondition(successFunc), "Mismatch in active replicator count, expected count %d actual %d", expCount, count)
}

func (rt *RestTester) WaitForReplicationStatusForDB(dbName string, replicationID string, targetStatus string) {
	var status db.ReplicationStatus
	successFunc := func() bool {
		status = rt.GetReplicationStatusForDB(dbName, replicationID)
		return status.Status == targetStatus
	}
	require.NoError(rt.TB(), rt.WaitForCondition(successFunc), "Expected status: %s, actual status: %s", targetStatus, status.Status)
}

func (rt *RestTester) WaitForReplicationStatus(replicationID string, targetStatus string) {
	rt.WaitForReplicationStatusForDB("{{.db}}", replicationID, targetStatus)
}

func (rt *RestTester) GetReplications() (replications map[string]db.ReplicationCfg) {
	rawResponse := rt.SendAdminRequest("GET", "/{{.db}}/_replication/", "")
	RequireStatus(rt.TB(), rawResponse, 200)
	require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &replications))
	return replications
}

func (rt *RestTester) GetReplicationStatus(replicationID string) (status db.ReplicationStatus) {
	return rt.GetReplicationStatusForDB("{{.db}}", replicationID)
}

func (rt *RestTester) GetReplicationStatusForDB(dbName string, replicationID string) (status db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/"+dbName+"/_replicationStatus/"+replicationID, "")
	RequireStatus(rt.TB(), rawResponse, 200)
	require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &status))
	return status
}

func (rt *RestTester) GetReplicationStatuses(queryString string) (statuses []db.ReplicationStatus) {
	rawResponse := rt.SendAdminRequest("GET", "/{{.db}}/_replicationStatus/"+queryString, "")
	RequireStatus(rt.TB(), rawResponse, 200)
	require.NoError(rt.TB(), base.JSONUnmarshal(rawResponse.Body.Bytes(), &statuses))
	return statuses
}

// RunResync takes database offline, runs resync and waits for it to complete and takes database online. Returns the completed resync status.
func (rt *RestTester) RunResync() db.ResyncManagerResponseDCP {
	rt.TakeDbOffline()
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_resync", "")
	RequireStatus(rt.TB(), resp, http.StatusOK)
	return rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
}

// WaitForResyncDCPStatus waits for the resync status to reach the expected status and returns the final status.
func (rt *RestTester) WaitForResyncDCPStatus(status db.BackgroundProcessState) db.ResyncManagerResponseDCP {
	var resyncStatus db.ResyncManagerResponseDCP
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		response := rt.SendAdminRequest("GET", "/{{.db}}/_resync", "")
		RequireStatus(rt.TB(), response, http.StatusOK)
		require.NoError(rt.TB(), json.Unmarshal(response.BodyBytes(), &resyncStatus))

		assert.Equal(c, status, resyncStatus.State)
		if slices.Contains([]db.BackgroundProcessState{db.BackgroundProcessStateCompleted, db.BackgroundProcessStateStopped}, status) {
			var output any
			_, err := rt.Bucket().DefaultDataStore().Get(rt.GetDatabase().ResyncManager.GetHeartbeatDocID(rt.TB()), output)
			assert.True(c, base.IsDocNotFoundError(err), "expected heartbeat doc to be deleted, got: %v", err)
		}
	}, time.Second*10, time.Millisecond*10)
	return resyncStatus
}

// UpdatePersistedBucketName will update the persisted config bucket name to name specified in parameters
func (rt *RestTester) UpdatePersistedBucketName(dbConfig *DatabaseConfig, newBucketName *string) (*DatabaseConfig, error) {
	updatedDbConfig := DatabaseConfig{}
	_, err := rt.ServerContext().BootstrapContext.UpdateConfig(base.TestCtx(rt.TB()), *dbConfig.Bucket, rt.ServerContext().Config.Bootstrap.ConfigGroupID, dbConfig.Name, func(originalConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {

		bucketDbConfig := dbConfig
		bucketDbConfig.cfgCas = originalConfig.cfgCas
		bucketDbConfig.Bucket = newBucketName

		return bucketDbConfig, nil
	})
	return &updatedDbConfig, err
}

func (rt *RestTester) InsertDbConfigToBucket(config *DatabaseConfig, bucketName string) {
	_, insertErr := rt.ServerContext().BootstrapContext.InsertConfig(base.TestCtx(rt.TB()), bucketName, rt.ServerContext().Config.Bootstrap.ConfigGroupID, config)
	require.NoError(rt.TB(), insertErr)
}

func (rt *RestTester) RemoveDbConfigFromBucket(dbName string, bucketName string) {
	deleteErr := rt.ServerContext().BootstrapContext.DeleteConfig(base.TestCtx(rt.TB()), bucketName, rt.ServerContext().Config.Bootstrap.ConfigGroupID, dbName)
	require.NoError(rt.TB(), deleteErr)
}

func (rt *RestTester) PersistDbConfigToBucket(dbConfig DbConfig, bucketName string) {
	version, err := GenerateDatabaseConfigVersionID(rt.Context(), "", &dbConfig)
	require.NoError(rt.TB(), err)

	metadataID, metadataIDError := rt.ServerContext().BootstrapContext.ComputeMetadataIDForDbConfig(base.TestCtx(rt.TB()), &dbConfig)
	require.NoError(rt.TB(), metadataIDError)

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
	passiveRT.CreateUser("alice", []string{"*"})

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
	// Initialize RT and bucket
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

// TakeDbOffline takes the database offline.
func (rt *RestTester) TakeDbOffline() {
	if rt.PersistentConfig {
		resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", `{"offline":true}`)
		RequireStatus(rt.TB(), resp, http.StatusCreated)
		rt.WaitForDBState(db.RunStateString[db.DBOffline])
	} else {
		resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_offline", "")
		RequireStatus(rt.TB(), resp, http.StatusOK)
	}
	require.Equal(rt.TB(), db.DBOffline, atomic.LoadUint32(&rt.GetDatabase().State))
}

// TakeDbOnline takes the database online and waits for online status.
func (rt *RestTester) TakeDbOnline() {
	if rt.PersistentConfig {
		resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", `{"offline":false}`)
		RequireStatus(rt.TB(), resp, http.StatusCreated)
	} else {
		resp := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_online", "")
		RequireStatus(rt.TB(), resp, http.StatusOK)
	}
	rt.WaitForDBOnline()
}

// RequireDbOnline asserts that the state of the database is online
func (rt *RestTester) RequireDbOnline() {
	response := rt.SendAdminRequest("GET", "/{{.db}}/", "")
	var body db.Body
	require.NoError(rt.TB(), base.JSONUnmarshal(response.Body.Bytes(), &body))
	require.Equal(rt.TB(), "Online", body["state"].(string))
}

// PutDocWithAttachment will upsert the document with a given contents and attachments.
func (rt *RestTester) PutDocWithAttachment(docID string, body string, attachmentName, attachmentBody string) DocVersion {
	// create new body with a 1.x style inline attachment body like `{"_attachments": {"camera.txt": {"data": "Q2Fub24gRU9TIDVEIE1hcmsgSVY="}}}`.
	require.NotEmpty(rt.TB(), attachmentName)
	require.NotEmpty(rt.TB(), attachmentBody)
	var rawBody db.Body
	require.NoError(rt.TB(), base.JSONUnmarshal([]byte(body), &rawBody))
	require.NotContains(rt.TB(), rawBody, db.BodyAttachments)
	rawBody[db.BodyAttachments] = map[string]any{
		attachmentName: map[string]any{"data": attachmentBody},
	}
	return rt.PutDoc(docID, string(base.MustJSONMarshal(rt.TB(), rawBody)))
}

type RawDocResponse struct {
	Xattrs RawDocXattrs `json:"_xattrs"`
}

// RawDocXattrs is a non-exhaustive set of xattrs returned by the _raw endpoint. Used for test assertions.
// TODO: Replace with JSON v2's inline - this implementation puts well-known fields into the "other" map as well, which isn't great but not worth fixing.
type RawDocXattrs struct {
	RawDocXattrsWellKnown
	RawDocXattrsOthers
}

// RawDocXattrsWellKnown contains well known fields - see RawDocXattrsOthers for those not known until runtime.
type RawDocXattrsWellKnown struct {
	Sync       db.SyncDataJSON `json:"_sync"`
	GlobalSync map[string]any  `json:"_globalSync"`
	VV         map[string]any  `json:"_vv"`
	MOU        map[string]any  `json:"_mou"`
}

// RawDocXattrsOthers contains fields that can only be known at runtime (e.g. user xattr names)
type RawDocXattrsOthers map[string]any

func (t *RawDocXattrs) MarshalJSON() ([]byte, error) {
	// overwrite fields in Main into Extra
	data, err := json.Marshal(t.RawDocXattrsWellKnown)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &t.RawDocXattrsOthers); err != nil {
		return nil, err
	}
	return json.Marshal(t.RawDocXattrsOthers)
}

func (t *RawDocXattrs) UnmarshalJSON(p []byte) error {
	// unmarshal others first - and overwrite with known fields after
	if err := json.Unmarshal(p, &t.RawDocXattrsOthers); err != nil {
		return err
	}
	return json.Unmarshal(p, &t.RawDocXattrsWellKnown)
}

var _ json.Unmarshaler = &RawDocXattrs{}
