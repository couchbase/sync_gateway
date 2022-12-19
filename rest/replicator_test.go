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
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestActiveReplicatorBlipsync uses an ActiveReplicator with another RestTester instance to connect and cleanly disconnect.
func TestActiveReplicatorBlipsync(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
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
	passiveDBURL.User = url.UserPassword("alice", "pass")
	stats, err := base.SyncGatewayStats.NewDBStats("test", false, false, false)
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
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
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
	require.Equal(t, http.StatusUpgradeRequired, response.StatusCode)
}

// Test that the username and password fields in the replicator still work and get redacted appropriately.
// This should log a deprecation notice.
func TestReplicatorDeprecatedCredentials(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	passiveRT := NewRestTesterDefaultCollection(t, //  CBG-2319: replicator currently requires default collection
		&RestTesterConfig{DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
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

	activeRT := NewRestTester(t, nil)
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
	"password": "pass"
}
`
	resp := activeRT.SendAdminRequest("POST", "/db/_replication/", replConfig)
	RequireStatus(t, resp, 201)

	activeRT.WaitForReplicationStatus(t.Name(), db.ReplicationStateRunning)

	err = passiveRT.WaitForRev("test", rev)
	require.NoError(t, err)

	resp = activeRT.SendAdminRequest("GET", "/db/_replication/"+t.Name(), "")
	RequireStatus(t, resp, 200)

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

	passiveRT := NewRestTesterDefaultCollection(t, //  CBG-2319: replicator currently requires default collection
		&RestTesterConfig{
			DatabaseConfig: &DatabaseConfig{}, // replicator requires default collection
		})
	defer passiveRT.Close()

	adminSrv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer adminSrv.Close()

	activeRT := NewRestTesterDefaultCollection(t, nil) //  CBG-2319: replicator currently requires default collection
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Disable checkpointing at an interval
	activeRT.GetDatabase().SGReplicateMgr.CheckpointInterval = 0
	err := activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
	require.NoError(t, err)

	database, err := db.CreateDatabase(activeRT.GetDatabase())
	require.NoError(t, err)
	rev, doc, err := database.GetSingleDatabaseCollectionWithUser().Put(activeCtx, "test", db.Body{})
	require.NoError(t, err)
	seq := strconv.FormatUint(doc.Sequence, 10)

	replConfig := `
{
	"replication_id": "` + t.Name() + `",
	"remote": "` + adminSrv.URL + `/db",
	"direction": "push",
	"continuous": true
}
`
	resp := activeRT.SendAdminRequest("POST", "/db/_replication/", replConfig)
	RequireStatus(t, resp, 201)

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

	// FIXME: CBG-2266 this test reads in persistent config

	// Create test buckets to replicate between
	passiveBucket := base.GetTestBucket(t)
	defer passiveBucket.Close()

	activeBucket := base.GetTestBucket(t)
	defer activeBucket.Close()

	// Set up passive bucket RT
	rt := NewRestTester(t, &RestTesterConfig{CustomTestBucket: passiveBucket})
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive replications
	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Start SG nodes for default group, group A and group B
	groupIDs := []string{"", "GroupA", "GroupB"}
	var adminHosts []string
	var serverContexts []*ServerContext
	for i, group := range groupIDs {
		serverErr := make(chan error, 0)

		config := BootstrapStartupConfigForTest(t)
		portOffset := i * 10
		adminInterface := fmt.Sprintf("127.0.0.1:%d", 4985+BootstrapTestPortOffset+portOffset)
		adminHosts = append(adminHosts, "http://"+adminInterface)
		config.API.PublicInterface = fmt.Sprintf("127.0.0.1:%d", 4984+BootstrapTestPortOffset+portOffset)
		config.API.AdminInterface = adminInterface
		config.API.MetricsInterface = fmt.Sprintf("127.0.0.1:%d", 4986+BootstrapTestPortOffset+portOffset)
		uniqueUUID, err := uuid.NewRandom()
		require.NoError(t, err)
		config.Bootstrap.ConfigGroupID = group + uniqueUUID.String()

		ctx := base.TestCtx(t)
		sc, err := SetupServerContext(ctx, &config, true)
		require.NoError(t, err)
		serverContexts = append(serverContexts, sc)
		ctx = sc.SetContextLogID(ctx, config.Bootstrap.ConfigGroupID)
		defer func() {
			sc.Close(ctx)
			require.NoError(t, <-serverErr)
		}()
		go func() {
			serverErr <- StartServer(ctx, &config, sc)
		}()
		require.NoError(t, sc.WaitForRESTAPIs())

		// Set up db config
		resp := BootstrapAdminRequestCustomHost(t, http.MethodPut, adminHosts[i], "/db/",
			fmt.Sprintf(
				`{"bucket": "%s", "num_index_replicas": 0, "use_views": %t, "import_docs": true, "sync":"%s"}`,
				activeBucket.GetName(), base.TestsDisableGSI(), channels.DefaultSyncFunction,
			),
		)
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
		}
		resp := BootstrapAdminRequestCustomHost(t, http.MethodPost, adminHosts[i], "/db/_replication/", MarshalConfig(t, replicationConfig))
		resp.RequireStatus(http.StatusCreated)
	}

	for groupNum, group := range groupIDs {
		channel := "chan" + group
		key := "doc" + group
		body := fmt.Sprintf(`{"channels":["%s"]}`, channel)
		// default data store - we're not using a named scope/collection in this test
		added, err := activeBucket.DefaultDataStore().Add(key, 0, []byte(body))
		require.NoError(t, err)
		require.True(t, added)

		// Force on-demand import and cache
		for _, host := range adminHosts {
			resp := BootstrapAdminRequestCustomHost(t, http.MethodGet, host, "/db/"+key, "")
			resp.RequireStatus(http.StatusOK)
		}

		for scNum, sc := range serverContexts {
			var expectedPushed int64 = 0
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
	rt := NewRestTester(t, &RestTesterConfig{SgReplicateEnabled: true})
	defer rt.Close()

	srv := httptest.NewServer(rt.TestAdminHandler())
	defer srv.Close()

	replConf := `
	{
	  "replication_id": "pushandpull-with-target-oneshot-adhoc",
	  "remote": "` + srv.URL + `/db",
	  "direction": "pushAndPull",
	  "adhoc": true
	}`

	resp := rt.SendAdminRequest("PUT", "/db/_replication/pushandpull-with-target-oneshot-adhoc", replConf)
	RequireStatus(t, resp, http.StatusCreated)

	// With the error hitting the replicationStatus endpoint will either return running, if not completed, and once
	// completed panics. With the fix after running it'll return a 404 as replication no longer exists.
	stateError := rt.WaitForCondition(func() bool {
		resp = rt.SendAdminRequest("GET", "/db/_replicationStatus/pushandpull-with-target-oneshot-adhoc", "")
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
			rtConfig := &RestTesterConfig{
				SyncFn: syncFunc,
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					Users: map[string]*auth.PrincipalConfig{
						"alice": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("chanAlpha", "chanBeta", "chanCharlie", "chanHotel", "chanIndia"),
						},
						"bob": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("chanDelta", "chanEcho"),
						},
					},
				}},
			}
			// Set up buckets, rest testers, and set up servers
			passiveRT := NewRestTesterDefaultCollection(t, rtConfig) //  CBG-2319: replicator currently requires default collection
			defer passiveRT.Close()

			publicSrv := httptest.NewServer(passiveRT.TestPublicHandler())
			defer publicSrv.Close()

			adminSrv := httptest.NewServer(passiveRT.TestAdminHandler())
			defer adminSrv.Close()

			activeRT := NewRestTesterDefaultCollection(t, rtConfig) //  CBG-2319: replicator currently requires default collection
			defer activeRT.Close()

			// Change RT depending on direction
			var senderRT *RestTester   // RT that has the initial docs that get replicated to the other bucket
			var receiverRT *RestTester // RT that gets the docs replicated to it
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
			resp := senderRT.SendAdminRequest("POST", "/db/_bulk_docs", bulkDocsBody)
			RequireStatus(t, resp, http.StatusCreated)

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
					"remote_password": "pass"
				}`

			resp = activeRT.SendAdminRequest("PUT", "/db/_replication/"+replName, replConf)
			RequireStatus(t, resp, http.StatusCreated)

			activeCtx := activeRT.Context()
			err = activeRT.GetDatabase().SGReplicateMgr.StartReplications(activeCtx)
			require.NoError(t, err)
			activeRT.WaitForReplicationStatus(replName, db.ReplicationStateRunning)

			value, _ := base.WaitForStat(receiverRT.GetDatabase().DbStats.Database().NumDocWrites.Value, 6)
			assert.EqualValues(t, 6, value)

			changesResults, err := receiverRT.WaitForChanges(6, "/db/_changes?since=0&include_docs=true", "", true)
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
						"batch": 200
					}`

			resp = activeRT.SendAdminRequest("PUT", "/db/_replication/"+replName, replConf)
			RequireStatus(t, resp, http.StatusCreated)
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
	rt := NewRestTesterDefaultCollection(t, nil) //  CBG-2319: replicator currently requires default collection
	defer rt.Close()

	var body db.Body

	// Put document as usual
	response := rt.SendAdminRequest("PUT", "/db/doc1", `{"foo": "bar"}`)
	RequireStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)

	// Get a document with rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true&rev="+revID, ``)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	// Get a document without specifying rev using replicator2
	response = rt.SendAdminRequest("GET", "/db/doc1?replicator2=true", ``)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}
}
func TestBasicPutReplicator2(t *testing.T) {
	rt := NewRestTesterDefaultCollection(t, nil) //  CBG-2319: replicator currently requires default collection
	defer rt.Close()

	var (
		body  db.Body
		revID string
		err   error
	)

	response := rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true", `{}`)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	// Put basic doc with replicator2 flag and ensure it saves correctly
	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID, `{"foo": "bar"}`)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("GET", "/db/doc1", ``)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusOK)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.Equal(t, "bar", body["foo"])
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotFound)
	}
}

func TestDeletedPutReplicator2(t *testing.T) {
	rt := NewRestTesterDefaultCollection(t, nil) //  CBG-2319: replicator currently requires default collection
	defer rt.Close()

	var body db.Body

	response := rt.SendAdminRequest("PUT", "/db/doc1", "{}")
	RequireStatus(t, response, http.StatusCreated)
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)
	assert.NoError(t, err)
	assert.True(t, body["ok"].(bool))
	revID := body["rev"].(string)
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().NumDocWrites.Value())

	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID+"&deleted=true", "{}")
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		revID = body["rev"].(string)
		assert.Equal(t, 2, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/db/doc1", ``)
		RequireStatus(t, response, http.StatusNotFound)
		assert.Equal(t, 0, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}

	response = rt.SendAdminRequest("PUT", "/db/doc1?replicator2=true&rev="+revID+"&deleted=false", `{}`)
	if base.IsEnterpriseEdition() {
		RequireStatus(t, response, http.StatusCreated)
		err = base.JSONUnmarshal(response.Body.Bytes(), &body)
		assert.NoError(t, err)
		assert.True(t, body["ok"].(bool))
		assert.Equal(t, 3, int(rt.GetDatabase().DbStats.Database().NumDocWrites.Value()))

		response = rt.SendAdminRequest("GET", "/db/doc1", ``)
		RequireStatus(t, response, http.StatusOK)
		assert.Equal(t, 1, int(rt.GetDatabase().DbStats.Database().NumDocReadsRest.Value()))
	} else {
		RequireStatus(t, response, http.StatusNotImplemented)
	}
}
