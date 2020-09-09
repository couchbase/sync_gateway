package rest

import (
	"encoding/json"
	"expvar"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestActiveReplicatorBlipsync uses an ActiveReplicator with another RestTester instance to connect and cleanly disconnect.
func TestActiveReplicatorBlipsync(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)()

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
			},
		},
		noAdminParty: true,
	})
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:                  t.Name(),
		Direction:           db.ActiveReplicatorTypePushAndPull,
		ActiveDB:            &db.Database{DatabaseContext: rt.GetDatabase()},
		RemoteDBURL:         passiveDBURL,
		Continuous:          true,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats("test").DBReplicatorStats(t.Name()),
	})

	startNumReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	startNumReplicationsActive := rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

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

// TestActiveReplicatorHeartbeats uses an ActiveReplicator with another RestTester instance to connect, and waits for several websocket ping/pongs.
func TestActiveReplicatorHeartbeats(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyWebSocket, base.KeyWebSocketFrame)()

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
			},
		},
		noAdminParty: true,
	})
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:                    t.Name(),
		Direction:             db.ActiveReplicatorTypePush,
		ActiveDB:              &db.Database{DatabaseContext: rt.GetDatabase()},
		RemoteDBURL:           passiveDBURL,
		WebsocketPingInterval: time.Millisecond * 10,
		Continuous:            true,
		ReplicationStatsMap:   base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})

	pingCountStart := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count"))
	pingGoroutinesStart := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))

	assert.NoError(t, ar.Start())

	time.Sleep(time.Millisecond * 50)

	pingGoroutines := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))
	assert.Equal(t, 1+pingGoroutinesStart, pingGoroutines, "Expected ping sender goroutine to be 1 more than start")

	pingCount := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count"))
	assert.Truef(t, pingCount > pingCountStart, "Expected ping count to be > pingCountStart")

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

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Passive
	tb2 := base.GetTestBucket(t)

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	remoteDoc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPull)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, strconv.FormatUint(remoteDoc.Sequence, 10), ar.GetStatus().LastSeqPull)
}

// TestActiveReplicatorPullAttachments:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document with an attachment on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
//   - Creates a second doc which references the same attachment.
func TestActiveReplicatorPullAttachments(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Passive
	tb2 := base.GetTestBucket(t)

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	attachment := `"_attachments":{"hi.txt":{"data":"aGk=","content_type":"text/plain"}}`

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","doc_num":1,`+attachment+`,"channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, int64(0), ar.Pull.GetStats().GetAttachment.Value())

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, int64(1), ar.Pull.GetStats().GetAttachment.Value())

	docID = t.Name() + "rt2doc2"
	resp = rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","doc_num":2,`+attachment+`,"channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID = respRevID(t, resp)

	// wait for the new document written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(2, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 2)
	assert.Equal(t, docID, changesResults.Results[1].ID)

	doc2, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc2.SyncData.CurrentRev)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])

	// Because we're targeting a Hydrogen node that supports proveAttachment, we only end up sending the attachment once.
	// If targeting a pre-hydrogen node, GetAttachment would be 2.
	assert.Equal(t, int64(1), ar.Pull.GetStats().GetAttachment.Value())
	assert.Equal(t, int64(1), ar.Pull.GetStats().ProveAttachment.Value())
}

// TestActiveReplicatorPullMergeConflictingAttachments:
//   - Creates an initial revision on rt2 which is replicated to rt1.
//   - Stops the replicator, and adds different attachments to the doc on both rt1 and rt2 at conflicting revisions.
//   - Starts the replicator to trigger conflict resolution to merge both attachments in the conflict.
func TestActiveReplicatorPullMergeConflictingAttachments(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skip("Test uses EE-only features for custom conflict resolution")
	}

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

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

			defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

			// Increase checkpoint persistence frequency for cross-node status verification
			defer SetDefaultCheckpointInterval(50 * time.Millisecond)()

			// Disable sequence batching for multi-RT tests (pending CBG-1000)
			defer db.SuspendSequenceBatching()()

			// Passive
			rt2 := NewRestTester(t, &RestTesterConfig{
				DatabaseConfig: &DbConfig{
					Users: map[string]*db.PrincipalConfig{
						"alice": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("alice"),
						},
					},
				},
				noAdminParty: true,
			})
			defer rt2.Close()

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword("alice", "pass")

			// Active
			rt1 := NewRestTester(t, &RestTesterConfig{
				DatabaseConfig: &DbConfig{
					Replications: map[string]*db.ReplicationConfig{
						"repl1": {
							Remote:                 passiveDBURL.String(),
							Direction:              db.ActiveReplicatorTypePull,
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
				},
			})
			defer rt1.Close()

			err = rt1.GetDatabase().SGReplicateMgr.StartReplications()
			require.NoError(t, err)

			rt1.waitForAssignedReplications(1)

			docID := test.name + "doc1"
			putDocResp := rt2.putDoc(docID, test.initialRevBody)
			require.True(t, putDocResp.Ok)
			rev1 := putDocResp.Rev

			// wait for the document originally written to rt2 to arrive at rt1
			changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			lastSeq := changesResults.Last_Seq.(string)

			resp := rt1.SendAdminRequest(http.MethodPut, "/db/_replicationStatus/repl1?action=stop", "")
			assertStatus(t, resp, http.StatusOK)

			rt1.waitForReplicationStatus("repl1", db.ReplicationStateStopped)

			resp = rt1.SendAdminRequest(http.MethodPut, "/db/"+docID+"?rev="+rev1, test.localConflictingRevBody)
			assertStatus(t, resp, http.StatusCreated)

			changesResults, err = rt1.WaitForChanges(1, "/db/_changes?since="+lastSeq, "", true)
			require.NoError(t, err)
			assert.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			lastSeq = changesResults.Last_Seq.(string)

			resp = rt2.SendAdminRequest(http.MethodPut, "/db/"+docID+"?rev="+rev1, test.remoteConflictingRevBody)
			assertStatus(t, resp, http.StatusCreated)

			resp = rt1.SendAdminRequest(http.MethodPut, "/db/_replicationStatus/repl1?action=start", "")
			assertStatus(t, resp, http.StatusOK)

			rt1.waitForReplicationStatus("repl1", db.ReplicationStateRunning)

			changesResults, err = rt1.WaitForChanges(1, "/db/_changes?since="+lastSeq, "", true)
			require.NoError(t, err)
			assert.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			lastSeq = changesResults.Last_Seq.(string)

			doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			revGen, _ := db.ParseRevID(doc.SyncData.CurrentRev)

			assert.Equal(t, 3, revGen)
			assert.Equal(t, "merged", doc.Body()["source"].(string))

			assert.Nil(t, doc.Body()[db.BodyAttachments], "_attachments property should not be in resolved doc body")

			assert.Len(t, doc.SyncData.Attachments, test.expectedAttachments, "mismatch in expected number of attachments in sync data of resolved doc")
			for attName, att := range doc.SyncData.Attachments {
				attMap := att.(map[string]interface{})
				assert.Equal(t, true, attMap["stub"].(bool), "attachment %q should be a stub", attName)
				assert.NotEmpty(t, attMap["digest"].(string), "attachment %q should have digest", attName)
				assert.True(t, attMap["revpos"].(float64) >= 1, "attachment %q revpos should be at least 1", attName)
				assert.True(t, attMap["length"].(float64) >= 1, "attachment %q length should be at least 1 byte", attName)
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

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	const (
		changesBatchSize  = 10
		numRT2DocsInitial = 13 // 2 batches of changes
		numRT2DocsTotal   = 24 // 2 more batches
	)

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Create first batch of docs
	docIDPrefix := t.Name() + "rt2doc"
	for i := 0; i < numRT2DocsInitial; i++ {
		resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
	}

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:       true,
		ChangesBatchSize: changesBatchSize,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval:  time.Minute * 5,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

	assert.NoError(t, ar.Start())

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(numRT2DocsInitial, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT2DocsInitial)
	docIDsSeen := make(map[string]bool, numRT2DocsInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numRT2DocsInitial; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])
	}

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+numRT2DocsInitial, numRevsSentTotal)
	assert.Equal(t, int64(numRT2DocsInitial), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT2DocsInitial), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs
	for i := numRT2DocsInitial; i < numRT2DocsTotal; i++ {
		resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	ar = db.NewActiveReplicator(&arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start())

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(numRT2DocsTotal, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT2DocsTotal)

	docIDsSeen = make(map[string]bool, numRT2DocsTotal)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}

	for i := 0; i < numRT2DocsTotal; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])
	}

	// Make sure we've not started any more since:0 replications on rt2 since the first one
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+numRT2DocsTotal, numRevsSentTotal)
	assert.Equal(t, int64(numRT2DocsTotal-numRT2DocsInitial), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT2DocsTotal-numRT2DocsInitial), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorPullFromCheckpointIgnored:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates identical documents on rt1 and rt2.
//   - Starts a pull replication to ensure that even ignored revisions are checkpointed.
func TestActiveReplicatorPullFromCheckpointIgnored(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	const (
		changesBatchSize  = 10
		numRT2DocsInitial = 13 // 2 batches of changes
		numRT2DocsTotal   = 24 // 2 more batches
	)

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	// Create first batch of docs
	docIDPrefix := t.Name() + "doc"
	for i := 0; i < numRT2DocsInitial; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt1RevID := respRevID(t, resp)
		resp = rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt2RevID := respRevID(t, resp)
		require.Equal(t, rt1RevID, rt2RevID)
	}

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:       true,
		ChangesBatchSize: changesBatchSize,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval:  time.Minute * 5,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()

	assert.NoError(t, ar.Start())

	_, ok := base.WaitForStat(func() int64 {
		return ar.Pull.Checkpointer.Stats().AlreadyKnownSequenceCount
	}, numRT2DocsInitial)
	assert.True(t, ok)

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(numRT2DocsInitial, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT2DocsInitial)
	docIDsSeen := make(map[string]bool, numRT2DocsInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numRT2DocsInitial; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		_, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
	}

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, int64(0), numRevsSentTotal)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs
	for i := numRT2DocsInitial; i < numRT2DocsTotal; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt1RevID := respRevID(t, resp)
		resp = rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt2RevID := respRevID(t, resp)
		require.Equal(t, rt1RevID, rt2RevID)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	ar = db.NewActiveReplicator(&arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start())

	_, ok = base.WaitForStat(func() int64 {
		return ar.Pull.Checkpointer.Stats().AlreadyKnownSequenceCount
	}, numRT2DocsTotal-numRT2DocsInitial)
	assert.True(t, ok)

	// Make sure we've not started any more since:0 replications on rt2 since the first one
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, int64(0), numRevsSentTotal)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorPullOneshot:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullOneshot(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyReplicate)()

	// Passive
	tb2 := base.GetTestBucket(t)

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	remoteDoc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPull)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

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

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])
	assert.Equal(t, strconv.FormatUint(remoteDoc.Sequence, 10), ar.GetStatus().LastSeqPull)
}

// TestActiveReplicatorPushBasic:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which can be pushed by the replicator.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasic(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Passive
	tb2 := base.GetTestBucket(t)

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Active
	tb1 := base.GetTestBucket(t)

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	localDoc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, strconv.FormatUint(localDoc.Sequence, 10), ar.GetStatus().LastSeqPush)
}

// TestActiveReplicatorPushAttachments:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document with an attachment on rt1 which can be pushed by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pushing changes to rt2.
//   - Creates a second doc which references the same attachment.
func TestActiveReplicatorPushAttachments(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	attachment := `"_attachments":{"hi.txt":{"data":"aGk=","content_type":"text/plain"}}`

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","doc_num":1,`+attachment+`,"channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, int64(0), ar.Push.GetStats().HandleGetAttachment.Value())

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, int64(1), ar.Push.GetStats().HandleGetAttachment.Value())

	docID = t.Name() + "rt1doc2"
	resp = rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","doc_num":2,`+attachment+`,"channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID = respRevID(t, resp)

	// wait for the new document written to rt1 to arrive at rt2
	changesResults, err = rt2.WaitForChanges(2, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 2)
	assert.Equal(t, docID, changesResults.Results[1].ID)

	doc2, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc2.SyncData.CurrentRev)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	// Because we're targeting a Hydrogen node that supports proveAttachment, we only end up sending the attachment once.
	// If targeting a pre-hydrogen node, HandleGetAttachment would be 2.
	assert.Equal(t, int64(1), ar.Push.GetStats().HandleGetAttachment.Value())
	assert.Equal(t, int64(1), ar.Push.GetStats().HandleProveAttachment.Value())
}

// TestActiveReplicatorPushFromCheckpoint:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt1 which can be pushed by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt1.
//   - Starts the push replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPushFromCheckpoint(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	const (
		changesBatchSize  = 10
		numRT1DocsInitial = 13 // 2 batches of changes
		numRT1DocsTotal   = 24 // 2 more batches
	)

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	// Create first batch of docs
	docIDPrefix := t.Name() + "rt2doc"
	for i := 0; i < numRT1DocsInitial; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
	}

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:       true,
		ChangesBatchSize: changesBatchSize,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval: time.Minute * 5,
	}

	// Create the first active replicator to pull from seq:0
	arConfig.ReplicationStatsMap = base.SyncGatewayStats.NewDBStats(t.Name() + "1").DBReplicatorStats(t.Name())
	ar := db.NewActiveReplicator(&arConfig)

	startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()

	assert.NoError(t, ar.Start())

	// wait for all of the documents originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(numRT1DocsInitial, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT1DocsInitial)
	docIDsSeen := make(map[string]bool, numRT1DocsInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numRT1DocsInitial; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])
	}

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, startNumRevsSentTotal+numRT1DocsInitial, numRevsSentTotal)
	assert.Equal(t, int64(numRT1DocsInitial), ar.Push.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT1DocsInitial), ar.Push.Checkpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs
	for i := numRT1DocsInitial; i < numRT1DocsTotal; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	arConfig.ReplicationStatsMap = base.SyncGatewayStats.NewDBStats(t.Name() + "2").DBReplicatorStats(t.Name())
	ar = db.NewActiveReplicator(&arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start())

	// wait for all of the documents originally written to rt1 to arrive at rt2
	changesResults, err = rt2.WaitForChanges(numRT1DocsTotal, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numRT1DocsTotal)

	docIDsSeen = make(map[string]bool, numRT1DocsTotal)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}

	for i := 0; i < numRT1DocsTotal; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])
	}

	// Make sure we've not started any more since:0 replications on rt1 since the first one
	endNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure the new replicator has only sent new mutations
	numRevsSentNewReplicator := ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, numRT1DocsTotal-numRT1DocsInitial, int(numRevsSentNewReplicator))
	assert.Equal(t, int64(numRT1DocsTotal-numRT1DocsInitial), ar.Push.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numRT1DocsTotal-numRT1DocsInitial), ar.Push.Checkpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorPushFromCheckpointIgnored:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt1 which can be pushed by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt1.
//   - Starts the push replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPushFromCheckpointIgnored(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	const (
		changesBatchSize  = 10
		numRT1DocsInitial = 13 // 2 batches of changes
		numRT1DocsTotal   = 24 // 2 more batches
	)

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Create first batch of docs
	docIDPrefix := t.Name() + "doc"
	for i := 0; i < numRT1DocsInitial; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt1RevID := respRevID(t, resp)
		resp = rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt2RevID := respRevID(t, resp)
		require.Equal(t, rt1RevID, rt2RevID)
	}

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:       true,
		ChangesBatchSize: changesBatchSize,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval:  time.Minute * 5,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)

	startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()

	assert.NoError(t, ar.Start())

	_, ok := base.WaitForStat(func() int64 {
		return ar.Push.Checkpointer.Stats().AlreadyKnownSequenceCount
	}, numRT1DocsInitial)
	assert.True(t, ok)

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, int64(0), numRevsSentTotal)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs
	for i := numRT1DocsInitial; i < numRT1DocsTotal; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt1RevID := respRevID(t, resp)
		resp = rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
		rt2RevID := respRevID(t, resp)
		require.Equal(t, rt1RevID, rt2RevID)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	ar = db.NewActiveReplicator(&arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start())

	_, ok = base.WaitForStat(func() int64 {
		return ar.Push.Checkpointer.Stats().AlreadyKnownSequenceCount
	}, numRT1DocsTotal-numRT1DocsInitial)
	assert.True(t, ok)

	// Make sure we've not started any more since:0 replications on rt1 since the first one
	endNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure rt1 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, int64(0), numRevsSentTotal)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorPushOneshot:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which can be pushed by the replicator.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushOneshot(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Passive
	tb2 := base.GetTestBucket(t)

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Active
	tb1 := base.GetTestBucket(t)

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	localDoc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

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

	doc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, strconv.FormatUint(localDoc.Sequence, 10), ar.GetStatus().LastSeqPush)
}

// TestActiveReplicatorPullTombstone:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
//   - Deletes the document in rt2, and waits for the tombstone to get to rt1.
func TestActiveReplicatorPullTombstone(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyReplicate)()

	// Passive
	tb2 := base.GetTestBucket(t)

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])

	// Tombstone the doc in rt2
	resp = rt2.SendAdminRequest(http.MethodDelete, "/db/"+docID+"?rev="+revID, ``)
	assertStatus(t, resp, http.StatusOK)
	revID = respRevID(t, resp)

	// wait for the tombstone written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(1, "/db/_changes?since="+strconv.FormatUint(doc.Sequence, 10), "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err = rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
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

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyReplicate)()

	// Passive
	tb2 := base.GetTestBucket(t)

	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	docID := t.Name() + "rt2doc1"
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)

	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		PurgeOnRemoval:      true,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for the document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])

	resp = rt2.SendAdminRequest(http.MethodPut, "/db/"+docID+"?rev="+revID, `{"source":"rt2","channels":["bob"]}`)
	assertStatus(t, resp, http.StatusCreated)

	// wait for the channel removal written to rt2 to arrive at rt1 - we can't monitor _changes, because we've purged, not removed. But we can monitor the associated stat.
	base.WaitForStat(func() int64 {
		stats := ar.GetStatus()
		return stats.DocsPurged
	}, 1)

	doc, err = rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.Error(t, err)
	assert.True(t, base.IsDocNotFoundError(err), "Error returned wasn't a DocNotFound error")
	assert.Nil(t, doc)
}

// TestActiveReplicatorPullConflict:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
// TODO: extend test cases to include conflicts with common ancestors (i.e. update conflict instead of insert conflict)
func TestActiveReplicatorPullConflict(t *testing.T) {

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
			expectedLocalRevID:     db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"merged"}`)), // rev for merged body, with parent 1-b
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
			expectedLocalRevID:     db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"local"}`)), // rev for local body, transposed under parent 1-b
			expectedResolutionType: db.ConflictResolutionLocal,
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			if base.GTestBucketPool.NumUsableBuckets() < 2 {
				t.Skipf("test requires at least 2 usable test buckets")
			}
			defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD)()

			// Passive
			tb2 := base.GetTestBucket(t)

			rt2 := NewRestTester(t, &RestTesterConfig{
				TestBucket: tb2,
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
			defer rt2.Close()

			// Create revision on rt2 (remote)
			docID := test.name
			resp, err := rt2.PutDocumentWithRevID(docID, test.remoteRevID, "", test.remoteRevisionBody)
			assert.NoError(t, err)
			assertStatus(t, resp, http.StatusCreated)
			rt2revID := respRevID(t, resp)
			assert.Equal(t, test.remoteRevID, rt2revID)

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword("alice", "pass")

			// Active
			tb1 := base.GetTestBucket(t)

			rt1 := NewRestTester(t, &RestTesterConfig{
				TestBucket: tb1,
			})
			defer rt1.Close()

			// Create revision on rt1 (local)
			resp, err = rt1.PutDocumentWithRevID(docID, test.localRevID, "", test.localRevisionBody)
			assert.NoError(t, err)
			assertStatus(t, resp, http.StatusCreated)
			rt1revID := respRevID(t, resp)
			assert.Equal(t, test.localRevID, rt1revID)

			customConflictResolver, err := db.NewCustomConflictResolver(test.conflictResolver)
			require.NoError(t, err)
			replicationStats := base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name())
			ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
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
			})
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start())

			waitAndRequireCondition(t, func() bool { return ar.GetStatus().DocsRead == 1 }, "Expecting DocsRead == 1")
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
			}
			// wait for the document originally written to rt2 to arrive at rt1.  Should end up as winner under default conflict resolution

			changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedLocalRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLocalRevID, doc.SyncData.CurrentRev)
			assert.Equal(t, test.expectedLocalBody, doc.Body())
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

// TestActiveReplicatorPushAndPullConflict:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pushAndPull from rt2.
//   - verifies expected conflict resolution, and that expected result is replicated to both peers
// TODO: extend test cases to include conflicts with common ancestors (i.e. update conflict instead of insert conflict)
func TestActiveReplicatorPushAndPullConflict(t *testing.T) {

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
	}{
		{
			name:               "remoteWins",
			localRevisionBody:  db.Body{"source": "local"},
			localRevID:         "1-a",
			remoteRevisionBody: db.Body{"source": "remote"},
			remoteRevID:        "1-b",
			conflictResolver:   `function(conflict) {return conflict.RemoteDocument;}`,
			expectedLocalBody:  db.Body{"source": "remote"},
			expectedLocalRevID: "1-b",
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
			expectedLocalBody:  db.Body{"source": "merged"},
			expectedLocalRevID: db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"merged"}`)), // rev for merged body, with parent 1-b
		},

		{
			name:               "localWins",
			localRevisionBody:  db.Body{"source": "local"},
			localRevID:         "1-a",
			remoteRevisionBody: db.Body{"source": "remote"},
			remoteRevID:        "1-b",
			conflictResolver:   `function(conflict) {return conflict.LocalDocument;}`,
			expectedLocalBody:  db.Body{"source": "local"},
			expectedLocalRevID: db.CreateRevIDWithBytes(2, "1-b", []byte(`{"source":"local"}`)), // rev for local body, transposed under parent 1-b
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			if base.GTestBucketPool.NumUsableBuckets() < 2 {
				t.Skipf("test requires at least 2 usable test buckets")
			}
			defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD)()

			// Passive
			rt2 := NewRestTester(t, &RestTesterConfig{
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
			defer rt2.Close()

			// Create revision on rt2 (remote)
			docID := test.name
			resp, err := rt2.PutDocumentWithRevID(docID, test.remoteRevID, "", test.remoteRevisionBody)
			assert.NoError(t, err)
			assertStatus(t, resp, http.StatusCreated)
			rt2revID := respRevID(t, resp)
			assert.Equal(t, test.remoteRevID, rt2revID)

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword("alice", "pass")

			// Active
			rt1 := NewRestTester(t, &RestTesterConfig{
				TestBucket: base.GetTestBucket(t),
			})
			defer rt1.Close()

			// Create revision on rt1 (local)
			resp, err = rt1.PutDocumentWithRevID(docID, test.localRevID, "", test.localRevisionBody)
			assert.NoError(t, err)
			assertStatus(t, resp, http.StatusCreated)
			rt1revID := respRevID(t, resp)
			assert.Equal(t, test.localRevID, rt1revID)

			customConflictResolver, err := db.NewCustomConflictResolver(test.conflictResolver)
			require.NoError(t, err)
			ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:     200,
				ConflictResolverFunc: customConflictResolver,
				Continuous:           true,
				ReplicationStatsMap:  base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
			})
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start())

			// TODO: Use replication stats to wait for replication to complete
			time.Sleep(1 * time.Second)
			log.Printf("========================Replication should be done, checking with changes")
			// wait for the document originally written to rt2 to arrive at rt1.  Should end up as winner under default conflict resolution

			// Validate results on the local (rt1)
			changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedLocalRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLocalRevID, doc.SyncData.CurrentRev)
			assert.Equal(t, test.expectedLocalBody, doc.Body())
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

			// Validate results on the remote (rt2)
			changesResults, err = rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedLocalRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err = rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLocalRevID, doc.SyncData.CurrentRev)
			assert.Equal(t, test.expectedLocalBody, doc.Body())
			log.Printf("Doc %s is %+v", docID, doc)
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
	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Passive
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewTLSServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		InsecureSkipVerify:  true,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	require.NoError(t, ar.Start())

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])
}

// TestActiveReplicatorPushBasicWithInsecureSkipVerifyDisabled:
//   - Starts 2 RestTesters, one active, and one passive
//   - Creates a document on rt1 which can be pushed by the replicator to rt2.
//   - rt2 served using a self-signed TLS cert (via httptest)
//   - Uses an ActiveReplicator configured for push to start pushing changes to rt2.
func TestActiveReplicatorPushBasicWithInsecureSkipVerifyDisabled(t *testing.T) {
	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Passive
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewTLSServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		InsecureSkipVerify:  false,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// Start the replicator (implicit connect)
	assert.Error(t, ar.Start(), "Error certificate signed by unknown authority")
}

// TestActiveReplicatorRecoverFromLocalFlush:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt2 which is pulled to rt1.
//   - Checkpoints once finished.
//   - Recreates rt1 with a new bucket (to simulate a flush).
//   - Starts the replication again, and ensures that documents are re-replicated to it.
func TestActiveReplicatorRecoverFromLocalFlush(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 3 {
		t.Skipf("test requires at least 3 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	// Passive
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Create doc on rt2
	docID := t.Name() + "rt2doc"
	resp := rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt2.WaitForPendingChanges())

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
	})

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous: true,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval:  time.Minute * 5,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

	assert.NoError(t, ar.Start())

	// wait for document originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+1, numRevsSentTotal)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// close rt1, and release the underlying bucket back to the pool.
	rt1.Close()

	// recreate rt1 with a new bucket
	rt1 = NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	// Have to re-set ActiveDB because we recreated it with the new rt1.
	arConfig.ActiveDB = &db.Database{
		DatabaseContext: rt1.GetDatabase(),
	}
	ar = db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start())

	// we pulled the remote checkpoint, but the local checkpoint wasn't there to match it.
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)

	// wait for document originally written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err = rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])

	// one _changes from seq:0 with initial number of docs sent
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal+1, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+2, numRevsSentTotal)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorRecoverFromRemoteFlush:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Checkpoints once finished.
//   - Recreates rt2 with a new bucket (to simulate a flush).
//   - Starts the replication again, and ensures that post-flush, documents are re-replicated to it.
func TestActiveReplicatorRecoverFromRemoteFlush(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 3 {
		t.Skipf("test requires at least 3 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	// Create doc on rt1
	docID := t.Name() + "rt1doc"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt1.WaitForPendingChanges())

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous: true,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval: time.Minute * 5,
	}

	// Create the first active replicator to pull from seq:0
	arConfig.ReplicationStatsMap = base.SyncGatewayStats.NewDBStats(t.Name() + "1").DBReplicatorStats(t.Name())
	ar := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	startNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	//startNumRevsSentTotal := ar.Pull.GetStats().SendRevCount.Value()
	startNumRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()

	assert.NoError(t, ar.Start())

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, startNumRevsSentTotal+1, numRevsSentTotal)
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().GetCheckpointMissCount)
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// close rt2, and release the underlying bucket back to the pool.
	rt2.Close()

	// recreate rt2 with a new bucket, http server and update target URL in the replicator
	rt2 = NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	srv.Config.Handler = rt2.TestPublicHandler()

	passiveDBURL, err = url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")
	arConfig.RemoteDBURL = passiveDBURL
	arConfig.ReplicationStatsMap = base.SyncGatewayStats.NewDBStats(t.Name() + "2").DBReplicatorStats(t.Name())

	ar = db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start())

	// we pulled the remote checkpoint, but the local checkpoint wasn't there to match it.
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().GetCheckpointHitCount)

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err = rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err = rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	// one _changes from seq:0 with initial number of docs sent
	endNumChangesRequestedFromZeroTotal := rt1.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, numChangesRequestedFromZeroTotal+1, endNumChangesRequestedFromZeroTotal)

	// make sure the replicator has resent the rev
	numRevsSentTotal = ar.Push.GetStats().SendRevCount.Value()
	assert.Equal(t, startNumRevsSentTotal+1, numRevsSentTotal)
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)

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

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyBucket, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	// Passive
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()

	// Create doc1 on rt1
	docID := t.Name() + "rt1doc"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt1.WaitForPendingChanges())

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous: true,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval:  time.Minute * 5,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start())

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)
	lastSeq := changesResults.Last_Seq.(string)

	doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)

	cID := ar.Push.CheckpointID
	checkpointDocID := base.SyncPrefix + "local:checkpoint/" + cID

	firstCheckpoint, _, err := rt2.Bucket().GetRaw(checkpointDocID)
	require.NoError(t, err)

	// Create doc2 on rt1
	resp = rt1.SendAdminRequest(http.MethodPut, "/db/"+docID+"2", `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)

	assert.NoError(t, rt1.WaitForPendingChanges())

	// wait for new document to arrive at rt2
	changesResults, err = rt2.WaitForChanges(1, "/db/_changes?since="+lastSeq, "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID+"2", changesResults.Results[0].ID)

	doc, err = rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(2), ar.Push.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// roll back checkpoint value to first one and remove the associated doc
	err = rt2.Bucket().SetRaw(checkpointDocID, 0, firstCheckpoint)
	assert.NoError(t, err)

	rt2db, err := db.GetDatabase(rt2.GetDatabase(), nil)
	require.NoError(t, err)
	err = rt2db.Purge(docID + "2")
	assert.NoError(t, err)

	require.NoError(t, rt2.GetDatabase().FlushChannelCache())
	rt2.GetDatabase().FlushRevisionCacheForTest()

	assert.NoError(t, ar.Start())

	// wait for new document to arrive at rt2 again
	changesResults, err = rt2.WaitForChanges(1, "/db/_changes?since="+lastSeq, "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID+"2", changesResults.Results[0].ID)

	doc, err = rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	require.NoError(t, err)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorRecoverFromMismatchedRev:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates a document on rt1 which is pushed to rt2.
//   - Modifies the checkpoint rev ID in the target bucket.
//   - Checkpoints again to ensure it is retried on error.
func TestActiveReplicatorRecoverFromMismatchedRev(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyBucket, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	// Passive
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous: true,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval:  time.Minute * 5,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	assert.NoError(t, ar.Start())

	pushCheckpointID := ar.Push.CheckpointID
	pushCheckpointDocID := base.SyncPrefix + "local:checkpoint/" + pushCheckpointID
	err = rt2.Bucket().SetRaw(pushCheckpointDocID, 0, []byte(`{"last_sequence":"0","_rev":"abc"}`))
	require.NoError(t, err)

	pullCheckpointID := ar.Pull.CheckpointID
	require.NoError(t, err)
	pullCheckpointDocID := base.SyncPrefix + "local:checkpoint/" + pullCheckpointID
	err = rt1.Bucket().SetRaw(pullCheckpointDocID, 0, []byte(`{"last_sequence":"0","_rev":"abc"}`))
	require.NoError(t, err)

	// Create doc1 on rt1
	docID := t.Name() + "rt1doc"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	assert.NoError(t, rt1.WaitForPendingChanges())

	// wait for document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	// Create doc2 on rt2
	docID = t.Name() + "rt2doc"
	resp = rt2.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt2","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	assert.NoError(t, rt2.WaitForPendingChanges())

	// wait for document originally written to rt2 to arrive at rt1
	changesResults, err = rt1.WaitForChanges(1, "/db/_changes?since=1", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	assert.Equal(t, int64(0), ar.Push.Checkpointer.Stats().SetCheckpointCount)
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Push.Checkpointer.Stats().SetCheckpointCount)

	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorIgnoreNoConflicts ensures the IgnoreNoConflicts flag allows Hydrogen<-->Hydrogen replication with no_conflicts set.
func TestActiveReplicatorIgnoreNoConflicts(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket)()

	// Passive
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
		DatabaseConfig: &DbConfig{
			AllowConflicts: base.BoolPtr(false),
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	localDoc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	})
	defer func() { assert.NoError(t, ar.Stop()) }()

	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for the document originally written to rt1 to arrive at rt2
	changesResults, err := rt2.WaitForChanges(1, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, 1)
	assert.Equal(t, docID, changesResults.Results[0].ID)

	doc, err := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
	assert.NoError(t, err)

	assert.Equal(t, revID, doc.SyncData.CurrentRev)
	assert.Equal(t, "rt1", doc.GetDeepMutableBody()["source"])

	assert.Equal(t, strconv.FormatUint(localDoc.Sequence, 10), ar.GetStatus().LastSeqPush)
}

// TestActiveReplicatorPullFromCheckpointModifiedHash:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt2 which can be pulled by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt2.
//   - Starts the pull replication again with a config change, validate checkpoint is reset
func TestActiveReplicatorPullModifiedHash(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

	const (
		changesBatchSize         = 10
		numDocsPerChannelInitial = 13 // 2 batches of changes
		numDocsPerChannelTotal   = 24 // 2 more batches
		numChannels              = 2  // two channels
	)

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("chan1", "chan2"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Create first batch of docs, creating numRT2DocsInitial in each channel
	docIDPrefix := t.Name() + "rt2doc"
	for i := 0; i < numDocsPerChannelInitial; i++ {
		rt2.putDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i), `{"source":"rt2","channels":["chan1"]}`)
		rt2.putDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i), `{"source":"rt2","channels":["chan2"]}`)
	}

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build passiveDBURL with basic auth creds
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	arConfig := db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:       true,
		ChangesBatchSize: changesBatchSize,
		Filter:           base.ByChannelFilter,
		FilterChannels:   []string{"chan1"},
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval:  time.Minute * 5,
		ReplicationStatsMap: base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull chan1 from seq:0
	ar := db.NewActiveReplicator(&arConfig)

	startNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	startNumRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()

	assert.NoError(t, ar.Start())

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(numDocsPerChannelInitial, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, numDocsPerChannelInitial)
	docIDsSeen := make(map[string]bool, numDocsPerChannelInitial)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}
	for i := 0; i < numDocsPerChannelInitial; i++ {
		docID := fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i)
		assert.True(t, docIDsSeen[docID])
		doc := rt1.getDoc(docID)
		assert.Equal(t, "rt2", doc["source"])
	}

	// one _changes from seq:0 with initial number of docs sent
	numChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+int64(numDocsPerChannelInitial), numRevsSentTotal)
	assert.Equal(t, int64(numDocsPerChannelInitial), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(numDocsPerChannelInitial), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// checkpoint assertions
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)

	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)

	assert.NoError(t, ar.Stop())

	// Second batch of docs, both channels
	for i := numDocsPerChannelInitial; i < numDocsPerChannelTotal; i++ {
		rt2.putDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan1", i), `{"source":"rt2","channels":["chan1"]}`)
		rt2.putDoc(fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i), `{"source":"rt2","channels":["chan2"]}`)
	}

	// Create a new replicator using the same replicationID but different channel filter, which should reset the checkpoint
	arConfig.FilterChannels = []string{"chan2"}
	ar = db.NewActiveReplicator(&arConfig)
	defer func() { assert.NoError(t, ar.Stop()) }()
	assert.NoError(t, ar.Start())

	// wait for all of the documents originally written to rt2 to arrive at rt1
	expectedChan1Docs := numDocsPerChannelInitial
	expectedChan2Docs := numDocsPerChannelTotal
	expectedTotalDocs := expectedChan1Docs + expectedChan2Docs
	changesResults, err = rt1.WaitForChanges(expectedTotalDocs, "/db/_changes?since=0", "", true)
	require.NoError(t, err)
	require.Len(t, changesResults.Results, expectedTotalDocs)

	docIDsSeen = make(map[string]bool, expectedTotalDocs)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}

	for i := 0; i < numDocsPerChannelTotal; i++ {
		docID := fmt.Sprintf("%s_%s_%d", docIDPrefix, "chan2", i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])
	}

	// Should have two replications since zero
	endNumChangesRequestedFromZeroTotal := rt2.GetDatabase().DbStats.CBLReplicationPull().NumPullReplSinceZero.Value()
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+2, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = rt2.GetDatabase().DbStats.CBLReplicationPull().RevSendCount.Value()
	assert.Equal(t, startNumRevsSentTotal+int64(expectedTotalDocs), numRevsSentTotal)
	assert.Equal(t, int64(expectedChan2Docs), ar.Pull.Checkpointer.Stats().ProcessedSequenceCount)
	assert.Equal(t, int64(expectedChan2Docs), ar.Pull.Checkpointer.Stats().ExpectedSequenceCount)

	// assert the second active replicator stats
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().GetCheckpointHitCount)
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().GetCheckpointMissCount)
	assert.Equal(t, int64(0), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), ar.Pull.Checkpointer.Stats().SetCheckpointCount)
}

// TestActiveReplicatorReconnectOnStart ensures ActiveReplicators retry their initial connection for cases like:
// - Incorrect credentials
// - Unroutable remote address
// Will test both indefinite retry, and a timeout.
func TestActiveReplicatorReconnectOnStart(t *testing.T) {
	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

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

					defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

					// Passive
					tb2 := base.GetTestBucket(t)
					rt2 := NewRestTester(t, &RestTesterConfig{
						TestBucket: tb2,
						DatabaseConfig: &DbConfig{
							Users: map[string]*db.PrincipalConfig{
								"alice": {
									Password:         base.StringPtr("pass"),
									ExplicitChannels: base.SetOf("alice"),
								},
							},
						},
						noAdminParty: true,
					})
					defer rt2.Close()

					// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
					srv := httptest.NewServer(rt2.TestPublicHandler())
					defer srv.Close()

					// Build remoteDBURL with basic auth creds
					remoteDBURL, err := url.Parse(srv.URL + "/db")
					require.NoError(t, err)

					// Add basic auth creds to target db URL
					username := "alice"
					if test.usernameOverride != "" {
						username = test.usernameOverride
					}
					remoteDBURL.User = url.UserPassword(username, "pass")

					if test.remoteURLHostOverride != "" {
						remoteDBURL.Host = test.remoteURLHostOverride
					}

					// Active
					tb1 := base.GetTestBucket(t)
					rt1 := NewRestTester(t, &RestTesterConfig{
						TestBucket: tb1,
					})
					defer rt1.Close()

					arConfig := db.ActiveReplicatorConfig{
						ID:          base.GenerateRandomID(),
						Direction:   db.ActiveReplicatorTypePush,
						RemoteDBURL: remoteDBURL,
						ActiveDB: &db.Database{
							DatabaseContext: rt1.GetDatabase(),
						},
						Continuous: true,
						// test isn't long running enough to worry about time-based checkpoints,
						// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
						CheckpointInterval: time.Minute * 5,
						// aggressive reconnect intervals for testing purposes
						InitialReconnectInterval: time.Millisecond,
						MaxReconnectInterval:     time.Millisecond * 50,
						TotalReconnectTimeout:    timeoutVal,
						ReplicationStatsMap:      base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
					}

					// Create the first active replicator to pull from seq:0
					ar := db.NewActiveReplicator(&arConfig)
					require.NoError(t, err)

					assert.Equal(t, int64(0), ar.Push.GetStats().NumConnectAttempts.Value())

					err = ar.Start()
					assert.Error(t, err, "expecting ar.Start() to return error, but it didn't")

					if test.expectedErrorIsConnectionRefused {
						assert.True(t, base.IsConnectionRefusedError(err))
					}

					if test.expectedErrorContains != "" {
						assert.True(t, strings.Contains(err.Error(), test.expectedErrorContains))
					}

					// wait for an arbitrary number of reconnect attempts
					waitAndRequireCondition(t, func() bool {
						return ar.Push.GetStats().NumConnectAttempts.Value() > 2
					}, "Expecting NumConnectAttempts > 2")

					if timeoutVal > 0 {
						time.Sleep(timeoutVal + time.Millisecond*250)
						// wait for the retry loop to hit the TotalReconnectTimeout and give up retrying
						waitAndRequireCondition(t, func() bool {
							return ar.Push.GetStats().NumReconnectsAborted.Value() > 0
						}, "Expecting NumReconnectsAborted > 0")
					}

					assert.NoError(t, ar.Stop())
				})
			}
		})
	}
}

// TestActiveReplicatorReconnectOnStartEventualSuccess ensures an active replicator with invalid creds retries,
// but succeeds once the user is created on the remote.
func TestActiveReplicatorReconnectOnStartEventualSuccess(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp)()

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket:   tb2,
		noAdminParty: true,
	})
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
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	arConfig := db.ActiveReplicatorConfig{
		ID:          base.GenerateRandomID(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: remoteDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous: true,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval: time.Minute * 5,
		// aggressive reconnect intervals for testing purposes
		InitialReconnectInterval: time.Millisecond,
		MaxReconnectInterval:     time.Millisecond * 50,
		ReplicationStatsMap:      base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	assert.Equal(t, int64(0), ar.Push.GetStats().NumConnectAttempts.Value())

	err = ar.Start()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unexpected status code 401 from target database"))

	// wait for an arbitrary number of reconnect attempts
	waitAndRequireCondition(t, func() bool {
		return ar.Push.GetStats().NumConnectAttempts.Value() > 3
	}, "Expecting NumConnectAttempts > 3")

	resp := rt2.SendAdminRequest(http.MethodPut, "/db/_user/alice", `{"password":"pass"}`)
	assertStatus(t, resp, http.StatusCreated)

	waitAndRequireCondition(t, func() bool {
		state, _ := ar.State()
		return state == db.ReplicationStateRunning
	}, "Expecting replication state to be running")

	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorReconnectSendActions ensures ActiveReplicator reconnect retry loops exit when the replicator is stopped
func TestActiveReplicatorReconnectSendActions(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp)()

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb2,
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("alice"),
				},
			},
		},
		noAdminParty: true,
	})
	defer rt2.Close()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	// Build remoteDBURL with basic auth creds
	remoteDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add incorrect basic auth creds to target db URL
	remoteDBURL.User = url.UserPassword("bob", "pass")

	// Active
	tb1 := base.GetTestBucket(t)
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	arConfig := db.ActiveReplicatorConfig{
		ID:          base.GenerateRandomID(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: remoteDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous: true,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval: time.Minute * 5,
		// aggressive reconnect intervals for testing purposes
		InitialReconnectInterval: time.Millisecond,
		MaxReconnectInterval:     time.Millisecond * 50,
		TotalReconnectTimeout:    time.Second * 5,
		ReplicationStatsMap:      base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
	}

	// Create the first active replicator to pull from seq:0
	ar := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	assert.Equal(t, int64(0), ar.Pull.GetStats().NumConnectAttempts.Value())

	err = ar.Start()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unexpected status code 401 from target database"))

	// wait for an arbitrary number of reconnect attempts
	waitAndRequireCondition(t, func() bool {
		return ar.Pull.GetStats().NumConnectAttempts.Value() > 3
	}, "Expecting NumConnectAttempts > 3")

	assert.NoError(t, ar.Stop())
	reconnectAttempts := ar.Pull.GetStats().NumConnectAttempts.Value()

	// wait for a bit to see if the reconnect loop has stopped
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, reconnectAttempts, ar.Pull.GetStats().NumConnectAttempts.Value())

	assert.NoError(t, ar.Reset())

	err = ar.Start()
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unexpected status code 401 from target database"))

	// wait for another set of reconnect attempts
	waitAndRequireCondition(t, func() bool {
		return ar.Pull.GetStats().NumConnectAttempts.Value() > 3
	}, "Expecting NumConnectAttempts > 3")

	assert.NoError(t, ar.Stop())
}

func waitAndRequireCondition(t *testing.T, fn func() bool, failureMsgAndArgs ...interface{}) {
	for i := 0; i <= 20; i++ {
		if i == 20 {
			require.Fail(t, "Condition failed to be satisfied", failureMsgAndArgs...)
		}
		if fn() {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func waitAndAssertCondition(t *testing.T, fn func() bool, failureMsgAndArgs ...interface{}) {
	for i := 0; i <= 20; i++ {
		if i == 20 {
			assert.Fail(t, "Condition failed to be satisfied", failureMsgAndArgs...)
		}
		if fn() {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
}

func TestBlipSyncNonUpgradableConnection(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)()
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DbConfig{
			Users: map[string]*db.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
			},
		},
		noAdminParty: true,
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

// TestActiveReplicatorPullConflictReadWriteIntlProps:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullConflictReadWriteIntlProps(t *testing.T) {

	createRevID := func(generation int, parentRevID string, body db.Body) string {
		rev, err := db.CreateRevID(generation, parentRevID, body)
		require.NoError(t, err, "Error creating revision")
		return rev
	}
	docExpiry := time.Now().Local().Add(time.Hour * time.Duration(4)).Format(time.RFC3339)

	// scenarios
	conflictResolutionTests := []struct {
		name               string
		localRevisionBody  db.Body
		localRevID         string
		remoteRevisionBody db.Body
		remoteRevID        string
		conflictResolver   string
		expectedLocalBody  db.Body
		expectedLocalRevID string
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
			localRevID: "1-a",
			remoteRevisionBody: db.Body{
				"source": "remote",
			},
			remoteRevID: "1-b",
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
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				"localDeleted": true,
				"source":       "merged",
			}),
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			if base.GTestBucketPool.NumUsableBuckets() < 2 {
				t.Skipf("test requires at least 2 usable test buckets")
			}
			defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

			// Passive
			tb2 := base.GetTestBucket(t)

			rt2 := NewRestTester(t, &RestTesterConfig{
				TestBucket: tb2,
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
			defer rt2.Close()

			// Create revision on rt2 (remote)
			docID := test.name
			resp, err := rt2.PutDocumentWithRevID(docID, test.remoteRevID, "", test.remoteRevisionBody)
			assert.NoError(t, err)
			assertStatus(t, resp, http.StatusCreated)
			rt2revID := respRevID(t, resp)
			assert.Equal(t, test.remoteRevID, rt2revID)

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)

			// Add basic auth creds to target db URL
			passiveDBURL.User = url.UserPassword("alice", "pass")

			// Active
			tb1 := base.GetTestBucket(t)

			rt1 := NewRestTester(t, &RestTesterConfig{
				TestBucket: tb1,
			})
			defer rt1.Close()

			// Create revision on rt1 (local)
			resp, err = rt1.PutDocumentWithRevID(docID, test.localRevID, "", test.localRevisionBody)
			assert.NoError(t, err)
			assertStatus(t, resp, http.StatusCreated)
			rt1revID := respRevID(t, resp)
			assert.Equal(t, test.localRevID, rt1revID)

			customConflictResolver, err := db.NewCustomConflictResolver(test.conflictResolver)
			require.NoError(t, err)
			replicationStats := base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name())
			ar := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
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
			})
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start())
			waitAndRequireCondition(t, func() bool { return ar.GetStatus().DocsRead == 1 })
			assert.Equal(t, 1, int(replicationStats.ConflictResolvedMergedCount.Value()))

			// Wait for the document originally written to rt2 to arrive at rt1.
			// Should end up as winner under default conflict resolution.
			changesResults, err := rt1.WaitForChanges(1, "/db/_changes?&since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedLocalRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLocalRevID, doc.SyncData.CurrentRev)
			log.Printf("doc.Body(): %v", doc.Body())
			assert.Equal(t, test.expectedLocalBody, doc.Body())
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

// This test ensures that the tombstone revision wins over non-tombstone revision
// whilst applying default conflict resolution policy through pushAndPull replication.
func TestDefaultConflictResolverWithTombstone(t *testing.T) {
	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()

	// requireErrorKeyNotFound asserts that reading specified document body via SDK
	// returns a key not found error.
	requireErrorKeyNotFound := func(rt *RestTester, docID string) {
		var body []byte
		_, err := rt.Bucket().Get(docID, &body)
		require.EqualError(t, err, fmt.Sprintf("Error during Get %s: key not found", docID))
	}

	// waitForTombstone waits until the specified tombstone revision is available
	// in the bucket backed by the specified RestTester instance.
	waitForTombstone := func(rt *RestTester, docID, revID string) {
		require.NoError(t, rt.WaitForCondition(func() bool {
			doc, _ := rt.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
			return doc.IsDeleted() && len(doc.Body()) == 0
		}))
	}

	// createOrUpdateDoc creates a new document or update an existing document with the
	// specified document id, revision id and body value in a channel named "alice".
	createOrUpdateDoc := func(rt *RestTester, docID, revID, bodyValue string) string {
		body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
		dbURL := "/db/" + docID
		if docID != "" {
			dbURL = "/db/" + docID + "?rev=" + revID
		}
		resp := rt.SendAdminRequest(http.MethodPut, dbURL, body)
		assertStatus(t, resp, http.StatusCreated)
		require.NoError(t, rt.WaitForPendingChanges())
		return respRevID(t, resp)
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
			expectedRevID:    "5-82185c1b8859f7854bff0cd201ba3fbc",
		},
		{
			// Revision tie with local digest is higher than the remote digest.
			// local generation = remote generation:
			//	- e.g. local is 3-c(T), remote is 3-b
			name:             "revGenTieLocalDigestHigher",
			remoteBodyValues: []string{"baz", "qux"},
			expectedRevID:    "5-5829b78ea578d88225dcb31fb40cbad9",
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
			expectedRevID:    "5-a56584cc3a3de18b723652c3a410a202",
		},
	}

	for _, test := range defaultConflictResolverWithTombstoneTests {
		t.Run(test.name, func(tt *testing.T) {
			// Passive
			rt2 := NewRestTester(t, &RestTesterConfig{
				TestBucket: base.GetTestBucket(t),
				DatabaseConfig: &DbConfig{
					Users: map[string]*db.PrincipalConfig{
						"alice": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("alice"),
						},
					},
				},
				noAdminParty: true,
			})
			defer rt2.Close()

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			// Build passiveDBURL with basic auth creds
			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)
			passiveDBURL.User = url.UserPassword("alice", "pass")

			// Active
			rt1 := NewRestTester(t, &RestTesterConfig{
				TestBucket: base.GetTestBucket(t),
			})
			defer rt1.Close()

			defaultConflictResolver, err := db.NewCustomConflictResolver(
				`function(conflict) { return defaultPolicy(conflict); }`)
			require.NoError(t, err, "Error creating custom conflict resolver")

			config := db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				Continuous:           true,
				ConflictResolverFunc: defaultConflictResolver,
				ReplicationStatsMap:  base.SyncGatewayStats.NewDBStats(t.Name()).DBReplicatorStats(t.Name()),
			}

			// Create the first revision of the document on rt1.
			docID := t.Name() + "foo"
			rt1RevIDCreated := createOrUpdateDoc(rt1, docID, "", "foo")

			// Create active replicator and start replication.
			ar := db.NewActiveReplicator(&config)
			require.NoError(t, ar.Start(), "Error starting replication")
			defer func() { require.NoError(t, ar.Stop(), "Error stopping replication") }()

			// Wait for the original document revision written to rt1 to arrive at rt2.
			rt2RevIDCreated := rt1RevIDCreated
			require.NoError(t, rt2.WaitForCondition(func() bool {
				doc, _ := rt2.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
				return doc != nil && doc.SyncData.CurrentRev == rt2RevIDCreated && len(doc.Body()) > 0
			}))

			// Stop replication.
			require.NoError(t, ar.Stop(), "Error stopping replication")

			// Update the document on rt1 to build a revision history.
			rt1RevIDUpdated := createOrUpdateDoc(rt1, docID, rt1RevIDCreated, "bar")

			// Tombstone the document on rt1 to mark the tip of the revision history for deletion.
			resp := rt1.SendAdminRequest(http.MethodDelete, "/db/"+docID+"?rev="+rt1RevIDUpdated, ``)
			assertStatus(t, resp, http.StatusOK)
			rt1RevIDDeleted := respRevID(t, resp)

			// Ensure that the tombstone revision is written to rt1 bucket with an empty body.
			waitForTombstone(rt1, docID, rt1RevIDDeleted)

			// Update the document on rt2 with the specified body values.
			rt2RevID := rt2RevIDCreated
			for _, bodyKey := range test.remoteBodyValues {
				rt2RevID = createOrUpdateDoc(rt2, docID, rt2RevID, bodyKey)
			}

			// Start replication.
			require.NoError(t, ar.Start(), "Error starting replication")

			// Wait for default conflict resolution policy to be applied through replication and
			// the winning revision to be written to both rt1 and rt2 buckets. Check whether the
			// winning revision is a tombstone; tombstone revision wins over non-tombstone revision.
			waitForTombstone(rt2, docID, test.expectedRevID)
			waitForTombstone(rt1, docID, test.expectedRevID)

			// Ensure that the document body of the winning tombstone revision written to both
			// rt1 and rt2 is empty, i.e., An attempt to read the document body of a tombstone
			// revision via SDK should return a "key not found" error.
			requireErrorKeyNotFound(rt2, docID)
			requireErrorKeyNotFound(rt1, docID)
		})
	}
}
