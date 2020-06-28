package rest

import (
	"expvar"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
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

	ar, err := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    db.ActiveReplicatorTypePushAndPull,
		ActiveDB:     &db.Database{DatabaseContext: rt.GetDatabase()},
		PassiveDBURL: passiveDBURL,
	})
	require.NoError(t, err)

	startNumReplicationsTotal := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsTotal))
	startNumReplicationsActive := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsActive))

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// Check total stat
	numReplicationsTotal := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsTotal))
	assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)

	// Check active stat
	assert.Equal(t, startNumReplicationsActive+2, base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsActive)))

	// Close the replicator (implicit disconnect)
	assert.NoError(t, ar.Close())

	// Wait for active stat to drop to original value
	numReplicationsActive, ok := base.WaitForStat(func() int64 {
		return base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsActive))
	}, startNumReplicationsActive)
	assert.True(t, ok)
	assert.Equal(t, startNumReplicationsActive, numReplicationsActive)

	// Verify total stat has not been decremented
	numReplicationsTotal = base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsTotal))
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

	ar, err := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:                    t.Name(),
		Direction:             db.ActiveReplicatorTypePush,
		ActiveDB:              &db.Database{DatabaseContext: rt.GetDatabase()},
		PassiveDBURL:          passiveDBURL,
		WebsocketPingInterval: time.Millisecond * 10,
	})
	require.NoError(t, err)

	assert.Equal(t, int64(0), base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count")))

	assert.NoError(t, ar.Start())

	time.Sleep(time.Millisecond * 50)

	pingGoroutines := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))
	assert.Equal(t, int64(1), pingGoroutines)

	pingCount := base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("sender_ping_count"))
	assert.Truef(t, pingCount > 0, "Expected ping count to be >0")

	assert.NoError(t, ar.Close())

	pingGoroutines = base.ExpvarVar2Int(expvar.Get("goblip").(*expvar.Map).Get("goroutines_sender_ping"))
	assert.Equal(t, int64(0), pingGoroutines)
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
	defer tb2.Close()
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
	defer tb1.Close()
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar, err := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    db.ActiveReplicatorTypePull,
		PassiveDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize: 200,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Close()) }()

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
}

// TestActiveReplicatorPullBasic:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt2 which can be pulled by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt2.
//   - Starts the pull replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPullFromCheckpoint(t *testing.T) {

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
		ID:           t.Name(),
		Direction:    db.ActiveReplicatorTypePull,
		PassiveDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:       false,
		ChangesBatchSize: changesBatchSize,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval: time.Minute * 5,
	}

	// Create the first active replicator to pull from seq:0
	ar, err := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	startNumChangesRequestedFromZeroTotal := base.ExpvarVar2Int(rt2.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsSinceZero))
	startNumRevsSentTotal := base.ExpvarVar2Int(rt2.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount))

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
	numChangesRequestedFromZeroTotal := base.ExpvarVar2Int(rt2.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsSinceZero))
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := base.ExpvarVar2Int(rt2.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount))
	assert.Equal(t, startNumRevsSentTotal+numRT2DocsInitial, numRevsSentTotal)
	assert.Equal(t, int64(numRT2DocsInitial), base.ExpvarVar2Int(ar.Pull.Stats.Get(db.ActiveReplicatorStatsKeyRevsReceivedTotal)))
	assert.Equal(t, int64(numRT2DocsInitial), base.ExpvarVar2Int(ar.Pull.Stats.Get(db.ActiveReplicatorStatsKeyChangesRevsReceivedTotal)))

	// checkpoint assertions
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatGetCheckpointHitTotal))
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatGetCheckpointMissTotal))
	// Since we bumped the checkpointer interval, we're only setting checkpoints on replicator close.
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatSetCheckpointTotal))
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatSetCheckpointTotal))

	assert.NoError(t, ar.Close())

	// Second batch of docs
	for i := numRT2DocsInitial; i < numRT2DocsTotal; i++ {
		resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	ar, err = db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Close()) }()
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
	endNumChangesRequestedFromZeroTotal := base.ExpvarVar2Int(rt2.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsSinceZero))
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure rt2 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = base.ExpvarVar2Int(rt2.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount))
	assert.Equal(t, startNumRevsSentTotal+numRT2DocsTotal, numRevsSentTotal)
	assert.Equal(t, int64(numRT2DocsTotal-numRT2DocsInitial), base.ExpvarVar2Int(ar.Pull.Stats.Get(db.ActiveReplicatorStatsKeyRevsReceivedTotal)))
	assert.Equal(t, int64(numRT2DocsTotal-numRT2DocsInitial), base.ExpvarVar2Int(ar.Pull.Stats.Get(db.ActiveReplicatorStatsKeyChangesRevsReceivedTotal)))

	// assert the second active replicator stats
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatGetCheckpointHitTotal))
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatGetCheckpointMissTotal))
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatSetCheckpointTotal))
	ar.Pull.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Pull.Checkpointer.StatSetCheckpointTotal))
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
	defer tb2.Close()
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
	defer tb1.Close()
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	docID := t.Name() + "rt1doc1"
	resp := rt1.SendAdminRequest(http.MethodPut, "/db/"+docID, `{"source":"rt1","channels":["alice"]}`)
	assertStatus(t, resp, http.StatusCreated)
	revID := respRevID(t, resp)

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")

	ar, err := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    db.ActiveReplicatorTypePush,
		PassiveDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize: 200,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Close()) }()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

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

// TestActiveReplicatorPushFromCheckpoint:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Creates enough documents on rt1 which can be pushed by a replicator running in rt1 to start setting checkpoints.
//   - Insert the second batch of docs into rt1.
//   - Starts the push replication again and asserts that the checkpoint is used.
func TestActiveReplicatorPushFromCheckpoint(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyReplicate, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)()

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
		ID:           t.Name(),
		Direction:    db.ActiveReplicatorTypePush,
		PassiveDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:       false,
		ChangesBatchSize: changesBatchSize,
		// test isn't long running enough to worry about time-based checkpoints,
		// to keep testing simple, bumped these up for deterministic checkpointing via CheckpointNow()
		CheckpointInterval: time.Minute * 5,
	}

	// Create the first active replicator to pull from seq:0
	ar, err := db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)

	startNumChangesRequestedFromZeroTotal := base.ExpvarVar2Int(rt1.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsSinceZero))
	startNumRevsSentTotal := base.ExpvarVar2Int(rt1.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount))

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
	numChangesRequestedFromZeroTotal := base.ExpvarVar2Int(rt1.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsSinceZero))
	assert.Equal(t, startNumChangesRequestedFromZeroTotal+1, numChangesRequestedFromZeroTotal)

	// rev assertions
	numRevsSentTotal := base.ExpvarVar2Int(rt1.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount))
	assert.Equal(t, startNumRevsSentTotal+numRT1DocsInitial, numRevsSentTotal)
	assert.Equal(t, int64(numRT1DocsInitial), base.ExpvarVar2Int(ar.Push.Stats.Get(db.ActiveReplicatorStatsKeyRevsSentTotal)))
	assert.Equal(t, int64(numRT1DocsInitial), base.ExpvarVar2Int(ar.Push.Stats.Get(db.ActiveReplicatorStatsKeyRevsRequestedTotal)))

	// checkpoint assertions
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Push.Checkpointer.StatGetCheckpointHitTotal))
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Push.Checkpointer.StatGetCheckpointMissTotal))
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Push.Checkpointer.StatSetCheckpointTotal))
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Push.Checkpointer.StatSetCheckpointTotal))

	assert.NoError(t, ar.Close())

	// Second batch of docs
	for i := numRT1DocsInitial; i < numRT1DocsTotal; i++ {
		resp := rt1.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"source":"rt1","channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
	}

	// Create a new replicator using the same config, which should use the checkpoint set from the first.
	ar, err = db.NewActiveReplicator(&arConfig)
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Close()) }()
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
	endNumChangesRequestedFromZeroTotal := base.ExpvarVar2Int(rt1.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsSinceZero))
	assert.Equal(t, numChangesRequestedFromZeroTotal, endNumChangesRequestedFromZeroTotal)

	// make sure rt1 thinks it has sent all of the revs via a 2.x replicator
	numRevsSentTotal = base.ExpvarVar2Int(rt1.GetDatabase().DbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount))
	assert.Equal(t, startNumRevsSentTotal+numRT1DocsTotal, numRevsSentTotal)
	assert.Equal(t, int64(numRT1DocsTotal-numRT1DocsInitial), base.ExpvarVar2Int(ar.Push.Stats.Get(db.ActiveReplicatorStatsKeyRevsSentTotal)))
	assert.Equal(t, int64(numRT1DocsTotal-numRT1DocsInitial), base.ExpvarVar2Int(ar.Push.Stats.Get(db.ActiveReplicatorStatsKeyRevsRequestedTotal)))

	// assert the second active replicator stats
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Push.Checkpointer.StatGetCheckpointHitTotal))
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Push.Checkpointer.StatGetCheckpointMissTotal))
	assert.Equal(t, int64(0), base.ExpvarVar2Int(ar.Push.Checkpointer.StatSetCheckpointTotal))
	ar.Push.Checkpointer.CheckpointNow()
	assert.Equal(t, int64(1), base.ExpvarVar2Int(ar.Push.Checkpointer.StatSetCheckpointTotal))
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
	defer tb2.Close()
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
	defer tb1.Close()
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar, err := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    db.ActiveReplicatorTypePull,
		PassiveDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize: 200,
		Continuous:       true,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Close()) }()

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
	defer tb2.Close()
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
	defer tb1.Close()
	rt1 := NewRestTester(t, &RestTesterConfig{
		TestBucket: tb1,
	})
	defer rt1.Close()

	ar, err := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    db.ActiveReplicatorTypePull,
		PassiveDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize: 200,
		Continuous:       true,
		PurgeOnRemoval:   true,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Close()) }()

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
			tb2 := base.GetTestBucket(t)
			defer tb2.Close()
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
			defer tb1.Close()
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
			ar, err := db.NewActiveReplicator(&db.ActiveReplicatorConfig{
				ID:           t.Name(),
				Direction:    db.ActiveReplicatorTypePull,
				PassiveDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize: 200,
				ConflictResolver: customConflictResolver,
			})
			require.NoError(t, err)
			defer func() { assert.NoError(t, ar.Close()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start())

			// TODO: Use replication stats to wait for replication to complete
			time.Sleep(1 * time.Second)
			log.Printf("========================Replication should be done, checking with changes")
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
