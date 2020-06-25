package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

	ar, err := db.NewActiveReplicator(context.Background(), &db.ActiveReplicatorConfig{
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

	ar, err := db.NewActiveReplicator(context.Background(), &db.ActiveReplicatorConfig{
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
	ar, err := db.NewActiveReplicator(context.Background(), &arConfig)
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
	ar, err = db.NewActiveReplicator(context.Background(), &arConfig)
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

	ar, err := db.NewActiveReplicator(context.Background(), &db.ActiveReplicatorConfig{
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
	ar, err := db.NewActiveReplicator(context.Background(), &arConfig)
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
	ar, err = db.NewActiveReplicator(context.Background(), &arConfig)
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
