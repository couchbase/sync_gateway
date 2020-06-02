package rest

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/replicator"
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

	ar, err := replicator.NewActiveReplicator(context.Background(), &replicator.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    replicator.ActiveReplicatorTypePushAndPull,
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
	assert.Equal(t, startNumReplicationsTotal+1, numReplicationsTotal)

	// Check active stat
	assert.Equal(t, startNumReplicationsActive+1, base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsActive)))

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
	assert.Equal(t, startNumReplicationsTotal+1, numReplicationsTotal)
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

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeyChanges)()

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

	ar, err := replicator.NewActiveReplicator(context.Background(), &replicator.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    replicator.ActiveReplicatorTypePull,
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
//   - Creates a document on rt2 which can be pulled by the replicator running in rt1.
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to pull changes from rt2.
//   - SetCheckpoint on the active replicator.
//   - Starts the pull replication again.
func TestActiveReplicatorPullFromCheckpoint(t *testing.T) {

	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skipf("test requires at least 2 usable test buckets")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyAll)()

	const (
		numRT2Docs         = 25
		checkpointInterval = 10
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

	docIDPrefix := t.Name() + "rt2doc"
	for i := 0; i < numRT2Docs; i++ {
		resp := rt2.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s%d", docIDPrefix, i), `{"source":"rt2","channels":["alice"]}`)
		assertStatus(t, resp, http.StatusCreated)
	}

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

	ar, err := replicator.NewActiveReplicator(context.Background(), &replicator.ActiveReplicatorConfig{
		ID:           t.Name(),
		Direction:    replicator.ActiveReplicatorTypePull,
		PassiveDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:   5,
		CheckpointInterval: checkpointInterval,
	})
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Close()) }()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start())

	// wait for all of the documents originally written to rt2 to arrive at rt1
	changesResults, err := rt1.WaitForChanges(numRT2Docs, "/db/_changes?since=0", "", true)
	require.NoError(t, err)

	require.Len(t, changesResults.Results, numRT2Docs)

	docIDsSeen := make(map[string]bool, numRT2Docs)
	for _, result := range changesResults.Results {
		docIDsSeen[result.ID] = true
	}

	for i := 0; i < numRT2Docs; i++ {
		docID := fmt.Sprintf("%s%d", docIDPrefix, i)
		assert.True(t, docIDsSeen[docID])

		doc, err := rt1.GetDatabase().GetDocument(docID, db.DocUnmarshalAll)
		assert.NoError(t, err)
		assert.Equal(t, "rt2", doc.GetDeepMutableBody()["source"])
	}
}
