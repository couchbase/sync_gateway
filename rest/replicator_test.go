package rest

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/replicator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestActiveReplicatorBlipsync starts a RestTester, and publishes the REST API on a httptest server, which the ActiveReplicator can call blipsync on.
func TestActiveReplicatorBlipsync(t *testing.T) {
	defer base.SetUpTestLogging(base.LevelTrace, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg, base.KeyReplicate)()

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

	targetDB, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	targetDB.User = url.UserPassword("alice", "pass")

	bar, err := replicator.NewBidirectionalActiveReplicator(context.Background(), &replicator.ActiveReplicatorConfig{
		ID:        t.Name(),
		Direction: replicator.ActiveReplicatorTypePushAndPull,
		TargetDB:  targetDB,
	})
	require.NoError(t, err)

	startNumReplicationsTotal := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsTotal))
	startNumReplicationsActive := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsActive))

	// Start the replicator (implicit connect)
	assert.NoError(t, bar.Start())

	// Check total stat
	numReplicationsTotal := base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsTotal))
	assert.Equal(t, startNumReplicationsTotal+1, numReplicationsTotal)

	// Check active stat
	assert.Equal(t, startNumReplicationsActive+1, base.ExpvarVar2Int(rt.GetDatabase().DbStats.StatsDatabase().Get(base.StatKeyNumReplicationsActive)))

	// Close the replicator (implicit disconnect)
	assert.NoError(t, bar.Close())

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
