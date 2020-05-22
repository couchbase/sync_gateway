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

	// 2 single-node clusters:
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

	assert.NoError(t, bar.Connect())
	assert.NoError(t, bar.Start())
	assert.NoError(t, bar.Close())
}
