package rest

import (
	"context"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/replicator"
	"github.com/stretchr/testify/require"
)

func TestSGR2Pull(t *testing.T) {
	if base.GTestBucketPool.NumUsableBuckets() < 2 {
		t.Skip("need at least 2 usable buckets for this test")
	}

	defer base.SetUpTestLogging(base.LevelTrace, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg, base.KeyReplicate)()

	// 2 single-node clusters:
	rt1 := NewRestTester(t, &RestTesterConfig{DatabaseConfig: &DbConfig{
		// active replicator config
	}})
	defer rt1.Close()
	rt2 := NewRestTester(t, &RestTesterConfig{DatabaseConfig: &DbConfig{
		// passive node config
	}})
	defer rt2.Close()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt2.TestAdminHandler())
	defer srv.Close()

	targetDB, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	bar, err := replicator.NewBidirectionalActiveReplicator(context.Background(), &replicator.ActiveReplicatorConfig{
		ID:        t.Name(),
		Direction: replicator.ActiveReplicatorTypePushAndPull,
		TargetDB:  targetDB,
	})
	require.NoError(t, err)
	defer bar.Close()

	require.NoError(t, bar.Connect())
	require.NoError(t, bar.Start())
}
