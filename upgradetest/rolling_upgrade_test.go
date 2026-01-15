package upgradetest

import (
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

// TestRollingUpgradeWithCRUD tests rolling upgrades with CRUD operations between specified versions using Docker Tags.
// If tag is "main", it uses the current branch of Sync Gateway.
func TestRollingUpgradeWithCRUD(t *testing.T) {
	tests := []struct {
		from string
		to   string
	}{
		{"3.3.2-enterprise", "4.0.2-enterprise"},
	}

	for _, test := range tests {
		t.Run(test.from+" -> "+test.to, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

			ctx := t.Context()

			// get shared bucket
			b := base.GetTestBucket(t)
			defer b.Close(ctx)

			bootstrap := bootstrapInfoFromTestBucket(b)

			fromNode := newSyncGatewayNode(ctx, t, test.from, &bootstrap)
			t.Cleanup(func() {
				// allow in-flight requests to drain
				time.Sleep(time.Millisecond * 500)
				fromNode.Close(ctx)
			})

			// create database on fromNode
			resp := fromNode.sendAdminRequest(http.MethodPut, "/db/", []byte(`
{
	"bucket": "`+b.BucketSpec.BucketName+`",
	"index": {
		"num_replicas": 0
	}
}`))
			require.Equal(t, http.StatusCreated, resp.StatusCode)

			// start running concurrent CRUD operations against fromNode on a set of documents
			// will cause test failures if any unexpected errors occur
			startCRUD(ctx, t, fromNode)

			// allow doc operations on fromNode to run a bit before upgrade
			time.Sleep(2 * time.Second)

			// start up new version
			toNode := newSyncGatewayNode(ctx, t, test.to, &bootstrap)
			t.Cleanup(func() {
				// allow in-flight requests to drain
				time.Sleep(time.Millisecond * 500)
				toNode.Close(ctx)
			})

			// database should already be loaded as a persistent one on startup
			resp = toNode.sendAdminRequest(http.MethodGet, "/db/", nil)
			require.Equal(t, http.StatusOK, resp.StatusCode)

			startCRUD(ctx, t, toNode)

			// let the CRUD loops run a bit longer
			time.Sleep(time.Second * 2)
		})
	}
}
