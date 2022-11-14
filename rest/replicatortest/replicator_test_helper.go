package replicatortest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbaselabs/walrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper functions for SGR testing

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
func setupSGRPeers(t *testing.T) (activeRT *rest.RestTester, passiveRT *rest.RestTester, remoteDBURLString string, teardown func()) {
	// Set up passive RestTester (rt2)
	passiveTestBucket := base.GetTestBucket(t)
	passiveRT = rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: passiveTestBucket.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {
					Password:         base.StringPtr("pass"),
					ExplicitChannels: base.SetOf("*"),
				},
			},
		}},
	})
	// Initalize RT and bucket
	_ = passiveRT.Bucket()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
	srv := httptest.NewServer(passiveRT.TestPublicHandler())

	// Build passiveDBURL with basic auth creds
	passiveDBURL, _ := url.Parse(srv.URL + "/db")
	passiveDBURL.User = url.UserPassword("alice", "pass")

	// Set up active RestTester (rt1)
	activeTestBucket := base.GetTestBucket(t)
	activeRT = rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket:   activeTestBucket.NoCloseClone(),
		SgReplicateEnabled: true,
	})
	// Initalize RT and bucket
	_ = activeRT.Bucket()

	teardown = func() {
		activeRT.Close()
		activeTestBucket.Close()
		srv.Close()
		passiveRT.Close()
		passiveTestBucket.Close()
	}
	return activeRT, passiveRT, passiveDBURL.String(), teardown
}

func reduceTestCheckpointInterval(interval time.Duration) func() {
	previousInterval := db.DefaultCheckpointInterval
	db.DefaultCheckpointInterval = interval
	return func() {
		db.DefaultCheckpointInterval = previousInterval
	}

}

// AddActiveRT returns a new RestTester backed by a no-close clone of TestBucket
func addActiveRT(t *testing.T, testBucket *base.TestBucket) (activeRT *rest.RestTester) {

	// Create a new rest tester, using a NoCloseClone of testBucket, which disables the TestBucketPool teardown
	activeRT = rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket:   testBucket.NoCloseClone(),
		SgReplicateEnabled: true,
	})

	// If this is a walrus bucket, we need to jump through some hoops to ensure the shared in-memory walrus bucket isn't
	// deleted when bucket.Close() is called during DatabaseContext.Close().
	// Using IgnoreClose in leakyBucket to no-op the close operation.
	// Because RestTester has Sync Gateway create the database context and bucket based on the bucketSpec, we can't
	// set up the leakyBucket wrapper prior to bucket creation.
	// Instead, we need to modify the leaky bucket config (created for vbno handling) after the fact.
	leakyBucket, ok := activeRT.GetDatabase().Bucket.(*base.LeakyBucket)
	if ok {
		underlyingBucket := leakyBucket.GetUnderlyingBucket()
		if _, ok := underlyingBucket.(*walrus.WalrusBucket); ok {
			leakyBucket.SetIgnoreClose(true)
		}
	}

	return activeRT
}

// requireRevID asserts that the specified document revision is written to the
// underlying bucket backed by the given RestTester instance.
func requireRevID(t *testing.T, rt *rest.RestTester, docID, revID string) {
	doc, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	require.NoError(t, err, "Error reading document from bucket")
	require.Equal(t, revID, doc.SyncData.CurrentRev)
}
func requireErrorKeyNotFound(t *testing.T, rt *rest.RestTester, docID string) {
	var body []byte
	_, err := rt.Bucket().Get(docID, &body)
	require.Error(t, err)
	require.True(t, base.IsKeyNotFoundError(rt.Bucket(), err))
}

// waitForTombstone waits until the specified tombstone revision is available
// in the bucket backed by the specified RestTester instance.
func waitForTombstone(t *testing.T, rt *rest.RestTester, docID string) {
	require.NoError(t, rt.WaitForPendingChanges())
	require.NoError(t, rt.WaitForCondition(func() bool {
		doc, _ := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		return doc.IsDeleted() && len(doc.Body()) == 0
	}))
}

// createOrUpdateDoc creates a new document or update an existing document with the
// specified document id, revision id and body value in a channel named "alice".
func createOrUpdateDoc(t *testing.T, rt *rest.RestTester, docID, revID, bodyValue string) string {
	body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
	dbURL := "/db/" + docID
	if revID != "" {
		dbURL = "/db/" + docID + "?rev=" + revID
	}
	resp := rt.SendAdminRequest(http.MethodPut, dbURL, body)
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())
	return rest.RespRevID(t, resp)
}
func getTestRevpos(t *testing.T, doc db.Body, attachmentKey string) (revpos int) {
	attachments := db.GetBodyAttachments(doc)
	if attachments == nil {
		return 0
	}
	attachment, ok := attachments[attachmentKey].(map[string]interface{})
	assert.True(t, ok)
	if !ok {
		return 0
	}
	revposInt64, ok := base.ToInt64(attachment["revpos"])
	assert.True(t, ok)
	return int(revposInt64)
}
