package replicatortest

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestActiveReplicatorMultiCollection is a test to get as much coverage of collections in ISGR as possible.
// Other tests can be more targeted if necessary for quicker/easier regression diagnosis.
// Summary:
//   - Starts 2 RestTesters, one active, and one passive each with a set of collections.
//   - Creates documents on both sides in all collections with identifying information in the document bodies.
//   - Uses an ActiveReplicator configured for push and pull to ensure that all documents are replicated both ways as expected.
func TestActiveReplicatorMultiCollection(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.TestRequiresCollections(t)

	// TODO (bbrks): Remove logging
	base.SetUpTestLogging(t, base.LevelTrace, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		numCollections       = 3
		numDocsPerCollection = 3
	)

	// rt2 passive
	rt2 := rest.NewRestTesterMultipleCollections(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "rt2_passive",
			},
		}}, numCollections)
	defer rt2.Close()

	rt2Collections := rt2.GetDbCollections()
	require.Len(t, rt2Collections, 3)
	passiveKeyspace1 := rt2Collections[0].ScopeName + "." + rt2Collections[0].Name
	passiveKeyspace2 := rt2Collections[1].ScopeName + "." + rt2Collections[1].Name
	passiveKeyspace3 := rt2Collections[2].ScopeName + "." + rt2Collections[2].Name

	// rt1 active
	rt1 := rest.NewRestTesterMultipleCollections(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: "rt1_active",
			},
		}}, numCollections)
	defer rt1.Close()

	rt1Collections := rt1.GetDbCollections()
	require.Len(t, rt1Collections, 3)
	activeKeyspace1 := rt1Collections[0].ScopeName + "." + rt1Collections[0].Name
	activeKeyspace2 := rt1Collections[1].ScopeName + "." + rt1Collections[1].Name
	activeKeyspace3 := rt1Collections[2].ScopeName + "." + rt1Collections[2].Name

	// TODO CBG-2319 CBG-2320: Force both sets of keyspaces to match (at least until mapping is implemented)
	assert.Equal(t, activeKeyspace1, passiveKeyspace1)
	assert.Equal(t, activeKeyspace2, passiveKeyspace2)
	assert.Equal(t, activeKeyspace3, passiveKeyspace3)

	var resp *rest.TestResponse
	// create docs
	for keyspaceNum := 1; keyspaceNum <= numCollections; keyspaceNum++ {
		for j := 1; j <= numDocsPerCollection; j++ {
			resp = rt1.SendAdminRequest(http.MethodPut,
				fmt.Sprintf("/{{.keyspace%d}}/active-doc%d", keyspaceNum, j),
				fmt.Sprintf(`{"source":"active", "sourceKeyspace":"%s"}`,
					rt1Collections[keyspaceNum-1].ScopeName+"."+rt1Collections[keyspaceNum-1].Name))
			rest.RequireStatus(t, resp, http.StatusCreated)

			resp = rt2.SendAdminRequest(http.MethodPut,
				fmt.Sprintf("/{{.keyspace%d}}/passive-doc%d", keyspaceNum, j),
				fmt.Sprintf(`{"source":"passive", "sourceKeyspace":"%s"}`,
					rt2Collections[keyspaceNum-1].ScopeName+"."+rt2Collections[keyspaceNum-1].Name))
			rest.RequireStatus(t, resp, http.StatusCreated)
		}
	}

	require.NoError(t, rt1.WaitForPendingChanges())
	require.NoError(t, rt2.WaitForPendingChanges())

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewServer(rt2.TestAdminHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/" + rt2.GetDatabase().Name)
	require.NoError(t, err)

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ctx1 := rt1.Context()
	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:    200,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  true,
		// TODO: CBG-2319 - Enable once filtering is implemented
		// KeyspaceMap: map[string]string{
		// 	activeKeyspace1: passiveKeyspace2,
		// 	activeKeyspace2: passiveKeyspace3,
		// },
	})

	assert.Equal(t, "", ar.GetStatus().LastSeqPull)

	// Start the replicator (implicit connect)
	require.NoError(t, ar.Start(ctx1))
	defer func() { assert.NoError(t, ar.Stop()) }()

	// check all docs were pushed and pulled
	for keyspaceNum := 1; keyspaceNum <= numCollections; keyspaceNum++ {
		// since we replicated a full collection of docs into another, expect double the amount in each now
		expectedNumDocs := numDocsPerCollection * 2

		changes, err := rt1.WaitForChanges(expectedNumDocs, fmt.Sprintf("/{{.keyspace%d}}/_changes", keyspaceNum), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		changes, err = rt2.WaitForChanges(expectedNumDocs, fmt.Sprintf("/{{.keyspace%d}}/_changes", keyspaceNum), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		for j := 1; j <= numDocsPerCollection; j++ {
			// check rt1 for passive docs (pull)
			resp = rt1.SendAdminRequest(http.MethodGet,
				fmt.Sprintf("/{{.keyspace%d}}/passive-doc%d", keyspaceNum, j), "")
			if rest.AssertStatus(t, resp, http.StatusOK) {
				var docBody db.Body
				require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
				assert.Equal(t, "passive", docBody["source"])
				assert.Equal(t, rt2Collections[keyspaceNum-1].ScopeName+"."+rt2Collections[keyspaceNum-1].Name, docBody["sourceKeyspace"])
			}

			// check rt2 for active docs (push)
			resp = rt2.SendAdminRequest(http.MethodGet,
				fmt.Sprintf("/{{.keyspace%d}}/active-doc%d", keyspaceNum, j), "")
			if rest.AssertStatus(t, resp, http.StatusOK) {
				var docBody db.Body
				require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
				assert.Equal(t, "active", docBody["source"])
				assert.Equal(t, rt1Collections[keyspaceNum-1].ScopeName+"."+rt1Collections[keyspaceNum-1].Name, docBody["sourceKeyspace"])
			}
		}
	}

	// Stop the replication
	err = ar.Stop()
	require.NoError(t, err)

	// These stats aren't reliable as the push and pull replications race each other in the test and interfere
	// the stat can range from total docs up to 2x total docs if one replicator pulls the other's changes
	checkedPushBefore := ar.GetStatus().DocsCheckedPush
	assert.GreaterOrEqual(t, checkedPushBefore, int64(numDocsPerCollection*numCollections))
	assert.LessOrEqual(t, checkedPushBefore, int64(numDocsPerCollection*numCollections*2))

	checkedPullBefore := ar.GetStatus().DocsCheckedPull
	assert.GreaterOrEqual(t, checkedPullBefore, int64(numDocsPerCollection*numCollections))
	assert.LessOrEqual(t, checkedPullBefore, int64(numDocsPerCollection*numCollections*2))

	//  create one more doc on each collection and make sure we're able to resume from the checkpoint.
	for keyspaceNum := 1; keyspaceNum <= numCollections; keyspaceNum++ {
		resp = rt1.SendAdminRequest(http.MethodPut,
			fmt.Sprintf("/{{.keyspace%d}}/active-doc%d", keyspaceNum, numDocsPerCollection+1),
			fmt.Sprintf(`{"source":"active", "sourceKeyspace":"%s"}`,
				rt1Collections[keyspaceNum-1].ScopeName+"."+rt1Collections[keyspaceNum-1].Name))
		rest.RequireStatus(t, resp, http.StatusCreated)

		resp = rt2.SendAdminRequest(http.MethodPut,
			fmt.Sprintf("/{{.keyspace%d}}/passive-doc%d", keyspaceNum, numDocsPerCollection+1),
			fmt.Sprintf(`{"source":"passive", "sourceKeyspace":"%s"}`,
				rt2Collections[keyspaceNum-1].ScopeName+"."+rt2Collections[keyspaceNum-1].Name))
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	require.NoError(t, rt1.WaitForPendingChanges())
	require.NoError(t, rt2.WaitForPendingChanges())

	err = ar.Start(ctx1)
	require.NoError(t, err)

	// check all docs were pushed and pulled
	for keyspaceNum := 1; keyspaceNum <= numCollections; keyspaceNum++ {
		// since we replicated a full collection of docs into another, expect double the amount in each now
		expectedNumDocs := (numDocsPerCollection + 1) * 2

		changes, err := rt1.WaitForChanges(expectedNumDocs, fmt.Sprintf("/{{.keyspace%d}}/_changes", keyspaceNum), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		changes, err = rt2.WaitForChanges(expectedNumDocs, fmt.Sprintf("/{{.keyspace%d}}/_changes", keyspaceNum), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		// check rt1 for passive docs (pull)
		resp = rt1.SendAdminRequest(http.MethodGet,
			fmt.Sprintf("/{{.keyspace%d}}/passive-doc%d", keyspaceNum, numDocsPerCollection+1), "")
		if rest.AssertStatus(t, resp, http.StatusOK) {
			var docBody db.Body
			require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
			assert.Equal(t, "passive", docBody["source"])
			assert.Equal(t, rt2Collections[keyspaceNum-1].ScopeName+"."+rt2Collections[keyspaceNum-1].Name, docBody["sourceKeyspace"])
		}

		// check rt2 for active docs (push)
		resp = rt2.SendAdminRequest(http.MethodGet,
			fmt.Sprintf("/{{.keyspace%d}}/active-doc%d", keyspaceNum, numDocsPerCollection+1), "")
		if rest.AssertStatus(t, resp, http.StatusOK) {
			var docBody db.Body
			require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
			assert.Equal(t, "active", docBody["source"])
			assert.Equal(t, rt1Collections[keyspaceNum-1].ScopeName+"."+rt1Collections[keyspaceNum-1].Name, docBody["sourceKeyspace"])
		}
	}

	checkedPush := ar.GetStatus().DocsCheckedPush
	checkedPull := ar.GetStatus().DocsCheckedPull

	// each push/pull replication should have pulled the same set of changes from each other (which is twice the number of total docs)
	assert.Equal(t, int64(numCollections*(numDocsPerCollection+1)*2), checkedPush)
	assert.Equal(t, int64(numCollections*(numDocsPerCollection+1)*2), checkedPull)

	// time.Sleep(time.Hour)

	// TODO CBG-2319: Ensure that ks3 was not pushed to passive, and only ks2 and ks3 were pulled
	// TODO CBG-2320: Ensure that KeyspaceMap is working by checking each doc has been replicated to the mapped collection.
}
