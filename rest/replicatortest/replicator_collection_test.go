// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

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
//   - Starts 2 RestTesters, one active, and one passive each with a set of 3 collections.
//   - Creates documents on both sides in all collections with identifying information in the document bodies.
//   - Uses an ActiveReplicator configured for push and pull to ensure that all documents are replicated both ways as expected.
//   - The replicator only replicates two of three collections, so we also check that we're filtering as expected.
//   - A future enhancement to the test can be made to ensure that the replication collections are remapped using collections_remote.
func TestActiveReplicatorMultiCollection(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.TestRequiresCollections(t)

	// TODO (bbrks): Remove logging
	base.SetUpTestLogging(t, base.LevelTrace, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg)

	const (
		rt1DbName            = "rt1_active"
		rt2DbName            = "rt2_passive"
		numCollections       = 3
		numDocsPerCollection = 3
	)

	base.RequireNumTestBuckets(t, 2)
	base.RequireNumTestDataStores(t, numCollections)
	base.TestRequiresCollections(t)

	// rt2 passive
	rt2 := rest.NewRestTesterMultipleCollections(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Name: rt2DbName,
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
				Name: rt1DbName,
			},
		}}, numCollections)
	defer rt1.Close()

	rt1Collections := rt1.GetDbCollections()
	require.Len(t, rt1Collections, 3)
	activeKeyspace1 := rt1Collections[0].ScopeName + "." + rt1Collections[0].Name
	activeKeyspace2 := rt1Collections[1].ScopeName + "." + rt1Collections[1].Name
	activeKeyspace3 := rt1Collections[2].ScopeName + "." + rt1Collections[2].Name

	// TODO CBG-2320: Force both sets of keyspaces to match (at least until mapping is implemented so we can support different ones)
	require.Equal(t, activeKeyspace1, passiveKeyspace1)
	require.Equal(t, activeKeyspace2, passiveKeyspace2)
	require.Equal(t, activeKeyspace3, passiveKeyspace3)

	var resp *rest.TestResponse
	// create docs in all collections

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

	passiveDBURL, err := url.Parse(srv.URL + "/" + rt2DbName)
	require.NoError(t, err)

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	localCollections := []string{activeKeyspace1, activeKeyspace3}
	nonReplicatedLocalCollections := []string{activeKeyspace2}
	remoteCollections := []string{passiveKeyspace2, ""}
	nonReplicatedRemoteCollections := []string{passiveKeyspace2} // TODO: Change to keyspace one when remapping is implemented

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
		CollectionsLocal:    localCollections,
		CollectionsRemote:   remoteCollections,
	})

	assert.Equal(t, "", ar.GetStatus().LastSeqPull)

	// Start the replicator (implicit connect)
	require.NoError(t, ar.Start(ctx1))

	// check all expected docs were pushed and pulled
	for _, localCollection := range localCollections {
		// TODO: CBG-2320 remap local to remote
		// remoteCollection := remoteCollections[i]
		remoteCollection := localCollection

		// since we replicated a full collection of docs into another, expect double the amount in each now
		expectedNumDocs := numDocsPerCollection * 2

		localKeyspace := rt1DbName + "." + localCollection
		changes, err := rt1.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", localKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		remoteKeyspace := rt2DbName + "." + remoteCollection
		changes, err = rt2.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", remoteKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		for j := 1; j <= numDocsPerCollection; j++ {
			// check rt1 for passive docs (pull)
			resp = rt1.SendAdminRequest(http.MethodGet,
				fmt.Sprintf("/%s/passive-doc%d", localKeyspace, j), "")
			if rest.AssertStatus(t, resp, http.StatusOK) {
				var docBody db.Body
				require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
				assert.Equal(t, "passive", docBody["source"])
				assert.Equal(t, remoteCollection, docBody["sourceKeyspace"])
			}

			// check rt2 for active docs (push)
			resp = rt2.SendAdminRequest(http.MethodGet,
				fmt.Sprintf("/%s/active-doc%d", remoteKeyspace, j), "")
			if rest.AssertStatus(t, resp, http.StatusOK) {
				var docBody db.Body
				require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
				assert.Equal(t, "active", docBody["source"])
				assert.Equal(t, localCollection, docBody["sourceKeyspace"])
			}
		}
	}

	// Stop the replication
	err = ar.Stop()
	require.NoError(t, err)

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
	defer func() { assert.NoError(t, ar.Stop()) }()

	// check all expected docs were pushed and pulled
	for _, localCollection := range localCollections {
		// TODO: CBG-2320 remap local to remote
		// remoteCollection := remoteCollections[i]
		remoteCollection := localCollection

		// since we replicated a full collection of docs into another, expect double the amount in each now
		expectedNumDocs := (numDocsPerCollection + 1) * 2

		localKeyspace := rt1DbName + "." + localCollection
		changes, err := rt1.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", localKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		remoteKeyspace := rt2DbName + "." + remoteCollection
		changes, err = rt2.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", remoteKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		// check rt1 for passive docs (pull)
		resp = rt1.SendAdminRequest(http.MethodGet,
			fmt.Sprintf("/%s/passive-doc%d", localKeyspace, numDocsPerCollection+1), "")
		if rest.AssertStatus(t, resp, http.StatusOK) {
			var docBody db.Body
			require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
			assert.Equal(t, "passive", docBody["source"])
			assert.Equal(t, remoteCollection, docBody["sourceKeyspace"])
		}

		// check rt2 for active docs (push)
		resp = rt2.SendAdminRequest(http.MethodGet,
			fmt.Sprintf("/%s/active-doc%d", remoteKeyspace, numDocsPerCollection+1), "")
		if rest.AssertStatus(t, resp, http.StatusOK) {
			var docBody db.Body
			require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
			assert.Equal(t, "active", docBody["source"])
			assert.Equal(t, localCollection, docBody["sourceKeyspace"])
		}
	}

	// and check we _didn't_ replicate docs in the absent collection
	for _, localCollection := range nonReplicatedLocalCollections {

		expectedNumDocs := numDocsPerCollection + 1 // we created a set of 3 docs (plus one later), but didn't replicate any in or out of here...

		localKeyspace := rt1DbName + "." + localCollection
		changes, err := rt1.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", localKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)
	}
	for _, remoteCollection := range nonReplicatedRemoteCollections {

		expectedNumDocs := numDocsPerCollection + 1 // we created a set of 3 docs (plus one later), but didn't replicate any in or out of here...

		remoteKeyspace := rt2DbName + "." + remoteCollection
		changes, err := rt2.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", remoteKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)
	}
}
