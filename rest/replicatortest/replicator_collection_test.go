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
	"strconv"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

// TestActiveReplicatorMultiCollection is a test to get as much coverage of collections in ISGR as possible.
// Other tests can be more targeted if necessary for quicker/easier regression diagnosis.
// Summary:
//   - Starts 2 RestTesters, one active, and one passive each with a set of 3 collections.
//   - Creates documents on both sides in all collections, with a range of channels, with some identifying information in the document bodies.
//   - Uses an ActiveReplicator configured for push and pull to ensure that all documents are replicated both ways as expected.
//   - The replicator only replicates two of three collections, so we also check that we're filtering as expected.
//   - The replicator also remaps local collections to different ones on the remote.
//   - The replicator also filters to a subset of channels for each of the replicated collections.
func TestActiveReplicatorMultiCollection(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)
	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp, base.KeySync, base.KeySyncMsg, base.KeyReplicate)

	const (
		rt1DbName            = "rt1_active"
		rt2DbName            = "rt2_passive"
		numCollections       = 3
		numDocsPerCollection = 9 // 3 in each channel (a, b, c)
	)
	channels := []string{"a", "b", "c"}

	base.RequireNumTestDataStores(t, numCollections)

	// rt2 passive
	rt2 := rest.NewRestTesterMultipleCollections(t, &rest.RestTesterConfig{
		SyncFn: `function(doc) { channel(doc.chan) }`,
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
		SyncFn: `function(doc) { channel(doc.chan) }`,
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

	t.Logf("remapping active %q to passive %q", activeKeyspace1, passiveKeyspace2)
	t.Logf("not remapping active %q", activeKeyspace3)
	localCollections := []string{activeKeyspace1, activeKeyspace3}
	remoteCollections := []string{passiveKeyspace2, ""}
	t.Logf("not replicating active %q", activeKeyspace2)
	nonReplicatedLocalCollections := []string{activeKeyspace2}
	t.Logf("not replicating passive %q", passiveKeyspace1)
	nonReplicatedRemoteCollections := []string{passiveKeyspace1}

	collectionsFilter := [][]string{{"b"}, {"a", "c"}}
	t.Logf("filtering to channels: %q", collectionsFilter)

	var resp *rest.TestResponse

	// create docs in all collections
	for keyspaceNum := 1; keyspaceNum <= numCollections; keyspaceNum++ {
		for j := 1; j <= numDocsPerCollection; j++ {
			chanName := channels[j%len(channels)]
			resp = rt1.SendAdminRequest(http.MethodPut,
				fmt.Sprintf("/{{.keyspace%d}}/active-doc%d", keyspaceNum, j),
				fmt.Sprintf(`{"source":"active", "sourceKeyspace":"%s", "chan":"%s"}`,
					rt1Collections[keyspaceNum-1].ScopeName+"."+rt1Collections[keyspaceNum-1].Name, chanName))
			rest.RequireStatus(t, resp, http.StatusCreated)

			resp = rt2.SendAdminRequest(http.MethodPut,
				fmt.Sprintf("/{{.keyspace%d}}/passive-doc%d", keyspaceNum, j),
				fmt.Sprintf(`{"source":"passive", "sourceKeyspace":"%s", "chan":"%s"}`,
					rt2Collections[keyspaceNum-1].ScopeName+"."+rt2Collections[keyspaceNum-1].Name, chanName))
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

	// The mapping of `""` for activeKeyspace3 requires that these two collections are the same name.
	assert.Equal(t, activeKeyspace3, passiveKeyspace3)

	ctx1 := rt1.Context()
	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		ChangesBatchSize:         200,
		Continuous:               true,
		ReplicationStatsMap:      dbstats,
		CollectionsEnabled:       true,
		CollectionsLocal:         localCollections,
		CollectionsRemote:        remoteCollections,
		Filter:                   base.ByChannelFilter,
		CollectionsChannelFilter: collectionsFilter,
	})
	require.NoError(t, err)

	assert.Equal(t, "", ar.GetStatus(ctx1).LastSeqPull)

	// Start the replicator (implicit connect)
	require.NoError(t, ar.Start(ctx1))

	// check all expected docs were pushed and pulled
	for i, localCollection := range localCollections {
		remoteCollection := remoteCollections[i]
		if remoteCollection == "" {
			remoteCollection = localCollection
		}

		// local set
		expectedNumDocs := numDocsPerCollection
		if slices.Contains(nonReplicatedLocalCollections, localCollection) ||
			slices.Contains(nonReplicatedRemoteCollections, remoteCollection) {
			// only one set of docs for non-replicated collections
		} else {
			// plus docs for filtered channels
			expectedNumDocs += (numDocsPerCollection / len(channels)) * len(collectionsFilter[i])
		}

		localKeyspace := rt1DbName + "." + localCollection
		changes, err := rt1.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", localKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		remoteKeyspace := rt2DbName + "." + remoteCollection
		changes, err = rt2.WaitForChanges(expectedNumDocs, fmt.Sprintf("/%s/_changes", remoteKeyspace), "", true)
		require.NoError(t, err)
		assert.Len(t, changes.Results, expectedNumDocs)

		for j := 1; j <= numDocsPerCollection; j++ {
			expectedStatus := http.StatusOK
			if !slices.Contains(collectionsFilter[i], channels[j%len(channels)]) {
				// doc doesn't match channel filter
				expectedStatus = http.StatusNotFound
			}

			// check rt1 for passive docs (pull)
			resp = rt1.SendAdminRequest(http.MethodGet,
				fmt.Sprintf("/%s/passive-doc%d", localKeyspace, j), "")
			rest.AssertStatus(t, resp, expectedStatus)
			if resp.Code == http.StatusOK {
				var docBody db.Body
				require.NoError(t, docBody.Unmarshal(resp.BodyBytes()))
				assert.Equal(t, "passive", docBody["source"])
				assert.Equal(t, remoteCollection, docBody["sourceKeyspace"])
			}

			// check rt2 for active docs (push)
			resp = rt2.SendAdminRequest(http.MethodGet,
				fmt.Sprintf("/%s/active-doc%d", remoteKeyspace, j), "")
			rest.AssertStatus(t, resp, expectedStatus)
			if resp.Code == http.StatusOK {
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
			fmt.Sprintf(`{"source":"active", "sourceKeyspace":"%s", "chan":["a","b","c"]}`,
				rt1Collections[keyspaceNum-1].ScopeName+"."+rt1Collections[keyspaceNum-1].Name))
		rest.RequireStatus(t, resp, http.StatusCreated)

		resp = rt2.SendAdminRequest(http.MethodPut,
			fmt.Sprintf("/{{.keyspace%d}}/passive-doc%d", keyspaceNum, numDocsPerCollection+1),
			fmt.Sprintf(`{"source":"passive", "sourceKeyspace":"%s", "chan":["a","b","c"]}`,
				rt2Collections[keyspaceNum-1].ScopeName+"."+rt2Collections[keyspaceNum-1].Name))
		rest.RequireStatus(t, resp, http.StatusCreated)
	}

	require.NoError(t, rt1.WaitForPendingChanges())
	require.NoError(t, rt2.WaitForPendingChanges())

	err = ar.Start(ctx1)
	require.NoError(t, err)
	defer func() { assert.NoError(t, ar.Stop()) }()

	// check all expected docs were pushed and pulled
	for i, localCollection := range localCollections {
		remoteCollection := remoteCollections[i]
		if remoteCollection == "" {
			remoteCollection = localCollection
		}

		// local set
		expectedNumDocs := numDocsPerCollection + 1
		if slices.Contains(nonReplicatedLocalCollections, localCollection) ||
			slices.Contains(nonReplicatedRemoteCollections, remoteCollection) {
			// only one set of docs for non-replicated collections
		} else {
			// plus docs for filtered channels
			expectedNumDocs += 1 + (numDocsPerCollection/len(channels))*len(collectionsFilter[i])
		}

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

// TestActiveReplicatorMultiCollectionMismatchedLocalRemote ensures that local and remote lists must be the same length.
func TestActiveReplicatorMultiCollectionMismatchedLocalRemote(t *testing.T) {

	base.TestRequiresCollections(t)

	localCollections := []string{"ks1", "ks3"}
	remoteCollections := []string{"ks2"}

	activeRT, _, remoteDbURLString, teardown := rest.SetupSGRPeers(t)
	defer teardown()

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	passiveDBURL, err := url.Parse(remoteDbURLString)
	require.NoError(t, err)

	ctx1 := activeRT.Context()
	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  true,
		CollectionsLocal:    localCollections,
		CollectionsRemote:   remoteCollections,
	})
	require.NoError(t, err)

	err = ar.Start(ctx1)
	assert.ErrorContains(t, err, "local and remote collections must be the same length")
	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorMultiCollectionMissingRemote attempts to map to a missing remote collection.
func TestActiveReplicatorMultiCollectionMissingRemote(t *testing.T) {

	base.TestRequiresCollections(t)

	activeRT, _, remoteDbURLString, teardown := rest.SetupSGRPeers(t)
	defer teardown()

	localCollection := activeRT.GetSingleTestDatabaseCollection().ScopeName + "." + activeRT.GetSingleTestDatabaseCollection().Name
	localCollections := []string{localCollection}
	remoteCollections := []string{"missing.collection"}

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	passiveDBURL, err := url.Parse(remoteDbURLString)
	require.NoError(t, err)

	ctx1 := activeRT.Context()
	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  true,
		CollectionsLocal:    localCollections,
		CollectionsRemote:   remoteCollections,
	})
	require.NoError(t, err)

	err = ar.Start(ctx1)
	assert.ErrorContains(t, err, "peer does not have collection")
	assert.NoError(t, ar.Stop())
}

// TestActiveReplicatorMultiCollectionMissingLocal attempts to use a missing local collection.
func TestActiveReplicatorMultiCollectionMissingLocal(t *testing.T) {

	base.TestRequiresCollections(t)

	activeRT, passiveRT, remoteDbURLString, teardown := rest.SetupSGRPeers(t)
	defer teardown()

	localCollection := activeRT.GetSingleTestDatabaseCollection().ScopeName + ".invalid"
	localCollections := []string{localCollection}
	remoteCollection := passiveRT.GetSingleTestDatabaseCollection().ScopeName + "." + passiveRT.GetSingleTestDatabaseCollection().Name
	remoteCollections := []string{remoteCollection}

	stats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false, nil, nil)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	passiveDBURL, err := url.Parse(remoteDbURLString)
	require.NoError(t, err)

	ctx1 := activeRT.Context()
	ar, err := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePushAndPull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		ReplicationStatsMap: dbstats,
		CollectionsEnabled:  true,
		CollectionsLocal:    localCollections,
		CollectionsRemote:   remoteCollections,
	})
	require.NoError(t, err)

	err = ar.Start(ctx1)
	assert.ErrorContains(t, err, "does not exist on this database")
	assert.NoError(t, ar.Stop())
}

func TestReplicatorMissingCollections(t *testing.T) {
	const numCollections = 2
	base.RequireNumTestDataStores(t, numCollections)
	base.RequireNumTestBuckets(t, 2)
	testCases := []struct {
		name       string
		direction  db.ActiveReplicatorDirection
		maxBackoff int
	}{
		{
			name:       "push replication,maxbackoff 0",
			direction:  db.ActiveReplicatorTypePush,
			maxBackoff: 0,
		},
		{
			name:       "pull replication, max backoff 0",
			direction:  db.ActiveReplicatorTypePull,
			maxBackoff: 0,
		},
		{
			name:       "push replication,maxbackoff 100",
			direction:  db.ActiveReplicatorTypePush,
			maxBackoff: 100,
		},
		{
			name:       "pull replication, max backoff 100",
			direction:  db.ActiveReplicatorTypePull,
			maxBackoff: 100,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			const username = "alice"
			passiveRT := rest.NewRestTester(t, &rest.RestTesterConfig{SgReplicateEnabled: true})
			defer passiveRT.Close()
			passiveRT.CreateUser(username, []string{"sg_test_0.sg_test_0", "sg_test_0.sg_test_1"})

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
			srv := httptest.NewServer(passiveRT.TestPublicHandler())
			defer srv.Close()

			// Build passiveDBURL with basic auth creds
			passiveDBURL, _ := url.Parse(srv.URL + "/" + passiveRT.GetDatabase().Name)
			passiveDBURL.User = url.UserPassword(username, rest.RestTesterDefaultUserPassword)

			activeRT := rest.NewRestTesterMultipleCollections(t, &rest.RestTesterConfig{SgReplicateEnabled: true}, numCollections)
			defer activeRT.Close()

			const numDocs = 1
			for i := 1; i <= numDocs; i++ {
				for _, keyspace := range []string{"{{.keyspace1}}", "{{.keyspace2}}"} {
					resp := activeRT.SendAdminRequest(http.MethodPut, fmt.Sprintf("/%s/active_doc_%d", keyspace, i), `{"foo": "bar"}`)
					rest.RequireStatus(t, resp, http.StatusCreated)
				}
				resp := passiveRT.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/passive_doc_%d", i), `{"foo": "bar"}`)
				rest.RequireStatus(t, resp, http.StatusCreated)

			}
			require.NoError(t, activeRT.WaitForPendingChanges())
			// use string since omitempty will occur for max_backoff_time, rendering test useless
			replicationID := strings.ReplaceAll(t.Name(), "/", "_")
			replicationConfig := `{
				"replication_id": "` + replicationID + `",
				"remote": "` + passiveDBURL.String() + `",
				"direction": "` + string(test.direction) + `",
				"collections_enabled": true,
				"max_backoff_time": ` + strconv.Itoa(test.maxBackoff) + `
			}`
			resp := activeRT.SendAdminRequest(http.MethodPost, "/{{.db}}/_replication/", replicationConfig)
			rest.RequireStatus(t, resp, http.StatusCreated)

			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateError)
		})
	}
}
