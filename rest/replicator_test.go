/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestActiveReplicatorBlipsync uses an ActiveReplicator with another RestTester instance to connect and cleanly disconnect.
func TestActiveReplicatorBlipsync(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
			},
		}},
	})
	defer rt.Close()
	ctx := rt.Context()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	srv := httptest.NewServer(rt.TestPublicHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	// Add basic auth creds to target db URL
	passiveDBURL.User = url.UserPassword("alice", "pass")
	stats, err := base.SyncGatewayStats.NewDBStats("test", false, false, false)
	require.NoError(t, err)
	dbstats, err := stats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx, &db.ActiveReplicatorConfig{
		ID:                  t.Name(),
		Direction:           db.ActiveReplicatorTypePushAndPull,
		ActiveDB:            &db.Database{DatabaseContext: rt.GetDatabase()},
		RemoteDBURL:         passiveDBURL,
		Continuous:          true,
		ReplicationStatsMap: dbstats,
	})

	startNumReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	startNumReplicationsActive := rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()

	// Start the replicator (implicit connect)
	assert.NoError(t, ar.Start(ctx))

	// Check total stat
	numReplicationsTotal := rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)

	// Check active stat
	assert.Equal(t, startNumReplicationsActive+2, rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value())

	// Close the replicator (implicit disconnect)
	assert.NoError(t, ar.Stop())

	// Wait for active stat to drop to original value
	numReplicationsActive, ok := base.WaitForStat(func() int64 {
		return rt.GetDatabase().DbStats.Database().NumReplicationsActive.Value()
	}, startNumReplicationsActive)
	assert.True(t, ok)
	assert.Equal(t, startNumReplicationsActive, numReplicationsActive)

	// Verify total stat has not been decremented
	numReplicationsTotal = rt.GetDatabase().DbStats.Database().NumReplicationsTotal.Value()
	assert.Equal(t, startNumReplicationsTotal+2, numReplicationsTotal)
}

func TestBlipSyncNonUpgradableConnection(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyHTTPResp)
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			Users: map[string]*auth.PrincipalConfig{
				"alice": {Password: base.StringPtr("pass")},
			},
		}},
	})
	defer rt.Close()

	// Make rt listen on an actual HTTP port, so it can receive the blipsync request.
	server := httptest.NewServer(rt.TestPublicHandler())
	defer server.Close()
	dbURL, err := url.Parse(server.URL + "/db/_blipsync")
	require.NoError(t, err)

	// Add basic auth credentials to target db URL
	dbURL.User = url.UserPassword("alice", "pass")
	request, err := http.NewRequest(http.MethodGet, dbURL.String(), nil)
	require.NoError(t, err, "Error creating new request")

	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err, "Error sending request")
	require.Equal(t, http.StatusUpgradeRequired, response.StatusCode)
}

// TestActiveReplicatorPullConflictReadWriteIntlProps:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Create the same document id with different content on rt1 and rt2
//   - Publishes the REST API on a httptest server for the passive node (so the active can connect to it)
//   - Uses an ActiveReplicator configured for pull to start pulling changes from rt2.
func TestActiveReplicatorPullConflictReadWriteIntlProps(t *testing.T) {

	base.LongRunningTest(t)

	createRevID := func(generation int, parentRevID string, body db.Body) string {
		rev, err := db.CreateRevID(generation, parentRevID, body)
		require.NoError(t, err, "Error creating revision")
		return rev
	}
	docExpiry := time.Now().Local().Add(time.Hour * time.Duration(4)).Format(time.RFC3339)

	// scenarios
	conflictResolutionTests := []struct {
		name                string
		commonAncestorRevID string
		localRevisionBody   db.Body
		localRevID          string
		remoteRevisionBody  db.Body
		remoteRevID         string
		conflictResolver    string
		expectedLocalBody   db.Body
		expectedLocalRevID  string
	}{
		{
			name: "mergeReadWriteIntlProps",
			localRevisionBody: db.Body{
				"source": "local",
			},
			localRevID: "1-a",
			remoteRevisionBody: db.Body{
				"source": "remote",
			},
			remoteRevID: "1-b",
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc.remoteDocId = conflict.RemoteDocument._id;
				mergedDoc.remoteRevId = conflict.RemoteDocument._rev;
				mergedDoc.localDocId = conflict.LocalDocument._id;
				mergedDoc.localRevId = conflict.LocalDocument._rev;
				mergedDoc._id = "foo";
				mergedDoc._rev = "2-c";
				mergedDoc._exp = 100;
				return mergedDoc;
			}`,
			expectedLocalBody: db.Body{
				db.BodyId:     "foo",
				db.BodyRev:    "2-c",
				db.BodyExpiry: json.Number("100"),
				"localDocId":  "mergeReadWriteIntlProps",
				"localRevId":  "1-a",
				"remoteDocId": "mergeReadWriteIntlProps",
				"remoteRevId": "1-b",
				"source":      "merged",
			},
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				db.BodyId:     "foo",
				db.BodyRev:    "2-c",
				db.BodyExpiry: json.Number("100"),
				"localDocId":  "mergeReadWriteIntlProps",
				"localRevId":  "1-a",
				"remoteDocId": "mergeReadWriteIntlProps",
				"remoteRevId": "1-b",
				"source":      "merged",
			}),
		},
		{
			name: "mergeReadWriteAttachments",
			localRevisionBody: map[string]interface{}{
				db.BodyAttachments: map[string]interface{}{
					"A": map[string]interface{}{
						"data": "QQo=",
					}},
				"source": "local",
			},
			localRevID: "1-a",
			remoteRevisionBody: map[string]interface{}{
				db.BodyAttachments: map[string]interface{}{
					"B": map[string]interface{}{
						"data": "Qgo=",
					}},
				"source": "remote",
			},
			remoteRevID: "1-b",
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				var mergedAttachments = new Object();

				dst = conflict.RemoteDocument._attachments;
				for (var key in dst) {
					mergedAttachments[key] = dst[key];
				}
				src = conflict.LocalDocument._attachments;
				for (var key in src) {
					mergedAttachments[key] = src[key];
				}
				mergedDoc._attachments = mergedAttachments;
				return mergedDoc;
			}`,
			expectedLocalBody: map[string]interface{}{
				"source": "merged",
			},
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				"source": "merged",
			}),
		},
		{
			name: "mergeReadIntlPropsLocalExpiry",
			localRevisionBody: db.Body{
				"source":      "local",
				db.BodyExpiry: docExpiry,
			},
			localRevID:         "1-a",
			remoteRevisionBody: db.Body{"source": "remote"},
			remoteRevID:        "1-b",
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc.localDocExp = conflict.LocalDocument._exp;
				return mergedDoc;
			}`,
			expectedLocalBody: db.Body{
				"localDocExp": docExpiry,
				"source":      "merged",
			},
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				"localDocExp": docExpiry,
				"source":      "merged",
			}),
		},
		{
			name: "mergeWriteIntlPropsExpiry",
			localRevisionBody: db.Body{
				"source":      "local",
				db.BodyExpiry: docExpiry,
			},
			localRevID: "1-a",
			remoteRevisionBody: db.Body{
				"source": "remote",
			},
			remoteRevID: "1-b",
			conflictResolver: fmt.Sprintf(`function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc._exp = %q;
				return mergedDoc;
			}`, docExpiry),
			expectedLocalBody: db.Body{
				db.BodyExpiry: docExpiry,
				"source":      "merged",
			},
			expectedLocalRevID: createRevID(2, "1-b", db.Body{
				db.BodyExpiry: docExpiry,
				"source":      "merged",
			}),
		},
		{
			name: "mergeReadIntlPropsDeletedWithLocalTombstone",
			localRevisionBody: db.Body{
				"source":       "local",
				db.BodyDeleted: true,
			},
			commonAncestorRevID: "1-a",
			localRevID:          "2-a",
			remoteRevisionBody: db.Body{
				"source": "remote",
			},
			remoteRevID: "2-b",
			conflictResolver: `function(conflict) {
				var mergedDoc = new Object();
				mergedDoc.source = "merged";
				mergedDoc.localDeleted = conflict.LocalDocument._deleted;
				return mergedDoc;
			}`,
			expectedLocalBody: db.Body{
				"localDeleted": true,
				"source":       "merged",
			},
			expectedLocalRevID: createRevID(3, "2-b", db.Body{
				"localDeleted": true,
				"source":       "merged",
			}),
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			base.RequireNumTestBuckets(t, 2)
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

			// Passive
			tb2 := base.GetTestBucket(t)

			rt2 := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket: tb2,
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					Users: map[string]*auth.PrincipalConfig{
						"alice": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("*"),
						},
					},
				}},
			})
			defer rt2.Close()

			// Create revision on rt2 (remote)
			docID := test.name
			if test.commonAncestorRevID != "" {
				_, err := rt2.PutDocumentWithRevID(docID, test.commonAncestorRevID, "", test.remoteRevisionBody)
				assert.NoError(t, err)
			}
			resp, err := rt2.PutDocumentWithRevID(docID, test.remoteRevID, test.commonAncestorRevID, test.remoteRevisionBody)
			assert.NoError(t, err)
			RequireStatus(t, resp, http.StatusCreated)
			rt2revID := RespRevID(t, resp)
			assert.Equal(t, test.remoteRevID, rt2revID)

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
				CustomTestBucket: tb1,
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			// Create revision on rt1 (local)
			if test.commonAncestorRevID != "" {
				_, err := rt1.PutDocumentWithRevID(docID, test.commonAncestorRevID, "", test.remoteRevisionBody)
				assert.NoError(t, err)
			}
			resp, err = rt1.PutDocumentWithRevID(docID, test.localRevID, test.commonAncestorRevID, test.localRevisionBody)
			assert.NoError(t, err)
			RequireStatus(t, resp, http.StatusCreated)
			rt1revID := RespRevID(t, resp)
			assert.Equal(t, test.localRevID, rt1revID)

			customConflictResolver, err := db.NewCustomConflictResolver(test.conflictResolver, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err)
			dbstats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
			require.NoError(t, err)
			replicationStats, err := dbstats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				ChangesBatchSize:     200,
				ConflictResolverFunc: customConflictResolver,
				Continuous:           true,
				ReplicationStatsMap:  replicationStats,
			})
			defer func() { assert.NoError(t, ar.Stop()) }()

			// Start the replicator (implicit connect)
			assert.NoError(t, ar.Start(ctx1))
			waitAndRequireCondition(t, func() bool { return ar.GetStatus().DocsRead == 1 })
			assert.Equal(t, 1, int(replicationStats.ConflictResolvedMergedCount.Value()))

			// Wait for the document originally written to rt2 to arrive at rt1.
			// Should end up as winner under default conflict resolution.
			changesResults, err := rt1.WaitForChanges(1, "/db/_changes?&since=0", "", true)
			require.NoError(t, err)
			require.Len(t, changesResults.Results, 1)
			assert.Equal(t, docID, changesResults.Results[0].ID)
			assert.Equal(t, test.expectedLocalRevID, changesResults.Results[0].Changes[0]["rev"])
			log.Printf("Changes response is %+v", changesResults)

			doc, err := rt1.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
			require.NoError(t, err)
			assert.Equal(t, test.expectedLocalRevID, doc.SyncData.CurrentRev)
			log.Printf("doc.Body(): %v", doc.Body())
			assert.Equal(t, test.expectedLocalBody, doc.Body())
			log.Printf("Doc %s is %+v", docID, doc)
			for revID, revInfo := range doc.SyncData.History {
				log.Printf("doc revision [%s]: %+v", revID, revInfo)
			}

			// Validate only one active leaf node remains after conflict resolution, and that all parents
			// of leaves have empty bodies
			activeCount := 0
			for _, revID := range doc.SyncData.History.GetLeaves() {
				revInfo, ok := doc.SyncData.History[revID]
				require.True(t, ok)
				if !revInfo.Deleted {
					activeCount++
				}
				if revInfo.Parent != "" {
					parentRevInfo, ok := doc.SyncData.History[revInfo.Parent]
					require.True(t, ok)
					assert.True(t, parentRevInfo.Body == nil)
				}
			}
			assert.Equal(t, 1, activeCount)
		})
	}
}

func TestSGR2TombstoneConflictHandling(t *testing.T) {
	base.LongRunningTest(t)

	base.RequireNumTestBuckets(t, 2)

	tombstoneTests := []struct {
		name               string
		longestBranchLocal bool
		resurrectLocal     bool
		sdkResurrect       bool
	}{

		{
			name:               "RemoteLongResurrectLocal",
			longestBranchLocal: false,
			resurrectLocal:     true,
			sdkResurrect:       false,
		},
		{
			name:               "LocalLongResurrectLocal",
			longestBranchLocal: true,
			resurrectLocal:     true,
			sdkResurrect:       false,
		},
		{
			name:               "RemoteLongResurrectRemote",
			longestBranchLocal: false,
			resurrectLocal:     false,
			sdkResurrect:       false,
		},
		{
			name:               "LocalLongResurrectRemote",
			longestBranchLocal: true,
			resurrectLocal:     false,
			sdkResurrect:       false,
		},

		{
			name:               "RemoteLongSDKResurrectLocal",
			longestBranchLocal: false,
			resurrectLocal:     true,
			sdkResurrect:       true,
		},
		{
			name:               "RemoteLongSDKResurrectRemote",
			longestBranchLocal: false,
			resurrectLocal:     false,
			sdkResurrect:       true,
		},
		{
			name:               "LocalLongSDKResurrectLocal",
			longestBranchLocal: true,
			resurrectLocal:     true,
			sdkResurrect:       true,
		},
		{
			name:               "LocalLongSDKResurrectRemote",
			longestBranchLocal: true,
			resurrectLocal:     false,
			sdkResurrect:       true,
		},
	}

	// requireTombstone validates tombstoned revision.
	requireTombstone := func(t *testing.T, bucket *base.TestBucket, docID string) {
		var rawBody db.Body
		_, err := bucket.Get(docID, &rawBody)
		if base.TestUseXattrs() {
			require.True(t, base.IsDocNotFoundError(err))
			require.Len(t, rawBody, 0)
		} else {
			require.NoError(t, err)
			require.Len(t, rawBody, 1)
			rawSyncData, ok := rawBody[base.SyncPropertyName].(map[string]interface{})
			require.True(t, ok)
			val, ok := rawSyncData["flags"].(float64)
			require.True(t, ok)
			require.NotEqual(t, 0, int(val)&channels.Deleted)
		}
	}

	compareDocRev := func(docRev, cmpRev string) (shouldRetry bool, err error, value interface{}) {
		docGen, docHash := db.ParseRevID(docRev)
		cmpGen, cmpHash := db.ParseRevID(cmpRev)
		if docGen == cmpGen {
			if docHash != cmpHash {
				return false, fmt.Errorf("rev generations match but hashes are different: %v, %v", docRev, cmpRev), nil
			}
			return false, nil, docRev
		}
		return true, nil, nil
	}

	maxAttempts := 200
	attemptSleepMs := 100

	for _, test := range tombstoneTests {

		t.Run(test.name, func(t *testing.T) {
			base.SetUpTestLogging(t, base.LevelDebug, base.KeyImport, base.KeyHTTP, base.KeySync, base.KeyChanges, base.KeyCRUD, base.KeyBucket, base.KeyReplicate)

			if test.sdkResurrect && !base.TestUseXattrs() {
				t.Skip("SDK resurrect test cases require xattrs to be enabled")
			}

			makeDoc := func(rt *RestTester, docid string, rev string, value string) string {
				var body db.Body
				resp := rt.SendAdminRequest("PUT", "/db/"+docid+"?rev="+rev, value)
				RequireStatus(t, resp, http.StatusCreated)
				err := json.Unmarshal(resp.BodyBytes(), &body)
				assert.NoError(t, err)
				return body["rev"].(string)
			}

			// Passive
			passiveBucket := base.GetTestBucket(t)
			remotePassiveRT := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket: passiveBucket,
			})
			defer remotePassiveRT.Close()

			srv := httptest.NewServer(remotePassiveRT.TestAdminHandler())
			defer srv.Close()

			// Active
			activeBucket := base.GetTestBucket(t)
			localActiveRT := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket:   activeBucket,
				SgReplicateEnabled: true,
			})
			defer localActiveRT.Close()

			replConf := `
			{
				"replication_id": "replication",
				"remote": "` + srv.URL + `/db",
				"direction": "pushAndPull",
				"continuous": true
			}`

			// Send up replication
			resp := localActiveRT.SendAdminRequest("PUT", "/db/_replication/replication", replConf)
			RequireStatus(t, resp, http.StatusCreated)

			// Create a doc with 3-revs
			resp = localActiveRT.SendAdminRequest("POST", "/db/_bulk_docs", `{"docs":[{"_id": "docid2", "_rev": "1-abc"}, {"_id": "docid2", "_rev": "2-abc", "_revisions": {"start": 2, "ids": ["abc", "abc"]}}, {"_id": "docid2", "_rev": "3-abc", "val":"test", "_revisions": {"start": 3, "ids": ["abc", "abc", "abc"]}}], "new_edits":false}`)
			RequireStatus(t, resp, http.StatusCreated)

			// Wait for the replication to be started
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

			// Wait for document to arrive on the doc is was put on
			err := localActiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, _ := localActiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if doc != nil {
					return compareDocRev(doc.SyncData.CurrentRev, "3-abc")
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			// Wait for document to be replicated
			err = remotePassiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, _ := remotePassiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if doc != nil {
					return compareDocRev(doc.SyncData.CurrentRev, "3-abc")
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			// Stop the replication
			_ = localActiveRT.SendAdminRequest("PUT", "/db/_replicationStatus/replication?action=stop", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateStopped)

			// Delete on the short branch and make another doc on the longer branch before deleting it
			if test.longestBranchLocal {
				// Delete doc on remote
				resp = remotePassiveRT.SendAdminRequest("PUT", "/db/docid2?rev=3-abc", `{"_deleted": true}`)
				RequireStatus(t, resp, http.StatusCreated)

				// Validate document revision created to prevent race conditions
				err = remotePassiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
					doc, docErr := remotePassiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					if assert.NoError(t, docErr) {
						if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "4-cc0337d9d38c8e5fc930ae3deda62bf8"); value != nil {
							requireTombstone(t, passiveBucket, "docid2")
						}
						return
					}
					return true, nil, nil
				}, maxAttempts, attemptSleepMs)
				assert.NoError(t, err)

				// Create another rev and then delete doc on local - ie tree is longer
				revid := makeDoc(localActiveRT, "docid2", "3-abc", `{"foo":"bar"}`)
				localActiveRT.DeleteDoc("docid2", revid)

				// Validate local is CBS tombstone, expect not found error
				// Expect KeyNotFound error retrieving local tombstone pre-replication
				requireTombstone(t, activeBucket, "docid2")

			} else {
				// Delete doc on localActiveRT (active / local)
				resp = localActiveRT.SendAdminRequest("PUT", "/db/docid2?rev=3-abc", `{"_deleted": true}`)
				RequireStatus(t, resp, http.StatusCreated)

				// Validate document revision created to prevent race conditions
				err = localActiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
					doc, docErr := localActiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					if assert.NoError(t, docErr) {
						if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "4-cc0337d9d38c8e5fc930ae3deda62bf8"); value != nil {
							requireTombstone(t, activeBucket, "docid2")
						}
						return
					}
					return true, nil, nil
				}, maxAttempts, attemptSleepMs)
				assert.NoError(t, err)

				// Create another rev and then delete doc on remotePassiveRT (passive) - ie, tree is longer
				revid := makeDoc(remotePassiveRT, "docid2", "3-abc", `{"foo":"bar"}`)
				remotePassiveRT.DeleteDoc("docid2", revid)

				// Validate local is CBS tombstone, expect not found error
				// Expect KeyNotFound error retrieving remote tombstone pre-replication
				requireTombstone(t, passiveBucket, "docid2")
			}

			// Start up repl again
			_ = localActiveRT.SendAdminRequest("PUT", "/db/_replicationStatus/replication?action=start", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

			// Wait for the recently longest branch to show up on both sides
			err = localActiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, docErr := localActiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if assert.NoError(t, docErr) {
					if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "5-4a5f5a35196c37c117737afd5be1fc9b"); value != nil {
						// Validate local is CBS tombstone, expect not found error
						// Expect KeyNotFound error retrieving local tombstone post-replication
						requireTombstone(t, activeBucket, "docid2")
					}
					return
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			err = remotePassiveRT.WaitForConditionShouldRetry(func() (shouldRetry bool, err error, value interface{}) {
				doc, docErr := remotePassiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
				if assert.NoError(t, docErr) {
					if shouldRetry, err, value = compareDocRev(doc.SyncData.CurrentRev, "5-4a5f5a35196c37c117737afd5be1fc9b"); value != nil {
						// Validate remote is CBS tombstone
						// Expect KeyNotFound error retrieving remote tombstone post-replication
						requireTombstone(t, passiveBucket, "docid2")
					}
					return
				}
				return true, nil, nil
			}, maxAttempts, attemptSleepMs)
			assert.NoError(t, err)

			// Stop the replication
			_ = localActiveRT.SendAdminRequest("PUT", "/db/_replicationStatus/replication?action=stop", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateStopped)

			// Resurrect Doc
			updatedBody := make(map[string]interface{})
			updatedBody["resurrection"] = true
			if test.resurrectLocal {
				if test.sdkResurrect {
					// resurrect doc via SDK on local
					err = activeBucket.Set("docid2", 0, nil, updatedBody)
					assert.NoError(t, err, "Unable to resurrect doc docid2")
					// force on-demand import
					_, getErr := localActiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					assert.NoError(t, getErr, "Unable to retrieve resurrected doc docid2")
				} else {
					resp = localActiveRT.SendAdminRequest("PUT", "/db/docid2", `{"resurrection": true}`)
					RequireStatus(t, resp, http.StatusCreated)
				}
			} else {
				if test.sdkResurrect {
					// resurrect doc via SDK on remote
					err = passiveBucket.Set("docid2", 0, nil, updatedBody)
					assert.NoError(t, err, "Unable to resurrect doc docid2")
					// force on-demand import
					_, getErr := remotePassiveRT.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), "docid2", db.DocUnmarshalSync)
					assert.NoError(t, getErr, "Unable to retrieve resurrected doc docid2")
				} else {
					resp = remotePassiveRT.SendAdminRequest("PUT", "/db/docid2", `{"resurrection": true}`)
					RequireStatus(t, resp, http.StatusCreated)
				}
			}

			// For SG resurrect, rev history is preserved, expect rev 6-...
			expectedRevID := "6-bf187e11c1f8913769dca26e56621036"
			if test.sdkResurrect {
				// For SDK resurrect, rev history is not preserved, expect rev 1-...
				expectedRevID = "1-e5d43a9cdc4a2d4e258800dfc37e9d77"
			}

			// Wait for doc to show up on side that the resurrection was done
			if test.resurrectLocal {
				err = localActiveRT.WaitForRev("docid2", expectedRevID)
			} else {
				err = remotePassiveRT.WaitForRev("docid2", expectedRevID)
			}
			require.NoError(t, err)

			// Start the replication
			_ = localActiveRT.SendAdminRequest("PUT", "/db/_replicationStatus/replication?action=start", "")
			localActiveRT.WaitForReplicationStatus("replication", db.ReplicationStateRunning)

			// Wait for doc to replicate from side resurrection was done on to the other side
			if test.resurrectLocal {
				err = remotePassiveRT.WaitForRev("docid2", expectedRevID)
			} else {
				err = localActiveRT.WaitForRev("docid2", expectedRevID)
			}
			assert.NoError(t, err)
		})
	}
}

// This test ensures that the local tombstone revision wins over non-tombstone revision
// whilst applying default conflict resolution policy through pushAndPull replication.
func TestDefaultConflictResolverWithTombstoneLocal(t *testing.T) {

	base.LongRunningTest(t)
	base.RequireNumTestBuckets(t, 2)
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	defaultConflictResolverWithTombstoneTests := []struct {
		name             string   // A unique name to identify the unit test.
		remoteBodyValues []string // Controls the remote revision generation.
		expectedRevID    string   // Expected document revision ID.
	}{
		{
			// Revision tie with local digest is lower than the remote digest.
			// local generation = remote generation:
			//	- e.g. local is 3-a(T), remote is 3-b
			name:             "revGenTieLocalDigestLower",
			remoteBodyValues: []string{"baz", "EADGBE"},
			expectedRevID:    "4-c6fe7cde8f7187705f9e048322a9c350",
		},
		{
			// Revision tie with local digest is higher than the remote digest.
			// local generation = remote generation:
			//	- e.g. local is 3-c(T), remote is 3-b
			name:             "revGenTieLocalDigestHigher",
			remoteBodyValues: []string{"baz", "qux"},
			expectedRevID:    "4-a210e8a790415d7e842e78e1d051cb3d",
		},
		{
			// Local revision generation is lower than remote revision generation.
			// local generation < remote generation:
			//  - e.g. local is 3-a(T), remote is 4-b
			name:             "revGenLocalLower",
			remoteBodyValues: []string{"baz", "qux", "grunt"},
			expectedRevID:    "5-fe3ac95144be01e9b455bfa163687f0e",
		},
		{
			// Local revision generation is higher than remote revision generation.
			// local generation > remote generation:
			//	- e.g. local is 3-a(T), remote is 2-b
			name:             "revGenLocalHigher",
			remoteBodyValues: []string{"baz"},
			expectedRevID:    "4-232b1f34f6b9341c54435eaf5447d85d",
		},
	}

	for _, test := range defaultConflictResolverWithTombstoneTests {
		t.Run(test.name, func(tt *testing.T) {
			// Passive
			rt2 := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket: base.GetTestBucket(t),
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					Users: map[string]*auth.PrincipalConfig{
						"alice": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("alice"),
						},
					},
				}},
			})
			defer rt2.Close()

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			// Build passiveDBURL with basic auth creds
			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)
			passiveDBURL.User = url.UserPassword("alice", "pass")

			// Active
			rt1 := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket: base.GetTestBucket(t),
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			defaultConflictResolver, err := db.NewCustomConflictResolver(
				`function(conflict) { return defaultPolicy(conflict); }`, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err, "Error creating custom conflict resolver")
			sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
			require.NoError(t, err)
			dbstats, err := sgwStats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			config := db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				Continuous:           true,
				ConflictResolverFunc: defaultConflictResolver,
				ReplicationStatsMap:  dbstats,
			}

			// Create the first revision of the document on rt1.
			docID := t.Name() + "foo"
			rt1RevIDCreated := createOrUpdateDoc(t, rt1, docID, "", "foo")

			// Create active replicator and start replication.
			ar := db.NewActiveReplicator(ctx1, &config)
			require.NoError(t, ar.Start(ctx1), "Error starting replication")
			defer func() { require.NoError(t, ar.Stop(), "Error stopping replication") }()

			// Wait for the original document revision written to rt1 to arrive at rt2.
			rt2RevIDCreated := rt1RevIDCreated
			require.NoError(t, rt2.WaitForCondition(func() bool {
				doc, _ := rt2.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
				return doc != nil && len(doc.Body()) > 0
			}))
			requireRevID(t, rt2, docID, rt2RevIDCreated)

			// Stop replication.
			require.NoError(t, ar.Stop(), "Error stopping replication")

			// Update the document on rt1 to build a revision history.
			rt1RevIDUpdated := createOrUpdateDoc(t, rt1, docID, rt1RevIDCreated, "bar")

			// Tombstone the document on rt1 to mark the tip of the revision history for deletion.
			resp := rt1.SendAdminRequest(http.MethodDelete, "/db/"+docID+"?rev="+rt1RevIDUpdated, ``)
			RequireStatus(t, resp, http.StatusOK)

			// Ensure that the tombstone revision is written to rt1 bucket with an empty body.
			waitForTombstone(t, rt1, docID)

			// Update the document on rt2 with the specified body values.
			rt2RevID := rt2RevIDCreated
			for _, bodyValue := range test.remoteBodyValues {
				rt2RevID = createOrUpdateDoc(t, rt2, docID, rt2RevID, bodyValue)
			}

			// Start replication.
			require.NoError(t, ar.Start(ctx1), "Error starting replication")

			// Wait for default conflict resolution policy to be applied through replication and
			// the winning revision to be written to both rt1 and rt2 buckets. Check whether the
			// winning revision is a tombstone; tombstone revision wins over non-tombstone revision.
			waitForTombstone(t, rt2, docID)
			waitForTombstone(t, rt1, docID)

			requireRevID(t, rt2, docID, test.expectedRevID)
			requireRevID(t, rt1, docID, test.expectedRevID)

			// Ensure that the document body of the winning tombstone revision written to both
			// rt1 and rt2 is empty, i.e., An attempt to read the document body of a tombstone
			// revision via SDK should return a "key not found" error.
			requireErrorKeyNotFound(t, rt2, docID)
			requireErrorKeyNotFound(t, rt1, docID)
		})
	}
}

// This test ensures that the remote tombstone revision wins over non-tombstone revision
// whilst applying default conflict resolution policy through pushAndPull replication.
func TestDefaultConflictResolverWithTombstoneRemote(t *testing.T) {

	base.LongRunningTest(t)
	base.RequireNumTestBuckets(t, 2)
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	defaultConflictResolverWithTombstoneTests := []struct {
		name            string   // A unique name to identify the unit test.
		localBodyValues []string // Controls the local revision generation.
		expectedRevID   string   // Expected document revision ID.
	}{
		{
			// Revision tie with remote digest is lower than the local digest.
			// local generation = remote generation:
			//	- e.g. local is 3-b, remote is 3-a(T)
			name:            "revGenTieRemoteDigestLower",
			localBodyValues: []string{"baz", "EADGBE"},
			expectedRevID:   "4-0748692c1535b62f59b2c276cc2a8bda",
		},
		{
			// Revision tie with remote digest is higher than the local digest.
			// local generation = remote generation:
			//	- e.g. local is 3-b, remote is 3-c(T)
			name:            "revGenTieRemoteDigestHigher",
			localBodyValues: []string{"baz", "qux"},
			expectedRevID:   "4-5afdb61ba968c9eaa7599e727c4c1b53",
		},
		{
			// Local revision generation is higher than remote revision generation.
			// local generation > remote generation:
			//  - e.g. local is 4-b, remote is 3-a(T)
			name:            "revGenRemoteLower",
			localBodyValues: []string{"baz", "qux", "grunt"},
			expectedRevID:   "5-962dc965fd8e7fd2bc3ffbcab85d53ba",
		},
		{
			// Local revision generation is lower than remote revision generation.
			// local generation < remote generation:
			//	- e.g. local is 2-b, remote is 3-a(T)
			name:            "revGenRemoteHigher",
			localBodyValues: []string{"grunt"},
			expectedRevID:   "3-cd4c29d9c84fc8b2a51c50e1234252c9",
		},
	}

	for _, test := range defaultConflictResolverWithTombstoneTests {
		t.Run(test.name, func(t *testing.T) {
			// Passive
			rt2 := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket: base.GetTestBucket(t),
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					Users: map[string]*auth.PrincipalConfig{
						"alice": {
							Password:         base.StringPtr("pass"),
							ExplicitChannels: base.SetOf("alice"),
						},
					},
				}},
			})
			defer rt2.Close()

			// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1
			srv := httptest.NewServer(rt2.TestPublicHandler())
			defer srv.Close()

			// Build passiveDBURL with basic auth creds
			passiveDBURL, err := url.Parse(srv.URL + "/db")
			require.NoError(t, err)
			passiveDBURL.User = url.UserPassword("alice", "pass")

			// Active
			rt1 := NewRestTester(t, &RestTesterConfig{
				CustomTestBucket: base.GetTestBucket(t),
			})
			defer rt1.Close()
			ctx1 := rt1.Context()

			defaultConflictResolver, err := db.NewCustomConflictResolver(
				`function(conflict) { return defaultPolicy(conflict); }`, rt1.GetDatabase().Options.JavascriptTimeout)
			require.NoError(t, err, "Error creating custom conflict resolver")
			sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
			require.NoError(t, err)
			dbstats, err := sgwStats.DBReplicatorStats(t.Name())
			require.NoError(t, err)

			config := db.ActiveReplicatorConfig{
				ID:          t.Name(),
				Direction:   db.ActiveReplicatorTypePushAndPull,
				RemoteDBURL: passiveDBURL,
				ActiveDB: &db.Database{
					DatabaseContext: rt1.GetDatabase(),
				},
				Continuous:           true,
				ConflictResolverFunc: defaultConflictResolver,
				ReplicationStatsMap:  dbstats,
			}

			// Create the first revision of the document on rt2.
			docID := test.name + "foo"
			rt2RevIDCreated := createOrUpdateDoc(t, rt2, docID, "", "foo")

			// Create active replicator and start replication.
			ar := db.NewActiveReplicator(ctx1, &config)
			require.NoError(t, ar.Start(ctx1), "Error starting replication")
			defer func() { require.NoError(t, ar.Stop(), "Error stopping replication") }()

			// Wait for the original document revision written to rt2 to arrive at rt1.
			rt1RevIDCreated := rt2RevIDCreated
			require.NoError(t, rt1.WaitForCondition(func() bool {
				doc, _ := rt1.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
				return doc != nil && len(doc.Body()) > 0
			}))
			requireRevID(t, rt1, docID, rt1RevIDCreated)

			// Stop replication.
			require.NoError(t, ar.Stop(), "Error stopping replication")

			// Update the document on rt2 to build a revision history.
			rt2RevIDUpdated := createOrUpdateDoc(t, rt2, docID, rt2RevIDCreated, "bar")

			// Tombstone the document on rt2 to mark the tip of the revision history for deletion.
			resp := rt2.SendAdminRequest(http.MethodDelete, "/db/"+docID+"?rev="+rt2RevIDUpdated, ``)
			RequireStatus(t, resp, http.StatusOK)
			rt2RevID := RespRevID(t, resp)
			log.Printf("rt2RevID: %s", rt2RevID)

			// Ensure that the tombstone revision is written to rt2 bucket with an empty body.
			waitForTombstone(t, rt2, docID)

			// Update the document on rt1 with the specified body values.
			rt1RevID := rt1RevIDCreated
			for _, bodyValue := range test.localBodyValues {
				rt1RevID = createOrUpdateDoc(t, rt1, docID, rt1RevID, bodyValue)
			}

			// Start replication.
			require.NoError(t, ar.Start(ctx1), "Error starting replication")

			// Wait for default conflict resolution policy to be applied through replication and
			// the winning revision to be written to both rt1 and rt2 buckets. Check whether the
			// winning revision is a tombstone; tombstone revision wins over non-tombstone revision.
			waitForTombstone(t, rt1, docID)
			waitForTombstone(t, rt2, docID)

			requireRevID(t, rt1, docID, test.expectedRevID)
			// Wait for conflict resolved doc (tombstone) to be pulled to passive bucket
			// Then require it is the expected rev
			require.NoError(t, rt2.WaitForCondition(func() bool {
				doc, _ := rt2.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
				return doc != nil && doc.SyncData.CurrentRev == test.expectedRevID
			}))

			// Ensure that the document body of the winning tombstone revision written to both
			// rt1 and rt2 is empty, i.e., An attempt to read the document body of a tombstone
			// revision via SDK should return a "key not found" error.
			requireErrorKeyNotFound(t, rt2, docID)
			requireErrorKeyNotFound(t, rt1, docID)
		})
	}
}

// TestLocalWinsConflictResolution:
//   - Starts 2 RestTesters, one active, and one passive.
//   - Validates document metadata (deleted, attachments) are preserved during LocalWins conflict
//     resolution, when local rev is rewritten as child of remote
func TestLocalWinsConflictResolution(t *testing.T) {

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only (non-default conflict resolver)")
	}

	type revisionState struct {
		generation       int
		propertyValue    string // test creates documents with body like {"prop": propertyValue}
		deleted          bool
		attachmentRevPos int
	}

	newRevisionState := func(generation int, propertyValue string, deleted bool, attachmentRevPos int) revisionState {
		return revisionState{
			generation:       generation,
			deleted:          deleted,
			attachmentRevPos: attachmentRevPos,
			propertyValue:    propertyValue,
		}
	}

	// makeRevBody creates a revision body with a value "prop" equal to property value, with an attachment
	// if attachmentRevPos is specified.
	makeRevBody := func(propertyValue string, attachmentRevPos, generation int) string {
		// No attachment if revpos==0 or is greater than current generation
		if attachmentRevPos == 0 || generation < attachmentRevPos {
			return fmt.Sprintf(`{"prop": %q}`, propertyValue)
		}

		// Create as new attachment if revpos matches generation
		if attachmentRevPos == generation {
			return fmt.Sprintf(`{"prop": %q, "_attachments": {"hello.txt": {"data":"aGVsbG8gd29ybGQ="}}}`, propertyValue)
		}

		// Otherwise include attachment as digest/stub/revpos
		return fmt.Sprintf(`{"prop": %q, "_attachments": {"hello.txt": {"stub":true,"revpos":%d,"digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`, propertyValue, attachmentRevPos)
	}

	conflictResolutionTests := []struct {
		name           string
		initialState   revisionState // Initial revision state on both nodes before conflict is introduced
		localMutation  revisionState // Revision state post-mutation on local node
		remoteMutation revisionState // Revision state post-mutation on remote node
		expectedResult revisionState // Expected revision state after conflict resolution and replication
	}{
		{
			// simpleMutation mutates remote and local
			name:           "simpleMutation",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(4, "b", false, 0),
			remoteMutation: newRevisionState(4, "c", false, 0),
			expectedResult: newRevisionState(5, "b", false, 0),
		},
		{
			// simpleMutation mutates local and tombstones remote, validates _deleted is applied
			name:           "mutateLocal_tombstoneRemote",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(4, "b", false, 0),
			remoteMutation: newRevisionState(4, "c", true, 0),
			expectedResult: newRevisionState(5, "b", false, 0),
		},
		{
			// simpleMutation includes an attachment prior to conflict, validates it's preserved
			name:           "localAttachment",
			initialState:   newRevisionState(3, "a", false, 2),
			localMutation:  newRevisionState(4, "b", false, 0),
			remoteMutation: newRevisionState(4, "c", false, 0),
			expectedResult: newRevisionState(5, "b", false, 3), // revpos==3 here because the revision isn't replicated until rev 3
		},
		{
			// localAttachmentPostConflict adds a local attachment on a conflicting branch
			name:           "localAttachmentPostConflict",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(6, "b", false, 5),
			remoteMutation: newRevisionState(6, "c", false, 0),
			expectedResult: newRevisionState(7, "b", false, 7),
		},
		{
			// remoteAttachmentPostConflict adds a remote attachment on a conflicting branch
			name:           "remoteAttachmentPostConflict",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(6, "b", false, 0),
			remoteMutation: newRevisionState(6, "c", false, 5),
			expectedResult: newRevisionState(7, "b", false, 0),
		},
		{
			// remoteAttachmentPostConflict adds the same attachment to local and remote conflicting branches
			name:           "conflictingDocMatchingAttachmentPostConflict",
			initialState:   newRevisionState(3, "a", false, 0),
			localMutation:  newRevisionState(6, "b", false, 4),
			remoteMutation: newRevisionState(6, "c", false, 5),
			expectedResult: newRevisionState(7, "b", false, 5),
		},
	}

	for _, test := range conflictResolutionTests {
		t.Run(test.name, func(t *testing.T) {
			base.RequireNumTestBuckets(t, 2)
			base.SetUpTestLogging(t, base.LevelTrace, base.KeyAll)

			activeRT, remoteRT, remoteURLString, teardown := setupSGRPeers(t)
			defer teardown()

			// Create initial revision(s) on local
			docID := test.name

			var parentRevID, newRevID string
			for gen := 1; gen <= test.initialState.generation; gen++ {
				newRevID = fmt.Sprintf("%d-initial", gen)
				resp := activeRT.putNewEditsFalse(docID, newRevID, parentRevID,
					makeRevBody(test.initialState.propertyValue, test.initialState.attachmentRevPos, gen))
				log.Printf("-- Added initial revision: %s", resp.Rev)
				parentRevID = newRevID
			}

			// Create replication, wait for initial revision to be replicated
			replicationID := test.name
			activeRT.createReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePushAndPull, nil, true, db.ConflictResolverLocalWins)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

			assert.NoError(t, remoteRT.WaitForRev(docID, newRevID))

			// Stop the replication
			response := activeRT.SendAdminRequest("PUT", "/db/_replicationStatus/"+replicationID+"?action=stop", "")
			RequireStatus(t, response, http.StatusOK)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

			rawResponse := activeRT.SendAdminRequest("GET", "/db/_raw/"+docID, "")
			log.Printf("-- local raw pre-update: %s", rawResponse.Body.Bytes())
			rawResponse = remoteRT.SendAdminRequest("GET", "/db/_raw/"+docID, "")
			log.Printf("-- remote raw pre-update: %s", rawResponse.Body.Bytes())

			// Update local and remote revisions
			localParentRevID := newRevID
			var newLocalRevID string
			for localGen := test.initialState.generation + 1; localGen <= test.localMutation.generation; localGen++ {
				// If deleted=true, tombstone on the last mutation
				if test.localMutation.deleted == true && localGen == test.localMutation.generation {
					activeRT.tombstoneDoc(docID, localParentRevID)
					continue
				}

				newLocalRevID = fmt.Sprintf("%d-local", localGen)
				// Local rev pos is greater of initial state revpos and localMutation rev pos
				localRevPos := test.initialState.attachmentRevPos
				if test.localMutation.attachmentRevPos > 0 {
					localRevPos = test.localMutation.attachmentRevPos
				}
				resp := activeRT.putNewEditsFalse(docID, newLocalRevID, localParentRevID, makeRevBody(test.localMutation.propertyValue, localRevPos, localGen))
				log.Printf("-- Added local revision: %s", resp.Rev)
				localParentRevID = newLocalRevID
			}

			remoteParentRevID := newRevID
			var newRemoteRevID string
			for remoteGen := test.initialState.generation + 1; remoteGen <= test.remoteMutation.generation; remoteGen++ {
				// If deleted=true, tombstone on the last mutation
				if test.remoteMutation.deleted == true && remoteGen == test.remoteMutation.generation {
					remoteRT.tombstoneDoc(docID, remoteParentRevID)
					continue
				}
				newRemoteRevID = fmt.Sprintf("%d-remote", remoteGen)

				// Local rev pos is greater of initial state revpos and remoteMutation rev pos
				remoteRevPos := test.initialState.attachmentRevPos
				if test.remoteMutation.attachmentRevPos > 0 {
					remoteRevPos = test.remoteMutation.attachmentRevPos
				}
				resp := remoteRT.putNewEditsFalse(docID, newRemoteRevID, remoteParentRevID, makeRevBody(test.remoteMutation.propertyValue, remoteRevPos, remoteGen))
				log.Printf("-- Added remote revision: %s", resp.Rev)
				remoteParentRevID = newRemoteRevID
			}

			rawResponse = activeRT.SendAdminRequest("GET", "/db/_raw/"+docID, "")
			log.Printf("-- local raw pre-replication: %s", rawResponse.Body.Bytes())
			rawResponse = remoteRT.SendAdminRequest("GET", "/db/_raw/"+docID, "")
			log.Printf("-- remote raw pre-replication: %s", rawResponse.Body.Bytes())

			// Restart the replication
			response = activeRT.SendAdminRequest("PUT", "/db/_replicationStatus/"+replicationID+"?action=start", "")
			RequireStatus(t, response, http.StatusOK)

			// Wait for expected property value on remote to determine replication complete
			waitErr := remoteRT.WaitForCondition(func() bool {
				var remoteDoc db.Body
				rawResponse := remoteRT.SendAdminRequest("GET", "/db/"+docID, "")
				require.NoError(t, base.JSONUnmarshal(rawResponse.Body.Bytes(), &remoteDoc))
				prop, ok := remoteDoc["prop"].(string)
				log.Printf("-- Waiting for property: %v, got property: %v", test.expectedResult.propertyValue, prop)
				return ok && prop == test.expectedResult.propertyValue
			})
			assert.NoError(t, waitErr)

			localDoc := activeRT.GetDoc(docID)
			localRevID := localDoc.ExtractRev()
			remoteDoc := remoteRT.GetDoc(docID)
			remoteRevID := remoteDoc.ExtractRev()

			assert.Equal(t, localRevID, remoteRevID) // local and remote rev IDs must match
			localGeneration, _ := db.ParseRevID(localRevID)
			assert.Equal(t, test.expectedResult.generation, localGeneration)               // validate expected generation
			assert.Equal(t, test.expectedResult.propertyValue, remoteDoc["prop"].(string)) // validate expected body
			assert.Equal(t, test.expectedResult.propertyValue, localDoc["prop"].(string))  // validate expected body

			remoteRevpos := getTestRevpos(t, remoteDoc, "hello.txt")
			assert.Equal(t, test.expectedResult.attachmentRevPos, remoteRevpos) // validate expected revpos

			rawResponse = activeRT.SendAdminRequest("GET", "/db/_raw/"+docID, "")
			log.Printf("-- local raw post-replication: %s", rawResponse.Body.Bytes())

			rawResponse = remoteRT.SendAdminRequest("GET", "/db/_raw/"+docID, "")
			log.Printf("-- remote raw post-replication: %s", rawResponse.Body.Bytes())
		})
	}
}

// This test can be used for testing replication to a pre-hydrogen SGR target. The test itself simply has a passive and
// active node and attempts to replicate and expects the replicator to enter an error state. The intention is that the
// passive side emulates a pre-hydrogen target by having ignoreNoConflicts set to false. In order to use this test this
// flag should be hardcoded during development. This can be set inside the sendChangesOptions struct under the _connect
// method in active_replicator_push.go
func TestSendChangesToNoConflictPreHydrogenTarget(t *testing.T) {
	t.Skip("Test is only for development purposes")

	base.RequireNumTestBuckets(t, 2)

	errorCountBefore := base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value()

	// Passive
	tb2 := base.GetTestBucket(t)
	rt2 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb2,
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AllowConflicts: base.BoolPtr(false),
		}},
	})
	defer rt2.Close()

	rt1 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	// Make rt2 listen on an actual HTTP port, so it can receive the blipsync request from rt1.
	srv := httptest.NewTLSServer(rt2.TestAdminHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          "test",
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:          true,
		InsecureSkipVerify:  true,
		ReplicationStatsMap: dbstats,
	})

	defer func() {
		require.NoError(t, ar.Stop())
	}()
	require.NoError(t, ar.Start(ctx1))

	assert.Equal(t, errorCountBefore, base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value())

	response := rt1.SendAdminRequest("PUT", "/db/doc1", "{}")
	RequireStatus(t, response, http.StatusCreated)

	err = rt2.WaitForCondition(func() bool {
		return base.SyncGatewayStats.GlobalStats.ResourceUtilizationStats().ErrorCount.Value() == errorCountBefore+1
	})
	assert.NoError(t, err)

	assert.Equal(t, db.ReplicationStateStopped, ar.GetStatus().Status)
	assert.Equal(t, db.PreHydrogenTargetAllowConflictsError.Error(), ar.GetStatus().ErrorMessage)
}

func TestReplicatorConflictAttachment(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	if !base.IsEnterpriseEdition() {
		t.Skipf("requires enterprise edition")
	}

	testCases := []struct {
		name                      string
		conflictResolution        db.ConflictResolverType
		expectedFinalRev          string
		expectedRevPos            int
		expectedAttachmentContent string
	}{
		{
			name:                      "local",
			conflictResolution:        db.ConflictResolverLocalWins,
			expectedFinalRev:          "6-3545745ab68aec5b00e745f9e0e3277c",
			expectedRevPos:            6,
			expectedAttachmentContent: "hello world",
		},
		{
			name:                      "remote",
			conflictResolution:        db.ConflictResolverRemoteWins,
			expectedFinalRev:          "5-remote",
			expectedRevPos:            4,
			expectedAttachmentContent: "goodbye cruel world",
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			activeRT, remoteRT, remoteURLString, teardown := setupSGRPeers(t)
			defer teardown()

			docID := test.name

			var parentRevID, newRevID string
			for gen := 1; gen <= 3; gen++ {
				newRevID = fmt.Sprintf("%d-initial", gen)
				resp := activeRT.putNewEditsFalse(docID, newRevID, parentRevID, "{}")
				parentRevID = newRevID
				assert.True(t, resp.Ok)
			}

			replicationID := "replication"
			activeRT.createReplication(replicationID, remoteURLString, db.ActiveReplicatorTypePushAndPull, nil, true, test.conflictResolution)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateRunning)

			assert.NoError(t, remoteRT.WaitForRev(docID, newRevID))

			response := activeRT.SendAdminRequest("PUT", "/db/_replicationStatus/"+replicationID+"?action=stop", "")
			RequireStatus(t, response, http.StatusOK)
			activeRT.WaitForReplicationStatus(replicationID, db.ReplicationStateStopped)

			nextGen := 4

			localGen := nextGen
			localParentRevID := newRevID
			newLocalRevID := fmt.Sprintf("%d-local", localGen)
			resp := activeRT.putNewEditsFalse(docID, newLocalRevID, localParentRevID, `{"_attachments": {"attach": {"data":"aGVsbG8gd29ybGQ="}}}`)
			assert.True(t, resp.Ok)
			localParentRevID = newLocalRevID

			localGen++
			newLocalRevID = fmt.Sprintf("%d-local", localGen)
			resp = activeRT.putNewEditsFalse(docID, newLocalRevID, localParentRevID, fmt.Sprintf(`{"_attachments": {"attach": {"stub": true, "revpos": %d, "digest":"sha1-Kq5sNclPz7QV2+lfQIuc6R7oRu0="}}}`, localGen-1))
			assert.True(t, resp.Ok)

			remoteGen := nextGen
			remoteParentRevID := newRevID
			newRemoteRevID := fmt.Sprintf("%d-remote", remoteGen)
			resp = remoteRT.putNewEditsFalse(docID, newRemoteRevID, remoteParentRevID, `{"_attachments": {"attach": {"data":"Z29vZGJ5ZSBjcnVlbCB3b3JsZA=="}}}`)
			assert.True(t, resp.Ok)
			remoteParentRevID = newRemoteRevID

			remoteGen++
			newRemoteRevID = fmt.Sprintf("%d-remote", remoteGen)
			resp = remoteRT.putNewEditsFalse(docID, newRemoteRevID, remoteParentRevID, fmt.Sprintf(`{"_attachments": {"attach": {"stub": true, "revpos": %d, "digest":"sha1-gwwPApfQR9bzBKpqoEYwFmKp98A="}}}`, remoteGen-1))

			response = activeRT.SendAdminRequest("PUT", "/db/_replicationStatus/"+replicationID+"?action=start", "")
			RequireStatus(t, response, http.StatusOK)

			waitErr := activeRT.WaitForRev(docID, test.expectedFinalRev)
			assert.NoError(t, waitErr)
			waitErr = remoteRT.WaitForRev(docID, test.expectedFinalRev)
			assert.NoError(t, waitErr)

			localDoc := activeRT.GetDoc(docID)
			localRevID := localDoc.ExtractRev()

			remoteDoc := remoteRT.GetDoc(docID)
			remoteRevID := remoteDoc.ExtractRev()

			assert.Equal(t, localRevID, remoteRevID)
			remoteRevpos := getTestRevpos(t, remoteDoc, "attach")
			assert.Equal(t, test.expectedRevPos, remoteRevpos)

			response = activeRT.SendAdminRequest("GET", "/db/"+docID+"/attach", "")
			assert.Equal(t, test.expectedAttachmentContent, string(response.BodyBytes()))
		})
	}
}

func TestConflictResolveMergeWithMutatedRev(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	base.RequireNumTestBuckets(t, 2)

	// Passive
	rt2 := NewRestTester(t, nil)
	defer rt2.Close()

	// Active
	rt1 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: base.GetTestBucket(t),
	})
	defer rt1.Close()
	ctx1 := rt1.Context()

	srv := httptest.NewServer(rt2.TestAdminHandler())
	defer srv.Close()

	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	customConflictResolver, err := db.NewCustomConflictResolver(`function(conflict){
			var mutatedLocal = conflict.LocalDocument;
			mutatedLocal.source = "merged";
			mutatedLocal["_deleted"] = true;
			mutatedLocal["_rev"] = "";
			return mutatedLocal;
		}`, rt1.GetDatabase().Options.JavascriptTimeout)
	require.NoError(t, err)

	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(ctx1, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePull,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: rt1.GetDatabase(),
		},
		Continuous:             false,
		ReplicationStatsMap:    dbstats,
		ConflictResolutionType: db.ConflictResolverCustom,
		ConflictResolverFunc:   customConflictResolver,
	})

	resp := rt2.SendAdminRequest("PUT", "/db/doc", "{}")
	RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt2.WaitForPendingChanges())

	resp = rt1.SendAdminRequest("PUT", "/db/doc", `{"some_val": "val"}`)
	RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt1.WaitForPendingChanges())

	require.NoError(t, ar.Start(ctx1))

	val, found := base.WaitForStat(func() int64 {
		dbRepStats, err := base.SyncGatewayStats.DbStats[t.Name()].DBReplicatorStats(ar.ID)
		require.NoError(t, err)
		return dbRepStats.PulledCount.Value()
	}, 1)
	assert.True(t, found)
	assert.Equal(t, int64(1), val)

	rt1.WaitForReplicationStatus(t.Name(), db.ReplicationStateStopped)
}

// CBG-1427 - ISGR should not try sending a delta when deltaSrc is a tombstone
func TestReplicatorDoNotSendDeltaWhenSrcIsTombstone(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for delta sync")
	}

	base.RequireNumTestBuckets(t, 2)

	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	// Passive //
	passiveBucket := base.GetTestBucket(t)
	passiveRT := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: passiveBucket,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: base.BoolPtr(true),
				},
			},
		},
	})
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator.
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeBucket := base.GetTestBucket(t)
	activeRT := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: activeBucket,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: base.BoolPtr(true),
				},
			},
		},
	})
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create a document //
	resp := activeRT.SendAdminRequest(http.MethodPut, "/db/test", `{"field1":"f1_1","field2":"f2_1"}`)
	RequireStatus(t, resp, http.StatusCreated)
	revID := RespRevID(t, resp)
	err := activeRT.WaitForRev("test", revID)
	require.NoError(t, err)

	// Set-up replicator //
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), true, false, false)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    1,
		DeltasEnabled:       true,
		ReplicationStatsMap: dbstats,
	})
	assert.Equal(t, "", ar.GetStatus().LastSeqPush)
	assert.NoError(t, ar.Start(activeCtx))

	// Wait for active to replicate to passive
	err = passiveRT.WaitForRev("test", revID)
	require.NoError(t, err)

	// Delete active document
	resp = activeRT.SendAdminRequest(http.MethodDelete, "/db/test?rev="+revID, "")
	RequireStatus(t, resp, http.StatusOK)
	revID = RespRevID(t, resp)

	// Replicate tombstone to passive
	err = passiveRT.WaitForCondition(func() bool {
		rawResponse := passiveRT.SendAdminRequest("GET", "/db/test?rev="+revID, "")
		return rawResponse.Code == 404
	})
	require.NoError(t, err)

	// Resurrect tombstoned document
	resp = activeRT.SendAdminRequest(http.MethodPut, "/db/test?rev="+revID, `{"field2":"f2_2"}`)
	RequireStatus(t, resp, http.StatusCreated)
	revID = RespRevID(t, resp)

	// Replicate resurrection to passive
	err = passiveRT.WaitForRev("test", revID)
	assert.NoError(t, err) // If error, problem not fixed

	// Shutdown replicator to close out
	require.NoError(t, ar.Stop())
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)
}

// CBG-1672 - Return 422 status for unprocessable deltas instead of 404 to use non-delta retry handling
// Should log "422 Unable to unmarshal mutable body for doc test deltaSrc=1-dbc7919edc9ec2576d527880186f8e8a"
// then fall back to full body replication
func TestUnprocessableDeltas(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for some delta sync")
	}

	base.RequireNumTestBuckets(t, 2)

	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Passive //
	passiveBucket := base.GetTestBucket(t)
	passiveRT := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: passiveBucket,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: base.BoolPtr(true),
				},
			},
		},
	})
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator.
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeBucket := base.GetTestBucket(t)
	activeRT := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: activeBucket,
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				DeltaSync: &DeltaSyncConfig{
					Enabled: base.BoolPtr(true),
				},
			},
		},
	})
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create a document //
	resp := activeRT.SendAdminRequest(http.MethodPut, "/db/test", `{"field1":"f1_1","field2":"f2_1"}`)
	RequireStatus(t, resp, http.StatusCreated)
	revID := RespRevID(t, resp)
	err := activeRT.WaitForRev("test", revID)
	require.NoError(t, err)

	// Set-up replicator //
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), true, false, false)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    200,
		DeltasEnabled:       true,
		ReplicationStatsMap: dbstats,
	})
	assert.Equal(t, "", ar.GetStatus().LastSeqPush)

	assert.NoError(t, ar.Start(activeCtx))

	err = passiveRT.WaitForRev("test", revID)
	require.NoError(t, err)

	assert.NoError(t, ar.Stop())

	// Make 2nd revision
	resp = activeRT.SendAdminRequest(http.MethodPut, "/db/test?rev="+revID, `{"field1":"f1_2","field2":"f2_2"}`)
	RequireStatus(t, resp, http.StatusCreated)
	revID = RespRevID(t, resp)
	err = activeRT.WaitForPendingChanges()
	require.NoError(t, err)

	rev, err := passiveRT.GetDatabase().GetSingleDatabaseCollection().GetRevisionCacheForTest().GetActive(base.TestCtx(t), "test", true)
	require.NoError(t, err)
	// Making body invalid to trigger log "Unable to unmarshal mutable body for doc" in handleRev
	// Which should give a HTTP 422
	rev.BodyBytes = []byte("{invalid}")
	passiveRT.GetDatabase().GetSingleDatabaseCollection().GetRevisionCacheForTest().Upsert(base.TestCtx(t), rev)

	assert.NoError(t, ar.Start(activeCtx))
	// Check if it replicated
	err = passiveRT.WaitForRev("test", revID)
	assert.NoError(t, err)

	assert.NoError(t, ar.Stop())
}

// CBG-1428 - check for regression of ISGR not ignoring _removed:true bodies when purgeOnRemoval is disabled
func TestReplicatorIgnoreRemovalBodies(t *testing.T) {

	base.RequireNumTestBuckets(t, 2)

	// Copies the behaviour of TestGetRemovedAsUser but with replication and no user
	defer db.SuspendSequenceBatching()()
	base.SetUpTestLogging(t, base.LevelInfo, base.KeyAll)

	// Passive //
	passiveBucket := base.GetTestBucket(t)
	passiveRT := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: passiveBucket,
	})
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeBucket := base.GetTestBucket(t)
	activeRT := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: activeBucket,
	})
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create the docs //
	// Doc rev 1
	resp := activeRT.SendAdminRequest(http.MethodPut, "/db/"+t.Name(), `{"key":"12","channels": ["rev1chan"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	rev1ID := RespRevID(t, resp)
	err := activeRT.WaitForRev(t.Name(), rev1ID)
	require.NoError(t, err)

	// doc rev 2
	resp = activeRT.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s?rev=%s", t.Name(), rev1ID), `{"key":"12","channels":["rev2+3chan"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	rev2ID := RespRevID(t, resp)
	err = activeRT.WaitForRev(t.Name(), rev2ID)
	require.NoError(t, err)

	// Doc rev 3
	resp = activeRT.SendAdminRequest(http.MethodPut, fmt.Sprintf("/db/%s?rev=%s", t.Name(), rev2ID), `{"key":"3","channels":["rev2+3chan"]}`)
	RequireStatus(t, resp, http.StatusCreated)
	rev3ID := RespRevID(t, resp)
	err = activeRT.WaitForRev(t.Name(), rev3ID)
	require.NoError(t, err)

	activeRT.GetDatabase().GetSingleDatabaseCollection().FlushRevisionCacheForTest()
	err = activeRT.Bucket().Delete(fmt.Sprintf("_sync:rev:%s:%d:%s", t.Name(), len(rev2ID), rev2ID))
	require.NoError(t, err)

	// Set-up replicator //
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)

	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          false,
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		PurgeOnRemoval:      false,
		Filter:              base.ByChannelFilter,
		FilterChannels:      []string{"rev1chan"},
	})
	docWriteFailuresBefore := ar.GetStatus().DocWriteFailures

	assert.NoError(t, ar.Start(activeCtx))
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateStopped)

	assert.Equal(t, docWriteFailuresBefore, ar.GetStatus().DocWriteFailures, "ISGR should ignore _remove:true bodies when purgeOnRemoval is disabled. CBG-1428 regression.")
}

// CBG-1995: Test the support for using an underscore prefix in the top-level body of a document
// Tests replication and Rest API
func TestUnderscorePrefixSupport(t *testing.T) {
	base.RequireNumTestBuckets(t, 2)

	// Passive //
	passiveRT := NewRestTester(t, nil)
	defer passiveRT.Close()

	// Make passive RT listen on an actual HTTP port, so it can receive the blipsync request from the active replicator
	srv := httptest.NewServer(passiveRT.TestAdminHandler())
	defer srv.Close()

	// Active //
	activeRT := NewRestTester(t, nil)
	defer activeRT.Close()
	activeCtx := activeRT.Context()

	// Create the document
	docID := t.Name()
	rawDoc := `{"_foo": true, "_exp": 120, "true": false, "_attachments": {"bar": {"data": "Zm9vYmFy"}}}`
	_ = activeRT.PutDoc(docID, rawDoc)

	// Set-up replicator
	passiveDBURL, err := url.Parse(srv.URL + "/db")
	require.NoError(t, err)
	sgwStats, err := base.SyncGatewayStats.NewDBStats(t.Name(), false, false, false)
	require.NoError(t, err)
	dbstats, err := sgwStats.DBReplicatorStats(t.Name())
	require.NoError(t, err)

	ar := db.NewActiveReplicator(activeCtx, &db.ActiveReplicatorConfig{
		ID:          t.Name(),
		Direction:   db.ActiveReplicatorTypePush,
		RemoteDBURL: passiveDBURL,
		ActiveDB: &db.Database{
			DatabaseContext: activeRT.GetDatabase(),
		},
		Continuous:          true,
		ChangesBatchSize:    200,
		ReplicationStatsMap: dbstats,
		PurgeOnRemoval:      false,
	})
	defer func() { require.NoError(t, ar.Stop()) }()

	require.NoError(t, ar.Start(activeCtx))
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	// Confirm document is replicated
	changesResults, err := passiveRT.WaitForChanges(1, "/db/_changes?since=0", "", true)
	assert.NoError(t, err)
	assert.Len(t, changesResults.Results, 1)

	err = passiveRT.WaitForPendingChanges()
	require.NoError(t, err)

	require.NoError(t, ar.Stop())

	// Assert document was replicated successfully
	doc := passiveRT.GetDoc(docID)
	assert.EqualValues(t, true, doc["_foo"])  // Confirm user defined value got created
	assert.EqualValues(t, nil, doc["_exp"])   // Confirm expiry was consumed
	assert.EqualValues(t, false, doc["true"]) // Sanity check normal keys
	// Confirm attachment was created successfully
	resp := passiveRT.SendAdminRequest("GET", "/db/"+t.Name()+"/bar", "")
	RequireStatus(t, resp, 200)

	// Edit existing document
	rev := doc["_rev"]
	require.NotNil(t, rev)
	rawDoc = fmt.Sprintf(`{"_rev": "%s","_foo": false, "test": true}`, rev)
	_ = activeRT.PutDoc(docID, rawDoc)

	// Replicate modified document
	require.NoError(t, ar.Start(activeCtx))
	activeRT.WaitForReplicationStatus(ar.ID, db.ReplicationStateRunning)

	changesResults, err = passiveRT.WaitForChanges(1, fmt.Sprintf("/db/_changes?since=%v", changesResults.Last_Seq), "", true)
	assert.NoError(t, err)
	assert.Len(t, changesResults.Results, 1)

	err = passiveRT.WaitForPendingChanges()
	require.NoError(t, err)

	// Verify document replicated successfully
	doc = passiveRT.GetDoc(docID)
	assert.NotEqualValues(t, doc["_rev"], rev) // Confirm rev got replaced with new rev
	assert.EqualValues(t, false, doc["_foo"])  // Confirm user defined value got created
	assert.EqualValues(t, true, doc["test"])
	// Confirm attachment was removed successfully in latest revision
	resp = passiveRT.SendAdminRequest("GET", "/db/"+docID+"/bar", "")
	RequireStatus(t, resp, 404)

	// Add disallowed _removed tag in document
	rawDoc = fmt.Sprintf(`{"_rev": "%s","_removed": false}`, doc["_rev"])
	resp = activeRT.SendAdminRequest("PUT", "/db/"+docID, rawDoc)
	RequireStatus(t, resp, 404)

	// Add disallowed _purged tag in document
	rawDoc = fmt.Sprintf(`{"_rev": "%s","_purged": true}`, doc["_rev"])
	resp = activeRT.SendAdminRequest("PUT", "/db/"+docID, rawDoc)
	RequireStatus(t, resp, 400)
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

// createOrUpdateDoc creates a new document or update an existing document with the
// specified document id, revision id and body value in a channel named "alice".
func createOrUpdateDoc(t *testing.T, rt *RestTester, docID, revID, bodyValue string) string {
	body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
	dbURL := "/db/" + docID
	if revID != "" {
		dbURL = "/db/" + docID + "?rev=" + revID
	}
	resp := rt.SendAdminRequest(http.MethodPut, dbURL, body)
	RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())
	return RespRevID(t, resp)
}

// waitForTombstone waits until the specified tombstone revision is available
// in the bucket backed by the specified RestTester instance.
func waitForTombstone(t *testing.T, rt *RestTester, docID string) {
	require.NoError(t, rt.WaitForPendingChanges())
	require.NoError(t, rt.WaitForCondition(func() bool {
		doc, _ := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
		return doc.IsDeleted() && len(doc.Body()) == 0
	}))
}

// requireErrorKeyNotFound asserts that reading specified document body via SDK
// returns a key not found error.
func requireErrorKeyNotFound(t *testing.T, rt *RestTester, docID string) {
	var body []byte
	_, err := rt.Bucket().Get(docID, &body)
	require.Error(t, err)
	require.True(t, base.IsKeyNotFoundError(rt.Bucket(), err))
}

// requireRevID asserts that the specified document revision is written to the
// underlying bucket backed by the given RestTester instance.
func requireRevID(t *testing.T, rt *RestTester, docID, revID string) {
	doc, err := rt.GetDatabase().GetSingleDatabaseCollection().GetDocument(base.TestCtx(t), docID, db.DocUnmarshalAll)
	require.NoError(t, err, "Error reading document from bucket")
	require.Equal(t, revID, doc.SyncData.CurrentRev)
}
