// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package db

import (
	"net/http"
	"strings"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

// TestImportFailureLogging shows the logs printed for each way a document can fail to import, for on-demand and feed imports.
func TestImportFailureLogging(t *testing.T) {
	testCases := []struct {
		name           string
		syncFn         string
		importFilter   string
		casMismatch    bool // the document changes after the feed event, so the event is out of date by the time it is imported
		modes          []ImportMode
		expectedLogs   []string
		unexpectedLogs []string
	}{
		{
			name:         "sync function rejection",
			syncFn:       `function(doc, oldDoc) { if (!doc.type) { throw({forbidden: "Missing required property"}); } channel(doc.channels); }`,
			modes:        []ImportMode{ImportOnDemand, ImportFromFeed},
			expectedLogs: []string{"Sync fn rejected doc"},
			// the sync function rejection is already logged, so the import does not log it again
			unexpectedLogs: []string{"Error importing doc", "Unable to import doc", "Did not import doc", "Did not update document"},
		},
		{
			name:         "import filter rejection",
			importFilter: `function(doc) { return doc.type == "mobile"; }`,
			modes:        []ImportMode{ImportOnDemand, ImportFromFeed},
			expectedLogs: []string{"excluded by document import function"},
		},
		{
			// an on-demand import reloads the document on a cas mismatch, so only a feed import can fail this way
			name:         "cas mismatch",
			casMismatch:  true,
			modes:        []ImportMode{ImportFromFeed},
			expectedLogs: []string{"has been subsequently updated"},
		},
	}
	for _, testCase := range testCases {
		for _, mode := range testCase.modes {
			modeName := "on-demand"
			if mode == ImportFromFeed {
				modeName = "feed"
			}
			t.Run(testCase.name+"/"+modeName, func(t *testing.T) {
				base.SetUpTestLogging(t, base.LevelDebug, base.KeyCRUD, base.KeyImport)
				db, ctx := SetupTestDB(t)
				defer db.Close(ctx)
				collection, ctx := GetSingleDatabaseCollectionWithUser(ctx, t, db)
				if testCase.syncFn != "" {
					_, err := collection.UpdateSyncFun(ctx, testCase.syncFn)
					require.NoError(t, err)
				}
				if testCase.importFilter != "" {
					collection.importFilterFunction = NewImportFilterFunction(ctx, testCase.importFilter, 0)
				}

				docID := sgtest.SafeDocumentName(t, t.Name())
				_, doc, err := collection.Put(ctx, docID, Body{"type": "mobile", "channels": "A"})
				require.NoError(t, err)
				externalCas, err := collection.dataStore.WriteCas(ctx, docID, 0, doc.Cas, []byte(`{"channels": "A"}`), 0)
				require.NoError(t, err)

				var event sgbucket.FeedEvent
				if mode == ImportFromFeed {
					event = feedEventForDoc(t, collection, docID)
				}
				if testCase.casMismatch {
					_, err = collection.dataStore.WriteCas(ctx, docID, 0, externalCas, []byte(`{"type": "mobile", "channels": "B"}`), 0)
					require.NoError(t, err)
				}

				logs := base.CaptureConsoleLogs(func() {
					if mode == ImportFromFeed {
						NewImportListener(ctx, "", db.DatabaseContext).ImportFeedEvent(ctx, collection, event)
					} else {
						_, err := collection.GetDocument(ctx, docID, DocUnmarshalAll)
						status, _ := base.ErrorAsHTTPStatus(err)
						require.Equal(t, http.StatusNotFound, status)
					}
				})

				var docLines []string
				for line := range strings.SplitSeq(logs, "\n") {
					if strings.Contains(line, docID) {
						docLines = append(docLines, line)
					}
				}
				t.Logf("%d log lines for %s:\n%s", len(docLines), docID, strings.Join(docLines, "\n"))
				for _, msg := range testCase.expectedLogs {
					assert.Contains(t, logs, msg)
				}
				for _, msg := range testCase.unexpectedLogs {
					assert.NotContains(t, logs, msg)
				}
			})
		}
	}
}

// feedEventForDoc returns the DCP mutation that the import feed would receive for the document's current version.
func feedEventForDoc(t *testing.T, collection *DatabaseCollectionWithUser, docID string) sgbucket.FeedEvent {
	body, xattrs, cas, err := collection.dataStore.GetWithXattrs(base.TestCtx(t), docID, collection.syncGlobalSyncMouRevSeqNoAndUserXattrKeys())
	require.NoError(t, err)
	var feedXattrs []sgbucket.Xattr
	for name, value := range xattrs {
		// virtual xattrs are not sent on the feed
		if !strings.HasPrefix(name, "$") {
			feedXattrs = append(feedXattrs, sgbucket.Xattr{Name: name, Value: value})
		}
	}
	return sgbucket.FeedEvent{
		Opcode:   sgbucket.FeedOpMutation,
		Key:      []byte(docID),
		Value:    sgbucket.EncodeValueWithXattrs(body, feedXattrs...),
		DataType: base.MemcachedDataTypeJSON | base.MemcachedDataTypeXattr,
		Cas:      cas,
		RevNo:    RetrieveDocRevSeqNo(t, xattrs[base.VirtualXattrRevSeqNo]),
	}
}
