// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package replicatortest

import (
	"fmt"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// Helper functions for SGR testing

func reduceTestCheckpointInterval(interval time.Duration) func() {
	previousInterval := db.DefaultCheckpointInterval
	db.DefaultCheckpointInterval = interval
	return func() {
		db.DefaultCheckpointInterval = previousInterval
	}

}

// requirePersistedReplicationProgress waits for the status document and the checkpoint of a
// replication to reflect expectedDocs documents processed. A node picking up a reassigned replication
// inherits its stats from the status document and resumes from the checkpoint, and the two are written
// on independent tickers - so waiting for both before a rebalance makes the stats reported after it
// deterministic. A checkpoint is only written once a sequence has been processed, so its presence also
// means an already-replicated document won't be processed again.
func requirePersistedReplicationProgress(rt *rest.RestTester, replicationID string, direction db.ActiveReplicatorDirection, expectedDocs int64) {
	rt.TB().Helper()
	var checkpointID, statName string
	var persistedDocs func(*db.ReplicationStatus) int64
	switch direction {
	case db.ActiveReplicatorTypePush:
		checkpointID, statName = db.PushCheckpointID(replicationID), "DocsCheckedPush"
		persistedDocs = func(status *db.ReplicationStatus) int64 { return status.DocsCheckedPush }
	case db.ActiveReplicatorTypePull:
		checkpointID, statName = db.PullCheckpointID(replicationID), "DocsRead"
		persistedDocs = func(status *db.ReplicationStatus) int64 { return status.DocsRead }
	default:
		require.FailNow(rt.TB(), "unsupported replication direction "+string(direction))
	}
	require.EventuallyWithT(rt.TB(), func(c *assert.CollectT) {
		status, err := db.LoadReplicationStatus(rt.Context(), rt.GetDatabase(), replicationID)
		if !assert.NoError(c, err) {
			return
		}
		assert.Equal(c, expectedDocs, persistedDocs(status), "%s: %s in persisted replication status document", replicationID, statName)
	}, 20*time.Second, 10*time.Millisecond)
	rt.WaitForCheckpointLastSequence(db.RealSpecialDocID(db.DocTypeLocal, db.CheckpointDocIDPrefix+checkpointID))
}

// createOrUpdateDoc creates a new document the specified document id, and body value in a channel named "alice".
func createDoc(rt *rest.RestTester, docID string, bodyValue string) rest.DocVersion {
	body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
	updatedVersion := rt.PutDoc(docID, body)
	// make sure doc is available to changes feed
	rt.WaitForPendingChanges()
	return updatedVersion
}

// updateDoc update an existing document with the specified document id, version and body value in a channel named "alice".
func updateDoc(rt *rest.RestTester, docID string, version rest.DocVersion, bodyValue string) rest.DocVersion {
	body := fmt.Sprintf(`{"key":%q,"channels":["alice"]}`, bodyValue)
	updatedVersion := rt.UpdateDoc(docID, version, body)
	// make sure doc is available to changes feed
	rt.WaitForPendingChanges()
	return updatedVersion
}

// requireCheckpointSequence waits for the checkpointer's processed and expected sequence counts to reach
// expectedSeqCount. This avoids the race where WaitForChanges returns after the document write but before
// the checkpointer's BLIP callback has recorded the sequence.
func requireCheckpointSequence(t *testing.T, checkpointer *db.Checkpointer, expectedSeqCount int64) {
	t.Helper()
	base.RequireWaitForStat(t, func() int64 { return checkpointer.Stats().ProcessedSequenceCount }, expectedSeqCount, "ProcessedSequenceCount")
	base.RequireWaitForStat(t, func() int64 { return checkpointer.Stats().ExpectedSequenceCount }, expectedSeqCount, "ExpectedSequenceCount")
}

func getTestRevpos(t *testing.T, doc db.Body, attachmentKey string) (revpos int) {
	attachments := db.GetBodyAttachments(doc)
	if attachments == nil {
		return 0
	}
	attachment, ok := attachments[attachmentKey].(map[string]any)
	assert.True(t, ok)
	if !ok {
		return 0
	}
	revposInt64, ok := base.ToInt64(attachment["revpos"])
	assert.True(t, ok)
	return int(revposInt64)
}
