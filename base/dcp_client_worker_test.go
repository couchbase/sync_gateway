// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"testing"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// TestDCPWorkerCountsIdleCheckpoints checks that a checkpoint write made when only _sync: docs were seen since the last
// one is counted as an idle KV op, and that the checkpoint is still written.
func TestDCPWorkerCountsIdleCheckpoints(t *testing.T) {
	testCases := []struct {
		name              string
		keys              [][]byte // nil is an event with no document, such as a seqno advance
		idleCheckpointOps bool
		expectedIdleKVOps int64
	}{
		{name: "_sync doc", keys: [][]byte{[]byte("_sync:heartbeat")}, idleCheckpointOps: true, expectedIdleKVOps: 1},
		{name: "client doc", keys: [][]byte{[]byte("doc1")}, idleCheckpointOps: true, expectedIdleKVOps: 0},
		{name: "seqno advance", keys: [][]byte{nil}, idleCheckpointOps: true, expectedIdleKVOps: 0},
		{name: "counting off", keys: [][]byte{[]byte("_sync:heartbeat")}, idleCheckpointOps: false, expectedIdleKVOps: 0},
		// Each event persists, because the worker's persist frequency is measured from a time that is never set.
		{name: "_sync then client doc", keys: [][]byte{[]byte("_sync:heartbeat"), []byte("doc1")}, idleCheckpointOps: true, expectedIdleKVOps: 1},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := TestCtx(t)
			bucket := GetTestBucket(t)
			defer bucket.Close(ctx)

			checkpointPrefix := t.Name() + "_"
			metadata := NewDCPMetadataCS(ctx, bucket.GetSingleDataStore(), 1, 1, checkpointPrefix)
			worker := NewDCPWorker(0, metadata, nil, nil, make(chan bool), nil, checkpointPrefix, []uint16{0},
				&DCPWorkerOptions{idleCheckpointOps: testCase.idleCheckpointOps})

			idleKVOpsBefore := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value()
			for i, key := range testCase.keys {
				worker.updateSeq(ctx, key, 0, uint64(i+1))
			}
			idleKVOps := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value() - idleKVOpsBefore

			assert.Equal(t, testCase.expectedIdleKVOps, idleKVOps)
			_, _, err := bucket.GetSingleDataStore().GetRaw(ctx, metadata.getMetadataKey(0))
			require.NoError(t, err, "checkpoint must be written whether or not it is counted as idle")
		})
	}
}
