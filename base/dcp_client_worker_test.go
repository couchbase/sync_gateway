// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"testing"
	"time"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"

	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

// persistCountingMetadata is an in-memory DCPMetadataStore that counts Persist calls.
type persistCountingMetadata struct {
	*DCPMetadataMem
	persistCount int
}

func (m *persistCountingMetadata) Persist(_ context.Context, _ int, _ []uint16) {
	m.persistCount++
}

func newTestDCPWorker(t *testing.T, callback sgbucket.FeedEventCallbackFunc, checkpointPrefix string, persistFrequency time.Duration, ignoreDeletes bool) (*DCPWorker, *persistCountingMetadata) {
	metadata := &persistCountingMetadata{DCPMetadataMem: NewDCPMetadataMem(1)}
	options := &DCPWorkerOptions{
		ignoreDeletes:        ignoreDeletes,
		metaPersistFrequency: &persistFrequency,
	}
	endStream := func(endStreamEvent) { require.FailNow(t, "unexpected end stream event") }
	return NewDCPWorker(0, metadata, callback, endStream, make(chan bool), nil, checkpointPrefix, []uint16{0}, options), metadata
}

func TestDCPWorkerCheckpointPersistence(t *testing.T) {
	const checkpointPrefix = "_sync:dcp_ck:test:"
	const seq = 10
	testCases := []struct {
		name             string
		event            streamEvent
		callbackResult   bool
		ignoreDeletes    bool
		checkpointPrefix string
		expectedPersists int
		expectedSeq      gocbcore.SeqNo
	}{
		{
			name:             "mutation, callback requests checkpoint",
			event:            mutationEvent{key: []byte("doc1"), seq: seq},
			callbackResult:   true,
			checkpointPrefix: checkpointPrefix,
			expectedPersists: 1,
			expectedSeq:      seq,
		},
		{
			name:             "mutation, callback declines checkpoint",
			event:            mutationEvent{key: []byte("doc1"), seq: seq},
			callbackResult:   false,
			checkpointPrefix: checkpointPrefix,
			expectedPersists: 0,
			expectedSeq:      seq,
		},
		{
			name:             "deletion, callback requests checkpoint",
			event:            deletionEvent{key: []byte("doc1"), seq: seq},
			callbackResult:   true,
			checkpointPrefix: checkpointPrefix,
			expectedPersists: 1,
			expectedSeq:      seq,
		},
		{
			name:             "deletion, callback declines checkpoint",
			event:            deletionEvent{key: []byte("doc1"), seq: seq},
			callbackResult:   false,
			checkpointPrefix: checkpointPrefix,
			expectedPersists: 0,
			expectedSeq:      seq,
		},
		{
			name:             "deletion with ignoreDeletes skips callback and persists",
			event:            deletionEvent{key: []byte("doc1"), seq: seq},
			callbackResult:   false,
			ignoreDeletes:    true,
			checkpointPrefix: checkpointPrefix,
			expectedPersists: 1,
			expectedSeq:      seq,
		},
		{
			name:             "checkpoint document is not checkpointed",
			event:            mutationEvent{key: []byte(checkpointPrefix + "0"), seq: seq},
			callbackResult:   true,
			checkpointPrefix: checkpointPrefix,
			expectedPersists: 0,
			expectedSeq:      0,
		},
		{
			name:             "seqno advanced does not persist",
			event:            seqnoAdvancedEvent{seq: seq},
			checkpointPrefix: checkpointPrefix,
			expectedPersists: 0,
			expectedSeq:      seq,
		},
		{
			name:             "empty checkpoint prefix still updates sequence",
			event:            mutationEvent{key: []byte("doc1"), seq: seq},
			callbackResult:   true,
			checkpointPrefix: "",
			expectedPersists: 1,
			expectedSeq:      seq,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			callbackCount := 0
			callback := func(sgbucket.FeedEvent) bool {
				callbackCount++
				return tc.callbackResult
			}
			worker, metadata := newTestDCPWorker(t, callback, tc.checkpointPrefix, 0, tc.ignoreDeletes)
			worker.handleEvent(TestCtx(t), tc.event)
			assert.Equal(t, tc.expectedPersists, metadata.persistCount)
			assert.Equal(t, tc.expectedSeq, metadata.GetMeta(0).StartSeqNo)
			if _, ok := tc.event.(seqnoAdvancedEvent); ok || tc.ignoreDeletes {
				assert.Equal(t, 0, callbackCount)
			} else {
				assert.Equal(t, 1, callbackCount)
			}
		})
	}
}

func TestDCPWorkerCheckpointPersistFrequency(t *testing.T) {
	ctx := TestCtx(t)
	callbackResult := true
	callback := func(sgbucket.FeedEvent) bool { return callbackResult }
	worker, metadata := newTestDCPWorker(t, callback, "_sync:dcp_ck:test:", time.Hour, false)

	// a declined checkpoint does not start the persist interval
	callbackResult = false
	worker.handleEvent(ctx, mutationEvent{key: []byte("doc1"), seq: 1})
	require.Equal(t, 0, metadata.persistCount)

	callbackResult = true
	worker.handleEvent(ctx, mutationEvent{key: []byte("doc2"), seq: 2})
	require.Equal(t, 1, metadata.persistCount)

	// within the persist interval, so no further persistence
	worker.handleEvent(ctx, mutationEvent{key: []byte("doc3"), seq: 3})
	worker.handleEvent(ctx, seqnoAdvancedEvent{seq: 4})
	require.Equal(t, 1, metadata.persistCount)
	require.Equal(t, gocbcore.SeqNo(4), metadata.GetMeta(0).StartSeqNo)

	// close flushes the deferred checkpoint
	worker.Close(ctx)
	require.Equal(t, 2, metadata.persistCount)
	worker.Close(ctx)
	require.Equal(t, 2, metadata.persistCount)
}

func TestDCPWorkerCloseWithoutRequestedCheckpoint(t *testing.T) {
	ctx := TestCtx(t)
	callback := func(sgbucket.FeedEvent) bool { return false }
	worker, metadata := newTestDCPWorker(t, callback, "_sync:dcp_ck:test:", 0, false)
	worker.handleEvent(ctx, mutationEvent{key: []byte("doc1"), seq: 1})
	worker.handleEvent(ctx, seqnoAdvancedEvent{seq: 2})
	worker.Close(ctx)
	require.Equal(t, 0, metadata.persistCount)
}

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
		// A zero persist frequency makes each event persist.
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
				&DCPWorkerOptions{idleCheckpointOps: testCase.idleCheckpointOps, metaPersistFrequency: new(time.Duration)})

			idleKVOpsBefore := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value()
			for i, key := range testCase.keys {
				worker.updateSeq(ctx, key, 0, uint64(i+1), true)
			}
			idleKVOps := SyncGatewayStats.GlobalStats.ResourceUtilizationStats().NumIdleKvOps.Value() - idleKVOpsBefore

			assert.Equal(t, testCase.expectedIdleKVOps, idleKVOps)
			_, _, err := bucket.GetSingleDataStore().GetRaw(ctx, metadata.getMetadataKey(0))
			require.NoError(t, err, "checkpoint must be written whether or not it is counted as idle")
		})
	}
}
