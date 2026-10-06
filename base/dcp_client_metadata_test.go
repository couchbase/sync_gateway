// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"sync"
	"testing"

	"github.com/couchbase/gocbcore/v10"
)

// TestDCPMetadataCSSetFailoverEntriesDuringPersist mirrors GoCBDCPClient, where the OpenStream callback sets failover
// entries on a gocbcore goroutine while the DCP worker persists metadata for every vbucket assigned to it.
func TestDCPMetadataCSSetFailoverEntriesDuringPersist(t *testing.T) {
	ctx := TestCtx(t)
	bucket := GetTestBucket(t)
	defer bucket.Close(ctx)

	vbIDs := []uint16{0, 1, 2, 3}
	metadata := NewDCPMetadataCS(ctx, bucket.GetSingleDataStore(), uint16(len(vbIDs)), 1, t.Name())

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			vbID := vbIDs[i%len(vbIDs)]
			metadata.SetFailoverEntries(vbID, []gocbcore.FailoverEntry{{VbUUID: gocbcore.VbUUID(i), SeqNo: gocbcore.SeqNo(i)}})
		}
	})
	for range 100 {
		metadata.Persist(ctx, 0, vbIDs)
	}
	wg.Wait()
}

// TestDCPMetadataRollbackDuringUpdateSeq mirrors GoCBDCPClient, where openStream rolls back and reads a vbucket's
// metadata, and GetMetadata reads every vbucket, while the owning DCP worker records snapshots and sequences.
func TestDCPMetadataRollbackDuringUpdateSeq(t *testing.T) {
	ctx := TestCtx(t)
	const vbID = 0
	metadata := NewDCPMetadataMem(1)

	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			metadata.SetSnapshot(snapshotEvent{streamEventCommon: streamEventCommon{vbID: vbID}, startSeq: uint64(i), endSeq: uint64(i + 1)})
			metadata.UpdateSeq(vbID, uint64(i))
		}
	})
	for i := range 100 {
		metadata.Rollback(ctx, vbID, gocbcore.SeqNo(i))
		_ = metadata.GetMeta(vbID)
	}
	wg.Wait()
}
