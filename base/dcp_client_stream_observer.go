// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"

	"github.com/couchbase/gocbcore/v10"
)

// DCPClient implementation of the gocbcore.StreamObserver interface.  Primarily routes events
// to the DCPClient's workers to be processed, but performs the following additional functionality:
//   - key-based filtering for document-based events (Deletion, Expiration, Mutation)
//   - stream End handling, including restart on error
func (dc *DCPClient) SnapshotMarker(snapshotMarker gocbcore.DcpSnapshotMarker) {

	e := snapshotEvent{
		streamEventCommon: streamEventCommon{
			vbID:     snapshotMarker.VbID,
			streamID: snapshotMarker.StreamID,
		},
		startSeq:     snapshotMarker.StartSeqNo,
		endSeq:       snapshotMarker.EndSeqNo,
		snapshotType: snapshotMarker.SnapshotType,
	}
	dc.workerForVbno(snapshotMarker.VbID).Send(e)
}

func (dc *DCPClient) Mutation(mutation gocbcore.DcpMutation) {
	if dc.afterEndSeq(mutation.VbID, mutation.SeqNo) {
		return
	}

	if dc.filteredKey(mutation.Key) {
		return
	}

	e := mutationEvent{
		streamEventCommon: streamEventCommon{
			vbID:     mutation.VbID,
			streamID: mutation.StreamID,
		},
		seq:        mutation.SeqNo,
		flags:      mutation.Flags,
		expiry:     mutation.Expiry,
		cas:        mutation.Cas,
		datatype:   mutation.Datatype,
		collection: mutation.CollectionID,
		key:        mutation.Key,
		value:      mutation.Value,
	}
	dc.workerForVbno(mutation.VbID).Send(e)
}

func (dc *DCPClient) Deletion(deletion gocbcore.DcpDeletion) {
	if dc.afterEndSeq(deletion.VbID, deletion.SeqNo) {
		return
	}

	if dc.filteredKey(deletion.Key) {
		return
	}

	e := deletionEvent{
		streamEventCommon: streamEventCommon{
			vbID:     deletion.VbID,
			streamID: deletion.StreamID,
		},
		seq:        deletion.SeqNo,
		cas:        deletion.Cas,
		datatype:   deletion.Datatype,
		collection: deletion.CollectionID,
		key:        deletion.Key,
		value:      deletion.Value,
	}
	dc.workerForVbno(deletion.VbID).Send(e)

}

func (dc *DCPClient) End(end gocbcore.DcpStreamEnd, err error) {

	e := endStreamEvent{
		streamEventCommon: streamEventCommon{
			vbID:     end.VbID,
			streamID: end.StreamID,
		},
		err: err}
	dc.workerForVbno(end.VbID).Send(e)

}

func (dc *DCPClient) Expiration(expiration gocbcore.DcpExpiration) {
	// SG doesn't opt in to expirations, so they'll come through as deletion events
	// (cf.https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/expiry-opcode-output.md)
	WarnfCtx(context.TODO(), "Unexpected DCP expiration event (vb:%d) for key %v", expiration.VbID, UD(string(expiration.Key)))
}

func (dc *DCPClient) CreateCollection(creation gocbcore.DcpCollectionCreation) {
	// Not used by SG at this time
}

func (dc *DCPClient) DeleteCollection(deletion gocbcore.DcpCollectionDeletion) {
	// Not used by SG at this time
}

func (dc *DCPClient) FlushCollection(flush gocbcore.DcpCollectionFlush) {
	// Not used by SG at this time
}

func (dc *DCPClient) CreateScope(creation gocbcore.DcpScopeCreation) {
	// Not used by SG at this time
}

func (dc *DCPClient) DeleteScope(deletion gocbcore.DcpScopeDeletion) {
	// Not used by SG at this time
}

func (dc *DCPClient) ModifyCollection(modification gocbcore.DcpCollectionModification) {
	// Not used by SG at this time
}

func (dc *DCPClient) OSOSnapshot(snapshot gocbcore.DcpOSOSnapshot) {
	// Not used by SG at this time
}

func (dc *DCPClient) SeqNoAdvanced(seqNoAdvanced gocbcore.DcpSeqNoAdvanced) {
	dc.workerForVbno(seqNoAdvanced.VbID).Send(seqnoAdvancedEvent{
		streamEventCommon: streamEventCommon{
			vbID:     seqNoAdvanced.VbID,
			streamID: seqNoAdvanced.StreamID,
		},
		seq: seqNoAdvanced.SeqNo,
	})
}

// A one-shot DCP feed specifies the endSeq when opening the stream, but Couchbase Server only ends the DCP stream
// when it reaches the end of a snapshot that is greater than or equal to the endSeq value.  DCPClient should not
// process any sequences in that final snapshot that are greater than endSeq - afterEndSeq provides that additional check.
func (dc *DCPClient) afterEndSeq(vbID uint16, seq uint64) bool {
	if len(dc.endSeqNos) > 0 && dc.endSeqNos[vbID] < seq {
		return true
	}
	return false
}

func (dc *DCPClient) filteredKey(key []byte) bool {
	return false
}
