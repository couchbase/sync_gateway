// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"github.com/couchbase/gocbcore/v10"
)

// DCPClient implementation of the gocbcore.StreamObserver interface.  Primarily routes events
// to the DCPClient's workers to be processed, but performs the following additional functionality:
//   - key-based filtering for document-based events (Deletion, Expiration, Mutation)
//   - stream End handling, including restart on error
func (dc *GoCBDCPClient) SnapshotMarker(snapshotMarker gocbcore.DcpSnapshotMarker) {

	e := snapshotEvent{
		streamEventCommon: streamEventCommon{
			vbID:     snapshotMarker.VbID,
			streamID: snapshotMarker.StreamID,
		},
		startSeq:     snapshotMarker.StartSeqNo,
		endSeq:       snapshotMarker.EndSeqNo,
		snapshotType: snapshotMarker.SnapshotType,
	}
	dc.workerForVbno(snapshotMarker.VbID).Send(dc.ctx, e)
}

func (dc *GoCBDCPClient) Mutation(mutation gocbcore.DcpMutation) {
	InfofCtx(dc.ctx, KeyDCP, "CBG-4640 DEBUG DCPClient.Mutation: key=%s vbID=%d collectionID=%d datatype=%d valueLen=%d seqNo=%d cas=%d", UD(string(mutation.Key)), mutation.VbID, mutation.CollectionID, mutation.Datatype, len(mutation.Value), mutation.SeqNo, mutation.Cas)

	if dc.filteredKey(mutation.Key) {
		return
	}

	e := mutationEvent{
		streamEventCommon: streamEventCommon{
			vbID:     mutation.VbID,
			streamID: mutation.StreamID,
		},
		seq:        mutation.SeqNo,
		revNo:      mutation.RevNo,
		flags:      mutation.Flags,
		expiry:     mutation.Expiry,
		cas:        mutation.Cas,
		datatype:   mutation.Datatype,
		collection: mutation.CollectionID,

		// The byte slices must be copied to ensure that memory associated with the underlying memd mutationEvent and Packet are independent and can be released or reused by gocbcore as needed.
		key:   EfficientBytesClone(mutation.Key),
		value: EfficientBytesClone(mutation.Value),
	}
	dc.workerForVbno(mutation.VbID).Send(dc.ctx, e)
}

func (dc *GoCBDCPClient) Deletion(deletion gocbcore.DcpDeletion) {
	InfofCtx(dc.ctx, KeyDCP, "CBG-4640 DEBUG DCPClient.Deletion: key=%s vbID=%d collectionID=%d datatype=%d valueLen=%d seqNo=%d cas=%d", UD(string(deletion.Key)), deletion.VbID, deletion.CollectionID, deletion.Datatype, len(deletion.Value), deletion.SeqNo, deletion.Cas)

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
		revNo:      deletion.RevNo,
		datatype:   deletion.Datatype,
		collection: deletion.CollectionID,

		// The byte slices must be copied to ensure that memory associated with the underlying memd mutationEvent and Packet are independent and can be released or reused by gocbcore as needed.
		key:   EfficientBytesClone(deletion.Key),
		value: EfficientBytesClone(deletion.Value),
	}
	dc.workerForVbno(deletion.VbID).Send(dc.ctx, e)

}

func (dc *GoCBDCPClient) End(end gocbcore.DcpStreamEnd, err error) {

	e := endStreamEvent{
		streamEventCommon: streamEventCommon{
			vbID:     end.VbID,
			streamID: end.StreamID,
		},
		err: err}
	dc.workerForVbno(end.VbID).Send(dc.ctx, e)

}

func (dc *GoCBDCPClient) Expiration(expiration gocbcore.DcpExpiration) {
	// SG doesn't opt in to expirations, so they'll come through as deletion events
	// (cf.https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/expiry-opcode-output.md)
	WarnfCtx(dc.ctx, "Unexpected DCP expiration event (vb:%d) for key %v", expiration.VbID, UD(string(expiration.Key)))
}

func (dc *GoCBDCPClient) CreateCollection(creation gocbcore.DcpCollectionCreation) {
	// Not used by SG at this time
}

func (dc *GoCBDCPClient) DeleteCollection(deletion gocbcore.DcpCollectionDeletion) {
	// Not used by SG at this time
}

func (dc *GoCBDCPClient) FlushCollection(flush gocbcore.DcpCollectionFlush) {
	// Not used by SG at this time
}

func (dc *GoCBDCPClient) CreateScope(creation gocbcore.DcpScopeCreation) {
	// Not used by SG at this time
}

func (dc *GoCBDCPClient) DeleteScope(deletion gocbcore.DcpScopeDeletion) {
	// Not used by SG at this time
}

func (dc *GoCBDCPClient) ModifyCollection(modification gocbcore.DcpCollectionModification) {
	// Not used by SG at this time
}

func (dc *GoCBDCPClient) OSOSnapshot(snapshot gocbcore.DcpOSOSnapshot) {
	// Not used by SG at this time
}

func (dc *GoCBDCPClient) SeqNoAdvanced(seqNoAdvanced gocbcore.DcpSeqNoAdvanced) {
	InfofCtx(dc.ctx, KeyDCP, "CBG-4640 DEBUG DCPClient.SeqNoAdvanced: vbID=%d seqNo=%d", seqNoAdvanced.VbID, seqNoAdvanced.SeqNo)
	dc.workerForVbno(seqNoAdvanced.VbID).Send(dc.ctx, seqnoAdvancedEvent{
		streamEventCommon: streamEventCommon{
			vbID:     seqNoAdvanced.VbID,
			streamID: seqNoAdvanced.StreamID,
		},
		seq: seqNoAdvanced.SeqNo,
	})
}

func (dc *GoCBDCPClient) filteredKey(key []byte) bool {
	return false
}
