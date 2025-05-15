// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"strings"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
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
	dc.workerForVbno(snapshotMarker.VbID).Send(dc.ctx, e)
}

func (dc *DCPClient) Mutation(mutation gocbcore.DcpMutation) {

	filterKey, dcpEventType := dc.filteredKey(mutation.Key)
	if filterKey {
		return
	}

	e := mutationEvent{
		streamEventCommon: streamEventCommon{
			vbID:     mutation.VbID,
			streamID: mutation.StreamID,
		},
		seq:             mutation.SeqNo,
		revNo:           mutation.RevNo,
		flags:           mutation.Flags,
		expiry:          mutation.Expiry,
		cas:             mutation.Cas,
		datatype:        mutation.Datatype,
		collection:      mutation.CollectionID,
		DCPDocEventType: dcpEventType,

		// The byte slices must be copied to ensure that memory associated with the underlying memd mutationEvent and Packet are independent and can be released or reused by gocbcore as needed.
		key:   EfficientBytesClone(mutation.Key),
		value: EfficientBytesClone(mutation.Value),
	}
	dc.workerForVbno(mutation.VbID).Send(dc.ctx, e)
}

func (dc *DCPClient) Deletion(deletion gocbcore.DcpDeletion) {

	filterKey, dcpEventType := dc.filteredKey(deletion.Key)
	if filterKey {
		return
	}

	e := deletionEvent{
		streamEventCommon: streamEventCommon{
			vbID:     deletion.VbID,
			streamID: deletion.StreamID,
		},
		seq:             deletion.SeqNo,
		cas:             deletion.Cas,
		revNo:           deletion.RevNo,
		datatype:        deletion.Datatype,
		collection:      deletion.CollectionID,
		DCPDocEventType: dcpEventType,

		// The byte slices must be copied to ensure that memory associated with the underlying memd mutationEvent and Packet are independent and can be released or reused by gocbcore as needed.
		key:   EfficientBytesClone(deletion.Key),
		value: EfficientBytesClone(deletion.Value),
	}
	dc.workerForVbno(deletion.VbID).Send(dc.ctx, e)

}

func (dc *DCPClient) End(end gocbcore.DcpStreamEnd, err error) {

	e := endStreamEvent{
		streamEventCommon: streamEventCommon{
			vbID:     end.VbID,
			streamID: end.StreamID,
		},
		err: err}
	dc.workerForVbno(end.VbID).Send(dc.ctx, e)

}

func (dc *DCPClient) Expiration(expiration gocbcore.DcpExpiration) {
	// SG doesn't opt in to expirations, so they'll come through as deletion events
	// (cf.https://github.com/couchbase/kv_engine/blob/master/docs/dcp/documentation/expiry-opcode-output.md)
	WarnfCtx(dc.ctx, "Unexpected DCP expiration event (vb:%d) for key %v", expiration.VbID, UD(string(expiration.Key)))
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
	dc.workerForVbno(seqNoAdvanced.VbID).Send(dc.ctx, seqnoAdvancedEvent{
		streamEventCommon: streamEventCommon{
			vbID:     seqNoAdvanced.VbID,
			streamID: seqNoAdvanced.StreamID,
		},
		seq: seqNoAdvanced.SeqNo,
	})
}

// filteredKey will filter keys we don't care about off the mutation DCP feed and will return DCP event type on non-filtered docs
func (dc *DCPClient) filteredKey(key []byte) (bool, sgbucket.FeedItemDocType) {
	// if not metadata keys are defined then don't filter, this will be nil for non sharded import feed
	if dc.MetadataKeys == nil {
		return false, 0
	}
	docID := string(key)
	// any keys that doesn't have _sync prefix need to be processed
	if !strings.HasPrefix(docID, dc.MetadataKeys.SyncPrefix) {
		return false, sgbucket.FeedItemTypeCustomerDocument
	}
	if strings.HasPrefix(docID, dc.MetadataKeys.UserPrefix) {
		return false, sgbucket.FeedItemTypeUserDoc
	}
	if strings.HasPrefix(docID, dc.MetadataKeys.RolePrefix) {
		return false, sgbucket.FeedItemTypeRoleDoc
	}
	if strings.HasPrefix(docID, dc.MetadataKeys.UnusedSeqPrefix) {
		return false, sgbucket.FeedItemTypeUnusedSeqDoc
	}
	if strings.HasPrefix(docID, dc.MetadataKeys.UnusedSeqRange) {
		return false, sgbucket.FeedItemTypeUnusedSeqRangeDoc
	}
	if strings.HasPrefix(docID, dc.MetadataKeys.SgCFGPrefix) {
		return false, sgbucket.FeedItemTypeSgCFGDoc
	}

	return true, 0
}
