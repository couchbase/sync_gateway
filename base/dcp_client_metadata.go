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
	"fmt"
	"math"

	"github.com/couchbase/gocbcore/v10"
)

type DCPMetadataStoreType int

const (
	// DCPMetadataCS uses CouchbaseBucketStore interface backed metadata storage
	DCPMetadataStoreCS = iota
	// DCPMetadataInMemory uses in memory metadata storage
	DCPMetadataStoreInMemory
)

type DCPMetadata struct {
	VbUUID          gocbcore.VbUUID
	StartSeqNo      gocbcore.SeqNo
	EndSeqNo        gocbcore.SeqNo
	SnapStartSeqNo  gocbcore.SeqNo
	SnapEndSeqNo    gocbcore.SeqNo
	FailoverEntries []gocbcore.FailoverEntry
}

type DCPMetadataStore interface {
	// Rollback resets vBucket metadata, preserving endSeqno
	Rollback(ctx context.Context, vbID uint16, vbUUID gocbcore.VbUUID, startSeqNo gocbcore.SeqNo)

	// SetMeta updates the DCPMetadata for a vbucket
	SetMeta(vbID uint16, meta DCPMetadata)

	// GetMeta retrieves DCPMetadata for a vbucket
	GetMeta(vbID uint16) DCPMetadata

	// SetEndSeqNos sets the end sequence numbers for all specified vbuckets
	SetEndSeqNos(map[uint16]uint64)

	// SetSnapshot updates the metadata based on a DCP snapshotEvent
	SetSnapshot(e snapshotEvent)

	// UpdateSeq updates the last sequence processed for a vbucket
	UpdateSeq(vbID uint16, seq uint64)

	// SetFailoverEntries sets the failover history (vbuUUID, seq) for a vbucket
	SetFailoverEntries(vbID uint16, entries []gocbcore.FailoverEntry)

	// Persist writes the metadata for the specified workerID and vbucket IDs to the backing store
	Persist(workerID int, vbIDs []uint16)

	// Purge removes all metadata associated with the metadata store from the bucket.  It does not remove the
	// in-memory metadata.
	Purge(numWorkers int)
}

type DCPMetadataMem struct {
	metadata []DCPMetadata
}

func NewDCPMetadataMem(numVbuckets uint16) *DCPMetadataMem {
	m := &DCPMetadataMem{
		metadata: make([]DCPMetadata, numVbuckets),
	}
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		m.metadata[vbNo] = DCPMetadata{
			FailoverEntries: make([]gocbcore.FailoverEntry, 0),
			EndSeqNo:        math.MaxUint64,
		}
	}
	return m
}

// Rollback resets the metadata, preserving EndSeqNo
func (m *DCPMetadataMem) Rollback(ctx context.Context, vbID uint16, vbUUID gocbcore.VbUUID, startSeqNo gocbcore.SeqNo) {
	m.metadata[vbID] = DCPMetadata{
		VbUUID:          vbUUID,
		StartSeqNo:      startSeqNo,
		EndSeqNo:        m.metadata[vbID].EndSeqNo,
		SnapStartSeqNo:  startSeqNo,
		SnapEndSeqNo:    startSeqNo,
		FailoverEntries: make([]gocbcore.FailoverEntry, 0),
	}
	InfofCtx(ctx, KeyDCP, "rolling back vb:%d with metadata set to %v", vbID, m.metadata[vbID])
}

func (m *DCPMetadataMem) SetMeta(vbID uint16, meta DCPMetadata) {
	m.metadata[vbID] = meta
}

func (m *DCPMetadataMem) GetMeta(vbID uint16) DCPMetadata {
	return m.metadata[vbID]
}

func (m *DCPMetadataMem) SetSnapshot(e snapshotEvent) {
	m.metadata[e.vbID].SnapStartSeqNo = gocbcore.SeqNo(e.startSeq)
	m.metadata[e.vbID].SnapEndSeqNo = gocbcore.SeqNo(e.endSeq)
}

func (m *DCPMetadataMem) UpdateSeq(vbID uint16, seq uint64) {
	m.metadata[vbID].StartSeqNo = gocbcore.SeqNo(seq)
}

func (m *DCPMetadataMem) SetFailoverEntries(vbID uint16, fe []gocbcore.FailoverEntry) {
	m.metadata[vbID].FailoverEntries = fe
	m.metadata[vbID].VbUUID = getVbUUID(fe, m.metadata[vbID].StartSeqNo)
}

// SetEndSeqNos will update the metadata endSeqNos to the values provided.  Vbuckets not
// present in the endSeqNos map will have their EndSeqNo set to zero.
func (m *DCPMetadataMem) SetEndSeqNos(endSeqNos map[uint16]uint64) {
	for i := 0; i < len(m.metadata); i++ {
		endSeqNo, _ := endSeqNos[uint16(i)]
		m.metadata[i].EndSeqNo = gocbcore.SeqNo(endSeqNo)
	}
}

// Persist is no-op for in-memory metadata store
func (md *DCPMetadataMem) Persist(workerID int, vbIDs []uint16) {
	return
}

// Purge is no-op for in-memory metadata store
func (md *DCPMetadataMem) Purge(numWorkers int) {
	return
}

// Reset sets metadata sequences to zero, but maintains vbucket UUID and failover entries.  Used for scenarios
// that want to restart a feed from zero, but detect failover
func (md *DCPMetadata) Reset() {
	md.SnapStartSeqNo = 0
	md.SnapEndSeqNo = 0
	md.StartSeqNo = 0
	md.EndSeqNo = 0
}

func GetVBUUIDs(metadata []DCPMetadata) []uint64 {
	uuids := make([]uint64, 0, len(metadata))
	for _, meta := range metadata {
		uuids = append(uuids, uint64(meta.VbUUID))
	}
	return uuids
}

func BuildDCPMetadataSliceFromVBUUIDs(vbUUIDS []uint64) []DCPMetadata {
	metadata := make([]DCPMetadata, 0, len(vbUUIDS))
	for _, vbUUID := range vbUUIDS {
		metadata = append(metadata, DCPMetadata{
			VbUUID: gocbcore.VbUUID(vbUUID),
		})
	}
	return metadata
}

// DCPMetadataCS stores DCP metadata in the specified CouchbaseBucketStore.  It does not require that the store is the
// same one being streamed over DCP.
type DCPMetadataCS struct {
	dataStore DataStore
	keyPrefix string
	metadata  []DCPMetadata
}

func NewDCPMetadataCS(store DataStore, numVbuckets uint16, numWorkers int, keyPrefix string) *DCPMetadataCS {

	m := &DCPMetadataCS{
		dataStore: store,
		keyPrefix: keyPrefix,
		metadata:  make([]DCPMetadata, numVbuckets),
	}
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		m.metadata[vbNo] = DCPMetadata{
			FailoverEntries: make([]gocbcore.FailoverEntry, 0),
			EndSeqNo:        math.MaxUint64,
		}
	}

	// Initialize any persisted metadata
	for i := 0; i < numWorkers; i++ {
		m.load(i)
	}

	return m
}

func (m *DCPMetadataCS) Rollback(ctx context.Context, vbID uint16, vbUUID gocbcore.VbUUID, startSeqNo gocbcore.SeqNo) {
	m.metadata[vbID] = DCPMetadata{
		VbUUID:          vbUUID,
		StartSeqNo:      startSeqNo,
		EndSeqNo:        m.metadata[vbID].EndSeqNo,
		SnapStartSeqNo:  startSeqNo,
		SnapEndSeqNo:    startSeqNo,
		FailoverEntries: make([]gocbcore.FailoverEntry, 0),
	}
	TracefCtx(ctx, KeyDCP, "rolling back vb:%d with metadata set to %+v", vbID, m.metadata[vbID])
}

func (m *DCPMetadataCS) SetMeta(vbNo uint16, metadata DCPMetadata) {
	m.metadata[vbNo] = metadata
}

func (m *DCPMetadataCS) GetMeta(vbNo uint16) DCPMetadata {
	return m.metadata[vbNo]
}

func (m *DCPMetadataCS) SetSnapshot(e snapshotEvent) {
	m.metadata[e.vbID].SnapStartSeqNo = gocbcore.SeqNo(e.startSeq)
	m.metadata[e.vbID].SnapEndSeqNo = gocbcore.SeqNo(e.endSeq)
}

func (m *DCPMetadataCS) UpdateSeq(vbNo uint16, seq uint64) {
	m.metadata[vbNo].StartSeqNo = gocbcore.SeqNo(seq)
}

func (m *DCPMetadataCS) SetFailoverEntries(vbID uint16, fe []gocbcore.FailoverEntry) {
	m.metadata[vbID].FailoverEntries = fe
	m.metadata[vbID].VbUUID = getVbUUID(fe, m.metadata[vbID].StartSeqNo)
}

// SetEndSeqNos will update the metadata endSeqNos to the values provided.  Vbuckets not
// present in the endSeqNos map will have their EndSeqNo set to zero.
func (m *DCPMetadataCS) SetEndSeqNos(endSeqNos map[uint16]uint64) {
	for i := 0; i < len(m.metadata); i++ {
		endSeqNo, _ := endSeqNos[uint16(i)]
		m.metadata[i].EndSeqNo = gocbcore.SeqNo(endSeqNo)
	}
}

// Persist is called by worker.  Triggers persistence of metadata for all listed vbuckets.  This set must be the same
// set that has been assigned to the worker.  There's no synchronization on m.metadata - relies on DCP worker to
// avoid read/write races on vbucket data.  Calls to persist must be blocking on the worker goroutine, and vbuckets are
// only assigned to a single worker
func (m *DCPMetadataCS) Persist(workerID int, vbIDs []uint16) {

	meta := WorkerMetadata{}
	meta.DCPMeta = make(map[uint16]DCPMetadata)
	for _, vbID := range vbIDs {
		meta.DCPMeta[vbID] = m.metadata[vbID]
	}
	err := m.dataStore.Set(m.getMetadataKey(workerID), 0, nil, meta)
	if err != nil {
		InfofCtx(context.TODO(), KeyDCP, "Unable to persist DCP metadata: %v", err)
	} else {
		TracefCtx(context.TODO(), KeyDCP, "Persisted metadata for worker %d: %v", workerID, meta)
		// log.Printf("Persisted metadata for worker %d (%s): %v", workerID, m.getMetadataKey(workerID), meta)
	}
	return
}

func (m *DCPMetadataCS) load(workerID int) {
	var meta WorkerMetadata
	_, err := m.dataStore.Get(m.getMetadataKey(workerID), &meta)
	if err != nil {
		if IsKeyNotFoundError(m.dataStore, err) {
			return
		}
		InfofCtx(context.TODO(), KeyDCP, "Error loading persisted metadata - metadata will be reset for worker %d: %s", workerID, err)
	}

	TracefCtx(context.TODO(), KeyDCP, "Loaded metadata for worker %d: %v", workerID, meta)
	// log.Printf("Loaded metadata for worker %d (%s): %v", workerID, m.getMetadataKey(workerID), meta)
	for vbID, metadata := range meta.DCPMeta {
		m.metadata[vbID] = metadata
	}
}

func (m *DCPMetadataCS) Purge(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		err := m.dataStore.Delete(m.getMetadataKey(i))
		if err != nil && !IsKeyNotFoundError(m.dataStore, err) {
			InfofCtx(context.TODO(), KeyDCP, "Unable to remove DCP checkpoint for key %s: %v", m.getMetadataKey(i), err)
		}
	}
}

func (m *DCPMetadataCS) getMetadataKey(workerID int) string {
	return fmt.Sprintf("%s%d", m.keyPrefix, workerID)
}

type WorkerMetadata struct {
	DCPMeta map[uint16]DCPMetadata
}
