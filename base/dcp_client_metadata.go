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
	"sync"

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
	// Rollback resets vBucket metadata to the vBucket UUID and sequence number provided
	Rollback(ctx context.Context, vbID uint16, startSeqNo gocbcore.SeqNo)

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
	Persist(ctx context.Context, workerID int, vbIDs []uint16)

	// Purge removes all metadata associated with the metadata store from the bucket.  It does not remove the
	// in-memory metadata.
	Purge(ctx context.Context, numWorkers int)

	// GetKeyPrefix will retrieve the key prefix used for metadata persistence
	GetKeyPrefix() string
}

type dcpMetadataBase struct {
	_metadata []DCPMetadata
	// vbLocks guards _metadata per vbucket, which is written by the owning DCP worker, the OpenStream callback and
	// rollback, and read by GetMetadata.
	vbLocks []sync.Mutex
}

func newDCPMetadataBase(numVbuckets uint16) dcpMetadataBase {
	m := dcpMetadataBase{
		_metadata: make([]DCPMetadata, numVbuckets),
		vbLocks:   make([]sync.Mutex, numVbuckets),
	}
	for vbNo := range numVbuckets {
		m._metadata[vbNo] = DCPMetadata{
			FailoverEntries: make([]gocbcore.FailoverEntry, 0),
			EndSeqNo:        math.MaxUint64,
		}
	}
	return m
}

type DCPMetadataMem struct {
	dcpMetadataBase
}

// verify these types match the interface
var (
	_ DCPMetadataStore = &DCPMetadataCS{}
	_ DCPMetadataStore = &DCPMetadataMem{}
)

func NewDCPMetadataMem(numVbuckets uint16) *DCPMetadataMem {
	return &DCPMetadataMem{dcpMetadataBase: newDCPMetadataBase(numVbuckets)}
}

// Rollback resets vBucket metadata to the vBucket UUID and sequence number provided
func (m *dcpMetadataBase) Rollback(ctx context.Context, vbID uint16, startSeqNo gocbcore.SeqNo) {
	meta := m.rollback(vbID, startSeqNo)
	TracefCtx(ctx, KeyDCP, "rolling back vb:%d with metadata set to %+v", vbID, meta)
}

// rollback applies the rollback under the vbucket lock and returns a copy of the updated metadata.
func (m *dcpMetadataBase) rollback(vbID uint16, startSeqNo gocbcore.SeqNo) DCPMetadata {
	m.vbLocks[vbID].Lock()
	defer m.vbLocks[vbID].Unlock()
	var rollbackVbuuid gocbcore.VbUUID
	for _, failoverLog := range m._metadata[vbID].FailoverEntries {
		if failoverLog.SeqNo <= startSeqNo {
			rollbackVbuuid = failoverLog.VbUUID
			break
		}
	}
	// use the lower value of the start sequence number that we last saved, or the value that was provided from KV as the rollback point
	newStartSeqNo := min(startSeqNo, m._metadata[vbID].StartSeqNo)
	m._metadata[vbID].VbUUID = rollbackVbuuid
	m._metadata[vbID].StartSeqNo = newStartSeqNo
	m._metadata[vbID].SnapStartSeqNo = newStartSeqNo
	m._metadata[vbID].SnapEndSeqNo = newStartSeqNo
	return m._metadata[vbID]
}

func (m *dcpMetadataBase) SetMeta(vbID uint16, meta DCPMetadata) {
	m.vbLocks[vbID].Lock()
	defer m.vbLocks[vbID].Unlock()
	m._metadata[vbID] = meta
}

func (m *dcpMetadataBase) GetMeta(vbID uint16) DCPMetadata {
	m.vbLocks[vbID].Lock()
	defer m.vbLocks[vbID].Unlock()
	return m._metadata[vbID]
}

func (m *dcpMetadataBase) SetSnapshot(e snapshotEvent) {
	m.vbLocks[e.vbID].Lock()
	defer m.vbLocks[e.vbID].Unlock()
	m._metadata[e.vbID].SnapStartSeqNo = gocbcore.SeqNo(e.startSeq)
	m._metadata[e.vbID].SnapEndSeqNo = gocbcore.SeqNo(e.endSeq)
}

func (m *dcpMetadataBase) UpdateSeq(vbID uint16, seq uint64) {
	m.vbLocks[vbID].Lock()
	defer m.vbLocks[vbID].Unlock()
	m._metadata[vbID].StartSeqNo = gocbcore.SeqNo(seq)
}

func (m *dcpMetadataBase) SetFailoverEntries(vbID uint16, fe []gocbcore.FailoverEntry) {
	m.vbLocks[vbID].Lock()
	defer m.vbLocks[vbID].Unlock()
	m._metadata[vbID].FailoverEntries = fe
	m._metadata[vbID].VbUUID = getVbUUID(fe, m._metadata[vbID].StartSeqNo)
}

// SetEndSeqNos will update the metadata endSeqNos to the values provided.  Vbuckets not
// present in the endSeqNos map will have their EndSeqNo set to zero.
func (m *dcpMetadataBase) SetEndSeqNos(endSeqNos map[uint16]uint64) {
	for i := range len(m._metadata) {
		m.setEndSeqNo(uint16(i), gocbcore.SeqNo(endSeqNos[uint16(i)]))
	}
}

func (m *dcpMetadataBase) setEndSeqNo(vbID uint16, endSeqNo gocbcore.SeqNo) {
	m.vbLocks[vbID].Lock()
	defer m.vbLocks[vbID].Unlock()
	m._metadata[vbID].EndSeqNo = endSeqNo
}

// Persist is no-op for in-memory metadata store
func (md *DCPMetadataMem) Persist(_ context.Context, workerID int, vbIDs []uint16) {
}

// Purge is no-op for in-memory metadata store
func (md *DCPMetadataMem) Purge(_ context.Context, numWorkers int) {
}

func (md *DCPMetadataMem) GetKeyPrefix() string {
	return ""
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
	dcpMetadataBase
}

func NewDCPMetadataCS(ctx context.Context, store DataStore, numVbuckets uint16, numWorkers int, keyPrefix string) *DCPMetadataCS {

	m := &DCPMetadataCS{
		dataStore:       store,
		keyPrefix:       keyPrefix,
		dcpMetadataBase: newDCPMetadataBase(numVbuckets),
	}

	// Initialize any persisted metadata
	for i := range numWorkers {
		m.load(ctx, i)
	}

	return m
}

// Persist is called by worker.  Triggers persistence of metadata for all listed vbuckets.  This set must be the same
// set that has been assigned to the worker.  Each vbucket is copied under its lock, and the write happens outside it.
func (m *DCPMetadataCS) Persist(ctx context.Context, workerID int, vbIDs []uint16) {

	meta := WorkerMetadata{}
	meta.DCPMeta = make(map[uint16]DCPMetadata, len(vbIDs))
	for _, vbID := range vbIDs {
		meta.DCPMeta[vbID] = m.GetMeta(vbID)
	}
	err := m.dataStore.Set(ctx, m.getMetadataKey(workerID), 0, nil, meta)
	if err != nil {
		InfofCtx(ctx, KeyDCP, "Unable to persist DCP metadata: %v", err)
	} else {
		TracefCtx(ctx, KeyDCP, "Persisted metadata for worker %d: %v", workerID, meta)
	}
}

func (m *DCPMetadataCS) load(ctx context.Context, workerID int) {
	var meta WorkerMetadata
	_, err := m.dataStore.Get(ctx, m.getMetadataKey(workerID), &meta)
	if err != nil {
		if IsDocNotFoundError(err) {
			return
		}
		InfofCtx(ctx, KeyDCP, "Error loading persisted metadata - metadata will be reset for worker %d: %s", workerID, err)
	}

	TracefCtx(ctx, KeyDCP, "Loaded metadata for worker %d: %v", workerID, meta)
	for vbID, metadata := range meta.DCPMeta {
		m.SetMeta(vbID, metadata)
	}
}

func (m *DCPMetadataCS) Purge(ctx context.Context, numWorkers int) {
	for i := range numWorkers {
		err := m.dataStore.Delete(ctx, m.getMetadataKey(i))
		if err != nil && !IsDocNotFoundError(err) {
			InfofCtx(ctx, KeyDCP, "Unable to remove DCP checkpoint for key %s: %v", m.getMetadataKey(i), err)
		}
	}
}

func (m *DCPMetadataCS) GetKeyPrefix() string {
	return m.keyPrefix
}

func (m *DCPMetadataCS) getMetadataKey(workerID int) string {
	return fmt.Sprintf("%s%d", m.keyPrefix, workerID)
}

type WorkerMetadata struct {
	DCPMeta map[uint16]DCPMetadata
}
