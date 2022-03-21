package base

import (
	"context"
	"fmt"
	"math"

	"github.com/couchbase/gocbcore/v10"
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
	// Rollback resets vbucket metadata, but preserves endSeqNo
	Rollback(vbID uint16)

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
	metadata  []DCPMetadata
	endSeqNos []gocbcore.SeqNo
}

func NewDCPMetadataMem(numVbuckets uint16) *DCPMetadataMem {
	m := &DCPMetadataMem{
		metadata:  make([]DCPMetadata, numVbuckets),
		endSeqNos: make([]gocbcore.SeqNo, numVbuckets),
	}
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		m.metadata[vbNo] = DCPMetadata{
			FailoverEntries: make([]gocbcore.FailoverEntry, 0),
			EndSeqNo:        math.MaxUint64,
		}
	}
	return m
}

// Rollback resets the metadata, preserving EndSeqNo if set
func (m *DCPMetadataMem) Rollback(vbID uint16) {
	m.metadata[vbID] = DCPMetadata{
		VbUUID:          0,
		StartSeqNo:      0,
		EndSeqNo:        m.endSeqNos[vbID],
		SnapStartSeqNo:  0,
		SnapEndSeqNo:    0,
		FailoverEntries: make([]gocbcore.FailoverEntry, 0),
	}
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
		m.endSeqNos[i] = gocbcore.SeqNo(endSeqNo)
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

// DCPMetadataCS stores DCP metadata in the specified CouchbaseStore.  It does not require that the store is the
// same one being streamed over DCP.
type DCPMetadataCS struct {
	bucket    Bucket
	keyPrefix string
	metadata  []DCPMetadata
	endSeqNos []gocbcore.SeqNo
}

func NewDCPMetadataCS(store Bucket, numVbuckets uint16, numWorkers int, keyPrefix string) *DCPMetadataCS {

	m := &DCPMetadataCS{
		bucket:    store,
		keyPrefix: keyPrefix,
		metadata:  make([]DCPMetadata, numVbuckets),
		endSeqNos: make([]gocbcore.SeqNo, numVbuckets),
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

func (m *DCPMetadataCS) Rollback(vbID uint16) {
	// Preserve endSeqNo on rollback
	endSeqNo := m.metadata[vbID].EndSeqNo
	m.metadata[vbID] = DCPMetadata{
		VbUUID:          0,
		StartSeqNo:      0,
		EndSeqNo:        endSeqNo,
		SnapStartSeqNo:  0,
		SnapEndSeqNo:    0,
		FailoverEntries: make([]gocbcore.FailoverEntry, 0),
	}
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
	err := m.bucket.Set(m.getMetadataKey(workerID), 0, nil, meta)
	if err != nil {
		InfofCtx(context.TODO(), KeyDCP, "Unable to persist DCP metadata: %v", err)
	} else {
		TracefCtx(context.TODO(), KeyDCP, "Persisted metadata for worker %d: %v", workerID, meta)
		//log.Printf("Persisted metadata for worker %d (%s): %v", workerID, m.getMetadataKey(workerID), meta)
	}
	return
}

func (m *DCPMetadataCS) load(workerID int) {
	var meta WorkerMetadata
	_, err := m.bucket.Get(m.getMetadataKey(workerID), &meta)
	if err != nil {
		if IsKeyNotFoundError(m.bucket, err) {
			return
		}
		InfofCtx(context.TODO(), KeyDCP, "Error loading persisted metadata - metadata will be reset for worker %d: %s", workerID, err)
	}

	TracefCtx(context.TODO(), KeyDCP, "Loaded metadata for worker %d: %v", workerID, meta)
	//log.Printf("Loaded metadata for worker %d (%s): %v", workerID, m.getMetadataKey(workerID), meta)
	for vbID, metadata := range meta.DCPMeta {
		m.metadata[vbID] = metadata
	}
}

func (m *DCPMetadataCS) Purge(numWorkers int) {
	for i := 0; i < numWorkers; i++ {
		err := m.bucket.Delete(m.getMetadataKey(i))
		if err != nil && !IsKeyNotFoundError(m.bucket, err) {
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
