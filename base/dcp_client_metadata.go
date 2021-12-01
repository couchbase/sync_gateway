package base

import (
	"math"

	"github.com/couchbase/gocbcore/v10"
)

type DCPMetadata struct {
	vbUUID          gocbcore.VbUUID
	startSeqNo      gocbcore.SeqNo
	endSeqNo        gocbcore.SeqNo
	snapStartSeqNo  gocbcore.SeqNo
	snapEndSeqNo    gocbcore.SeqNo
	failoverEntries []gocbcore.FailoverEntry
}

type DCPMetadataStore interface {
	Rollback(vbID uint16)
	SetMeta(vbID uint16, meta DCPMetadata)
	GetMeta(vbID uint16) DCPMetadata
	SetEndSeqNos(map[uint16]uint64)
	SetSnapshot(e snapshotEvent)
	UpdateSeq(vbID uint16, seq uint64)
	SetFailoverEntries(vbID uint16, entries []gocbcore.FailoverEntry)
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
			failoverEntries: make([]gocbcore.FailoverEntry, 0),
			endSeqNo:        math.MaxUint64,
		}
	}
	return m
}

// Rollback resets the metadata, preserving endSeqNo if set
func (m *DCPMetadataMem) Rollback(vbID uint16) {
	m.metadata[vbID] = DCPMetadata{
		vbUUID:          0,
		startSeqNo:      0,
		endSeqNo:        m.endSeqNos[vbID],
		snapStartSeqNo:  0,
		snapEndSeqNo:    0,
		failoverEntries: make([]gocbcore.FailoverEntry, 0),
	}
}

func (m *DCPMetadataMem) SetMeta(vbID uint16, meta DCPMetadata) {
	m.metadata[vbID] = meta
}

func (m *DCPMetadataMem) GetMeta(vbID uint16) DCPMetadata {
	return m.metadata[vbID]
}

func (m *DCPMetadataMem) SetSnapshot(e snapshotEvent) {
	m.metadata[e.vbID].snapStartSeqNo = gocbcore.SeqNo(e.startSeq)
	m.metadata[e.vbID].snapEndSeqNo = gocbcore.SeqNo(e.endSeq)
}

func (m *DCPMetadataMem) UpdateSeq(vbID uint16, seq uint64) {
	m.metadata[vbID].startSeqNo = gocbcore.SeqNo(seq)
}

func (m *DCPMetadataMem) SetFailoverEntries(vbID uint16, fe []gocbcore.FailoverEntry) {
	m.metadata[vbID].failoverEntries = fe
	m.metadata[vbID].vbUUID = getVbUUID(fe, m.metadata[vbID].startSeqNo)
}

// SetEndSeqNos will update the metadata endSeqNos to the values provided.  Vbuckets not
// present in the endSeqNos map will have their endSeqNo set to zero.
func (m *DCPMetadataMem) SetEndSeqNos(endSeqNos map[uint16]uint64) {
	for i := 0; i < len(m.metadata); i++ {
		endSeqNo, _ := endSeqNos[uint16(i)]
		m.metadata[i].endSeqNo = gocbcore.SeqNo(endSeqNo)
		m.endSeqNos[i] = gocbcore.SeqNo(endSeqNo)
	}
}

// Reset sets metadata sequences to zero, but maintains vbucket UUID and failover entries.  Used for scenarios
// that want to restart a feed from zero, but detect failover
func (md *DCPMetadata) Reset() {
	md.snapStartSeqNo = 0
	md.snapEndSeqNo = 0
	md.startSeqNo = 0
	md.endSeqNo = 0
}
