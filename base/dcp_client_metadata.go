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
	GetMeta(vbID uint16) DCPMetadata
	SetEndSeqNos(map[uint16]uint64)
	SetSnapshot(e snapshotEvent)
	UpdateSeq(vbNo uint16, seq uint64)
	SetFailoverEntries(vbNo uint16, entries []gocbcore.FailoverEntry)
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

func (m *DCPMetadataMem) GetMeta(vbNo uint16) DCPMetadata {
	return m.metadata[vbNo]
}

func (m *DCPMetadataMem) SetSnapshot(e snapshotEvent) {
	m.metadata[e.vbID].snapStartSeqNo = gocbcore.SeqNo(e.startSeq)
	m.metadata[e.vbID].snapEndSeqNo = gocbcore.SeqNo(e.endSeq)
}

func (m *DCPMetadataMem) UpdateSeq(vbNo uint16, seq uint64) {
	m.metadata[vbNo].startSeqNo = gocbcore.SeqNo(seq)
}

func (m *DCPMetadataMem) SetFailoverEntries(vbNo uint16, fe []gocbcore.FailoverEntry) {
	m.metadata[vbNo].failoverEntries = fe
}

// SetEndSeqNos will update the metadata endSeqNos to the values provided.  Vbuckets not
// present in the endSeqNos map will have their endSeqNo set to zero.
func (m *DCPMetadataMem) SetEndSeqNos(endSeqNos map[uint16]uint64) {
	for i := 0; i < len(m.metadata); i++ {
		endSeqNo, _ := endSeqNos[uint16(i)]
		m.metadata[i].endSeqNo = gocbcore.SeqNo(endSeqNo)
	}
}
