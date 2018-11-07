package base

import (
	"expvar"
)

// References into expvar stats that correspond to a particular database context
type ReplicationStats struct {
	Stats *expvar.Map
}

func NewReplicationStats() *ReplicationStats {
	replStats := ReplicationStats{
		Stats: NewEmptyReplicationStats(),
	}
	return &replStats
}

func NewEmptyReplicationStats() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(StatKeyNumDocsTransferred, ExpvarFloatVal(0))
	result.Set(StatKeyNumDocsTransferredPerSec, ExpvarFloatVal(0))
	result.Set(StatKeyBandwidth, ExpvarFloatVal(0))
	result.Set(StatKeyDataReplicatedSize, ExpvarFloatVal(0))
	result.Set(StatKeyNumAttachmentsTransfered, ExpvarFloatVal(0))
	result.Set(StatKeyAvgAttachmentSize, ExpvarFloatVal(0))
	result.Set(StatKeyNumTempFailures, ExpvarFloatVal(0))
	result.Set(StatKeyNumPermFailures, ExpvarFloatVal(0))
	result.Set(StatKeyPendingBacklog, ExpvarFloatVal(0))
	result.Set(StatKeyBatchSize, ExpvarFloatVal(0))
	result.Set(StatKeyDocTransferLatency, ExpvarFloatVal(0))
	result.Set(StatKeyDocsCheckedSent, ExpvarFloatVal(0))
	return result
}

func (d *ReplicationStats) ExpvarMap() *expvar.Map {
	return d.Stats
}
