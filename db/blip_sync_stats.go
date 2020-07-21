package db

import (
	"expvar"

	"github.com/couchbase/sync_gateway/base"
)

type BlipSyncStats struct {
	DeltaEnabledPullReplicationCount *expvar.Int // global
	HandleRevCount                   *expvar.Int // handleRev
	HandleRevErrorCount              *expvar.Int
	HandleRevDeltaRecvCount          *expvar.Int
	HandleRevBytes                   *expvar.Int
	HandleRevProcessingTime          *expvar.Int
	HandleRevDocsPurgedCount         *expvar.Int
	SendRevCount                     *expvar.Int // sendRev
	SendRevDeltaRequestedCount       *expvar.Int
	SendRevDeltaSentCount            *expvar.Int
	SendRevBytes                     *expvar.Int
	SendRevErrorTotal                *expvar.Int
	SendRevErrorConflictCount        *expvar.Int
	SendRevErrorRejectedCount        *expvar.Int
	SendRevErrorOtherCount           *expvar.Int
	HandleChangesCount               *expvar.Int // handleChanges/handleProposeChanges
	HandleChangesTime                *expvar.Int
	HandleChangesDeltaRequestedCount *expvar.Int
	GetAttachmentCount               *expvar.Int // getAttachment
	GetAttachmentBytes               *expvar.Int
	HandleChangesResponseCount       *expvar.Int // handleChangesResponse
	HandleChangesResponseTime        *expvar.Int
	HandleChangesSendRevCount        *expvar.Int //  - (duplicates SendRevCount, included for support of CBL expvars)
	HandleChangesSendRevLatency      *expvar.Int
	HandleChangesSendRevTime         *expvar.Int
	SubChangesContinuousActive       *expvar.Int // subChanges
	SubChangesContinuousTotal        *expvar.Int
	SubChangesOneShotActive          *expvar.Int
	SubChangesOneShotTotal           *expvar.Int
}

func NewBlipSyncStats() *BlipSyncStats {
	return &BlipSyncStats{
		DeltaEnabledPullReplicationCount: &expvar.Int{}, // global
		HandleRevCount:                   &expvar.Int{}, // handleRev
		HandleRevErrorCount:              &expvar.Int{},
		HandleRevDeltaRecvCount:          &expvar.Int{},
		HandleRevBytes:                   &expvar.Int{},
		HandleRevProcessingTime:          &expvar.Int{},
		HandleRevDocsPurgedCount:         &expvar.Int{},
		SendRevCount:                     &expvar.Int{}, // sendRev
		SendRevDeltaRequestedCount:       &expvar.Int{},
		SendRevDeltaSentCount:            &expvar.Int{},
		SendRevBytes:                     &expvar.Int{},
		SendRevErrorTotal:                &expvar.Int{},
		SendRevErrorConflictCount:        &expvar.Int{},
		SendRevErrorRejectedCount:        &expvar.Int{},
		SendRevErrorOtherCount:           &expvar.Int{},
		HandleChangesCount:               &expvar.Int{}, // handleChanges/handleProposeChanges
		HandleChangesTime:                &expvar.Int{},
		HandleChangesDeltaRequestedCount: &expvar.Int{},
		GetAttachmentCount:               &expvar.Int{}, // getAttachment
		GetAttachmentBytes:               &expvar.Int{},
		HandleChangesResponseCount:       &expvar.Int{}, // handleChangesResponse
		HandleChangesResponseTime:        &expvar.Int{},
		HandleChangesSendRevCount:        &expvar.Int{}, //  - (duplicates SendRevCount, included for support of CBL expvars)
		HandleChangesSendRevLatency:      &expvar.Int{},
		HandleChangesSendRevTime:         &expvar.Int{},
		SubChangesContinuousActive:       &expvar.Int{}, // subChanges
		SubChangesContinuousTotal:        &expvar.Int{},
		SubChangesOneShotActive:          &expvar.Int{},
		SubChangesOneShotTotal:           &expvar.Int{},
	}
}

// Stats mappings
// Create BlipSyncStats mapped to the corresponding CBL replication stats from DatabaseStats
func BlipSyncStatsForCBL(dbStats *DatabaseStats) *BlipSyncStats {
	blipStats := NewBlipSyncStats()

	blipStats.HandleChangesCount = dbStats.StatsCblReplicationPush().Get(base.StatKeyProposeChangeCount).(*expvar.Int)
	blipStats.HandleChangesTime = dbStats.StatsCblReplicationPush().Get(base.StatKeyProposeChangeTime).(*expvar.Int)

	blipStats.SendRevDeltaSentCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltasSent).(*expvar.Int)

	blipStats.SendRevDeltaRequestedCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltasRequested).(*expvar.Int)
	blipStats.SendRevBytes = dbStats.StatsDatabase().Get(base.StatKeyDocReadsBytesBlip).(*expvar.Int)
	blipStats.SendRevCount = dbStats.StatsDatabase().Get(base.StatKeyNumDocReadsBlip).(*expvar.Int)

	blipStats.HandleRevBytes = dbStats.StatsDatabase().Get(base.StatKeyDocWritesBytesBlip).(*expvar.Int)
	blipStats.HandleRevProcessingTime = dbStats.StatsCblReplicationPush().Get(base.StatKeyWriteProcessingTime).(*expvar.Int)

	blipStats.HandleRevDeltaRecvCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltaPushDocCount).(*expvar.Int)
	blipStats.HandleRevCount = dbStats.StatsCblReplicationPush().Get(base.StatKeyDocPushCount).(*expvar.Int)

	blipStats.GetAttachmentCount = dbStats.StatsCblReplicationPull().Get(base.StatKeyAttachmentPullCount).(*expvar.Int)
	blipStats.GetAttachmentBytes = dbStats.StatsCblReplicationPull().Get(base.StatKeyAttachmentPullBytes).(*expvar.Int)

	blipStats.HandleChangesResponseCount = dbStats.StatsCblReplicationPull().Get(base.StatKeyRequestChangesCount).(*expvar.Int)
	blipStats.HandleChangesResponseTime = dbStats.StatsCblReplicationPull().Get(base.StatKeyRequestChangesTime).(*expvar.Int)
	blipStats.HandleChangesSendRevCount = dbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount).(*expvar.Int)
	blipStats.HandleChangesSendRevLatency = dbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendLatency).(*expvar.Int)
	blipStats.HandleChangesSendRevTime = dbStats.StatsCblReplicationPull().Get(base.StatKeyRevProcessingTime).(*expvar.Int)

	// TODO: these are strictly cross-replication stats, maybe do elsewhere?
	blipStats.SubChangesContinuousActive = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsActiveContinuous).(*expvar.Int)
	blipStats.SubChangesContinuousTotal = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsTotalContinuous).(*expvar.Int)
	blipStats.SubChangesOneShotActive = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsActiveOneShot).(*expvar.Int)
	blipStats.SubChangesOneShotTotal = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsTotalOneShot).(*expvar.Int)

	blipStats.DeltaEnabledPullReplicationCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltaPullReplicationCount).(*expvar.Int)

	return blipStats
}
