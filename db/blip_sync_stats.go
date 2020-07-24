package db

import (
	"expvar"

	"github.com/couchbase/sync_gateway/base"
)

type BlipSyncStats struct {
	DeltaEnabledPullReplicationCount *base.SgwIntStat // global
	HandleRevCount                   *base.SgwIntStat // handleRev
	HandleRevErrorCount              *expvar.Int
	HandleRevDeltaRecvCount          *base.SgwIntStat
	HandleRevBytes                   *expvar.Int
	HandleRevProcessingTime          *base.SgwIntStat
	HandleRevDocsPurgedCount         *expvar.Int
	SendRevCount                     *expvar.Int // sendRev
	SendRevDeltaRequestedCount       *base.SgwIntStat
	SendRevDeltaSentCount            *base.SgwIntStat
	SendRevBytes                     *expvar.Int
	SendRevErrorTotal                *expvar.Int
	SendRevErrorConflictCount        *expvar.Int
	SendRevErrorRejectedCount        *expvar.Int
	SendRevErrorOtherCount           *expvar.Int
	HandleChangesCount               *base.SgwIntStat // handleChanges/handleProposeChanges
	HandleChangesTime                *base.SgwIntStat
	HandleChangesDeltaRequestedCount *expvar.Int
	HandleGetAttachment              *expvar.Int // handleGetAttachment
	HandleGetAttachmentBytes         *expvar.Int
	GetAttachment                    *base.SgwIntStat // getAttachment
	GetAttachmentBytes               *base.SgwIntStat
	HandleChangesResponseCount       *base.SgwIntStat // handleChangesResponse
	HandleChangesResponseTime        *base.SgwIntStat
	HandleChangesSendRevCount        *base.SgwIntStat //  - (duplicates SendRevCount, included for support of CBL expvars)
	HandleChangesSendRevLatency      *base.SgwIntStat
	HandleChangesSendRevTime         *base.SgwIntStat
	SubChangesContinuousActive       *base.SgwIntStat // subChanges
	SubChangesContinuousTotal        *base.SgwIntStat
	SubChangesOneShotActive          *base.SgwIntStat
	SubChangesOneShotTotal           *base.SgwIntStat
	SendChangesCount                 *expvar.Int // sendChagnes
	NumConnectAttempts               *expvar.Int
	NumReconnectsAborted             *expvar.Int
}

func NewBlipSyncStats() *BlipSyncStats {
	return &BlipSyncStats{
		DeltaEnabledPullReplicationCount: &base.SgwIntStat{}, // global
		HandleRevCount:                   &base.SgwIntStat{}, // handleRev
		HandleRevErrorCount:              &expvar.Int{},
		HandleRevDeltaRecvCount:          &base.SgwIntStat{},
		HandleRevBytes:                   &expvar.Int{},
		HandleRevProcessingTime:          &base.SgwIntStat{},
		HandleRevDocsPurgedCount:         &expvar.Int{},
		SendRevCount:                     &expvar.Int{}, // sendRev
		SendRevDeltaRequestedCount:       &base.SgwIntStat{},
		SendRevDeltaSentCount:            &base.SgwIntStat{},
		SendRevBytes:                     &expvar.Int{},
		SendRevErrorTotal:                &expvar.Int{},
		SendRevErrorConflictCount:        &expvar.Int{},
		SendRevErrorRejectedCount:        &expvar.Int{},
		SendRevErrorOtherCount:           &expvar.Int{},
		HandleChangesCount:               &base.SgwIntStat{}, // handleChanges/handleProposeChanges
		HandleChangesTime:                &base.SgwIntStat{},
		HandleChangesDeltaRequestedCount: &expvar.Int{},
		HandleGetAttachment:              &expvar.Int{}, // handleGetAttachment
		HandleGetAttachmentBytes:         &expvar.Int{},
		GetAttachment:                    &base.SgwIntStat{}, // getAttachment
		GetAttachmentBytes:               &base.SgwIntStat{},
		HandleChangesResponseCount:       &base.SgwIntStat{}, // handleChangesResponse
		HandleChangesResponseTime:        &base.SgwIntStat{},
		HandleChangesSendRevCount:        &base.SgwIntStat{}, //  - (duplicates SendRevCount, included for support of CBL expvars)
		HandleChangesSendRevLatency:      &base.SgwIntStat{},
		HandleChangesSendRevTime:         &base.SgwIntStat{},
		SubChangesContinuousActive:       &base.SgwIntStat{}, // subChanges
		SubChangesContinuousTotal:        &base.SgwIntStat{},
		SubChangesOneShotActive:          &base.SgwIntStat{},
		SubChangesOneShotTotal:           &base.SgwIntStat{},
		SendChangesCount:                 &expvar.Int{},
		NumConnectAttempts:               &expvar.Int{},
		NumReconnectsAborted:             &expvar.Int{},
	}
}

// Stats mappings
// Create BlipSyncStats mapped to the corresponding CBL replication stats from DatabaseStats
func BlipSyncStatsForCBL(dbStats *DatabaseStats) *BlipSyncStats {
	blipStats := NewBlipSyncStats()

	// blipStats.HandleChangesCount = dbStats.StatsCblReplicationPush().Get(base.StatKeyProposeChangeCount).(*expvar.Int)
	// blipStats.HandleChangesTime = dbStats.StatsCblReplicationPush().Get(base.StatKeyProposeChangeTime).(*expvar.Int)

	blipStats.HandleChangesCount = dbStats.NewStats.CBLReplicationPush().ProposeChangeCount
	blipStats.HandleChangesTime = dbStats.NewStats.CBLReplicationPush().ProposeChangeTime

	// blipStats.SendRevDeltaSentCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltasSent).(*expvar.Int)
	blipStats.SendRevDeltaSentCount = dbStats.NewStats.DeltaSync().DeltasSent

	// blipStats.SendRevDeltaRequestedCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltasRequested).(*expvar.Int)
	blipStats.SendRevDeltaRequestedCount = dbStats.NewStats.DeltaSync().DeltasRequested
	blipStats.SendRevBytes = dbStats.StatsDatabase().Get(base.StatKeyDocReadsBytesBlip).(*expvar.Int)
	blipStats.SendRevCount = dbStats.StatsDatabase().Get(base.StatKeyNumDocReadsBlip).(*expvar.Int)

	blipStats.HandleRevBytes = dbStats.StatsDatabase().Get(base.StatKeyDocWritesBytesBlip).(*expvar.Int)
	// blipStats.HandleRevProcessingTime = dbStats.StatsCblReplicationPush().Get(base.StatKeyWriteProcessingTime).(*expvar.Int)
	blipStats.HandleRevProcessingTime = dbStats.NewStats.CBLReplicationPush().WriteProcessingTime

	// blipStats.HandleRevDeltaRecvCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltaPushDocCount).(*expvar.Int)
	blipStats.HandleRevDeltaRecvCount = dbStats.NewStats.DeltaSync().DeltaPushDocCount
	// blipStats.HandleRevCount = dbStats.StatsCblReplicationPush().Get(base.StatKeyDocPushCount).(*expvar.Int)
	blipStats.HandleRevCount = dbStats.NewStats.CBLReplicationPush().DocPushCount

	// blipStats.GetAttachmentCount = dbStats.StatsCblReplicationPull().Get(base.StatKeyAttachmentPullCount).(*expvar.Int)
	blipStats.GetAttachment = dbStats.NewStats.CBLReplicationPull().AttachmentPullCount
	// blipStats.GetAttachmentBytes = dbStats.StatsCblReplicationPull().Get(base.StatKeyAttachmentPullBytes).(*expvar.Int)
	blipStats.GetAttachmentBytes = dbStats.NewStats.CBLReplicationPull().AttachmentPullBytes

	// blipStats.HandleChangesResponseCount = dbStats.StatsCblReplicationPull().Get(base.StatKeyRequestChangesCount).(*expvar.Int)
	blipStats.HandleChangesResponseCount = dbStats.NewStats.CBLReplicationPull().RequestChangesCount
	// blipStats.HandleChangesResponseTime = dbStats.StatsCblReplicationPull().Get(base.StatKeyRequestChangesTime).(*expvar.Int)
	blipStats.HandleChangesResponseTime = dbStats.NewStats.CBLReplicationPull().RequestChangesTime
	// blipStats.HandleChangesSendRevCount = dbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendCount).(*expvar.Int)
	blipStats.HandleChangesSendRevCount = dbStats.NewStats.CBLReplicationPull().RevSendCount
	// blipStats.HandleChangesSendRevLatency = dbStats.StatsCblReplicationPull().Get(base.StatKeyRevSendLatency).(*expvar.Int)
	blipStats.HandleChangesSendRevLatency = dbStats.NewStats.CBLReplicationPull().RevSendLatency
	// blipStats.HandleChangesSendRevTime = dbStats.StatsCblReplicationPull().Get(base.StatKeyRevProcessingTime).(*expvar.Int)
	blipStats.HandleChangesSendRevTime = dbStats.NewStats.CBLReplicationPull().RevProcessingTime

	// TODO: these are strictly cross-replication stats, maybe do elsewhere?
	// blipStats.SubChangesContinuousActive = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsActiveContinuous).(*expvar.Int)
	blipStats.SubChangesContinuousActive = dbStats.NewStats.CBLReplicationPull().NumPullReplActiveContinuous
	// blipStats.SubChangesContinuousTotal = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsTotalContinuous).(*expvar.Int)
	blipStats.SubChangesContinuousTotal = dbStats.NewStats.CBLReplicationPull().NumPullReplTotalContinuous
	// blipStats.SubChangesOneShotActive = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsActiveOneShot).(*expvar.Int)
	blipStats.SubChangesOneShotActive = dbStats.NewStats.CBLReplicationPull().NumPullReplActiveOneShot
	// blipStats.SubChangesOneShotTotal = dbStats.StatsCblReplicationPull().Get(base.StatKeyPullReplicationsTotalOneShot).(*expvar.Int)
	blipStats.SubChangesOneShotTotal = dbStats.NewStats.CBLReplicationPull().NumPullReplTotalOneShot

	// blipStats.DeltaEnabledPullReplicationCount = dbStats.StatsDeltaSync().Get(base.StatKeyDeltaPullReplicationCount).(*expvar.Int)
	blipStats.DeltaEnabledPullReplicationCount = dbStats.NewStats.DeltaSync().DeltaPullReplicationCount

	return blipStats
}

func initReplicationStat(statMap *expvar.Map, key string) (stat *expvar.Int) {
	expvarVar := statMap.Get(key)
	if expvarVar == nil {
		stat = base.ExpvarIntVal(0)
		statMap.Set(key, stat)
	} else {
		stat = expvarVar.(*expvar.Int)
	}
	return stat
}

func BlipSyncStatsForSGRPush(statsMap *expvar.Map) *BlipSyncStats {
	blipStats := NewBlipSyncStats()
	if statsMap == nil {
		base.Warnf("statsMap not provided for SGRPush initialization - replication stats will not be published")
		statsMap = new(expvar.Map).Init()
	}

	blipStats.HandleGetAttachmentBytes = initReplicationStat(statsMap, base.StatKeySgrNumAttachmentBytesPushed)
	blipStats.HandleGetAttachment = initReplicationStat(statsMap, base.StatKeySgrNumAttachmentsPushed)

	blipStats.SendRevCount = initReplicationStat(statsMap, base.StatKeySgrNumDocsPushed)
	blipStats.SendRevErrorTotal = initReplicationStat(statsMap, base.StatKeySgrNumDocsFailedToPush)
	blipStats.SendRevErrorConflictCount = initReplicationStat(statsMap, base.StatKeySgrPushConflictCount)
	blipStats.SendRevErrorRejectedCount = initReplicationStat(statsMap, base.StatKeySgrPushRejectedCount)
	blipStats.SendRevDeltaSentCount = initReplicationStat(statsMap, base.StatKeySgrPushDeltaSentCount)
	blipStats.SendChangesCount = initReplicationStat(statsMap, base.StatKeySgrDocsCheckedSent)
	return blipStats
}

func BlipSyncStatsForSGRPull(statsMap *expvar.Map) *BlipSyncStats {
	blipStats := NewBlipSyncStats()
	if statsMap == nil {
		base.Warnf("statsMap not provided for SGRPull initialization - replication stats will not be published")
		statsMap = new(expvar.Map).Init()
	}

	blipStats.GetAttachmentBytes = initReplicationStat(statsMap, base.StatKeySgrNumAttachmentBytesPulled)
	blipStats.GetAttachment = initReplicationStat(statsMap, base.StatKeySgrNumAttachmentsPulled)
	blipStats.HandleRevCount = initReplicationStat(statsMap, base.StatKeySgrPulledCount)
	blipStats.HandleRevDocsPurgedCount = initReplicationStat(statsMap, base.StatKeySgrPurgedCount)
	blipStats.HandleRevErrorCount = initReplicationStat(statsMap, base.StatKeySgrFailedToPullCount)
	blipStats.HandleRevDeltaRecvCount = initReplicationStat(statsMap, base.StatKeySgrDeltaRecvCount)
	blipStats.HandleChangesDeltaRequestedCount = initReplicationStat(statsMap, base.StatKeySgrDeltaRequestedCount)
	blipStats.HandleChangesCount = initReplicationStat(statsMap, base.StatKeySgrDocsCheckedRecv)

	return blipStats
}
