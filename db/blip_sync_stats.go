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
	HandleRevBytes                   *base.SgwIntStat
	HandleRevProcessingTime          *base.SgwIntStat
	HandleRevDocsPurgedCount         *expvar.Int
	SendRevCount                     *base.SgwIntStat // sendRev
	SendRevDeltaRequestedCount       *base.SgwIntStat
	SendRevDeltaSentCount            *base.SgwIntStat
	SendRevBytes                     *base.SgwIntStat
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
		HandleRevBytes:                   &base.SgwIntStat{},
		HandleRevProcessingTime:          &base.SgwIntStat{},
		HandleRevDocsPurgedCount:         &expvar.Int{},
		SendRevCount:                     &base.SgwIntStat{}, // sendRev
		SendRevDeltaRequestedCount:       &base.SgwIntStat{},
		SendRevDeltaSentCount:            &base.SgwIntStat{},
		SendRevBytes:                     &base.SgwIntStat{},
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

	blipStats.HandleChangesCount = dbStats.NewStats.CBLReplicationPush().ProposeChangeCount
	blipStats.HandleChangesTime = dbStats.NewStats.CBLReplicationPush().ProposeChangeTime

	blipStats.SendRevDeltaSentCount = dbStats.NewStats.DeltaSync().DeltasSent

	blipStats.SendRevDeltaRequestedCount = dbStats.NewStats.DeltaSync().DeltasRequested
	blipStats.SendRevBytes = dbStats.NewStats.Database().DocReadsBytesBlip
	blipStats.SendRevCount = dbStats.NewStats.Database().NumDocReadsBlip

	blipStats.HandleRevBytes = dbStats.NewStats.Database().DocWritesBytesBlip
	blipStats.HandleRevProcessingTime = dbStats.NewStats.CBLReplicationPush().WriteProcessingTime

	blipStats.HandleRevDeltaRecvCount = dbStats.NewStats.DeltaSync().DeltaPushDocCount
	blipStats.HandleRevCount = dbStats.NewStats.CBLReplicationPush().DocPushCount

	blipStats.GetAttachment = dbStats.NewStats.CBLReplicationPull().AttachmentPullCount
	blipStats.GetAttachmentBytes = dbStats.NewStats.CBLReplicationPull().AttachmentPullBytes

	blipStats.HandleChangesResponseCount = dbStats.NewStats.CBLReplicationPull().RequestChangesCount
	blipStats.HandleChangesResponseTime = dbStats.NewStats.CBLReplicationPull().RequestChangesTime
	blipStats.HandleChangesSendRevCount = dbStats.NewStats.CBLReplicationPull().RevSendCount
	blipStats.HandleChangesSendRevLatency = dbStats.NewStats.CBLReplicationPull().RevSendLatency
	blipStats.HandleChangesSendRevTime = dbStats.NewStats.CBLReplicationPull().RevProcessingTime

	// TODO: these are strictly cross-replication stats, maybe do elsewhere?
	blipStats.SubChangesContinuousActive = dbStats.NewStats.CBLReplicationPull().NumPullReplActiveContinuous
	blipStats.SubChangesContinuousTotal = dbStats.NewStats.CBLReplicationPull().NumPullReplTotalContinuous
	blipStats.SubChangesOneShotActive = dbStats.NewStats.CBLReplicationPull().NumPullReplActiveOneShot
	blipStats.SubChangesOneShotTotal = dbStats.NewStats.CBLReplicationPull().NumPullReplTotalOneShot

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
