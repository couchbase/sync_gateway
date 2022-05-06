/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"github.com/couchbase/sync_gateway/base"
)

// Note: To have any of these appear in expvars they must be connected to a stat inside of stats.go - This is done via
// the BlipSyncStatsForCBL, BlipSyncStatsForSGRPush and BlipSyncStatsForSGRPull functions.
type BlipSyncStats struct {
	DeltaEnabledPullReplicationCount *base.SgwIntStat // global
	HandleRevCount                   *base.SgwIntStat // handleRev
	HandleRevErrorCount              *base.SgwIntStat
	HandleRevDeltaRecvCount          *base.SgwIntStat
	HandleRevBytes                   *base.SgwIntStat
	HandleRevProcessingTime          *base.SgwIntStat
	HandleRevDocsPurgedCount         *base.SgwIntStat
	HandleGetRevCount                *base.SgwIntStat // Connected Client API
	HandlePutRevCount                *base.SgwIntStat // Connected Client API
	HandlePutRevErrorCount           *base.SgwIntStat // Connected Client API
	HandlePutRevDeltaRecvCount       *base.SgwIntStat // Connected Client API
	HandlePutRevBytes                *base.SgwIntStat // Connected Client API
	HandlePutRevProcessingTime       *base.SgwIntStat // Connected Client API
	HandlePutRevDocsPurgedCount      *base.SgwIntStat // Connected Client API
	SendRevCount                     *base.SgwIntStat // sendRev
	SendRevDeltaRequestedCount       *base.SgwIntStat
	SendRevDeltaSentCount            *base.SgwIntStat
	SendRevBytes                     *base.SgwIntStat
	SendRevErrorTotal                *base.SgwIntStat
	SendRevErrorConflictCount        *base.SgwIntStat
	SendRevErrorRejectedCount        *base.SgwIntStat
	SendRevErrorOtherCount           *base.SgwIntStat
	HandleChangesCount               *base.SgwIntStat // handleChanges/handleProposeChanges
	HandleChangesTime                *base.SgwIntStat
	HandleChangesDeltaRequestedCount *base.SgwIntStat
	HandleProveAttachment            *base.SgwIntStat // handleProveAttachment
	HandleGetAttachment              *base.SgwIntStat // handleGetAttachment
	HandleGetAttachmentBytes         *base.SgwIntStat
	ProveAttachment                  *base.SgwIntStat // sendProveAttachment
	GetAttachment                    *base.SgwIntStat // sendGetAttachment
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
	SendChangesCount                 *base.SgwIntStat // sendChanges
	NumConnectAttempts               *base.SgwIntStat
	NumReconnectsAborted             *base.SgwIntStat
}

func NewBlipSyncStats() *BlipSyncStats {
	return &BlipSyncStats{
		DeltaEnabledPullReplicationCount: &base.SgwIntStat{}, // global
		HandleRevCount:                   &base.SgwIntStat{}, // handleRev
		HandleRevErrorCount:              &base.SgwIntStat{},
		HandleRevDeltaRecvCount:          &base.SgwIntStat{},
		HandleRevBytes:                   &base.SgwIntStat{},
		HandleRevProcessingTime:          &base.SgwIntStat{},
		HandleRevDocsPurgedCount:         &base.SgwIntStat{},
		HandleGetRevCount:                &base.SgwIntStat{},
		HandlePutRevCount:                &base.SgwIntStat{},
		HandlePutRevErrorCount:           &base.SgwIntStat{},
		HandlePutRevDeltaRecvCount:       &base.SgwIntStat{},
		HandlePutRevBytes:                &base.SgwIntStat{},
		HandlePutRevProcessingTime:       &base.SgwIntStat{},
		HandlePutRevDocsPurgedCount:      &base.SgwIntStat{},
		SendRevCount:                     &base.SgwIntStat{}, // sendRev
		SendRevDeltaRequestedCount:       &base.SgwIntStat{},
		SendRevDeltaSentCount:            &base.SgwIntStat{},
		SendRevBytes:                     &base.SgwIntStat{},
		SendRevErrorTotal:                &base.SgwIntStat{},
		SendRevErrorConflictCount:        &base.SgwIntStat{},
		SendRevErrorRejectedCount:        &base.SgwIntStat{},
		SendRevErrorOtherCount:           &base.SgwIntStat{},
		HandleChangesCount:               &base.SgwIntStat{}, // handleChanges/handleProposeChanges
		HandleChangesTime:                &base.SgwIntStat{},
		HandleChangesDeltaRequestedCount: &base.SgwIntStat{},
		HandleProveAttachment:            &base.SgwIntStat{}, // handleProveAttachment
		HandleGetAttachment:              &base.SgwIntStat{}, // handleGetAttachment
		HandleGetAttachmentBytes:         &base.SgwIntStat{},
		ProveAttachment:                  &base.SgwIntStat{}, // sendProveAttachment
		GetAttachment:                    &base.SgwIntStat{}, // sendGetAttachment
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
		SendChangesCount:                 &base.SgwIntStat{},
		NumConnectAttempts:               &base.SgwIntStat{},
		NumReconnectsAborted:             &base.SgwIntStat{},
	}
}

// Stats mappings
// Create BlipSyncStats mapped to the corresponding CBL replication stats from DatabaseStats
func BlipSyncStatsForCBL(dbStats *base.DbStats) *BlipSyncStats {
	blipStats := NewBlipSyncStats()

	blipStats.HandleChangesCount = dbStats.CBLReplicationPush().ProposeChangeCount
	blipStats.HandleChangesTime = dbStats.CBLReplicationPush().ProposeChangeTime

	if dbStats.DeltaSync() != nil {
		blipStats.SendRevDeltaRequestedCount = dbStats.DeltaSync().DeltasRequested
		blipStats.SendRevDeltaSentCount = dbStats.DeltaSync().DeltasSent
		blipStats.HandleRevDeltaRecvCount = dbStats.DeltaSync().DeltaPushDocCount
		blipStats.DeltaEnabledPullReplicationCount = dbStats.DeltaSync().DeltaPullReplicationCount
	}

	blipStats.SendRevBytes = dbStats.Database().DocReadsBytesBlip
	blipStats.SendRevCount = dbStats.Database().NumDocReadsBlip

	blipStats.HandleRevBytes = dbStats.Database().DocWritesBytesBlip
	blipStats.HandleRevProcessingTime = dbStats.CBLReplicationPush().WriteProcessingTime

	blipStats.HandleRevCount = dbStats.CBLReplicationPush().DocPushCount

	blipStats.HandleGetAttachment = dbStats.CBLReplicationPull().AttachmentPullCount
	blipStats.HandleGetAttachmentBytes = dbStats.CBLReplicationPull().AttachmentPullBytes

	blipStats.HandleChangesResponseCount = dbStats.CBLReplicationPull().RequestChangesCount
	blipStats.HandleChangesResponseTime = dbStats.CBLReplicationPull().RequestChangesTime
	blipStats.HandleChangesSendRevCount = dbStats.CBLReplicationPull().RevSendCount
	blipStats.HandleChangesSendRevLatency = dbStats.CBLReplicationPull().RevSendLatency
	blipStats.HandleChangesSendRevTime = dbStats.CBLReplicationPull().RevProcessingTime

	// TODO: these are strictly cross-replication stats, maybe do elsewhere?
	blipStats.SubChangesContinuousActive = dbStats.CBLReplicationPull().NumPullReplActiveContinuous
	blipStats.SubChangesContinuousTotal = dbStats.CBLReplicationPull().NumPullReplTotalContinuous
	blipStats.SubChangesOneShotActive = dbStats.CBLReplicationPull().NumPullReplActiveOneShot
	blipStats.SubChangesOneShotTotal = dbStats.CBLReplicationPull().NumPullReplTotalOneShot

	return blipStats
}

func BlipSyncStatsForSGRPush(replicationStats *base.DbReplicatorStats) *BlipSyncStats {
	blipStats := NewBlipSyncStats()

	blipStats.HandleGetAttachmentBytes = replicationStats.NumAttachmentBytesPushed
	blipStats.HandleGetAttachment = replicationStats.NumAttachmentPushed

	blipStats.SendRevCount = replicationStats.NumDocPushed
	blipStats.SendRevErrorTotal = replicationStats.NumDocsFailedToPush
	blipStats.SendRevErrorConflictCount = replicationStats.PushConflictCount
	blipStats.SendRevErrorRejectedCount = replicationStats.PushRejectedCount
	blipStats.SendRevDeltaSentCount = replicationStats.PushDeltaSentCount
	blipStats.SendChangesCount = replicationStats.DocsCheckedSent
	blipStats.NumConnectAttempts = replicationStats.NumConnectAttemptsPush
	blipStats.NumReconnectsAborted = replicationStats.NumReconnectsAbortedPush

	return blipStats
}

func BlipSyncStatsForSGRPull(replicationStats *base.DbReplicatorStats) *BlipSyncStats {
	blipStats := NewBlipSyncStats()

	blipStats.GetAttachmentBytes = replicationStats.NumAttachmentBytesPulled
	blipStats.GetAttachment = replicationStats.NumAttachmentsPulled
	blipStats.HandleRevCount = replicationStats.PulledCount
	blipStats.HandleRevDocsPurgedCount = replicationStats.PurgedCount
	blipStats.HandleRevErrorCount = replicationStats.FailedToPullCount
	blipStats.HandleRevDeltaRecvCount = replicationStats.DeltaReceivedCount
	blipStats.HandleChangesDeltaRequestedCount = replicationStats.DeltaRequestedCount
	blipStats.HandleChangesCount = replicationStats.DocsCheckedReceived
	blipStats.NumConnectAttempts = replicationStats.NumConnectAttemptsPull
	blipStats.NumReconnectsAborted = replicationStats.NumReconnectsAbortedPull

	return blipStats
}
