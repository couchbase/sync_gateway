package db

import (
	"expvar"

	"github.com/couchbase/sync_gateway/base"
)

// References into expvar stats that correspond to a particular database context.  To make it easier to
// get references to the expvar maps that compose the database stats, keep explicit references to all
// contained expvar maps.  When the entire structure needs to be added to a parent expvar map, convert
// to an expvar map by calling the ExparMap() method.
type DatabaseStats struct {
	StatsCache                *expvar.Map
	StatsDatabase             *expvar.Map
	StatsDeltaSync            *expvar.Map
	StatsSharedBucketImport   *expvar.Map
	StatsCblReplicationPush   *expvar.Map
	StatsCblReplicationPull   *expvar.Map
	StatsCblReplicationCommon *expvar.Map
	StatsSecurity             *expvar.Map
	StatsGsiViews             *expvar.Map
}

func NewDatabaseStats() *DatabaseStats {

	dbStats := DatabaseStats{
		StatsCache:                NewEmptyStatsCache(),
		StatsDatabase:             NewEmptyStatsDatabase(),
		StatsDeltaSync:            NewEmptyStatsDeltaSync(),
		StatsSharedBucketImport:   NewEmptyStatsSharedBucketImport(),
		StatsCblReplicationPush:   NewEmptyStatsCblReplicationPush(),
		StatsCblReplicationPull:   NewEmptyStatsCblReplicationPull(),
		StatsCblReplicationCommon: NewEmptyStatsCblReplicationCommon(),
		StatsSecurity:             NewEmptyStatsSecurity(),
		StatsGsiViews:             NewEmptyStatsGsiViews(),
	}

	return &dbStats
}

// Convert the entire dbstats structure into an expvar map to embed into a parent expvar map
func (d *DatabaseStats) ExpvarMap() *expvar.Map {

	result := new(expvar.Map).Init()
	result.Set(base.StatsGroupKeyCache, d.StatsCache)
	result.Set(base.StatsGroupKeyDatabase, d.StatsDatabase)
	result.Set(base.StatsGroupKeyDeltaSync, d.StatsDeltaSync)
	result.Set(base.StatsGroupKeySharedBucketImport, d.StatsSharedBucketImport)
	result.Set(base.StatsGroupKeyCblReplicationPush, d.StatsCblReplicationPush)
	result.Set(base.StatsGroupKeyCblReplicationPull, d.StatsCblReplicationPull)
	result.Set(base.StatsGroupKeyCblReplicationCommon, d.StatsCblReplicationCommon)
	result.Set(base.StatsGroupKeySecurity, d.StatsSecurity)
	result.Set(base.StatsGroupKeyGsiViews, d.StatsGsiViews)
	return result

}

func NewEmptyStatsCache() (dbStatsMap *expvar.Map) {

	result := new(expvar.Map).Init()
	result.Set(base.StatKeyNumSkippedSeqs, base.ExpvarIntVal(0))
	result.Set(base.StatKeyRevisionCacheHits, base.ExpvarFloatVal(0.0))
	result.Set(base.StatKeyRevisionCacheMisses, base.ExpvarFloatVal(0.0))
	result.Set(base.StatKeyChanCachePerf, base.ExpvarFloatVal(0.0))
	result.Set(base.StatKeyRevCacheUtilization, base.ExpvarFloatVal(0.0))
	result.Set(base.StatKeyChanCacheUtilization, base.ExpvarFloatVal(0.0))

	return result
}

func NewEmptyStatsDatabase() (dbStatsMap *expvar.Map) {

	result := new(expvar.Map).Init()
	result.Set(base.StatKeyNumReplicationConnsActive, base.ExpvarIntVal(0))
	result.Set(base.StatKeyNumReplicationsPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyNumReplicationsClosed, base.ExpvarIntVal(0))
	result.Set(base.StatKeyDocWritesPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyDocReadsPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyReplicationReadsPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyReplicationErrors, base.ExpvarIntVal(0))
	result.Set(base.StatKeyReplicationRate, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyReplicationBacklog, base.ExpvarIntVal(0))
	result.Set(base.StatKeyConnsPerUser, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyNewConnsPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyPercentReplicationsContinuous, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyNumberInitialSync, base.ExpvarIntVal(0))
	result.Set(base.StatKeyOldRevsDocMisses, base.ExpvarIntVal(0))
	return result
}

func NewEmptyStatsDeltaSync() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(base.StatKeyNetBandwidthSavings, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyDeltaHitRatio, base.ExpvarFloatVal(0))
	return result
}

func NewEmptyStatsSharedBucketImport() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(base.StatKeyImportBacklog, base.ExpvarIntVal(0))
	return result
}

func NewEmptyStatsCblReplicationPush() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(base.StatKeyWriteProcessingTime, base.ExpvarFloatVal(0))
	result.Set(base.StatKeySyncTime, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyProposeChangeTime, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyProposeChangesPerSec, base.ExpvarFloatVal(0))
	return result
}

func NewEmptyStatsCblReplicationPull() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(base.StatKeyRequestChangesLatency, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyDcpCachingLatency, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyRevSendLatency, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyInitPullLatency, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyMaxPending, base.ExpvarFloatVal(0))
	return result
}

func NewEmptyStatsCblReplicationCommon() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(base.StatKeyAvgDocSizePull, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyAvgDocSizePush, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyPercentDocsConflicts, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyAvgWritesInConflict, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyTotalNumAttachments, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyAvgAttachmentSize, base.ExpvarFloatVal(0))
	return result
}

func NewEmptyStatsSecurity() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(base.StatKeyAccessQueriesPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyNumDocsRejected, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyNumAccessErrors, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyAuthSuccessCount, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyAuthFailedCount, base.ExpvarFloatVal(0))
	return result
}

func NewEmptyStatsGsiViews() (dbStatsMap *expvar.Map) {
	result := new(expvar.Map).Init()
	result.Set(base.StatKeyTotalQueriesPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyChannelQueriesPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyRoleAccessQueriesPerSec, base.ExpvarFloatVal(0))
	result.Set(base.StatKeyQueryProcessingTime, base.ExpvarFloatVal(0))
	return result
}
