package db

import (
	"expvar"
	"github.com/couchbase/sync_gateway/base"
)

// Wrapper around *expvars.Map for database stats that provide:
//
//    - A lazy loading mechanism
//    - Initialize all stats in a stat group to their zero values
//
type DatabaseStats struct {

	// The expvars map the stats for this db will be stored in
	storage *expvar.Map
}

func NewDatabaseStats() *DatabaseStats {
	dbStats := DatabaseStats{
		storage: new(expvar.Map),
	}
	return &dbStats
}

// Convert the entire dbstats structure into an expvar map to embed into a parent expvar map
func (d *DatabaseStats) ExpvarMap() *expvar.Map {
	return d.storage
}

func (d *DatabaseStats) StatsCache() (stats *expvar.Map) {
	statsCache := d.StatsByKey(base.StatsGroupKeyCache)
	return statsCache
}

func (d *DatabaseStats) StatsDatabase() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeyDatabase)
}

func (d *DatabaseStats) StatsDeltaSync() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeyDeltaSync)
}

func (d *DatabaseStats) SharedBucketImport() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeySharedBucketImport)
}

func (d *DatabaseStats) CblReplicationPush() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeyCblReplicationPush)
}

func (d *DatabaseStats) StatsCblReplicationPull() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeyCblReplicationPull)
}

func (d *DatabaseStats) StatsCblReplicationCommon() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeyCblReplicationCommon)
}

func (d *DatabaseStats) StatsSecurity() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeySecurity)
}

func (d *DatabaseStats) StatsGsiViews() (stats *expvar.Map) {
	return d.StatsByKey(base.StatsGroupKeyGsiViews)
}

func (d *DatabaseStats) StatsByKey(key string) (stats *expvar.Map) {
	var subStatsMap *expvar.Map
	subStatsVar := d.storage.Get(key)
	if subStatsVar == nil {
		subStatsMap = initEmptyStatsMap(key)
		d.storage.Set(key, subStatsMap)
	} else {
		subStatsMap = subStatsVar.(*expvar.Map)
	}

	return subStatsMap
}

func initEmptyStatsMap(key string) *expvar.Map {

	result := new(expvar.Map)

	switch key {
	case base.StatsGroupKeyCache:
		result.Set(base.StatKeyNumSkippedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevisionCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevisionCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsActive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsRemoval, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsTombstone, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheNumChannels, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheMaxEntries, base.ExpvarIntVal(0))
	case base.StatsGroupKeyDatabase:
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
		result.Set(base.StatKeySequenceGets, base.ExpvarIntVal(0))
		result.Set(base.StatKeySequenceReserves, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAbandonedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyCrc32cMatchCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumReplicationsActive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumReplicationsTotal, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocWrites, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocWritesBytes, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocReadsRest, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocReadsBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocReadsBytesBlip, base.ExpvarIntVal(0))
	case base.StatsGroupKeyDeltaSync:
		result.Set(base.StatKeyNetBandwidthSavings, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyDeltaHitRatio, base.ExpvarFloatVal(0))
	case base.StatsGroupKeySharedBucketImport:
		result.Set(base.StatKeyImportBacklog, base.ExpvarIntVal(0))
	case base.StatsGroupKeyCblReplicationPush:
		result.Set(base.StatKeyWriteProcessingTime, base.ExpvarFloatVal(0))
		result.Set(base.StatKeySyncTime, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyProposeChangeTime, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyProposeChangesPerSec, base.ExpvarFloatVal(0))
	case base.StatsGroupKeyCblReplicationPull:
		result.Set(base.StatKeyPullReplicationsActiveContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsActiveOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsSinceZero, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyMaxPending, new(base.IntMax))
		result.Set(base.StatKeyAttachmentsPulledCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentsPulledBytes, base.ExpvarIntVal(0))
	case base.StatsGroupKeyCblReplicationCommon:
		result.Set(base.StatKeyAvgDocSizePull, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyAvgDocSizePush, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyPercentDocsConflicts, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyAvgWritesInConflict, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyTotalNumAttachments, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyAvgAttachmentSize, base.ExpvarFloatVal(0))
	case base.StatsGroupKeySecurity:
		result.Set(base.StatKeyAccessQueriesPerSec, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyNumDocsRejected, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumAccessErrors, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthSuccessCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthFailedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyTotalAuthTime, base.ExpvarIntVal(0))
	case base.StatsGroupKeyGsiViews:
		result.Set(base.StatKeyTotalQueriesPerSec, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyChannelQueriesPerSec, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyRoleAccessQueriesPerSec, base.ExpvarFloatVal(0))
		result.Set(base.StatKeyQueryProcessingTime, base.ExpvarFloatVal(0))
	}

	return result
}

// Update database-specific stats that are more efficiently calculated at stats collection time
func (db *DatabaseContext) UpdateCalculatedStats() {

	// Max channel cache size
	if cache, ok := db.changeCache.(*changeCache); ok {
		db.DbStats.StatsCache().Set(base.StatKeyChannelCacheMaxEntries, base.ExpvarIntVal(cache.MaxCacheSize()))
	}

}

func (db *DatabaseContext) IncrementDocReads(bytes int, delta int) {
	if bytes > 0 {

	}
}
