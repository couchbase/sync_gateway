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
		storage: new(expvar.Map).Init(),
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

	result := new(expvar.Map).Init()

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
		result.Set(base.StatKeyDocWritesBytesBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocReadsBytesBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnXattrSizeCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnChannelsPerDocCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnGrantsPerDocCount, base.ExpvarIntVal(0))
	case base.StatsGroupKeyDeltaSync:
		result.Set(base.StatKeyDeltasRequested, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltasSent, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaPullReplicationCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaPushDocCount, base.ExpvarIntVal(0))
	case base.StatsGroupKeySharedBucketImport:
		result.Set(base.StatKeyImportCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyImportErrorCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyImportProcessingTime, base.ExpvarIntVal(0))
	case base.StatsGroupKeyCblReplicationPush:
		result.Set(base.StatKeyDocPushCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWriteProcessingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeySyncFunctionCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeySyncFunctionTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyProposeChangeCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyProposeChangeTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentPushCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentPushBytes, base.ExpvarIntVal(0))
		result.Set(base.StatKeyConflictWriteCount, base.ExpvarIntVal(0))
	case base.StatsGroupKeyCblReplicationPull:
		result.Set(base.StatKeyPullReplicationsActiveContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsActiveOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsSinceZero, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsCaughtUp, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyMaxPending, new(base.IntMax))
		result.Set(base.StatKeyAttachmentPullCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentPullBytes, base.ExpvarIntVal(0))
	case base.StatsGroupKeySecurity:
		result.Set(base.StatKeyNumDocsRejected, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumAccessErrors, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthSuccessCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthFailedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyTotalAuthTime, base.ExpvarIntVal(0))
	case base.StatsGroupKeyGsiViews:
		// GsiView stat keys are dynamically generated based on query names - see query.go
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
