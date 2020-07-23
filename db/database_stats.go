package db

import (
	"expvar"
	"sync"

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

	statsCacheMapSync,
	statsDatabaseMapSync,
	statsDeltaSyncMapSync,
	sharedBucketImportMapSync,
	cblReplicationPushSync,
	cblReplicationPullSync,
	statsSecuritySync,
	statsGsiViewsSync,
	statsReplicationsSync sync.Once

	statsCacheMap,
	statsDatabaseMap,
	statsDeltaSyncMap,
	sharedBucketImportMap,
	cblReplicationPush,
	cblReplicationPull,
	statsSecurity,
	statsGsiViews *expvar.Map
	statsReplications *expvar.Map

	NewStats *base.DbStats
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
	d.statsCacheMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyCache)
	})
	return d.statsCacheMap
}

func (d *DatabaseStats) StatsDatabase() (stats *expvar.Map) {
	d.statsDatabaseMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyDatabase)
	})
	return d.statsDatabaseMap
}

func (d *DatabaseStats) StatsDeltaSync() (stats *expvar.Map) {
	d.statsDeltaSyncMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyDeltaSync)
	})
	return d.statsDeltaSyncMap
}

func (d *DatabaseStats) StatsSharedBucketImport() (stats *expvar.Map) {
	d.sharedBucketImportMapSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeySharedBucketImport)
	})
	return d.sharedBucketImportMap
}

func (d *DatabaseStats) StatsCblReplicationPush() (stats *expvar.Map) {
	d.cblReplicationPushSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyCblReplicationPush)
	})
	return d.cblReplicationPush
}

func (d *DatabaseStats) StatsCblReplicationPull() (stats *expvar.Map) {
	d.cblReplicationPullSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyCblReplicationPull)
	})
	return d.cblReplicationPull
}

func (d *DatabaseStats) StatsSecurity() (stats *expvar.Map) {
	d.statsSecuritySync.Do(func() {
		d.StatsByKey(base.StatsGroupKeySecurity)
	})
	return d.statsSecurity
}

func (d *DatabaseStats) StatsGsiViews() (stats *expvar.Map) {

	d.statsGsiViewsSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyGsiViews)
	})
	return d.statsGsiViews
}

func (d *DatabaseStats) StatsReplications() (stats *expvar.Map) {
	d.statsReplicationsSync.Do(func() {
		d.StatsByKey(base.StatsGroupKeyReplications)
	})
	return d.statsReplications
}

func (d *DatabaseStats) StatsByKey(key string) (stats *expvar.Map) {
	var subStatsMap *expvar.Map
	subStatsVar := d.storage.Get(key)
	if subStatsVar == nil {
		subStatsMap = initEmptyStatsMap(key, d)
		d.storage.Set(key, subStatsMap)
	} else {
		subStatsMap = subStatsVar.(*expvar.Map)
	}

	return subStatsMap
}

func initEmptyStatsMap(key string, d *DatabaseStats) *expvar.Map {

	result := new(expvar.Map).Init()

	switch key {
	case base.StatsGroupKeyCache:
		result.Set(base.StatKeyRevisionCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevisionCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevisionCacheBypass, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsActive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsTombstone, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheRevsRemoval, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheNumChannels, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheMaxEntries, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCachePendingQueries, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheChannelsAdded, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheChannelsEvictedInactive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheChannelsEvictedNRU, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheCompactCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheCompactTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyChannelCacheBypassCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyActiveChannels, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumSkippedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAbandonedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyHighSeqCached, base.ExpvarIntVal(0))
		result.Set(base.StatKeyHighSeqStable, base.ExpvarIntVal(0))
		result.Set(base.StatKeySkippedSeqLen, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPendingSeqLen, base.ExpvarIntVal(0))
		d.statsCacheMap = result
	case base.StatsGroupKeyDatabase:
		result.Set(base.StatKeySequenceGetCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeySequenceReservedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeySequenceReleasedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAbandonedSeqs, base.ExpvarIntVal(0))
		result.Set(base.StatKeyCrc32cMatchCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumReplicationsActive, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumReplicationsTotal, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocWrites, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocWritesBytes, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocWritesXattrBytes, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocReadsRest, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumDocReadsBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocWritesBytesBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDocReadsBytesBlip, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnXattrSizeCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnChannelsPerDocCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyWarnGrantsPerDocCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpCachingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpReceivedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDcpReceivedTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyCachingDcpStats, new(expvar.Map).Init())
		result.Set(base.StatKeyImportDcpStats, new(expvar.Map).Init())
		result.Set(base.StatKeyHighSeqFeed, new(base.IntMax))
		d.statsDatabaseMap = result
	case base.StatsGroupKeyDeltaSync:
		result.Set(base.StatKeyDeltasRequested, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltasSent, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaPullReplicationCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaCacheHits, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaCacheMisses, base.ExpvarIntVal(0))
		result.Set(base.StatKeyDeltaPushDocCount, base.ExpvarIntVal(0))
		d.statsDeltaSyncMap = result
	// case base.StatsGroupKeySharedBucketImport:
	// 	result.Set(base.StatKeyImportCount, base.ExpvarIntVal(0))
	// 	result.Set(base.StatKeyImportCancelCAS, base.ExpvarIntVal(0))
	// 	result.Set(base.StatKeyImportErrorCount, base.ExpvarIntVal(0))
	// 	result.Set(base.StatKeyImportProcessingTime, base.ExpvarIntVal(0))
	// 	result.Set(base.StatKeyImportHighSeq, base.ExpvarUInt64Val(0))
	// 	result.Set(base.StatKeyImportPartitions, base.ExpvarIntVal(0))
	// 	d.sharedBucketImportMap = result
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
		d.cblReplicationPush = result
	case base.StatsGroupKeyCblReplicationPull:
		result.Set(base.StatKeyPullReplicationsActiveContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsActiveOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalContinuous, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsTotalOneShot, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsSinceZero, base.ExpvarIntVal(0))
		result.Set(base.StatKeyPullReplicationsCaughtUp, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRequestChangesTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevSendLatency, base.ExpvarIntVal(0))
		result.Set(base.StatKeyRevProcessingTime, base.ExpvarIntVal(0))
		result.Set(base.StatKeyMaxPending, new(base.IntMax))
		result.Set(base.StatKeyAttachmentPullCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAttachmentPullBytes, base.ExpvarIntVal(0))
		d.cblReplicationPull = result
	case base.StatsGroupKeySecurity:
		result.Set(base.StatKeyNumDocsRejected, base.ExpvarIntVal(0))
		result.Set(base.StatKeyNumAccessErrors, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthSuccessCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyAuthFailedCount, base.ExpvarIntVal(0))
		result.Set(base.StatKeyTotalAuthTime, base.ExpvarIntVal(0))
		d.statsSecurity = result
	case base.StatsGroupKeyGsiViews:
		// GsiView stat keys are dynamically generated based on query names - see query.go
		d.statsGsiViews = result
	case base.StatsGroupKeyReplications:
		// Replications stat keys are generated per replication, see blip_sync_stats.go
		d.statsReplications = result
	}

	return result
}

// Update database-specific stats that are more efficiently calculated at stats collection time
func (db *DatabaseContext) UpdateCalculatedStats() {

	if db.changeCache != nil {
		db.changeCache.updateStats()
		channelCache := db.changeCache.getChannelCache()
		db.DbStats.StatsCache().Set(base.StatKeyChannelCacheMaxEntries, base.ExpvarIntVal(channelCache.MaxCacheSize()))
		db.DbStats.StatsCache().Set(base.StatKeyHighSeqCached, base.ExpvarUInt64Val(channelCache.GetHighCacheSequence()))
	}

}
