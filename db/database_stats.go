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
	case base.StatsGroupKeyDatabase:
		result.Set(base.StatKeyCachingDcpStats, new(expvar.Map).Init())
		result.Set(base.StatKeyImportDcpStats, new(expvar.Map).Init())
		d.statsDatabaseMap = result
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
		db.DbStats.NewStats.Cache().ChannelCacheMaxEntries.Set(int64(channelCache.MaxCacheSize()))
		db.DbStats.NewStats.Cache().HighSeqCached.Set(int64(channelCache.GetHighCacheSequence()))
	}

}
