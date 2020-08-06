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
	statsDatabaseMap *expvar.Map

	NewStats *base.DbStats
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
