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

	statsDatabaseMapSync sync.Once
	statsDatabaseMap     *expvar.Map

	NewStats *base.DbStats
}

func NewDatabaseStats() *DatabaseStats {
	dbStats := DatabaseStats{
		storage: new(expvar.Map).Init(),
	}
	return &dbStats
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
