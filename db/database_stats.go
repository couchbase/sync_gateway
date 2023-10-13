/*
Copyright 2018-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import "context"

// Wrapper around *expvars.Map for database stats that provide:
//
//    - A lazy loading mechanism
//    - Initialize all stats in a stat group to their zero values
//
// type DatabaseStats struct {
// 	NewStats *base.DbStats
// }

// Update database-specific stats that are more efficiently calculated at stats collection time
func (db *DatabaseContext) UpdateCalculatedStats(ctx context.Context) {

	if db == nil || db.DbStats == nil {
		return
	}
	db.changeCache.updateStats(ctx)
	channelCache := db.changeCache.getChannelCache()
	if channelCache != nil {
		db.DbStats.Cache().ChannelCacheMaxEntries.Set(int64(channelCache.MaxCacheSize(ctx)))
		db.DbStats.Cache().HighSeqCached.Set(int64(channelCache.GetHighCacheSequence()))
	}

}
