/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

// CacheMemoryController manages a single memory budget shared between a revision cache and
// its associated delta cache. All accounting is done via atomic operations so the hot-path
// capacity check (IsOverCapacity) is entirely lock-free.
type CacheMemoryController struct {
	capacity           int64            // total byte limit; 0 = unlimited
	bytesInUseForShard atomic.Int64     // bytes held by the revision cache + delta cache shard
	globalUsageStat    *base.SgwIntStat // shared observable stat. Overall footprint of the caches combined.
}

func newCacheMemoryController(capacityBytes int64, totalBytesStat *base.SgwIntStat) *CacheMemoryController {
	return &CacheMemoryController{
		capacity:        capacityBytes,
		globalUsageStat: totalBytesStat,
	}
}

// IsOverCapacity will check if overall memory footprint is over the configured for this shard/cache.
func (mc *CacheMemoryController) IsOverCapacity() bool {
	if mc.capacity == 0 {
		return false
	}
	// IsOverCapacity returns true only when usage strictly exceeds the limit,
	// so a cache exactly at capacity is not evicted.
	return mc.bytesInUseForShard.Load() > mc.capacity
}

func (mc *CacheMemoryController) bytesToEvict() int64 {
	excess := mc.bytesInUseForShard.Load() - mc.capacity
	if excess < 0 {
		return 0
	}
	return excess
}

func (mc *CacheMemoryController) incrementBytesCount(n int64) {
	mc.bytesInUseForShard.Add(n)
	mc.globalUsageStat.Add(n)
}

func (mc *CacheMemoryController) decrementBytesCount(n int64) {
	mc.bytesInUseForShard.Add(-n)
	mc.globalUsageStat.Add(-n)
}
