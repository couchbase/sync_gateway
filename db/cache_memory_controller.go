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
	"context"
	"sync/atomic"

	"github.com/couchbase/sync_gateway/base"
)

// CacheMemoryController manages a single memory budget shared between a revision cache and
// its associated delta cache. All accounting is done via atomic operations so the hot-path
// capacity check (IsOverCapacity) is entirely lock-free.
type CacheMemoryController struct {
	capacity       int64            // total byte limit; 0 = unlimited
	currBytesCount atomic.Int64     // bytes held by the revision cache + delta cache shard
	totalBytesStat *base.SgwIntStat // shared observable stat. Overall footprint of the caches combined.
}

// globalCacheAccessCounter is a monotonically increasing counter stamped onto every
// cache item on insert and on access. It provides a total ordering across both the
// revision and delta caches for cross-cache LRU eviction decisions.
// Using a counter rather than time.Now() keeps the hot-path cost to a single
// atomic increment (~1–2 ns vs ~25 ns for a clock read).
var globalCacheAccessCounter atomic.Uint64

func nextAccessOrder() uint64 {
	return globalCacheAccessCounter.Add(1)
}

func newCacheMemoryController(capacityBytes int64, totalBytesStat *base.SgwIntStat) *CacheMemoryController {
	return &CacheMemoryController{
		capacity:       capacityBytes,
		totalBytesStat: totalBytesStat,
	}
}

// IsOverCapacity will check if overall memory footprint is over the configured for this shard/cache.
func (mc *CacheMemoryController) IsOverCapacity() bool {
	if mc.capacity == 0 {
		return false
	}
	return mc.currBytesCount.Load() > mc.capacity
}

func (mc *CacheMemoryController) bytesToEvict() int64 {
	excess := mc.currBytesCount.Load() - mc.capacity
	if excess < 0 {
		return 0
	}
	return excess
}

func (mc *CacheMemoryController) incrementBytesCount(ctx context.Context, n int64) {
	mc.currBytesCount.Add(n)
	mc.totalBytesStat.Add(n)
}

func (mc *CacheMemoryController) decrementBytesCount(ctx context.Context, n int64) {
	mc.currBytesCount.Add(-n)
	mc.totalBytesStat.Add(-n)
}
