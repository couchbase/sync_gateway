/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package channelcachetest holds the db-level tests for the channel cache -
// channel_cache.go and channel_cache_single.go, including the late-log surface. Split out of
// package db so the channel cache can be tested and its coverage measured on its own.
package channelcachetest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 2048, ParallelBucketInit: true}
	db.TestBucketPoolWithIndexes(ctx, m, tbpOptions)
}
