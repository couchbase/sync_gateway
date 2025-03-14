/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package indextest runs tests which add or remove GSI tests. These exist in a separate package from db to avoid index churn, and the bucket pool function will drop all indexes in the bucket pool before running tests.
package indextest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	// these tests are only meant to be be run against Couchbase Server with GSI
	if base.UnitTestUrlIsWalrus() || base.TestsDisableGSI() {
		base.SkipTestMain(m, "these tests are only meant to be be run against Couchbase Server with GSI")
	}

	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 2048, NumCollectionsPerBucket: 1}
	db.TestBucketPoolEnsureNoIndexes(ctx, m, tbpOptions)
}
