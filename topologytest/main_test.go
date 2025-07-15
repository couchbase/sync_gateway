/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package topologytest

import (
	"context"
	"os"
	"strconv"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	ctx := context.Background() // start of test process
	runTests, _ := strconv.ParseBool(os.Getenv(base.TbpEnvTopologyTests))
	if !base.UnitTestUrlIsWalrus() && !runTests {
		base.SkipTestMain(m, "Tests are disabled for Couchbase Server by default, to enable set %s=true environment variable", base.TbpEnvTopologyTests)
		return
	}
	tbpOptions := base.TestBucketPoolOptions{
		MemWatermarkThresholdMB: 2048,
		NumCollectionsPerBucket: 1, // tests use one named collection
		RequireXDCR:             true,
		DefaultNumBuckets:       4, // XDCR tests run in pairs, so having four buckets means the tests run faster
	}
	// Do not create indexes for this test, so they are built by server_context.go
	db.TestBucketPoolWithIndexes(ctx, m, tbpOptions)
}
