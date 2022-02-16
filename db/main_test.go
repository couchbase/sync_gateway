/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	// can't use defer because of os.Exit
	teardownFuncs := make([]func(), 0)
	teardownFuncs = append(teardownFuncs, base.SetUpGlobalTestLogging(m))
	teardownFuncs = append(teardownFuncs, base.SetUpGlobalTestProfiling(m))
	teardownFuncs = append(teardownFuncs, base.SetUpGlobalTestMemoryWatermark(m, 2048))

	base.SkipPrometheusStatsRegistration = true

	base.GTestBucketPool = base.NewTestBucketPool(ViewsAndGSIBucketReadier, ViewsAndGSIBucketInit)
	teardownFuncs = append(teardownFuncs, base.GTestBucketPool.Close)

	// Run the test suite
	status := m.Run()

	for _, fn := range teardownFuncs {
		fn()
	}

	os.Exit(status)
}
