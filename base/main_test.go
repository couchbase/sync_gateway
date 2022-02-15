/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	// can't use defer because of os.Exit
	teardownFuncs := make([]func(), 0)
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestLogging(m))
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestProfiling(m))
	teardownFuncs = append(teardownFuncs, SetUpGlobalTestMemoryWatermark(m, 2048))

	SkipPrometheusStatsRegistration = true

	GTestBucketPool = NewTestBucketPool(FlushBucketEmptierFunc, NoopInitFunc)
	teardownFuncs = append(teardownFuncs, GTestBucketPool.Close)

	// Run the test suite
	status := m.Run()

	for _, fn := range teardownFuncs {
		fn()
	}

	os.Exit(status)
}
