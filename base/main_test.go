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
	defer SetUpGlobalTestLogging(m)()
	defer SetUpGlobalTestProfiling(m)()

	SkipPrometheusStatsRegistration = true

	GTestBucketPool = NewTestBucketPool(FlushBucketEmptierFunc, NoopInitFunc)

	status := m.Run()

	GTestBucketPool.Close()

	os.Exit(status)
}
