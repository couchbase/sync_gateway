/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"os"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	defer base.SetUpGlobalTestLogging(m)()
	defer base.SetUpGlobalTestHeapProfiling(m)()

	base.SkipPrometheusStatsRegistration = true

	base.GTestBucketPool = base.NewTestBucketPool(db.ViewsAndGSIBucketReadier, db.ViewsAndGSIBucketInit)

	status := m.Run()

	base.GTestBucketPool.Close()

	os.Exit(status)
}
