/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package cbltest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/cbltestclient"
)

func TestMain(m *testing.M) {
	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 2048}
	// The test server processes are shared by every test in the binary, so they outlive any one
	// test and are only stopped once all of them have run.
	tbpOptions.TeardownFuncs = append(tbpOptions.TeardownFuncs, func() {
		cbltestclient.ShutdownPool(ctx)
	})
	rest.TestBucketPoolRestWithIndexes(ctx, m, tbpOptions)
}
