/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package xdcr

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{
		MemWatermarkThresholdMB: 2048,
		RequireXDCR:             true,
		DefaultNumBuckets:       4, // use 4 buckets for testing since each test will use 2 buckets for XDCR
		NumCollectionsPerBucket: 1, // use either default collection or a single named collection
	}
	base.TestBucketPoolNoIndexes(ctx, m, tbpOptions)
}
