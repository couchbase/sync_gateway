/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package manualbucketpooltest contains tests that add and drop buckets themselves.
// Be cautious about adding new tests to this package as the more tests run, the more likely
// Couchbase Server will be overwhelmed with overhead of bucket churn. This is required for
// tests that drop the _default._default collection.
package manualbucketpooltest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	tbpOptions := base.TestBucketPoolOptions{
		MemWatermarkThresholdMB: 8192,
		NumBuckets:              base.Ptr(0),
	}
	base.TestBucketPoolMain(ctx, m, base.NoopTBPBucketReadierFunc, base.NoopInitFunc, tbpOptions)
}
