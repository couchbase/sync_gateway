/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

// Package defaultcollectiondroptest is deliberately its own package. The single test it
// contains DROPS the _default collection. A dropped-_default bucket returned to a shared TestBucketPool would
// break every sibling test that relies on _default existing, so this destructive test is
// isolated in a package whose pool only ever services this one test.
package defaultcollectiondroptest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 8192}
	db.TestBucketPoolWithIndexes(ctx, m, tbpOptions)
}
