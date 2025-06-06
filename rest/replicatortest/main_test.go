/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package replicatortest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
)

func TestMain(m *testing.M) {
	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 8192, NumCollectionsPerBucket: 3}
	rest.TestBucketPoolRestWithIndexes(ctx, m, tbpOptions)
}
