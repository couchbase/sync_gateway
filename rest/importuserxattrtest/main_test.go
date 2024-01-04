/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package importuserxattrtest

import (
	"context"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

func TestMain(m *testing.M) {
	// user xattr tests require xattrs and EE
	if !base.TestUseXattrs() || !base.IsEnterpriseEdition() {
		return
	}

	ctx := context.Background() // start of test process
	tbpOptions := base.TestBucketPoolOptions{MemWatermarkThresholdMB: 2048}
	db.TestBucketPoolWithIndexes(ctx, m, tbpOptions)
}
