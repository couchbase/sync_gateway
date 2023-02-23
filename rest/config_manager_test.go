// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

func TestBootstrapConfig(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test only works against Couchbase Server")
	}

	base.TestRequiresCollections(t)

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP, base.KeyDCP)

	// Start SG with no databases
	config := BootstrapStartupConfigForTest(t)
	ctx := base.TestCtx(t)
	sc, err := SetupServerContext(ctx, &config, true)
	require.NoError(t, err)
	defer func() {
		sc.Close(ctx)
	}()

	bootstrapContext := sc.BootstrapContext

	// Get a test bucket for bootstrap testing
	tb := base.GetTestBucket(t)
	defer func() {
		fmt.Println("closing test bucket")
		tb.Close()
	}()
	bucketName := tb.GetName()
	db1Name := "db"
	configGroup1 := "cg1"

	var dbConfig1 *DatabaseConfig

	_, err = bootstrapContext.GetConfig(bucketName, configGroup1, db1Name, dbConfig1)
	require.Error(t, err)
}
