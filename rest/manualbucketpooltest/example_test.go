/*
Copyright 2026-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package manualbucketpooltest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
)

// TestCreateDatabaseOnBareBucket is a minimal example of creating a database with RestTester
// on a bucket created outside the pool.
func TestCreateDatabaseOnBareBucket(t *testing.T) {
	ctx := base.TestCtx(t)

	tb := base.GTestBucketPool.CreateTestBucket(t)
	t.Cleanup(func() { base.GTestBucketPool.RemoveBucket(tb) })

	// create a single named collection, but you can use CreateDataStore directly as well
	base.GTestBucketPool.CreateCollections(ctx, tb, 1)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	resp := rt.CreateDatabase("db", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	rt.CreateTestDoc("doc1")
	rt.GetDoc("doc1")
}
