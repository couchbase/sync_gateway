// Copyright 2026-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package manualbucketpooltest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
	"github.com/couchbase/sync_gateway/testing/sgtest"
)

func TestFreshDeploymentWithNoDefaultAndOptOutMetadataCollection(t *testing.T) {
	base.TestRequiresCollections(t)
	if sgtest.UnitTestUrlIsWalrus() {
		t.Skip("test requires dropping _default collection")
	}

	ctx := base.TestCtx(t)

	tb := base.GTestBucketPool.CreateTestBucket(t)
	t.Cleanup(func() { base.GTestBucketPool.RemoveBucket(tb) })

	const (
		scope       = "sg_test_0"
		collection1 = "sg_test_0"
	)

	require.NoError(t, tb.CreateDataStore(ctx, base.ScopeAndCollectionName{Scope: scope, Collection: collection1}))

	if !sgtest.UnitTestUrlIsWalrus() {
		require.NoError(t, tb.DropDataStore(ctx, base.ScopeAndCollectionName{
			Scope: base.DefaultScope, Collection: base.DefaultCollection,
		}), "dropping _default._default should succeed on Couchbase Server")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(false)
	dbConfig.Scopes = rest.ScopesConfig{
		scope: rest.ScopeConfig{
			Collections: rest.CollectionsConfig{collection1: {}},
		},
	}
	resp := rt.CreateDatabase("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusBadRequest)
	assert.Contains(t, resp.Body.String(), "must enable use_system_metadata_collection")

	// recover by opting-in
	dbConfig.UseSystemMobileMetadataCollection = base.Ptr(true)
	resp = rt.CreateDatabase("db1", dbConfig)
	rest.RequireStatus(t, resp, http.StatusCreated)

	summaryResp := rt.GetAllDBsVerbose()
	require.Len(t, summaryResp, 1)
	assert.Equal(t, summaryResp[0].DBName, "db1")
	assert.Nil(t, summaryResp[0].DatabaseError)
	assert.Equal(t, db.RunStateString[db.DBOnline], summaryResp[0].State)
}
