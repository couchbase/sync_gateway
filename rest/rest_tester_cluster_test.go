// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestPersistentDbConfigWithInvalidUpsert(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyHTTP)

	ctx := base.TestCtx(t)
	rtc := NewRestTesterCluster(t, nil)
	defer rtc.Close(ctx)

	const db = "db"

	dbConfig := dbConfigForTestBucket(rtc.testBucket)

	// Create database on a random node.
	resp := rtc.RoundRobin().CreateDatabase(db, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// A duplicate create shouldn't work, even if this were a node that doesn't have the database loaded yet.
	// But this _will_ trigger an on-demand load on this node. So now we have 2 nodes running the database.
	resp = rtc.RoundRobin().CreateDatabase(db, dbConfig)
	// CouchDB returns this status and body in this scenario
	RequireStatus(t, resp, http.StatusPreconditionFailed)
	assert.Contains(t, string(resp.BodyBytes()), "Duplicate database name")

	// The remaining nodes will get the config via polling.
	count, err := rtc.RefreshClusterDbConfigs()
	require.NoError(t, err)
	assert.Equal(t, rtc.NumNodes()-2, count)

	// Sanity-check they have all loaded after the forced update.
	rtc.ForEachNode(func(rt *RestTester) {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+db+"/", "")
		RequireStatus(t, resp, http.StatusOK)
	})

	// Now we'll attempt to write an invalid database to a single node.
	// Ensure it doesn't get unloaded and is rolled back to the original database config.
	rtNode := rtc.RoundRobin()

	// upsert with an invalid config option
	resp = rtNode.UpsertDbConfig(db, DbConfig{RevsLimit: base.Ptr(uint32(0))})
	RequireStatus(t, resp, http.StatusBadRequest)

	// On the same node, make sure the database is still running.
	resp = rtNode.SendAdminRequest(http.MethodGet, "/"+db+"/", "")
	RequireStatus(t, resp, http.StatusOK)

	// and make sure we roll back the database to the previous version (without revs_limit set)
	resp = rtNode.SendAdminRequest(http.MethodGet, "/"+db+"/_config", "")
	RequireStatus(t, resp, http.StatusOK)
	assert.NotContains(t, string(resp.BodyBytes()), `"revs_limit":`)

	// remove the db config directly from the bucket
	docID := PersistentConfigKey(base.TestCtx(t), rtc.groupID, db)
	require.NoError(t, rtNode.GetDatabase().MetadataStore.Delete(ctx, docID))

	// ensure all nodes remove the database
	count, err = rtc.RefreshClusterDbConfigs()
	require.NoError(t, err)
	assert.Equal(t, 0, count)
	rtc.ForEachNode(func(rt *RestTester) {
		resp := rt.SendAdminRequest(http.MethodGet, "/"+db+"/", "")
		RequireStatus(t, resp, http.StatusNotFound)
	})

}

// Ensures that a database remains offline when using async online with an invalid config that causes StartOnlineProcesses to fail.
func TestPersistentDbConfigAsyncOnlineWithInvalidConfig(t *testing.T) {

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.StartOffline = base.Ptr(true)
	resp := rt.CreateDatabase("db", dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	// create invalid user to cause asyncDatabaseOnline -> StartOnlineProcesses error
	rt.GetDatabase().Options.ConfigPrincipals.Users = map[string]*auth.PrincipalConfig{
		"alice": {
			JWTChannels: base.SetOf("asdf"),
		},
	}

	// simulate regular startup
	atomic.StoreUint32(&rt.GetDatabase().State, db.DBStarting)
	rt.WaitForDBState(db.RunStateString[db.DBStarting])

	// Can't trigger error case from REST API - requires incompatible or difficult to test configurations (persistent config, offline, active async init)
	rt.ServerContext().asyncDatabaseOnline(base.NewNonCancelCtx(), rt.GetDatabase(), nil, rt.ServerContext().GetDbVersion("db"))

	// Error should cause db to stay offline - originally a bug caused it to go offline then back to online.
	// Since we're not running asyncDatabaseOnline inside a goroutine, we don't see the Starting->Offline->Online transition, only the final state
	rt.WaitForDBState(db.RunStateString[db.DBOffline])

	require.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().TotalOnlineFatalErrors.Value())
	require.Equal(t, int64(0), rt.GetDatabase().DbStats.Database().TotalInitFatalErrors.Value())
}
