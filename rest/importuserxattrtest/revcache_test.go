// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importuserxattrtest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/couchbase/sync_gateway/testing/assert"
	"github.com/couchbase/sync_gateway/testing/require"
)

func TestUserXattrRevCache(t *testing.T) {

	// need to disable sequence batching given two rest testers allocating sequence batches
	// can mean that the changes feed has to wait for sequences from another to be released to
	// maintain ordering
	defer db.SuspendSequenceBatching()()

	ctx := base.TestCtx(t)
	docKey := t.Name()
	channelName := []string{"ABC", "DEF"}
	rtc, rt, rt2 := newUserXattrCluster(t)
	defer rtc.Close(ctx)

	dataStore := rt2.GetSingleDataStore()

	ctx = rt2.Context()
	a := rt2.ServerContext().Database(ctx, rt2.GetDatabase().Name).Authenticator(ctx)
	userABC, err := a.NewUser("userABC", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userABC))

	userDEF, err := a.NewUser("userDEF", "letmein", channels.BaseSetOf(t, "DEF"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userDEF))

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rt.WaitForPendingChanges()

	cas, err := rt.GetSingleDataStore().Get(ctx, docKey, nil)
	require.NoError(t, err)

	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{revCacheXattrKey: base.MustJSONMarshal(t, "DEF")}, nil)
	require.NoError(t, err)

	rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	rest.RequireStatus(t, resp, http.StatusOK)

	// get new cas to pass to UpdateXattrs
	cas, err = rt.GetSingleDataStore().Get(ctx, docKey, nil)
	require.NoError(t, err)

	// Add channel ABC to the userXattr
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{revCacheXattrKey: base.MustJSONMarshal(t, channelName)}, nil)
	require.NoError(t, err)

	// wait for import of the xattr change on both nodes
	rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userABC", false)
	rt2.WaitForChanges(1, "/{{.keyspace}}/_changes", "userABC", false)

	// GET the doc with userABC to ensure it is accessible on both nodes
	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userABC")
	assert.Equal(t, resp.Code, http.StatusOK)
	resp = rt.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userABC")
	assert.Equal(t, resp.Code, http.StatusOK)
}

func TestUserXattrDeleteWithRevCache(t *testing.T) {
	defer db.SuspendSequenceBatching()()

	ctx := base.TestCtx(t)
	docKey := t.Name()
	rtc, rt, rt2 := newUserXattrCluster(t)
	defer rtc.Close(ctx)

	dataStore := rt2.GetSingleDataStore()

	ctx = rt2.Context()
	a := rt2.ServerContext().Database(ctx, rt2.GetDatabase().Name).Authenticator(ctx)

	userDEF, err := a.NewUser("userDEF", "letmein", channels.BaseSetOf(t, "DEF"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userDEF))

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rt.WaitForPendingChanges()

	cas, err := rt.GetSingleDataStore().Get(ctx, docKey, nil)
	require.NoError(t, err)

	// Write DEF to the userXattrStore to give userDEF access
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{revCacheXattrKey: base.MustJSONMarshal(t, "DEF")}, nil)
	assert.NoError(t, err)

	rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	rest.RequireStatus(t, resp, http.StatusOK)

	cas, err = rt.GetSingleDataStore().Get(ctx, docKey, nil)
	require.NoError(t, err)

	// Delete DEF from the userXattr, removing the doc from channel DEF
	err = dataStore.RemoveXattrs(ctx, docKey, []string{revCacheXattrKey}, cas)
	require.NoError(t, err)

	// wait for import of the xattr change on both nodes
	rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	rt2.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)

	// GET the doc with userDEF on both nodes to ensure userDEF no longer has access
	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	assert.Equal(t, resp.Code, http.StatusForbidden)
	resp = rt.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	assert.Equal(t, resp.Code, http.StatusForbidden)
}

// revCacheXattrKey is the user xattr that the sync function of newUserXattrCluster reads channels from.
const revCacheXattrKey = "channels"

// newUserXattrCluster starts a two node cluster running one database whose sync function assigns channels from the
// user xattr revCacheXattrKey.
func newUserXattrCluster(t *testing.T) (rtc *rest.RestTesterCluster, rt1, rt2 *rest.RestTester) {
	rtc = rest.NewRestTesterCluster(t, &rest.RestTesterClusterConfig{
		NumNodes: 2,
		SyncFn: `function (doc, oldDoc, meta){
				if (meta.xattrs.channels !== undefined){
					channel(meta.xattrs.channels);
				}
			}`,
	})
	rt1 = rtc.Node(0)
	rt2 = rtc.Node(1)
	dbConfig := rt1.NewDbConfig()
	dbConfig.AutoImport = true
	dbConfig.UserXattrKey = new(revCacheXattrKey)
	dbConfig.ImportPartitions = new(uint16(2)) // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
	rest.RequireStatus(t, rt1.CreateDatabase("db", dbConfig), http.StatusCreated)
	_, err := rtc.RefreshClusterDbConfigs()
	require.NoError(t, err)
	// Node 1 joining moves a cbgt partition off node 0, so wait for the split to settle before the test writes docs.
	if base.IsEnterpriseEdition() && !base.UnitTestUrlIsWalrus() {
		rtc.ForEachNode(func(rt *rest.RestTester) {
			base.RequireWaitForStat(t, rt.GetDatabase().DbStats.SharedBucketImport().ImportPartitions.Value, 1)
		})
	}
	return rtc, rt1, rt2
}
