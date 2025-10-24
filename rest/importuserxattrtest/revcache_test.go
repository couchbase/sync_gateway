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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserXattrRevCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	// need to disable sequence batching given two rest testers allocating sequence batches
	// can mean that the changes feed has to wait for sequences from another to be released to
	// maintain ordering
	defer db.SuspendSequenceBatching()()

	ctx := base.TestCtx(t)
	docKey := t.Name()
	xattrKey := "channels"
	channelName := []string{"ABC", "DEF"}
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	syncFn := `function (doc, oldDoc, meta){
				if (meta.xattrs.channels !== undefined){
					channel(meta.xattrs.channels);
					console.log(JSON.stringify(meta));
				}
			}`

	// Sync function to set channel access to a channels UserXattrKey
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:       true,
			UserXattrKey:     &xattrKey,
			ImportPartitions: base.Ptr(uint16(2)), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
		}},
		SyncFn: syncFn,
	})
	defer rt.Close()

	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:       true,
			UserXattrKey:     &xattrKey,
			ImportPartitions: base.Ptr(uint16(2)), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
		}},
		SyncFn: syncFn,
	})
	defer rt2.Close()

	dataStore := rt2.GetSingleDataStore()

	ctx = rt2.Context()
	a := rt2.ServerContext().Database(ctx, "db").Authenticator(ctx)
	userABC, err := a.NewUser("userABC", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userABC))

	userDEF, err := a.NewUser("userDEF", "letmein", channels.BaseSetOf(t, "DEF"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userDEF))

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rt.WaitForPendingChanges()

	cas, err := rt.GetSingleDataStore().Get(docKey, nil)
	require.NoError(t, err)

	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, "DEF")}, nil)
	require.NoError(t, err)

	rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	rest.RequireStatus(t, resp, http.StatusOK)

	// get new cas to pass to UpdateXattrs
	cas, err = rt.GetSingleDataStore().Get(docKey, nil)
	require.NoError(t, err)

	// Add channel ABC to the userXattr
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, channelName)}, nil)
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	// Sync function to set channel access to a channels UserXattrKey
	syncFn := `
			function (doc, oldDoc, meta){
				if (meta.xattrs.channels !== undefined){
					channel(meta.xattrs.channels);
					console.log(JSON.stringify(meta));
				}
			}`

	docKey := t.Name()
	xattrKey := "channels"
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			ImportPartitions: base.Ptr(uint16(2)), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
			AutoImport:       true,
			UserXattrKey:     &xattrKey,
		}},
		SyncFn: syncFn,
	})
	defer rt.Close()

	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			ImportPartitions: base.Ptr(uint16(2)), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
			AutoImport:       true,
			UserXattrKey:     &xattrKey,
		}},
		SyncFn: syncFn,
	})
	defer rt2.Close()

	dataStore := rt2.GetSingleDataStore()

	ctx = rt2.Context()
	a := rt2.ServerContext().Database(ctx, "db").Authenticator(ctx)

	userDEF, err := a.NewUser("userDEF", "letmein", channels.BaseSetOf(t, "DEF"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userDEF))

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	rt.WaitForPendingChanges()

	cas, err := rt.GetSingleDataStore().Get(docKey, nil)
	require.NoError(t, err)

	// Write DEF to the userXattrStore to give userDEF access
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, "DEF")}, nil)
	assert.NoError(t, err)

	rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	rest.RequireStatus(t, resp, http.StatusOK)

	cas, err = rt.GetSingleDataStore().Get(docKey, nil)
	require.NoError(t, err)

	// Delete DEF from the userXattr, removing the doc from channel DEF
	err = dataStore.RemoveXattrs(ctx, docKey, []string{xattrKey}, cas)
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
