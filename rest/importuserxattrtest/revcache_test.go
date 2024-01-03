package importuserxattrtest

import (
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserXattrRevCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

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
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
		}},
		SyncFn: syncFn,
	})
	defer rt.Close()

	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),

		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:       true,
			UserXattrKey:     &xattrKey,
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
		}},
		SyncFn: syncFn,
	})
	defer rt2.Close()

	dataStore := rt2.GetSingleDataStore()
	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

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
	require.NoError(t, rt.WaitForPendingChanges())
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, "DEF")
	assert.NoError(t, err)

	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Add channel ABC to the userXattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// wait for import of the xattr change on both nodes
	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userABC", false)
	assert.NoError(t, err)
	_, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes", "userABC", false)
	assert.NoError(t, err)

	// GET the doc with userABC to ensure it is accessible on both nodes
	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userABC")
	assert.Equal(t, resp.Code, http.StatusOK)
	resp = rt.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userABC")
	assert.Equal(t, resp.Code, http.StatusOK)
}

func TestUserXattrDeleteWithRevCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}
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
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
			AutoImport:       true,
			UserXattrKey:     &xattrKey,
		}},
		SyncFn: syncFn,
	})
	defer rt.Close()

	rt2 := rest.NewRestTester(t, &rest.RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
			AutoImport:       true,
			UserXattrKey:     &xattrKey,
		}},
		SyncFn: syncFn,
	})
	defer rt2.Close()

	dataStore := rt2.GetSingleDataStore()
	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	ctx = rt2.Context()
	a := rt2.ServerContext().Database(ctx, "db").Authenticator(ctx)

	userDEF, err := a.NewUser("userDEF", "letmein", channels.BaseSetOf(t, "DEF"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userDEF))

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())

	// Write DEF to the userXattrStore to give userDEF access
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, "DEF")
	assert.NoError(t, err)

	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Delete DEF from the userXattr, removing the doc from channel DEF
	_, err = userXattrStore.DeleteUserXattr(docKey, xattrKey)
	assert.NoError(t, err)

	// wait for import of the xattr change on both nodes
	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)
	_, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)

	// GET the doc with userDEF on both nodes to ensure userDEF no longer has access
	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	assert.Equal(t, resp.Code, http.StatusForbidden)
	resp = rt.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	assert.Equal(t, resp.Code, http.StatusForbidden)
}
