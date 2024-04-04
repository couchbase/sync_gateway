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
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserXattrAutoImport(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:   true,
			UserXattrKey: &xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
	})

	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	// Add doc
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)

	ctx := rt.Context()
	cas, err := dataStore.Get(docKey, nil)

	require.NoError(t, err)
	// Add xattr to doc
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, channelName)}, nil)
	assert.NoError(t, err)

	// Wait for doc to be imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has ran twice (once for PUT and once for xattr addition)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Get Xattr and ensure channel value set correctly
	xattrs, cas, err := dataStore.GetXattrs(ctx, docKey, []string{base.SyncXattrName})
	assert.NoError(t, err)
	require.Contains(t, xattrs, base.SyncXattrName)
	var syncData db.SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

	// Update xattr again but same value and ensure it isn't imported again (crc32 hash should match)
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, channelName)}, nil)
	require.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.Database().Crc32MatchCount.Value() == 1
	})
	assert.NoError(t, err)

	xattrs, _, err = dataStore.GetXattrs(ctx, docKey, []string{base.SyncXattrName})
	assert.NoError(t, err)
	require.Contains(t, xattrs, base.SyncXattrName)
	var syncData2 db.SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData2))

	assert.Equal(t, syncData.Crc32c, syncData2.Crc32c)
	assert.Equal(t, syncData.Crc32cUserXattr, syncData2.Crc32cUserXattr)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	// Update body but same value and ensure it isn't imported again (crc32 hash should match)
	err = dataStore.Set(docKey, 0, nil, map[string]interface{}{})
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.Database().Crc32MatchCount.Value() == 2
	})
	assert.NoError(t, err)

	var syncData3 db.SyncData
	xattrs, _, err = dataStore.GetXattrs(ctx, docKey, []string{base.SyncXattrName})
	assert.NoError(t, err)
	require.Contains(t, xattrs, base.SyncXattrName)
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData3))

	assert.Equal(t, syncData2.Crc32c, syncData3.Crc32c)
	assert.Equal(t, syncData2.Crc32cUserXattr, syncData3.Crc32cUserXattr)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())

	// Update body and ensure import occurs
	updateVal := []byte(`{"prop":"val"}`)
	err = dataStore.Set(docKey, 0, nil, updateVal)
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	assert.Equal(t, int64(3), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	var syncData4 db.SyncData
	xattrs, _, err = dataStore.GetXattrs(ctx, docKey, []string{base.SyncXattrName})
	assert.NoError(t, err)
	require.Contains(t, xattrs, base.SyncXattrName)
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData4))

	assert.Equal(t, base.Crc32cHashString(updateVal), syncData4.Crc32c)
	assert.Equal(t, syncData3.Crc32cUserXattr, syncData4.Crc32cUserXattr)
}

func TestUserXattrOnDemandImportGET(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:   false,
			UserXattrKey: &xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
	})

	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	// Add doc with SDK
	err := dataStore.Set(docKey, 0, nil, []byte(`{}`))
	assert.NoError(t, err)

	// GET to trigger import
	resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docKey, "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has been ran on import
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	cas, err := dataStore.Get(docKey, nil)
	require.NoError(t, err)

	ctx := base.TestCtx(t)
	// Write user xattr
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, channelName)}, nil)
	require.NoError(t, err)

	// GET to trigger import
	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docKey, "")
	rest.RequireStatus(t, resp, http.StatusOK)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	// Ensure sync function has ran on import
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Get sync data for doc and ensure user xattr has been used correctly to set channel
	xattrs, cas, err := dataStore.GetXattrs(ctx, docKey, []string{base.SyncXattrName})
	require.NoError(t, err)

	require.Contains(t, xattrs, base.SyncXattrName)
	var syncData db.SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

	// Write same xattr value
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, channelName)}, nil)
	require.NoError(t, err)

	// Perform GET and ensure import isn't triggered as crc32 hash is the same
	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docKey, "")
	rest.RequireStatus(t, resp, http.StatusOK)

	xattrs, _, err = dataStore.GetXattrs(ctx, docKey, []string{base.SyncXattrName})
	assert.NoError(t, err)

	require.Contains(t, xattrs, base.SyncXattrName)
	var syncData2 db.SyncData
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData2))

	assert.Equal(t, syncData.Crc32c, syncData2.Crc32c)
	assert.Equal(t, syncData.Crc32cUserXattr, syncData2.Crc32cUserXattr)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
}

func TestUserXattrOnDemandImportWrite(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:   false,
			UserXattrKey: &xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
	})

	defer rt.Close()

	dataStore := rt.GetSingleDataStore()

	// Initial PUT
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	// SDK PUT
	err := dataStore.Set(docKey, 0, nil, []byte(`{"update": "update"}`))
	assert.NoError(t, err)

	// Trigger Import
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	rest.RequireStatus(t, resp, http.StatusConflict)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has ran on import
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())
	cas, err := dataStore.Get(docKey, nil)
	require.NoError(t, err)

	ctx := base.TestCtx(t)
	// Write user xattr
	_, err = dataStore.UpdateXattrs(ctx, docKey, 0, cas, map[string][]byte{xattrKey: base.MustJSONMarshal(t, channelName)}, nil)
	require.NoError(t, err)

	// Trigger import
	resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{"update": "update"}`)
	rest.RequireStatus(t, resp, http.StatusConflict)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	// Ensure sync function has ran on import
	assert.Equal(t, int64(3), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	xattrs, _, err := dataStore.GetXattrs(ctx, docKey, []string{base.SyncXattrName})
	require.NoError(t, err)
	var syncData db.SyncData
	require.Contains(t, xattrs, base.SyncXattrName)
	require.NoError(t, base.JSONUnmarshal(xattrs[base.SyncXattrName], &syncData))
	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())
}

// TestAutoImportUserXattrNoSyncData:
//   - Define rest tester with sync function assigning channels defined in user xattr
//   - Insert doc via SDK with user xattr defined to single channel
//   - Wait for import feed to pick the doc up and assert the doc is correctly assigned the channel defined in user xattr
//   - Insert another doc via SDK with user xattr defined with array of channels
//   - Wait for import feed to pick the doc up and assert the doc is correctly assigned the channels defined in user xattr
func TestAutoImportUserXattrNoSyncData(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rtConfig := rest.RestTesterConfig{
		SyncFn: `function (doc, oldDoc, meta) {
   if (meta.xattrs.channels === undefined) {
      console.log("no user_xattr_key defined");
      throw ({
         forbidden: "Missing required property - metadata channel info"
      });
   } else {
      console.log(meta.xattrs.channels);
      channel(meta.xattrs.channels);
   }
}`,
		DatabaseConfig: &rest.DatabaseConfig{DbConfig: rest.DbConfig{
			AutoImport:   true,
			UserXattrKey: base.StringPtr("channels"),
		}},
	}

	rt := rest.NewRestTester(t,
		&rtConfig)
	defer rt.Close()
	const (
		docKey  = "doc1"
		docKey2 = "doc2"
	)
	ctx := base.TestCtx(t)
	dataStore := rt.GetSingleDataStore()

	// Write doc with user xattr defined and assert it correctly imports
	userXattrVal := "chan1"
	val := make(map[string]interface{})
	val["test"] = "doc"
	_, err := dataStore.WriteCasWithXattr(ctx, docKey, "channels", 0, 0, val, userXattrVal, nil)
	assert.NoError(t, err)

	// Wait for doc to be imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has run on import
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Assert the sync data has correct channels populated
	syncData, err := rt.GetSingleTestDatabaseCollection().GetDocSyncData(ctx, docKey)
	require.NoError(t, err)
	assert.Equal(t, []string{userXattrVal}, syncData.Channels.KeySet())
	assert.Len(t, syncData.Channels, 1)

	// Write doc with array of channels in user xattr and assert it correctly imports
	userXattrValArray := []string{"chan1", "chan2"}
	_, err = dataStore.WriteCasWithXattr(ctx, docKey2, "channels", 0, 0, val, userXattrValArray, nil)
	assert.NoError(t, err)

	// Wait for doc to be imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	// Ensure sync function has run on import
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Assert the sync data has correct channels populated
	syncData, err = rt.GetSingleTestDatabaseCollection().GetDocSyncData(ctx, docKey2)
	require.NoError(t, err)
	assert.Equal(t, userXattrValArray, syncData.Channels.KeySet())
	assert.Len(t, syncData.Channels, 2)
}
