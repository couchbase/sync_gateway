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
	"time"

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
	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	require.True(t, ok)

	// Add doc
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, "{}")
	rest.RequireStatus(t, resp, http.StatusCreated)

	// Add xattr to doc
	_, err := userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Wait for doc to be imported
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure sync function has ran twice (once for PUT and once for xattr addition)
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	ctx := base.TestCtx(t)
	// Get Xattr and ensure channel value set correctly
	var syncData db.SyncData
	_, err = dataStore.GetXattr(ctx, docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

	// Update xattr again but same value and ensure it isn't imported again (crc32 hash should match)
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.Database().Crc32MatchCount.Value() == 1
	})
	assert.NoError(t, err)

	var syncData2 db.SyncData
	_, err = dataStore.GetXattr(ctx, docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

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
	_, err = dataStore.GetXattr(ctx, docKey, base.SyncXattrName, &syncData3)
	assert.NoError(t, err)

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
	_, err = dataStore.GetXattr(ctx, docKey, base.SyncXattrName, &syncData4)
	assert.NoError(t, err)

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

	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	require.True(t, ok)

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

	// Write user xattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

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

	ctx := base.TestCtx(t)
	// Get sync data for doc and ensure user xattr has been used correctly to set channel
	var syncData db.SyncData
	_, err = dataStore.GetXattr(ctx, docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

	// Write same xattr value
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Perform GET and ensure import isn't triggered as crc32 hash is the same
	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docKey, "")
	rest.RequireStatus(t, resp, http.StatusOK)

	var syncData2 db.SyncData
	_, err = dataStore.GetXattr(ctx, docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

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

	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	require.True(t, ok)

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

	// Write user xattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

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

	ctx := base.TestCtx(t)
	var syncData db.SyncData
	_, err = dataStore.GetXattr(ctx, docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())
}

// TestAutoImportUserXattrNoSyncData:
//   - Define rest tester with sync function assigning channels defined in user xattr
//   - Insert doc via SDK with user xattr defined to single channel
//   - Wait for import feed to pick the doc up and assert the doc is correctly assigned the channel defined in user xattr
//   - Insert another doc via SDK with user xattr defined with array of channels
//   - Wait for import feed to pick the doc up and assert the doc is correctly assigned the channels defined in user xattr
func TestAutoImportUserXattrNoSyncData(t *testing.T) {
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

	userXattrChan := "chan1"
	userXattrVal := map[string][]byte{
		"channels": base.MustJSONMarshal(t, userXattrChan),
	}

	// Write doc with user xattr defined and assert it correctly imports
	val := make(map[string]interface{})
	val["test"] = "doc"
	_, err := dataStore.WriteWithXattrs(ctx, docKey, 0, 0, base.MustJSONMarshal(t, val), userXattrVal, nil)
	require.NoError(t, err)

	// Wait for doc to be imported
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(1), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())
	}, 10*time.Second, 100*time.Millisecond)

	// Ensure sync function has run on import
	assert.Equal(t, int64(1), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Assert the sync data has correct channels populated
	syncData, err := rt.GetSingleTestDatabaseCollection().GetDocSyncData(ctx, docKey)
	require.NoError(t, err)
	assert.Equal(t, []string{userXattrChan}, syncData.Channels.KeySet())
	assert.Len(t, syncData.Channels, 1)

	// Write doc with array of channels in user xattr and assert it correctly imports
	userXattrValArray := []string{"chan1", "chan2"}
	userXattrVal = map[string][]byte{
		"channels": base.MustJSONMarshal(t, userXattrValArray),
	}
	_, err = dataStore.WriteWithXattrs(ctx, docKey2, 0, 0, base.MustJSONMarshal(t, val), userXattrVal, nil)
	require.NoError(t, err)

	// Wait for doc to be imported
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Equal(c, int64(2), rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())
	}, 10*time.Second, 100*time.Millisecond)

	// Ensure sync function has run on import
	assert.Equal(t, int64(2), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	// Assert the sync data has correct channels populated
	syncData, err = rt.GetSingleTestDatabaseCollection().GetDocSyncData(ctx, docKey2)
	require.NoError(t, err)
	assert.Len(t, syncData.Channels, 2)
}

// TestUnmarshalDocFromImportFeed:
//   - Construct data as seen over dcp on the import feed with both _sync and user xattr defined
//   - Assert each value returned from UnmarshalDocumentSyncDataFromFeed is as expected
//   - Construct data as seen over dcp on the imp[ort feed with just user xattr defined
//   - Assert each value returned from UnmarshalDocumentSyncDataFromFeed is as expected
//   - Construct data as seen over dcp on the import feed with no xattrs at all
//   - Assert each value returned from UnmarshalDocumentSyncDataFromFeed is as expected
func TestUnmarshalDocFromImportFeed(t *testing.T) {

	const (
		userXattrKey = "channels"
		syncXattr    = `{"sequence":200}`
		channelName  = "chan1"
	)

	// construct data into dcp format with both _sync xattr and user xattr defined
	body := []byte(`{"test":"document"}`)

	// 3.1.x backport NOTE:
	// We don't have sgbucket.Xattr or EncodeValueWithXattrs available, as those changes are mixed with Rosmar refactoring, so it's not easy to cherry-pick.
	// `value` has been replaced with pre-computed values taken from `main` during this backport.

	value := []byte{0x0, 0x0, 0x0, 0x2e, 0x0, 0x0, 0x0, 0x17, 0x5f, 0x73, 0x79, 0x6e, 0x63, 0x0, 0x7b, 0x22, 0x73, 0x65, 0x71, 0x75, 0x65, 0x6e, 0x63, 0x65, 0x22, 0x3a, 0x32, 0x30, 0x30, 0x7d, 0x0, 0x0, 0x0, 0x0, 0xf, 0x63, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x73, 0x0, 0x63, 0x68, 0x61, 0x6e, 0x31, 0x0, 0x7b, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x3a, 0x22, 0x64, 0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x22, 0x7d}

	syncData, rawBody, rawXattr, rawUserXattr, err := db.UnmarshalDocumentSyncDataFromFeed(value, 5, userXattrKey, false)
	require.NoError(t, err)
	assert.Equal(t, syncXattr, string(rawXattr))
	assert.Equal(t, uint64(200), syncData.Sequence)
	assert.Equal(t, channelName, string(rawUserXattr))
	assert.Equal(t, body, rawBody)

	// construct data into dcp format with just user xattr defined
	value = []byte{0x0, 0x0, 0x0, 0x13, 0x0, 0x0, 0x0, 0xf, 0x63, 0x68, 0x61, 0x6e, 0x6e, 0x65, 0x6c, 0x73, 0x0, 0x63, 0x68, 0x61, 0x6e, 0x31, 0x0, 0x7b, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x3a, 0x22, 0x64, 0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x22, 0x7d}

	syncData, rawBody, rawXattr, rawUserXattr, err = db.UnmarshalDocumentSyncDataFromFeed(value, 5, userXattrKey, false)
	require.NoError(t, err)
	assert.Nil(t, syncData)
	assert.Nil(t, rawXattr)
	assert.Equal(t, channelName, string(rawUserXattr))
	assert.Equal(t, body, rawBody)

	// construct data into dcp format with no xattr defined
	value = []byte{0x0, 0x0, 0x0, 0x0, 0x7b, 0x22, 0x74, 0x65, 0x73, 0x74, 0x22, 0x3a, 0x22, 0x64, 0x6f, 0x63, 0x75, 0x6d, 0x65, 0x6e, 0x74, 0x22, 0x7d}

	syncData, rawBody, rawXattr, rawUserXattr, err = db.UnmarshalDocumentSyncDataFromFeed(value, 5, userXattrKey, false)
	require.NoError(t, err)
	assert.Nil(t, syncData)
	assert.Nil(t, rawXattr)
	assert.Nil(t, rawUserXattr)
	assert.Equal(t, body, rawBody)
}
