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
)

func TestUserXattrAutoImport(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

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
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

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
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

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
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

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
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

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
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

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
