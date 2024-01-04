// Copyright 2024-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package importuserxattrtest

import (
	"log"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUserXattrAvoidRevisionIDGeneration(t *testing.T) {
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
					console.log(JSON.stringify(meta));
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

	require.NoError(t, rt.WaitForPendingChanges())

	// Get current sync data
	var syncData db.SyncData
	_, err := dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	docRev, err := rt.GetSingleTestDatabaseCollection().GetRevisionCacheForTest().Get(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(docRev.Channels.ToArray()))
	assert.Equal(t, syncData.CurrentRev, docRev.RevID)

	// Write xattr to trigger import of user xattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure import worked and sequence incremented but that sequence did not
	var syncData2 db.SyncData
	_, err = dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	docRev2, err := rt.GetSingleTestDatabaseCollection().GetRevisionCacheForTest().Get(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
	assert.NoError(t, err)
	assert.Equal(t, syncData2.CurrentRev, docRev2.RevID)

	assert.Equal(t, syncData.CurrentRev, syncData2.CurrentRev)
	assert.True(t, syncData2.Sequence > syncData.Sequence)
	assert.Equal(t, []string{channelName}, syncData2.Channels.KeySet())
	assert.Equal(t, syncData2.Channels.KeySet(), docRev2.Channels.ToArray())

	err = rt.GetSingleDataStore().Set(docKey, 0, nil, []byte(`{"update": "update"}`))
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		log.Printf("Import count is: %v", rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	var syncData3 db.SyncData
	_, err = dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	assert.NotEqual(t, syncData2.CurrentRev, syncData3.CurrentRev)
}
