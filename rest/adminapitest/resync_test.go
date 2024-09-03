// Copyright 2023-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package adminapitest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/gocbcore/v10"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResyncRollback ensures that we allow rollback of
func TestResyncRollback(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test doesn't works with walrus")
	}
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		SyncFn: `function(doc) { channel("x") }`, // use custom sync function to increment sync function counter
	})

	defer rt.Close()

	numDocs := 10
	for i := 0; i < numDocs; i++ {
		rt.CreateTestDoc(fmt.Sprintf("doc%v", i))
	}
	assert.Equal(t, int64(numDocs), rt.GetDatabase().DbStats.Database().SyncFunctionCount.Value())

	response := rt.SendAdminRequest("POST", "/{{.db}}/_offline", "")
	rest.RequireStatus(t, response, http.StatusOK)
	require.NoError(t, rt.WaitForDBState(db.RunStateString[db.DBOffline]))

	// we need to wait for the resync to start and not finish so we get a partial completion
	resp := rt.SendAdminRequest("POST", "/{{.db}}/_resync", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	_ = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateRunning)

	// immediately stop the resync process (we just need the status data to be persisted to the bucket), we are looking for partial completion
	resp = rt.SendAdminRequest("POST", "/{{.db}}/_resync?action=stop", "")
	rest.RequireStatus(t, resp, http.StatusOK)
	status := rt.WaitForResyncDCPStatus(db.BackgroundProcessStateStopped)
	// make sure this hasn't accidentally completed
	require.Equal(t, db.BackgroundProcessStateStopped, status.State)

	// alter persisted dcp metadata from the first run to force a rollback
	name := db.GenerateResyncDCPStreamName(status.ResyncID)
	checkpointPrefix := fmt.Sprintf("%s:%v", rt.GetDatabase().MetadataKeys.DCPCheckpointPrefix(rt.GetDatabase().Options.GroupID), name)
	meta := base.NewDCPMetadataCS(rt.Context(), rt.Bucket().DefaultDataStore(), 1024, 8, checkpointPrefix)
	vbMeta := meta.GetMeta(0)
	var garbageVBUUID gocbcore.VbUUID = 1234
	vbMeta.VbUUID = garbageVBUUID
	meta.SetMeta(0, vbMeta)
	meta.Persist(rt.Context(), 0, []uint16{0})

	response = rt.SendAdminRequest("POST", "/db/_resync?action=start", "")
	rest.RequireStatus(t, response, http.StatusOK)
	status = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)
}

func TestResyncRegenerateSequencesPrincipals(t *testing.T) {
	base.TestRequiresDCPResync(t)
	if !base.TestsUseNamedCollections() {
		t.Skip("Test requires named collections, performs default collection handling independently")
	}

	testCases := []struct {
		name                  string
		defaultCollectionOnly bool
		specifyCollections    bool
	}{
		{
			name:                  "defaultCollectionOnly=true, specifyCollections=false",
			defaultCollectionOnly: true,
			specifyCollections:    false,
		},
		{
			name:                  "defaultCollectionOnly=true, specifyCollections=true",
			defaultCollectionOnly: true,
			specifyCollections:    true,
		},
		{
			name:                  "defaultCollectionOnly=false, specifyCollections=true",
			defaultCollectionOnly: false,
			specifyCollections:    true,
		},
		{
			name:                  "defaultCollectionOnly=false, specifyCollections=false",
			defaultCollectionOnly: false,
			specifyCollections:    false,
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			rt := rest.NewRestTester(t, &rest.RestTesterConfig{
				PersistentConfig: true,
			})
			defer rt.Close()

			dbConfig := rt.NewDbConfig()
			ds, err := rt.TestBucket.GetNamedDataStore(0)
			require.NoError(t, err)
			scopeName := ds.ScopeName()
			collectionName := ds.CollectionName()
			if test.defaultCollectionOnly {
				dbConfig.Scopes = nil
				scopeName = base.DefaultScope
				collectionName = base.DefaultCollection
			}
			rest.RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

			username := "alice"
			standardDoc := "doc"
			rt.CreateUser(username, nil)

			ctx := rt.Context()
			user, err := rt.GetDatabase().Authenticator(ctx).GetUser(username)
			require.NoError(t, err)
			originalUserSeq := user.Sequence()
			require.NotEqual(t, 0, originalUserSeq)

			rt.CreateTestDoc(standardDoc)
			collection, ctx := rt.GetSingleTestDatabaseCollection()
			doc, err := collection.GetDocument(ctx, standardDoc, db.DocUnmarshalSync)
			require.NoError(t, err)
			oldDocSeq := doc.Sequence
			require.NotEqual(t, 0, oldDocSeq)

			rt.TakeDbOffline()

			body := ""
			if test.specifyCollections {
				// regenerate_sequences=true with collections will not regenerate sequences of principals
				collectionsToResync := map[string][]string{
					scopeName: []string{collectionName},
				}
				body = fmt.Sprintf(`{"scopes": %s}`, base.MustJSONMarshal(t, collectionsToResync))
			}
			rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_resync?action=start&regenerate_sequences=true", body), http.StatusOK)
			_ = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

			// validate if user doc has changed sequence
			user, err = rt.GetDatabase().Authenticator(ctx).GetUser(username)
			require.NoError(t, err)

			require.NotEqual(t, 0, user.Sequence())
			if test.specifyCollections {
				require.Equal(t, originalUserSeq, user.Sequence())
			} else {
				require.NotEqual(t, originalUserSeq, user.Sequence())
			}

			// regular doc will always change sequence
			doc, err = collection.GetDocument(ctx, standardDoc, db.DocUnmarshalSync)
			require.NoError(t, err)
			require.NotEqual(t, 0, doc.Sequence)
			require.NotEqual(t, oldDocSeq, doc.Sequence)
		})
	}
}

func TestResyncInvalidatePrincipals(t *testing.T) {
	initialSyncFn := `
	function(doc) {
		access(doc.userName, "channelABC");
		access("role:" + doc.roleName, "channelABC");
		role(doc.userName, "role:roleABC");
	}`

	updatedSyncFn := `
	function(doc) {
		access(doc.userName, "channelDEF");
		access("role:" + doc.roleName, "channelDEF");
		role(doc.userName, "role:roleDEF");
	}`

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{
		PersistentConfig: true,
		SyncFn:           initialSyncFn,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	ds := rt.TestBucket.GetSingleDataStore()
	scopeName := ds.ScopeName()
	collectionName := ds.CollectionName()

	rest.RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)

	// Set up user and role
	username := "alice"
	rolename := "foo"
	grantingDocID := "grantDoc"
	grantingDocBody := `{
		"userName":"alice",
		"roleName":"foo"
	}`
	rt.CreateUser(username, nil)

	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+rolename, rest.GetRolePayload(t, rolename, rt.GetSingleDataStore(), nil))
	rest.RequireStatus(t, response, http.StatusCreated)

	// Write doc to perform dynamic grants
	response = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/"+grantingDocID, grantingDocBody)
	rest.RequireStatus(t, response, http.StatusCreated)

	ctx := rt.Context()
	user, err := rt.GetDatabase().Authenticator(ctx).GetUser(username)
	require.NoError(t, err)
	channels := user.CollectionChannels(scopeName, collectionName)
	_, ok := channels["channelABC"]
	require.True(t, ok, "user should have channel channelABC")
	roles := user.RoleNames()
	_, ok = roles["roleABC"]
	require.True(t, ok, "user should have role roleABC")

	rt.TakeDbOffline()

	// Update the sync function
	rt.SyncFn = updatedSyncFn
	updatedDbConfig := rt.NewDbConfig()
	rt.UpsertDbConfig("db1", updatedDbConfig)
	rt.TakeDbOffline()

	// Run resync
	rest.RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_resync?action=start", ""), http.StatusOK)
	_ = rt.WaitForResyncDCPStatus(db.BackgroundProcessStateCompleted)

	// validate user channels and roles have been updated
	user, err = rt.GetDatabase().Authenticator(ctx).GetUser(username)
	require.NoError(t, err)
	channels = user.CollectionChannels(scopeName, collectionName)
	_, ok = channels["channelABC"]
	require.False(t, ok, "user should not have channel channelABC")
	_, ok = channels["channelDEF"]
	require.True(t, ok, "user should have channel channelDEF")
	roles = user.RoleNames()
	_, ok = roles["roleABC"]
	require.False(t, ok, "user should not have role roleABC")
	_, ok = roles["roleDEF"]
	require.True(t, ok, "user should have role roleDEF")
}
