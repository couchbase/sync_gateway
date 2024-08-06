//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package changestest

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/channels"

	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/require"
)

func TestMultiCollectionChangesAdmin(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	rt := rest.NewRestTesterMultipleCollections(t, nil, numCollections)
	defer rt.Close()

	// Put several documents, will be retrieved via query
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/abc1", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)

	_ = rt.WaitForPendingChanges()

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	requireAdminChangesCount(rt, "{{.keyspace1}}", 1)
	requireAdminChangesCount(rt, "{{.keyspace2}}", 1)

	// Put more documents, should be served via DCP/cache
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/abc2", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	requireAdminChangesCount(rt, "{{.keyspace1}}", 2)
	requireAdminChangesCount(rt, "{{.keyspace2}}", 2)
}

func TestMultiCollectionChangesAdminSameChannelName(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyChanges, base.KeyCache)

	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	rt := rest.NewRestTesterMultipleCollections(t, nil, numCollections)
	defer rt.Close()

	// Put several documents, will be retrieved via query
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs1_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs1_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	requireAdminChangesCount(rt, "{{.keyspace1}}", 1)
	requireAdminChangesCount(rt, "{{.keyspace2}}", 1)

	// Put more documents, should be served via DCP/cache
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs2_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs2_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	requireAdminChangesCount(rt, "{{.keyspace1}}", 2)
	requireAdminChangesCount(rt, "{{.keyspace2}}", 2)
}

func TestMultiCollectionChangesUser(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyChanges, base.KeyCache, base.KeyCRUD)
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	rtConfig := &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction}
	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, numCollections)
	defer rt.Close()

	// Create user with access to channel PBS in both collections
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents, will be retrieved via query
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs1_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs1_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changes := rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	changes = rt.GetChanges("/{{.keyspace2}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)

	// Put more documents, should be served via DCP/cache
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs2_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs2_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changes = rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 2)
	changes = rt.GetChanges("/{{.keyspace2}}/_changes", "bernard")
	require.Len(t, changes.Results, 2)
}

func TestMultiCollectionChangesMultiChannelOneShot(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyChanges, base.KeyCache, base.KeyCRUD)
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	rt := rest.NewRestTesterMultipleCollections(t, &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction}, numCollections)
	defer rt.Close()

	// Create user with access to channel PBS in both collections
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "BRN"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))
	charlie, err := a.NewUser("charlie", "letmein", channels.BaseSetOf(t, "CHR"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(charlie))
	BRNchr, err := a.NewUser("BRNchr", "letmein", channels.BaseSetOf(t, "CHR", "BRN"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(BRNchr))

	// Put several documents, will be retrieved via query
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/brn1_c1", `{"channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)

	// Keep rev to delete doc later
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	brn1_c1Rev := body["rev"].(string)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/chr1_c1", `{"channels":["CHR"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	// Issue changes request.  Will initialize cache for channels, and return docs via queries
	changes := rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	changes = rt.GetChanges("/{{.keyspace2}}/_changes", "bernard")
	require.Len(t, changes.Results, 0)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/brn2_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/chr2_c1", `{"value":1, "channels":["CHR"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/brn2_c2", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/chr2_c2", `{"value":1, "channels":["CHR"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changes = rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 2)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/brn3_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changes = rt.GetChanges("/{{.keyspace1}}/_changes", "charlie")
	require.Len(t, changes.Results, 2)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/brn4_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace1}}/brn1_c1?rev=%s", brn1_c1Rev), ``)
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/brn4_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/chr3_c1", `{"value":1, "channels":["CHR"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changes = rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 4)
	changes = rt.GetChanges("/{{.keyspace2}}/_changes", "BRNchr")
	// 2 docs in BRN, 1 doc in CHR in collection 2
	require.Len(t, changes.Results, 3)

	rt.RequireContinuousFeedChangesCount(t, "bernard", 1, 4, 500)
	rt.RequireContinuousFeedChangesCount(t, "bernard", 2, 2, 500)

	rt.RequireContinuousFeedChangesCount(t, "charlie", 1, 3, 500)
	rt.RequireContinuousFeedChangesCount(t, "charlie", 2, 1, 500)
	rt.RequireContinuousFeedChangesCount(t, "BRNchr", 1, 7, 1000)
	rt.RequireContinuousFeedChangesCount(t, "BRNchr", 2, 3, 1000)
}

// TestMultiCollectionChangesUserDynamicGrant tests a dynamic channel grant when that channel is not already resident
// in the cache (post-grant changes triggers query backfill of the cache)
func TestMultiCollectionChangesUserDynamicGrant(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyChanges, base.KeyCache, base.KeyCRUD)
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	rtConfig := &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction}
	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, numCollections)
	defer rt.Close()

	// Create user with access to channel PBS in both collections
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs1_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/abc1_c1", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs1_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/abc1_c2", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	require.NoError(t, rt.WaitForPendingChanges())

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changes := rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	changes = rt.GetChanges("/{{.keyspace2}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	lastSeq := changes.Last_Seq.String()

	// Grant user access to channel ABC in collection 1
	err = rt.SetAdminChannels("bernard", rt.GetKeyspaces()[0], "ABC", "PBS")
	require.NoError(t, err)
	require.NoError(t, rt.WaitForPendingChanges())

	// confirm that change from c1 is sent, along with user doc
	changes = rt.GetChanges("/{{.keyspace1}}/_changes?since="+lastSeq, "bernard")
	require.Len(t, changes.Results, 2)
	// Confirm that access hasn't been granted in c2, expect only user doc
	changes = rt.GetChanges("/{{.keyspace2}}/_changes?since="+lastSeq, "bernard")
	require.Len(t, changes.Results, 1)
}

// TestMultiCollectionChangesUserDynamicGrant tests a dynamic channel grant when that channel is resident
// in the cache (verifies cache buffering of principals)
func TestMultiCollectionChangesUserDynamicGrantDCP(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyChanges, base.KeyCache, base.KeyCRUD)
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	rtConfig := &rest.RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction}
	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, numCollections)
	defer rt.Close()

	// Create user with access to channel PBS in both collections
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "PBS"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs1_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/abc1_c1", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs1_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/abc1_c2", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	// Issue changes request.  Will initialize cache for user channel (PBS), and return docs via query
	changes := rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	changes = rt.GetChanges("/{{.keyspace2}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	lastSeq := changes.Last_Seq.String()

	// Issue admin changes request for channel ABC, to initialize cache for that channel in each collection
	changes = rt.PostChangesAdmin("/{{.keyspace1}}/_changes?filter=sync_gateway/bychannel&channels=ABC", "{}")
	require.Len(t, changes.Results, 1)

	changes = rt.PostChangesAdmin("/{{.keyspace2}}/_changes?filter=sync_gateway/bychannel&channels=ABC", "{}")
	require.Len(t, changes.Results, 1)

	// Grant user access to channel ABC in collection 1
	err = rt.SetAdminChannels("bernard", rt.GetKeyspaces()[0], "ABC", "PBS")
	require.NoError(t, err)

	// Write additional docs to the cached channels, should be served via DCP/cache
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/abc2_c1", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/abc2_c2", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs2_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs2_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	// Expect 4 documents in collection with ABC grant:
	//  - backfill of 1 ABC doc written prior to lastSeq
	//  - user doc
	//  - 1 PBS docs after lastSeq
	//  - 1 ABC docs after lastSeq
	changes = rt.GetChanges("/{{.keyspace1}}/_changes?since="+lastSeq, "bernard")
	require.Len(t, changes.Results, 4)

	// Expect 2 documents in collection without ABC grant
	//  - user doc
	//  - 1 PBS doc after lastSeq
	changes = rt.GetChanges("/{{.keyspace2}}/_changes?since="+lastSeq, "bernard")
	require.Len(t, changes.Results, 2)
}

func TestMultiCollectionChangesCustomSyncFunctions(t *testing.T) {
	base.TestRequiresCollections(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyHTTP, base.KeyChanges, base.KeyCache, base.KeyCRUD)
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)

	testBucket := base.GetTestBucket(t)
	scopesConfig := rest.GetCollectionsConfig(t, testBucket, numCollections)
	dataStoreNames := rest.GetDataStoreNamesFromScopesConfig(scopesConfig)

	c1SyncFunction := `function(doc, oldDoc) { channel("collection1")}`
	c2SyncFunction := `
	function(doc, oldDoc) {
		channel("collection2")
		if (doc.public) {
			channel("!")
		}
	}`
	scopesConfig[dataStoreNames[0].ScopeName()].Collections[dataStoreNames[0].CollectionName()] = &rest.CollectionConfig{SyncFn: &c1SyncFunction}
	scopesConfig[dataStoreNames[1].ScopeName()].Collections[dataStoreNames[1].CollectionName()] = &rest.CollectionConfig{SyncFn: &c2SyncFunction}

	rtConfig := &rest.RestTesterConfig{
		CustomTestBucket: testBucket,
		DatabaseConfig: &rest.DatabaseConfig{
			DbConfig: rest.DbConfig{
				Scopes: scopesConfig,
			},
		},
	}

	rt := rest.NewRestTesterMultipleCollections(t, rtConfig, numCollections)
	defer rt.Close()

	// Create user with access to channel collection1 in both collections
	ctx := rt.Context()
	a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)
	bernard, err := a.NewUser("bernard", "letmein", channels.BaseSetOf(t, "collection1"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put two documents
	response := rt.SendAdminRequest("PUT", "/{{.keyspace1}}/doc1", `{"value":1}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/doc1", `{"value":1}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/publicDoc", `{"value":1, "public":true}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changes := rt.GetChanges("/{{.keyspace1}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "doc1", changes.Results[0].ID)

	changes = rt.GetChanges("/{{.keyspace2}}/_changes", "bernard")
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "publicDoc", changes.Results[0].ID)
}

func requireAdminChangesCount(rt *rest.RestTester, keyspace string, expectedChangeCount int) {
	changesResponse := rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/%s/_changes", keyspace), "")
	rest.RequireStatus(rt.TB(), changesResponse, http.StatusOK)
	var changes rest.ChangesResults
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	require.NoError(rt.TB(), err, "Error unmarshalling changes response: %s", changesResponse.BodyString())
	require.Len(rt.TB(), changes.Results, expectedChangeCount)
}
