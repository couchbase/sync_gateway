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
	"log"
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

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changesResponse := rt.SendAdminRequest("GET", "/{{.keyspace1}}/_changes?since=0", "")
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace2}}/_changes?since=0", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	// Put more documents, should be served via DCP/cache
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/abc2", `{"value":1, "channels":["ABC"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace1}}/_changes?since=0", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace2}}/_changes?since=0", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())
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

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changesResponse := rt.SendAdminRequest("GET", "/{{.keyspace1}}/_changes?since=0", "")
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace2}}/_changes?since=0", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	// Put more documents, should be served via DCP/cache
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs2_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs2_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace1}}/_changes?since=0", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace2}}/_changes?since=0", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())
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

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changesResponse := rt.SendUserRequest("GET", "/{{.keyspace1}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace2}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	// Put more documents, should be served via DCP/cache
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/pbs2_c1", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/pbs2_c2", `{"value":1, "channels":["PBS"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace1}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace2}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())
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
	changesResponse := rt.GetChangesOneShot(t, "keyspace1", 0, "bernard", 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.GetChangesOneShot(t, "keyspace2", 0, "bernard", 0)
	logChangesResponse(t, changesResponse.Body.Bytes())

	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/brn2_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/chr2_c1", `{"value":1, "channels":["CHR"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/brn2_c2", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/chr2_c2", `{"value":1, "channels":["CHR"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changesResponse = rt.GetChangesOneShot(t, "keyspace1", 0, "bernard", 2)
	logChangesResponse(t, changesResponse.Body.Bytes())

	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/brn3_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changesResponse = rt.GetChangesOneShot(t, "keyspace1", 0, "charlie", 2)
	logChangesResponse(t, changesResponse.Body.Bytes())

	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/brn4_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.keyspace1}}/brn1_c1?rev=%s", brn1_c1Rev), ``)
	rest.RequireStatus(t, response, 200)

	response = rt.SendAdminRequest("PUT", "/{{.keyspace2}}/brn4_c1", `{"value":1, "channels":["BRN"]}`)
	rest.RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/{{.keyspace1}}/chr3_c1", `{"value":1, "channels":["CHR"]}`)
	rest.RequireStatus(t, response, 201)
	_ = rt.WaitForPendingChanges()

	changesResponse = rt.GetChangesOneShot(t, "keyspace1", 0, "bernard", 4)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.GetChangesOneShot(t, "keyspace2", 0, "BRNchr", 3) // 2 docs in BRN, 1 doc in CHR in collection 2
	logChangesResponse(t, changesResponse.Body.Bytes())

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

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changesResponse := rt.SendUserRequest("GET", "/{{.keyspace1}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace2}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())
	lastSeq := changes.Last_Seq

	// Grant user access to channel ABC in collection 1
	err = rt.SetAdminChannels("bernard", rt.GetKeyspaces()[0], "ABC", "PBS")
	require.NoError(t, err)
	require.NoError(t, rt.WaitForPendingChanges())

	// confirm that change from c1 is sent, along with user doc
	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace1}}/_changes?since="+lastSeq.String(), "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())

	// Confirm that access hasn't been granted in c2, expect only user doc
	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace2}}/_changes?since="+lastSeq.String(), "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())
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

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Issue changes request.  Will initialize cache for user channel (PBS), and return docs via query
	changesResponse := rt.SendUserRequest("GET", "/{{.keyspace1}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace2}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())
	lastSeq := changes.Last_Seq

	// Issue admin changes request for channel ABC, to initialize cache for that channel in each collection
	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace1}}/_changes?filter=sync_gateway/bychannel&channels=ABC", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendAdminRequest("GET", "/{{.keyspace2}}/_changes?filter=sync_gateway/bychannel&channels=ABC", "")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	logChangesResponse(t, changesResponse.Body.Bytes())

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

	// Expect 5 documents in collection with ABC grant:
	//  - backfill of 1 ABC doc written prior to lastSeq
	//  - user doc
	//  - 1 PBS docs after lastSeq
	//  - 1 ABC docs after lastSeq
	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace1}}/_changes?since="+lastSeq.String(), "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Len(t, changes.Results, 4)
	logChangesResponse(t, changesResponse.Body.Bytes())

	// Expect 2 documents in collection without ABC grant
	//  - user doc
	//  - 1 PBS doc after lastSeq
	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace2}}/_changes?since="+lastSeq.String(), "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	assert.Len(t, changes.Results, 2)
	logChangesResponse(t, changesResponse.Body.Bytes())
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

	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	// Issue changes request.  Will initialize cache for channels, and return docs via query
	changesResponse := rt.SendUserRequest("GET", "/{{.keyspace1}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "doc1", changes.Results[0].ID)
	logChangesResponse(t, changesResponse.Body.Bytes())

	changesResponse = rt.SendUserRequest("GET", "/{{.keyspace2}}/_changes?since=0", "", "bernard")
	err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
	assert.NoError(t, err, "Error unmarshalling changes response")
	require.Len(t, changes.Results, 1)
	assert.Equal(t, "publicDoc", changes.Results[0].ID)
	logChangesResponse(t, changesResponse.Body.Bytes())
}

func logChangesResponse(t *testing.T, response []byte) {
	var changes struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}

	err := base.JSONUnmarshal(response, &changes)
	require.NoError(t, err, "unable to marshal changes for logging")

	for _, entry := range changes.Results {
		log.Printf("changes Response entry: %+v", entry)
	}
	return
}
