//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/auth"

	"golang.org/x/exp/maps"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

type putResponse struct {
	OK  bool   `json:"ok"`
	Rev string `json:"rev"`
}

func TestGetAllChannelsByUser(t *testing.T) {
	if base.TestsUseNamedCollections() {
		t.Skip("This test only works with default collection")
	}

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,

		SyncFn: `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.user, doc.role);}`,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()

	dbName := "db"
	rt.CreateDatabase("db", dbConfig)

	// create sessions before users
	const alice = "alice"
	const bob = "bob"

	// Put user alice and assert admin assigned channels are returned by the get all_channels endpoint
	response := rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice,
		`{"name": "`+alice+`", "password": "`+RestTesterDefaultUserPassword+`", "admin_channels": ["A","B","C"]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	var channelMap getAllChannelsResponse
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants["_default._default"]), []string{"A", "B", "C"})

	// Assert non existent user returns 404
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusNotFound)

	// Put user bob and assert on channels returned by all_channels
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+bob,
		`{"name": "`+bob+`", "password": "`+RestTesterDefaultUserPassword+`", "admin_channels": []}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants["_default._default"]), []string{})

	// Assign new channel to user bob and assert all_channels includes it
	response = rt.SendAdminRequest(http.MethodPut,
		"/{{.keyspace}}/doc1",
		`{"accessChannel":"NewChannel", "accessUser":["bob","alice"]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants["_default._default"]), []string{"!", "NewChannel"})

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants["_default._default"]), []string{"!", "NewChannel"})

	response = rt.SendAdminRequest("PUT", "/db/_role/role1", `{"admin_channels":["chan"]}`)
	RequireStatus(t, response, http.StatusCreated)

	// Assign new channel to user bob through role1 and assert all_channels includes it
	response = rt.SendAdminRequest(http.MethodPut,
		"/{{.keyspace}}/doc2",
		`{"role":"role:role1", "user":"bob"}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants["_default._default"]), []string{"!", "NewChannel"})
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"]["_default._default"]), []string{"chan", "!"})

}

func TestGetAllChannelsByUserWithCollections(t *testing.T) {
	SyncFn := `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.user, doc.role);}`
	base.TestRequiresCollections(t)

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true}, 2)
	defer rt.Close()

	dbName := "db"
	tb := base.GetTestBucket(t)
	defer tb.Close(rt.Context())
	scopesConfig := GetCollectionsConfig(t, tb, 2)
	for scope, _ := range scopesConfig {
		for _, cnf := range scopesConfig[scope].Collections {
			cnf.SyncFn = &SyncFn
		}
	}
	dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)
	rt.CreateDatabase("db", dbConfig)

	scopeName := rt.GetDbCollections()[0].ScopeName
	collection1Name := rt.GetDbCollections()[0].Name
	collection2Name := rt.GetDbCollections()[1].Name
	scopesConfig[scopeName].Collections[collection1Name] = &CollectionConfig{}
	keyspace1 := scopeName + "." + collection1Name
	keyspace2 := scopeName + "." + collection2Name

	collectionPayload := `,"%s": {
					"admin_channels":[%s]
				}`

	// create sessions before users
	const alice = "alice"
	const bob = "bob"
	userPayload := `{
		%s
		"collection_access": {
			"%s": {
				"%s": {
					"admin_channels":["A", "B", "C"]
				}
				%s
			}
		}
	}`
	// Put user alice and assert admin assigned channels are returned by the get all_channels endpoint
	response := rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`,
			scopeName, collection1Name, fmt.Sprintf(collectionPayload, collection2Name, `"a"`)))
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	var channelMap getAllChannelsResponse
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[keyspace1]), []string{"A", "B", "C"})

	// Assert non existent user returns 404
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusNotFound)

	// Put user bob and assert on channels returned by all_channels
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+bob,
		`{"name": "`+bob+`", "password": "`+RestTesterDefaultUserPassword+`", "admin_channels": []}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[keyspace1]), []string{})

	// Assign new channel to user bob and assert all_channels includes it
	response = rt.SendAdminRequest(http.MethodPut,
		fmt.Sprintf("/%s/%s", rt.GetKeyspaces()[0], "doc1"),
		`{"accessChannel":"NewChannel", "accessUser":["bob","alice"]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)

	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[keyspace1]), []string{"!", "NewChannel"})

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[keyspace1]), []string{"NewChannel", "!"})

	response = rt.SendAdminRequest("PUT", "/db/_role/role1", fmt.Sprintf(`
		{		
			"collection_access": {
				"%s": {
					"%s": {
						"admin_channels":["role1Chan"]
					},
					"%s": {
						"admin_channels":[]
					}
				}
			}
		}`, scopeName, collection2Name, collection1Name))
	RequireStatus(t, response, http.StatusCreated)

	// Assign new channel to user bob through role1 and assert all_channels includes it
	response = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace2}}/doc2",
		`{"role":"role:role1", "user":"bob", "accessUser":"alice", "accessChannel":["aliceDynamic"]}`)
	RequireStatus(t, response, http.StatusCreated)
	var putResp putResponse
	err = json.Unmarshal(response.BodyBytes(), &putResp)
	require.NoError(t, err)

	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`,
			scopeName, collection1Name, fmt.Sprintf(collectionPayload, collection2Name, `"a", "chan2coll2"`)))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"][keyspace2]), []string{"role1Chan", "!"})
	assert.Equal(t, channelMap.DynamicRoleGrants["role1"][keyspace2]["role1Chan"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 4, EndSeq: 0}})

	revID := putResp.Rev
	response = rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/{{.keyspace2}}/doc2?rev=%s", revID), "")
	RequireStatus(t, response, http.StatusOK)
	// and wait for a few to be done before we proceed with updating database config underneath replication
	_, err = rt.WaitForChanges(1, "/{{.keyspace2}}/_changes", "", true)
	require.NoError(t, err)
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	t.Log(channelMap)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"][keyspace2]), []string{"role1Chan", "!"})
	assert.Equal(t, channelMap.DynamicRoleGrants["role1"][keyspace2]["role1Chan"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 5, EndSeq: 7}})
	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_role/role1", ``)
	RequireStatus(t, response, http.StatusOK)
	t.Log(response.BodyString())

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[keyspace2]), []string{"a", "chan2coll2"})
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[keyspace2]), []string{"!", "aliceDynamic"})

	//Remove channels a and chan2coll2 and assert theyre still in DynamicGrants
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`,
			scopeName, collection1Name, fmt.Sprintf(collectionPayload, collection2Name, ``)))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[keyspace2]), []string{"a", "chan2coll2"})
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[keyspace2]), []string{"!", "aliceDynamic"})
	assert.Equal(t, channelMap.AdminGrants[keyspace2]["a"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 1, EndSeq: 8}})
	assert.Equal(t, channelMap.DynamicGrants[keyspace2]["aliceDynamic"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 5, EndSeq: 6}})

}

func TestGetAllChannelsByUserWithSingleNamedCollection(t *testing.T) {
	SyncFn := `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.user, doc.role);}`
	base.TestRequiresCollections(t)

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true}, 1)
	defer rt.Close()

	const dbName = "db"

	// implicit default scope/collection
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = nil
	resp := rt.CreateDatabase(dbName, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	expectedKeyspaces := []string{
		dbName,
	}
	assert.Equal(t, expectedKeyspaces, rt.GetKeyspaces())

	newCollection := base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: t.Name()}
	require.NoError(t, rt.TestBucket.CreateDataStore(base.TestCtx(t), newCollection))
	defer func() {
		require.NoError(t, rt.TestBucket.DropDataStore(newCollection))
	}()

	resp = rt.UpsertDbConfig(dbName, DbConfig{Scopes: ScopesConfig{
		base.DefaultScope: {Collections: CollectionsConfig{
			base.DefaultCollection:         {SyncFn: &SyncFn},
			newCollection.CollectionName(): {SyncFn: &SyncFn},
		}},
	}})
	RequireStatus(t, resp, http.StatusCreated)

	const alice = "alice"
	userPayload := `{
		%s
		"admin_channels":[%s],
		"collection_access": {
			"%s": {
				"%s": {
					"admin_channels":["A", "B", "C"]
				}

			}
		}
	}`
	// Put user alice and assert admin assigned channels are returned by the get all_channels endpoint
	response := rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`,
			`"foo", "bar"`, "_default", newCollection.Collection))
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	var channelMap getAllChannelsResponse
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[newCollection.String()]), []string{"A", "B", "C"})
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants["_default._default"]), []string{"foo", "bar"})

	// Assign new channel to user alice and assert all_channels includes it
	newKeyspace := dbName + "." + newCollection.String()
	response = rt.SendAdminRequest(http.MethodPut,
		fmt.Sprintf("/%s/%s", newKeyspace, "doc1"),
		`{"accessChannel":"dynChannel", "accessUser":["alice"]}`)
	RequireStatus(t, response, http.StatusCreated)
	var putResp putResponse
	err = json.Unmarshal(response.BodyBytes(), &putResp)
	require.NoError(t, err)

	revID := putResp.Rev
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)

	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[newCollection.String()]), []string{"!", "dynChannel"})

	// remove dynchannel from collection_access channels and assert it shows up in dynamic grants
	response = rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/%s/%s?rev=%s", newKeyspace, "doc1", revID), "")
	RequireStatus(t, response, http.StatusOK)

	// Overwrite admin_channels and remove foo and bar channels
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`,
			`"channel"`, "_default", newCollection.Collection))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)

	require.NoError(t, err)

	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[newCollection.String()]), []string{"!", "dynChannel"})
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants["_default._default"]), []string{"foo", "bar", "channel"})
}
