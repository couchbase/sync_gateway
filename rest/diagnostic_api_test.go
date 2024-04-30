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
	"strings"
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

const fakeUpdatedTime = 1234

// convertUpdatedTimeToConstant replaces the updated_at field in the response with a constant value to be able to diff
func convertUpdatedTimeToConstant(t testing.TB, output []byte) string {
	var channelMap getAllChannelsResponse

	err := json.Unmarshal(output, &channelMap)
	require.NoError(t, err)

	for keyspace, channels := range channelMap.AdminGrants {
		for channelName, grant := range channels {
			if grant.UpdatedAt == 0 {
				continue
			}
			grant.UpdatedAt = fakeUpdatedTime
			channelMap.AdminGrants[keyspace][channelName] = grant
		}
	}
	for roleName, keyspaces := range channelMap.DynamicRoleGrants {
		for keyspace, channels := range keyspaces {
			for channelName, grant := range channels {

				if grant.UpdatedAt == 0 {
					continue
				}
				grant.UpdatedAt = fakeUpdatedTime
				channelMap.DynamicRoleGrants[roleName][keyspace][channelName] = grant
			}
		}
	}
	fmt.Printf("Converted response: %s\n", string(base.MustJSONMarshal(t, channelMap)))
	return string(base.MustJSONMarshal(t, channelMap))
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
	channelMap = getAllChannelsResponse{}
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
		`{"role":["role:role1","role:roleInexistent"], "user":"bob", "accessUser":"alice", "accessChannel":["aliceDynamic"]}`)
	RequireStatus(t, response, http.StatusCreated)
	var putResp putResponse
	err = json.Unmarshal(response.BodyBytes(), &putResp)
	require.NoError(t, err)

	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein","admin_roles":["role1"],`,
			scopeName, collection1Name, fmt.Sprintf(collectionPayload, collection2Name, `"a", "chan2coll2"`)))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"][keyspace2]), []string{"role1Chan"})
	assert.Equal(t, channelMap.DynamicRoleGrants["role1"][keyspace2]["role1Chan"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 5, EndSeq: 0}})
	assert.NotContains(t, maps.Keys(channelMap.DynamicRoleGrants), "roleInexistent")

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
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"][keyspace2]), []string{"role1Chan"})
	assert.Equal(t, channelMap.DynamicRoleGrants["role1"][keyspace2]["role1Chan"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 5, EndSeq: 7}})

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[keyspace2]), []string{"a", "chan2coll2"})
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[keyspace2]), []string{"!", "aliceDynamic"})

	//Remove channels a and chan2coll2 and assert theyre still in DynamicGrants
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein","admin_roles":["inexistentRole2"],`,
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
	// assert inexistent role is not in the map
	assert.NotContains(t, maps.Keys(channelMap.AdminRoleGrants), "inexistentRole2")

	//Reassign channel A and assert on sequence ranges
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein","admin_roles":["role1"],`,
			scopeName, collection1Name, fmt.Sprintf(collectionPayload, collection2Name, `"a"`)))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[keyspace2]), []string{"a", "chan2coll2"})
	assert.ElementsMatch(t, channelMap.AdminGrants[keyspace2]["a"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 1, EndSeq: 8}, {StartSeq: 9, EndSeq: 0}})

	//Remove role1
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein","admin_roles":[],`,
			scopeName, collection1Name, fmt.Sprintf(collectionPayload, collection2Name, `"a"`)))
	RequireStatus(t, response, http.StatusOK)

	// Re-add role1 and assert on entries
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein","admin_roles":["role1"],`,
			scopeName, collection1Name, fmt.Sprintf(collectionPayload, collection2Name, `"a"`)))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.AdminRoleGrants["role1"][keyspace2]["role1Chan"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 6, EndSeq: 8}, {StartSeq: 9, EndSeq: 10}, {StartSeq: 11, EndSeq: 0}})
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
	newKeyspace := newCollection.String()
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
	ks := dbName + "." + newCollection.String()
	response = rt.SendAdminRequest(http.MethodPut,
		fmt.Sprintf("/%s/%s", ks, "doc1"),
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
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[newKeyspace]), []string{"!", "dynChannel"})

	// remove dynchannel from collection_access channels and assert it shows up in dynamic grants
	response = rt.SendAdminRequest(http.MethodDelete,
		fmt.Sprintf("/%s/%s?rev=%s", ks, "doc1", revID), "")
	RequireStatus(t, response, http.StatusOK)

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
		}`, "_default", newCollection.Collection, "_default"))
	RequireStatus(t, response, http.StatusCreated)

	// Overwrite admin_channels and remove foo and bar channels
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein", "admin_roles":["role1"],`,
			`"channel"`, "_default", newCollection.Collection))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)

	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicGrants[newKeyspace]), []string{"!", "dynChannel"})
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants["_default._default"]), []string{"foo", "bar", "channel"})
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminRoleGrants["role1"]["_default._default"]), []string{})
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminRoleGrants["role1"][newKeyspace]), []string{"role1Chan"})
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"][newKeyspace]), []string{})

	// Remove role and assert on role channels and sequences
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein", "admin_roles":[],`,
			`"channel"`, "_default", newCollection.Collection))
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminRoleGrants["role1"]["_default._default"]), []string{})
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminRoleGrants["role1"][newKeyspace]), []string{"role1Chan"})
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"][newKeyspace]), []string{})
	assert.ElementsMatch(t, channelMap.AdminRoleGrants["role1"][newKeyspace]["role1Chan"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 5, EndSeq: 6}})
}

func TestGetAllChannelsByUser2CollectionsTo1(t *testing.T) {
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
	const alice = "alice"
	userPayload := `{
		%s
		"collection_access": {
			"%s": {
				"%s": {
					"admin_channels":["coll1chan"]
				},
				"%s": {
					"admin_channels":["coll2chan"]
				}
			}
		}
	}`
	// Put user alice and assert admin assigned channels are returned by the get all_channels endpoint
	response := rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`,
			scopeName, collection1Name, collection2Name))
	RequireStatus(t, response, http.StatusCreated)

	resp := rt.UpsertDbConfig(dbName, DbConfig{Scopes: ScopesConfig{
		scopeName: {Collections: CollectionsConfig{
			collection1Name: {SyncFn: &SyncFn},
		}},
	}})
	RequireStatus(t, resp, http.StatusCreated)

	// ensure endpoint does not error and deleted collection is not there
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	var channelMap getAllChannelsResponse
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminGrants[keyspace1]), []string{"coll1chan"})
	assert.NotContains(t, maps.Keys(channelMap.AdminGrants), keyspace2)

}

func TestGetAllChannelsByUserDeletedRole(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true}, 2)
	defer rt.Close()

	dbName := "db"
	tb := base.GetTestBucket(t)
	defer tb.Close(rt.Context())
	scopesConfig := GetCollectionsConfig(t, tb, 1)

	dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)
	rt.CreateDatabase("db", dbConfig)

	scopeName := rt.GetDbCollections()[0].ScopeName
	collection1Name := rt.GetDbCollections()[0].Name
	scopesConfig[scopeName].Collections[collection1Name] = &CollectionConfig{}
	keyspace1 := scopeName + "." + collection1Name

	const alice = "alice"
	//userPayload := `{
	//	%s
	//	"admin_roles":["role1"],
	//}`

	response := rt.SendAdminRequest("PUT", "/db/_role/role1", fmt.Sprintf(`
		{
			"collection_access": {
				"%s": {
					"%s": {
						"admin_channels":["A", "B", "C"]
					}
				}
			}
		}`, scopeName, collection1Name))
	RequireStatus(t, response, http.StatusCreated)

	// Put user alice and assert admin assigned channels are returned by the get all_channels endpoint
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, `{"admin_roles":["role1"],"email":"bob@couchbase.com","password":"letmein"}`)
	RequireStatus(t, response, http.StatusCreated)

	// Delete role and assert channel is not there anymore
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_role/role1", ``)
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendDiagnosticRequest(http.MethodGet, "/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	var channelMap getAllChannelsResponse
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminRoleGrants["role1"][keyspace1]), []string{})
}

func TestGetAllChannelsByUserRoleHistory(t *testing.T) {
	SyncFn := `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.user, doc.role);}`
	base.TestRequiresCollections(t)

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true}, 2)
	defer rt.Close()

	dbName := "db"
	tb := base.GetTestBucket(t)
	defer tb.Close(rt.Context())
	scopesConfig := GetCollectionsConfig(t, tb, 1)
	for scope, _ := range scopesConfig {
		for _, cnf := range scopesConfig[scope].Collections {
			cnf.SyncFn = &SyncFn
		}
	}
	dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)
	rt.CreateDatabase("db", dbConfig)

	scopeName := rt.GetDbCollections()[0].ScopeName
	collection1Name := rt.GetDbCollections()[0].Name
	scopesConfig[scopeName].Collections[collection1Name] = &CollectionConfig{}
	keyspace1 := scopeName + "." + collection1Name

	const alice = "alice"

	// Assign channel to role and remove it
	response := rt.SendAdminRequest("PUT", "/db/_role/role1", fmt.Sprintf(`
		{"collection_access": {"%s": {"%s": {"admin_channels":["role1Chan"]}}}}`, scopeName, collection1Name))
	RequireStatus(t, response, http.StatusCreated)
	response = rt.SendAdminRequest("PUT", "/db/_role/role1", fmt.Sprintf(`
		{"collection_access": {"%s": {"%s": {"admin_channels":[]}}}}`, scopeName, collection1Name))
	RequireStatus(t, response, http.StatusOK)

	// Put user alice with role1
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice, `{"admin_roles":["role1"],"email":"bob@couchbase.com","password":"letmein"}`)
	RequireStatus(t, response, http.StatusCreated)

	// Assert channels in role history from before the role was granted are not returned by the get all_channels endpoint
	response = rt.SendDiagnosticRequest(http.MethodGet, "/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	var channelMap getAllChannelsResponse
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, maps.Keys(channelMap.AdminRoleGrants["role1"][keyspace1]), []string{})
	assert.ElementsMatch(t, maps.Keys(channelMap.DynamicRoleGrants["role1"][keyspace1]), []string{})

	// Assign a channel to role1 and assert get user channels response has the right sequence span
	response = rt.SendAdminRequest("PUT", "/db/_role/role1", fmt.Sprintf(`
		{"collection_access": {"%s": {"%s": {"admin_channels":["newChannel"]}}}}`, scopeName, collection1Name))
	RequireStatus(t, response, http.StatusOK)

	// Assert on sequence span for newChannel
	response = rt.SendDiagnosticRequest(http.MethodGet, "/"+dbName+"/_user/"+alice+"/_all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	channelMap = getAllChannelsResponse{}
	t.Log(response.BodyString())
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.AdminRoleGrants["role1"][keyspace1]["newChannel"].Entries, []auth.GrantHistorySequencePair{{StartSeq: 4, EndSeq: 0}})
}

type grant interface {
	request(rt *RestTester)
}

func compareAllChannelsOutput(rt *RestTester, username string, expectedOutput string) {
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.db}}/_user/"+username+"/_all_channels", ``)
	RequireStatus(rt.TB, response, http.StatusOK)
	rt.TB.Logf("All channels response: %s", response.BodyString())
	require.JSONEq(rt.TB, expectedOutput, convertUpdatedTimeToConstant(rt.TB, response.BodyBytes()))

}

type userGrant struct {
	user          string
	adminChannels map[string][]string
	roles         []string
	output        string
}

func (g *userGrant) getUserPayload(t testing.TB) string {
	config := auth.PrincipalConfig{
		Name:     base.StringPtr(g.user),
		Password: base.StringPtr(RestTesterDefaultUserPassword),
	}
	if len(g.roles) > 0 {
		config.ExplicitRoleNames = base.SetOf(g.roles...)
	}

	for keyspace, chans := range g.adminChannels {
		scopeName, collectionName := strings.Split(keyspace, ".")[0], strings.Split(keyspace, ".")[1]
		if base.IsDefaultCollection(scopeName, collectionName) {
			config.ExplicitChannels = base.SetFromArray(chans)
		} else {
			config.SetExplicitChannels(scopeName, collectionName, chans...)
		}
	}

	return string(base.MustJSONMarshal(t, config))
}

func (g userGrant) request(rt *RestTester) {
	payload := g.getUserPayload(rt.TB)
	rt.TB.Logf("Issuing admin grant: %+v", payload)
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+g.user, payload)
	if response.Code != http.StatusCreated && response.Code != http.StatusOK {
		rt.TB.Fatalf("Expected 200 or 201 exit code")
	}
	compareAllChannelsOutput(rt, g.user, g.output)
}

type roleGrant struct {
	role          string
	adminChannels map[string][]string
}

func (g roleGrant) getPayload(t testing.TB) string {
	config := auth.PrincipalConfig{
		Name:     base.StringPtr(g.role),
		Password: base.StringPtr(RestTesterDefaultUserPassword),
	}
	for keyspace, chans := range g.adminChannels {
		scopeName, collectionName := strings.Split(keyspace, ".")[0], strings.Split(keyspace, ".")[1]
		if base.IsDefaultCollection(scopeName, collectionName) {
			config.ExplicitChannels = base.SetFromArray(chans)
		} else {
			config.SetExplicitChannels(scopeName, collectionName, chans...)
		}
	}
	return string(base.MustJSONMarshal(t, config))
}

func (g roleGrant) request(rt *RestTester) {
	payload := g.getPayload(rt.TB)
	rt.TB.Logf("Issuing admin grant: %+v", payload)
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+g.role, payload)
	if response.Code != http.StatusCreated && response.Code != http.StatusOK {
		rt.TB.Fatalf("Expected 200 or 201 exit code")
	}
}

type dynamicGrantPutDoc struct {
	uri  string
	body string
}

func (g dynamicGrantPutDoc) request(rt *RestTester) {
	RequireStatus(rt.TB, rt.SendAdminRequest(http.MethodPut, g.uri, g.body), http.StatusCreated)
}

type dynamicGrantDeleteDoc struct {
	uri string
}

func (g dynamicGrantDeleteDoc) request(rt *RestTester) {
	// get latest version of doc so we can delete it
	resp := rt.SendAdminRequest(http.MethodGet, g.uri, "")
	RequireStatus(rt.TB, resp, 200)
	var r struct {
		RevID *string `json:"_rev"`
	}
	require.NoError(rt.TB, base.JSONUnmarshal(resp.BodyBytes(), &r))
	resp = rt.SendAdminRequest(http.MethodDelete, g.uri+"?rev="+*r.RevID, "")
	RequireStatus(rt.TB, resp, http.StatusOK)
}

type checkGrant struct {
	user   string
	output string
}

func (g checkGrant) request(rt *RestTester) {
	compareAllChannelsOutput(rt, g.user, g.output)
}

func TestGetAllChannelsExample(t *testing.T) {
	defaultKeyspace := "_default._default"
	tests := []struct {
		name          string
		adminChannels []string
		grants        []grant
	}{
		{
			name: "admin channels once",
			grants: []grant{
				// grant 1
				userGrant{
					user: "alice",
					adminChannels: map[string][]string{
						defaultKeyspace: []string{"A", "B", "C"},
					},
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
 },
 "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
 }}`,
				}}},
		{
			name: "no admin channels, then A",
			grants: []grant{
				// grant 1
				userGrant{
					user: "alice",
					output: `
{"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
 }}`,
				},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: []string{"A"}},
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["2-0"] }
	}
 },
 "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
			},
		},
		{
			name: "A,B,C->nothing->A",
			grants: []grant{
				// grant 1
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: []string{"A", "B", "C"}},
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
},
 "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
				// grant 2
				userGrant{
					user: "alice",
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
},
 "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
				// grant 3
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: []string{"A"}},
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": {
			"channel_source": "Admin",
			"entries" : ["1-3"],
			"updated_at": 1234
		},
		"C": {
			"channel_source": "Admin",
			"entries" : ["1-3"],
			"updated_at": 1234
		}
	}
},
"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
			},
		},
		{
			name: "alice+bob",
			grants: []grant{
				// grant 1
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: []string{"A", "B", "C"}},
					output: `
{ "admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
},
 "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
				// grant 2
				userGrant{
					user: "alice",
					output: `
{
	"admin_grants": {
		"_default._default": {
			"A": { "entries" : ["1-0"] },
			"B": { "entries" : ["1-0"] },
			"C": { "entries" : ["1-0"] }
		}
	},
	"dynamic_grants": {
		"_default._default": {
			"!": { "entries": ["1-0"] }
		}
	}
}`,
				},
				// grant 3
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: []string{"A"}},
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": {
			"channel_source": "Admin",
			"entries" : ["1-3"],
			"updated_at": 1234
		},
		"C": {
			"channel_source": "Admin",
			"entries" : ["1-3"],
			"updated_at": 1234
		}
	}
},
 "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
				// grant 4
				userGrant{
					user: "bob",
					output: `
{"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
			},
		},
		{
			name: "alice+bob role grant",
			grants: []grant{
				// grant 1
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: []string{"A", "B", "C"}},
					output: `
{ "admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
},
"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
				// grant 2
				userGrant{
					user:          "bob",
					adminChannels: map[string][]string{defaultKeyspace: []string{"D", "E", "F"}},
					output: `
{"admin_grants": {
	"_default._default": {
		"D": { "entries" : ["2-0"] },
		"E": { "entries" : ["2-0"] },
		"F": { "entries" : ["2-0"] }
	}
},
"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] }
	}
}}`,
				},
				// grant 3
				dynamicGrantPutDoc{
					uri: "/{{.db}}/doc1",

					body: `{"accessChannel":"NewChannel", "accessUser":["bob","alice"]}`,
				},
				// grant 4
				checkGrant{
					user: "bob",
					output: `
{"admin_grants": {
	"_default._default": {
		"D": { "entries" : ["2-0"] },
		"E": { "entries" : ["2-0"] },
		"F": { "entries" : ["2-0"] }
	}
}, "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 5
				checkGrant{
					user: "alice",
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
}, "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 6
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: []string{"chan"}},
				},
				// grant 7
				checkGrant{
					user: "bob",
					output: `
{"admin_grants": {
	"_default._default": {
		"D": { "entries" : ["2-0"] },
		"E": { "entries" : ["2-0"] },
		"F": { "entries" : ["2-0"] }
	}
},
"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 8
				checkGrant{
					user: "alice",
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
}, "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 9
				dynamicGrantPutDoc{
					uri:  "/{{.keyspace}}/doc2",
					body: `{"role":["role:role1", "role:roleInexistent"], "user":"bob"}`,
				},
				// grant 10
				checkGrant{
					user: "bob",
					output: `
{"admin_grants": {
	"_default._default": {
		"D": { "entries" : ["2-0"] },
		"E": { "entries" : ["2-0"] },
		"F": { "entries" : ["2-0"] }
	}
},
"dynamic_role_grants": {
	"role1": {
		"_default._default": {
			"chan": {
				"channel_source": "Admin",
				"entries": ["5-0"]
			}
		}
	}
},
"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 11
				checkGrant{
					user: "alice",
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
}, "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 12
				// this changes nothing
				dynamicGrantDeleteDoc{
					uri: "/{{.keyspace}}/doc2",
				},
				// grant 13
				checkGrant{
					user: "bob",
					output: `
{"admin_grants": {
	"_default._default": {
		"D": { "entries" : ["2-0"] },
		"E": { "entries" : ["2-0"] },
		"F": { "entries" : ["2-0"] }
	}
},
"dynamic_role_grants": {
	"role1": {
		"_default._default": {
			"chan": {
				"channel_source": "Admin",
				"updated_at": 1234,
				"entries": ["5-6"]
			}
		}
	}
},
"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 14
				checkGrant{
					user: "alice",
					output: `
{"admin_grants": {
	"_default._default": {
		"A": { "entries" : ["1-0"] },
		"B": { "entries" : ["1-0"] },
		"C": { "entries" : ["1-0"] }
	}
}, "dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
				// grant 15
				dynamicGrantPutDoc{
					uri:  "/{{.keyspace}}/doc2",
					body: `{"role":["role:role1", "role:roleInexistent"], "user":"bob"}`,
				},
				// grant 16
				userGrant{
					user:  "bob",
					roles: []string{"role1"},
					output: `
{"admin_grants": {
	"_default._default": {
		"D": { "entries" : ["2-0"] },
		"E": { "entries" : ["2-0"] },
		"F": { "entries" : ["2-0"] }
	}
},
"dynamic_role_grants": {
	"role1": {
		"_default._default": {
			"chan": {
				"channel_source": "Admin",
				"updated_at": 1234,
				"entries": ["7-0", "5-6"]
			}
		}
	}
},
"dynamic_grants": {
	"_default._default": {
		"!": { "entries": ["1-0"] },
		"NewChannel": { "entries": ["3-0"] }
	}
}}`,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				PersistentConfig: true,
			})
			defer rt.Close()

			dbConfig := rt.NewDbConfig()
			dbConfig.Scopes = nil
			dbConfig.Sync = base.StringPtr(`function(doc) { channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.user, doc.role);}`)
			rt.CreateDatabase("db", dbConfig)

			// create user with adminChannels1
			for i, grant := range test.grants {
				t.Logf("Processing grant %d", i+1)
				grant.request(rt)
			}

		})
	}

}
