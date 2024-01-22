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

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"

	"github.com/stretchr/testify/require"
)

func TestGetAllChannelsByUser(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		SyncFn:           `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.user, doc.role);}`,
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

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	var channelMap allChannels
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.Channels.ToArray(), []string{"A", "B", "C", "!"})

	// Assert non existent user returns 404
	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/all_channels", ``)
	RequireStatus(t, response, http.StatusNotFound)

	// Put user bob and assert on channels returned by all_channels
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+bob,
		`{"name": "`+bob+`", "password": "`+RestTesterDefaultUserPassword+`", "admin_channels": []}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.Channels.ToArray(), []string{"!"})

	// Assign new channel to user bob and assert all_channels includes it
	response = rt.SendAdminRequest(http.MethodPut,
		"/{{.keyspace}}/doc1",
		`{"accessChannel":"NewChannel", "accessUser":["bob","alice"]}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.Channels.ToArray(), []string{"!", "NewChannel"})

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.Channels.ToArray(), []string{"A", "B", "C", "!", "NewChannel"})

	response = rt.SendAdminRequest("PUT", "/db/_role/role1", `{"admin_channels":["chan"]}`)
	RequireStatus(t, response, http.StatusCreated)

	// Assign new channel to user bob and assert all_channels includes it
	response = rt.SendAdminRequest(http.MethodPut,
		"/{{.keyspace}}/doc2",
		`{"role":"role:role1", "user":"bob"}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.Channels.ToArray(), []string{"!", "NewChannel", "chan"})

}

func TestGetAllChannelsByUserWithCollections(t *testing.T) {
	SyncFn := `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);}`

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
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

	collectionPayload := fmt.Sprintf(`,"%s": {
					"admin_channels":["a"]
				}`, collection2Name)

	// create sessions before users
	const alice = "alice"
	const bob = "bob"
	userPayload := `{
		%s
		"admin_channels":["foo", "bar"],
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
			scopeName, collection1Name, collectionPayload))
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	t.Log(response.Body.String())
	var channelMap map[string]map[string]*auth.CollectionAccessConfig
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap[scopeName][collection1Name].Channels_.ToArray(), []string{"A", "B", "C", "!"})

	// Assert non existent user returns 404
	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/all_channels", ``)
	RequireStatus(t, response, http.StatusNotFound)

	// Put user bob and assert on channels returned by all_channels
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+bob,
		`{"name": "`+bob+`", "password": "`+RestTesterDefaultUserPassword+`", "admin_channels": []}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap[scopeName][collection1Name].Channels_.ToArray(), []string{"!"})

	// Assign new channel to user bob and assert all_channels includes it
	response = rt.SendAdminRequest(http.MethodPut,
		fmt.Sprintf("/%s/%s", rt.GetKeyspaces()[0], "doc1"),
		`{"accessChannel":"NewChannel", "accessUser":["bob","alice"]}`)
	RequireStatus(t, response, http.StatusCreated)
	t.Log(rt.GetKeyspaces()[1])

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+bob+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	t.Log(response.Body.String())

	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap[scopeName][collection1Name].Channels_.ToArray(), []string{"!", "NewChannel"})

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	t.Log(response.Body.String())

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap[scopeName][collection1Name].Channels_.ToArray(), []string{"A", "B", "C", "!", "NewChannel"})
}
