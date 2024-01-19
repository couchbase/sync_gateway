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
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetAllChannelsByUser(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("Test requires Couchbase Server")
	}
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		SyncFn:           `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);}`,
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
	err := json.Unmarshal(response.ResponseRecorder.Body.Bytes(), &channelMap)
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

	err = json.Unmarshal(response.ResponseRecorder.Body.Bytes(), &channelMap)
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
	err = json.Unmarshal(response.ResponseRecorder.Body.Bytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.Channels.ToArray(), []string{"!", "NewChannel"})

	response = rt.SendAdminRequest(http.MethodGet,
		"/"+dbName+"/_user/"+alice+"/all_channels", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.ResponseRecorder.Body.Bytes(), &channelMap)
	require.NoError(t, err)
	assert.ElementsMatch(t, channelMap.Channels.ToArray(), []string{"A", "B", "C", "!", "NewChannel"})
}
