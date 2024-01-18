package rest

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"testing"
)

func TestGetAllChannelsByUser(t *testing.T) {
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
		"/"+dbName+"/doc1",
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
