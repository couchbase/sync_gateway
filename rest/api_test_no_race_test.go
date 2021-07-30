//  Copyright 2016-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

// This file contains tests which depend on the race detector being disabled.  Contains changes tests
// that have unpredictable timing when running w/ race detector due to longpoll/continuous changes request
// processing.
// +build !race

package rest

import (
	"fmt"
	"log"
	"sync"
	"testing"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChangesAccessNotifyInteger(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges, base.KeyHTTP)()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);}`})
	defer rt.Close()

	// Create user:
	a := rt.ServerContext().Database("db").Authenticator()
	bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t, "ABC"))
	assert.NoError(t, err)
	assert.NoError(t, a.Save(bernard))

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	caughtUpWaiter := rt.GetDatabase().NewPullReplicationCaughtUpWaiter(t)
	// Start longpoll changes request
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var changes struct {
			Results  []db.ChangeEntry
			Last_Seq db.SequenceID
		}
		changesJSON := `{"style":"all_docs", "heartbeat":300000, "feed":"longpoll", "limit":50, "since":"0"}`
		changesResponse := rt.Send(requestByUser("POST", "/db/_changes", changesJSON, "bernard"))
		err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
		goassert.Equals(t, len(changes.Results), 3)
	}()

	// Wait for changes to get into wait mode
	caughtUpWaiter.AddAndWait(1)

	// Put document that triggers access grant for user, PBS
	response = rt.SendAdminRequest("PUT", "/db/access1", `{"accessUser":"bernard", "accessChannel":["PBS"]}`)
	assertStatus(t, response, 201)

	wg.Wait()
}

// Test for SG issue #1999.  Verify that the notify handling works as expected when the user specifies a channel filter that includes channels
// the user doesn't have access to, where those channels have been updated more recently than the user and/or the valid channels.  Non-granted
// channels in the filter were being included in the waiter initialization, but not in the subsequent wait.  Resulting difference in count was resulting
// in longpoll terminating without any changes.
func TestChangesNotifyChannelFilter(t *testing.T) {

	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyChanges, base.KeyHTTP)()

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	// Create user:
	userResponse := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["ABC"]}`)
	assertStatus(t, userResponse, 201)

	// Get user, to trigger all_channels calculation and bump the user change count BEFORE we write the PBS docs - otherwise the user key count
	// will still be higher than the latest change count.
	userResponse = rt.SendAdminRequest("GET", "/db/_user/bernard", "")
	assertStatus(t, userResponse, 200)

	/*
		a := it.ServerContext().Database("db").Authenticator()
		bernard, err := a.NewUser("bernard", "letmein", channels.SetOf(t,"ABC"))
		goassert.True(t, err == nil)
		a.Save(bernard)
	*/

	// Put several documents in channel PBS
	response := rt.SendAdminRequest("PUT", "/db/pbs1", `{"value":1, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs2", `{"value":2, "channel":["PBS"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("PUT", "/db/pbs3", `{"value":3, "channel":["PBS"]}`)
	assertStatus(t, response, 201)

	// Run an initial changes request to get the user doc, and update since based on last_seq:
	var initialChanges struct {
		Results  []db.ChangeEntry
		Last_Seq db.SequenceID
	}
	changesJSON := `{"style":"all_docs", 
					 "heartbeat":300000, 
					 "feed":"longpoll", 
					 "limit":50, 
					 "since":"%s",
					 "filter":"` + base.ByChannelFilter + `",
					 "channels":"ABC,PBS"}`
	sinceZeroJSON := fmt.Sprintf(changesJSON, "0")
	changesResponse := rt.Send(requestByUser("POST", "/db/_changes", sinceZeroJSON, "bernard"))
	err := base.JSONUnmarshal(changesResponse.Body.Bytes(), &initialChanges)
	assert.NoError(t, err, "Unexpected error unmarshalling initialChanges")
	lastSeq := initialChanges.Last_Seq.String()
	goassert.Equals(t, lastSeq, "1")

	caughtUpWaiter := rt.GetDatabase().NewPullReplicationCaughtUpWaiter(t)
	caughtUpWaiter.Add(1)
	// Start longpoll changes request, requesting (unavailable) channel PBS.  Should block.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var changes struct {
			Results  []db.ChangeEntry
			Last_Seq db.SequenceID
		}
		sinceLastJSON := fmt.Sprintf(changesJSON, lastSeq)
		changesResponse := rt.Send(requestByUser("POST", "/db/_changes", sinceLastJSON, "bernard"))
		err = base.JSONUnmarshal(changesResponse.Body.Bytes(), &changes)
		goassert.Equals(t, len(changes.Results), 1)
	}()

	// Wait to see if the longpoll will terminate on wait before a document shows up on the channel
	caughtUpWaiter.Wait()

	// Put public document that triggers termination of the longpoll
	response = rt.SendAdminRequest("PUT", "/db/abc1", `{"value":3, "channel":["ABC"]}`)
	assertStatus(t, response, 201)
	wg.Wait()
}

func TestSetupAndValidate(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skipping this test; it only works on Walrus bucket")
	}
	defer base.SetUpTestLogging(base.LevelDebug, base.KeyAll)()
	t.Run("Run setupAndValidate with valid config", func(t *testing.T) {
		configFile := createTempFile(t, []byte(`{
          "databases": {
            "db": {
              "bucket": "data_bucket",
              "enable_shared_bucket_access": true,
              "import_docs": true,
              "server": "couchbase://localhost",
              "username": "Administrator",
              "password": "password",
              "use_views": false,
              "revs_limit": 200,
              "num_index_replicas": 1,
              "users": {
                "GUEST": { "admin_channels": ["*"] }
              }
            }
          },
          "logging": {
            "console": {
              "enabled": true,
              "log_level": "debug",
              "log_keys": [
                "*"
              ],
              "color_enabled": true
            }
          }
        }`))
		defer deleteTempFile(t, configFile)
		args := []string{"sync_gateway", configFile.Name()}
		config, err := setupServerConfig(args)
		require.NoError(t, err, "Error reading config file")
		require.NotNil(t, config)

		db := config.Databases["db"]
		require.NotNil(t, db)

		assert.Equal(t, "db", db.Name)
		assert.NotNil(t, db.Bucket)

		assert.Equal(t, "data_bucket", *db.Bucket)
		assert.NotNil(t, db.Server)

		assert.NotNil(t, db.EnableXattrs)
		assert.True(t, *db.EnableXattrs)

		assert.Equal(t, "couchbase://localhost", *db.Server)
		assert.Equal(t, "Administrator", db.Username)

		assert.Equal(t, "password", db.Password)
		require.NotNil(t, db.UseViews)
		assert.False(t, *db.UseViews)

		assert.NotNil(t, db.RevsLimit)
		assert.Equal(t, 200, int(*db.RevsLimit))

		assert.NotNil(t, db.NumIndexReplicas)
		assert.Equal(t, 1, int(*db.NumIndexReplicas))

		require.NotNil(t, config.Logging)
		require.NotNil(t, config.Logging.Console)

		require.NotNil(t, config.Logging.Console.ColorEnabled)
		assert.True(t, *config.Logging.Console.ColorEnabled)

		require.NotNil(t, config.Logging.Console.Enabled)
		assert.True(t, *config.Logging.Console.Enabled)

		require.NotNil(t, config.Logging.Console.LogLevel)
		assert.Equal(t, "debug", config.Logging.Console.LogLevel.String())
		assert.Equal(t, []string{"*"}, config.Logging.Console.LogKeys)
	})

	t.Run("Run setupAndValidate with unknown field in config file", func(t *testing.T) {
		configFile := createTempFile(t, []byte(`{"unknownKey":"unknownValue"}`))
		defer deleteTempFile(t, configFile)
		args := []string{"sync_gateway", configFile.Name()}
		config, err := setupServerConfig(args)
		require.Error(t, err, "Should throw error reading file")
		assert.Contains(t, err.Error(), "unrecognized JSON field")
		assert.Nil(t, config)
	})

	t.Run("Run setupAndValidate with a config file that doesn't exist", func(t *testing.T) {
		configFile := createTempFile(t, []byte(``))
		args := []string{"sync_gateway", configFile.Name()}
		deleteTempFile(t, configFile)
		config, err := setupServerConfig(args)
		require.Error(t, err, "Should throw error reading file")
		assert.Contains(t, err.Error(), "Error reading config file")
		assert.Nil(t, config)
	})

	t.Run("Run setupAndValidate with illegal value for stats_log_freq_secs", func(t *testing.T) {
		configFile := createTempFile(t, []byte(`
		{
		  "databases": {
		    "db": {
		      "bucket": "leaky_bucket",
		      "server": "couchbase://localhost",
		      "username": "Administrator",
		      "password": "password"
		    }
		  },
		  "unsupported": {
		    "stats_log_freq_secs": 1
		  }
		}`))
		defer deleteTempFile(t, configFile)
		args := []string{"sync_gateway", configFile.Name()}
		config, err := setupServerConfig(args)
		require.Error(t, err, "Should throw error reading file")
		assert.Contains(t, err.Error(), "minimum value for unsupported.stats_log_freq_secs is: 10")
		assert.Nil(t, config)
	})

}

// Make sure that a client cannot open multiple subChanges subscriptions on a single blip context (SG #3222)
//
// - Open two continuous subChanges feeds, and asserts that it gets an error on the 2nd one.
// - Open a one-off subChanges request, assert no error
func TestMultipleOustandingChangesSubscriptions(t *testing.T) {

	// FIXME: CBG-1547: Skipping with race due to data race caused by something in this test

	defer base.SetUpTestLogging(base.LevelInfo, base.KeyHTTP, base.KeySync, base.KeySyncMsg)()

	bt, err := NewBlipTester(t)
	require.NoError(t, err, "Error creating BlipTester")
	defer bt.Close()

	bt.blipContext.HandlerForProfile["changes"] = func(request *blip.Message) {
		if !request.NoReply() {
			// Send an empty response to avoid the Sync: Invalid response to 'changes' message
			response := request.Response()
			emptyResponseVal := []interface{}{}
			emptyResponseValBytes, err := base.JSONMarshal(emptyResponseVal)
			assert.NoError(t, err, "Error marshalling response")
			response.SetBody(emptyResponseValBytes)
		}
	}

	// Send continous subChanges to subscribe to changes, which will cause the "changes" profile handler above to be called back
	subChangesRequest := blip.NewRequest()
	subChangesRequest.SetProfile("subChanges")
	subChangesRequest.Properties["continuous"] = "true"
	subChangesRequest.SetCompressed(false)
	sent := bt.sender.Send(subChangesRequest)
	goassert.True(t, sent)
	subChangesResponse := subChangesRequest.Response()
	goassert.Equals(t, subChangesResponse.SerialNumber(), subChangesRequest.SerialNumber())
	errorCode := subChangesResponse.Properties["Error-Code"]
	log.Printf("errorCode: %v", errorCode)
	goassert.True(t, errorCode == "")

	// Send a second continuous subchanges request, expect an error
	subChangesRequest2 := blip.NewRequest()
	subChangesRequest2.SetProfile("subChanges")
	subChangesRequest2.Properties["continuous"] = "true"
	subChangesRequest2.SetCompressed(false)
	sent2 := bt.sender.Send(subChangesRequest2)
	goassert.True(t, sent2)
	subChangesResponse2 := subChangesRequest2.Response()
	goassert.Equals(t, subChangesResponse2.SerialNumber(), subChangesRequest2.SerialNumber())
	errorCode2 := subChangesResponse2.Properties["Error-Code"]
	log.Printf("errorCode2: %v", errorCode2)
	goassert.True(t, errorCode2 == "500")

	// Send a thirst subChanges request, but this time continuous = false.  Should not return an error
	subChangesRequest3 := blip.NewRequest()
	subChangesRequest3.SetProfile("subChanges")
	subChangesRequest3.Properties["continuous"] = "false"
	subChangesRequest3.SetCompressed(false)
	sent3 := bt.sender.Send(subChangesRequest3)
	goassert.True(t, sent3)
	subChangesResponse3 := subChangesRequest3.Response()
	goassert.Equals(t, subChangesResponse3.SerialNumber(), subChangesRequest3.SerialNumber())
	errorCode3 := subChangesResponse3.Properties["Error-Code"]
	log.Printf("errorCode: %v", errorCode3)
	goassert.True(t, errorCode == "")

}
