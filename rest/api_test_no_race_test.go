//  Copyright (c) 2016 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

// This file contains tests which depend on the race detector being disabled.  Contains changes tests
// that have unpredictable timing when running w/ race detector due to longpoll/continuous changes request
// processing.
//go:build !race
// +build !race

package rest

import (
	"fmt"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
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
