/*
Copyright 2024-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"

	"github.com/couchbase/sync_gateway/db"

	"github.com/stretchr/testify/assert"
)

func TestGetAlldocChannels(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	version := rt.PutDoc("doc", `{"channel":["CHAN1"]}`)
	updatedVersion := rt.UpdateDoc("doc", version, `{"channel":["CHAN2"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1", "CHAN2"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN3"]}`)
	updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN1"]}`)

	response := rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/doc/_all_channels", "")
	RequireStatus(t, response, http.StatusOK)

	var channelMap map[string][]string
	err := json.Unmarshal(response.BodyBytes(), &channelMap)
	assert.NoError(t, err)
	assert.ElementsMatch(t, channelMap["CHAN1"], []string{"6-0", "1-2", "3-5"})
	assert.ElementsMatch(t, channelMap["CHAN2"], []string{"4-5", "2-3"})
	assert.ElementsMatch(t, channelMap["CHAN3"], []string{"5-6"})

	for i := 1; i <= 10; i++ {
		updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{}`)
		updatedVersion = rt.UpdateDoc("doc", updatedVersion, `{"channel":["CHAN3"]}`)
	}
	response = rt.SendAdminRequest("GET", "/{{.keyspace}}/doc", "")
	RequireStatus(t, response, http.StatusOK)
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/doc/_all_channels", "")
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &channelMap)
	assert.NoError(t, err)

	// If the channel is still in channel_set, then the total will be 5 entries in history and 1 in channel_set
	assert.Equal(t, len(channelMap["CHAN3"]), db.DocumentHistoryMaxEntriesPerChannel+1)

}

func TestGetDocDryRuns(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)
	rt := NewRestTester(t, &RestTesterConfig{PersistentConfig: true})
	defer rt.Close()
	bucket := rt.Bucket().GetName()
	ImportFilter := `"function(doc) { if (doc.user.num) { return true; } else { return false; } }"`
	SyncFn := `"function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}"`
	resp := rt.SendAdminRequest("PUT", "/db/", fmt.Sprintf(
		`{"bucket":"%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "sync":%s, "import_filter":%s}`,
		bucket, base.TestUseXattrs(), SyncFn, ImportFilter))
	RequireStatus(t, resp, http.StatusCreated)
	response := rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync", `{"accessChannel": ["dynamicChan5412"],"accessUser": "user","channel": ["dynamicChan222"],"expiry":10}`)
	RequireStatus(t, response, http.StatusOK)
	var respMap SyncFnDryRun
	var respMapinit SyncFnDryRun
	err := json.Unmarshal(response.BodyBytes(), &respMapinit)
	assert.NoError(t, err)
	assert.ElementsMatch(t, respMapinit.Exception, nil)
	assert.Equal(t, respMapinit.Roles, channels.AccessMap{})
	assert.Equal(t, respMapinit.Access, channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")})
	assert.EqualValues(t, *respMapinit.Expiry, 10)
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync", `{"role": ["role:role1"], "accessUser": "user"}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.Equal(t, respMap.Roles, channels.AccessMap{"user": channels.BaseSetOf(t, "role1")})
	newSyncFn := `"function(doc,oldDoc){if (doc.user.num >= 100) {channel(doc.channel);} else {throw({forbidden: 'user num too low'});}if (oldDoc){ console.log(oldDoc); if (oldDoc.user.num > doc.user.num) { access(oldDoc.user.name, doc.channel);} else {access(doc.user.name[0], doc.channel);}}}"`
	resp = rt.SendAdminRequest("POST", "/db/_config", fmt.Sprintf(
		`{"bucket":"%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "sync":%s, "import_filter":%s}`,
		bucket, base.TestUseXattrs(), newSyncFn, ImportFilter))
	RequireStatus(t, resp, http.StatusCreated)

	_ = rt.PutDoc("doc", `{"user":{"num":123, "name":["user1"]}, "channel":"channel1"}`)

	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync", `{"user":{"num":23}}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.Equal(t, respMap.Exception, "403 user num too low")

	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync?doc_id=doc", `{"user":{"num":150}, "channel":"abc"}`)
	RequireStatus(t, response, http.StatusOK)
	var newrespMap SyncFnDryRun
	err = json.Unmarshal(response.BodyBytes(), &newrespMap)
	assert.NoError(t, err)
	assert.Equal(t, newrespMap.Exception, "TypeError: Cannot access member '0' of undefined")

	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync?doc_id=doc", `{"user":{"num":120, "name":["user2"]}, "channel":"channel2"}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.Equal(t, respMap.Access, channels.AccessMap{"user1": channels.BaseSetOf(t, "channel2")})

	// get doc from bucket with no body provided
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync?doc_id=doc", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.Equal(t, respMap.Access, channels.AccessMap{"user1": channels.BaseSetOf(t, "channel1")})

	// Get doc that doesnt exist, will error
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync?doc_id=doc404", ``)
	RequireStatus(t, response, http.StatusNotFound)
	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.Equal(t, respMap.Exception, "")

	// no doc id no body, will error
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync?doc_id=", ``)
	RequireStatus(t, response, http.StatusInternalServerError)

	// Import filter import=false and type error
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_import_filter", `{"accessUser": "user"}`)
	RequireStatus(t, response, http.StatusOK)

	var respMap2 ImportFilterDryRun
	err = json.Unmarshal(response.BodyBytes(), &respMap2)
	assert.NoError(t, err)
	assert.Equal(t, respMap2.Error, "TypeError: Cannot access member 'num' of undefined")
	assert.False(t, respMap2.ShouldImport)

	// Import filter import=true and no error
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_import_filter", `{"user":{"num":23}}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap2)
	assert.NoError(t, err)
	assert.Equal(t, respMap2.Error, "")
	assert.True(t, respMap2.ShouldImport)

	// Import filter import=true and no error
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_import_filter", `{"user":23}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap2)
	assert.NoError(t, err)
	assert.Equal(t, respMap2.Error, "")
	assert.False(t, respMap2.ShouldImport)

	_ = rt.PutDoc("doc2", `{"user":{"num":125}}`)
	// Import filter get doc from bucket with no body
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_import_filter?doc_id=doc2", ``)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap2)
	assert.NoError(t, err)
	assert.Equal(t, respMap2.Error, "")
	assert.True(t, respMap2.ShouldImport)

	// Import filter get doc from bucket error doc not found
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_import_filter?doc_id=doc404", ``)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &respMap2)
	assert.NoError(t, err)
	if base.UnitTestUrlIsWalrus() {
		assert.Equal(t, respMap2.Error, `key "doc404" missing`)
	} else {
		assert.Contains(t, respMap2.Error, "<ud>doc404</ud>: Not Found")
	}
	assert.False(t, respMap2.ShouldImport)

	// Import filter get doc from bucket error body provided
	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_import_filter?doc_id=doc2", `{"user":{"num":23}}`)
	RequireStatus(t, response, http.StatusBadRequest)

	newSyncFn = `"function(doc,oldDoc){if(oldDoc){ channel(oldDoc.channel)} else {channel(doc.channel)} }"`
	resp = rt.SendAdminRequest("POST", "/db/_config", fmt.Sprintf(
		`{"bucket":"%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "sync":%s, "import_filter":%s}`,
		bucket, base.TestUseXattrs(), newSyncFn, ImportFilter))
	RequireStatus(t, resp, http.StatusCreated)
	_ = rt.PutDoc("doc22", `{"chan1":"channel1", "channel":"chanOld"}`)

	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync", `{"channel":"channel2"}`)
	RequireStatus(t, response, http.StatusOK)

	err = json.Unmarshal(response.BodyBytes(), &respMap)
	assert.NoError(t, err)
	assert.EqualValues(t, respMap.Channels.ToArray(), []string{"channel2"})

	response = rt.SendDiagnosticRequest("GET", "/{{.keyspace}}/_sync?doc_id=doc22", `{"channel":"chanNew"}`)
	RequireStatus(t, response, http.StatusOK)
	err = json.Unmarshal(response.BodyBytes(), &newrespMap)
	assert.NoError(t, err)
	assert.EqualValues(t, newrespMap.Channels.ToArray(), []string{"chanOld"})
}

func TestGetAllChannelsUserDocIntersection(t *testing.T) {
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
						"{{.keyspace}}": {"A", "B", "C"},
					},
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
							"A": { "entries" : ["1-0"]},
							"B": { "entries" : ["1-0"]},
							"C": { "entries" : ["1-0"]}
						}}}`,
				},
				docGrant{
					userName:       "alice",
					dynamicChannel: "A",
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["1-0"]}
					}}}`,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				PersistentConfig: true,
				SyncFn:           `function(doc) {channel(doc.channel); access(doc.user, doc.channel); role(doc.user, doc.role);}`,
			})
			defer rt.Close()

			RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

			// iterate and execute grants in each test case
			for i, grant := range test.grants {
				t.Logf("Processing grant %d", i+1)
				grant.request(rt)
			}

		})
	}
}

func TestGetUserDocAccessSpan(t *testing.T) {
	tests := []struct {
		name          string
		adminChannels []string
		grants        []grant
	}{
		{
			name: "admin channels once",
			grants: []grant{
				docGrant{dynamicChannel: "A"},
				userGrant{
					user: "alice",
					adminChannels: map[string][]string{
						"{{.keyspace}}": {"A", "B", "C"},
					},
					docIDs:      []string{"doc"},
					docUserTest: true,
					output:      `{"doc": {"A": { "entries" : ["2-0"]}}}`,
				},
			},
		},
		{
			name: "multiple history entries",
			grants: []grant{
				// grant 1
				userGrant{
					user: "alice",
					adminChannels: map[string][]string{
						"{{.keyspace}}": {"A"},
					},
				},
				docGrant{dynamicChannel: "A"},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					docIDs:        []string{"doc"},
					docUserTest:   true,
					output:        `{"doc": {"A": { "entries" : ["2-3", "4-0"]}}}`,
				},
			},
		},
		{
			name: "limit history entries to 10",
			grants: []grant{
				// grant 1
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// create doc
				docGrant{dynamicChannel: "A"},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 3
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 4
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 5
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 6
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 7
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 8
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 9
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 10
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 11
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 12
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 13
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 14
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 15
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 16
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 17
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 18
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 19
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 20
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 19
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// grant 20
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
				},
				// grant 23
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					docIDs:        []string{"doc"},
					docUserTest:   true,
					output:        `{"doc": {"A": { "entries" : ["2-5","6-7","8-9","10-11","12-13","14-15","16-17","18-19","20-21","22-23","24-0"]}}}`,
				},
			},
		},
		{
			name: "admin role grant channels",
			grants: []grant{
				// grant 1
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A", "B"}},
				},
				docGrant{dynamicChannel: "A"},
				userGrant{
					user:        "alice",
					roles:       []string{"role1"},
					docIDs:      []string{"doc"},
					docUserTest: true,
					output:      `{"doc": {"A": { "entries" : ["3-0"]}}}`,
				},
			},
		},
		{
			name: "dynamic grant channels",
			grants: []grant{
				userGrant{
					user: "alice",
				},
				docGrant{
					userName:       "alice",
					dynamicChannel: "A",
					docIDs:         []string{"doc"},
					docUserTest:    true,
					output:         `{"doc": {"A": { "entries" : ["2-0"]}}}`,
				},
			},
		},
		{
			name: "dynamic role grant channels",
			grants: []grant{
				// create user
				userGrant{
					user: "alice",
				},
				// create role with channels
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A", "B"}},
				},
				// assign role through the sync fn and check output
				docGrant{
					userName:       "alice",
					dynamicRole:    "role1",
					dynamicChannel: "A",
					docIDs:         []string{"doc"},
					docUserTest:    true,
					output:         `{"doc": {"A": { "entries" : ["3-0"]}}}`,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				PersistentConfig: true,
				SyncFn:           `function(doc) {channel(doc.channel); access(doc.user, doc.channel); role(doc.user, doc.role);}`,
			})
			defer rt.Close()

			RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

			// iterate and execute grants in each test case
			for i, grant := range test.grants {
				t.Logf("Processing grant %d", i+1)
				grant.request(rt)
			}

		})
	}
}
