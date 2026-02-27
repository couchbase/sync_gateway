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

	"github.com/stretchr/testify/require"

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
		{
			name: "channel assigned through both dynamic and admin grants, assert earlier sequence (dynamic) is used",
			grants: []grant{
				docGrant{dynamicChannel: "A", docID: "docA"},
				// create user with no channels
				userGrant{
					user: "alice",
				},
				// create another doc and assign dynamic chan through sync fn
				docGrant{
					userName:       "alice",
					dynamicChannel: "A",
				},
				// assign same channels through admin_channels and assert on sequences
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					docIDs:        []string{"doc", "docA"},
					docUserTest:   true,
					output:        `{"doc": {"A": { "entries" : ["3-0"]}}, "docA":{"A": { "entries" : ["3-0"]}}}`,
				},
			},
		},
		{
			name: "channel assigned through both admin and admin role grants, assert earlier sequence (admin) is used",
			grants: []grant{
				docGrant{dynamicChannel: "A"},
				// create user and assign channel through admin_channels
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// create role with same channel
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// assign role through admin_roles and assert on sequences
				userGrant{
					user:        "alice",
					roles:       []string{"role1"},
					docIDs:      []string{"doc"},
					docUserTest: true,
					output:      `{"doc": {"A": { "entries" : ["2-0"]}}}`,
				},
			},
		},
		{
			name: "channel assigned through both admin and admin role grants, assert earlier sequence (admin role) is used",
			grants: []grant{
				docGrant{dynamicChannel: "A"},
				// create user with no channels
				userGrant{
					user: "alice",
				},
				// create role with channel
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// assign role through admin_roles
				userGrant{
					user:  "alice",
					roles: []string{"role1"},
				},
				// assign role channel through admin_channels and assert on sequences
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					docIDs:        []string{"doc"},
					docUserTest:   true,
					output:        `{"doc": {"A": { "entries" : ["4-0"]}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin grants, assert earlier sequence (dynamic role) is used",
			grants: []grant{
				// create user with no channels
				userGrant{
					user: "alice",
				},
				// create role with channel
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// create doc and assign role through sync fn
				docGrant{
					userName:       "alice",
					dynamicRole:    "role1",
					dynamicChannel: "A",
				},
				// assign role cahnnel to user through admin_channels and assert on sequences
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					docIDs:        []string{"doc"},
					docUserTest:   true,
					output:        `{"doc": {"A": { "entries" : ["3-0"]}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin grants, assert earlier sequence (admin) is used",
			grants: []grant{
				// create user with channel
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// create role with same channel
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// assign role to user through sync fn and assert channel sequence is from admin_channels
				docGrant{
					userName:       "alice",
					docIDs:         []string{"doc"},
					dynamicChannel: "A",
					docUserTest:    true,
					dynamicRole:    "role1",
					output:         `{"doc": {"A": { "entries" : ["3-0"]}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin role grants, assert earlier sequence (dynamic role) is used",
			grants: []grant{
				docGrant{dynamicChannel: "A"},
				// create user with no channels
				userGrant{
					user: "alice",
				},
				// create role with channel
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// create another role with same channel
				roleGrant{
					role:          "role2",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// assign first role through sync fn
				docGrant{
					docID:          "doc2",
					userName:       "alice",
					dynamicRole:    "role1",
					dynamicChannel: "docChan",
				},
				// assign second role through admin_roles and assert sequence is from dynamic (first) role
				userGrant{
					user:        "alice",
					roles:       []string{"role2"},
					docIDs:      []string{"doc"},
					docUserTest: true,
					output:      `{"doc": {"A": { "entries" : ["5-0"]}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin role grants, assert earlier sequence (admin role) is used",
			grants: []grant{
				docGrant{dynamicChannel: "A"},
				// create user with no channels
				userGrant{
					user: "alice",
				},
				// create role with channel
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// create another role with same channel
				roleGrant{
					role:          "role2",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// assign role through admin_roles
				userGrant{
					user:  "alice",
					roles: []string{"role1"},
				},
				// assign other role through sync fn and assert earlier sequences are returned
				docGrant{
					docID:          "doc2",
					userName:       "alice",
					dynamicRole:    "role2",
					docIDs:         []string{"doc"}, // doc is the doc made in the first grant
					dynamicChannel: "A",
					docUserTest:    true,
					output:         `{"doc": {"A": { "entries" : ["5-0"]}}}`,
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

func TestGetUserDocAccessSpanWithSingleNamedCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	bucket := base.GetTestBucket(t)
	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true, CustomTestBucket: bucket}, 1)
	defer rt.Close()
	SyncFn := `function(doc) {channel(doc.channel);}`
	// add single named collection
	newCollection := base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: "sg_test_0"}
	require.NoError(t, bucket.CreateDataStore(base.TestCtx(t), newCollection))
	defer func() {
		require.NoError(t, rt.TestBucket.DropDataStore(newCollection))
	}()

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = ScopesConfig{
		base.DefaultScope: {
			Collections: CollectionsConfig{
				base.DefaultCollection:         {SyncFn: &SyncFn},
				newCollection.CollectionName(): {SyncFn: &SyncFn},
			},
		},
	}
	RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

	grant1 := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace1}}": {"defaultCollChan"},
			"{{.keyspace2}}": {"coll2Chan"},
		},
	}
	grant1.request(rt)
	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace1}}/doc1", `{"channel":"defaultCollChan"}`)
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace2}}/doc1", `{"channel":"coll2Chan"}`)
	RequireStatus(t, resp, http.StatusCreated)

	expectedOutput1 := `{"doc1": {"defaultCollChan": { "entries" : ["2-0"]}}}`
	response := rt.SendDiagnosticRequest(http.MethodGet, "/{{.keyspace1}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput1), response.BodyString())

	expectedOutput2 := `{"doc1": {"coll2Chan": { "entries" : ["3-0"]}}}`
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace2}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput2), response.BodyString())
}

func TestGetUserDocAccessSpanWithMultiCollections(t *testing.T) {
	base.LongRunningTest(t)

	base.TestRequiresCollections(t)

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true, SyncFn: `function(doc) {channel(doc.channel);}`}, 2)
	defer rt.Close()

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	resp := rt.SendAdminRequest(http.MethodPut, "/{{.keyspace1}}/doc1", `{"foo":"bar", "channel":["coll1Chan"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	resp = rt.SendAdminRequest(http.MethodPut, "/{{.keyspace2}}/doc1", `{"foo":"bar", "channel":["coll2Chan"]}`)
	RequireStatus(t, resp, http.StatusCreated)

	// check removed channel in keyspace2 is in history before deleting collection 2
	grant1 := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace1}}": {"coll1Chan"},
			"{{.keyspace2}}": {"coll2Chan"},
		},
	}
	grant1.request(rt)

	expectedOutput1 := `{"doc1": {"coll1Chan": { "entries" : ["3-0"]}}}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace1}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput1), response.BodyString())

	expectedOutput2 := `{"doc1": {"coll2Chan": { "entries" : ["3-0"]}}}`
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace2}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput2), response.BodyString())

	// delete collection 2
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = GetCollectionsConfig(t, rt.TestBucket, 1)
	RequireStatus(t, rt.UpsertDbConfig("db", dbConfig), http.StatusCreated)

}

func TestGetUserDocAccessSpanDeletedRole(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		SyncFn: `function(doc) {channel(doc.channel); access(doc.user, doc.channel); role(doc.user, doc.role);}`,
	})
	defer rt.Close()
	ctx := base.TestCtx(t)
	testBucket := base.GetTestBucket(t)
	defer testBucket.Close(ctx)

	// Create role with 1 channel and assign it to user
	roleGrant1 := roleGrant{role: "role1", adminChannels: map[string][]string{"{{.keyspace}}": {"A"}}}
	roleGrant1.request(rt)

	userGrant1 := userGrant{
		user:  "alice",
		roles: []string{"role1"},
	}
	userGrant1.request(rt)

	// create doc in channel A
	doc := docGrant{dynamicChannel: "A"}
	doc.request(rt)
	userGrant1 = userGrant{
		user: "alice",
	}
	userGrant1.request(rt)

	// Delete role and assert its channels no longer appear in response
	resp := rt.SendAdminRequest("DELETE", "/db/_role/role1", ``)
	RequireStatus(t, resp, http.StatusOK)

	// seq 3 doc was made, seq 5 role was removed from user
	expectedOutput := `{"doc": {"A": { "entries" : ["3-5"]}}}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// put doc in multiple channels, remove from some channels, assert response gets right sequences for each channels
func TestGetUserDocAccessMultiChannel(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()
	userGrant := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"A", "B", "C"},
		},
	}
	userGrant.request(rt)

	version := rt.PutDoc("doc1", `{"channel":["A"]}`)
	updatedVersion := rt.UpdateDoc("doc1", version, `{"channel":["B", "A"]}`)
	updatedVersion = rt.UpdateDoc("doc1", updatedVersion, `{"channel":["C", "B"]}`)

	// assert sequences are registered correctly
	expectedOutput := `{"doc1": {"A": { "entries" : ["2-4"]}, "B": { "entries" : ["3-0"]},  "C": { "entries" : ["4-0"]} }}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())

	// remove all channels
	_ = rt.UpdateDoc("doc1", updatedVersion, `{"channel":[]}`)

	// assert sequences spans end here
	expectedOutput = `{"doc1": {"A": { "entries" : ["2-4"]}, "B": { "entries" : ["3-5"]},  "C": { "entries" : ["4-5"]} }}`
	response = rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// give user access to chanA through admin API and role, remove admin API assignment, and assert access span is admin assignment to 0
func TestGetUserDocAccessContinuousRoleAdminAPI(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel);}`})
	defer rt.Close()

	roleGrant1 := roleGrant{role: "role1", adminChannels: map[string][]string{"{{.keyspace}}": {"A"}}}
	roleGrant1.request(rt)

	// create doc in channel A
	doc := docGrant{dynamicChannel: "A"}
	doc.request(rt)

	userGrant1 := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"A"},
		},
	}
	userGrant1.request(rt)

	userGrant1 = userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"!"},
		},
		roles: []string{"role1"},
	}
	userGrant1.request(rt)

	// assert sequences are registered correctly
	expectedOutput := `{"doc": {"A": { "entries" : ["3-4", "4-0"]} }}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// give user access to chanA through admin API and sync fn, remove admin API assignment, and assert access span is admin assignment to 0
func TestGetUserDocAccessContinuousSyncFnAdminAPI(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.user, doc.channel);}`})
	defer rt.Close()

	userGrant1 := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"A"},
		},
	}
	userGrant1.request(rt)

	// create doc in channel A
	doc := docGrant{dynamicChannel: "A", userName: "alice"}
	doc.request(rt)

	userGrant1 = userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"!"},
		},
	}
	userGrant1.request(rt)

	// assert sequences are registered correctly
	expectedOutput := `{"doc": {"A": { "entries" : ["2-0"]} }}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// give user access to chanA through sync fn after removing doc from chan
func TestGetUserDocAccessDynamicGrantOnChanRemoval(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.user, doc.dynamicChan);}`})
	defer rt.Close()

	userGrant1 := userGrant{
		user: "alice",
	}
	userGrant1.request(rt)

	version := rt.PutDoc("doc1", `{"channel":["A"]}`)
	_ = rt.UpdateDoc("doc1", version, `{"dynamicChan":"A", "user":"alice"}`)

	// assert sequences are registered correctly
	expectedOutput := `{}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// give role access to chanA through sync fn, remove doc from channel and keep role assignment
func TestGetUserDocAccessDynamicRoleChanRemoval(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.user, doc.dynamicChan);}`})
	defer rt.Close()

	// create role1
	roleGrant1 := roleGrant{role: "role1"}
	roleGrant1.request(rt)

	// create doc in channel A, assign chan A to role1
	version := rt.PutDoc("doc1", `{"channel":["A"], "user":"role:role1", "dynamicChan":"A"}`)

	userGrant1 := userGrant{
		user:  "alice",
		roles: []string{"role1"},
	}
	userGrant1.request(rt)

	// update doc1 to remove chan A
	_ = rt.UpdateDoc("doc1", version, `{"user":"role:role1", "dynamicChan":"A"}`)

	// assert sequences are registered correctly
	expectedOutput := `{"doc1": {"A": { "entries" : ["3-4"]} }}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// give role access to chanA through sync fn, remove channel from role and keep doc in chan
func TestGetUserDocAccessDynamicRoleChanRemoval2(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.user, doc.dynamicChan);}`})
	defer rt.Close()

	// create role1
	roleGrant1 := roleGrant{role: "role1"}
	roleGrant1.request(rt)

	// create doc in channel A, assign chan A to role1
	version := rt.PutDoc("doc1", `{"channel":["A"], "user":"role:role1", "dynamicChan":"A"}`)

	ks := rt.GetSingleDataStore().ScopeName() + "." + rt.GetSingleDataStore().CollectionName()
	userGrant1 := userGrant{
		user:   "alice",
		roles:  []string{"role1"},
		output: fmt.Sprintf(`{"all_channels":{"%s":{"A":{"entries":["3-0"]}}}}`, ks),
	}
	userGrant1.request(rt)

	// update doc1 to remove chan A from role1
	_ = rt.UpdateDoc("doc1", version, `{"channel":["A"]}`)

	// assert sequences are registered correctly
	expectedOutput := `{"doc1": {"A": { "entries" : ["3-4"]} }}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1", ``)
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// multiple doc ids
func TestGetUserDocAccessMultiDoc(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.dynamicChan, doc.user);}`})
	defer rt.Close()

	// update doc1 to remove chan A
	userGrant1 := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"A"},
		}}
	userGrant1.request(rt)

	_ = rt.PutDoc("doc1", `{"channel":["A"]}`)
	_ = rt.PutDoc("doc2", `{"channel":["A"]}`)
	_ = rt.PutDoc("doc3", `{"channel":["A"]}`)
	_ = rt.PutDoc("doc4", `{"channel":["B"]}`)

	// assert sequences are registered correctly
	expectedOutput := `{"doc1":{"A":{"entries":["2-0"]}},"doc2":{"A":{"entries":["3-0"]}},"doc3":{"A":{"entries":["4-0"]}}}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1,doc2,doc3,doc4", ``)
	t.Log(response.BodyString())
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// multiple doc ids, one not found
func TestGetUserDocAccessMultiDocNotFound(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.dynamicChan, doc.user);}`})
	defer rt.Close()

	// update doc1 to remove chan A
	userGrant1 := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"A"},
		}}
	userGrant1.request(rt)

	_ = rt.PutDoc("doc1", `{"channel":["A"]}`)
	_ = rt.PutDoc("doc2", `{"channel":["A"]}`)
	_ = rt.PutDoc("doc3", `{"channel":["A"]}`)

	// assert sequences are registered correctly
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1,doc2,doc3,doc4", ``)
	RequireStatus(rt.TB(), response, http.StatusNotFound)
	assert.Contains(t, response.Body.String(), "doc doc4 not found")
}

// no doc id
func TestGetUserDocAccessNoDocID(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.dynamicChan, doc.user);}`})
	defer rt.Close()

	// update doc1 to remove chan A
	userGrant1 := userGrant{
		user: "alice",
	}
	userGrant1.request(rt)

	_ = rt.PutDoc("doc1", `{}`)

	// assert sequences are registered correctly
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=", ``)
	RequireStatus(rt.TB(), response, http.StatusBadRequest)
	assert.Contains(t, response.Body.String(), "empty doc id given in request")
}

// duplicate doc ids
func TestGetUserDocAccessDuplicates(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); access(doc.dynamicChan, doc.user);}`})
	defer rt.Close()

	// update doc1 to remove chan A
	userGrant1 := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace}}": {"A"},
		}}
	userGrant1.request(rt)

	_ = rt.PutDoc("doc1", `{"channel":["A"]}`)
	_ = rt.PutDoc("doc2", `{"channel":["A"]}`)

	// assert no duplicates
	expectedOutput := `{"doc1":{"A":{"entries":["2-0"]}}}`
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/alice?docids=doc1,doc1,doc1,doc1", ``)
	t.Log(response.BodyString())
	RequireStatus(rt.TB(), response, http.StatusOK)
	require.JSONEq(rt.TB(), rt.mustTemplateResource(expectedOutput), response.BodyString())
}

// Tests the Diagnostic Endpoint to dry run Sync Function
func TestSyncFuncDryRun(t *testing.T) {
	base.SkipImportTestsIfNotEnabled(t)

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	rt.CreateDatabase("db", dbConfig)

	// When the tests run with named scopes and collections, then the default
	// sync function is:
	// function(doc){channel("<collection_name>");}
	// when the tests run in default scope and collection then the default
	// sync function is:
	// function(doc){channel(doc.channels);}
	const docChannelName = "chanNew"
	var defaultChannelName string
	dbc, _ := rt.GetSingleTestDatabaseCollection()
	if dbc.IsDefaultCollection() {
		defaultChannelName = docChannelName
	} else {
		defaultChannelName = dbc.Name
	}

	tests := []struct {
		name            string
		dbSyncFunction  string
		existingDocBody string
		request         SyncFnDryRunPayload
		requestDocID    bool
		expectedOutput  SyncFnDryRun
		expectedStatus  int
	}{
		{
			name: "request sync function and doc body",
			request: SyncFnDryRunPayload{
				Function: `function(doc) {
					channel(doc.channel);
					access(doc.accessUser, doc.accessChannel);
					role(doc.accessUser, doc.role);
					expiry(doc.expiry);
				}`,
				Doc: db.Body{
					"accessChannel": []string{"dynamicChan5412"},
					"accessUser":    "user",
					"channel":       []string{"dynamicChan222"},
					"expiry":        10,
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "db sync function and doc",
			dbSyncFunction: `function(doc) {
				channel(doc.channel);
				access(doc.accessUser, doc.accessChannel);
				role(doc.accessUser, doc.role);
				expiry(doc.expiry);
			}`,
			existingDocBody: `{"accessChannel": ["dynamicChan5412"], "accessUser": "user", "channel": ["dynamicChan222"], "expiry": 10}`,
			requestDocID:    true,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "db sync function with request doc body",
			dbSyncFunction: `function(doc) {
				channel(doc.channel);
				access(doc.accessUser, doc.accessChannel);
				role(doc.accessUser, doc.role);
				expiry(doc.expiry);
			}`,
			request: SyncFnDryRunPayload{
				Doc: db.Body{
					"accessChannel": []string{"dynamicChan5412"},
					"accessUser":    "user",
					"channel":       []string{"dynamicChan222"},
					"expiry":        10,
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request sync function with db doc",
			request: SyncFnDryRunPayload{
				Function: `function(doc) {
					channel(doc.channel);
					access(doc.accessUser, doc.accessChannel);
					role(doc.accessUser, doc.role);
					expiry(doc.expiry);
				}`,
			},
			requestDocID:    true,
			existingDocBody: `{"accessChannel": ["dynamicChan5412"], "accessUser": "user", "channel": ["dynamicChan222"], "expiry": 10}`,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "sync function precedence check",
			dbSyncFunction: `function(doc) {
				channel("channel_from_db_sync_func");
			}`,
			request: SyncFnDryRunPayload{
				Function: `function(doc) {
					channel("channel_from_request_sync_func");
				}`,
				Doc: db.Body{"foo": "bar"},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"channel_from_request_sync_func"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request sync function and doc with db oldDoc",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc) {
					if (doc) {
						channel(doc.newDoc);
					};
					if (oldDoc) {
						channel(oldDoc.oldDoc);
					};
				}`,
				Doc: db.Body{"newDoc": "newdoc_channel"},
			},
			requestDocID:    true, // fetch oldDoc by ID
			existingDocBody: `{"oldDoc": "olddoc_channel"}`,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetOf("newdoc_channel", "olddoc_channel"),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request sync function and doc with inline oldDoc",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc) {
					if (doc) {
						channel(doc.newDoc);
					};
					if (oldDoc) {
						channel(oldDoc.oldDoc);
					};
				}`,
				Doc:    db.Body{"newDoc": "newdoc_channel"},
				OldDoc: db.Body{"oldDoc": "olddoc_channel"},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetOf("newdoc_channel", "olddoc_channel"),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "sync func exception",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc) {
					if (doc.user.num >= 100) {
						channel(doc.channel);
					} else {
						throw({forbidden: 'user num too low'});
					}
				}`,
				Doc: db.Body{"user": map[string]any{"num": 23}},
			},
			expectedOutput: SyncFnDryRun{
				Channels:  base.SetFromArray([]string{}),
				Access:    channels.AccessMap{},
				Roles:     channels.AccessMap{},
				Exception: "403 user num too low",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "sync func exception typeError",
			dbSyncFunction: `function(doc, oldDoc) {
				if (doc.user.num >= 100) {
					channel(doc.channel);
				} else {
					throw({forbidden: 'user num too low'});
				}
				if (oldDoc) {
					console.log("got oldDoc");
				    // This will cause a TypeError since doc.user.name is undefined (on the new doc)
					access(doc.user.name[0], doc.channel);
				}
			}`,
			request: SyncFnDryRunPayload{
				Doc: db.Body{"user": map[string]any{"num": 150}, "channel": "abc"},
			},
			requestDocID:    true, // fetch oldDoc by ID
			existingDocBody: `{"user":{"num":123, "name":["user1"]}, "channel":"channel1"}`,
			expectedOutput: SyncFnDryRun{
				Exception: "Error returned from Sync Function: TypeError: Cannot access member '0' of undefined",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{`got oldDoc`},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "default sync function with request doc",
			request: SyncFnDryRunPayload{
				Doc: db.Body{"channels": docChannelName},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetOf(defaultChannelName),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "logging with request sync fn",
			requestDocID:    true,
			existingDocBody: `{"channel": "chanLog", "logerror": true, "loginfo": true}`,
			request: SyncFnDryRunPayload{
				Function: `function(doc) {
					channel(doc.channel);
					if (doc.logerror) {
						console.error("This is a console.error log from doc.logerror");
					} else {
						console.log("This is a console.log log from doc.logerror");
					}
					if (doc.loginfo) {
						console.log("This is a console.log log from doc.loginfo");
					} else {
						console.error("This is a console.error log from doc.loginfo");
					}
					console.log("one more info for good measure...");
				}`,
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"chanLog"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{"This is a console.error log from doc.logerror"},
					Info:   []string{"This is a console.log log from doc.loginfo", "one more info for good measure..."},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "logging with db sync function",
			requestDocID:    true,
			existingDocBody: `{"channel": "chanLog", "logerror": true, "loginfo": true}`,
			dbSyncFunction: `function(doc) {
				channel(doc.channel);
				if (doc.logerror) {
					console.error("This is a console.error log from doc.logerror");
				} else {
					console.log("This is a console.log log from doc.logerror");
				}
				if (doc.loginfo) {
					console.log("This is a console.log log from doc.loginfo");
				} else {
					console.error("This is a console.error log from doc.loginfo");
				}
				console.log("one more info for good measure...");
			}`,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"chanLog"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{"This is a console.error log from doc.logerror"},
					Info:   []string{"This is a console.log log from doc.loginfo", "one more info for good measure..."},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with request doc sync func and user name",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					requireUser("sgw-user");
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
				UserCtx: &SyncDryRunUserCtx{
					Name: "sgw-user",
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with request doc sync func and incorrect user name",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					requireUser("sgw-user");
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
				UserCtx: &SyncDryRunUserCtx{
					Name: "sgw-user1",
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels:  base.SetFromArray([]string{}),
				Access:    channels.AccessMap{},
				Roles:     channels.AccessMap{},
				Exception: "403 sg wrong user",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with request doc sync func and user name and access channel",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					requireAccess("access-channel");
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
				UserCtx: &SyncDryRunUserCtx{
					Name:     "sgw-user1",
					Channels: []string{"access-channel"},
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with request doc sync func and user name and invalid access channel",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					requireAccess("access-channel");
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
				UserCtx: &SyncDryRunUserCtx{
					Name:     "sgw-user1",
					Channels: []string{"incorrect-channel"},
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels:  base.SetFromArray([]string{}),
				Access:    channels.AccessMap{},
				Roles:     channels.AccessMap{},
				Exception: "403 sg missing channel access",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with request doc sync func and user name and role",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					requireRole("sgw-role");
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
				UserCtx: &SyncDryRunUserCtx{
					Name:  "sgw-user1",
					Roles: []string{"sgw-role"},
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with request doc sync func and user name and invalid role",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					requireRole("sgw-role");
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
				UserCtx: &SyncDryRunUserCtx{
					Name:  "sgw-user1",
					Roles: []string{"sgw-role1"},
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels:  base.SetFromArray([]string{}),
				Access:    channels.AccessMap{},
				Roles:     channels.AccessMap{},
				Exception: "403 sg missing role",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "missing _id in request doc",
			dbSyncFunction: `function(doc, oldDoc, meta) {
				console.log(JSON.stringify(doc))
			}`,
			request: SyncFnDryRunPayload{
				Doc: db.Body{
					"foo": "bar",
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{fmt.Sprintf("{\"_id\":\"%s\",\"_rev\":\"1-cd809becc169215072fd567eebd8b8de\",\"foo\":\"bar\"}", defaultSyncDryRunDocID)},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "doc id in request doc",
			dbSyncFunction: `function(doc, oldDoc, meta) {
				console.log(JSON.stringify(doc))
			}`,
			request: SyncFnDryRunPayload{
				Doc: db.Body{
					db.BodyId: "test",
					"foo":     "bar",
				},
			},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{"{\"_id\":\"test\",\"_rev\":\"1-cd809becc169215072fd567eebd8b8de\",\"foo\":\"bar\"}"},
				},
			},
			expectedStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		rt.Run(test.name, func(t *testing.T) {
			if test.dbSyncFunction != "" {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/sync", test.dbSyncFunction), http.StatusOK)
			} else {
				// reset to default sync function
				RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/_config/sync", ""), http.StatusOK)
			}

			if test.existingDocBody != "" {
				rt.PutDoc(test.name, test.existingDocBody)
			}

			url := "/{{.keyspace}}/_sync"
			if test.requestDocID {
				test.request.DocID = test.name
			}

			bodyBytes, err := json.Marshal(test.request)
			require.NoError(t, err)
			resp := rt.SendDiagnosticRequest("POST", url, string(bodyBytes))
			RequireStatus(t, resp, test.expectedStatus)

			var output SyncFnDryRun
			err = json.Unmarshal(resp.Body.Bytes(), &output)
			require.NoError(t, err)
			assert.Equal(t, test.expectedOutput, output)
		})
	}
}

// Tests the Diagnostic Endpoint when user xattrs is passed in the request body
// This is a separate test, as enabling user xattrs is an EE feature only.
func TestSyncFuncDryRunUserXattrs(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for some config properties")
	}
	base.SkipImportTestsIfNotEnabled(t)

	ctx := base.TestCtx(t)
	bucket := base.GetTestBucket(t)
	defer bucket.Close(ctx)
	dataStore := bucket.GetSingleDataStore()

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
		CustomTestBucket: bucket,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	rt.CreateDatabase("db", dbConfig)

	tests := []struct {
		name            string
		dbSyncFunction  string
		existingDocBody string
		request         SyncFnDryRunPayload
		requestDocID    bool
		expectedOutput  SyncFnDryRun
		expectedStatus  int
		xattrKey        string
		xattrVal        any
	}{
		{
			name: "dry run with request user meta and sync function",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					if (meta.xattrs.channelXattr === undefined){
					  console.log("no user_xattr_key defined")
					  channel(null)
					} else {
					  channel(meta.xattrs.channelXattr)
					}
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
				Meta: SyncFnDryRunMetaMap{Xattrs: map[string]any{"channelXattr": []string{"channel1", "channel3", "useradmin"}}},
			},
			xattrKey: "channelXattr",
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"channel1", "channel3", "useradmin"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with request doc user meta and sync function",
			request: SyncFnDryRunPayload{
				Function: `function(doc, oldDoc, meta) {
					if (meta.xattrs.channelXattr === undefined){
					  console.log("no user_xattr_key defined")
					  channel(null)
					} else {
					  channel(meta.xattrs.channelXattr)
					}
				}`,
				Doc: db.Body{
					"foo": "bar",
				},
			},
			existingDocBody: `{"docVersion": 1}`,
			xattrKey:        "channelXattr",
			xattrVal:        []string{"channel1", "channel3", "useradmin"},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"channel1", "channel3", "useradmin"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			requestDocID:   true,
			expectedStatus: http.StatusOK,
		},
		{
			name: "dry run with existing doc user meta and sync function",
			dbSyncFunction: `function(doc, oldDoc, meta) {
					if (meta.xattrs.channelXattr === undefined){
					  console.log("no user_xattr_key defined")
					  channel(null)
					} else {
					  channel(meta.xattrs.channelXattr)
					}
				}`,
			existingDocBody: `{"docVersion": 1}`,
			xattrKey:        "channelXattr",
			xattrVal:        []string{"channel1", "channel3", "useradmin"},
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"channel1", "channel3", "useradmin"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			requestDocID:   true,
			expectedStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		rt.Run(test.name, func(t *testing.T) {
			if test.dbSyncFunction != "" {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPut, "/{{.keyspace}}/_config/sync", test.dbSyncFunction), http.StatusOK)
			} else {
				// reset to default sync function
				RequireStatus(t, rt.SendAdminRequest(http.MethodDelete, "/{{.keyspace}}/_config/sync", ""), http.StatusOK)
			}

			if test.xattrKey != "" {
				RequireStatus(t, rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_config", fmt.Sprintf(`{"user_xattr_key": "%s"}`, test.xattrKey)), http.StatusCreated)
			}

			if test.existingDocBody != "" {
				rt.PutDoc(test.name, test.existingDocBody)
				if test.xattrKey != "" {
					_, err := dataStore.SetXattrs(ctx, test.name, map[string][]byte{test.xattrKey: base.MustJSONMarshal(t, test.xattrVal)})
					require.NoError(t, err)
				}
			}

			url := "/{{.keyspace}}/_sync"
			if test.requestDocID {
				test.request.DocID = test.name
			}

			bodyBytes, err := json.Marshal(test.request)
			require.NoError(t, err)

			resp := rt.SendDiagnosticRequest("POST", url, string(bodyBytes))
			RequireStatus(t, resp, test.expectedStatus)

			var output SyncFnDryRun
			err = json.Unmarshal(resp.Body.Bytes(), &output)
			require.NoError(t, err)
			assert.Equal(t, test.expectedOutput, output)
		})
	}
}

func TestSyncFuncDryRunErrors(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// doc ID not found
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc_id": "missing"}`), http.StatusNotFound)
	// only oldDoc provided
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"oldDoc": {"foo":"old"}}`), http.StatusBadRequest)
	// doc ID and inline oldDoc provided
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc_id": "somedoc", "oldDoc": {"foo":"old"}}`), http.StatusBadRequest)
	// no doc ID or inline body provided
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{}`), http.StatusBadRequest)
	// invalid request json
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc": {"invalid_json"}`), http.StatusBadRequest)
	// invalid doc body type
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc": "invalid_doc"}`), http.StatusBadRequest)
	// invalid doc body type
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc": {"_sync":"this is a forbidden field"}}`), http.StatusBadRequest)
	// no user name
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc": {"foo":"bar"}, "userCtx": {"roles": ["role1", "role2"]} }`), http.StatusBadRequest)
}

func TestSyncFuncDryRunUserXattrErrors(t *testing.T) {
	if !base.IsEnterpriseEdition() {
		t.Skipf("Requires EE for some config properties")
	}
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.Name = "db1"
	dbConfig.UserXattrKey = base.Ptr("channelXattrs")

	RequireStatus(t, rt.CreateDatabase("db1", dbConfig), http.StatusCreated)
	// Invalid meta body
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"meta":{"foo": "bar"}}`), http.StatusBadRequest)
	// Invalid meta body
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc": {"foo":"bar"}, "meta":{"xattrs": {"foo": "bar"}}}`), http.StatusBadRequest)
	// Multiple xattr keys
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_sync", `{"doc": {"foo":"bar"}, "meta":{"xattrs": {"channelXattrs": "channel1", "channelXattrs2": "channel2"}}}`), http.StatusBadRequest)
}

func TestImportFilterDryRun(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	tests := []struct {
		name            string
		dbImportFilter  string
		request         ImportFilterDryRunPayload
		requestDocID    bool
		existingDocBody string
		expectedOutput  ImportFilterDryRun
		expectedStatus  int
	}{
		{
			name: "db import filter",
			dbImportFilter: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			request: ImportFilterDryRunPayload{
				Function: "",
				Doc: db.Body{
					"user": map[string]interface{}{"num": 23},
				},
			},
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request db import filter",
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
				Doc: db.Body{
					"user": map[string]interface{}{"num": 23},
				},
			},
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "db and request import filter",
			dbImportFilter: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
				Doc: db.Body{
					"user": map[string]interface{}{"num": 23},
				},
			},
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "db import filter type error",
			dbImportFilter: `function(doc) {
						// This will cause a Type error since the document tested will not contain doc.user.num
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			request: ImportFilterDryRunPayload{
				Doc: db.Body{
					"accessUser": "user",
				},
			},
			expectedOutput: ImportFilterDryRun{
				Exception: "Error returned from Import Filter: TypeError: Cannot access member 'num' of undefined",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request import filter type error",
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						// This will cause a Type error since the document tested will not contain doc.user.num
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
				Doc: db.Body{
					"accessUser": "user",
				},
			},
			expectedOutput: ImportFilterDryRun{
				Exception: "Error returned from Import Filter: TypeError: Cannot access member 'num' of undefined",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "db and request import filter type error",
			dbImportFilter: `function(doc) {
						// This will cause a Type error since the document tested will not contain doc.user.num
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
					// This will cause a Type error since the document tested will not contain doc.user.num
					if (doc.user.num) {
						return true;
					} else {
						return false;
					}
				}`,
				Doc: db.Body{
					"accessUser": "user",
				},
			},
			expectedOutput: ImportFilterDryRun{
				Exception: "Error returned from Import Filter: TypeError: Cannot access member 'num' of undefined",
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "db import filter does not import doc",
			dbImportFilter: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			request: ImportFilterDryRunPayload{
				Doc: db.Body{
					"user": 23,
				},
			},
			expectedOutput: ImportFilterDryRun{
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request import filter does not import doc",
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
				Doc: db.Body{
					"user": 23,
				},
			},
			expectedOutput: ImportFilterDryRun{
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request  and db import filter does not import doc",
			dbImportFilter: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
				Doc: db.Body{
					"user": 23,
				},
			},
			expectedOutput: ImportFilterDryRun{
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "db import filter with existing doc",
			dbImportFilter: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			requestDocID:    true,
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request import filter with existing doc",
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			},
			requestDocID:    true,
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "request and db import filter with existing doc",
			dbImportFilter: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						if (doc.user.num) {
							return true;
						} else {
							return false;
						}
					}`,
			},
			requestDocID:    true,
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no import filter with existing doc",
			requestDocID:    true,
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "no import filter with request doc",
			request: ImportFilterDryRunPayload{
				Doc: db.Body{
					"user": 23,
				},
			},
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{},
					Info:   []string{},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "logging with db import filter",
			dbImportFilter: `function(doc) {
						if (doc.logerror) {
							console.error("This is a console.error log from doc.logerror");
						} else {
							console.log("This is a console.log log from doc.logerror");
						}
						if (doc.loginfo) {
							console.log("This is a console.log log from doc.loginfo");
						} else {
							console.error("This is a console.error log from doc.loginfo");
						}
						console.log("one more info for good measure...");
						return true
					}`,
			requestDocID:    true,
			existingDocBody: `{ "logerror": true, "loginfo": true}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{"This is a console.error log from doc.logerror"},
					Info:   []string{"This is a console.log log from doc.loginfo", "one more info for good measure..."},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name: "logging with request import filter",
			request: ImportFilterDryRunPayload{
				Function: `function(doc) {
						if (doc.logerror) {
							console.error("This is a console.error log from doc.logerror");
						} else {
							console.log("This is a console.log log from doc.logerror");
						}
						if (doc.loginfo) {
							console.log("This is a console.log log from doc.loginfo");
						} else {
							console.error("This is a console.error log from doc.loginfo");
						}
						console.log("one more info for good measure...");
						return true
					}`,
			},
			requestDocID:    true,
			existingDocBody: `{ "logerror": true, "loginfo": true}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
				Logging: DryRunLogging{
					Errors: []string{"This is a console.error log from doc.logerror"},
					Info:   []string{"This is a console.log log from doc.loginfo", "one more info for good measure..."},
				},
			},
			expectedStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		rt.Run(test.name, func(t *testing.T) {

			RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/_config/import_filter", test.dbImportFilter), http.StatusOK)

			if test.existingDocBody != "" {
				rt.PutDoc(test.name, test.existingDocBody)
			}

			url := "/{{.keyspace}}/_import_filter"
			if test.requestDocID {
				test.request.DocID = test.name
			}

			bodyBytes, err := json.Marshal(test.request)
			require.NoError(t, err)

			resp := rt.SendDiagnosticRequest("POST", url, string(bodyBytes))
			RequireStatus(t, resp, test.expectedStatus)

			var output ImportFilterDryRun
			err = json.Unmarshal(resp.Body.Bytes(), &output)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedOutput, output)
		})
	}
}

func TestImportFilterDryRunErrors(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	// doc ID not found
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_import_filter", `{"doc_id": "missing"}`), http.StatusNotFound)
	// invalid request
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_import_filter", `{"doc_id": "doc", "doc": { "user" : {"num": 23 }}}`), http.StatusBadRequest)
	// invalid request json
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_import_filter", `{"doc": {"invalid_json"}`), http.StatusBadRequest)
	// invalid doc body type
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_import_filter", `{"doc": "invalid_doc"}`), http.StatusBadRequest)
	// no docID and no body
	RequireStatus(t, rt.SendDiagnosticRequest(http.MethodPost, "/{{.keyspace}}/_import_filter", `{}`), http.StatusBadRequest)
}
