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
	if base.TestsUseNamedCollections() {
		t.Skip("Only works with default collection until CBG-4003 is fixed")
	}
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	tests := []struct {
		name            string
		dbSyncFunction  string
		syncFunction    string
		document        any
		docID           string
		existingDoc     bool
		existingDocID   string
		existingDocBody string
		expectedOutput  SyncFnDryRun
		expectedStatus  int
	}{
		{
			name:           "custom_sync_func-db_sync_func-doc_body-no_doc_id-no_existing_doc",
			dbSyncFunction: "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			syncFunction:   "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			document:       map[string]any{"accessChannel": []string{"dynamicChan5412"}, "accessUser": "user", "channel": []string{"dynamicChan222"}, "expiry": 10},
			existingDoc:    false,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "custom_sync_func-db_sync_func-doc_body-no_doc_id-no_existing_doc-role",
			dbSyncFunction: "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			syncFunction:   "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			document:       map[string]any{"role": []string{"role:role1"}, "accessUser": "user"},
			existingDoc:    false,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{"user": channels.BaseSetOf(t, "role1")},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "custom_sync_func-no_db_sync_func-doc_body-no_existing_doc-no_doc_id",
			dbSyncFunction: "",
			syncFunction:   "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			document:       map[string]any{"accessChannel": []string{"dynamicChan5412"}, "accessUser": "user", "channel": []string{"dynamicChan222"}, "expiry": 10},
			existingDoc:    false,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "no_custom_sync_func-db_sync_func-doc_body-no_existing_doc-no_doc_id",
			dbSyncFunction: "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			syncFunction:   "",
			document:       map[string]any{"accessChannel": []string{"dynamicChan5412"}, "accessUser": "user", "channel": []string{"dynamicChan222"}, "expiry": 10},
			existingDoc:    false,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "custom_sync_func-db_sync_func-doc_body-existing_doc-no_doc_id",
			dbSyncFunction:  "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			syncFunction:    "function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel); role(doc.accessUser, doc.role); expiry(doc.expiry);}",
			document:        map[string]any{"accessChannel": []string{"dynamicChan5412"}, "accessUser": "user", "channel": []string{"dynamicChan222"}, "expiry": 10},
			existingDoc:     true,
			existingDocID:   "doc1",
			existingDocBody: `{"accessChannel": ["dynamicChan5412"],"accessUser": "user","channel": ["dynamicChan222"],"expiry":10}`,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"dynamicChan222"}),
				Access:   channels.AccessMap{"user": channels.BaseSetOf(t, "dynamicChan5412")},
				Roles:    channels.AccessMap{},
				Expiry:   base.Ptr(uint32(10)),
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "no_custom_sync_func-db_sync_func-doc_body-no_existing_doc-no_doc_id-sync_func_exception",
			dbSyncFunction: "function(doc,oldDoc){if (doc.user.num >= 100) {channel(doc.channel);} else {throw({forbidden: 'user num too low'});}if (oldDoc){ console.log(oldDoc); if (oldDoc.user.num > doc.user.num) { access(oldDoc.user.name, doc.channel);} else {access(doc.user.name[0], doc.channel);}}}",
			syncFunction:   "",
			document:       map[string]any{"user": map[string]any{"num": 23}},
			existingDoc:    false,
			expectedOutput: SyncFnDryRun{
				Channels:  base.SetFromArray([]string{}),
				Access:    channels.AccessMap{},
				Roles:     channels.AccessMap{},
				Exception: "403 user num too low",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-doc_body-no_existing_doc-doc_id-sync_func_exception_typeError",
			dbSyncFunction:  "function(doc,oldDoc){if (doc.user.num >= 100) {channel(doc.channel);} else {throw({forbidden: 'user num too low'});}if (oldDoc){ console.log(oldDoc); if (oldDoc.user.num > doc.user.num) { access(oldDoc.user.name, doc.channel);} else {access(doc.user.name[0], doc.channel);}}}",
			syncFunction:    "",
			document:        map[string]any{"user": map[string]any{"num": 150}, "channel": "abc"},
			docID:           "doc",
			existingDoc:     true,
			existingDocID:   "doc",
			existingDocBody: `{"user":{"num":123, "name":["user1"]}, "channel":"channel1"}`,
			expectedOutput: SyncFnDryRun{
				Exception: "Error returned from Sync Function: TypeError: Cannot access member '0' of undefined",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-doc_body-existing_doc-doc_id",
			dbSyncFunction:  "function(doc,oldDoc){if (doc.user.num >= 100) {channel(doc.channel);} else {throw({forbidden: 'user num too low'});}if (oldDoc){ console.log(oldDoc); if (oldDoc.user.num > doc.user.num) { access(oldDoc.user.name, doc.channel);} else {access(doc.user.name[0], doc.channel);}}}",
			syncFunction:    "",
			document:        map[string]any{"user": map[string]any{"num": 120, "name": []string{"user2"}}, "channel": "channel2"},
			docID:           "doc",
			existingDoc:     true,
			existingDocID:   "doc",
			existingDocBody: `{"user":{"num":123, "name":["user1"]}, "channel":"channel1"}`,
			expectedOutput: SyncFnDryRun{
				Channels:  base.SetFromArray([]string{"channel2"}),
				Access:    channels.AccessMap{"user1": channels.BaseSetOf(t, "channel2")},
				Roles:     channels.AccessMap{},
				Exception: "",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-no_doc_body-existing_doc-doc_id",
			dbSyncFunction:  "function(doc,oldDoc){if (doc.user.num >= 100) {channel(doc.channel);} else {throw({forbidden: 'user num too low'});}if (oldDoc){ console.log(oldDoc); if (oldDoc.user.num > doc.user.num) { access(oldDoc.user.name, doc.channel);} else {access(doc.user.name[0], doc.channel);}}}",
			syncFunction:    "",
			docID:           "doc",
			existingDoc:     true,
			existingDocID:   "doc",
			existingDocBody: `{"user":{"num":123, "name":["user1"]}, "channel":"channel1"}`,
			expectedOutput: SyncFnDryRun{
				Channels:  base.SetFromArray([]string{"channel1"}),
				Access:    channels.AccessMap{},
				Roles:     channels.AccessMap{},
				Exception: "",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-no_doc_body-existing_doc-invalid_doc_id",
			dbSyncFunction:  "function(doc,oldDoc){if (doc.user.num >= 100) {channel(doc.channel);} else {throw({forbidden: 'user num too low'});}if (oldDoc){ console.log(oldDoc); if (oldDoc.user.num > doc.user.num) { access(oldDoc.user.name, doc.channel);} else {access(doc.user.name[0], doc.channel);}}}",
			syncFunction:    "",
			docID:           "doc404",
			existingDoc:     true,
			existingDocID:   "doc",
			existingDocBody: `{"user":{"num":123, "name":["user1"]}, "channel":"channel1"}`,
			expectedOutput:  SyncFnDryRun{},
			expectedStatus:  http.StatusNotFound,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-no_doc_body-existing_doc-no_doc_id",
			dbSyncFunction:  "function(doc,oldDoc){if (doc.user.num >= 100) {channel(doc.channel);} else {throw({forbidden: 'user num too low'});}if (oldDoc){ console.log(oldDoc); if (oldDoc.user.num > doc.user.num) { access(oldDoc.user.name, doc.channel);} else {access(doc.user.name[0], doc.channel);}}}",
			syncFunction:    "",
			docID:           "",
			existingDoc:     true,
			existingDocID:   "doc",
			existingDocBody: `{"user":{"num":123, "name":["user1"]}, "channel":"channel1"}`,
			expectedOutput:  SyncFnDryRun{},
			expectedStatus:  http.StatusBadRequest,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-doc_body-existing_doc-no_doc_id-new_doc_channel",
			dbSyncFunction:  "function(doc,oldDoc){if(oldDoc){ channel(oldDoc.channel)} else {channel(doc.channel)} }",
			syncFunction:    "",
			document:        map[string]any{"channel": "channel2"},
			docID:           "",
			existingDoc:     true,
			existingDocID:   "doc22",
			existingDocBody: `{"chan1":"channel1", "channel":"chanOld"}`,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"channel2"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-doc_body-existing_doc-doc_id-old_doc_channel",
			dbSyncFunction:  "function(doc,oldDoc){if(oldDoc){ channel(oldDoc.channel)} else {channel(doc.channel)} }",
			syncFunction:    "",
			document:        map[string]any{"channel": "chanNew"},
			docID:           "doc22",
			existingDoc:     true,
			existingDocID:   "doc22",
			existingDocBody: `{"chan1":"channel1", "channel":"chanOld"}`,
			expectedOutput: SyncFnDryRun{
				Channels: base.SetFromArray([]string{"chanOld"}),
				Access:   channels.AccessMap{},
				Roles:    channels.AccessMap{},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no_custom_sync_func-db_sync_func-invalid_doc_body-existing_doc-doc_id",
			dbSyncFunction:  "function(doc,oldDoc){if(oldDoc){ channel(oldDoc.channel)} else {channel(doc.channel)} }",
			syncFunction:    "",
			document:        `{"channel": "chanNew", "oldchannel":}`,
			docID:           "doc22",
			existingDoc:     true,
			existingDocID:   "doc22",
			existingDocBody: `{"chan1":"channel1", "channel":"chanOld"}`,
			expectedOutput:  SyncFnDryRun{},
			expectedStatus:  http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rt := NewRestTester(t, &RestTesterConfig{
				PersistentConfig: true,
				SyncFn:           test.dbSyncFunction,
			})
			defer rt.Close()

			RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

			url := "/{{.keyspace}}/_sync"
			if test.existingDoc {
				ver := rt.PutDoc(test.existingDocID, test.existingDocBody)
				rt.WaitForVersion(test.existingDocID, ver)
			}
			if test.docID != "" {
				url += "?doc_id=" + test.docID
			}
			bodyMap := make(map[string]interface{})
			if test.syncFunction != "" {
				bodyMap["sync_function"] = test.syncFunction
			}
			if test.document != nil {
				bodyMap["doc"] = test.document
			}
			bodyBytes, _ := json.Marshal(bodyMap)
			resp := rt.SendDiagnosticRequest("POST", url, string(bodyBytes))
			RequireStatus(t, resp, test.expectedStatus)

			var output SyncFnDryRun
			err := json.Unmarshal(resp.Body.Bytes(), &output)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedOutput, output)
		})
	}
}

func TestImportFilterDryRun(t *testing.T) {

	base.SkipImportTestsIfNotEnabled(t)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	tests := []struct {
		name            string
		dbImportFilter  string
		importFilter    string
		document        any
		docID           string
		existingDoc     bool
		existingDocID   string
		existingDocBody string
		expectedOutput  ImportFilterDryRun
		expectedStatus  int
	}{
		{
			name:           "db_import_filter-no_custom_import_filter-doc_body-no_existing_doc",
			dbImportFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:   "",
			document:       map[string]interface{}{"user": map[string]interface{}{"num": 23}},
			existingDoc:    false,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:         "no_db_import_filter-custom_import_filter-doc_body-no_existing_doc",
			importFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:     map[string]interface{}{"user": map[string]interface{}{"num": 23}},
			existingDoc:  false,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "db_import_filter-custom_import_filter-doc_body-no_existing_doc",
			dbImportFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:   "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:       map[string]interface{}{"user": map[string]interface{}{"num": 23}},
			existingDoc:    false,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "db_import_filter-no_custom_import_filter-doc_body-no_existing_doc-filter_typeError",
			dbImportFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:   "",
			document:       map[string]any{"accessUser": "user"},
			existingDoc:    false,
			expectedOutput: ImportFilterDryRun{
				Error: "Error returned from Import Filter: TypeError: Cannot access member 'num' of undefined",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:         "no_db_import_filter-custom_import_filter-doc_body-no_existing_doc-filter_typeError",
			importFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:     map[string]any{"accessUser": "user"},
			existingDoc:  false,
			expectedOutput: ImportFilterDryRun{
				Error: "Error returned from Import Filter: TypeError: Cannot access member 'num' of undefined",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "db_import_filter-custom_import_filter-doc_body-no_existing_doc-filter_typeError",
			dbImportFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:   "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:       map[string]any{"accessUser": "user"},
			existingDoc:    false,
			expectedOutput: ImportFilterDryRun{
				Error: "Error returned from Import Filter: TypeError: Cannot access member 'num' of undefined",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "db_import_filter-no_custom_import_filter-doc_body-no_existing_doc-filter_false",
			dbImportFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:   "",
			document:       map[string]any{"user": 23},
			existingDoc:    false,
			expectedOutput: ImportFilterDryRun{},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "no_db_import_filter-custom_import_filter-doc_body-no_existing_doc-filter_false",
			importFilter:   "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:       map[string]any{"user": 23},
			existingDoc:    false,
			expectedOutput: ImportFilterDryRun{},
			expectedStatus: http.StatusOK,
		},
		{
			name:           "db_import_filter-custom_import_filter-doc_body-no_existing_doc-filter_false",
			dbImportFilter: "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:   "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:       map[string]any{"user": 23},
			existingDoc:    false,
			expectedOutput: ImportFilterDryRun{},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "db_import_filter-no_custom_import_filter-no_doc_body-existing_doc-docid",
			dbImportFilter:  "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			docID:           "db_import_filter-no_custom_import_filter-no_doc_body-existing_doc-docid",
			existingDoc:     true,
			existingDocID:   "db_import_filter-no_custom_import_filter-no_doc_body-existing_doc-docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "no_db_import_filter-custom_import_filter-no_doc_body-existing_doc-docid",
			importFilter:    "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			docID:           "no_db_import_filter-custom_import_filter-no_doc_body-existing_doc-docid",
			existingDoc:     true,
			existingDocID:   "no_db_import_filter-custom_import_filter-no_doc_body-existing_doc-docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "db_import_filter-custom_import_filter-no_doc_body-existing_doc-docid",
			dbImportFilter:  "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:    "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			docID:           "db_import_filter-custom_import_filter-no_doc_body-existing_doc-docid",
			existingDoc:     true,
			existingDocID:   "db_import_filter-custom_import_filter-no_doc_body-existing_doc-docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				ShouldImport: true,
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:            "db_import_filter-no_custom_import_filter-no_doc_body-existing_doc-invalid_docid",
			dbImportFilter:  "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			docID:           "doc404",
			existingDoc:     true,
			existingDocID:   "db_import_filter-no_custom_import_filter-no_doc_body-existing_doc-invalid_docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				Error: "not_found",
			},
			expectedStatus: http.StatusNotFound,
		},
		{
			name:            "no_db_import_filter-custom_import_filter-no_doc_body-existing_doc-invalid_docid",
			importFilter:    "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			docID:           "doc404",
			existingDoc:     true,
			existingDocID:   "no_db_import_filter-custom_import_filter-no_doc_body-existing_doc-invalid_docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				Error: "not_found",
			},
			expectedStatus: http.StatusNotFound,
		},
		{
			name:            "db_import_filter-custom_import_filter-no_doc_body-existing_doc-invalid_docid",
			dbImportFilter:  "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:    "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			docID:           "doc404",
			existingDoc:     true,
			existingDocID:   "db_import_filter-custom_import_filter-no_doc_body-existing_doc-invalid_docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				Error: "not_found",
			},
			expectedStatus: http.StatusNotFound,
		},
		{
			name:            "db_import_filter-no_custom_import_filter-doc_body-existing_doc-docid",
			dbImportFilter:  "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:        map[string]interface{}{"user": map[string]interface{}{"num": 23}},
			docID:           "db_import_filter-no_custom_import_filter-doc_body-existing_doc-docid",
			existingDoc:     true,
			existingDocID:   "db_import_filter-no_custom_import_filter-doc_body-existing_doc-docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				Error: "Bad Request",
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:            "no_db_import_filter-custom_import_filter-doc_body-existing_doc-docid",
			importFilter:    "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:        map[string]interface{}{"user": map[string]interface{}{"num": 23}},
			docID:           "no_db_import_filter-custom_import_filter-doc_body-existing_doc-docid",
			existingDoc:     true,
			existingDocID:   "no_db_import_filter-custom_import_filter-doc_body-existing_doc-docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				Error: "Bad Request",
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:            "db_import_filter-custom_import_filter-doc_body-existing_doc-docid",
			dbImportFilter:  "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			importFilter:    "function(doc) { if (doc.user.num) { return true; } else { return false; } }",
			document:        map[string]interface{}{"user": map[string]interface{}{"num": 23}},
			docID:           "db_import_filter-custom_import_filter-doc_body-existing_doc-docid",
			existingDoc:     true,
			existingDocID:   "db_import_filter-custom_import_filter-doc_body-existing_doc-docid",
			existingDocBody: `{"user":{"num":125}}`,
			expectedOutput: ImportFilterDryRun{
				Error: "Bad Request",
			},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			RequireStatus(t, rt.SendAdminRequest("PUT", "/{{.keyspace}}/_config/import_filter", test.dbImportFilter), http.StatusOK)

			url := "/{{.keyspace}}/_import_filter"
			if test.existingDoc {
				ver := rt.PutDoc(test.existingDocID, test.existingDocBody)
				rt.WaitForVersion(test.existingDocID, ver)
			}
			if test.docID != "" {
				url += "?doc_id=" + test.docID
			}
			bodyMap := make(map[string]interface{})
			if test.importFilter != "" {
				bodyMap["import_filter"] = test.importFilter
			}
			if test.document != nil {
				bodyMap["doc"] = test.document
			}
			bodyBytes, _ := json.Marshal(bodyMap)
			resp := rt.SendDiagnosticRequest("POST", url, string(bodyBytes))
			RequireStatus(t, resp, test.expectedStatus)

			var output ImportFilterDryRun
			err := json.Unmarshal(resp.Body.Bytes(), &output)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedOutput, output)
		})
	}
}
