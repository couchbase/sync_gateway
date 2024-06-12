//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/require"
)

type grant interface {
	request(rt *RestTester)
}

func compareAllChannelsOutput(rt *RestTester, username string, expectedOutput string) {
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.db}}/_user/"+username+"/_all_channels", ``)
	RequireStatus(rt.TB, response, http.StatusOK)
	rt.TB.Logf("All channels response: %s", response.BodyString())
	require.JSONEq(rt.TB, rt.mustTemplateResource(expectedOutput), response.BodyString())
}

func compareUserDocSeqsOutput(rt *RestTester, username string, docids []string, expectedOutput string) {
	response := rt.SendDiagnosticRequest(http.MethodGet,
		"/{{.keyspace}}/_user/"+username+"?docids="+strings.Join(docids, ","), ``)
	RequireStatus(rt.TB, response, http.StatusOK)
	rt.TB.Logf("All channels response: %s", response.BodyString())
	require.JSONEq(rt.TB, rt.mustTemplateResource(expectedOutput), response.BodyString())
}

type userGrant struct {
	user          string
	adminChannels map[string][]string
	docIDs        []string
	docUserTest   bool
	roles         []string
	output        string
}

func (g *userGrant) getUserPayload(rt *RestTester) string {
	config := auth.PrincipalConfig{
		Name:     base.StringPtr(g.user),
		Password: base.StringPtr(RestTesterDefaultUserPassword),
	}
	if len(g.roles) > 0 {
		config.ExplicitRoleNames = base.SetOf(g.roles...)
	}

	for keyspace, chans := range g.adminChannels {
		_, scope, collection, err := ParseKeyspace(rt.mustTemplateResource(keyspace))
		require.NoError(rt.TB, err)
		if scope == nil && collection == nil {
			config.ExplicitChannels = base.SetFromArray(chans)
			continue
		}
		require.NotNil(rt.TB, scope, "Could not find scope from keyspace %s", keyspace)
		require.NotNil(rt.TB, collection, "Could not find collection from keyspace %s", keyspace)
		if base.IsDefaultCollection(*scope, *collection) {
			config.ExplicitChannels = base.SetFromArray(chans)
		} else {
			config.SetExplicitChannels(*scope, *collection, chans...)
		}
	}

	return string(base.MustJSONMarshal(rt.TB, config))
}

func (g userGrant) request(rt *RestTester) {
	payload := g.getUserPayload(rt)
	rt.TB.Logf("Issuing admin grant: %+v", payload)
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+g.user, payload)
	if response.Code != http.StatusCreated && response.Code != http.StatusOK {
		rt.TB.Fatalf("Expected 200 or 201 exit code")
	}
	if g.output != "" {
		if g.docUserTest {
			compareUserDocSeqsOutput(rt, g.user, g.docIDs, g.output)
		} else {
			compareAllChannelsOutput(rt, g.user, g.output)
		}
	}
}

type roleGrant struct {
	role          string
	adminChannels map[string][]string
}

func (g roleGrant) getPayload(rt *RestTester) string {
	config := auth.PrincipalConfig{
		Password: base.StringPtr(RestTesterDefaultUserPassword),
	}
	for keyspace, chans := range g.adminChannels {
		_, scope, collection, err := ParseKeyspace(rt.mustTemplateResource(keyspace))
		require.NoError(rt.TB, err)
		if scope == nil && collection == nil {
			config.ExplicitChannels = base.SetFromArray(chans)
			continue
		}
		require.NotNil(rt.TB, scope, "Could not find scope from keyspace %s", keyspace)
		require.NotNil(rt.TB, collection, "Could not find collection from keyspace %s", keyspace)
		if base.IsDefaultCollection(*scope, *collection) {
			config.ExplicitChannels = base.SetFromArray(chans)
		} else {
			config.SetExplicitChannels(*scope, *collection, chans...)
		}
	}
	return string(base.MustJSONMarshal(rt.TB, config))
}

func (g roleGrant) request(rt *RestTester) {
	payload := g.getPayload(rt)
	rt.TB.Logf("Issuing admin grant: %+v", payload)
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_role/"+g.role, payload)
	if response.Code != http.StatusCreated && response.Code != http.StatusOK {
		rt.TB.Fatalf("Expected 200 or 201 exit code")
	}
}

type docGrant struct {
	userName       string
	dynamicRole    string
	docIDs         []string
	docID          string
	docUserTest    bool
	dynamicChannel string
	output         string
}

func (g docGrant) getPayload() string {
	role := fmt.Sprintf(`"role":"role:%s",`, g.dynamicRole)
	if g.dynamicRole == "" {
		role = ""
	}
	user := fmt.Sprintf(`"user":["%s"],`, g.userName)
	if g.userName == "" {
		user = ""
	}
	payload := fmt.Sprintf(`{%s %s "channel":"%s"}`, user, role, g.dynamicChannel)

	return payload
}

func (g docGrant) request(rt *RestTester) {
	payload := g.getPayload()
	docID := "doc"
	if g.docID != "" {
		docID = g.docID
	}
	rt.TB.Logf("Issuing dynamic grant: %+v", payload)
	response := rt.SendAdminRequest(http.MethodPut, fmt.Sprintf("/{{.keyspace}}/%s", docID), payload)
	if response.Code != http.StatusCreated && response.Code != http.StatusOK {
		rt.TB.Fatalf("Expected 200 or 201 exit code, got %d, output: %s", response.Code, response.Body.String())
	}
	if g.output != "" {
		if g.docUserTest {
			compareUserDocSeqsOutput(rt, g.userName, g.docIDs, g.output)
		} else {
			compareAllChannelsOutput(rt, g.userName, g.output)
		}
	}
}

func TestGetAllChannelsByUser(t *testing.T) {
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
				}}},
		{
			name: "multiple history entries",
			grants: []grant{
				// grant 1
				userGrant{
					user: "alice",
					adminChannels: map[string][]string{
						"{{.keyspace}}": {"A"},
					},
					output: `
{"all_channels":{"{{.scopeAndCollection}}": {
		"A": { "entries" : ["1-0"]}
	}}}`,
				},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"!"}},
					output: `
{"all_channels":{"{{.scopeAndCollection}}": {
		"A": { "entries" : ["1-2"]}
	}}}`,
				},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					output: `
{"all_channels":{"{{.scopeAndCollection}}": {
		"A": { "entries" : ["1-2", "3-0"]}
	}}}`,
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
					output: `
				{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["1-4", "5-6", "7-8", "9-10", "11-12","13-14","15-16","17-18","19-20","21-22","23-0"]}
					}}}`,
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
				userGrant{
					user:  "alice",
					roles: []string{"role1"},
					output: `
				{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["2-0"]},
						"B": { "entries" : ["2-0"]}
					}}}`,
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
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["2-0"]}
					}}}`,
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
					dynamicChannel: "chan1",
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["3-0"]},
						"B": { "entries" : ["3-0"]},
						"chan1": { "entries" : ["3-0"]}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic and admin grants, assert earlier sequence (admin) is used",
			grants: []grant{
				// create user and assign channels through admin_channels
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// assign channels through sync fn and assert on sequences
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
		{
			name: "channel assigned through both dynamic and admin grants, assert earlier sequence (dynamic) is used",
			grants: []grant{
				// create user with no channels
				userGrant{
					user: "alice",
				},
				// create doc and assign dynamic chan through sync fn
				docGrant{
					userName:       "alice",
					dynamicChannel: "A",
				},
				// assign same channels through admin_channels and assert on sequences
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["2-0"]}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both admin and admin role grants, assert earlier sequence (admin) is used",
			grants: []grant{
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
					user:  "alice",
					roles: []string{"role1"},
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["1-0"]}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both admin and admin role grants, assert earlier sequence (admin role) is used",
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
				// assign role through admin_roles
				userGrant{
					user:  "alice",
					roles: []string{"role1"},
				},
				// assign role channel through admin_channels and assert on sequences
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["3-0"]}
					}}}`, // A start sequence would be 4 if its broken
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
					dynamicChannel: "docChan",
				},
				// assign role cahnnel to user through admin_channels and assert on sequences
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["3-0"]},
						"docChan": { "entries" : ["3-0"]}
					}}}`,
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
					dynamicRole:    "role1",
					dynamicChannel: "docChan",
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["1-0"]},
						"docChan": { "entries" : ["3-0"]}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin role grants, assert earlier sequence (dynamic role) is used",
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
				// create another role with same channel
				roleGrant{
					role:          "role2",
					adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
				},
				// assign first role through sync fn
				docGrant{
					userName:       "alice",
					dynamicRole:    "role1",
					dynamicChannel: "docChan",
				},
				// assign second role through admin_roles and assert sequence is from dynamic (first) role
				userGrant{
					user:  "alice",
					roles: []string{"role2"},
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["4-0"]},
						"docChan": { "entries" : ["4-0"]}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin role grants, assert earlier sequence (admin role) is used",
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
					userName:       "alice",
					dynamicRole:    "role2",
					dynamicChannel: "docChan",
					output: `
					{"all_channels":{"{{.scopeAndCollection}}": {
						"A": { "entries" : ["4-0"]},
						"docChan": { "entries" : ["5-0"]}
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

func TestGetAllChannelsByUserWithSingleNamedCollection(t *testing.T) {
	base.TestRequiresCollections(t)

	bucket := base.GetTestBucket(t)
	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true, CustomTestBucket: bucket}, 1)
	defer rt.Close()

	// add single named collection
	newCollection := base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: t.Name()}
	require.NoError(t, bucket.CreateDataStore(base.TestCtx(t), newCollection))
	defer func() {
		require.NoError(t, rt.TestBucket.DropDataStore(newCollection))
	}()

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = ScopesConfig{
		base.DefaultScope: {Collections: CollectionsConfig{
			base.DefaultCollection:         {},
			newCollection.CollectionName(): {},
		}},
	}
	RequireStatus(t, rt.CreateDatabase("db", dbConfig), http.StatusCreated)

	// Test that the keyspace with no channels assigned does not have a key in the response
	grant := userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace1}}": {},
			"{{.keyspace2}}": {"D"}},
		output: `
		{
			"all_channels":{
				"{{.scopeAndCollection2}}":{
					"D":{
						"entries":["1-0"]
					}
				}
			}}`,
	}
	grant.request(rt)

	// add channel to single named collection and assert its handled
	grant = userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace1}}": {"A"},
		},
		output: `
		{
		   "all_channels":{
			  "{{.scopeAndCollection1}}":{
				 "A":{
					"entries":["2-0"]
				 }
			  },
			  "{{.scopeAndCollection2}}":{
				 "D":{
					"entries":[
					   "1-0"
					]
				 }
			  }
		   }
		}`,
	}
	grant.request(rt)

}

func TestGetAllChannelsByUserWithMultiCollections(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true}, 2)
	defer rt.Close()

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)

	// Test that the keyspace with no channels assigned does not have a key in the response
	grant := userGrant{
		user:          "alice",
		adminChannels: map[string][]string{"{{.keyspace1}}": {"D"}},
		output: `
		{
			"all_channels":{
				"{{.scopeAndCollection1}}":{
					"D":{
						"entries":["1-0"]
					}
				}
			}}`,
	}
	grant.request(rt)

	// add channel to collection with no channels and assert multi collection is handled
	grant = userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace2}}": {"A"},
		},
		output: `
		{
		   "all_channels":{
			  "{{.scopeAndCollection2}}":{
				 "A":{
					"entries":["2-0"]
				 }
			  },
			  "{{.scopeAndCollection1}}":{
				 "D":{
					"entries":[
					   "1-0"
					]
				 }
			  }
		   }
		}`,
	}
	grant.request(rt)

	// check removed channel in keyspace2 is in history before deleting collection 2
	grant = userGrant{
		user: "alice",
		adminChannels: map[string][]string{
			"{{.keyspace1}}": {"D"},
			"{{.keyspace2}}": {"!"},
		},
		output: `
		{
		   "all_channels":{
			  "{{.scopeAndCollection2}}":{
				 "A":{
					"entries":["2-3"]
				 }
			  },
			  "{{.scopeAndCollection1}}":{
				 "D":{
					"entries":["1-0"]
				 }
			  }
		   }
		}`,
	}
	grant.request(rt)

	// delete collection 2
	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = GetCollectionsConfig(t, rt.TestBucket, 1)
	RequireStatus(t, rt.UpsertDbConfig("db", dbConfig), http.StatusCreated)

	// check deleted collection is not there
	grant = userGrant{
		user:          "alice",
		adminChannels: map[string][]string{"{{.keyspace}}": {"D"}},
		output: `
		{
		   "all_channels":{
			  "{{.scopeAndCollection}}":{
				 "D":{
					"entries":[
					   "1-0"
					]
				 }
			  }
		   }
		}`,
	}
	grant.request(rt)
}

func TestGetAllChannelsByUserDeletedRole(t *testing.T) {
	if !base.TestUseCouchbaseServer() {
		t.Skip("Requires Couchbase Server")
	}
	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// Create role with 1 channel and assign it to user
	roleGrant := roleGrant{role: "role1", adminChannels: map[string][]string{"{{.keyspace}}": {"role1Chan"}}}
	roleGrant.request(rt)
	userGrant := userGrant{
		user:  "alice",
		roles: []string{"role1"},
		output: `
		{
			"all_channels":{
				"{{.scopeAndCollection}}":{
					"role1Chan":{
						"entries":["2-0"]
					}
				}
			}}`,
	}
	userGrant.request(rt)

	// Delete role and assert its channels no longer appear in response
	resp := rt.SendAdminRequest("DELETE", "/db/_role/role1", ``)
	RequireStatus(t, resp, http.StatusOK)

	userGrant.output = `{"all_channels":{"_default._default":{"!":{"entries":["2-3"]},"role1Chan":{"entries":["2-3"]}}}}`
	userGrant.roles = []string{}
	userGrant.request(rt)

}

func TestGetAllChannelsByUserNonexistentAndDeletedUser(t *testing.T) {

	rt := NewRestTesterPersistentConfig(t)
	defer rt.Close()

	// assert the endpoint returns 404 when user is not found
	resp := rt.SendDiagnosticRequest("GET", "/db/_user/user1/_all_channels", ``)
	RequireStatus(t, resp, http.StatusNotFound)

	// Create user and assert on response
	userGrant := userGrant{
		user:          "user1",
		adminChannels: map[string][]string{"{{.keyspace}}": {"A"}},
		output: `
		{
			"all_channels":{
				"{{.scopeAndCollection}}":{
					"A":{
						"entries":["1-0"]
					}
				}
			}}`,
	}
	userGrant.request(rt)

	// delete user
	resp = rt.SendAdminRequest("DELETE", "/db/_user/user1", ``)
	RequireStatus(t, resp, http.StatusOK)

	// Get deleted user all channels, expect 404
	resp = rt.SendDiagnosticRequest("GET", "/db/_user/user1/_all_channels", ``)
	RequireStatus(t, resp, http.StatusNotFound)
}
