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
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"strings"
	"testing"
)

type grant interface {
	request(rt *RestTester)
}

const defaultKeyspace = "_default._default"
const fakeUpdatedTime = 1234

// convertUpdatedTimeToConstant replaces the updated_at field in the response with a constant value to be able to diff
func convertUpdatedTimeToConstant(t testing.TB, output []byte) string {
	var channelMap allChannels
	err := json.Unmarshal(output, &channelMap)
	require.NoError(t, err)
	for keyspace, channels := range channelMap.Channels {
		for channelName, grant := range channels {
			if grant.UpdatedAt == 0 {
				continue
			}
			grant.UpdatedAt = fakeUpdatedTime
			channelMap.Channels[keyspace][channelName] = grant
		}
	}

	fmt.Printf("Converted response: %s\n", string(base.MustJSONMarshal(t, channelMap)))
	return string(base.MustJSONMarshal(t, channelMap))
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
	if g.output != "" {
		compareAllChannelsOutput(rt, g.user, g.output)
	}
}

type roleGrant struct {
	role          string
	adminChannels map[string][]string
}

func (g roleGrant) getPayload(t testing.TB) string {
	config := auth.PrincipalConfig{
		//Name:     base.StringPtr(g.role),
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

type docGrant struct {
	userName        string
	dynamicRoles    string
	dynamicChannels string
	output          string
}

func (g docGrant) getPayload(t testing.TB) string {
	role := fmt.Sprintf(`"role":"role:%s",`, g.dynamicRoles)
	if g.dynamicRoles == "" {
		role = ""
	}
	user := fmt.Sprintf(`"user":["%s"],`, g.userName)
	if g.userName == "" {
		user = ""
	}
	payload := fmt.Sprintf(`{%s %s "channel":"%s"}`, user, role, g.dynamicChannels)

	return payload
}

func (g docGrant) request(rt *RestTester) {
	payload := g.getPayload(rt.TB)
	rt.TB.Logf("Issuing dynamic grant: %+v", payload)
	response := rt.SendAdminRequest(http.MethodPut, "/{{.db}}/doc", payload)
	if response.Code != http.StatusCreated && response.Code != http.StatusOK {
		rt.TB.Fatalf("Expected 200 or 201 exit code")
	}
	if g.output != "" {
		compareAllChannelsOutput(rt, g.userName, g.output)
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
						defaultKeyspace: {"A", "B", "C"},
					},
					output: `
{"all_channels":{"_default._default": {
		"A": { "entries" : ["1-0"], "updated_at":0},
		"B": { "entries" : ["1-0"], "updated_at":0},
		"C": { "entries" : ["1-0"], "updated_at":0}
	}}}`,
				}}},
		{
			name: "multiple history entries",
			grants: []grant{
				// grant 1
				userGrant{
					user: "alice",
					adminChannels: map[string][]string{
						defaultKeyspace: {"A"},
					},
					output: `
{"all_channels":{"_default._default": {
		"A": { "entries" : ["1-0"], "updated_at":0}
	}}}`,
				},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
					output: `
{"all_channels":{"_default._default": {
		"A": { "entries" : ["1-2"], "updated_at":1234}
	}}}`,
				},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
					output: `
{"all_channels":{"_default._default": {
		"A": { "entries" : ["1-2", "3-0"], "updated_at":1234}
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
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 2
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 3
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 4
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 5
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 6
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 7
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 8
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 9
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 10
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 11
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 12
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 13
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 14
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 15
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 16
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 17
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 18
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 19
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 20
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 19
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				// grant 20
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"!"}},
				},
				// grant 23
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
					output: `
				{"all_channels":{"_default._default": {
						"A": { "entries" : ["1-4", "5-6", "7-8", "9-10", "11-12","13-14","15-16","17-18","19-20","21-22","23-0"], "updated_at":1234}
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
					adminChannels: map[string][]string{defaultKeyspace: {"A", "B"}},
				},
				userGrant{
					user:  "alice",
					roles: []string{"role1"},
					output: `
				{"all_channels":{"_default._default": {
						"A": { "entries" : ["2-0"], "updated_at":0},
						"B": { "entries" : ["2-0"], "updated_at":0}
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
					userName:        "alice",
					dynamicChannels: "A",
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["2-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "dynamic role grant channels",
			grants: []grant{
				userGrant{
					user: "alice",
				},
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: {"A", "B"}},
				},
				docGrant{
					userName:        "alice",
					dynamicRoles:    "role1",
					dynamicChannels: "chan1",
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["3-0"], "updated_at":0},
						"B": { "entries" : ["3-0"], "updated_at":0},
						"chan1": { "entries" : ["3-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic and admin grants, assert earlier sequence (admin) is used",
			grants: []grant{
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				docGrant{
					userName:        "alice",
					dynamicChannels: "A",
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["1-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic and admin grants, assert earlier sequence (dynamic) is used",
			grants: []grant{
				userGrant{
					user: "alice",
				},
				docGrant{
					userName:        "alice",
					dynamicChannels: "A",
				},
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["2-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both admin and admin role grants, assert earlier sequence (admin) is used",
			grants: []grant{
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				userGrant{
					user:  "alice",
					roles: []string{"role1"},
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["1-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both admin and admin role grants, assert earlier sequence (admin role) is used",
			grants: []grant{
				userGrant{
					user: "alice",
				},
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				userGrant{
					user:  "alice",
					roles: []string{"role1"},
				},
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["3-0"], "updated_at":0}
					}}}`, // A start sequence would be 4 if its broken
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin grants, assert earlier sequence (dynamic role) is used",
			grants: []grant{
				userGrant{
					user: "alice",
				},
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				docGrant{
					userName:        "alice",
					dynamicRoles:    "role1",
					dynamicChannels: "docChan",
				},
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["3-0"], "updated_at":0},
						"docChan": { "entries" : ["3-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin grants, assert earlier sequence (admin) is used",
			grants: []grant{
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				docGrant{
					userName:        "alice",
					dynamicRoles:    "role1",
					dynamicChannels: "docChan",
				},
				userGrant{
					user:          "alice",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["1-0"], "updated_at":0},
						"docChan": { "entries" : ["3-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin role grants, assert earlier sequence (dynamic role) is used",
			grants: []grant{
				userGrant{
					user: "alice",
				},
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				roleGrant{
					role:          "role2",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				docGrant{
					userName:        "alice",
					dynamicRoles:    "role1",
					dynamicChannels: "docChan",
				},
				userGrant{
					user:  "alice",
					roles: []string{"role2"},
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["4-0"], "updated_at":0},
						"docChan": { "entries" : ["4-0"], "updated_at":0}
					}}}`,
				},
			},
		},
		{
			name: "channel assigned through both dynamic role and admin role grants, assert earlier sequence (admin role) is used",
			grants: []grant{
				userGrant{
					user: "alice",
				},
				roleGrant{
					role:          "role1",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				roleGrant{
					role:          "role2",
					adminChannels: map[string][]string{defaultKeyspace: {"A"}},
				},
				userGrant{
					user:  "alice",
					roles: []string{"role2"},
				},
				docGrant{
					userName:        "alice",
					dynamicRoles:    "role1",
					dynamicChannels: "docChan",
					output: `
					{"all_channels":{"_default._default": {
						"A": { "entries" : ["4-0"], "updated_at":0},
						"docChan": { "entries" : ["5-0"], "updated_at":0}
					}}}`,
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
			dbConfig.Sync = base.StringPtr(`function(doc) {channel(doc.channel); access(doc.user, doc.channel); role(doc.user, doc.role);}`)
			rt.CreateDatabase("db", dbConfig)

			// create user with adminChannels1
			for i, grant := range test.grants {
				t.Logf("Processing grant %d", i+1)
				grant.request(rt)
			}

		})
	}
}

func TestGetAllChannelsByUserWithSingleNamedCollection(t *testing.T) {
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
	require.NoError(t, rt.TestBucket.CreateDataStore(base.TestCtx(t), newCollection))
	defer func() {
		require.NoError(t, rt.TestBucket.DropDataStore(newCollection))
	}()

	resp = rt.UpsertDbConfig(dbName, DbConfig{Scopes: ScopesConfig{
		base.DefaultScope: {Collections: CollectionsConfig{
			base.DefaultCollection:         {},
			newCollection.CollectionName(): {},
		}},
	}})
	RequireStatus(t, resp, http.StatusCreated)

	// Test that the keyspace with no channels assigned does not have a key in the response
	grant := userGrant{
		user:          "alice",
		adminChannels: map[string][]string{defaultKeyspace: {}, newCollection.String(): {"D"}},
		output: fmt.Sprintf(`
		{
			"all_channels":{
				"%s":{
					"D":{
						"entries":["1-0"],
						"updated_at":0
					}
				}
			}}`, newCollection.String()),
	}
	grant.request(rt)

	// check single named collection is handled
	grant = userGrant{
		user:          "alice",
		adminChannels: map[string][]string{defaultKeyspace: {"A"}, newCollection.String(): {"D"}},
		output: fmt.Sprintf(`
{
   "all_channels":{
      "_default._default":{
         "A":{
            "entries":["2-0"],
            "updated_at":0
         }
      },
      "%s":{
         "D":{
            "entries":[
               "1-0"
            ],
            "updated_at":0
         }
      }
   }
}`, newCollection.String()),
	}
	grant.request(rt)

}

func TestGetAllChannelsByUserWithMultiCollections(t *testing.T) {
	base.TestRequiresCollections(t)

	rt := NewRestTesterMultipleCollections(t, &RestTesterConfig{PersistentConfig: true}, 2)
	defer rt.Close()

	dbName := "db"
	tb := base.GetTestBucket(t)
	defer tb.Close(rt.Context())
	scopesConfig := GetCollectionsConfig(t, tb, 2)

	dbConfig := makeDbConfig(tb.GetName(), dbName, scopesConfig)
	rt.CreateDatabase("db", dbConfig)

	scopeName := rt.GetDbCollections()[0].ScopeName
	collection1Name := rt.GetDbCollections()[0].Name
	collection2Name := rt.GetDbCollections()[1].Name
	scopesConfig[scopeName].Collections[collection1Name] = &CollectionConfig{}
	keyspace1 := scopeName + "." + collection1Name
	keyspace2 := scopeName + "." + collection2Name

	// Test that the keyspace with no channels assigned does not have a key in the response
	grant := userGrant{
		user:          "alice",
		adminChannels: map[string][]string{keyspace1: {"D"}},
		output: fmt.Sprintf(`
		{
			"all_channels":{
				"%s":{
					"D":{
						"entries":["1-0"],
						"updated_at":0
					}
				}
			}}`, keyspace1),
	}
	grant.request(rt)

	// check single named collection is handled
	grant = userGrant{
		user:          "alice",
		adminChannels: map[string][]string{keyspace2: {"A"}, keyspace1: {"D"}},
		output: fmt.Sprintf(`
		{
		   "all_channels":{
			  "%s":{
				 "A":{
					"entries":["2-0"],
					"updated_at":0
				 }
			  },
			  "%s":{
				 "D":{
					"entries":[
					   "1-0"
					],
					"updated_at":0
				 }
			  }
		   }
		}`, keyspace2, keyspace1),
	}
	grant.request(rt)

	// check removed channel in keyspace2 is in history before deleting collection 2
	grant = userGrant{
		user:          "alice",
		adminChannels: map[string][]string{keyspace1: {"D"}, keyspace2: {"!"}},
		output: fmt.Sprintf(`
		{
		   "all_channels":{
			  "%s":{
				 "A":{
					"entries":["2-3"],
					"updated_at":1234
				 }
			  },
			  "%s":{
				 "D":{
					"entries":["1-0"],
					"updated_at":0
				 }
			  }
		   }
		}`, keyspace2, keyspace1),
	}
	grant.request(rt)

	// delete collection 2
	scopesConfig = GetCollectionsConfig(t, tb, 1)
	dbConfig = makeDbConfig(tb.GetName(), dbName, scopesConfig)
	rt.UpsertDbConfig("db", dbConfig)

	// check deleted collection is not there
	grant = userGrant{
		user:          "alice",
		adminChannels: map[string][]string{keyspace1: {"D"}},
		output: fmt.Sprintf(`
		{
		   "all_channels":{
			  "%s":{
				 "D":{
					"entries":[
					   "1-0"
					],
					"updated_at":0
				 }
			  }
		   }
		}`, keyspace1),
	}
	grant.request(rt)
}

func TestGetAllChannelsByUserDeletedRole(t *testing.T) {

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = nil
	rt.CreateDatabase("db", dbConfig)

	// Create role with 1 channel and assign it to user
	roleGrant := roleGrant{role: "role1", adminChannels: map[string][]string{defaultKeyspace: {"role1Chan"}}}
	userGrant := userGrant{
		user:  "alice",
		roles: []string{"role1"},
		output: `
		{
			"all_channels":{
				"_default._default":{
					"role1Chan":{
						"entries":["2-0"],
						"updated_at":0
					}
				}
			}}`,
	}
	roleGrant.request(rt)
	userGrant.request(rt)

	resp := rt.SendAdminRequest("DELETE", "/db/_role/role1", ``)
	RequireStatus(t, resp, http.StatusOK)

	// Delete role and assert its channels no longer appear in response
	userGrant.output = `{}`
	userGrant.roles = []string{}
	userGrant.request(rt)

}

func TestGetAllChannelsByUserNonexistentAndDeletedUser(t *testing.T) {

	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = nil
	rt.CreateDatabase("db", dbConfig)
	// assert the endpoint returns 404 when user is not found
	resp := rt.SendDiagnosticRequest("GET", "/db/_user/user1/_all_channels", ``)
	RequireStatus(t, resp, http.StatusNotFound)

	// Create user and assert on response
	userGrant := userGrant{
		user:          "user1",
		adminChannels: map[string][]string{defaultKeyspace: {"A"}},
		output: `
		{
			"all_channels":{
				"_default._default":{
					"A":{
						"entries":["1-0"],
						"updated_at":0
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
