// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package functionsapitest

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const dummyGraphQLSchema = `
    type Query {
        square(n: Int!) : Int!
    }`

var allowAll = &functions.Allow{Channels: []string{"*"}}

// The GraphQL schema:
const testGraphQLSchema = `
	type User {
		id: ID! #Int
		name: String!
		Emails: [String!]! #Mutation via Admin Only
	}
	type Query {
		getUser(id: ID!): User
		getEmails(name: String!): User
		getAllUsers: [User!]!
	}
	type Mutation {
		updateName(id:ID!, name: String!): User
		addEmail(id:ID!, email: String!): User  #can be done via admin only
	}

`

// The GraphQL configuration:
var testGraphQLConfig = functions.GraphQLConfig{
	Schema: base.StringPtr(testGraphQLSchema),
	Resolvers: map[string]functions.GraphQLResolverConfig{
		"Query": {
			"getUser": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("getUserWithID", {id: args.id});}`,
				Allow: allowAll,
			},
			"getEmails": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("getEmailsWithName", {name: args.name});}`,
				Allow: allowAll,
			},
			"getAllUsers": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 0) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("all");}`,
				Allow: allowAll,
			},
		},
		"Mutation": {
			"updateName": {
				Type: "javascript",
				Code: `function(parent, args, context, info){
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 2) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						
						var currentUser = context.user.function("getUserWithID", {id: args.id});
						if (!currentUser) return undefined;
						currentUser.name = args.name;
						return currentUser;
				}`,
				Allow: allowAll,
			},
			"addEmail": {
				Type: "javascript",
				Code: `function(parent, args, context, info){
						//if (!parent.id) throw "Invalid parent";
						if (Object.keys(args).length != 2) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						context.requireMutating();
						var currentUser = context.user.function("getUserWithID", {id: args.id});
						if (!currentUser) return undefined;

						//case: args.email already present in the Emails array
						
						for (var i = 0; i < currentUser.Emails.length; i++) {
							console.log(currentUser.Emails[i]);
							if(currentUser.Emails[i]==args.email){
								return currentUser;
							}
						}
						currentUser.Emails.push(args.email);
						return currentUser;
				}`,

				Allow: &functions.Allow{Users: base.Set{}}, // only admins
			},
		},
	},
}

// JS function helpers:
var testGraphQLUserFunctionsConfig = functions.FunctionsConfig{
	Definitions: functions.FunctionsDefs{
		"all": {
			Type: "javascript",
			Code: `function(context, args) {
                        return [
                        {id: 1,"name": "user1", Emails: ["abc@gmail.com"]},
                        {id: 2,"name": "user2", Emails: ["xyz@gmail.com","def@gmail.com"]},
                        {id: 3,"name": "user3", Emails: ["ipo@gmail.com"]} ];}`,
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
		"getUserWithID": {
			Type: "javascript",
			Code: `function(context, args) {
                        var all = context.user.function("all");
                        for (var i = 0; i < all.length; i++)
                            if (all[i].id == args.id) return all[i];
                        return undefined;}`,
			Args:  []string{"id"},
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
		"getEmailsWithName": {
			Type: "javascript",
			Code: `function(context, args) {
                        var all = context.user.function("all");
                        for (var i = 0; i < all.length; i++)
                            if (all[i].name == args.name) return all[i];
                        return undefined;}`,
			Args:  []string{"name"},
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
	},
}

// When feature flag is not enabled, all API calls return 404:
func TestFunctionsConfigGetWithoutFeatureFlagGraphQL(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{EnableUserQueries: false})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("GraphQL, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 404)
	})
	t.Run("GraphQL", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 404)
	})
}

// This will be used both by functions and graphQL
func runTestFunctionsConfigMVCC(t *testing.T, rt *rest.RestTester, uri string, newValue string) {
	// Get initial etag:
	response := rt.SendAdminRequest("GET", uri, "")
	rest.AssertStatus(t, response, 200)
	etag := response.Header().Get("Etag")
	assert.Regexp(t, `"[^"]+"`, etag)

	// Update config, just to change its etag:
	response = rt.SendAdminRequest("PUT", uri, newValue)
	rest.AssertStatus(t, response, 200)
	newEtag := response.Header().Get("Etag")
	assert.Regexp(t, `"[^"]+"`, newEtag)
	assert.NotEqual(t, etag, newEtag)

	// A GET should also return the new etag:
	response = rt.SendAdminRequest("GET", uri, "")
	rest.AssertStatus(t, response, 200)
	assert.Equal(t, newEtag, response.Header().Get("Etag"))

	// Try to update using If-Match with the old etag:
	headers := map[string]string{"If-Match": etag}
	response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
	rest.AssertStatus(t, response, 412)

	// Now update successfully using the current etag:
	headers["If-Match"] = newEtag
	response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
	rest.AssertStatus(t, response, 200)
	newestEtag := response.Header().Get("Etag")
	assert.Regexp(t, `"[^"]+"`, newestEtag)
	assert.NotEqual(t, etag, newestEtag)
	assert.NotEqual(t, newEtag, newestEtag)

	// Try to delete using If-Match with the previous etag:
	response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
	rest.AssertStatus(t, response, 412)

	// Now delete successfully using the current etag:
	headers["If-Match"] = newestEtag
	response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
	rest.AssertStatus(t, response, 200)
}

// Test use of "Etag" and "If-Match" headers to safely update graphql config.
func TestFunctionsConfigMVCCGraphQL(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(dummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("GraphQL", func(t *testing.T) {
		runTestFunctionsConfigMVCC(t, rt, "/db/_config/graphql", `{
			"schema": "type Query {square(n: Int!) : Int!}", "resolvers":{}
		}`)
	})
}

func TestFunctionsConfigGraphQLGetEmpty(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 404)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 404)
	})
}

func TestFunctionsConfigGraphQLGet(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(testGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 404)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		var body functions.GraphQLConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, base.StringPtr(testGraphQLSchema), body.Schema)
	})
}
func TestFunctionsConfigGraphQLPut(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(testGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/graphql", "{}")
		rest.AssertStatus(t, response, 404)
		response = rt.SendRequest("DELETE", "/db/_config/graphql", "{}")
		rest.AssertStatus(t, response, 404)
	})
	t.Run("ReplaceBogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
            "schema": "obviously not a valid schema ^_^"
        }`)
		rest.AssertStatus(t, response, 400)
	})
	t.Run("Replace", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
            "schema": "type Query {sum(n: Int!) : Int!}",
            "resolvers": {
                "Query": {
                    "sum": {
                        "type": "javascript",
                        "code": "function(context,args){return args.n + args.n;}"
                    }
                }
            }
        }`)
		rest.AssertStatus(t, response, 200)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ sum(n:3) }"}`)
		rest.AssertStatus(t, response, 200)
		assert.Equal(t, `{"data":{"sum":6}}`, string(response.BodyBytes()))
	})
	t.Run("Delete", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 200)

		assert.Nil(t, rt.GetDatabase().Options.GraphQL)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ sum(n:3) }"}`)
		rest.AssertStatus(t, response, 503)
	})
}

// Test for GraphQL Valid Configuration Schema
func TestValidGraphQLConfigurationValues(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(dummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	// If max_schema_size >= given schema size then Valid
	t.Run("Check max_schema_size allowed", func(t *testing.T) {
		schema := `type Query {sum(n: Int!) : Int!}`
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", fmt.Sprintf(`{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			},
			"max_schema_size" : %d
		}`, len(schema)))
		rest.AssertStatus(t, response, 200)

		response = rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 200)
		assert.Contains(t, string(response.BodyBytes()), `"max_schema_size":32`)
	})

	// If max_resolver_count >= given number of resolvers then Valid
	// here max_resolver_count allowed is 2 and total resolvers are also 2, hence it is a valid config
	t.Run("Check max_resolver_count allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int! \n square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					},
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_resolver_count" : 2
		}`)
		rest.AssertStatus(t, response, 200)

		response = rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 200)
		assert.Contains(t, string(response.BodyBytes()), `"max_resolver_count":2`)
	})

	// If max_request_size >= length of JSON-encoded arguments passed to a function then Valid
	t.Run("Check max_request_size allowed", func(t *testing.T) {
		requestQuery := `{"query": "query($numberToBeSquared:Int!){ square(n:$numberToBeSquared) }", "variables": {"numberToBeSquared": 4}}`
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", fmt.Sprintf(`{
			"schema": "type Query {square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_request_size" : %d
		}`, len(requestQuery)))
		rest.AssertStatus(t, response, 200)

		response = rt.SendAdminRequest("POST", "/db/_graphql", requestQuery)
		rest.AssertStatus(t, response, 200)
		assert.Equal(t, `{"data":{"square":16}}`, string(response.BodyBytes()))

		response = rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 200)
		assert.Contains(t, string(response.BodyBytes()), fmt.Sprintf(`"max_request_size":%d`, len(requestQuery)))

		headerMap := map[string]string{
			"Content-type": "application/graphql",
		}
		response = rt.SendAdminRequestWithHeaders("POST", "/db/_graphql", `query{square(n:4)}`, headerMap)
		rest.AssertStatus(t, response, 200)
		assert.Equal(t, `{"data":{"square":16}}`, string(response.BodyBytes()))

		queryParam := `query($numberToBeSquared:Int!){ square(n:$numberToBeSquared) }`
		variableParam := `{"numberToBeSquared": 4}`
		getRequestURL := fmt.Sprintf("/db/_graphql?query=%s&variables=%s", queryParam, variableParam)
		response = rt.SendAdminRequest("GET", getRequestURL, "")
		rest.AssertStatus(t, response, 200)
		assert.Equal(t, `{"data":{"square":16}}`, string(response.BodyBytes()))

	})

	// only one out of the schema or schemaFile is allowed
	// here only SchemaFile is provided, hence it is a valid config
	t.Run("Provide only schema or schema file", func(t *testing.T) {
		const fileName = "schema.graphql"
		validSchema := "type Query {sum(n: Int!) : Int!}"
		tmp := t.TempDir()
		err := os.WriteFile(filepath.Join(tmp, fileName), []byte(validSchema), 0600)
		require.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schemaFile": "`+filepath.Join(tmp, fileName)+`",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			}
		}`)
		rest.AssertStatus(t, response, 200)

		response = rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		rest.AssertStatus(t, response, 200)
		assert.Contains(t, string(response.BodyBytes()), `"schemaFile":"schema.graphql"`)
	})
}

// This function checks for failure when invalid GraphQL configuration
// values are provided.
func TestInvalidGraphQLConfigurationValues(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL: &functions.GraphQLConfig{
			Schema:    base.StringPtr(dummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	// The max_schema_size allowed here is 20 bytes and the given schema is 32 bytes.
	// Hence, this will be a bad request
	t.Run("Check max_schema_size allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			},
			"max_schema_size" : 20
		}`)

		var responseMap map[string]interface{}
		err := json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)

		rest.AssertStatus(t, response, 400)
		assert.Contains(t, responseMap["reason"], "GraphQL schema too large")
		assert.Contains(t, responseMap["error"], "Bad Request")
	})

	// The max_resolver_count allowed here is 1 and we have provided
	// 2 resolvers (sum and square). Hence, this will be a bad request
	t.Run("Check max_resolver_count allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int! \n square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					},
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_resolver_count" : 1
		}`)

		var responseMap map[string]interface{}
		err := json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)

		rest.AssertStatus(t, response, 400)
		assert.Contains(t, responseMap["reason"], "too many GraphQL resolvers")
		assert.Contains(t, responseMap["error"], "Bad Request")

	})

	// The maximum length in bytes of the JSON-encoded arguments passed to
	// a function at runtime (max_request_size) allowed here is 5 but we
	// have supplied larger arguments in the POST request.
	t.Run("Check max_request_size allowed", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {square(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"square": {
						"type": "javascript",
						"code": "function(context,args){return args.n * args.n;}"
					}
				}
			},
			"max_request_size" : 5
		}`)
		rest.AssertStatus(t, response, 200)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query($numberToBeSquared:Int!){ square(n:$numberToBeSquared) }", "variables": {"numberToBeSquared": 4}}`)

		var responseMap map[string]interface{}
		err := json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)

		rest.AssertStatus(t, response, 413)
		assert.Contains(t, responseMap["reason"], "Arguments too large")
		assert.Contains(t, responseMap["error"], "Request Entity Too Large")

		headerMap := map[string]string{
			"Content-type": "application/graphql",
		}
		response = rt.SendAdminRequestWithHeaders("POST", "/db/_graphql", `query{square(n:4)}`, headerMap)
		rest.AssertStatus(t, response, 413)
		err = json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)
		assert.Contains(t, responseMap["reason"], "Arguments too large")
		assert.Contains(t, responseMap["error"], "Request Entity Too Large")

		queryParam := `query($numberToBeSquared:Int!){ square(n:$numberToBeSquared) }`
		variableParam := `{"numberToBeSquared": 4}`
		getRequestURL := fmt.Sprintf("/db/_graphql?query=%s&variables=%s", queryParam, variableParam)

		response = rt.SendAdminRequest("GET", getRequestURL, "")
		rest.AssertStatus(t, response, 200)
		err = json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)
		assert.Contains(t, responseMap["reason"], "Arguments too large")
		assert.Contains(t, responseMap["error"], "Request Entity Too Large")
	})

	// Only one out of schema and schemaFile is allowed to be present
	// but we have provided both of them. Hence, this will be a bad request.
	t.Run("Provide both schema and schema file", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"schemaFile": "someInvalidPath/someInvalidFileName",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			}
		}`)

		var responseMap map[string]interface{}
		err := json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)

		rest.AssertStatus(t, response, 400)
		assert.Contains(t, responseMap["reason"], "GraphQL config: only one of `schema` and `schemaFile` may be used")
		assert.Contains(t, responseMap["error"], "Bad Request")
	})

	// Flow: Create a file with a bogus schema --> Send the request --> Capture the error --> Delete the created file
	t.Run("Provide invalid schema file", func(t *testing.T) {
		const fileName = "schema.graphql"
		bogusSchema := "obviously not a valid schema ^_^"
		tmp := os.TempDir()
		err := os.WriteFile(filepath.Join(tmp, fileName), []byte(bogusSchema), 0600)
		require.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schemaFile": "`+filepath.Join(tmp, fileName)+`",
			"resolvers": {
				"Query": {
					"sum": {
						"type": "javascript",
						"code": "function(context,args){return args.n + args.n;}"
					}
				}
			}
		}`)
		var responseMap map[string]interface{}
		err = json.Unmarshal(response.BodyBytes(), &responseMap)
		assert.NoError(t, err)

		rest.AssertStatus(t, response, 400)
		assert.Contains(t, responseMap["reason"], "Syntax Error GraphQL")
		assert.Contains(t, responseMap["reason"], "Unexpected Name")
		assert.Contains(t, responseMap["error"], "Bad Request")
	})
}

func TestSchemaSyntax(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		GraphQL:       &testGraphQLConfig,
		UserFunctions: &testGraphQLUserFunctionsConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/graphql", "{}")
		rest.AssertStatus(t, response, 404)
		response = rt.SendRequest("DELETE", "/db/_config/graphql", "{}")
		rest.AssertStatus(t, response, 404)
	})
}
