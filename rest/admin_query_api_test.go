//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//////// ALL USER QUERY APIS:

// When feature flag is not enabled, all API calls return 404:
func TestUserQueryDBConfigGetWithoutFeatureFlag(t *testing.T) {
	ctx := base.TestCtx(t)
	config := bootstrapStartupConfigForTest(t)
	err := config.SetupAndValidateLogging(ctx)
	assert.NoError(t, err)
	rt := NewRestTester(t, &RestTesterConfig{EnableUserQueries: false})
	defer rt.Close()

	t.Run("Functions, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All Functions", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Single Function", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})

	t.Run("Queries, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/queries", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All Queries", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Single Query", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})

	t.Run("GraphQL, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("GraphQL", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

// Test use of "Etag" and "If-Match" headers to safely update function/query/graphql config.
func TestUserQueryDBConfigMVCC(t *testing.T) {
	base.LongRunningTest(t)

	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"xxx": {
				SourceCode: "return 42;",
			},
		},
		UserQueries: map[string]*db.UserQueryConfig{
			"xxx": {
				Statement: "SELECT 42",
			},
		},
		GraphQL: &db.GraphQLConfig{
			Schema:    base.StringPtr(kDummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	runTest := func(t *testing.T, uri string, newValue string) {
		// Get initial etag:
		response := rt.SendAdminRequest("GET", uri, "")
		assert.Equal(t, 200, response.Result().StatusCode)
		etag := response.HeaderMap.Get("Etag")
		assert.Regexp(t, `"[^"]+"`, etag)

		// Update config, just to change its etag:
		response = rt.SendAdminRequest("PUT", uri, newValue)
		assert.Equal(t, 200, response.Result().StatusCode)
		newEtag := response.HeaderMap.Get("Etag")
		assert.Regexp(t, `"[^"]+"`, newEtag)
		assert.NotEqual(t, etag, newEtag)

		// A GET should also return the new etag:
		response = rt.SendAdminRequest("GET", uri, "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, newEtag, response.HeaderMap.Get("Etag"))

		// Try to update using If-Match with the old etag:
		headers := map[string]string{"If-Match": etag}
		response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
		assert.Equal(t, 412, response.Result().StatusCode)

		// Now update successfully using the current etag:
		headers["If-Match"] = newEtag
		response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
		assert.Equal(t, 200, response.Result().StatusCode)
		newestEtag := response.HeaderMap.Get("Etag")
		assert.Regexp(t, `"[^"]+"`, newestEtag)
		assert.NotEqual(t, etag, newestEtag)
		assert.NotEqual(t, newEtag, newestEtag)

		// Try to delete using If-Match with the previous etag:
		response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
		assert.Equal(t, 412, response.Result().StatusCode)

		// Now delete successfully using the current etag:
		headers["If-Match"] = newestEtag
		response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
		assert.Equal(t, 200, response.Result().StatusCode)
	}

	t.Run("Function", func(t *testing.T) {
		runTest(t, "/db/_config/functions/xxx", `{
			"javascript": "return 69;"
		}`)
	})

	t.Run("Functions", func(t *testing.T) {
		runTest(t, "/db/_config/functions", `{
			"yyy": {"javascript": "return 69;"}
		}`)
	})

	t.Run("Query", func(t *testing.T) {
		runTest(t, "/db/_config/queries/xxx", `{
			"statement": "select 69"
		}`)
	})

	t.Run("Queries", func(t *testing.T) {
		runTest(t, "/db/_config/queries", `{
			"yyy": {"statement": "select 69"}
		}`)
	})

	t.Run("GraphQL", func(t *testing.T) {
		runTest(t, "/db/_config/graphql", `{
			"schema": "type Query {square(n: Int!) : Int!}", "resolvers":{}
		}`)
	})
}

//////// JAVASCRIPT FUNCTIONS:

// When there's no existing config, API calls return 404 or empty objects:
func TestUserQueryDBConfigGetMissing(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		var body db.UserFunctionConfigMap
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, 0, len(body))
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
func TestUserQueryDBConfigGet(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"square": {
				SourceCode: "return args.numero * args.numero;",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		var body db.UserFunctionConfigMap
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.NotNil(t, body["square"])
	})
	t.Run("Single", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/square", "")
		var body db.UserFunctionConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, "return args.numero * args.numero;", body.SourceCode)
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/bogus", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestUserQueryDBConfigPut(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"square": {
				SourceCode: "return args.numero * args.numero;",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/functions", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/functions", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions", `{
			"sum": {"javascript": "return args.numero + args.numero;",
					"parameters": ["numero"],
					"allow": {"channels": ["*"]}}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions["sum"])
		assert.Nil(t, rt.GetDatabase().Options.UserFunctions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, "26", string(response.BodyBytes()))

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("DeleteAll", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Equal(t, 0, len(rt.GetDatabase().Options.UserFunctions))

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestUserQueryDBConfigPutOne(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"square": {
				SourceCode: "return args.numero * args.numero;",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/functions/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/function/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Bogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `[]`)
		assert.Equal(t, 400, response.Result().StatusCode)
		response = rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{"ruby": "foo"}`)
		assert.Equal(t, 400, response.Result().StatusCode)
	})
	t.Run("Add", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/sum", `{
			"javascript": "return args.numero + args.numero;",
			"parameters": ["numero"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		assert.Equal(t, "26", string(response.BodyBytes()))
	})
	t.Run("ReplaceOne", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{
			"javascript": "return -args.n * args.n;",
			"parameters": ["n"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		assert.Equal(t, "-169", string(response.BodyBytes()))
	})
	t.Run("DeleteOne", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions/square", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.UserFunctions["square"])
		assert.Equal(t, 1, len(rt.GetDatabase().Options.UserFunctions))

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

//////// N1QL QUERIES

func TestDBConfigUserQueryGetEmpty(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/queries", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries", "")
		var body db.UserQueryMap
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, 0, len(body))
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
func TestDBConfigUserQueryGet(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserQueries: map[string]*db.UserQueryConfig{
			"square": {
				Statement:  "SELECT $$numero * $$numero",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
			"xxx": {
				Statement: "SELECT 42",
			},
			"yyy": {
				Statement: "SELECT 23",
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/queries", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries", "")
		var body db.UserQueryMap
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.NotNil(t, body["square"])
		assert.NotNil(t, body["xxx"])
		assert.NotNil(t, body["yyy"])
	})
	t.Run("Single", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries/square", "")
		var body db.UserQueryConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, "SELECT $numero * $numero", body.Statement)
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries/bogus", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestDBConfigUserQueryPut(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserQueries: map[string]*db.UserQueryConfig{
			"square": {
				Statement:  "SELECT $$numero * $$numero",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
			"xxx": {
				Statement: "SELECT 42",
			},
			"yyy": {
				Statement: "SELECT 23",
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/queries", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/queries", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Bogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/queries", `[]`)
		assert.Equal(t, 400, response.Result().StatusCode)
		response = rt.SendAdminRequest("PUT", "/db/_config/queries", `{
			"sum": {"StaTEmént": "SELECT $numero + $numero"}
		}`)
		assert.Equal(t, 400, response.Result().StatusCode)
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/queries", `{
			"sum": {"statement": "SELECT $numero + $numero",
					"parameters": ["numero"],
					"allow": {"channels": ["*"]}}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Equal(t, 1, len(rt.GetDatabase().Options.UserQueries))
		assert.NotNil(t, rt.GetDatabase().Options.UserQueries["sum"])

		if base.UnitTestUrlIsWalrus() {
			t.Skip("This test is Couchbase Server only (requires N1QL)")
		}
		response = rt.SendAdminRequest("GET", "/db/_query/sum?numero=13", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, "[{\"$1\":26}\n]\n", string(response.BodyBytes()))

		response = rt.SendAdminRequest("GET", "/db/_query/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("DeleteAll", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/queries", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Equal(t, 0, len(rt.GetDatabase().Options.UserQueries))

		response = rt.SendAdminRequest("GET", "/db/_query/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestDBConfigUserQueryPutOne(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		UserQueries: map[string]*db.UserQueryConfig{
			"square": {
				Statement:  "SELECT $$numero * $$numero",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/queries/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/queries/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Bogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/queries/square", `[]`)
		assert.Equal(t, 400, response.Result().StatusCode)
		response = rt.SendAdminRequest("PUT", "/db/_config/queries/square", `{"StaTEmént": "SELECT $numero + $numero"}`)
		assert.Equal(t, 400, response.Result().StatusCode)
	})
	t.Run("Add", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/queries/sum", `{
			"statement": "SELECT $numero + $numero",
			"parameters": ["numero"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserQueries["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserQueries["square"])

		if base.UnitTestUrlIsWalrus() {
			t.Skip("This test is Couchbase Server only (requires N1QL)")
		}
		response = rt.SendAdminRequest("GET", "/db/_query/sum?numero=13", "")
		assert.Equal(t, "[{\"$1\":26}\n]\n", string(response.BodyBytes()))
	})
	t.Run("ReplaceOne", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/queries/square", `{
			"statement": "SELECT -$n * $n",
			"parameters": ["n"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserQueries["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserQueries["square"])

		if base.UnitTestUrlIsWalrus() {
			t.Skip("This test is Couchbase Server only (requires N1QL)")
		}
		response = rt.SendAdminRequest("GET", "/db/_query/square?n=13", "")
		assert.Equal(t, "[{\"$1\":-169}\n]\n", string(response.BodyBytes()))
	})
	t.Run("DeleteOne", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/queries/square", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.UserQueries["square"])
		assert.Equal(t, 1, len(rt.GetDatabase().Options.UserQueries))

		response = rt.SendAdminRequest("GET", "/db/_query/square?n=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

//////// GRAPHQL

const kDummyGraphQLSchema = `
	type Query {
		square(n: Int!) : Int!
	}`

func TestDBConfigUserGraphQLGetEmpty(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
func TestDBConfigUserGraphQLGet(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		GraphQL: &db.GraphQLConfig{
			Schema:    base.StringPtr(kDummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		var body db.GraphQLConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, base.StringPtr(kDummyGraphQLSchema), body.Schema)
	})
}

func TestDBConfigUserGraphQLPut(t *testing.T) {
	rt := newRestTesterForUserQueries(t, DbConfig{
		GraphQL: &db.GraphQLConfig{
			Schema:    base.StringPtr(kDummyGraphQLSchema),
			Resolvers: nil,
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/graphql", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/graphql", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("ReplaceBogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "obviously not a valid schema ^_^"
		}`)
		assert.Equal(t, 400, response.Result().StatusCode)
	})
	t.Run("Replace", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/graphql", `{
			"schema": "type Query {sum(n: Int!) : Int!}",
			"resolvers": {
				"Query": {
					"sum": "return args.n + args.n;"
				}
			}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ sum(n:3) }"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, `{"data":{"sum":6}}`, string(response.BodyBytes()))
	})
	t.Run("Delete", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/graphql", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.GraphQL)

		response = rt.SendAdminRequest("POST", "/db/_graphql", `{"query": "query{ sum(n:3) }"}`)
		assert.Equal(t, 503, response.Result().StatusCode)
	})
}

//////// UTILITIES:

// Creates a new RestTester using persistent config, and a database "db".
// Only the user-query-related fields are copied from `queryConfig`; the rest are ignored.
func newRestTesterForUserQueries(t *testing.T, queryConfig DbConfig) *RestTester {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires persistent configs")
		return nil
	}

	rt := NewRestTester(t, &RestTesterConfig{
		groupID:           base.StringPtr(t.Name()), // Avoids race conditions between tests
		EnableUserQueries: true,
		persistentConfig:  true,
	})

	_ = rt.Bucket() // initializes the bucket as a side effect
	dbConfig := dbConfigForTestBucket(rt.TestBucket)
	dbConfig.UserFunctions = queryConfig.UserFunctions
	dbConfig.UserQueries = queryConfig.UserQueries
	dbConfig.GraphQL = queryConfig.GraphQL

	resp, err := rt.CreateDatabase("db", dbConfig)
	if !assert.NoError(t, err) || !AssertStatus(t, resp, 201) {
		rt.Close()
		t.FailNow()
		return nil // (never reached)
	}
	return rt
}
