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

func TestDBConfigUserFunctionGetNone(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		var body db.UserFunctionMap
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, 0, len(body))
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
func TestDBConfigUserFunctionGet(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"square": {
				SourceCode: "return args.numero * args.numero;",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	}}

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		var body db.UserFunctionMap
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

func TestDBConfigUserFunctionPut(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"square": {
				SourceCode: "return args.numero * args.numero;",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	}}

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

func TestDBConfigUserFunctionPutOne(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		UserFunctions: map[string]*db.UserFunctionConfig{
			"square": {
				SourceCode: "return args.numero * args.numero;",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	}}

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/function/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/function/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
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
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
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
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		UserQueries: map[string]*db.UserQueryConfig{
			"square": {
				Statement:  "SELECT $numero * $numero",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	}}

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/queries", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/queries", "")
		var body db.UserQueryMap
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.NotNil(t, body["square"])
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
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		UserQueries: map[string]*db.UserQueryConfig{
			"square": {
				Statement:  "SELECT $numero * $numero",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	}}

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/queries", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/queries", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/queries", `{
			"sum": {"statement": "SELECT $numero + $numero",
					"parameters": ["numero"],
					"allow": {"channels": ["*"]}}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserQueries["sum"])
		assert.Nil(t, rt.GetDatabase().Options.UserQueries["square"])

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
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		UserQueries: map[string]*db.UserQueryConfig{
			"square": {
				Statement:  "SELECT $numero * $numero",
				Parameters: []string{"numero"},
				Allow:      &db.UserQueryAllow{Channels: []string{"wonderland"}},
			},
		},
	}}

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/query/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/query/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
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
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		var body db.GraphQLConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Nil(t, body.Schema)
		assert.Nil(t, body.SchemaFile)
		assert.Nil(t, body.Resolvers)
	})
}
func TestDBConfigUserGraphQLGet(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	schema := kDummyGraphQLSchema
	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		GraphQL: &db.GraphQLConfig{
			Schema:    &schema,
			Resolvers: nil,
		},
	}}

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/graphql", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/graphql", "")
		var body db.GraphQLConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, &schema, body.Schema)
	})
}

func TestDBConfigUserGraphQLPut(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{guestEnabled: true})
	defer rt.Close()

	schema := kDummyGraphQLSchema
	rt.DatabaseConfig = &DatabaseConfig{DbConfig: DbConfig{
		GraphQL: &db.GraphQLConfig{
			Schema:    &schema,
			Resolvers: nil,
		},
	}}

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
