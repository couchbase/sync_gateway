/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functions

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"

	"github.com/graphql-go/graphql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The GraphQL schema:
var kTestGraphQLSchema = `
	type Task {
		id: ID!
		title: String!
		description: String
		done: Boolean
		tags: [String!]
		secretNotes: String		# Admin-only
	}
	type Query {
		square(n: Int!): Int!
		infinite: Int!
		task(id: ID!): Task
		tasks: [Task!]!
		toDo: [Task!]!
	}
	type Mutation {
		complete(id: ID!): Task
		addTag(id: ID!, tag: String!): Task
	}
`

// The GraphQL configuration:
var kTestGraphQLConfig = GraphQLConfig{
	Schema: &kTestGraphQLSchema,
	Resolvers: map[string]GraphQLResolverConfig{
		"Query": {
			"square": {
				Type:  "javascript",
				Code:  `function(parent, args, context, info) {return args.n * args.n;}`,
				Allow: allowAll,
			},
			"infinite": {
				Type:  "javascript",
				Code:  `function(parent, args, context, info) {return context.user.function("infinite");}`,
				Allow: allowAll,
			},
			"task": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 1) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						return context.user.function("getTask", {id: args.id});}`,
				Allow: allowAll,
			},
			"tasks": {
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
			"toDo": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						if (Object.keys(parent).length != 0) throw "Unexpected parent";
						if (Object.keys(args).length != 0) throw "Unexpected args";
						if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
						if (!context.user) throw "Missing context.user";
						if (!context.admin) throw "Missing context.admin";
						var result=new Array(); var all = context.user.function("all");
						for (var i = 0; i < all.length; i++)
							if (!all[i].done) result.push(all[i]);
						return result;}`,
				Allow: allowAll,
			},
		},
		"Mutation": {
			"complete": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
							if (Object.keys(parent).length != 0) throw "Unexpected parent";
							if (Object.keys(args).length != 1) throw "Unexpected args";
							if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
							if (!context.user) throw "Missing context.user";
							if (!context.admin) throw "Missing context.admin";
							context.requireMutating();
							var task = context.user.function("getTask", {id: args.id});
							if (!task) return undefined;
							task.done = true;
							return task;}`,
				Allow: allowAll,
			},
			"addTag": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
							context.requireMutating();
							var task = context.user.function("getTask", {id: args.id});
							if (!task) return undefined;
							if (!task.tags) task.tags = [];
							task.tags.push(args.tag);
							return task;}`,
				Allow: allowAll,
			},
		},
		"Task": {
			"secretNotes": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
								if (!parent.id) throw "Invalid parent";
								if (Object.keys(args).length != 0) throw "Unexpected args";
								if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
								if (!context.user) throw "Missing context.user";
								if (!context.admin) throw "Missing context.admin";
								return "TOP SECRET!";}`,
				Allow: &Allow{Users: base.Set{}}, // only admins
			},
		},
	},
}

// JS function helpers:
var kTestGraphQLUserFunctionsConfig = FunctionsConfig{
	Definitions: FunctionsDefs{
		"all": {
			Type: "javascript",
			Code: `function(context, args) {
						return [
						{id: "a", "title": "Applesauce", done:true, tags:["fruit","soft"]},
						{id: "b", "title": "Beer", description: "Bass ale please"},
						{id: "m", "title": "Mangoes"} ];}`,
			Allow: &Allow{Channels: []string{"*"}},
		},
		"getTask": {
			Type: "javascript",
			Code: `function(context, args, parent, info) {
						var all = context.user.function("all");
						for (var i = 0; i < all.length; i++)
							if (all[i].id == args.id) return all[i];
						return undefined;}`,
			Args:  []string{"id"},
			Allow: &Allow{Channels: []string{"*"}},
		},
		"infinite": {
			Type: "javascript",
			Code: `function(context, args, parent, info) {
				var result = context.user.graphql("query{ infinite }");
				if (result.errors) throw "GraphQL query failed:" + result.errors[0].message;
				return -1;}`,
			Allow: &Allow{Channels: []string{"*"}},
		},
	},
}

var kTestTypenameResolverSchema = `interface Book {
	id: ID!
}
type Textbook implements Book {
	id: ID!
	courses: [String!]!
}
type ColoringBook implements Book {
	id: ID!
	colors: [String!]!
}
type Query {
	books: [Book!]!
}`

var kTestTypenameResolverQuery = `
query {
	books {
		id
		... on Textbook {
			courses
		}
		... on ColoringBook{
			colors
		}
	}
}
`

func assertGraphQLResult(t *testing.T, expected string, result *graphql.Result, err error) {
	if !assert.NoError(t, err) || !assert.NotNil(t, result) {
		return
	}
	if !assert.Zerof(t, len(result.Errors), "Unexpected GraphQL errors: %v", result.Errors) {
		for _, err := range result.Errors {
			t.Logf("\t%v", err)
			t.Logf("\t\t%T %#v", err.OriginalError(), err.OriginalError())
		}
		return
	}
	j, err := json.Marshal(result.Data)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(j))
}

// Per the spec, GraphQL errors are not indicated via `err`, rather through an `errors`
// property in the `result` object.
func assertGraphQLError(t *testing.T, expectedErrorText string, result *graphql.Result, err error) {
	if !assert.NoError(t, err) || !assert.NotNil(t, result) {
		return
	}
	if len(result.Errors) == 0 {
		data, err := json.Marshal(result.Data)
		assert.NoError(t, err)
		assert.NotZero(t, len(result.Errors), "Expected GraphQL error but got none; data is %s", string(data))
		return
	}
	for _, err := range result.Errors {
		if strings.Contains(err.Error(), expectedErrorText) {
			return
		}
	}
	t.Logf("GraphQL error did not contain expected string %q: actually %#v", expectedErrorText, result.Errors)
	t.Fail()
}

// Unit test for GraphQL queries.
func TestUserGraphQL(t *testing.T) {
	// FIXME : this test doesn't work because the access view does not exist on the collection ???
	t.Skip("Skipping test until access view is available with collections")

	// base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db, ctx := setupTestDBWithFunctions(t, &kTestGraphQLUserFunctionsConfig, &kTestGraphQLConfig)
	defer db.Close(ctx)

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserGraphQLAsAdmin(t, ctx, db) })

	// Now create a user and make it current:
	db.SetUser(addUserAlice(t, db))
	assert.True(t, db.User().RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserGraphQLAsUser(t, ctx, db) })
}

func testUserGraphQLCommon(t *testing.T, ctx context.Context, db *db.Database) {
	// Successful query:
	result, err := db.UserGraphQLQuery(`query{ square(n: 12) }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"square":144}`, result, err)

	result, err = db.UserGraphQLQuery(`query($num:Int!){ square(n: $num) }`, "",
		map[string]any{"num": 12}, false, ctx)
	assertGraphQLResult(t, `{"square":144}`, result, err)

	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {id,title,done} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"task":{"done":true,"id":"a","title":"Applesauce"}}`, result, err)

	result, err = db.UserGraphQLQuery(`query{ tasks {title} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"tasks":[{"title":"Applesauce"},{"title":"Beer"},{"title":"Mangoes"}]}`, result, err)

	// ERRORS:

	// Nonexistent query:
	result, err = db.UserGraphQLQuery(`query{ bogus(id:"a") {id} }`, "", nil, false, ctx)
	assertGraphQLError(t, "Cannot query field \"bogus\" on type \"Query\"", result, err)

	// Invalid argument:
	result, err = db.UserGraphQLQuery(`query{ task(foo:69) {id,title,done} }`, "", nil, false, ctx)
	assertGraphQLError(t, "Unknown argument \"foo\"", result, err)

	// Mutation when no mutations allowed (mutationAllowed = false):
	result, err = db.UserGraphQLQuery(`mutation{ complete(id:"b") {done} }`, "", nil, false, ctx)
	assertGraphQLError(t, "403", result, err)

	// Infinite regress:
	result, err = db.UserGraphQLQuery(`query{ infinite }`, "", nil, false, ctx)
	assertGraphQLError(t, "508", result, err)
}

func testUserGraphQLAsAdmin(t *testing.T, ctx context.Context, db *db.Database) {
	testUserGraphQLCommon(t, ctx, db)

	// Admin tests updating "a":
	result, err := db.UserGraphQLQuery(`mutation{ addTag(id:"a", tag:"cold") {id,title,done,tags} }`, "", nil, true, ctx)
	assertGraphQLResult(t, `{"addTag":{"done":true,"id":"a","tags":["fruit","soft","cold"],"title":"Applesauce"}}`, result, err)

	// Admin-only field:
	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"task":{"secretNotes":"TOP SECRET!"}}`, result, err)
}

func testUserGraphQLAsUser(t *testing.T, ctx context.Context, db *db.Database) {
	testUserGraphQLCommon(t, ctx, db)

	// Regular user tests updating "m":
	result, err := db.UserGraphQLQuery(`mutation{ addTag(id:"m", tag:"ripe") {id,title,done,tags} }`, "", nil, true, ctx)
	assertGraphQLResult(t, `{"addTag":{"done":null,"id":"m","tags":["ripe"],"title":"Mangoes"}}`, result, err)

	// ERRORS:

	// Can't get admin-only field:
	result, err = db.UserGraphQLQuery(`query{ task(id:"a") {secretNotes} }`, "", nil, false, ctx)
	assertGraphQLError(t, "403", result, err)
}

// ////// GRAPHQL N1QL RESOLVER TESTS

// The GraphQL configuration, using N1QL in some resolvers:
var kTestGraphQLConfigWithN1QL = GraphQLConfig{
	Schema: &kTestGraphQLSchema,
	Resolvers: map[string]GraphQLResolverConfig{
		"Query": {
			"square": {
				Type:  "javascript",
				Code:  `function(parent, args, context, info) {return args.n * args.n;}`,
				Allow: allowAll,
			},
			"infinite": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
						var result = context.user.graphql("query{ infinite }");
						if (result.errors) throw "GraphQL query failed: " + result.errors[0].message;
						return -1;}`,
				Allow: allowAll,
			},
			"task": {
				// This tests the ability of the resolver to return the 1st result row when the
				// GraphQL return type is not a List.
				Type:  "query",
				Code:  `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE meta().id = $args.id AND type = "task"`,
				Allow: allowAll,
			},
			"tasks": {
				Type:  "query",
				Code:  `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE type = "task" ORDER BY title`,
				Allow: allowAll,
			},
			"toDo": {
				Type:  "query",
				Code:  `SELECT db.*, meta().id as id FROM $_keyspace AS db WHERE type = "task" AND NOT done ORDER BY title`,
				Allow: allowAll,
			},
		},
		"Mutation": {
			"complete": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
					var task = context.user.defaultCollection.get(args.id);
					if (!task) return null;
					task.id = args.id;
					if (!task.done) {
					  task.done = true;
					  context.user.defaultCollection.save(task, args.id);
					}
					return task;}`,
				Allow: allowAll,
			},
			"addTag": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
							var task = context.user.defaultCollection.get(args.id);
							if (!task) return null;
							task.id = args.id;
							if (!task.tags) task.tags = [];
							task.tags.push(args.tag);
							context.user.defaultCollection.save(task, args.id);
							return task;}`,
				Allow: allowAll,
			},
		},
		"Task": {
			"secretNotes": {
				Type: "javascript",
				Code: `function(parent, args, context, info) {
								if (!parent.id) throw "Invalid parent";
								if (Object.keys(args).length != 0) throw "Unexpected args";
								if (Object.keys(info) != "selectedFieldNames") throw "Unexpected info";
								if (!context.user) throw "Missing context.user";
								if (!context.admin) throw "Missing context.admin";
								return "TOP SECRET!";}`,
				Allow: &Allow{Users: base.Set{}}, // only admins
			},
			"description": {
				// This tests the magic $parent parameter,
				// and returning the single column of the single row when the result is a scalar.
				Type:  "query",
				Code:  `SELECT $parent.description`,
				Allow: allowAll,
			},
		},
	},
}

type Body = db.Body

// Unit test for GraphQL queries.
func TestUserGraphQLWithN1QL(t *testing.T) {
	if base.TestsUseNamedCollections() {
		t.Skip("Skipping test because collections are enabled")
	}
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only (requires N1QL)")
	}

	db, ctx := setupTestDBWithFunctions(t, nil, &kTestGraphQLConfigWithN1QL)
	defer db.Close(ctx)

	collection, err := db.GetDefaultDatabaseCollectionWithUser()
	require.NoError(t, err)
	_, _, _ = collection.Put(ctx, "a", Body{"type": "task", "title": "Applesauce", "done": true, "tags": []string{"fruit", "soft"}, "channels": "wonderland"})
	_, _, _ = collection.Put(ctx, "b", Body{"type": "task", "title": "Beer", "description": "Bass ale please", "channels": "wonderland"})
	_, _, _ = collection.Put(ctx, "m", Body{"type": "task", "title": "Mangoes", "channels": "wonderland"})

	n1qlStore, ok := base.AsN1QLStore(db.Bucket.DefaultDataStore())
	require.True(t, ok)

	createdPrimaryIdx := createPrimaryIndex(t, n1qlStore)

	if createdPrimaryIdx {
		defer func() {
			err := n1qlStore.DropIndex(base.TestCtx(t), base.PrimaryIndexName)
			require.NoError(t, err)
		}()

	}
	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserGraphQLAsAdmin(t, ctx, db) })

	// Now create a user and make it current:
	db.SetUser(addUserAlice(t, db))
	require.True(t, db.User().RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserGraphQLAsUser(t, ctx, db) })

	// Test the N1QL resolver that uses $parent:
	result, err := db.UserGraphQLQuery(`query{ task(id:"b") {description,title} }`, "", nil, false, ctx)
	assertGraphQLResult(t, `{"task":{"description":"Bass ale please","title":"Beer"}}`, result, err)
}

func TestGraphQLMaxSchemaSize(t *testing.T) {
	var schema = `
	type Task {
		id: ID!
		title: String!
		description: String
		done: Boolean
		tags: [String!]
		secretNotes: String		# Admin-only
	}`
	var config = GraphQLConfig{
		MaxSchemaSize: base.IntPtr(20),
		Schema:        &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": {
					Type: "javascript",
					Code: `function(context,args) {return args.n * args.n;}`,
				},
			},
		},
	}
	_, err := CompileGraphQL(base.TestCtx(t), &config)
	assert.ErrorContains(t, err, "GraphQL schema too large (> 20 bytes)")
}

func TestGraphQLMaxResolverCount(t *testing.T) {
	var schema = `
	type Task {
		id: ID!
		title: String!
		description: String
		done: Boolean
		tags: [String!]
		secretNotes: String		# Admin-only
	}`
	var config = GraphQLConfig{
		MaxResolverCount: base.IntPtr(1),
		Schema:           &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": {
					Type: "javascript",
					Code: `function(context,args) {return args.n * args.n;}`,
				},
			},
			"Mutation": {
				"complete": {
					Type: "javascript",
					Code: `function(context, args, parent, info) { }`,
				},
			},
		},
	}
	_, err := CompileGraphQL(base.TestCtx(t), &config)
	assert.ErrorContains(t, err, "too many GraphQL resolvers (> 1)")
}

func TestArgsInResolverConfig(t *testing.T) {
	var config = GraphQLConfig{
		Schema: base.StringPtr(`type Query{}`),
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": {
					Type: "javascript",
					Code: `function(parent, args, context, info) {return args.n * args.n;}`,
					Args: []string{"n"},
				},
			},
		},
	}
	_, err := CompileGraphQL(base.TestCtx(t), &config)
	assert.ErrorContains(t, err, `'args' is not valid in a GraphQL resolver config`)
}

func TestUnresolvedTypesInSchema(t *testing.T) {
	var config = GraphQLConfig{
		Schema:    base.StringPtr(`type Query{} type abc{def:kkk}`),
		Resolvers: nil,
	}
	_, err := CompileGraphQL(base.TestCtx(t), &config)
	assert.ErrorContains(t, err, `GraphQL Schema object has no registered TypeMap -- this probably means the schema has unresolved types`)
}

func TestInvalidMutationType(t *testing.T) {
	t.Run("Unrecognized type cpp", func(t *testing.T) {
		var config = GraphQLConfig{
			Schema: base.StringPtr(`type Query{}`),
			Resolvers: map[string]GraphQLResolverConfig{
				"Mutation": {
					"complete": {
						Type: "cpp",
						Code: `{}`,
					},
				},
			},
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.ErrorContains(t, err, `unrecognized 'type' "cpp"`)
	})
	t.Run("Unrecognized type query", func(t *testing.T) {
		var config = GraphQLConfig{
			Schema: base.StringPtr(`type Query{}`),
			Resolvers: map[string]GraphQLResolverConfig{
				"Mutation": {
					"complete": {
						Type: "query",
						Code: `SELECT 1;`,
					},
				},
			},
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.ErrorContains(t, err, `GraphQL mutations must be implemented in JavaScript`)
	})
}

func TestCompilationErrorInResolverCode(t *testing.T) {
	var config = GraphQLConfig{
		Schema: base.StringPtr(`type Query{ square(n: Int!): Int! }`),
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": {
					Type: "javascript",
					Code: `function(parent, args, context, info, 3) { }`,
				},
			},
		},
	}
	_, err := CompileGraphQL(base.TestCtx(t), &config)
	assert.ErrorContains(t, err, `500 Error compiling GraphQL resolver "Query:square"`)
}

func TestGraphQLMaxCodeSize(t *testing.T) {
	var schema = `type Query {square(n: Int!) : Int!}`
	var config = GraphQLConfig{
		MaxCodeSize: base.IntPtr(2),
		Schema:      &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": {
					Type: "javascript",
					Code: `function(parent, args, context, info) {return args.n * args.n;}`,
				},
			},
		},
	}
	_, err := CompileGraphQL(base.TestCtx(t), &config)
	assert.ErrorContains(t, err, "resolver square code too large (> 2 bytes)")
}

// Unit Test for Typename resolver for interfaces in GraphQL schema
func TestTypenameResolver(t *testing.T) {
	t.Run("Typename Resolver must be a Javascript Function", func(t *testing.T) {
		var config = GraphQLConfig{
			Schema: &kTestTypenameResolverSchema,
			Resolvers: map[string]GraphQLResolverConfig{
				"Book": {
					"__typename": {
						Type: "cpp",
						Code: `function(context, value) {
								switch (value.type) {
								  case "textbook": return "Textbook";
								  case "coloringBook": return "ColoringBook";
								  default: return null;
								}
							  }`,
					},
				},
			},
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.ErrorContains(t, err, "a GraphQL '__typename__' resolver must be JavaScript")
	})
	t.Run("Error in compiling typename resolver", func(t *testing.T) {
		var config = GraphQLConfig{
			Schema: &kTestTypenameResolverSchema,
			Resolvers: map[string]GraphQLResolverConfig{
				"Book": {
					"__typename": {
						Type: "javascript",
						Code: `function(context, value, 3) {
								switch (value.type) {
								  default: return null;
								}
							  }`,
					},
				},
			},
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.ErrorContains(t, err, `Error compiling GraphQL type-name resolver "Book"`)
	})
	t.Run("Typename Resolver should not have allow", func(t *testing.T) {
		var config = GraphQLConfig{
			Schema: &kTestTypenameResolverSchema,
			Resolvers: map[string]GraphQLResolverConfig{
				"Book": {
					"__typename": {
						Type: "javascript",
						Code: `function(context, value) {
								switch (args.type) {
								  case "textbook": return "Textbook";
								  case "coloringBook": return "ColoringBook";
								  default: return null;
								}
							  }`,
						Allow: allowAll,
					},
				},
			},
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.ErrorContains(t, err, "'allow' is not valid in a GraphQL '__typename__' resolver")
	})

	t.Run("Correct Schema and Query produces the result", func(t *testing.T) {
		var config = GraphQLConfig{
			Schema: &kTestTypenameResolverSchema,
			Resolvers: map[string]GraphQLResolverConfig{
				"Book": {
					"__typename": {
						Type: "javascript",
						Code: `function(context, value) {
								switch (value.type) {
								  case "textbook": return "Textbook";
								  case "coloringBook": return "ColoringBook";
								  default:        return null;
								}
							  }`,
					},
				},
				"Query": {
					"books": {
						Type: "javascript",
						Code: `function(parent, args, context, info) {return [{"id":"abc", "courses":["science"], "type": "textbook"},{"id":"efg", "colors":["red"], "type": "coloringBook"}] }`,
					},
				},
			},
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.NoError(t, err)
		db, ctx := setupTestDBWithFunctions(t, nil, &config)
		defer db.Close(ctx)
		result, err := db.UserGraphQLQuery(kTestTypenameResolverQuery, "", nil, false, ctx)
		assertGraphQLResult(t, `{"books":[{"courses":["science"],"id":"abc"},{"colors":["red"],"id":"efg"}]}`, result, err)
	})
}

// Unit Tests for Invalid Schema/SchemaFile in the getSchema Function
func TestInvalidSchemaAndSchemaFile(t *testing.T) {
	t.Run("Both Schema and SchemaFile are provided", func(t *testing.T) {
		var config = GraphQLConfig{
			Schema:     base.StringPtr(`type Query{ square(n: Int!): Int! }`),
			SchemaFile: base.StringPtr("someInvalidPath/someInvalidFileName"),
			Resolvers:  nil,
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.ErrorContains(t, err, "GraphQL config: only one of `schema` and `schemaFile` may be used")
	})

	t.Run("Neither Schema nor SchemaFile Provided", func(t *testing.T) {
		var config = GraphQLConfig{
			Resolvers: nil,
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		assert.ErrorContains(t, err, "GraphQL config: either `schema` or `schemaFile` must be defined")
	})

	t.Run("cannot read SchemaFile", func(t *testing.T) {
		var config = GraphQLConfig{
			SchemaFile: base.StringPtr("dummySchemaFile.txt"),
		}
		_, err := CompileGraphQL(base.TestCtx(t), &config)
		fmt.Println(err)
		assert.ErrorContains(t, err, "can't read file")
	})
}

// Unit Tests for Valid SchemaFile in the getSchema Function
func TestValidSchemaFile(t *testing.T) {
	t.Run("Only SchemaFile is Provided", func(t *testing.T) {
		validSchema := "type Query {sum(n: Int!) : Int!}"
		err := os.WriteFile("schema.graphql", []byte(validSchema), 0666)
		assert.NoError(t, err)
		var config = GraphQLConfig{
			SchemaFile: base.StringPtr("schema.graphql"),
		}
		_, err = CompileGraphQL(base.TestCtx(t), &config)
		assert.NoError(t, err)

		err = os.Remove("schema.graphql")
		assert.NoError(t, err)
	})
}

func TestFixOfCVE_2022_37315(t *testing.T) {
	// Tests fix of CVE 2022-37315, reported as https://github.com/graphql-go/graphql/issues/637
	// Without the fix, this test panics:
	// 		runtime: goroutine stack exceeds 1000000000-byte limit
	// 		runtime: sp=0xc0209c03c8 stack=[0xc0209c0000, 0xc0409c0000]
	// 		fatal error: stack overflow

	var schema = `String r`
	var config = GraphQLConfig{
		Schema: &schema,
		Resolvers: map[string]GraphQLResolverConfig{
			"Query": {
				"square": {
					Type: "javascript",
					Code: `function(parent, args, context, info) {return args.n * args.n;}`,
				},
			},
		},
	}
	_, err := CompileGraphQL(base.TestCtx(t), &config)
	assert.ErrorContains(t, err, `Syntax Error GraphQL (1:1) Unexpected Name "String"`)
}
