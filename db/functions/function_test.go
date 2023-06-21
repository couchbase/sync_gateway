//go:build cb_sg_v8

/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package functions

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sg-bucket/js"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	v8 "github.com/snej/v8go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const kUserFunctionMaxCallDepth = 20

var allowAll = &Allow{Channels: []string{"*"}}

var kTestFunctionsConfig = FunctionsConfig{
	Definitions: FunctionsDefs{
		"square": &FunctionConfig{
			Type:  "javascript",
			Code:  "function(context, args) {return args.numero * args.numero;}",
			Args:  []string{"numero"},
			Allow: &Allow{Channels: []string{"wonderland"}},
		},
		"cube": &FunctionConfig{
			Type: "javascript",
			Code: `function(context, args) {let square = context.user.function("square",args);
					console.log("cube: square is", square); return square * args.numero;}`,
			Args:  []string{"numero"},
			Allow: &Allow{Channels: []string{"wonderland"}},
		},
		"exceptional": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {throw "oops";}`,
			Allow: allowAll,
		},
		"call_fn": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.user.function("square", {numero: 7});}`,
			Allow: allowAll,
		},
		"factorial": &FunctionConfig{
			Type: "javascript",
			Args: []string{"n"},
			Code: `function(context, args) {if (args.n <= 1) return 1;
						else return args.n * context.user.function("factorial", {n: args.n-1});}`,
			Allow: allowAll,
		},
		"endless": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {while(true);}`,
			Allow: allowAll,
		},
		"great_and_terrible": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return "I am OZ the great and terrible";}`,
			Allow: &Allow{Channels: []string{"oz", "narnia"}},
		},
		"call_forbidden": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.user.function("great_and_terrible");}`,
			Allow: allowAll,
		},
		"sudo_call_forbidden": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.admin.function("great_and_terrible");}`,
			Allow: allowAll,
		},
		"admin_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return "OK";}`,
			Allow: nil, // no 'allow' property means admin-only
		},
		"require_admin": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireAdmin(); return "OK";}`,
			Allow: allowAll,
		},
		"user_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {if (!context.user.name) throw "No user"; return context.user.name;}`,
			Allow: &Allow{Channels: []string{"user-${context.user.name}"}},
		},
		"alice_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireUser("alice"); return "OK";}`,
			Allow: allowAll,
		},
		"pevensies_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireUser(["peter","jane","eustace","lucy"]); return "OK";}`,
			Allow: allowAll,
		},
		"wonderland_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireAccess("wonderland"); context.requireAccess(["wonderland", "snark"]); return "OK";}`,
			Allow: allowAll,
		},
		"narnia_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireAccess("narnia"); return "OK";}`,
			Allow: allowAll,
		},
		"hero_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireRole(["hero", "antihero"]); return "OK";}`,
			Allow: allowAll,
		},
		"villain_only": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireRole(["villain"]); return "OK";}`,
			Allow: allowAll,
		},

		"getDoc": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.user.defaultCollection.get(args.docID);}`,
			Args:  []string{"docID"},
			Allow: allowAll,
		},
		"putDoc": &FunctionConfig{
			Type:     "javascript",
			Code:     `function(context, args) {return context.user.defaultCollection.save(args.doc, args.docID);}`,
			Args:     []string{"docID", "doc"},
			Mutating: true,
			Allow:    allowAll,
		},
		"delDoc": &FunctionConfig{
			Type:     "javascript",
			Code:     `function(context, args) {return context.user.defaultCollection.delete(args.docID);}`,
			Args:     []string{"docID"},
			Mutating: true,
			Allow:    allowAll,
		},

		"illegal_putDoc": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.user.function("putDoc", args);}`,
			Args:  []string{"docID", "doc"},
			Allow: allowAll,
		},

		"legal_putDoc": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.admin.function("putDoc", args);}`,
			Args:  []string{"docID", "doc"},
			Allow: allowAll,
		},

		// This fn has Mutating but calls one that doesn't
		"nested_illegal_putDoc": &FunctionConfig{
			Type:     "javascript",
			Code:     `function(context, args) {return context.user.function("illegal_putDoc", args);}`,
			Args:     []string{"docID", "doc"},
			Allow:    allowAll,
			Mutating: true,
		},

		// This fn uses context.admin to call a non-mutating function
		"admin_illegal_putDoc": &FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.admin.function("illegal_putDoc", args);}`,
			Args:  []string{"docID", "doc"},
			Allow: allowAll,
		},
	},
}

// Adds a user "alice" to the database, with role "hero"
// and access to channels "wonderland" and "lookingglass".
func addUserAlice(t *testing.T, db *db.Database) auth.User {
	authenticator := db.Authenticator(base.TestCtx(t))
	hero, err := authenticator.NewRole("hero", base.SetOf("heroes"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(hero))

	villain, err := authenticator.NewRole("villain", base.SetOf("villains"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(villain))

	user, err := authenticator.NewUser("alice", "pass", base.SetOf("wonderland", "lookingglass", "city-London", "user-alice"))
	require.NoError(t, err)
	user.SetExplicitRoles(channels.TimedSet{"hero": channels.NewVbSimpleSequence(1)}, 1)
	require.NoError(t, authenticator.Save(user), "Save")

	// Have to call GetUser to get a user object that's properly configured:
	user, err = authenticator.GetUser("alice")
	require.NoError(t, err)
	return user
}

// Unit test for JS user functions.
func TestUserFunctions(t *testing.T) {
	db, ctx := setupTestDBWithFunctions(t, &kTestFunctionsConfig, nil)
	defer db.Close(ctx)

	assert.NotNil(t, db.UserFunctions)
	assert.NotNil(t, db.UserFunctions.Definitions["square"])

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserFunctionsAsAdmin(t, ctx, db) })

	// Now create a user and make it current:
	db.SetUser(addUserAlice(t, db))
	assert.True(t, db.User().RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserFunctionsAsUser(t, ctx, db) })
}

// User function tests that work the same for admin and non-admin user:
func testUserFunctionsCommon(t *testing.T, ctx context.Context, db *db.Database) {
	// Basic call passing a parameter:
	result, err := db.CallUserFunction("square", map[string]any{"numero": 42}, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 42*42, result)

	// Function that calls a function:
	result, err = db.CallUserFunction("call_fn", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 7*7, result)

	// `requireUser` test that passes:
	result, err = db.CallUserFunction("alice_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// `requireChannel` test that passes:
	result, err = db.CallUserFunction("wonderland_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// `requireRole` test that passes:
	result, err = db.CallUserFunction("hero_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// Max call depth:
	result, err = db.CallUserFunction("factorial", map[string]any{"n": kUserFunctionMaxCallDepth}, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, 2.43290200817664e+18, result)

	// ERRORS:

	// Missing a parameter:
	_, err = db.CallUserFunction("square", nil, true, ctx)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "numero")
	assert.ErrorContains(t, err, "square")

	// Extra parameter:
	_, err = db.CallUserFunction("square", map[string]any{"numero": 42, "number": 0}, true, ctx)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "number")
	assert.ErrorContains(t, err, "square")

	// Function throws an exception:
	_, err = db.CallUserFunction("exceptional", nil, true, ctx)
	assert.ErrorContains(t, err, "oops")
	assert.ErrorContains(t, err, "exceptional")
	jserr := err.(*v8.JSError)
	assert.NotNil(t, jserr)

	// Call depth limit:
	_, err = db.CallUserFunction("factorial", map[string]any{"n": kUserFunctionMaxCallDepth + 1}, true, ctx)
	assert.ErrorContains(t, err, "User function recursion too deep")
	assert.ErrorContains(t, err, "factorial")

	// Timeout:
	briefCtx, cancelFn := context.WithTimeout(ctx, 2*time.Second)
	defer cancelFn()
	_, err = db.CallUserFunction("endless", nil, true, briefCtx)
	assert.ErrorContains(t, err, "context deadline exceeded")
}

// User-function tests, run as admin:
func testUserFunctionsAsAdmin(t *testing.T, ctx context.Context, db *db.Database) {
	testUserFunctionsCommon(t, ctx, db)

	// Admin-only (success):
	result, err := db.CallUserFunction("admin_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("require_admin", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("pevensies_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("narnia_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("villain_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// ERRORS:

	// Checking `context.user.name`:
	_, err = db.CallUserFunction("user_only", nil, true, ctx)
	assert.ErrorContains(t, err, "No user")
	jserr := err.(*v8.JSError)
	assert.NotNil(t, jserr)

	// No such function:
	_, err = db.CallUserFunction("xxxx", nil, true, ctx)
	assertHTTPError(t, err, 404)
}

// User-function tests, run as user "alice":
func testUserFunctionsAsUser(t *testing.T, ctx context.Context, db *db.Database) {
	testUserFunctionsCommon(t, ctx, db)

	// Checking `context.user.name`:
	result, err := db.CallUserFunction("user_only", nil, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, "alice", result)

	// Checking `context.admin.func`
	_, err = db.CallUserFunction("sudo_call_forbidden", nil, true, ctx)
	assert.NoError(t, err)

	// No such function:
	_, err = db.CallUserFunction("xxxx", nil, true, ctx)
	assertHTTPError(t, err, 403) // not 404 as for an admin

	_, err = db.CallUserFunction("great_and_terrible", nil, true, ctx)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("call_forbidden", nil, true, ctx)
	assertHTTPError(t, err, 403)
	assert.ErrorContains(t, err, "great_and_terrible") // failed fn name should appear in error

	_, err = db.CallUserFunction("admin_only", nil, true, ctx)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("require_admin", nil, true, ctx)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("pevensies_only", nil, true, ctx)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("narnia_only", nil, true, ctx)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("villain_only", nil, true, ctx)
	assertHTTPError(t, err, 403)
}

// Test CRUD operations
func TestUserFunctionsCRUD(t *testing.T) {
	db, ctx := setupTestDBWithFunctions(t, &kTestFunctionsConfig, nil)
	defer db.Close(ctx)

	body := map[string]any{"key": "value"}

	t.Run("Create a doc with random ID", func(t *testing.T) {
		result, err := db.CallUserFunction("putDoc", map[string]any{"docID": nil, "doc": body}, true, ctx)
		assert.NoError(t, err)
		assert.IsType(t, "", result)
		_, err = db.CallUserFunction("getDoc", map[string]any{"docID": result}, true, ctx)
		assert.NoError(t, err)
	})

	docID := "foo"

	t.Run("Missing document", func(t *testing.T) {
		result, err := db.CallUserFunction("getDoc", map[string]any{"docID": docID}, true, ctx)
		assert.NoError(t, err)
		assert.EqualValues(t, nil, result)
	})

	docParams := map[string]any{
		"docID": docID,
		"doc":   body,
	}

	t.Run("Illegal mutation", func(t *testing.T) {
		// Illegal mutation (passing mutationAllowed = false):
		_, err := db.CallUserFunction("putDoc", docParams, false, ctx)
		assertHTTPError(t, err, 403)
	})

	t.Run("Successful save as admin", func(t *testing.T) {
		result, err := db.CallUserFunction("putDoc", docParams, true, ctx)
		assert.NoError(t, err)
		assert.EqualValues(t, docID, result) // save() returns docID
	})

	t.Run("Existing document", func(t *testing.T) {
		result, err := db.CallUserFunction("getDoc", map[string]any{"docID": docID}, true, ctx)
		assert.NoError(t, err)
		revID, ok := result.(map[string]any)["_rev"].(string)
		assert.True(t, ok)
		assert.NotEmpty(t, revID)
		assert.True(t, strings.HasPrefix(revID, "1-"))
		body["_id"] = docID
		body["_rev"] = revID
		assert.EqualValues(t, body, result)
	})

	t.Run("Update document with revID", func(t *testing.T) {
		body["key2"] = 2
		_, err := db.CallUserFunction("putDoc", docParams, true, ctx)
		assert.NoError(t, err)
	})

	t.Run("Save fails with conflict", func(t *testing.T) {
		body["key3"] = 3
		body["_rev"] = "9-9999"
		result, err := db.CallUserFunction("putDoc", docParams, true, ctx)
		assert.NoError(t, err)
		assert.Nil(t, result)
	})

	t.Run("Update document without revID", func(t *testing.T) {
		body["key3"] = 4
		delete(body, "_rev")
		result, err := db.CallUserFunction("putDoc", docParams, true, ctx)
		assert.NoError(t, err)
		assert.Equal(t, docID, result)
	})

	t.Run("Get doc again to verify revision", func(t *testing.T) {
		result, err := db.CallUserFunction("getDoc", map[string]any{"docID": docID}, true, ctx)
		assert.NoError(t, err)
		revID, ok := result.(map[string]any)["_rev"].(string)
		assert.True(t, ok)
		assert.NotEmpty(t, revID)
		assert.True(t, strings.HasPrefix(revID, "3-"))
	})

	t.Run("illegal_putDoc", func(t *testing.T) {
		// Illegal mutation (a non-mutating function calling putDoc)
		_, err := db.CallUserFunction("illegal_putDoc", docParams, true, ctx)
		assertHTTPError(t, err, 403)
	})
	t.Run("nested_illegal_putDoc", func(t *testing.T) {
		// Illegal mutation (a non-mutating function calling putDoc)
		_, err := db.CallUserFunction("nested_illegal_putDoc", docParams, true, ctx)
		assertHTTPError(t, err, 403)
	})
	/*  This currently fails; it's ambiguous what should happen. TODO (jens 2-Feb-2023)
	t.Run("admin_illegal_putDoc", func(t *testing.T) {
		// Illegal mutation (a non-mutating function calling putDoc)
		_, err := db.CallUserFunction("admin_illegal_putDoc", docParams, true, ctx)
		assertHTTPError(t, err, 403)
	})
	*/
	t.Run("legal_putDoc", func(t *testing.T) {
		// Legal mutation (a non-mutating function calling putDoc, but via 'admin')
		_, err := db.CallUserFunction("legal_putDoc", docParams, true, ctx)
		assert.NoError(t, err)
	})
	t.Run("delDoc", func(t *testing.T) {
		// Delete doc:
		_, err := db.CallUserFunction("delDoc", map[string]any{"docID": docID}, true, ctx)
		assert.NoError(t, err)
	})
}

func testValidateFunctions(t *testing.T, fnConfig *FunctionsConfig, gqConfig *GraphQLConfig) error {
	ctx := base.TestCtx(t)
	vm := js.V8.NewVM(ctx)
	defer vm.Close()
	return ValidateFunctions(ctx, vm, fnConfig, gqConfig)
}

// Test that JS syntax errors are detected when the db opens.
func TestUserFunctionSyntaxError(t *testing.T) {
	var functionConfig = FunctionsConfig{
		Definitions: FunctionsDefs{
			"square": &FunctionConfig{
				Code:  "return args.numero * args.numero;",
				Args:  []string{"numero"},
				Allow: &Allow{Channels: []string{"wonderland"}},
			},
			"syntax_error": &FunctionConfig{
				Code:  "returm )42(",
				Allow: allowAll,
			},
		},
	}

	err := testValidateFunctions(t, &functionConfig, nil)
	assert.Error(t, err)
}

func TestUserFunctionsMaxFunctionCount(t *testing.T) {
	var functionConfig = FunctionsConfig{
		MaxFunctionCount: base.IntPtr(1),
		Definitions: FunctionsDefs{
			"square": &FunctionConfig{
				Type:  "javascript",
				Code:  "function(context, args) {return args.numero * args.numero;}",
				Args:  []string{"numero"},
				Allow: &Allow{Channels: []string{"wonderland"}},
			},
			"exceptional": &FunctionConfig{
				Type:  "javascript",
				Code:  `function(context, args) {throw "oops";}`,
				Allow: allowAll,
			},
		},
	}
	err := testValidateFunctions(t, &functionConfig, nil)
	assert.ErrorContains(t, err, "too many functions (> 1)")
}

func TestUserFunctionsMaxCodeSize(t *testing.T) {
	var functionConfig = FunctionsConfig{
		MaxCodeSize: base.IntPtr(20),
		Definitions: FunctionsDefs{
			"square": &FunctionConfig{
				Type:  "javascript",
				Code:  "function(context, args) {return args.numero * args.numero;}",
				Args:  []string{"numero"},
				Allow: &Allow{Channels: []string{"wonderland"}},
			},
		},
	}
	err := testValidateFunctions(t, &functionConfig, nil)
	assert.ErrorContains(t, err, "function square: code is too large (> 20 bytes)")
}

//////// UTILITY FUNCTIONS:

// If certain environment variables are set, for example to turn on XATTR support, then update
// the DatabaseContextOptions accordingly
func AddOptionsFromEnvironmentVariables(dbcOptions *db.DatabaseContextOptions) {
	if base.TestUseXattrs() {
		dbcOptions.EnableXattr = true
	}

	if base.TestsDisableGSI() {
		dbcOptions.UseViews = true
	}
}

func assertHTTPError(t *testing.T, err error, status int) bool {
	var httpErr *base.HTTPError
	return assert.Error(t, err) &&
		assert.ErrorAs(t, err, &httpErr, "Error is %T, %v", err, err) &&
		assert.Equal(t, status, httpErr.Status, "Error is: %#v", err)
}

//////// SETUP FUNCTIONS

func setupTestDBWithFunctions(t *testing.T, fnConfig *FunctionsConfig, gqConfig *GraphQLConfig) (*db.Database, context.Context) {
	return setupTestDBWithFunctionsAndLogging(t, fnConfig, gqConfig, true)
}

func setupTestDBWithFunctionsAndLogging(t *testing.T, fnConfig *FunctionsConfig, gqConfig *GraphQLConfig, jsDebugLogging bool) (*db.Database, context.Context) {
	cacheOptions := db.DefaultCacheOptions()
	options := db.DatabaseContextOptions{
		CacheOptions:     &cacheOptions,
		Scopes:           db.GetScopesOptionsDefaultCollectionOnly(t),
		FunctionsConfig:  &Config{fnConfig, gqConfig},
		JavaScriptEngine: base.StringPtr("V8"), // Not compatible with Otto, it's too old
	}
	return setupTestDBWithOptions(t, options, jsDebugLogging)
}

func setupTestDBWithOptions(t testing.TB, dbcOptions db.DatabaseContextOptions, jsDebugLogging bool) (*db.Database, context.Context) {

	tBucket := base.GetTestBucket(t)
	return setupTestDBForBucketWithOptions(t, tBucket, dbcOptions, jsDebugLogging)
}

func setupTestDBForBucketWithOptions(t testing.TB, tBucket base.Bucket, dbcOptions db.DatabaseContextOptions, jsDebugLogging bool) (*db.Database, context.Context) {
	ctx := base.TestCtx(t)
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	dbCtx, err := db.NewDatabaseContext(ctx, "db", tBucket, false, dbcOptions)
	assert.NoError(t, err, "Couldn't create context for database 'db'")
	db, err := db.CreateDatabase(dbCtx)
	assert.NoError(t, err, "Couldn't create database 'db'")
	ctx = db.AddDatabaseLogContext(ctx)
	if jsDebugLogging {
		base.SetUpTestLogging(t, base.LevelDebug, base.KeyJavascript) // Enable debug JS logging!
	}
	return db, ctx
}

// createPrimaryIndex returns true if there was no index created before
func createPrimaryIndex(t *testing.T, n1qlStore base.N1QLStore) bool {
	hasPrimary, _, err := base.GetIndexMeta(n1qlStore, base.PrimaryIndexName)
	assert.NoError(t, err)
	if hasPrimary {
		return false
	}
	err = n1qlStore.CreatePrimaryIndex(base.PrimaryIndexName, nil)
	assert.NoError(t, err)
	return true
}
