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

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
)

var allowAll = &Allow{Channels: []string{"*"}}

var kUserFunctionConfig = FunctionConfigMap{
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
		Allow: &Allow{Channels: []string{"user-$(context.user.name)"}},
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
		Type:  "javascript",
		Code:  `function(context, args) {return context.user.defaultCollection.save(args.docID, args.doc);}`,
		Args:  []string{"docID", "doc"},
		Allow: allowAll,
	},
	"delDoc": &FunctionConfig{
		Type:  "javascript",
		Code:  `function(context, args) {return context.user.defaultCollection.delete(args.docID);}`,
		Args:  []string{"docID"},
		Allow: allowAll,
	},
}

// Adds a user "alice" to the database, with role "hero"
// and access to channels "wonderland" and "lookingglass".
func addUserAlice(t *testing.T, db *db.Database) auth.User {
	var err error
	authenticator := auth.NewAuthenticator(db.MetadataStore, db, auth.DefaultAuthenticatorOptions())
	hero, err := authenticator.NewRole("hero", base.SetOf("heroes"))
	assert.NoError(t, err)
	assert.NoError(t, authenticator.Save(hero))
	villain, err := authenticator.NewRole("villain", base.SetOf("villains"))
	assert.NoError(t, err)
	assert.NoError(t, authenticator.Save(villain))

	user, err := authenticator.NewUser("alice", "pass", base.SetOf("wonderland", "lookingglass", "city-London", "user-alice"))
	assert.NoError(t, err)
	user.SetExplicitRoles(channels.TimedSet{"hero": channels.NewVbSimpleSequence(1)}, 1)
	assert.NoError(t, authenticator.Save(user), "Save")

	// Have to call GetUser to get a user object that's properly configured:
	user, err = authenticator.GetUser("alice")
	assert.NoError(t, err)
	return user
}

// Unit test for JS user functions.
func TestUserFunctions(t *testing.T) {
	// base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db, ctx := setupTestDBWithFunctions(t, kUserFunctionConfig, nil)
	defer db.Close(ctx)

	assert.NotNil(t, db.Options.UserFunctions["square"])

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
	result, err = db.CallUserFunction("factorial", map[string]any{"n": 20}, true, ctx)
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
	jserr := err.(*jsError)
	assert.NotNil(t, jserr)

	// Call depth limit:
	_, err = db.CallUserFunction("factorial", map[string]any{"n": kUserFunctionMaxCallDepth + 1}, true, ctx)
	assert.ErrorContains(t, err, "User function recursion too deep")
	assert.ErrorContains(t, err, "factorial")
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
	jserr := err.(*jsError)
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
	// base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db, ctx := setupTestDBWithFunctions(t, kUserFunctionConfig, nil)
	defer db.Close(ctx)

	body := map[string]any{"key": "value"}

	// Create a doc with random ID:
	result, err := db.CallUserFunction("putDoc", map[string]any{"docID": nil, "doc": body}, true, ctx)
	assert.NoError(t, err)
	assert.IsType(t, "", result)
	_, err = db.CallUserFunction("getDoc", map[string]any{"docID": result}, true, ctx)
	assert.NoError(t, err)

	docID := "foo"

	// Missing document:
	result, err = db.CallUserFunction("getDoc", map[string]any{"docID": docID}, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, nil, result)

	docParams := map[string]any{
		"docID": docID,
		"doc":   body,
	}

	// Illegal mutation:
	_, err = db.CallUserFunction("putDoc", docParams, false, ctx)
	assertHTTPError(t, err, 403)

	// Successful save (as admin):
	result, err = db.CallUserFunction("putDoc", docParams, true, ctx)
	assert.NoError(t, err)
	assert.EqualValues(t, docID, result) // save() returns docID

	// Existing document:
	result, err = db.CallUserFunction("getDoc", map[string]any{"docID": docID}, true, ctx)
	assert.NoError(t, err)
	revID, ok := result.(map[string]any)["_rev"].(string)
	assert.True(t, ok)
	assert.NotEmpty(t, revID)
	assert.True(t, strings.HasPrefix(revID, "1-"))
	body["_id"] = docID
	body["_rev"] = revID
	assert.EqualValues(t, body, result)

	// Update document with revID:
	body["key2"] = 2
	_, err = db.CallUserFunction("putDoc", docParams, true, ctx)
	assert.NoError(t, err)

	// Save fails with conflict:
	body["key3"] = 3
	body["_rev"] = "9-9999"
	result, err = db.CallUserFunction("putDoc", docParams, true, ctx)
	assert.NoError(t, err)
	assert.Nil(t, result)

	// Update document without revID:
	body["key3"] = 4
	delete(body, "_revid")
	result, err = db.CallUserFunction("putDoc", docParams, true, ctx)
	assert.NoError(t, err)
	assert.Equal(t, docID, result)

	// Get doc again to verify revision:
	result, err = db.CallUserFunction("getDoc", map[string]any{"docID": docID}, true, ctx)
	assert.NoError(t, err)
	revID, ok = result.(map[string]any)["_rev"].(string)
	assert.True(t, ok)
	assert.NotEmpty(t, revID)
	assert.True(t, strings.HasPrefix(revID, "3-"))

	// Delete doc:
	_, err = db.CallUserFunction("delDoc", map[string]any{"docID": docID}, true, ctx)
	assert.NoError(t, err)
}

// Test that JS syntax errors are detected when the db opens.
func TestUserFunctionSyntaxError(t *testing.T) {
	var kUserFunctionBadConfig = FunctionConfigMap{
		"square": &FunctionConfig{
			Code:  "return args.numero * args.numero;",
			Args:  []string{"numero"},
			Allow: &Allow{Channels: []string{"wonderland"}},
		},
		"syntax_error": &FunctionConfig{
			Code:  "returm )42(",
			Allow: allowAll,
		},
	}

	_, err := CompileFunctions(kUserFunctionBadConfig)
	assert.Error(t, err)
}

// Low-level test of channel-name parameter expansion for user query/function auth
func TestUserFunctionAllow(t *testing.T) {
	// base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db, ctx := setupTestDBWithFunctions(t, kUserFunctionConfig, nil)
	defer db.Close(ctx)

	authenticator := auth.NewAuthenticator(db.MetadataStore, db, auth.DefaultAuthenticatorOptions())
	user, err := authenticator.NewUser("maurice", "pass", base.SetOf("city-Paris"))
	_ = user.SetEmail("maurice@academie.fr")
	assert.NoError(t, err)

	params := map[string]any{
		"CITY":  "Paris",
		"BREAD": "Baguette",
		"YEAR":  2020,
		"WORDS": []string{"ouais", "fromage", "amour", "vachement"},
	}

	allow := Allow{}

	ch, err := allow.expandPattern("someChannel", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "someChannel")

	ch, err = allow.expandPattern("sales-$CITY-all", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "sales-Paris-all")

	ch, err = allow.expandPattern("sales$(CITY)All", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "salesParisAll")

	ch, err = allow.expandPattern("sales$CITY-$BREAD", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "salesParis-Baguette")

	ch, err = allow.expandPattern("sales-upTo-$YEAR", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "sales-upTo-2020")

	ch, err = allow.expandPattern("employee-$(context.user.name)", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "employee-maurice")

	ch, err = allow.expandPattern("employee-$(user.name)", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "employee-maurice")

	ch, err = allow.expandPattern("$(context.user.email)", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "maurice@academie.fr")

	ch, err = allow.expandPattern("$(user.email)", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "maurice@academie.fr")

	// Should replace `$$` with `$`
	ch, err = allow.expandPattern("expen$$ive", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "expen$ive")

	// No-ops since the `$` does not match a pattern:
	ch, err = allow.expandPattern("$+wow", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "$+wow")

	ch, err = allow.expandPattern("foobar$", params, user)
	assert.NoError(t, err)
	assert.Equal(t, ch, "foobar$")

	// error: param value is not a string
	_, err = allow.expandPattern("knows-$WORDS", params, user)
	assert.NotNil(t, err)

	// error: undefined parameter
	_, err = allow.expandPattern("sales-upTo-$FOO", params, user)
	assert.NotNil(t, err)
}

func assertHTTPError(t *testing.T, err error, status int) bool {
	var httpErr *base.HTTPError
	return assert.Error(t, err) &&
		assert.ErrorAs(t, err, &httpErr) &&
		assert.Equal(t, status, httpErr.Status)
}
