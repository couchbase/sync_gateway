/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
)

var allowAll = &UserQueryAllow{Channels: []string{"*"}}

var kUserFunctionConfig = UserFunctionConfigMap{
	"square": &UserFunctionConfig{
		SourceCode: "return args.numero * args.numero;",
		Parameters: []string{"numero"},
		Allow:      &UserQueryAllow{Channels: []string{"wonderland"}},
	},
	"exceptional": &UserFunctionConfig{
		SourceCode: `throw "oops";`,
		Allow:      allowAll,
	},
	"call_fn": &UserFunctionConfig{
		SourceCode: `return context.app.func("square", {numero: 7});`,
		Allow:      allowAll,
	},
	"great_and_terrible": &UserFunctionConfig{
		SourceCode: `return "I am OZ the great and terrible";`,
		Allow:      &UserQueryAllow{Channels: []string{"oz", "narnia"}},
	},
	"call_forbidden": &UserFunctionConfig{
		SourceCode: `return context.app.func("great_and_terrible");`,
		Allow:      allowAll,
	},
	"admin_only": &UserFunctionConfig{
		SourceCode: `return "OK";`,
		Allow:      nil, // no 'allow' property means admin-only
	},
	"require_admin": &UserFunctionConfig{
		SourceCode: `context.user.requireAdmin(); return "OK";`,
		Allow:      allowAll,
	},
	"user_only": &UserFunctionConfig{
		SourceCode: `if (!context.user.name) throw "No user"; return context.user.name;`,
		Allow:      &UserQueryAllow{Channels: []string{"user-$(context.user.name)"}},
	},
	"alice_only": &UserFunctionConfig{
		SourceCode: `context.user.requireName("alice"); return "OK";`,
		Allow:      allowAll,
	},
	"pevensies_only": &UserFunctionConfig{
		SourceCode: `context.user.requireName(["peter","jane","eustace","lucy"]); return "OK";`,
		Allow:      allowAll,
	},
	"wonderland_only": &UserFunctionConfig{
		SourceCode: `context.user.requireAccess("wonderland"); context.user.requireAccess(["wonderland", "snark"]); return "OK";`,
		Allow:      allowAll,
	},
	"narnia_only": &UserFunctionConfig{
		SourceCode: `context.user.requireAccess("narnia"); return "OK";`,
		Allow:      allowAll,
	},
	"hero_only": &UserFunctionConfig{
		SourceCode: `context.user.requireRole(["hero", "antihero"]); return "OK";`,
		Allow:      allowAll,
	},
	"villain_only": &UserFunctionConfig{
		SourceCode: `context.user.requireRole(["villain"]); return "OK";`,
		Allow:      allowAll,
	},

	"getDoc": &UserFunctionConfig{
		SourceCode: `return context.app.get(args.docID);`,
		Parameters: []string{"docID"},
		Allow:      allowAll,
	},
	"putDoc": &UserFunctionConfig{
		SourceCode: `return context.app.save(args.docID, args.doc);`,
		Parameters: []string{"docID", "doc"},
		Allow:      allowAll,
	},
}

// Adds a user "alice" to the database, with role "hero"
// and access to channels "wonderland" and "lookingglass".
func addUserAlice(t *testing.T, db *Database) auth.User {
	var err error
	authenticator := auth.NewAuthenticator(db.Bucket, db, auth.DefaultAuthenticatorOptions())
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
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	cacheOptions := DefaultCacheOptions()
	db, ctx := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions:  &cacheOptions,
		UserFunctions: kUserFunctionConfig,
	})
	defer db.Close(ctx)

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserFunctionsAsAdmin(t, ctx, db) })

	// Now create a user and make it current:
	db.user = addUserAlice(t, db)
	assert.True(t, db.user.RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserFunctionsAsUser(t, ctx, db) })
}

// User function tests that work the same for admin and non-admin user:
func testUserFunctionsCommon(t *testing.T, ctx context.Context, db *Database) {
	// Basic call passing a parameter:
	result, err := db.CallUserFunction(ctx, "square", map[string]interface{}{"numero": 42}, true)
	assert.NoError(t, err)
	assert.EqualValues(t, 42*42, result)

	// Function that calls a function:
	result, err = db.CallUserFunction(ctx, "call_fn", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, 7*7, result)

	// `requireName` test that passes:
	result, err = db.CallUserFunction(ctx, "alice_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// `requireChannel` test that passes:
	result, err = db.CallUserFunction(ctx, "wonderland_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// `requireRole` test that passes:
	result, err = db.CallUserFunction(ctx, "hero_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// ERRORS:

	// Missing a parameter:
	_, err = db.CallUserFunction(ctx, "square", nil, true)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "numero")
	assert.ErrorContains(t, err, "square")

	// Extra parameter:
	_, err = db.CallUserFunction(ctx, "square", map[string]interface{}{"numero": 42, "number": 0}, true)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "number")
	assert.ErrorContains(t, err, "square")

	// Function throws an exception:
	_, err = db.CallUserFunction(ctx, "exceptional", nil, true)
	assert.ErrorContains(t, err, "oops")
	assert.ErrorContains(t, err, "exceptional")
	jserr := err.(*jsError)
	assert.NotNil(t, jserr)
}

// User-function tests, run as admin:
func testUserFunctionsAsAdmin(t *testing.T, ctx context.Context, db *Database) {
	testUserFunctionsCommon(t, ctx, db)

	// Admin-only (success):
	result, err := db.CallUserFunction(ctx, "admin_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction(ctx, "require_admin", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction(ctx, "pevensies_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction(ctx, "narnia_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction(ctx, "villain_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// ERRORS:

	// Checking `context.user.name`:
	_, err = db.CallUserFunction(ctx, "user_only", nil, true)
	assert.ErrorContains(t, err, "No user")
	jserr := err.(*jsError)
	assert.NotNil(t, jserr)

	// No such function:
	_, err = db.CallUserFunction(ctx, "xxxx", nil, true)
	assertHTTPError(t, err, 404)
}

// User-function tests, run as user "alice":
func testUserFunctionsAsUser(t *testing.T, ctx context.Context, db *Database) {
	testUserFunctionsCommon(t, ctx, db)

	// Checking `context.user.name`:
	result, err := db.CallUserFunction(ctx, "user_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "alice", result)

	// No such function:
	_, err = db.CallUserFunction(ctx, "xxxx", nil, true)
	assertHTTPError(t, err, 403) // not 404 as for an admin

	_, err = db.CallUserFunction(ctx, "great_and_terrible", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction(ctx, "call_forbidden", nil, true)
	assertHTTPError(t, err, 403)
	assert.ErrorContains(t, err, "great_and_terrible") // failed fn name should appear in error

	_, err = db.CallUserFunction(ctx, "admin_only", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction(ctx, "require_admin", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction(ctx, "pevensies_only", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction(ctx, "narnia_only", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction(ctx, "villain_only", nil, true)
	assertHTTPError(t, err, 403)
}

// Test CRUD operations
func TestUserFunctionsCRUD(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	cacheOptions := DefaultCacheOptions()
	db, ctx := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions:  &cacheOptions,
		UserFunctions: kUserFunctionConfig,
	})
	defer db.Close(ctx)

	docID := "foo"

	// Missing document:
	result, err := db.CallUserFunction(ctx, "getDoc", map[string]interface{}{"docID": docID}, false)
	assert.NoError(t, err)
	assert.EqualValues(t, nil, result)

	doc := map[string]interface{}{"key": "value"}

	docParams := map[string]interface{}{
		"docID": docID,
		"doc":   doc,
	}

	// Illegal mutation:
	_, err = db.CallUserFunction(ctx, "putDoc", docParams, false)
	assertHTTPError(t, err, 403)

	// Successful save:
	result, err = db.CallUserFunction(ctx, "putDoc", docParams, true)
	assert.NoError(t, err)
	assert.EqualValues(t, nil, result)

	// Existing document:
	result, err = db.CallUserFunction(ctx, "getDoc", map[string]interface{}{"docID": docID}, false)
	assert.NoError(t, err)
	revID, ok := result.(map[string]interface{})["_rev"].(string)
	assert.True(t, ok)
	assert.NotEmpty(t, revID)
	assert.True(t, strings.HasPrefix(revID, "1-"))
	doc["_id"] = docID
	doc["_rev"] = revID
	assert.EqualValues(t, doc, result)

	// Update document with revID:
	doc["key2"] = 2
	_, err = db.CallUserFunction(ctx, "putDoc", docParams, true)
	assert.NoError(t, err)

	// Update document without revID:
	doc["key3"] = 3
	delete(doc, "_revid")
	result, err = db.CallUserFunction(ctx, "putDoc", docParams, true)
	assert.NoError(t, err)

	// Get doc again to verify revision:
	result, err = db.CallUserFunction(ctx, "getDoc", map[string]interface{}{"docID": docID}, false)
	revID, ok = result.(map[string]interface{})["_rev"].(string)
	assert.True(t, ok)
	assert.NotEmpty(t, revID)
	assert.True(t, strings.HasPrefix(revID, "3-"))
}

var kUserFunctionBadConfig = UserFunctionConfigMap{
	"square": &UserFunctionConfig{
		SourceCode: "return args.numero * args.numero;",
		Parameters: []string{"numero"},
		Allow:      &UserQueryAllow{Channels: []string{"wonderland"}},
	},
	"syntax_error": &UserFunctionConfig{
		SourceCode: "returm )42(",
		Allow:      allowAll,
	},
}

// Test that JS syntax errors are detected when the db opens.
func TestUserFunctionSyntaxError(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	ctx := base.TestCtx(t)
	cacheOptions := DefaultCacheOptions()
	dbcOptions := DatabaseContextOptions{
		CacheOptions:  &cacheOptions,
		UserFunctions: kUserFunctionBadConfig,
	}
	tBucket := base.GetTestBucket(t)
	defer tBucket.Close()
	AddOptionsFromEnvironmentVariables(&dbcOptions)
	_, err := NewDatabaseContext(ctx, "db", tBucket, false, dbcOptions)
	assert.Error(t, err)
}

// Low-level test of channel-name parameter expansion for user query/function auth
func TestUserFunctionAllow(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	cacheOptions := DefaultCacheOptions()
	db, ctx := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions:  &cacheOptions,
		UserFunctions: kUserFunctionConfig,
	})
	defer db.Close(ctx)

	authenticator := auth.NewAuthenticator(db.Bucket, db, auth.DefaultAuthenticatorOptions())
	user, err := authenticator.NewUser("maurice", "pass", base.SetOf("city-Paris"))
	_ = user.SetEmail("maurice@academie.fr")
	assert.NoError(t, err)

	params := map[string]interface{}{
		"CITY":  "Paris",
		"BREAD": "Baguette",
		"YEAR":  2020,
		"WORDS": []string{"ouais", "fromage", "amour", "vachement"},
	}

	allow := UserQueryAllow{}

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
