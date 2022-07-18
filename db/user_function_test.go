/*
Copyright 2017-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"testing"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/stretchr/testify/assert"
)

var allowAll = &UserQueryAllow{Channels: []string{"*"}}

var kUserFunctionConfig = UserFunctionMap{
	"square": &UserFunctionConfig{
		SourceCode: "return args.numero * args.numero;",
		Parameters: []string{"numero"},
		Allow:      &UserQueryAllow{Channels: []string{"wonderland"}},
	},
	"syntax_error": &UserFunctionConfig{
		SourceCode: "returm zomethng!",
		Allow:      allowAll,
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
}

func TestUserFunctions(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	cacheOptions := DefaultCacheOptions()
	db := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions:  &cacheOptions,
		UserFunctions: kUserFunctionConfig,
	})
	defer db.Close()

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserFunctionsAsAdmin(t, db) })

	// Now create a user and make it current:
	var err error
	authenticator := auth.NewAuthenticator(db.Bucket, db, auth.DefaultAuthenticatorOptions())
	hero, err := authenticator.NewRole("hero", base.SetOf("heroes"))
	assert.NoError(t, err)
	assert.NoError(t, authenticator.Save(hero))
	villain, err := authenticator.NewRole("villain", base.SetOf("villains"))
	assert.NoError(t, err)
	assert.NoError(t, authenticator.Save(villain))

	user, err := authenticator.NewUser("alice", "pass", base.SetOf("wonderland", "lookingglass"))
	assert.NoError(t, err)
	user.SetExplicitRoles(channels.TimedSet{"hero": channels.NewVbSimpleSequence(1)}, 1)
	assert.NoError(t, authenticator.Save(user), "Save")
	db.user, err = authenticator.GetUser("alice")
	assert.NoError(t, err)
	assert.True(t, db.user.RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserFunctionsAsUser(t, db) })
}

// User function tests that work the same for admin and non-admin user:
func testUserFunctionsCommon(t *testing.T, db *Database) {
	// Basic call passing a parameter:
	result, err := db.CallUserFunction("square", map[string]interface{}{"numero": 42}, true)
	assert.NoError(t, err)
	assert.EqualValues(t, 42*42, result)

	// Function that calls a function:
	result, err = db.CallUserFunction("call_fn", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, 7*7, result)

	// `requireName` test that passes:
	result, err = db.CallUserFunction("alice_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// `requireChannel` test that passes:
	result, err = db.CallUserFunction("wonderland_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// `requireRole` test that passes:
	result, err = db.CallUserFunction("hero_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// ERRORS:

	// Missing a parameter:
	_, err = db.CallUserFunction("square", nil, true)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "numero")
	assert.ErrorContains(t, err, "square")

	// Extra parameter:
	_, err = db.CallUserFunction("square", map[string]interface{}{"numero": 42, "number": 0}, true)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "number")
	assert.ErrorContains(t, err, "square")

	// Function definition has a syntax error:
	_, err = db.CallUserFunction("syntax_error", nil, true)
	assertHTTPError(t, err, 500)
	assert.ErrorContains(t, err, "syntax_error")

	// Function throws an exception:
	_, err = db.CallUserFunction("exceptional", nil, true)
	assert.ErrorContains(t, err, "oops")
	assert.ErrorContains(t, err, "exceptional")
	jserr := err.(*jsError)
	assert.NotNil(t, jserr)
}

// User-function tests, run as admin:
func testUserFunctionsAsAdmin(t *testing.T, db *Database) {
	testUserFunctionsCommon(t, db)

	// Admin-only (success):
	result, err := db.CallUserFunction("admin_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("require_admin", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("pevensies_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("narnia_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	result, err = db.CallUserFunction("villain_only", nil, true)
	assert.NoError(t, err)
	assert.EqualValues(t, "OK", result)

	// No such function:
	_, err = db.CallUserFunction("xxxx", nil, true)
	assertHTTPError(t, err, 404)
}

// User-function tests, run as user "alice":
func testUserFunctionsAsUser(t *testing.T, db *Database) {
	testUserFunctionsCommon(t, db)

	// No such function:
	_, err := db.CallUserFunction("xxxx", nil, true)
	assertHTTPError(t, err, 403) // not 404 as for an admin

	_, err = db.CallUserFunction("great_and_terrible", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("call_forbidden", nil, true)
	assertHTTPError(t, err, 403)
	assert.ErrorContains(t, err, "great_and_terrible") // failed fn name should appear in error

	_, err = db.CallUserFunction("admin_only", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("require_admin", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("pevensies_only", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("narnia_only", nil, true)
	assertHTTPError(t, err, 403)

	_, err = db.CallUserFunction("villain_only", nil, true)
	assertHTTPError(t, err, 403)
}
