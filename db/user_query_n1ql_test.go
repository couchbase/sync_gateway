/*
Copyright 2020-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"encoding/json"
	"strings"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

var kUserN1QLFunctionsConfig = UserFunctionConfigMap{
	"airports_in_city": &UserFunctionConfig{
		Type:  "query",
		Code:  `SELECT $city AS city`,
		Args:  []string{"city"},
		Allow: &UserQueryAllow{Channels: []string{"city-$city", "allcities"}},
	},
	"square": &UserFunctionConfig{
		Type:  "query",
		Code:  "SELECT $numero * $numero AS square",
		Args:  []string{"numero"},
		Allow: &UserQueryAllow{Channels: []string{"wonderland"}},
	},
	"user": &UserFunctionConfig{
		Type:  "query",
		Code:  "SELECT $user AS `user`",
		Allow: &UserQueryAllow{Channels: []string{"*"}},
	},
	"user_parts": &UserFunctionConfig{
		Type:  "query",
		Code:  "SELECT $user.name AS name, $user.email AS email",
		Allow: &UserQueryAllow{Channels: []string{"user-$(user.name)"}},
	},
	"admin_only": &UserFunctionConfig{
		Type:  "query",
		Code:  `SELECT "ok" AS status`,
		Allow: nil, // no 'allow' property means admin-only
	},
	"inject": &UserFunctionConfig{
		Type:  "query",
		Code:  `SELECT $foo`,
		Args:  []string{"foo"},
		Allow: &UserQueryAllow{Channels: []string{"*"}},
	},
	"syntax_error": &UserFunctionConfig{
		Type:  "query",
		Code:  "SELEKT OOK? FR0M OOK!",
		Allow: allowAll,
	},
}

func callUserQuery(db *Database, name string, args map[string]interface{}) (interface{}, error) {
	return db.CallUserFunction(name, args, false)
}

func queryResultString(t *testing.T, iter sgbucket.QueryResultIterator) string {
	if iter == nil {
		return ""
	}
	result := []interface{}{}
	var row interface{}
	for iter.Next(&row) {
		result = append(result, row)
	}
	assert.NoError(t, iter.Close())
	j, err := json.Marshal(result)
	if !assert.NoError(t, err) {
		return ""
	}
	return string(j)
}

func assertQueryResults(t *testing.T, expected string, iter sgbucket.QueryResultIterator) {
	if actual := queryResultString(t, iter); actual != "" {
		assert.Equal(t, expected, actual)
	}
}

// Unit test for user N1QL queries.
func TestUserN1QLQueries(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only (requires N1QL)")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	cacheOptions := DefaultCacheOptions()
	db := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions:  &cacheOptions,
		UserFunctions: kUserN1QLFunctionsConfig,
	})
	defer db.Close()

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserQueriesAsAdmin(t, db) })

	// Now create a user and make it current:
	db.user = addUserAlice(t, db)
	assert.True(t, db.user.RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserQueriesAsUser(t, db) })
}

func testUserQueriesCommon(t *testing.T, db *Database) {
	// dynamic channel list
	fn, err := db.GetUserFunction("airports_in_city", map[string]interface{}{"city": "London"}, false)
	assert.NoError(t, err)
	iter, err := fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"city":"London"}]`, iter)

	fn, err = db.GetUserFunction("square", map[string]interface{}{"numero": 16}, false)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"square":256}]`, iter)

	fn, err = db.GetUserFunction("inject", map[string]interface{}{"foo": "1337 as pwned"}, false)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"$1":"1337 as pwned"}]`, iter)

	// ERRORS:

	// Missing a parameter:
	_, err = callUserQuery(db, "square", nil)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "numero")
	assert.ErrorContains(t, err, "square")

	// Extra parameter:
	_, err = callUserQuery(db, "square", map[string]interface{}{"numero": 42, "number": 0})
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "number")
	assert.ErrorContains(t, err, "square")

	// Function definition has a syntax error:
	_, err = callUserQuery(db, "syntax_error", nil)
	assertHTTPError(t, err, 500)
	assert.ErrorContains(t, err, "syntax_error")
}

func testUserQueriesAsAdmin(t *testing.T, db *Database) {
	testUserQueriesCommon(t, db)

	fn, err := db.GetUserFunction("user", nil, false)
	assert.NoError(t, err)
	iter, err := fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"user":{}}]`, iter)

	fn, err = db.GetUserFunction("user_parts", nil, false)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{}]`, iter)

	// admin only:
	fn, err = db.GetUserFunction("admin_only", nil, false)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"status":"ok"}]`, iter)

	// ERRORS:

	// No such query:
	_, err = callUserQuery(db, "xxxx", nil)
	assertHTTPError(t, err, 404)
}

func testUserQueriesAsUser(t *testing.T, db *Database) {
	testUserQueriesCommon(t, db)

	fn, err := db.GetUserFunction("user", nil, false)
	assert.NoError(t, err)
	// (Can't compare the entire result string because the order of items in the "channels" array
	// is undefined and can change from one run to another.)
	iter, err := fn.Iterate()
	assert.NoError(t, err)
	resultStr := queryResultString(t, iter)
	assert.True(t, strings.HasPrefix(resultStr, `[{"user":{"channels":["`))
	assert.True(t, strings.HasSuffix(resultStr, `"],"email":"","name":"alice","roles":["hero"]}}]`))

	fn, err = db.GetUserFunction("user_parts", nil, false)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"email":"","name":"alice"}]`, iter)

	// ERRORS:

	// Not allowed (admin only):
	_, err = callUserQuery(db, "admin_only", nil)
	assertHTTPError(t, err, 403)

	// Not allowed (dynamic channel list):
	_, err = callUserQuery(db, "airports_in_city", map[string]interface{}{"city": "Chicago"})
	assertHTTPError(t, err, 403)

	// No such query:
	_, err = callUserQuery(db, "xxxx", nil)
	assertHTTPError(t, err, 403) // not 404 as for an admin
}
