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
	"strings"
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
)

var kUserN1QLFunctionsConfig = FunctionsConfig{
	Definitions: FunctionsDefs{
		"airports_in_city": &FunctionConfig{
			Type:  "query",
			Code:  `SELECT $args.city AS city`,
			Args:  []string{"city"},
			Allow: &Allow{Channels: []string{"city-${args.city}", "allcities"}},
		},
		"square": &FunctionConfig{
			Type:  "query",
			Code:  "SELECT $args.numero * $args.numero AS square",
			Args:  []string{"numero"},
			Allow: &Allow{Channels: []string{"wonderland"}},
		},
		"user": &FunctionConfig{
			Type:  "query",
			Code:  "SELECT $user AS `user`",
			Allow: &Allow{Channels: []string{"*"}},
		},
		"user_parts": &FunctionConfig{
			Type:  "query",
			Code:  "SELECT $user.name AS name, $user.email AS email",
			Allow: &Allow{Channels: []string{"user-${context.user.name}"}},
		},
		"admin_only": &FunctionConfig{
			Type:  "query",
			Code:  `SELECT "ok" AS status`,
			Allow: nil, // no 'allow' property means admin-only
		},
		"inject": &FunctionConfig{
			Type:  "query",
			Code:  `SELECT $args.foo`,
			Args:  []string{"foo"},
			Allow: &Allow{Channels: []string{"*"}},
		},
		"syntax_error": &FunctionConfig{
			Type:  "query",
			Code:  "SELECT OOK? FR0M OOK!",
			Allow: allowAll,
		},
	},
}

func callUserQuery(ctx context.Context, db *db.Database, name string, args map[string]any) (any, error) {
	return db.CallUserFunction(name, args, false, ctx)
}

func queryResultString(t *testing.T, iter sgbucket.QueryResultIterator) string {
	if iter == nil {
		return ""
	}
	result := []any{}
	var row any
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

	//base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	db, ctx := setupTestDBWithFunctions(t, &kUserN1QLFunctionsConfig, nil)
	defer db.Close(ctx)

	// First run the tests as an admin:
	t.Run("AsAdmin", func(t *testing.T) { testUserQueriesAsAdmin(t, ctx, db) })

	// Now create a user and make it current:
	db.SetUser(addUserAlice(t, db))
	assert.True(t, db.User().RoleNames().Contains("hero"))

	// Repeat the tests as user "alice":
	t.Run("AsUser", func(t *testing.T) { testUserQueriesAsUser(t, ctx, db) })
}

func testUserQueriesCommon(t *testing.T, ctx context.Context, db *db.Database) {
	// dynamic channel list
	fn, err := db.GetUserFunction("airports_in_city", map[string]any{"city": "London"}, false, ctx)
	assert.NoError(t, err)
	iter, err := fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"city":"London"}]`, iter)

	fn, err = db.GetUserFunction("square", map[string]any{"numero": 16}, false, ctx)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"square":256}]`, iter)

	fn, err = db.GetUserFunction("inject", map[string]any{"foo": "1337 as pwned"}, false, ctx)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"foo":"1337 as pwned"}]`, iter)

	// ERRORS:

	// Missing a parameter:
	_, err = callUserQuery(ctx, db, "square", nil)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "numero")
	assert.ErrorContains(t, err, "square")

	// Extra parameter:
	_, err = callUserQuery(ctx, db, "square", map[string]any{"numero": 42, "number": 0})
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "number")
	assert.ErrorContains(t, err, "square")

	// Function definition has a syntax error:
	_, err = callUserQuery(ctx, db, "syntax_error", nil)
	assertHTTPError(t, err, 500)
	assert.ErrorContains(t, err, "syntax_error")
}

func testUserQueriesAsAdmin(t *testing.T, ctx context.Context, db *db.Database) {
	testUserQueriesCommon(t, ctx, db)

	fn, err := db.GetUserFunction("user", nil, false, ctx)
	assert.NoError(t, err)
	iter, err := fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"user":{}}]`, iter)

	fn, err = db.GetUserFunction("user_parts", nil, false, ctx)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{}]`, iter)

	// admin only:
	fn, err = db.GetUserFunction("admin_only", nil, false, ctx)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"status":"ok"}]`, iter)

	// ERRORS:

	// No such query:
	_, err = callUserQuery(ctx, db, "xxxx", nil)
	assertHTTPError(t, err, 404)
}

func testUserQueriesAsUser(t *testing.T, ctx context.Context, db *db.Database) {
	testUserQueriesCommon(t, ctx, db)

	fn, err := db.GetUserFunction("user", nil, false, ctx)
	assert.NoError(t, err)
	// (Can't compare the entire result string because the order of items in the "channels" array
	// is undefined and can change from one run to another.)
	iter, err := fn.Iterate()
	assert.NoError(t, err)
	resultStr := queryResultString(t, iter)
	assert.True(t, strings.HasPrefix(resultStr, `[{"user":{"channels":["`))
	assert.True(t, strings.HasSuffix(resultStr, `"],"email":"","name":"alice","roles":["hero"]}}]`))

	fn, err = db.GetUserFunction("user_parts", nil, false, ctx)
	assert.NoError(t, err)
	iter, err = fn.Iterate()
	assert.NoError(t, err)
	assertQueryResults(t, `[{"email":"","name":"alice"}]`, iter)

	// ERRORS:

	// Not allowed (admin only):
	_, err = callUserQuery(ctx, db, "admin_only", nil)
	assertHTTPError(t, err, 403)

	// Not allowed (dynamic channel list):
	_, err = callUserQuery(ctx, db, "airports_in_city", map[string]any{"city": "Chicago"})
	assertHTTPError(t, err, 403)

	// No such query:
	_, err = callUserQuery(ctx, db, "xxxx", nil)
	assertHTTPError(t, err, 403) // not 404 as for an admin
}

// Unit test to ensure only SELECT queries can run
func TestUserN1QLQueriesInvalid(t *testing.T) {
	var kBadN1QLFunctionsConfig = FunctionsConfig{
		Definitions: FunctionsDefs{
			"evil_mutant": &FunctionConfig{
				Type:  "query",
				Code:  `DROP COLLECTION Students`,
				Allow: &Allow{Channels: []string{"*"}},
			},
		},
	}

	_, err := CompileFunctions(kBadN1QLFunctionsConfig)
	assert.ErrorContains(t, err, "only SELECT queries are allowed") // See fn validateN1QLQuery

	var kOKN1QLFunctionsConfig = FunctionsConfig{
		Definitions: FunctionsDefs{
			"lowercase": &FunctionConfig{
				Type:  "query",
				Code:  `seleCt $args.city AS city`,
				Args:  []string{"city"},
				Allow: &Allow{Channels: []string{"*"}},
			},
			"parens": &FunctionConfig{
				Type:  "query",
				Code:  `  (select $args.city AS city)`,
				Args:  []string{"city"},
				Allow: &Allow{Channels: []string{"*"}},
			},
		},
	}

	_, err = CompileFunctions(kOKN1QLFunctionsConfig)
	assert.NoError(t, err)
}
