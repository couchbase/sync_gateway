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
	"testing"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
)

var kUserQueriesConfig = UserQueryMap{
	"airports_in_city": &UserQueryConfig{
		Statement:  `SELECT $city as city`,
		Parameters: []string{"city"},
		Allow:      &UserQueryAllow{Channels: []string{"city-$city", "allcities"}},
	},
	"square": &UserQueryConfig{
		Statement:  "SELECT $numero * $numero as square",
		Parameters: []string{"numero"},
		Allow:      &UserQueryAllow{Channels: []string{"wonderland"}},
	},
	"syntax_error": &UserQueryConfig{
		Statement: "SELEKT OOK? FR0M OOK!",
		Allow:     allowAll,
	},
	"admin_only": &UserQueryConfig{
		Statement: `SELECT "ok" as status`,
		Allow:     nil, // no 'allow' property means admin-only
	},
}

func assertQueryResults(t *testing.T, expected string, iter sgbucket.QueryResultIterator) {
	if iter == nil {
		return
	}
	result := []interface{}{}
	var row interface{}
	for iter.Next(&row) {
		result = append(result, row)
	}
	assert.NoError(t, iter.Close())
	j, err := json.Marshal(result)
	assert.NoError(t, err)
	assert.Equal(t, expected, string(j))
}

// Unit test for user N1QL queries.
func TestUserQueries(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test is Couchbase Server only (requires N1QL)")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	cacheOptions := DefaultCacheOptions()
	db := setupTestDBWithOptions(t, DatabaseContextOptions{
		CacheOptions: &cacheOptions,
		UserQueries:  kUserQueriesConfig,
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
	iter, err := db.UserQuery("airports_in_city", map[string]interface{}{"city": "London"})
	assert.NoError(t, err)
	assertQueryResults(t, `[{"city":"London"}]`, iter)

	iter, err = db.UserQuery("square", map[string]interface{}{"numero": 16})
	assert.NoError(t, err)
	assertQueryResults(t, `[{"square":256}]`, iter)

	// ERRORS:

	// Missing a parameter:
	_, err = db.UserQuery("square", nil)
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "numero")
	assert.ErrorContains(t, err, "square")

	// Extra parameter:
	_, err = db.UserQuery("square", map[string]interface{}{"numero": 42, "number": 0})
	assertHTTPError(t, err, 400)
	assert.ErrorContains(t, err, "number")
	assert.ErrorContains(t, err, "square")

	// Function definition has a syntax error:
	_, err = db.UserQuery("syntax_error", nil)
	assertHTTPError(t, err, 500)
	assert.ErrorContains(t, err, "syntax_error")
}

func testUserQueriesAsAdmin(t *testing.T, db *Database) {
	testUserQueriesCommon(t, db)

	// admin only:
	iter, err := db.UserQuery("admin_only", nil)
	assert.NoError(t, err)
	assertQueryResults(t, `[{"status":"ok"}]`, iter)

	// ERRORS:

	// No such query:
	_, err = db.UserQuery("xxxx", nil)
	assertHTTPError(t, err, 404)
}

func testUserQueriesAsUser(t *testing.T, db *Database) {
	testUserQueriesCommon(t, db)

	// ERRORS:

	// Not allowed (admin only):
	_, err := db.UserQuery("admin_only", nil)
	assertHTTPError(t, err, 403)

	// Not allowed (dynamic channel list):
	_, err = db.UserQuery("airports_in_city", map[string]interface{}{"city": "Chicago"})
	assertHTTPError(t, err, 403)

	// No such query:
	_, err = db.UserQuery("xxxx", nil)
	assertHTTPError(t, err, 403) // not 404 as for an admin
}
