package rest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	goassert "github.com/couchbaselabs/go.assert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUsersAPI tests pagination of QueryPrincipals
func TestUsersAPI(t *testing.T) {

	// Create rest tester with low pagination limit
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				QueryPaginationLimit: base.IntPtr(5),
				// Disable the guest user to support testing the zero user boundary condition
				Guest: &db.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	// Validate the zero user case
	var responseUsers []string
	response := rt.SendAdminRequest("GET", "/db/_user/", "")
	assertStatus(t, response, 200)
	err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(responseUsers))

	// Test for user counts going from 1 to a few multiples of QueryPaginationLimit to check boundary conditions
	for i := 1; i < 13; i++ {
		userName := fmt.Sprintf("user%d", i)
		response := rt.SendAdminRequest("PUT", "/db/_user/"+userName, `{"password":"letmein", "admin_channels":["foo", "bar"]}`)
		assertStatus(t, response, 201)

		// check user count
		response = rt.SendAdminRequest("GET", "/db/_user/", "")
		assertStatus(t, response, 200)
		err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
		assert.NoError(t, err)
		assert.Equal(t, i, len(responseUsers))

		// validate no duplicate users returned in response
		userMap := make(map[string]interface{})
		for _, name := range responseUsers {
			_, ok := userMap[name]
			assert.False(t, ok)
			userMap[name] = struct{}{}
		}
	}
}

// TestUsersAPIDetails tests users endpoint with name_only=false when using views (unsupported combination, should return 400)
func TestUsersAPIDetailsViews(t *testing.T) {

	if !base.TestsDisableGSI() {
		t.Skip("This test only works with UseViews=true")
	}

	// Create rest tester with low pagination limit
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				QueryPaginationLimit: base.IntPtr(5),
				// Disable the guest user to support testing the zero user boundary condition
				Guest: &db.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	// Validate error handling
	response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_user/?%s=false", paramNameOnly), "")
	assertStatus(t, response, 400)

}

// TestUsersAPIDetails tests users endpoint with name_only=false
func TestUsersAPIDetails(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	// Create rest tester with low pagination limit
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				QueryPaginationLimit: base.IntPtr(5),
				// Disable the guest user to support testing the zero user boundary condition
				Guest: &db.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	validateUsersNameOnlyFalse(t, rt)
}

// TestUsersAPIDetails tests users endpoint with name_only=false, with guest disabled.
// Note: this isn't using subtests with the test above to avoid requiring multiple buckets or
// dealing with RT close races
func TestUsersAPIDetailsWithoutGuest(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	// Create rest tester with low pagination limit
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				QueryPaginationLimit: base.IntPtr(5),
			},
		},
		guestEnabled: false,
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	validateUsersNameOnlyFalse(t, rt)

}

// validateUsersNameOnlyFalse validates query results with paramNameOnly=false.  Includes
// users with/without email, enabled/disabled, various limits
func validateUsersNameOnlyFalse(t *testing.T, rt *RestTester) {
	// Validate the zero user case
	var responseUsers []db.PrincipalConfig
	response := rt.SendAdminRequest("GET", "/db/_user/?"+paramNameOnly+"=false", "")
	assertStatus(t, response, 200)
	err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(responseUsers))

	// Test for user counts going from 1 to a few multiples of QueryPaginationLimit to check boundary conditions.
	for i := 1; i <= 12; i++ {
		userName := fmt.Sprintf("user%d", i)
		disabled := "false"
		if userName == "user3" || userName == "user8" {
			disabled = "true"
		}
		// don't set email in some users
		var response *TestResponse
		if userName == "user5" {
			response = rt.SendAdminRequest("PUT", "/db/_user/"+userName, `{"password":"letmein", "disabled":`+disabled+`, "admin_channels":["foo", "bar"]}`)
		} else {
			response = rt.SendAdminRequest("PUT", "/db/_user/"+userName, `{"password":"letmein", "email": "`+userName+`@foo.com", "disabled":`+disabled+`, "admin_channels":["foo", "bar"]}`)
		}
		assertStatus(t, response, 201)

		// check user count
		response = rt.SendAdminRequest("GET", "/db/_user/?"+paramNameOnly+"=false", "")
		assertStatus(t, response, 200)
		err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
		assert.NoError(t, err)
		assert.Equal(t, i, len(responseUsers))

		// Check property values, and validate no duplicate users returned in response
		userMap := make(map[string]interface{})
		for _, principal := range responseUsers {
			if *principal.Name != "user5" {
				assert.Equal(t, *principal.Name+"@foo.com", principal.Email)
			}
			if *principal.Name == "user3" || *principal.Name == "user8" {
				assert.Equal(t, true, *principal.Disabled)
			} else {
				assert.Equal(t, false, *principal.Disabled)
			}
			_, ok := userMap[*principal.Name]
			assert.False(t, ok)
			userMap[*principal.Name] = struct{}{}
		}
	}
}

// TestUsersAPIDetailsWithLimit tests users endpoint with name_only=false and limit
func TestUsersAPIDetailsWithLimit(t *testing.T) {

	if base.TestsDisableGSI() {
		t.Skip("This test only works with Couchbase Server and UseViews=false")
	}

	// Create rest tester with low pagination limit
	rtConfig := &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				QueryPaginationLimit: base.IntPtr(5),
				// Disable the guest user to support testing the zero user boundary condition
				Guest: &db.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	// Validate the zero user case with limit
	var responseUsers []db.PrincipalConfig
	response := rt.SendAdminRequest("GET", "/db/_user/?"+paramNameOnly+"=false&"+paramLimit+"=10", "")
	assertStatus(t, response, 200)
	err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(responseUsers))

	// Create users
	numUsers := 12
	for i := 1; i <= numUsers; i++ {
		userName := fmt.Sprintf("user%d", i)
		response := rt.SendAdminRequest("PUT", "/db/_user/"+userName, `{"password":"letmein", "admin_channels":["foo", "bar"]}`)
		assertStatus(t, response, 201)
	}

	// limit without name_only=false should return Bad Request
	response = rt.SendAdminRequest("GET", "/db/_user/?"+paramLimit+"=10", "")
	assertStatus(t, response, 400)

	testCases := []struct {
		name          string
		limit         string
		expectedCount int
	}{
		{
			name:          "limit<pagination limit",
			limit:         "3",
			expectedCount: 3,
		},
		{
			name:          "limit=pagination limit",
			limit:         "5",
			expectedCount: 5,
		},
		{
			name:          "limit>pagination limit",
			limit:         "8",
			expectedCount: 8,
		},
		{
			name:          "limit=multiple of pagination limit",
			limit:         "10",
			expectedCount: 10,
		},
		{
			name:          "limit>multiple of pagination limit",
			limit:         "11",
			expectedCount: 11,
		},
		{
			name:          "limit>resultset",
			limit:         "25",
			expectedCount: numUsers,
		},
		{
			name:          "limit=0",
			limit:         "0",
			expectedCount: numUsers,
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			response = rt.SendAdminRequest("GET", "/db/_user/?"+paramNameOnly+"=false&"+paramLimit+"="+testCase.limit, "")
			assertStatus(t, response, 200)
			err = json.Unmarshal(response.Body.Bytes(), &responseUsers)
			assert.NoError(t, err)
			assert.Equal(t, testCase.expectedCount, len(responseUsers))

			// validate no duplicate users returned in response
			userMap := make(map[string]interface{})
			for _, principal := range responseUsers {
				_, ok := userMap[*principal.Name]
				assert.False(t, ok)
				userMap[*principal.Name] = struct{}{}
			}
		})
	}

}

func TestUserAPI(t *testing.T) {

	// PUT a user
	rt := NewRestTester(t, nil)
	defer rt.Close()

	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/snej", ""), 404)
	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)

	// GET the user and make sure the result is OK
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.Equals(t, body["name"], "snej")
	goassert.Equals(t, body["email"], "jens@couchbase.com")
	goassert.DeepEquals(t, body["admin_channels"], []interface{}{"bar", "foo"})
	goassert.DeepEquals(t, body["all_channels"], []interface{}{"!", "bar", "foo"})
	goassert.Equals(t, body["password"], nil)

	// Check the list of all users:
	response = rt.SendAdminRequest("GET", "/db/_user/", "")
	assertStatus(t, response, 200)
	goassert.Equals(t, string(response.Body.Bytes()), `["snej"]`)

	// Check that the actual User object is correct:
	user, _ := rt.ServerContext().Database("db").Authenticator().GetUser("snej")
	goassert.Equals(t, user.Name(), "snej")
	goassert.Equals(t, user.Email(), "jens@couchbase.com")
	goassert.DeepEquals(t, user.ExplicitChannels(), channels.TimedSet{"bar": channels.NewVbSimpleSequence(0x1), "foo": channels.NewVbSimpleSequence(0x1)})
	goassert.True(t, user.Authenticate("letmein"))

	// Change the password and verify it:
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"123", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 200)

	user, _ = rt.ServerContext().Database("db").Authenticator().GetUser("snej")
	goassert.True(t, user.Authenticate("123"))

	// DELETE the user
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/snej", ""), 200)
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/snej", ""), 404)

	// POST a user
	response = rt.SendAdminRequest("POST", "/db/_user", `{"name":"snej", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 301)
	rt.Bucket().Dump()

	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"snej", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.Equals(t, body["name"], "snej")

	// Create a role
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response = rt.SendAdminRequest("PUT", "/db/_role/hipster", `{"admin_channels":["fedoras", "fixies"]}`)
	assertStatus(t, response, 201)

	// Give the user that role
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"admin_channels":["foo", "bar"],"admin_roles":["hipster"]}`)
	assertStatus(t, response, 200)

	// GET the user and verify that it shows the channels inherited from the role
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	assertStatus(t, response, 200)
	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	goassert.DeepEquals(t, body["admin_roles"], []interface{}{"hipster"})
	goassert.DeepEquals(t, body["all_channels"], []interface{}{"!", "bar", "fedoras", "fixies", "foo"})

	// DELETE the user
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/snej", ""), 200)

	// POST a user with URL encoded '|' in name see #2870
	assertStatus(t, rt.SendAdminRequest("POST", "/db/_user/", `{"name":"0%7C59", "password":"letmein", "admin_channels":["foo", "bar"]}`), 201)

	// GET the user, will fail
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%7C59", ""), 404)

	// DELETE the user, will fail
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%7C59", ""), 404)

	// GET the user, double escape username, will succeed
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%257C59", ""), 200)

	// DELETE the user, double escape username, will succeed
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%257C59", ""), 200)

	// POST a user with URL encoded '|' and non-encoded @ in name see #2870
	assertStatus(t, rt.SendAdminRequest("POST", "/db/_user/", `{"name":"0%7C@59", "password":"letmein", "admin_channels":["foo", "bar"]}`), 201)

	// GET the user, will fail
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%7C@59", ""), 404)

	// DELETE the user, will fail
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%7C@59", ""), 404)

	// GET the user, double escape username, will succeed
	assertStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%257C%4059", ""), 200)

	// DELETE the user, double escape username, will succeed
	assertStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%257C%4059", ""), 200)

}

func TestUnauthorizedAccessForDB(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	dbName := "db"

	response := rt.SendRequest(http.MethodGet, "/"+dbName+"/", "")
	assertStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendRequest(http.MethodGet, "/notadb/", "")
	assertStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	// create sessions before users
	const alice = "alice"
	const bob = "bob"
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice,
		`{"name": "`+alice+`", "password": "letmein"}`)
	assertStatus(t, response, http.StatusCreated)

	response = rt.SendRequest(http.MethodPost,
		"/"+dbName+"/_session",
		`{"name": "`+alice+`", "password": "letmein"}`)
	assertStatus(t, response, http.StatusOK)

	cookie := response.Header().Get("Set-Cookie")
	aliceSessionHeaders := map[string]string{
		"Cookie": cookie,
	}

	// alice user can see realdb
	response = rt.SendUserRequestWithHeaders(http.MethodGet, "/"+dbName+"/", "", nil, alice, "letmein")
	assertStatus(t, response, http.StatusOK)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/"+dbName+"/", "", aliceSessionHeaders)
	assertStatus(t, response, http.StatusOK)

	response = rt.SendUserRequestWithHeaders(http.MethodGet, "/notadb/", "", nil, alice, "letmein")
	assertStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/notadb/", "", aliceSessionHeaders)
	assertStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)

	response = rt.SendUserRequestWithHeaders(http.MethodGet, "/"+dbName+"/", "", nil, bob, "letmein")
	assertStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)

}
