package rest

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
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
	assert.Equal(t, "snej", body["name"])
	assert.Equal(t, "jens@couchbase.com", body["email"])
	assert.Equal(t, []interface{}{"bar", "foo"}, body["admin_channels"])
	assert.Equal(t, []interface{}{"!", "bar", "foo"}, body["all_channels"])
	assert.Equal(t, nil, body["password"])

	// Check the list of all users:
	response = rt.SendAdminRequest("GET", "/db/_user/", "")
	assertStatus(t, response, 200)
	assert.Equal(t, `["snej"]`, string(response.Body.Bytes()))

	// Check that the actual User object is correct:
	user, _ := rt.ServerContext().Database("db").Authenticator(base.TestCtx(t)).GetUser("snej")
	assert.Equal(t, "snej", user.Name())
	assert.Equal(t, "jens@couchbase.com", user.Email())
	assert.Equal(t, channels.TimedSet{"bar": channels.NewVbSimpleSequence(0x1), "foo": channels.NewVbSimpleSequence(0x1)}, user.ExplicitChannels())
	assert.True(t, user.Authenticate("letmein"))

	// Change the password and verify it:
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"123", "admin_channels":["foo", "bar"]}`)
	assertStatus(t, response, 200)

	user, _ = rt.ServerContext().Database("db").Authenticator(base.TestCtx(t)).GetUser("snej")
	assert.True(t, user.Authenticate("123"))

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
	assert.Equal(t, "snej", body["name"])

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
	assert.Equal(t, []interface{}{"hipster"}, body["admin_roles"])
	assert.Equal(t, []interface{}{"!", "bar", "fedoras", "fixies", "foo"}, body["all_channels"])

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
