// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package rest

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
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
				Guest: &auth.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			},
		},
	}
	rt := NewRestTester(t,
		rtConfig)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	// Validate the zero user case
	var responseUsers []string
	response := rt.SendAdminRequest("GET", "/db/_user/", "")
	RequireStatus(t, response, 200)
	err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(responseUsers))

	// Test for user counts going from 1 to a few multiples of QueryPaginationLimit to check boundary conditions
	for i := 1; i < 13; i++ {
		userName := fmt.Sprintf("user%d", i)
		response := rt.SendAdminRequest("PUT", "/db/_user/"+userName, GetUserPayload(t, "", "letmein", "", collection, []string{"foo", "bar"}, nil))
		RequireStatus(t, response, 201)

		// check user count
		response = rt.SendAdminRequest("GET", "/db/_user/", "")
		RequireStatus(t, response, 200)
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
				Guest: &auth.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	// Validate error handling
	response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_user/?%s=false", paramNameOnly), "")
	RequireStatus(t, response, 400)

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
				Guest: &auth.PrincipalConfig{
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
		GuestEnabled: false,
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()

	validateUsersNameOnlyFalse(t, rt)

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
				Guest: &auth.PrincipalConfig{
					Disabled: base.BoolPtr(false),
				},
			},
		},
	}
	rt := NewRestTester(t, rtConfig)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	// Validate the zero user case with limit
	var responseUsers []auth.PrincipalConfig
	response := rt.SendAdminRequest("GET", "/db/_user/?"+paramNameOnly+"=false&"+paramLimit+"=10", "")
	RequireStatus(t, response, 200)
	err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(responseUsers))

	// Create users
	numUsers := 12
	for i := 1; i <= numUsers; i++ {
		userName := fmt.Sprintf("user%d", i)
		response := rt.SendAdminRequest("PUT", "/db/_user/"+userName, GetUserPayload(t, "", "letmein", "", collection, []string{"foo", "bar"}, nil))
		RequireStatus(t, response, 201)
	}

	// limit without name_only=false should return Bad Request
	response = rt.SendAdminRequest("GET", "/db/_user/?"+paramLimit+"=10", "")
	RequireStatus(t, response, 400)

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
			RequireStatus(t, response, 200)
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
	ctx := rt.Context()
	collection := rt.GetSingleTestDatabaseCollection()
	c := collection.Name
	s := collection.ScopeName

	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/snej", ""), 404)

	response := rt.SendAdminRequest("PUT", "/db/_user/snej", GetUserPayload(t, "", "letmein", "jens@couchbase.com", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 201)

	user, err := rt.ServerContext().Database(ctx, "db").Authenticator(ctx).GetUser("snej")
	require.NoError(t, err)
	require.NotNil(t, user)
	// GET the user and make sure the result is OK
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	RequireStatus(t, response, 200)

	princ := auth.PrincipalConfig{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &princ))
	allChans := princ.GetChannels(s, c).ToArray()
	adminChans := princ.GetExplicitChannels(s, c).ToArray()
	sort.Strings(allChans)
	sort.Strings(adminChans)
	assert.Equal(t, "snej", *princ.Name)
	assert.Equal(t, "jens@couchbase.com", *princ.Email)
	assert.Equal(t, []string{"bar", "foo"}, adminChans)
	assert.Equal(t, []string{"!", "bar", "foo"}, allChans)
	assert.Nil(t, princ.Password)

	// Check the list of all users:
	response = rt.SendAdminRequest("GET", "/db/_user/", "")
	RequireStatus(t, response, 200)
	assert.Equal(t, `["snej"]`, string(response.Body.Bytes()))

	// Check that the actual User object is correct:
	user, _ = rt.ServerContext().Database(ctx, "db").Authenticator(ctx).GetUser("snej")
	assert.Equal(t, "snej", user.Name())
	assert.Equal(t, "jens@couchbase.com", user.Email())
	assert.Equal(t, channels.TimedSet{"bar": channels.NewVbSimpleSequence(0x1), "foo": channels.NewVbSimpleSequence(0x1)}, user.CollectionExplicitChannels(s, c))
	assert.True(t, user.Authenticate("letmein"))

	// Change the password and verify it:
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", GetUserPayload(t, "", "123", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 200)

	user, _ = rt.ServerContext().Database(ctx, "db").Authenticator(ctx).GetUser("snej")
	assert.True(t, user.Authenticate("123"))

	// DELETE the user
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/snej", ""), 200)
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/snej", ""), 404)

	// POST a user
	response = rt.SendAdminRequest("POST", "/db/_user", GetUserPayload(t, "snej", "letmein", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 301)

	response = rt.SendAdminRequest("POST", "/db/_user/", GetUserPayload(t, "snej", "letmein", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	RequireStatus(t, response, 200)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "snej", body["name"])

	// Create a role
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_role/hipster", ""), 404)
	response = rt.SendAdminRequest("PUT", "/db/_role/hipster", GetRolePayload(t, "", "", collection, []string{"fedoras", "fixies"}))
	RequireStatus(t, response, 201)

	// Give the user that role
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", GetUserPayload(t, "", "", "", collection, []string{"foo", "bar"}, []string{"hipster"}))
	RequireStatus(t, response, 200)

	// GET the user and verify that it shows the channels inherited from the role
	response = rt.SendAdminRequest("GET", "/db/_user/snej", "")
	RequireStatus(t, response, 200)
	princConfig := auth.PrincipalConfig{}
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &princConfig))
	assert.Equal(t, []string{"hipster"}, princConfig.ExplicitRoleNames.ToArray())
	allChans = princConfig.GetChannels(s, c).ToArray()
	sort.Strings(allChans)
	assert.Equal(t, []string{"!", "bar", "fedoras", "fixies", "foo"}, allChans)

	// DELETE the user
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/snej", ""), 200)

	// POST a user with URL encoded '|' in name see #2870
	RequireStatus(t, rt.SendAdminRequest("POST", "/db/_user/", GetUserPayload(t, "0%7C59", "letmein", "", collection, []string{"foo", "bar"}, nil)), 201)

	// GET the user, will fail
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%7C59", ""), 404)

	// DELETE the user, will fail
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%7C59", ""), 404)

	// GET the user, double escape username, will succeed
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%257C59", ""), 200)

	// DELETE the user, double escape username, will succeed
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%257C59", ""), 200)

	// POST a user with URL encoded '|' and non-encoded @ in name see #2870
	RequireStatus(t, rt.SendAdminRequest("POST", "/db/_user/", GetUserPayload(t, "0%7C@59", "letmein", "", collection, []string{"foo", "bar"}, nil)), 201)

	// GET the user, will fail
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%7C@59", ""), 404)

	// DELETE the user, will fail
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%7C@59", ""), 404)

	// GET the user, double escape username, will succeed
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/0%257C%4059", ""), 200)

	// DELETE the user, double escape username, will succeed
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/0%257C%4059", ""), 200)

}

func TestGuestUser(t *testing.T) {

	guestUserEndpoint := fmt.Sprintf("/db/_user/%s", base.GuestUsername)

	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest(http.MethodGet, guestUserEndpoint, "")
	RequireStatus(t, response, http.StatusOK)
	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, base.GuestUsername, body["name"])
	// This ain't no admin-party, this ain't no nightclub, this ain't no fooling around:
	assert.Nil(t, body["admin_channels"])

	// Disable the guest user:
	response = rt.SendAdminRequest(http.MethodPut, guestUserEndpoint, `{"disabled":true}`)
	RequireStatus(t, response, http.StatusOK)

	// Get guest user and verify it is now disabled:
	response = rt.SendAdminRequest(http.MethodGet, guestUserEndpoint, "")
	RequireStatus(t, response, http.StatusOK)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, base.GuestUsername, body["name"])
	assert.True(t, body["disabled"].(bool))

	// Check that the actual User object is correct:
	ctx := rt.Context()
	user, _ := rt.ServerContext().Database(ctx, "db").Authenticator(ctx).GetUser("")
	assert.Empty(t, user.Name())
	assert.Nil(t, user.CollectionExplicitChannels(base.DefaultScope, base.DefaultCollection))
	assert.True(t, user.Disabled())

	// We can't delete the guest user, but we should get a reasonable error back.
	response = rt.SendAdminRequest(http.MethodDelete, guestUserEndpoint, "")
	RequireStatus(t, response, http.StatusMethodNotAllowed)
}

func TestUserAndRoleResponseContentType(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	// Create a user 'christopher' through PUT request with empty request body.
	var responseBody db.Body
	response := rt.SendAdminRequest(http.MethodPut, "/db/_user/christopher", "")
	assert.Equal(t, http.StatusBadRequest, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Create a user 'charles' through POST request with empty request body.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_user/charles", "")
	assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Create a user 'alice' through PUT request.
	response = rt.SendAdminRequest(http.MethodPut, "/db/_user/alice", GetUserPayload(t, "", "cGFzc3dvcmQ=", "alice@couchbase.com", collection, []string{"foo", "bar"}, nil))
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create another user 'bob' through POST request.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_user/", GetUserPayload(t, "bob", "cGFzc3dvcmQ=", "bob@couchbase.com", collection, []string{"foo", "bar"}, nil))
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the user details of user 'alice' through GET request.
	response = rt.SendAdminRequest(http.MethodGet, "/db/_user/alice", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Get the list of users through GET request.
	var users []string
	response = rt.SendAdminRequest(http.MethodGet, "/db/_user/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &users))
	assert.Subset(t, []string{"alice", "bob"}, users)

	// Check whether the /db/_user/bob resource exist on the server.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_user/bob", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the list of users through HEAD request.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_user/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete user 'alice'
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/alice", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete GUEST user instead of disabling.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/GUEST", "")
	assert.Equal(t, http.StatusMethodNotAllowed, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))

	// Delete user 'eve' who doesn't exists at this point of time.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve", "")
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))

	// Create a new user and save to database to create user session.
	authenticator := auth.NewAuthenticator(rt.MetadataStore(), nil, rt.GetDatabase().AuthenticatorOptions(rt.Context()))
	user, err := authenticator.NewUser("eve", "cGFzc3dvcmQ=", channels.BaseSetOf(t, "*"))
	assert.NoError(t, err, "Couldn't create new user")
	assert.NoError(t, authenticator.Save(user), "Couldn't save new user")

	// Create user session to check delete session request.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"eve"}`)
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))
	sessionId, _ := responseBody["session_id"].(string)
	require.NotEmpty(t, sessionId, "Couldn't parse sessionID from response body")

	// Delete user session using /db/_user/eve/_session/{sessionId}.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve/_session/"+sessionId, "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create user session to check delete session request.
	response = rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"eve"}`)
	assert.Equal(t, http.StatusOK, response.Code)
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))

	// Delete user session using /db/_user/eve/_session request.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_user/eve/_session", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create a role 'developer' through POST request
	response = rt.SendAdminRequest(http.MethodPost, "/db/_role/", GetRolePayload(t, "developer", "", collection, []string{"channel1", "channel2"}))
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Create another role 'coder' through PUT request.
	response = rt.SendAdminRequest(http.MethodPut, "/db/_role/coder", GetRolePayload(t, "", "", collection, []string{"channel3", "channel4"}))
	assert.Equal(t, http.StatusCreated, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Check whether the /db/_role/ resource exist on the server.
	response = rt.SendAdminRequest(http.MethodHead, "/db/_role/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Get the created roles through GET request.
	var roles []string
	response = rt.SendAdminRequest(http.MethodGet, "/db/_role/", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &roles))
	assert.Subset(t, []string{"coder", "developer"}, roles)

	// Delete role 'coder' from database.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_role/coder", "")
	assert.Equal(t, http.StatusOK, response.Code)
	assert.Empty(t, response.Header().Get("Content-Type"))

	// Delete role who doesn't exist.
	response = rt.SendAdminRequest(http.MethodDelete, "/db/_role/programmer", "")
	assert.Equal(t, http.StatusNotFound, response.Code)
	assert.Equal(t, "application/json", response.Header().Get("Content-Type"))
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &responseBody))
}

func TestUserXattrsRawGet(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("Test requires xattrs to be enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	docKey := t.Name()
	xattrKey := "xattrKey"

	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{
			DbConfig: DbConfig{
				AutoImport:   true,
				UserXattrKey: xattrKey,
			},
		},
	})
	defer rt.Close()

	userXattrStore, ok := base.AsUserXattrStore(rt.GetSingleDataStore())
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, "{}")
	RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())

	_, err := userXattrStore.WriteUserXattr(docKey, xattrKey, "val")
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImportStats.ImportCount.Value() == 1
	})
	require.NoError(t, err)

	resp = rt.SendAdminRequest("GET", "/{{.keyspace}}/_raw/"+docKey, "")
	RequireStatus(t, resp, http.StatusOK)

	var RawReturn struct {
		Meta struct {
			Xattrs map[string]interface{} `json:"xattrs"`
		} `json:"_meta"`
	}

	err = json.Unmarshal(resp.BodyBytes(), &RawReturn)
	require.NoError(t, err)

	assert.Equal(t, "val", RawReturn.Meta.Xattrs[xattrKey])
}

func TestObtainUserChannelsForDeletedRoleCasFail(t *testing.T) {
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip LeakyBucket test when running in integration")
	}

	testCases := []struct {
		Name      string
		RunBefore bool
	}{
		{
			"Delete On GetUser",
			true,
		},
		{
			"Delete On inheritedChannels",
			false,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.Name, func(t *testing.T) {
			rt := NewRestTester(t,
				&RestTesterConfig{
					SyncFn: `
			function(doc, oldDoc){
				if (doc._id === 'roleChannels'){
					access('role:role', doc.channels)
				}
				if (doc._id === 'userRoles'){
					role('user', doc.roles)
				}
			}
		`,
				})
			defer rt.Close()
			collection := rt.GetSingleTestDatabaseCollection()
			c := collection.Name
			s := collection.ScopeName

			// Create role
			resp := rt.SendAdminRequest("PUT", "/db/_role/role", GetRolePayload(t, "", "", collection, []string{"channel"}))
			RequireStatus(t, resp, http.StatusCreated)

			// Create user
			resp = rt.SendAdminRequest("PUT", "/db/_user/user", `{"password": "pass"}`)
			RequireStatus(t, resp, http.StatusCreated)

			// Add channel to role
			resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/roleChannels", `{"channels": "inherit"}`)
			RequireStatus(t, resp, http.StatusCreated)

			// Add role to user
			resp = rt.SendAdminRequest("PUT", "/{{.keyspace}}/userRoles", `{"roles": "role:role"}`)
			RequireStatus(t, resp, http.StatusCreated)
			tbDatastore := rt.TestBucket.GetSingleDataStore()
			leakyDataStore, ok := base.AsLeakyDataStore(tbDatastore)
			require.True(t, ok)

			triggerCallback := false
			leakyDataStore.SetUpdateCallback(func(key string) {
				if triggerCallback {
					triggerCallback = false
					resp = rt.SendAdminRequest("DELETE", "/db/_role/role", ``)
					RequireStatus(t, resp, http.StatusOK)
				}
			})

			if testCase.RunBefore {
				triggerCallback = true
			}

			authenticator := rt.GetDatabase().Authenticator(base.TestCtx(t))
			user, err := authenticator.GetUser("user")
			assert.NoError(t, err)

			if !testCase.RunBefore {
				triggerCallback = true
			}

			assert.Equal(t, []string{"!"}, user.InheritedCollectionChannels(s, c).AllKeys())

			// Ensure callback ran
			assert.False(t, triggerCallback)
		})

	}
}

func TestUserPasswordValidation(t *testing.T) {

	// PUT a user
	rt := NewRestTester(t, nil)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	response := rt.SendAdminRequest("PUT", "/db/_user/snej", GetUserPayload(t, "", "letmein", "jens@couchbase.com", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 201)

	// PUT a user without a password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", GetUserPayload(t, "", "", "ajres@couchbase.com", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 400)

	// POST a user without a password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", GetUserPayload(t, "ajresnopassword", "", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 400)

	// PUT a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", GetUserPayload(t, "ajresnopassword", "in", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 400)

	// POST a user with a two character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", GetUserPayload(t, "ajresnopassword", "in", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 400)

	// PUT a user with a zero character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/ajresnopassword", GetUserPayload(t, "ajresnopassword", "", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 400)

	// POST a user with a zero character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", GetUserPayload(t, "ajresnopassword", "", "", collection, []string{"foo", "bar"}, nil))
	RequireStatus(t, response, 400)

	// PUT update a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"an"}`)
	RequireStatus(t, response, 400)

	// PUT update a user with a one character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"a"}`)
	RequireStatus(t, response, 400)

	// PUT update a user with a zero character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":""}`)
	RequireStatus(t, response, 400)

	// PUT update a user with a three character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"abc"}`)
	RequireStatus(t, response, 200)
}
func TestUserAllowEmptyPassword(t *testing.T) {

	// PUT a user
	rt := NewRestTester(t, &RestTesterConfig{DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{AllowEmptyPassword: base.BoolPtr(true)}}})
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 201)

	// PUT a user without a password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword1", `{"email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 201)

	// POST a user without a password, should succeed
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword2", "email":"ajres@couchbase.com", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 201)

	// PUT a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword3", `{"email":"ajres@couchbase.com", "password":"in", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 400)

	// POST a user with a two character password, should fail
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword4", "email":"ajres@couchbase.com", "password":"an", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 400)

	// PUT a user with a zero character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/nopassword5", `{"email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 201)

	// POST a user with a zero character password, should succeed
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"nopassword6", "email":"ajres@couchbase.com", "password":"", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 201)

	// PUT update a user with a two character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"an"}`)
	RequireStatus(t, response, 400)

	// PUT update a user with a one character password, should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"a"}`)
	RequireStatus(t, response, 400)

	// PUT update a user with a zero character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":""}`)
	RequireStatus(t, response, 200)

	// PUT update a user with a three character password, should succeed
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"password":"abc"}`)
	RequireStatus(t, response, 200)
}

func TestPrincipalForbidUpdatingChannels(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{})
	defer rt.Close()

	// Users
	// PUT admin_channels
	response := rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 201)

	// PUT all_channels - should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "all_channels":["baz"]}`)
	RequireStatus(t, response, 400)

	// PUT admin_roles
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "password":"letmein", "admin_roles":["foo", "bar"]}`)
	RequireStatus(t, response, 200)

	// PUT roles - should fail
	response = rt.SendAdminRequest("PUT", "/db/_user/snej", `{"email":"jens@couchbase.com", "roles":["baz"]}`)
	RequireStatus(t, response, 400)

	// Roles
	// PUT admin_channels
	response = rt.SendAdminRequest("PUT", "/db/_role/test", `{"admin_channels":["foo", "bar"]}`)
	RequireStatus(t, response, 201)

	// PUT all_channels - should fail
	response = rt.SendAdminRequest("PUT", "/db/_role/test", `{"all_channels":["baz"]}`)
	RequireStatus(t, response, 400)
}

// Test user access grant while that user has an active changes feed.  (see issue #880)
func TestUserAccessRace(t *testing.T) {

	base.LongRunningTest(t)

	// This test only runs against Walrus due to known sporadic failures.
	// See https://github.com/couchbase/sync_gateway/issues/3006
	if !base.UnitTestUrlIsWalrus() {
		t.Skip("Skip this test under integration testing")
	}

	syncFunction := `
function(doc, oldDoc) {
  if (doc.type == "list") {
    channel("list-"+doc._id);
  } else if (doc.type == "profile") {
    channel("profiles");
    var user = doc._id.substring(doc._id.indexOf(":")+1);
    if (user !== doc.user_id) {
      throw({forbidden : "profile user_id must match docid"})
    }
    requireUser(user);
    access(user, "profiles");
    channel('profile-'+user);
  } else if (doc.type == "Want") {
    var parts = doc._id.split("-");
    var user = parts[1];
    var i = parts[2];
    requireUser(user);
    channel('profile-'+user);
    access(user, 'list-'+i);
  }
}

`
	rtConfig := RestTesterConfig{
		SyncFn: syncFunction,
	}
	rt := NewRestTester(t,
		&rtConfig)
	defer rt.Close()
	collection := rt.GetSingleTestDatabaseCollection()

	response := rt.SendAdminRequest("PUT", "/{{.db}}/_user/bernard", GetUserPayload(t, "bernard", "letmein", "", collection, []string{"profile-bernard"}, nil))
	RequireStatus(t, response, 201)

	// Try to force channel initialisation for user bernard
	response = rt.SendAdminRequest("GET", "/db/_user/bernard", "")
	RequireStatus(t, response, 200)

	// Create list docs
	input := `{"docs": [`

	for i := 1; i <= 100; i++ {
		if i > 1 {
			input = input + `,`
		}
		docId := fmt.Sprintf("l_%d", i)
		input = input + fmt.Sprintf(`{"_id":"%s", "type":"list"}`, docId)
	}
	input = input + `]}`
	response = rt.SendAdminRequest("POST", "/{{.keyspace}}/_bulk_docs", input)
	RequireStatus(t, response, http.StatusCreated)

	// Start changes feed
	var wg sync.WaitGroup

	// Init the public handler, to avoid data race initializing in the two usages below.
	_ = rt.SendRequest("GET", "/db", "")

	wg.Add(1)

	numExpectedChanges := 201

	go func() {
		defer wg.Done()

		since := ""

		maxTries := 10
		numTries := 0

		changesAccumulated := []db.ChangeEntry{}

		for {

			// Timeout allows us to read continuous changes after processing is complete.  Needs to be long enough to
			// ensure it doesn't terminate before the first change is sent.
			log.Printf("Invoking _changes?feed=continuous&since=%s&timeout=2000", since)
			changesResponse := rt.SendUserRequest("GET", fmt.Sprintf("/{{.keyspace}}/_changes?feed=continuous&since=%s&timeout=2000", since), "", "bernard")

			changes, err := rt.ReadContinuousChanges(changesResponse)
			assert.NoError(t, err)

			changesAccumulated = append(changesAccumulated, changes...)

			if len(changesAccumulated) >= numExpectedChanges {
				log.Printf("Got numExpectedChanges (%d).  Done", numExpectedChanges)
				break
			} else {
				log.Printf("Only received %d out of %d expected changes.  Attempt %d / %d.", len(changesAccumulated), numExpectedChanges, numTries, maxTries)
			}

			// Advance the since value if we got any changes
			if len(changes) > 0 {
				since = changes[len(changes)-1].Seq.String()
				log.Printf("Setting since value to: %s.", since)
			}

			numTries++
			if numTries > maxTries {
				t.Errorf("Giving up trying to receive %d changes.  Only received %d", numExpectedChanges, len(changesAccumulated))
				return
			}

		}

	}()

	// Make bulk docs calls, 100 docs each, all triggering access grants to the list docs.
	for j := 0; j < 1; j++ {

		input := `{"docs": [`
		for i := 1; i <= 100; i++ {
			if i > 1 {
				input = input + `,`
			}
			k := j*100 + i
			docId := fmt.Sprintf("Want-bernard-l_%d", k)
			input = input + fmt.Sprintf(`{"_id":"%s", "type":"Want", "owner":"bernard"}`, docId)
		}
		input = input + `]}`

		log.Printf("Sending 2nd round of _bulk_docs")
		response = rt.SendUserRequest("POST", "/{{.keyspace}}/_bulk_docs", input, "bernard")
		RequireStatus(t, response, http.StatusCreated)
		log.Printf("Sent 2nd round of _bulk_docs")

	}

	// wait for changes feed to complete (time out)
	wg.Wait()
}

// Test user delete while that user has an active changes feed (see issue 809)
func TestUserDeleteDuringChangesWithAccess(t *testing.T) {

	base.SetUpTestLogging(t, base.LevelInfo, base.KeyChanges, base.KeyCache, base.KeyHTTP)

	rtConfig := RestTesterConfig{SyncFn: `function(doc) {channel(doc.channel); if(doc.type == "setaccess") { access(doc.owner, doc.channel);}}`}
	rt := NewRestTester(t, &rtConfig)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_user/bernard", `{"name":"bernard", "password":"letmein", "admin_channels":["foo"]}`)
	RequireStatus(t, response, 201)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		changesResponse := rt.Send(RequestByUser("GET", "/{{.keyspace}}/_changes?feed=continuous&since=0&timeout=3000", "", "bernard"))
		// When testing single threaded, this reproduces the issue described in #809.
		// When testing multithreaded (-cpu 4 -race), there are three (valid) possibilities"
		// 1. The DELETE gets processed before the _changes auth completes: this will return 401
		// 2. The _changes request gets processed before the DELETE: the changes response will be closed when the user is deleted
		// 3. The DELETE is processed after the _changes auth completes, but before the MultiChangesFeed is instantiated.  The
		//  changes feed doesn't have a trigger to attempt a reload of the user in this scenario, so will continue until disconnected
		//  by the client.  This should be fixed more generally (to terminate all active user sessions when the user is deleted, not just
		//  changes feeds) but that enhancement is too high risk to introduce at this time.  The timeout on changes will terminate the unit
		//  test.
		if changesResponse.Code == 401 {
			// case 1 - ok
		} else {
			// case 2 - ensure no error processing the changes response.  The number of entries may vary, depending
			// on whether the changes loop performed an additional iteration before catching the deleted user.
			_, err := rt.ReadContinuousChanges(changesResponse)
			assert.NoError(t, err)
		}
	}()

	// TODO: sleep required to ensure the changes feed iteration starts before the delete gets processed.
	time.Sleep(500 * time.Millisecond)
	rt.SendAdminRequest("PUT", "/{{.keyspace}}/bernard_doc1", `{"type":"setaccess", "owner":"bernard","channel":"foo"}`)
	rt.SendAdminRequest("DELETE", "/db/_user/bernard", "")
	rt.SendAdminRequest("PUT", "/{{.keyspace}}/manny_doc1", `{"type":"setaccess", "owner":"manny","channel":"bar"}`)
	rt.SendAdminRequest("PUT", "/{{.keyspace}}/bernard_doc2", `{"type":"general", "channel":"foo"}`)

	// case 3
	for i := 0; i <= 5; i++ {
		docId := fmt.Sprintf("/{{.keyspace}}/bernard_doc%d", i+3)
		response = rt.SendAdminRequest("PUT", docId, `{"type":"setaccess", "owner":"bernard", "channel":"foo"}`)
		RequireStatus(t, response, http.StatusCreated)
	}

	wg.Wait()
}

// Helper functions

// validateUsersNameOnlyFalse validates query results with paramNameOnly=false.  Includes
// users with/without email, enabled/disabled, various limits
func validateUsersNameOnlyFalse(t *testing.T, rt *RestTester) {
	// Validate the zero user case
	var responseUsers []auth.PrincipalConfig
	response := rt.SendAdminRequest("GET", "/db/_user/?"+paramNameOnly+"=false", "")
	RequireStatus(t, response, 200)
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
		RequireStatus(t, response, 201)

		// check user count
		response = rt.SendAdminRequest("GET", "/db/_user/?"+paramNameOnly+"=false", "")
		RequireStatus(t, response, 200)
		err := json.Unmarshal(response.Body.Bytes(), &responseUsers)
		assert.NoError(t, err)
		assert.Equal(t, i, len(responseUsers))

		// Check property values, and validate no duplicate users returned in response
		userMap := make(map[string]interface{})
		for _, principal := range responseUsers {
			require.NotNil(t, principal.Name)
			if *principal.Name != "user5" {
				require.NotNil(t, principal.Email)
				assert.Equal(t, *principal.Name+"@foo.com", *principal.Email)
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

func TestFunkyUsernames(t *testing.T) {
	cases := []struct {
		Name     string
		UserName string
	}{
		{
			Name:     "hashes",
			UserName: "foo#bar",
		},
		{
			Name:     "question mark",
			UserName: "foo?bar",
		},
		{
			Name:     "dollars",
			UserName: "$foo$bar",
		},
		{
			Name:     "underscore prefix",
			UserName: "_sync-foobar",
		},
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			require.Truef(t, auth.IsValidPrincipalName(tc.UserName), "expected '%s' to be accepted", tc.UserName)
			rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
			defer rt.Close()

			ctx := rt.Context()
			a := rt.ServerContext().Database(ctx, "db").Authenticator(ctx)

			// Create a test user
			user, err := a.NewUser(tc.UserName, "letmein", channels.BaseSetOf(t, "foo"))
			require.NoError(t, err)
			require.NoError(t, a.Save(user))

			response := rt.SendUserRequest("PUT", "/{{.keyspace}}/AC+DC", `{"foo":"bar", "channels": ["foo"]}`, tc.UserName)
			RequireStatus(t, response, 201)

			response = rt.SendUserRequest("GET", "/{{.keyspace}}/_all_docs", "", tc.UserName)
			RequireStatus(t, response, 200)

			response = rt.SendUserRequest("GET", "/{{.keyspace}}/AC+DC", "", tc.UserName)
			RequireStatus(t, response, 200)
			var overlay struct {
				Rev string `json:"_rev"`
			}
			require.NoError(t, json.Unmarshal(response.Body.Bytes(), &overlay))

			response = rt.SendUserRequest("DELETE", "/{{.keyspace}}/AC+DC?rev="+overlay.Rev, "", tc.UserName)
			RequireStatus(t, response, 200)
		})
	}
}
func TestRemovingUserXattr(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	defer db.SuspendSequenceBatching()()

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	testCases := []struct {
		name          string
		autoImport    bool
		importTrigger func(t *testing.T, rt *RestTester, docKey string)
	}{
		{
			name:       "GET",
			autoImport: false,
			importTrigger: func(t *testing.T, rt *RestTester, docKey string) {
				resp := rt.SendAdminRequest("GET", "/{{.keyspace}}/"+docKey, "")
				RequireStatus(t, resp, http.StatusOK)
			},
		},
		{
			name:       "PUT",
			autoImport: false,
			importTrigger: func(t *testing.T, rt *RestTester, docKey string) {
				resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, "{}")
				RequireStatus(t, resp, http.StatusConflict)
			},
		},
		{
			name:       "Auto",
			autoImport: true,
			importTrigger: func(t *testing.T, rt *RestTester, docKey string) {
				// No op
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			docKey := testCase.name
			xattrKey := "myXattr"
			channelName := "testChan"

			// Sync function to set channel access to whatever xattr is
			rt := NewRestTester(t, &RestTesterConfig{
				DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
					AutoImport:   testCase.autoImport,
					UserXattrKey: xattrKey,
				}},
				SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
				}
			}`,
			})

			defer rt.Close()

			gocbBucket, ok := base.AsUserXattrStore(rt.GetSingleDataStore())
			if !ok {
				t.Skip("Test requires Couchbase Bucket")
			}

			// Initial PUT
			resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
			RequireStatus(t, resp, http.StatusCreated)

			// Add xattr
			_, err := gocbBucket.WriteUserXattr(docKey, xattrKey, channelName)
			assert.NoError(t, err)

			// Trigger import
			testCase.importTrigger(t, rt, docKey)
			err = rt.WaitForCondition(func() bool {
				return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
			})
			assert.NoError(t, err)

			// Get sync data for doc and ensure user xattr has been used correctly to set channel
			var syncData db.SyncData
			dataStore := rt.GetSingleDataStore()
			require.True(t, ok)
			_, err = dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData)
			assert.NoError(t, err)

			assert.Equal(t, []string{channelName}, syncData.Channels.KeySet())

			// Delete user xattr
			_, err = gocbBucket.DeleteUserXattr(docKey, xattrKey)
			assert.NoError(t, err)

			// Trigger import
			testCase.importTrigger(t, rt, docKey)
			err = rt.WaitForCondition(func() bool {
				return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
			})
			assert.NoError(t, err)

			// Ensure old channel set with user xattr has been removed
			var syncData2 db.SyncData
			_, err = dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData2)
			assert.NoError(t, err)

			assert.Equal(t, uint64(3), syncData2.Channels[channelName].Seq)
		})
	}
}

func TestUserXattrRevCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	ctx := base.TestCtx(t)
	docKey := t.Name()
	xattrKey := "channels"
	channelName := []string{"ABC", "DEF"}
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)
	syncFn := `function (doc, oldDoc, meta){
				if (meta.xattrs.channels !== undefined){
					channel(meta.xattrs.channels);
					console.log(JSON.stringify(meta));
				}
			}`

	// Sync function to set channel access to a channels UserXattrKey
	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:       true,
			UserXattrKey:     xattrKey,
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
		}},
		SyncFn: syncFn,
	})
	defer rt.Close()

	rt2 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),

		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:       true,
			UserXattrKey:     xattrKey,
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
		}},
		SyncFn: syncFn,
	})
	defer rt2.Close()

	dataStore := rt2.GetSingleDataStore()
	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	ctx = rt2.Context()
	a := rt2.ServerContext().Database(ctx, "db").Authenticator(ctx)
	userABC, err := a.NewUser("userABC", "letmein", channels.BaseSetOf(t, "ABC"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userABC))

	userDEF, err := a.NewUser("userDEF", "letmein", channels.BaseSetOf(t, "DEF"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userDEF))

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, "DEF")
	assert.NoError(t, err)

	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	RequireStatus(t, resp, http.StatusOK)

	// Add channel ABC to the userXattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// wait for import of the xattr change on both nodes
	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userABC", false)
	assert.NoError(t, err)
	_, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes", "userABC", false)
	assert.NoError(t, err)

	// GET the doc with userABC to ensure it is accessible on both nodes
	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userABC")
	assert.Equal(t, resp.Code, http.StatusOK)
	resp = rt.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userABC")
	assert.Equal(t, resp.Code, http.StatusOK)
}

func TestUserXattrDeleteWithRevCache(t *testing.T) {
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}
	ctx := base.TestCtx(t)
	// Sync function to set channel access to a channels UserXattrKey
	syncFn := `
			function (doc, oldDoc, meta){
				if (meta.xattrs.channels !== undefined){
					channel(meta.xattrs.channels);
					console.log(JSON.stringify(meta));
				}
			}`

	docKey := t.Name()
	xattrKey := "channels"
	tb := base.GetTestBucket(t)
	defer tb.Close(ctx)

	rt := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
			AutoImport:       true,
			UserXattrKey:     xattrKey,
		}},
		SyncFn: syncFn,
	})
	defer rt.Close()

	rt2 := NewRestTester(t, &RestTesterConfig{
		CustomTestBucket: tb.NoCloseClone(),
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			ImportPartitions: base.Uint16Ptr(2), // temporarily config to 2 import partitions (default 1 for rest tester) pending CBG-3438 + CBG-3439
			AutoImport:       true,
			UserXattrKey:     xattrKey,
		}},
		SyncFn: syncFn,
	})
	defer rt2.Close()

	dataStore := rt2.GetSingleDataStore()
	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	ctx = rt2.Context()
	a := rt2.ServerContext().Database(ctx, "db").Authenticator(ctx)

	userDEF, err := a.NewUser("userDEF", "letmein", channels.BaseSetOf(t, "DEF"))
	require.NoError(t, err)
	require.NoError(t, a.Save(userDEF))

	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	RequireStatus(t, resp, http.StatusCreated)
	require.NoError(t, rt.WaitForPendingChanges())

	// Write DEF to the userXattrStore to give userDEF access
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, "DEF")
	assert.NoError(t, err)

	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)

	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	RequireStatus(t, resp, http.StatusOK)

	// Delete DEF from the userXattr, removing the doc from channel DEF
	_, err = userXattrStore.DeleteUserXattr(docKey, xattrKey)
	assert.NoError(t, err)

	// wait for import of the xattr change on both nodes
	_, err = rt.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)
	_, err = rt2.WaitForChanges(1, "/{{.keyspace}}/_changes", "userDEF", false)
	assert.NoError(t, err)

	// GET the doc with userDEF on both nodes to ensure userDEF no longer has access
	resp = rt2.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	assert.Equal(t, resp.Code, http.StatusForbidden)
	resp = rt.SendUserRequest("GET", "/{{.keyspace}}/"+docKey, ``, "userDEF")
	assert.Equal(t, resp.Code, http.StatusForbidden)
}

func TestUserXattrAvoidRevisionIDGeneration(t *testing.T) {
	if !base.TestUseXattrs() {
		t.Skip("This test only works with XATTRS enabled")
	}

	if !base.IsEnterpriseEdition() {
		t.Skipf("test is EE only - user xattrs")
	}

	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	docKey := t.Name()
	xattrKey := "myXattr"
	channelName := "testChan"

	// Sync function to set channel access to whatever xattr is
	rt := NewRestTester(t, &RestTesterConfig{
		DatabaseConfig: &DatabaseConfig{DbConfig: DbConfig{
			AutoImport:   true,
			UserXattrKey: xattrKey,
		}},
		SyncFn: `
			function (doc, oldDoc, meta){
				if (meta.xattrs.myXattr !== undefined){
					channel(meta.xattrs.myXattr);
					console.log(JSON.stringify(meta));
				}
			}`,
	})

	defer rt.Close()

	dataStore := rt.GetSingleDataStore()
	userXattrStore, ok := base.AsUserXattrStore(dataStore)
	if !ok {
		t.Skip("Test requires Couchbase Bucket")
	}

	// Initial PUT
	resp := rt.SendAdminRequest("PUT", "/{{.keyspace}}/"+docKey, `{}`)
	RequireStatus(t, resp, http.StatusCreated)

	require.NoError(t, rt.WaitForPendingChanges())

	// Get current sync data
	var syncData db.SyncData
	_, err := dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData)
	assert.NoError(t, err)

	docRev, err := rt.GetSingleTestDatabaseCollection().GetRevisionCacheForTest().GetWithRev(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(docRev.Channels.ToArray()))
	assert.Equal(t, syncData.CurrentRev, docRev.RevID)

	// Write xattr to trigger import of user xattr
	_, err = userXattrStore.WriteUserXattr(docKey, xattrKey, channelName)
	assert.NoError(t, err)

	// Wait for import
	err = rt.WaitForCondition(func() bool {
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 1
	})
	assert.NoError(t, err)

	// Ensure import worked and sequence incremented but that sequence did not
	var syncData2 db.SyncData
	_, err = dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	docRev2, err := rt.GetSingleTestDatabaseCollection().GetRevisionCacheForTest().GetWithRev(base.TestCtx(t), docKey, syncData.CurrentRev, true, false)
	assert.NoError(t, err)
	assert.Equal(t, syncData2.CurrentRev, docRev2.RevID)

	assert.Equal(t, syncData.CurrentRev, syncData2.CurrentRev)
	assert.True(t, syncData2.Sequence > syncData.Sequence)
	assert.Equal(t, []string{channelName}, syncData2.Channels.KeySet())
	assert.Equal(t, syncData2.Channels.KeySet(), docRev2.Channels.ToArray())

	err = rt.GetSingleDataStore().Set(docKey, 0, nil, []byte(`{"update": "update"}`))
	assert.NoError(t, err)

	err = rt.WaitForCondition(func() bool {
		log.Printf("Import count is: %v", rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value())
		return rt.GetDatabase().DbStats.SharedBucketImport().ImportCount.Value() == 2
	})
	assert.NoError(t, err)

	var syncData3 db.SyncData
	_, err = dataStore.GetXattr(rt.Context(), docKey, base.SyncXattrName, &syncData2)
	assert.NoError(t, err)

	assert.NotEqual(t, syncData2.CurrentRev, syncData3.CurrentRev)
}

func TestGetUserCollectionAccess(t *testing.T) {
	numCollections := 2
	base.RequireNumTestDataStores(t, numCollections)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)

	ctx := base.TestCtx(t)
	testBucket := base.GetPersistentTestBucket(t)
	defer testBucket.Close(ctx)
	scopesConfig := GetCollectionsConfig(t, testBucket, 2)

	rtConfig := &RestTesterConfig{
		CustomTestBucket: testBucket,
		SyncFn:           `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);}`,
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	scope1Name := rt.GetDbCollections()[0].ScopeName
	collection1Name := rt.GetDbCollections()[0].Name
	collection2Name := rt.GetDbCollections()[1].Name
	scopesConfig[scope1Name].Collections[collection1Name] = &CollectionConfig{}

	collectionPayload := fmt.Sprintf(`,"%s": {
					"admin_channels":["foo", "bar1"]
				}`, collection2Name)

	// Create a user with collection metadata
	userRolePayload := `{
		%s
		"admin_channels":["foo", "bar"],
		"collection_access": {
			"%s": {
				"%s": {
					"admin_channels":["foo", "bar1"]
				}
				%s
			}
		}
	}`

	putResponse := rt.SendAdminRequest("PUT", "/db/_user/alice", fmt.Sprintf(userRolePayload, `"email":"alice@couchbase.com","password":"letmein",`, scope1Name, collection1Name, ""))
	RequireStatus(t, putResponse, 201)
	putResponse = rt.SendAdminRequest("PUT", "/db/_user/bob", fmt.Sprintf(userRolePayload, `"email":"bob@couchbase.com","password":"letmein",`, scope1Name, collection1Name, collectionPayload))
	RequireStatus(t, putResponse, 201)
	putResponse = rt.SendAdminRequest("PUT", "/db/_role/role1", fmt.Sprintf(userRolePayload, ``, scope1Name, collection1Name, collectionPayload))
	RequireStatus(t, putResponse, 201)

	getResponse := rt.SendAdminRequest("GET", "/db/_user/bob", "")
	RequireStatus(t, getResponse, 200)
	log.Printf("response:%s", getResponse.Body.Bytes())
	var responseConfig auth.PrincipalConfig
	err := json.Unmarshal(getResponse.Body.Bytes(), &responseConfig)
	require.NoError(t, err)
	collectionAccess, ok := responseConfig.CollectionAccess[scope1Name][collection1Name]
	require.True(t, ok)

	assert.Equal(t, channels.BaseSetOf(t, "foo", "bar1"), collectionAccess.ExplicitChannels_)
	assert.Equal(t, channels.BaseSetOf(t, "foo", "bar1", "!"), collectionAccess.Channels_)
	assert.Nil(t, collectionAccess.JWTChannels_)
	assert.Nil(t, collectionAccess.JWTLastUpdated)

	scopesConfig = GetCollectionsConfig(t, testBucket, 1)
	scopesConfigString, err := json.Marshal(scopesConfig)
	require.NoError(t, err)

	resp := rt.SendAdminRequest("PUT", "/db/_config", fmt.Sprintf(
		`{"bucket": "%s", "num_index_replicas": 0, "enable_shared_bucket_access": %t, "scopes":%s}`,
		testBucket.GetName(), base.TestUseXattrs(), string(scopesConfigString)))
	RequireStatus(t, resp, http.StatusCreated)

	//  Hide entries for collections that are no longer part of the database for GET /_user and /_role
	userResponse := rt.SendAdminRequest("GET", "/db/_user/bob", "")
	RequireStatus(t, userResponse, 200)
	assert.NotContains(t, userResponse.Body.Bytes(), collection2Name)

	userResponse = rt.SendAdminRequest("GET", "/db/_role/role1", "")
	RequireStatus(t, userResponse, 200)
	assert.NotContains(t, userResponse.Body.Bytes(), collection2Name)

	// Attempt to write collections that aren't defined for the database for PUT /_user and /_role
	putResponse = rt.SendAdminRequest("PUT", "/db/_user/alice2", fmt.Sprintf(userRolePayload, `"email":"alice@couchbase.com","password":"@232dfdg",`, scope1Name, collection1Name, `,"rgergeggrenhnnh": {
					"admin_channels":["foo", "bar1"]
				}`))
	RequireStatus(t, putResponse, 404)
	putResponse = rt.SendAdminRequest("PUT", "/db/_role/role1", fmt.Sprintf(userRolePayload, ``, scope1Name, collection1Name, collectionPayload))
	RequireStatus(t, putResponse, 404)
}

func TestPutUserCollectionAccess(t *testing.T) {
	base.RequireNumTestDataStores(t, 2)
	base.SetUpTestLogging(t, base.LevelDebug, base.KeyAll)
	testBucket := base.GetPersistentTestBucket(t)

	scopesConfig := GetCollectionsConfig(t, testBucket, 2)
	rtConfig := &RestTesterConfig{
		CustomTestBucket: testBucket,
		SyncFn:           `function(doc) {channel(doc.channel); access(doc.accessUser, doc.accessChannel);}`,
	}

	rt := NewRestTesterMultipleCollections(t, rtConfig, 2)
	defer rt.Close()

	scopeName := rt.GetDbCollections()[0].ScopeName
	collection1Name := rt.GetDbCollections()[0].Name
	collection2Name := rt.GetDbCollections()[1].Name
	scopesConfig[scopeName].Collections[collection1Name] = &CollectionConfig{}

	collectionPayload := fmt.Sprintf(`,"%s": {
					"admin_channels":["a"]
				}`, collection2Name)
	userPayload := `{
		%s
		"admin_channels":["foo", "bar"],
		"collection_access": {
			"%s": {
				"%s": {
					"admin_channels":[%s]
				}
				%s
			}
		}
	}`

	putResponse := rt.SendAdminRequest("PUT", "/db/_user/bob", fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`,
		scopeName, collection1Name, `"foo"`, collectionPayload))
	RequireStatus(t, putResponse, 201)

	// Upsert one collection and preserve existing
	putResponse = rt.SendAdminRequest("PUT", "/db/_user/bob", fmt.Sprintf(userPayload, `"email":"bob@couchbase.com",`,
		scopeName, collection1Name, `"foo", "bar"`, ""))
	RequireStatus(t, putResponse, 200)
	getResponse := rt.SendAdminRequest("GET", "/db/_user/bob", "")
	RequireStatus(t, getResponse, 200)
	assert.Contains(t, getResponse.ResponseRecorder.Body.String(), collection2Name)

	// Delete collection admin channels
	putResponse = rt.SendAdminRequest("PUT", "/db/_user/bob", fmt.Sprintf(userPayload, `"email":"bob@couchbase.com",`,
		scopeName, collection1Name, ``, ""))
	RequireStatus(t, putResponse, 200)

	getResponse = rt.SendAdminRequest("GET", "/db/_user/bob", "")
	RequireStatus(t, getResponse, 200)
	assert.Contains(t, getResponse.ResponseRecorder.Body.String(), `"all_channels":["!"]`)

	dbConfig := rt.NewDbConfig()
	dbConfig.Scopes = GetCollectionsConfig(rt.TB, rt.TestBucket, 1)
	resp := rt.ReplaceDbConfig(rt.GetDatabase().Name, dbConfig)
	RequireStatus(t, resp, http.StatusCreated)

	//  Hide entries for collections that are no longer part of the database for GET /_user and /_role
	userResponse := rt.SendAdminRequest("GET", "/db/_user/bob", "")
	RequireStatus(t, userResponse, http.StatusOK)
	assert.NotContains(t, userResponse.ResponseRecorder.Body.String(), collection2Name)

	// Attempt to write read-only properties for PUT /_user and /_role
	readOnlyProperties := []string{"all_channels", "jwt_channels", "jwt_last_updated"}
	for _, property := range readOnlyProperties {
		rdOnlyValue := `["ABC"]`
		if property == "jwt_last_updated" {
			rdOnlyValue = "22:55:44"
		}
		readOnlyCollectionPayload := fmt.Sprintf(`,"%s": {
					"%s":%s
				}`, collection2Name, property, rdOnlyValue)
		putResponse = rt.SendAdminRequest("PUT", "/db/_user/bob2", fmt.Sprintf(userPayload, `"email":"bob@couchbase.com","password":"letmein",`, scopeName, collection2Name, `["ABC"]`, readOnlyCollectionPayload))
		RequireStatus(t, putResponse, 400)
		putResponse = rt.SendAdminRequest("PUT", "/db/_role/role12", fmt.Sprintf(userPayload, ``, scopeName, collection2Name, `["ABC"]`, readOnlyCollectionPayload))
		RequireStatus(t, putResponse, 400)
	}
}

func TestUnauthorizedAccessForDB(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{
		PersistentConfig: true,
	})
	defer rt.Close()

	dbConfig := rt.NewDbConfig()

	dbName := "realdb"
	rt.CreateDatabase("realdb", dbConfig)

	response := rt.SendRequest(http.MethodGet, "/"+dbName+"/", "")
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	response = rt.SendRequest(http.MethodGet, "/notadb/", "")
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrLoginRequired.Message)

	// create sessions before users
	const alice = "alice"
	const bob = "bob"
	response = rt.SendAdminRequest(http.MethodPut,
		"/"+dbName+"/_user/"+alice,
		`{"name": "`+alice+`", "password": "`+RestTesterDefaultUserPassword+`"}`)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendRequest(http.MethodPost,
		"/"+dbName+"/_session",
		`{"name": "`+alice+`", "password": "`+RestTesterDefaultUserPassword+`"}`)
	RequireStatus(t, response, http.StatusOK)

	cookie := response.Header().Get("Set-Cookie")
	aliceSessionHeaders := map[string]string{
		"Cookie": cookie,
	}

	// alice user can see realdb
	response = rt.SendUserRequest(http.MethodGet, "/"+dbName+"/", "", alice)
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/"+dbName+"/", "", aliceSessionHeaders)
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendUserRequest(http.MethodGet, "/notadb/", "", alice)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/notadb/", "", aliceSessionHeaders)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)

	response = rt.SendUserRequest(http.MethodGet, "/"+dbName+"/", "", bob)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), ErrInvalidLogin.Message)

}
