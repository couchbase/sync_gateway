// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.
package rest

import (
	"encoding/base64"
	"fmt"
	"log"
	"net/http"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
	"github.com/couchbase/sync_gateway/db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCORSLoginOriginOnSessionPost(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.SendRequestWithHeaders("POST", "/db/_session", "{\"name\":\"jchris\",\"password\":\"secret\"}", reqHeaders)
	RequireStatus(t, response, 401)

	response = rt.SendRequestWithHeaders("POST", "/db/_facebook", `{"access_token":"true"}`, reqHeaders)
	assertGatewayStatus(t, response, 401)
}

// #issue 991
func TestCORSLoginOriginOnSessionPostNoCORSConfig(t *testing.T) {
	rt := NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()
	// Set CORS to nil, RestTester initializes CORS by default and it is inherited when creating a DatabaseContext
	rt.ServerContext().Config.API.CORS = nil

	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)
	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.SendRequestWithHeaders("POST", "/db/_session", `{"name":"jchris","password":"secret"}`, reqHeaders)
	RequireStatus(t, response, 400)
}

func TestNoCORSOriginOnSessionPost(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://staging.example.com",
	}

	response := rt.SendRequestWithHeaders("POST", "/db/_session", "{\"name\":\"jchris\",\"password\":\"secret\"}", reqHeaders)
	RequireStatus(t, response, 400)

	response = rt.SendRequestWithHeaders("POST", "/db/_facebook", `{"access_token":"true"}`, reqHeaders)
	assertGatewayStatus(t, response, 400)
}

func TestCORSLogoutOriginOnSessionDelete(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://example.com",
	}

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	RequireStatus(t, response, 404)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "no session", body["reason"])
}

func TestCORSLogoutOriginOnSessionDeleteNoCORSConfig(t *testing.T) {
	rt := NewRestTesterPersistentConfigNoDB(t)
	defer rt.Close()

	// Set CORS to nil, RestTester initializes CORS by default and it is inherited when creating a DatabaseContext
	rt.ServerContext().Config.API.CORS = nil

	const username = "alice"
	RequireStatus(t, rt.CreateDatabase("db", rt.NewDbConfig()), http.StatusCreated)
	rt.CreateUser(username, nil)

	reqHeaders := map[string]string{
		"Origin":        "http://example.com",
		"Authorization": GetBasicAuthHeader(t, username, RestTesterDefaultUserPassword),
	}

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	RequireStatus(t, response, 400)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "No CORS", body["reason"])
}

func TestNoCORSOriginOnSessionDelete(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	reqHeaders := map[string]string{
		"Origin": "http://staging.example.com",
	}

	response := rt.SendRequestWithHeaders("DELETE", "/db/_session", "", reqHeaders)
	RequireStatus(t, response, 400)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	assert.Equal(t, "No CORS", body["reason"])
}

func TestInvalidSession(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/{{.keyspace}}/testdoc", `{"hi": "there"}`)
	RequireStatus(t, response, 201)

	headers := map[string]string{}
	headers["Cookie"] = fmt.Sprintf("%s=%s", auth.DefaultCookieName, "FakeSession")
	response = rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/testdoc", "", headers)
	RequireStatus(t, response, 401)

	var body db.Body
	err := base.JSONUnmarshal(response.Body.Bytes(), &body)

	assert.NoError(t, err)
	assert.Equal(t, "Session Invalid", body["reason"])
}

// Test for issue 758 - basic auth with stale session cookie
func TestBasicAuthWithSessionCookie(t *testing.T) {

	rt := NewRestTester(t, &RestTesterConfig{SyncFn: channels.DocChannelsSyncFunction})
	defer rt.Close()

	// Create two users
	rt.CreateUser("bernard", []string{"bernard"})
	rt.CreateUser("manny", []string{"manny"})

	// Create a session for the first user
	response := rt.Send(RequestByUser("POST", "/db/_session", `{"name":"bernard", "password":"letmein"}`, "bernard"))
	log.Println("response.Header()", response.Header())
	assert.True(t, response.Header().Get("Set-Cookie") != "")

	cookie := response.Header().Get("Set-Cookie")

	// Create a doc as the first user, with session auth, channel-restricted to first user
	reqHeaders := map[string]string{
		"Cookie": cookie,
	}
	response = rt.SendRequestWithHeaders("PUT", "/{{.keyspace}}/bernardDoc", `{"hi": "there", "channels":["bernard"]}`, reqHeaders)
	RequireStatus(t, response, 201)
	response = rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/bernardDoc", "", reqHeaders)
	RequireStatus(t, response, 200)

	// Create a doc as the second user, with basic auth, channel-restricted to the second user
	response = rt.SendUserRequest("PUT", "/{{.keyspace}}/mannyDoc", `{"hi": "there", "channels":["manny"]}`, "manny")
	RequireStatus(t, response, 201)
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/mannyDoc", "", "manny")
	RequireStatus(t, response, 200)
	response = rt.SendUserRequest("GET", "/{{.keyspace}}/bernardDoc", "", "manny")
	RequireStatus(t, response, 403)

	// Attempt to retrieve the docs with the first user's cookie, second user's basic auth credentials.  Basic Auth should take precedence
	response = rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/bernardDoc", "", reqHeaders, "manny", "letmein")
	RequireStatus(t, response, 403)
	response = rt.SendUserRequestWithHeaders("GET", "/{{.keyspace}}/mannyDoc", "", reqHeaders, "manny", "letmein")
	RequireStatus(t, response, 200)
}

// Try to create session with invalid cert but valid credentials
func TestSessionFail(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	rt.CreateUser("user1", []string{"user1"})

	id, err := base.GenerateRandomSecret()
	require.NoError(t, err)

	// Create fake, invalid session
	fakeSession := auth.LoginSession{
		ID:         id,
		Username:   "user1",
		Expiration: time.Now().Add(4 * time.Hour),
		Ttl:        24 * time.Hour,
	}

	reqHeaders := map[string]string{
		"Cookie": auth.DefaultCookieName + "=" + fakeSession.ID,
	}

	// Attempt to create session with invalid cert but valid login
	response := rt.SendRequestWithHeaders("POST", "/db/_session", `{"name":"user1", "password":"letmein"}`, reqHeaders)
	RequireStatus(t, response, http.StatusOK)
}

func TestSessionLogin(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: false})
	defer rt.Close()

	// ensure guest is actually disabled
	resp := rt.SendRequest(http.MethodPut, "/db/doc", `{"hi": "there"}`)
	RequireStatus(t, resp, http.StatusUnauthorized)

	rt.CreateUser("pupshaw", []string{"*"})

	resp = rt.SendRequest(http.MethodGet, "/db/_session", "")
	RequireStatus(t, resp, http.StatusOK)

	resp = rt.SendRequest(http.MethodPost, "/db/_session", `{"name":"pupshaw", "password":"letmein"}`)
	RequireStatus(t, resp, http.StatusOK)
	cookie := resp.Header().Get("Set-Cookie")
	require.NotEmptyf(t, cookie, "Set-Cookie header not found in response: %v", resp)
	t.Logf("Set-Cookie: %s", cookie)

	headers := map[string]string{
		"Cookie": cookie,
	}
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/", "", headers, "", "")
	RequireStatus(t, resp, http.StatusOK)

	// invalid password
	resp = rt.SendRequest(http.MethodPost, "/db/_session", `{"name":"pupshaw", "password":"incorrectpassword"}`)
	RequireStatus(t, resp, http.StatusUnauthorized)
	t.Logf("Set-Cookie: %s", resp.Header().Get("Set-Cookie"))
	require.Emptyf(t, resp.Header().Get("Set-Cookie"), "Set-Cookie header should not be set in response: %v", resp)

	// invalid cookie (no username available)
	headers = map[string]string{
		"Cookie": "SyncGatewaySession=123456abcdef; Path=/db; Expires=Sat, 17 Aug 2024 15:20:52 GMT",
	}
	resp = rt.SendUserRequestWithHeaders(http.MethodGet, "/db/", "", headers, "", "")
	RequireStatus(t, resp, http.StatusUnauthorized)
}

func TestCustomCookieName(t *testing.T) {

	customCookieName := "TestCustomCookieName"

	rt := NewRestTester(t,
		&RestTesterConfig{
			DatabaseConfig: &DatabaseConfig{
				DbConfig: DbConfig{
					Name:              "db",
					SessionCookieName: customCookieName,
				},
			},
		},
	)

	defer rt.Close()

	// Disable guest user
	a := auth.NewAuthenticator(rt.MetadataStore(), nil, rt.GetDatabase().AuthenticatorOptions(rt.Context()))
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	// Create a user
	response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"1234"}`)
	RequireStatus(t, response, 201)

	// Create a session
	resp := rt.SendRequest("POST", "/db/_session", `{"name":"user1", "password":"1234"}`)
	assert.Equal(t, 200, resp.Code)

	// Extract the cookie from the create session response to verify the "Set-Cookie" value returned by Sync Gateway
	result := resp.Result()
	defer func() { assert.NoError(t, result.Body.Close()) }()
	cookies := result.Cookies()
	assert.True(t, len(cookies) == 1)
	cookie := cookies[0]
	assert.Equal(t, customCookieName, cookie.Name)
	assert.Equal(t, "/db", cookie.Path)

	// Attempt to use default cookie name to authenticate -- expect a 401 error
	headers := map[string]string{}
	headers["Cookie"] = fmt.Sprintf("%s=%s", auth.DefaultCookieName, cookie.Value)
	resp = rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/foo", `{}`, headers)
	RequireStatus(t, resp, http.StatusUnauthorized)

	// Attempt to use custom cookie name to authenticate
	headers["Cookie"] = fmt.Sprintf("%s=%s", customCookieName, cookie.Value)
	resp = rt.SendRequestWithHeaders("POST", "/{{.keyspace}}/", `{"_id": "foo", "key": "val"}`, headers)
	RequireStatus(t, resp, http.StatusOK)
}

// Test that TTL values greater than the default max offset TTL 2592000 seconds are processed correctly
// fixes #974
func TestSessionTtlGreaterThan30Days(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	a := auth.NewAuthenticator(rt.MetadataStore(), nil, rt.GetDatabase().AuthenticatorOptions(rt.Context()))
	user, err := a.GetUser("")
	assert.NoError(t, err)
	user.SetDisabled(true)
	err = a.Save(user)
	assert.NoError(t, err)

	user, err = a.GetUser("")
	assert.NoError(t, err)
	assert.True(t, user.Disabled())

	response := rt.SendRequest("PUT", "/db/doc", `{"hi": "there"}`)
	RequireStatus(t, response, 401)

	user, err = a.NewUser("pupshaw", "letmein", channels.BaseSetOf(t, "*"))
	require.NoError(t, err)
	assert.NoError(t, a.Save(user))

	// create a session with the maximum offset ttl value (30days) 2592000 seconds
	response = rt.SendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592000}`)
	RequireStatus(t, response, 200)

	layout := "2006-01-02T15:04:05"

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))

	log.Printf("expires %s", body["expires"].(string))
	expires, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.NoError(t, err)

	// create a session with a ttl value one second greater thatn the max offset ttl 2592001 seconds
	response = rt.SendAdminRequest("POST", "/db/_session", `{"name":"pupshaw", "ttl":2592001}`)
	RequireStatus(t, response, 200)

	body = nil
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	log.Printf("expires2 %s", body["expires"].(string))
	expires2, err := time.Parse(layout, body["expires"].(string)[:19])
	assert.NoError(t, err)

	// Allow a ten second drift between the expires dates, to pass test on slow servers
	acceptableTimeDelta := time.Duration(10) * time.Second

	// The difference between the two expires dates should be less than the acceptable time delta
	assert.True(t, expires2.Sub(expires) < acceptableTimeDelta)
}

// Check whether the session is getting extended or refreshed if 10% or more of the current
// expiration time has elapsed.
func TestSessionExtension(t *testing.T) {
	rt := NewRestTester(t, &RestTesterConfig{GuestEnabled: true})
	defer rt.Close()

	id, err := base.GenerateRandomSecret()
	require.NoError(t, err)

	const username = "Alice"

	authenticator := rt.GetDatabase().Authenticator(base.TestCtx(t))
	user, err := authenticator.NewUser(username, "Password", channels.BaseSetOf(t, "*"))
	require.NoError(t, err)
	require.NoError(t, authenticator.Save(user))

	// Fake session with more than 10% of the 24 hours TTL has elapsed. It should cause a new
	// cookie to be sent by the server with the same session ID and an extended expiration date.
	fakeSession := auth.LoginSession{
		ID:          id,
		Username:    username,
		Expiration:  time.Now().Add(4 * time.Hour),
		Ttl:         24 * time.Hour,
		SessionUUID: user.GetSessionUUID(),
	}

	assert.NoError(t, rt.MetadataStore().Set(authenticator.DocIDForSession(fakeSession.ID), 0, nil, fakeSession))
	reqHeaders := map[string]string{
		"Cookie": auth.DefaultCookieName + "=" + fakeSession.ID,
	}

	response := rt.SendRequestWithHeaders("PUT", "/{{.keyspace}}/doc1", `{"hi": "there"}`, reqHeaders)
	log.Printf("PUT Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	RequireStatus(t, response, http.StatusCreated)
	assert.Contains(t, response.Header().Get("Set-Cookie"), auth.DefaultCookieName+"="+fakeSession.ID)

	response = rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/doc1", "", reqHeaders)
	log.Printf("GET Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, "", response.Header().Get("Set-Cookie"))

	// Explicitly delete the fake session doc from the bucket to simulate the test
	// scenario for expired session. In reality, Sync Gateway rely on Couchbase
	// Server to nuke the expired document based on TTL. Couchbase Server periodically
	// removes all items with expiration times that have passed.
	assert.NoError(t, rt.MetadataStore().Delete(authenticator.DocIDForSession(fakeSession.ID)))

	response = rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/doc1", "", reqHeaders)
	log.Printf("GET Request: Set-Cookie: %v", response.Header().Get("Set-Cookie"))
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), "Session Invalid")
}

func TestSessionAPI(t *testing.T) {

	rt := NewRestTester(t, nil)
	defer rt.Close()

	// create session test users
	response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user1", "password":"1234"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user2", "password":"1234"}`)
	RequireStatus(t, response, 201)
	response = rt.SendAdminRequest("POST", "/db/_user/", `{"name":"user3", "password":"1234"}`)
	RequireStatus(t, response, 201)

	// create multiple sessions for the users
	user1sessions := make([]string, 5)
	user2sessions := make([]string, 5)
	user3sessions := make([]string, 5)

	for i := range 5 {
		user1sessions[i] = createSession(t, rt, "user1")
		user2sessions[i] = createSession(t, rt, "user2")
		user3sessions[i] = createSession(t, rt, "user3")
	}

	// GET Tests
	// 1. GET a session and make sure the result is OK
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	RequireStatus(t, response, 200)

	// DELETE tests
	// 1. DELETE a session by session id
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	RequireStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[0]), "")
	RequireNotFoundError(t, response)

	// 2. DELETE a session with user validation
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user1sessions[1]), "")
	RequireStatus(t, response, 200)

	// Attempt to GET the deleted session and make sure it's not found
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user1sessions[1]), "")
	RequireNotFoundError(t, response)

	// 3. DELETE a session not belonging to the user (should fail)
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_user/%s/_session/%s", "user1", user2sessions[0]), "")
	RequireNotFoundError(t, response)

	// GET the session and make sure it still exists
	response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[0]), "")
	RequireStatus(t, response, 200)

	// 4. DELETE all sessions for a user
	response = rt.SendAdminRequest("DELETE", "/db/_user/user2/_session", "")
	RequireStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := range 5 {
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user2sessions[i]), "")
		RequireNotFoundError(t, response)
	}

	// 5. DELETE sessions when password is changed
	// Change password for user3
	response = rt.SendAdminRequest("PUT", "/db/_user/user3", `{"password":"5678"}`)
	RequireStatus(t, response, 200)

	// Validate that all sessions were deleted
	for i := range 5 {
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_session/%s", user3sessions[i]), "")
		RequireNotFoundError(t, response)
	}

	// DELETE the users
	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user1", ""), 200)
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/user1", ""), 404)

	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user2", ""), 200)
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/user2", ""), 404)

	RequireStatus(t, rt.SendAdminRequest("DELETE", "/db/_user/user3", ""), 200)
	RequireStatus(t, rt.SendAdminRequest("GET", "/db/_user/user3", ""), 404)

}

func TestSessionPasswordInvalidation(t *testing.T) {
	testCases := []struct {
		name     string
		password string
	}{
		{
			name:     "emptypassword",
			password: "",
		},
		{
			name:     "realpassword",
			password: "password",
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			rtConfig := &RestTesterConfig{}
			if test.password == "" {
				rtConfig.DatabaseConfig = &DatabaseConfig{
					DbConfig: DbConfig{
						AllowEmptyPassword: base.Ptr(true),
					},
				}

			}
			rt := NewRestTester(t, rtConfig)
			defer rt.Close()

			const username = "user1"
			originalPassword := test.password

			// create session test users
			response := rt.SendAdminRequest(http.MethodPost, "/{{.db}}/_user/", GetUserPayload(t, username, originalPassword, "", rt.GetSingleDataStore(), []string{"*"}, nil))
			RequireStatus(t, response, http.StatusCreated)

			originalPasswordHeaders := map[string]string{
				"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+originalPassword)),
			}
			response = rt.SendRequestWithHeaders(http.MethodPost, "/db/_session", "", originalPasswordHeaders)
			RequireStatus(t, response, http.StatusOK)
			cookie := response.Header().Get("Set-Cookie")
			require.NotEqual(t, "", cookie)

			// Create a doc as the first user, with session auth, channel-restricted to first user
			cookieHeaders := map[string]string{
				"Cookie": cookie,
			}
			response = rt.SendRequestWithHeaders(http.MethodPut, "/{{.keyspace}}/doc1", `{"hi": "there"}`, cookieHeaders)
			RequireStatus(t, response, http.StatusCreated)
			response = rt.SendRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/doc1", "", cookieHeaders)
			RequireStatus(t, response, http.StatusOK)

			response = rt.SendRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/doc1", "", originalPasswordHeaders)
			RequireStatus(t, response, http.StatusOK)

			altPassword := "someotherpassword"
			// TODO CBG-3790: Add POST (upsert) support on User API, and specify only password here.
			// This test was relying on a bug (CBG-3610) which allowed only the password to be specified without wiping channels.
			response = rt.SendAdminRequest(http.MethodPut, "/{{.db}}/_user/"+username, GetUserPayload(t, username, altPassword, "", rt.GetSingleDataStore(), []string{"*"}, nil))
			RequireStatus(t, response, http.StatusOK)

			// make sure session is invalid
			response = rt.SendRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/doc1", "", cookieHeaders)
			RequireStatus(t, response, http.StatusUnauthorized)
			require.Contains(t, response.Body.String(), "Session no longer valid")

			// make sure doc is valid for password
			altPasswordHeaders := map[string]string{
				"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte(username+":"+altPassword)),
			}

			response = rt.SendRequestWithHeaders("GET", "/{{.keyspace}}/doc1", "", altPasswordHeaders)
			RequireStatus(t, response, http.StatusOK)
		},
		)
	}
}

func TestAllSessionDeleteInvalidation(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	const username = "user1"

	rt.CreateUser(username, []string{"*"})

	const numSessions = 3
	cookies := make([]string, numSessions)
	for i := range 3 {
		response := rt.SendUserRequest(http.MethodPost, "/{{.db}}/_session", "", username)
		RequireStatus(t, response, http.StatusOK)
		cookie := response.Header().Get("Set-Cookie")
		require.NotEqual(t, "", cookie)
		cookies[i] = cookie
	}
	// Create a doc as the first user, with session auth, channel-restricted to first user
	firstCookieHeaders := map[string]string{
		"Cookie": cookies[0],
	}
	response := rt.SendRequestWithHeaders(http.MethodPut, "/{{.keyspace}}/doc1", `{"hi": "there"}`, firstCookieHeaders)
	RequireStatus(t, response, http.StatusCreated)

	// make sure all sessions work to GET doc
	for _, cookie := range cookies {
		cookieHeaders := map[string]string{
			"Cookie": cookie,
		}

		response = rt.SendRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/doc1", "", cookieHeaders)
		RequireStatus(t, response, http.StatusOK)
	}

	// make sure password works to GET doc
	response = rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/doc1", "", username)
	RequireStatus(t, response, http.StatusOK)

	// DELETE all sessions for a user
	response = rt.SendAdminRequest("DELETE", fmt.Sprintf("/{{.db}}/_user/%s/_session", username), "")
	RequireStatus(t, response, http.StatusOK)

	// make sure all sessions are invalid
	for _, cookie := range cookies {
		cookieHeaders := map[string]string{
			"Cookie": cookie,
		}

		response = rt.SendRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/doc1", "", cookieHeaders)
		RequireStatus(t, response, http.StatusUnauthorized)
		require.Contains(t, response.Body.String(), "Session no longer valid")
	}

	// make sure password still works
	response = rt.SendUserRequest(http.MethodGet, "/{{.keyspace}}/doc1", "", username)
	RequireStatus(t, response, http.StatusOK)

}

// TestUserWithoutSessionUUID tests users that existed before we stamped SessionUUID into user docs
func TestUserWithoutSessionUUID(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	const username = "user1"

	authenticator := rt.GetDatabase().Authenticator(base.TestCtx(t))
	user, err := authenticator.NewUser(username, RestTesterDefaultUserPassword, base.SetOf("*"))
	require.NoError(t, err)
	require.NotNil(t, user)
	require.NoError(t, authenticator.Save(user))

	var rawUser map[string]any
	_, err = rt.MetadataStore().Get(authenticator.DocIDForUser(user.Name()), &rawUser)
	require.NoError(t, err)

	sessionUUIDKey := "session_uuid"
	_, exists := rawUser[sessionUUIDKey]
	require.True(t, exists)
	delete(rawUser, sessionUUIDKey)

	err = rt.MetadataStore().Set(authenticator.DocIDForUser(user.Name()), 0, nil, rawUser)
	require.NoError(t, err)

	response := rt.SendUserRequest(http.MethodPost, "/{{.db}}/_session", "", username)
	RequireStatus(t, response, http.StatusOK)

	RequireStatus(t, response, http.StatusOK)
	cookie := response.Header().Get("Set-Cookie")
	require.NotEqual(t, "", cookie)

	// Create a doc as the first user, with session auth, channel-restricted to first user
	cookieHeaders := map[string]string{
		"Cookie": cookie,
	}
	response = rt.SendRequestWithHeaders(http.MethodPut, "/{{.keyspace}}/doc1", `{"hi": "there"}`, cookieHeaders)
	RequireStatus(t, response, http.StatusCreated)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/doc1", "", cookieHeaders)
	RequireStatus(t, response, http.StatusOK)

	// delete all user sesssions
	response = rt.SendAdminRequest("DELETE", "/db/_user/user1/_session", "")
	RequireStatus(t, response, http.StatusOK)

	response = rt.SendRequestWithHeaders(http.MethodGet, "/{{.keyspace}}/doc1", "", cookieHeaders)
	RequireStatus(t, response, http.StatusUnauthorized)
	require.Contains(t, response.Body.String(), "Session no longer valid")
}

func createSession(t *testing.T, rt *RestTester, username string) string {

	response := rt.SendAdminRequest("POST", "/db/_session", fmt.Sprintf(`{"name":%q}`, username))
	RequireStatus(t, response, 200)

	var body db.Body
	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	sessionId := body["session_id"].(string)

	return sessionId
}

func TestSessionExpirationDateTimeFormat(t *testing.T) {
	rt := NewRestTester(t, nil)
	defer rt.Close()

	authenticator := auth.NewAuthenticator(rt.MetadataStore(), nil, rt.GetDatabase().AuthenticatorOptions(rt.Context()))
	user, err := authenticator.NewUser("alice", "letMe!n", channels.BaseSetOf(t, "*"))
	assert.NoError(t, err, "Couldn't create new user")
	assert.NoError(t, authenticator.Save(user), "Couldn't save new user")

	var body db.Body
	response := rt.SendAdminRequest(http.MethodPost, "/db/_session", `{"name":"alice"}`)
	RequireStatus(t, response, http.StatusOK)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	expires, err := time.Parse(time.RFC3339, body["expires"].(string))
	assert.NoError(t, err, "Couldn't parse session expiration datetime")
	assert.True(t, expires.Sub(time.Now()).Hours() <= 24, "Couldn't validate session expiration")

	sessionId := body["session_id"].(string)
	require.NotEmpty(t, sessionId, "Couldn't parse sessionID from response body")
	response = rt.SendAdminRequest(http.MethodGet, fmt.Sprintf("/db/_session/%s", sessionId), "")
	RequireStatus(t, response, http.StatusOK)

	require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
	expires, err = time.Parse(time.RFC3339, body["expires"].(string))
	assert.NoError(t, err, "Couldn't parse session expiration datetime")
	assert.True(t, expires.Sub(time.Now()).Hours() <= 24, "Couldn't validate session expiration")
}
