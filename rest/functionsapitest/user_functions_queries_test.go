//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

//go:build cb_sg_v8
// +build cb_sg_v8

package functionsapitest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/couchbase/sync_gateway/db"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

//////// FUNCTIONS EXECUTION API TESTS

/// AUTH TESTS

func createUser(t *testing.T, rt *rest.RestTester) (string, string) {
	username := "alice"
	password := "pass"

	userDetails := fmt.Sprintf(`{"name":"%s", "password":"%s", "admin_channels":["wonderland"]}`, username, password)
	resp := rt.SendAdminRequest("POST", "/db/_user/", userDetails)
	rest.RequireStatus(t, resp, http.StatusCreated)

	return username, password
}

var kUserFunctionAuthTestConfig = functions.FunctionsConfig{
	Definitions: functions.FunctionsDefs{
		"square": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  "function(context, args) {return args.numero * args.numero;}",
			Args:  []string{"numero"},
			Allow: &functions.Allow{Channels: []string{"wonderland"}},
		},
		"admin_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return "OK";}`,
			Allow: nil, // no 'allow' property means admin-only
		},
		"allow_all": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return "OK";}`,
			Allow: allowAll,
		},
	},
}

func TestUserFunctions(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: &kUserFunctionAuthTestConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsAdmin", func(t *testing.T) { testUserFunctionsAsAdmin(t, rt) })
	t.Run("AsUser", func(t *testing.T) { testUserFunctionsAsUser(t, rt) })
}

func TestJSFunctionAsGuest(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{GuestEnabled: true, EnableUserQueries: true})
	if rt == nil {
		return
	}
	defer rt.Close()

	rt.DatabaseConfig = &rest.DatabaseConfig{
		DbConfig: rest.DbConfig{
			UserFunctions: &kUserFunctionAuthTestConfig,
		},
	}

	sendReqFn := rt.SendRequest

	t.Run("function not configured", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 401, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "login required")
	})

	t.Run("allow all", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/allow_all", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, `"OK"`, string(response.BodyBytes()))
	})

	t.Run("user required", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/square", `{"numero": 42}`)
		assert.Equal(t, 401, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "Login required")
	})

	t.Run("admin-only", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 401, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "Login required")
	})
}

func testUserFunctionsCommon(t *testing.T, rt *rest.RestTester, sendReqFn func(string, string, string) *rest.TestResponse) {
	t.Run("commons/passing a param", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/square", `{"numero": 42}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "1764", string(response.BodyBytes()))
	})

	t.Run("commons/passing a param through query params", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/square?numero=42", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "1764", string(response.BodyBytes()))
	})

	t.Run("commons/allow all", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/allow_all", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, `"OK"`, string(response.BodyBytes()))
	})
}

func testUserFunctionsAsAdmin(t *testing.T, rt *rest.RestTester) {
	testUserFunctionsCommon(t, rt, rt.SendAdminRequest)

	t.Run("Admin-only", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
	})

	// negative cases:
	t.Run("function not configured", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func testUserFunctionsAsUser(t *testing.T, rt *rest.RestTester) {
	username, password := createUser(t, rt)
	sendReqFn := func(method, resource, body string) *rest.TestResponse {
		return rt.SendUserRequestWithHeaders(method, resource, body, nil, username, password)
	}
	testUserFunctionsCommon(t, rt, sendReqFn)

	// negative cases
	t.Run("function not configured", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("admin-only", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})
}

var kUserN1QLFunctionsAuthTestConfig = functions.FunctionsConfig{
	Definitions: functions.FunctionsDefs{
		"square": &functions.FunctionConfig{
			Type:  "query",
			Code:  "SELECT $args.numero * $args.numero AS square",
			Args:  []string{"numero"},
			Allow: &functions.Allow{Channels: []string{"wonderland"}},
		},
		"user": &functions.FunctionConfig{
			Type:  "query",
			Code:  "SELECT $user AS `user`", // use backticks for n1ql reserved keywords
			Allow: allowAll,
		},
		"admin_only": &functions.FunctionConfig{
			Type:  "query",
			Code:  `SELECT "ok" AS status`,
			Allow: nil, // no 'allow' property means admin-only
		},
	},
}

func TestUserN1QLQueries(t *testing.T) {

	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	// Configure User Queries
	request, err := json.Marshal(kUserN1QLFunctionsAuthTestConfig)
	assert.NoError(t, err)
	response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
	assert.Equal(t, 200, response.Result().StatusCode)

	t.Run("AsAdmin", func(t *testing.T) { testUserQueriesAsAdmin(t, rt) })
	t.Run("AsUser", func(t *testing.T) { testUserQueriesAsUser(t, rt) })
}

func TestN1QLFunctionAsGuest(t *testing.T) {
	if base.UnitTestUrlIsWalrus() {
		t.Skip("This test requires persistent configs")
	}

	rt := rest.NewRestTester(t, &rest.RestTesterConfig{GuestEnabled: true, EnableUserQueries: true})
	if rt == nil {
		return
	}
	defer rt.Close()

	rt.DatabaseConfig = &rest.DatabaseConfig{
		DbConfig: rest.DbConfig{
			UserFunctions: &kUserN1QLFunctionsAuthTestConfig,
		},
	}

	sendReqFn := rt.SendRequest

	t.Run("select user", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/user", "")
		if !assert.Equal(t, 200, response.Result().StatusCode) {
			return
		}
		var body []map[string]any
		if !assert.NoError(t, json.Unmarshal(response.BodyBytes(), &body)) {
			return
		}
		user, ok := body[0]["user"].(map[string]any)
		assert.True(t, ok, "Result 'user' property is missing or not an object")
		assert.Equal(t, "", user["name"])
	})

	t.Run("user required", func(t *testing.T) {
		t.Skip("Does not work with SG_TEST_USE_DEFAULT_COLLECTION=true CBG-2702")
		response := sendReqFn("POST", "/db/_function/square", `{"numero": 16}`)
		assert.Equal(t, 401, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "Login required")
	})

	t.Run("admin only", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 401, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "Login required")
	})

	t.Run("unconfigured query", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 401, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "login required")
	})
}

func testUserQueriesCommon(t *testing.T, rt *rest.RestTester, sendReqFn func(string, string, string) *rest.TestResponse) {
	// positive cases:
	t.Run("commons/passing a param", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/square", `{"numero": 16}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"square\":256}]", string(response.BodyBytes()))
	})
}

func testUserQueriesAsAdmin(t *testing.T, rt *rest.RestTester) {
	testUserQueriesCommon(t, rt, rt.SendAdminRequest)

	// positive cases:
	t.Run("select user", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/user", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"user\":{}}]", string(response.BodyBytes()))
	})

	t.Run("admin only", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"status\":\"ok\"}]", string(response.BodyBytes()))
	})

	//negative cases:
	t.Run("unconfigured query", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func testUserQueriesAsUser(t *testing.T, rt *rest.RestTester) {
	username, password := createUser(t, rt)
	sendReqFn := func(method, resource, body string) *rest.TestResponse {
		return rt.SendUserRequestWithHeaders(method, resource, body, nil, username, password)
	}
	testUserQueriesCommon(t, rt, sendReqFn)

	// positive cases:
	t.Run("select user", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/user", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.True(t, strings.HasPrefix(string(response.BodyBytes()), `[{"user":{"channels":["`) && strings.HasSuffix(string(response.BodyBytes()), "\"],\"name\":\"alice\"}}]"), "Unexpected response: %s", response.BodyBytes())
	})

	//negative cases:
	t.Run("admin only", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("unconfigured query", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})
}

func TestFunctionMutability(t *testing.T) {
	kUserMutabilityFunctionsTestConfig := &functions.FunctionsConfig{
		Definitions: functions.FunctionsDefs{
			"putDocMutabilityFalse": {
				Type:  "javascript",
				Code:  "function(context,args) { return context.user.defaultCollection.save(args.doc, args.docID); }",
				Args:  []string{"doc", "docID"},
				Allow: &functions.Allow{Channels: []string{"*"}},
			},
			"putDocMutabilityTrue": {
				Type:     "javascript",
				Code:     "function(context,args) { return context.user.defaultCollection.save(args.doc, args.docID); }",
				Args:     []string{"doc", "docID"},
				Allow:    &functions.Allow{Channels: []string{"*"}},
				Mutating: true,
			},
			"callerMutabilityFalse": {
				Type:     "javascript",
				Code:     "function(context,args) { var funcName = args.funcName; delete args.funcName; return context.user.function(funcName, args);}",
				Args:     []string{"doc", "docID", "funcName"},
				Allow:    &functions.Allow{Channels: []string{"*"}},
				Mutating: false,
			},
			"callerMutabilityTrue": {
				Type:     "javascript",
				Code:     "function(context,args) { var funcName = args.funcName; delete args.funcName; return context.user.function(funcName, args);}",
				Args:     []string{"doc", "docID", "funcName"},
				Allow:    &functions.Allow{Channels: []string{"*"}},
				Mutating: true,
			},
			//using context.admin priviledge overides its own mutatibility flag, it acts as though the REST API were called by an administrator.
			"callerOverride": {
				Type:     "javascript",
				Code:     "function(context,args) { var funcName = args.funcName; delete args.funcName; return context.admin.function(funcName, args);}",
				Args:     []string{"doc", "docID", "funcName"},
				Allow:    &functions.Allow{Channels: []string{"*"}},
				Mutating: false,
			},
		},
	}

	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	request, err := json.Marshal(kUserMutabilityFunctionsTestConfig)
	assert.NoError(t, err)

	var body string = `{
		"doc": {"key": 123},
		"docID": "Test123",
		"funcName": "%s"
	}`
	var putFuncName string
	var callerFuncName string

	response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
	assert.Equal(t, 200, response.Result().StatusCode)

	//Negative Cases
	t.Run("Func with mutating True calls another function with a mutating value of False", func(t *testing.T) {
		putFuncName = "putDocMutabilityFalse"
		callerFuncName = "callerMutabilityTrue"
		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", callerFuncName), fmt.Sprintf(body, putFuncName))
		assert.Equal(t, http.StatusForbidden, response.Result().StatusCode)
	})

	t.Run("Func with mutating False calls another function with a mutating value of True", func(t *testing.T) {
		putFuncName = "putDocMutabilityTrue"
		callerFuncName = "callerMutabilityFalse"
		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", callerFuncName), fmt.Sprintf(body, putFuncName))
		assert.Equal(t, http.StatusForbidden, response.Result().StatusCode)
	})

	t.Run("Func with mutating False calls another function with a mutating value of False", func(t *testing.T) {
		putFuncName = "putDocMutabilityFalse"
		callerFuncName = "callerMutabilityFalse"
		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", callerFuncName), fmt.Sprintf(body, putFuncName))
		assert.Equal(t, http.StatusForbidden, response.Result().StatusCode)
	})

	//Positive Cases

	//Mutability of the function being called is false. Still succeeds because it was called through the 'admin' object.
	t.Run("context.admin to call a Non-mutating function", func(t *testing.T) {
		putFuncName = "putDocMutabilityFalse"
		callerFuncName = "callerOverride"
		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", callerFuncName), fmt.Sprintf(body, putFuncName))
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
	})

	t.Run("Func with mutating True calls another function with a mutating value of True", func(t *testing.T) {
		putFuncName = "putDocMutabilityTrue"
		callerFuncName = "callerMutabilityTrue"
		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", callerFuncName), fmt.Sprintf(body, putFuncName))
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		assert.EqualValues(t, "\"Test123\"", string(response.BodyBytes()))
	})

	//using context.admin priviledge overides its own mutatibility flag, it acts as though the REST API were called by an administrator.
	t.Run("context.admin to call a mutating function", func(t *testing.T) {
		putFuncName = "putDocMutabilityTrue"
		callerFuncName = "callerOverride"
		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", callerFuncName), fmt.Sprintf(body, putFuncName))
		assert.Equal(t, http.StatusOK, response.Result().StatusCode)
		assert.EqualValues(t, "\"Test123\"", string(response.BodyBytes()))
	})
}

func TestFunctionTimeout(t *testing.T) {
	const timeout = 5 * time.Second
	oldTimeout := db.UserFunctionTimeout
	db.UserFunctionTimeout = timeout
	defer func() { db.UserFunctionTimeout = oldTimeout }()

	kUserTimeoutFunctionsTestConfig := &functions.FunctionsConfig{
		Definitions: functions.FunctionsDefs{
			"sleep": {
				Type: "javascript",
				Code: `function(context,args) {
					// simulating sleep using a while loop
					// can't use setTimeout in Otto interpreter
					var start = new Date().getTime();
					var ms = parseInt(args.ms)
					while (new Date().getTime() < start + ms);
				}`,
				Args:  []string{"ms"},
				Allow: allowAll,
			},
		},
	}

	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: kUserTimeoutFunctionsTestConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	// positive case:
	reqBody := fmt.Sprintf(`{"ms": %d}`, timeout.Milliseconds()/2)
	t.Run("under time limit", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_function/sleep", reqBody)
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	// negative case:
	reqBody = fmt.Sprintf(`{"ms": %d}`, 2*timeout.Milliseconds())
	t.Run("over time limit", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_function/sleep", reqBody)
		assert.Equal(t, 500, response.Result().StatusCode)
	})
}
