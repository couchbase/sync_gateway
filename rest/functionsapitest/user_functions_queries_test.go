//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package functionsapitest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/couchbase/sync_gateway/db"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
)

//////// FUNCTIONS EXECUTION API TESTS

/// AUTH TESTS

func createUserAlice(t *testing.T, rt *rest.RestTester) (string, string) {
	resp := rt.SendAdminRequest("POST", "/db/_role/", `{"name":"hero", "admin_channels":["heroes"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("POST", "/db/_role/", `{"name":"villain", "admin_channels":["villains"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	username := "alice"
	password := "pass"

	userDetails := fmt.Sprintf(`{"name":"%s", "password":"%s", "admin_channels":["wonderland", "lookingglass", "city-London", "user-alice"], "admin_roles": ["hero"]}`, username, password)
	resp = rt.SendAdminRequest("POST", "/db/_user/", userDetails)
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
		"exceptional": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {throw "oops";}`,
			Allow: allowAll,
		},
		"call_fn": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.user.function("square", {numero: 7});}`,
			Allow: allowAll,
		},
		"factorial": &functions.FunctionConfig{
			Type: "javascript",
			Args: []string{"n"},
			Code: `function(context, args) {if (args.n <= 1) return 1;
						else return args.n * context.user.function("factorial", {n: args.n-1});}`,
			Allow: allowAll,
		},
		"great_and_terrible": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return "I am OZ the great and terrible";}`,
			Allow: &functions.Allow{Channels: []string{"oz", "narnia"}},
		},
		"call_forbidden": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.user.function("great_and_terrible");}`,
			Allow: allowAll,
		},
		"sudo_call_forbidden": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return context.admin.function("great_and_terrible");}`,
			Allow: allowAll,
		},
		"admin_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {return "OK";}`,
			Allow: nil, // no 'allow' property means admin-only
		},
		"require_admin": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireAdmin(); return "OK";}`,
			Allow: allowAll,
		},
		"user_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {if (!context.user.name) throw "No user"; return context.user.name;}`,
			Allow: &functions.Allow{Channels: []string{"user-$${context.user.name}"}},
		},
		"alice_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireUser("alice"); return "OK";}`,
			Allow: allowAll,
		},
		"pevensies_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireUser(["peter","jane","eustace","lucy"]); return "OK";}`,
			Allow: allowAll,
		},
		"wonderland_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireAccess("wonderland"); context.requireAccess(["wonderland", "snark"]); return "OK";}`,
			Allow: allowAll,
		},
		"narnia_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireAccess("narnia"); return "OK";}`,
			Allow: allowAll,
		},
		"hero_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireRole(["hero", "antihero"]); return "OK";}`,
			Allow: allowAll,
		},
		"villain_only": &functions.FunctionConfig{
			Type:  "javascript",
			Code:  `function(context, args) {context.requireRole(["villain"]); return "OK";}`,
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

	// Updating kUserFunctionAuthTestConfig because $ and $$ in user_only functionConfig
	kUserFunctionAuthTestConfig.Definitions["user_only"] = &functions.FunctionConfig{
		Type:  "javascript",
		Code:  `function(context, args) {if (!context.user.name) throw "No user"; return context.user.name;}`,
		Allow: &functions.Allow{Channels: []string{"user-${context.user.name}"}},
	}

	rt.DatabaseConfig = &rest.DatabaseConfig{
		DbConfig: rest.DbConfig{
			UserFunctions: &kUserFunctionAuthTestConfig,
		},
	}

	testUserFunctionsCommon(t, rt, rt.SendRequest, true)
}

func testUserFunctionsCommon(t *testing.T, rt *rest.RestTester, sendReqFn func(string, string, string) *rest.TestResponse, isGuest bool) {
	t.Run("commons/passing a param through body", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/square", `{"numero": 42}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "1764", string(response.BodyBytes()))
	})

	t.Run("commons/passing a param through query params", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/square?numero=42", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "1764", string(response.BodyBytes()))
	})

	t.Run("commons/function that calls a function", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/call_fn", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "49", string(response.BodyBytes()))
	})

	t.Run("commons/`requireUser`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/alice_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
	})

	t.Run("commons/`requireChannel`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/wonderland_only", "")
		if isGuest {
			assert.Equal(t, 401, response.Result().StatusCode)
		} else {
			assert.Equal(t, 200, response.Result().StatusCode)
			assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
		}
	})

	t.Run("commons/`requireRole`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/hero_only", "")
		if isGuest {
			assert.Equal(t, 401, response.Result().StatusCode)
		} else {
			assert.Equal(t, 200, response.Result().StatusCode)
			assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
		}
	})

	t.Run("commons/equals max call depth", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/factorial?n=20", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "2432902008176640000", string(response.BodyBytes()))
	})

	// negative cases
	t.Run("commons/exceeding max call depth", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/factorial?n=30", "")
		assert.Equal(t, 508, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "Loop Detected")
		assert.Contains(t, string(response.BodyBytes()), "recursion too deep")
	})
}

func testUserFunctionsAsAdmin(t *testing.T, rt *rest.RestTester) {
	testUserFunctionsCommon(t, rt, rt.SendAdminRequest, false)

	// positive cases :
	t.Run("Admin-only", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
	})

	t.Run("`requireAdmin`", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/require_admin", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
	})

	t.Run("`requireUser`", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/pevensies_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
	})

	t.Run("`requireAccess`", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/narnia_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
	})

	t.Run("`requireRole`", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/villain_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))
	})

	// negative cases:
	t.Run("user only `context.user.name`", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/user_only", "")
		assert.Equal(t, 500, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "No user")
	})

	t.Run("function not configured", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func testUserFunctionsAsUser(t *testing.T, rt *rest.RestTester) {
	username, password := createUserAlice(t, rt)
	sendReqFn := func(method, resource, body string) *rest.TestResponse {
		return rt.SendUserRequestWithHeaders(method, resource, body, nil, username, password)
	}
	testUserFunctionsCommon(t, rt, sendReqFn, false)

	// positive cases:
	t.Run("user only (`context.user.name`)", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/user_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "\"alice\"", string(response.BodyBytes()))
	})

	t.Run("calling other function `context.admin.function`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/sudo_call_forbidden", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	// negative cases
	t.Run("function not configured", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("specific channels only", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/great_and_terrible", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("unauthorized channels", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/call_forbidden", "")
		assert.Equal(t, 403, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "great_and_terrible")
	})

	t.Run("admin-only", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("`requireAdmin`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/require_admin", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("`requireUser`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/pevensies_only", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("`requireAccess`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/narnia_only", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("`requireRole`", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/villain_only", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})
}

var kUserN1QLFunctionsAuthTestConfig = functions.FunctionsConfig{
	Definitions: functions.FunctionsDefs{
		"airports_in_city": &functions.FunctionConfig{
			Type:  "query",
			Code:  `SELECT $args.city AS city`,
			Args:  []string{"city"},
			Allow: &functions.Allow{Channels: []string{"city-${args.city}", "allcities"}},
		},
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
		"user_parts": &functions.FunctionConfig{
			Type:  "query",
			Code:  "SELECT $user.name AS name, $user.email AS email",
			Allow: &functions.Allow{Channels: []string{"user-${context.user.name}"}},
		},
		"admin_only": &functions.FunctionConfig{
			Type:  "query",
			Code:  `SELECT "ok" AS status`,
			Allow: nil, // no 'allow' property means admin-only
		},
		"inject": &functions.FunctionConfig{
			Type:  "query",
			Code:  `SELECT $args.foo`,
			Args:  []string{"foo"},
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
		"syntax_error": &functions.FunctionConfig{
			Type:  "query",
			Code:  "SELECT OOK? FR0M OOK!",
			Allow: allowAll,
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

	testUserQueriesCommon(t, rt, rt.SendRequest)
}

func testUserQueriesCommon(t *testing.T, rt *rest.RestTester, sendReqFn func(string, string, string) *rest.TestResponse) {
	// positive cases:
	t.Run("commons/dynamic channel list", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/airports_in_city", `{"city": "London"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"city\":\"London\"}\n]\n", string(response.BodyBytes()))
	})

	t.Run("commons/passing a param through body", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/square", `{"numero": 16}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"square\":256}\n]\n", string(response.BodyBytes()))
	})

	t.Run("commons/query injection through params", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/inject", `{"foo": "1337 as pwned"}`)
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"foo\":\"1337 as pwned\"}\n]\n", string(response.BodyBytes()))
	})

	// negative cases:
	t.Run("commons/missing a parameter", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/square", "")
		assert.Equal(t, 400, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "numero")
		assert.Contains(t, string(response.BodyBytes()), "square")
	})

	t.Run("commons/extra parameter", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/square", `{"numero": 42, "number": 0}`)
		assert.Equal(t, 400, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "number")
		assert.Contains(t, string(response.BodyBytes()), "square")
	})

	t.Run("commons/query syntax error", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/syntax_error", "")
		assert.Equal(t, 500, response.Result().StatusCode)
		assert.Contains(t, string(response.BodyBytes()), "syntax_error")
	})
}

func testUserQueriesAsAdmin(t *testing.T, rt *rest.RestTester) {
	testUserQueriesCommon(t, rt, rt.SendAdminRequest)

	// positive cases:
	t.Run("select user", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/user", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"user\":{}}\n]\n", string(response.BodyBytes()))
	})

	t.Run("select user parts (user.name and user.email)", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/user_parts", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{}\n]\n", string(response.BodyBytes()))
	})

	t.Run("admin only", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"status\":\"ok\"}\n]\n", string(response.BodyBytes()))
	})

	//negative cases:

	t.Run("unconfigured query", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func testUserQueriesAsUser(t *testing.T, rt *rest.RestTester) {
	username, password := createUserAlice(t, rt)
	sendReqFn := func(method, resource, body string) *rest.TestResponse {
		return rt.SendUserRequestWithHeaders(method, resource, body, nil, username, password)
	}
	testUserQueriesCommon(t, rt, sendReqFn)

	// positive cases:
	t.Run("select user", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/user", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.True(t, strings.HasPrefix(string(response.BodyBytes()), `[{"user":{"channels":["`))
		assert.True(t, strings.HasSuffix(string(response.BodyBytes()), "\"],\"email\":\"\",\"name\":\"alice\",\"roles\":[\"hero\"]}}\n]\n"))
	})

	t.Run("select user parts (user.name and user.email)", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/user_parts", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.EqualValues(t, "[{\"email\":\"\",\"name\":\"alice\"}\n]\n", string(response.BodyBytes()))
	})

	//negative cases:
	t.Run("admin only", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/admin_only", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("unauthorized dynamic channel list through query params", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/airports_in_city?city=Chicago", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("unauthorized dynamic channel list through body", func(t *testing.T) {
		response := sendReqFn("POST", "/db/_function/airports_in_city", `{"city": "Chicago"}`)
		assert.Equal(t, 403, response.Result().StatusCode)
	})

	t.Run("unconfigured query", func(t *testing.T) {
		response := sendReqFn("GET", "/db/_function/xxxx", "")
		assert.Equal(t, 403, response.Result().StatusCode)
	})
}

var kUserMutabilityFunctionsTestConfig = &functions.FunctionsConfig{
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

func TestFunctionMutability(t *testing.T) {
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

	//Mutability of the function being called is false. Will fail as once you’ve lost the ability to mutate, you can’t get it back.
	t.Run("context.admin to call a Non-mutating function", func(t *testing.T) {
		putFuncName = "putDocMutabilityFalse"
		callerFuncName = "callerOverride"
		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", callerFuncName), fmt.Sprintf(body, putFuncName))
		assert.Equal(t, http.StatusForbidden, response.Result().StatusCode)
	})

	//Positive Cases
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
	reqBody := fmt.Sprintf(`{"ms": %d}`, db.UserFunctionTimeout.Milliseconds()/2)
	t.Run("under time limit", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_function/sleep", reqBody)
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	// negative case:
	reqBody = fmt.Sprintf(`{"ms": %d}`, 2*db.UserFunctionTimeout.Milliseconds())
	t.Run("over time limit", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_function/sleep", reqBody)
		assert.Equal(t, 500, response.Result().StatusCode)
	})
}
