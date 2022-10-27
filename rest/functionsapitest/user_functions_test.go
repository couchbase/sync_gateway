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

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//////// FUNCTIONS CONFIG API TESTS (ADMIN ENDPOINTS)

// When there's no existing config, API calls return 404:
func TestFunctionsConfigGetMissing(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
func TestFunctionsConfigGet(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"square": {
					Type:  "javascript",
					Code:  "function(context,args){return args.numero * args.numero;}",
					Args:  []string{"numero"},
					Allow: &functions.Allow{Channels: []string{"wonderland"}},
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		var body functions.FunctionsConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.NotNil(t, body.Definitions["square"])
	})
	t.Run("Single", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/square", "")
		var body functions.FunctionConfig
		require.NoError(t, base.JSONUnmarshal(response.Body.Bytes(), &body))
		assert.Equal(t, "function(context,args){return args.numero * args.numero;}", body.Code)
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/bogus", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestFunctionsConfigPut(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"square": {
					Type:  "javascript",
					Code:  "function(context,args){return args.numero * args.numero;}",
					Args:  []string{"numero"},
					Allow: &functions.Allow{Channels: []string{"wonderland"}},
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/functions", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/functions", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions", `{
			"definitions": {
				"sum": {"type": "javascript",
						"code": "function(context,args){return args.numero + args.numero;}",
						"args": ["numero"],
						"allow": {"channels": ["*"]}} } }`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.Nil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		assert.Equal(t, 200, response.Result().StatusCode)
		assert.Equal(t, "26", string(response.BodyBytes()))

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("DeleteAll", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.UserFunctions)

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

func TestFunctionsConfigPutOne(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"square": {
					Type:  "javascript",
					Code:  "function(context,args){return args.numero * args.numero;}",
					Args:  []string{"numero"},
					Allow: &functions.Allow{Channels: []string{"wonderland"}},
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("PUT", "/db/_config/functions/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
		response = rt.SendRequest("DELETE", "/db/_config/function/square", "{}")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Bogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `[]`)
		assert.Equal(t, 400, response.Result().StatusCode)
		response = rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{"ruby": "foo"}`)
		assert.Equal(t, 400, response.Result().StatusCode)
	})
	t.Run("Add", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/sum", `{
			"type": "javascript",
			"code": "function(context,args){return args.numero + args.numero;}",
			"args": ["numero"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		assert.Equal(t, "26", string(response.BodyBytes()))
	})
	t.Run("ReplaceOne", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{
			"type": "javascript",
			"code": "function(context,args){return -args.n * args.n;}",
			"args": ["n"],
			"allow": {"channels": ["*"]}
		}`)
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		assert.Equal(t, "-169", string(response.BodyBytes()))
	})
	t.Run("DeleteOne", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions/square", "")
		assert.Equal(t, 200, response.Result().StatusCode)

		assert.Nil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])
		assert.Equal(t, 1, len(rt.GetDatabase().Options.UserFunctions.Definitions))

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

//////// FUNCTIONS CONFIG AND EXECUTION COMBINATIONS

var kUserFunctionConfig = &functions.FunctionsConfig{
	Definitions: functions.FunctionsDefs{
		"square": {
			Type:  "javascript",
			Code:  "function(context,args) {return args.n * args.n;}",
			Args:  []string{"n"},
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
		"squareN1QL": {
			Type:  "query",
			Code:  "SELECT $args.n * $args.n AS square",
			Args:  []string{"n"},
			Allow: &functions.Allow{Channels: []string{"*"}},
		},
	},
}

func TestSaveAndGet(t *testing.T) {
	// Setting up tester Config
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	defer rt.Close()

	request, err := json.Marshal(kUserFunctionConfig)
	assert.NoError(t, err)

	// Save The Function
	t.Run("Save The Functions", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	// Get The Function Definition and match with the one posted
	t.Run("Get All Functions And Check Schema", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.NotNil(t, response)

		var responseUserFunctionsConfig functions.FunctionsConfig
		err := json.Unmarshal(response.BodyBytes(), &responseUserFunctionsConfig)
		assert.NoError(t, err)
		assert.Equal(t, kUserFunctionConfig, &responseUserFunctionsConfig)

	})

	t.Run("Get and Check Schema Of A Specific Function", func(t *testing.T) {
		for functionName, functionDefinition := range kUserFunctionConfig.Definitions {
			response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_config/functions/%s", functionName), "")
			assert.NotNil(t, response)

			var responseUserFunctionConfig functions.FunctionConfig
			err := json.Unmarshal(response.BodyBytes(), &responseUserFunctionConfig)
			assert.NoError(t, err)
			assert.Equal(t, functionDefinition, &responseUserFunctionConfig)
		}

		// Check For Non-Existent Function
		response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_config/functions/%s", "nonExistent"), "")
		assert.NotNil(t, response)
		assert.Equal(t, 404, response.Result().StatusCode)
	})

	// GET: Run a Function and check the value
	t.Run("Run the Function and match evaluated result", func(t *testing.T) {
		functionNameToBeChecked := "square"
		response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_function/%s?n=4", functionNameToBeChecked), "")
		assert.NotNil(t, response)
		assert.Equal(t, "16", response.Body.String())

		functionNameToBeChecked = "squareN1QL"
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_function/%s?n=4", functionNameToBeChecked), "")
		assert.NotNil(t, response)

		expectedEvaluatedResponse := map[string]any{"square": float64(16)}
		var actualEvaluatedResponse []map[string]any
		err := json.Unmarshal(response.BodyBytes(), &actualEvaluatedResponse)
		assert.NoError(t, err)
		assert.Equal(t, expectedEvaluatedResponse["square"], actualEvaluatedResponse[0]["square"])
	})

	// POST: Run a Function and check the Value
	t.Run("Run The Function and Match Evaluated Result via POST", func(t *testing.T) {

		functionNameToBeChecked := "square"
		requestBody := `{"n": 4}`

		response := rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", functionNameToBeChecked), requestBody)
		assert.NotNil(t, response)
		assert.Equal(t, "16", response.Body.String())

		functionNameToBeChecked = "squareN1QL"
		response = rt.SendAdminRequest("POST", fmt.Sprintf("/db/_function/%s", functionNameToBeChecked), requestBody)
		assert.NotNil(t, response)

		expectedEvaluatedResponse := map[string]any{"square": float64(16)}
		var actualEvaluatedResponse []map[string]any
		err := json.Unmarshal(response.BodyBytes(), &actualEvaluatedResponse)
		assert.NoError(t, err)
		assert.Equal(t, expectedEvaluatedResponse["square"], actualEvaluatedResponse[0]["square"])
	})

	t.Run("Test For Able to Run Function for Non-Admin Users", func(t *testing.T) {
		response := rt.SendAdminRequest("POST", "/db/_user/", `{"name":"ritik","email":"ritik.raj@couchbase.com", "password":"letmein", "admin_channels":["*"]}`)
		assert.Equal(t, 201, response.Result().StatusCode)

		response = rt.SendUserRequestWithHeaders("GET", fmt.Sprintf("/db/_function/%s?n=4", "square"), "", nil, "ritik", "letmein")
		assert.NotNil(t, response)
		assert.Equal(t, "16", response.Body.String())
	})

}

func TestSaveAndUpdateAndGet(t *testing.T) {
	// Cloning the userFunctionConfig Map
	var kUserFunctionConfigCopy = &functions.FunctionsConfig{
		MaxFunctionCount: kUserFunctionConfig.MaxFunctionCount,
		MaxCodeSize:      kUserFunctionConfig.MaxCodeSize,
		MaxRequestSize:   kUserFunctionConfig.MaxRequestSize,
		Definitions:      map[string]*functions.FunctionConfig{},
	}
	for functionName, functionConfig := range kUserFunctionConfig.Definitions {
		kUserFunctionConfigCopy.Definitions[functionName] = functionConfig
	}

	// Setting up tester Config
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	defer rt.Close()

	request, err := json.Marshal(kUserFunctionConfigCopy)
	assert.NoError(t, err)

	// Save The Function
	t.Run("Save The Functions", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	// Get The Function Definition and match with the one posted
	t.Run("Get All Functions And Check Schema", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.NotNil(t, response)

		var responseUserFunctionsConfig functions.FunctionsConfig
		err := json.Unmarshal(response.BodyBytes(), &responseUserFunctionsConfig)
		assert.NoError(t, err)
		assert.Equal(t, kUserFunctionConfigCopy, &responseUserFunctionsConfig)
	})

	// Update a Function
	t.Run("Update The Function", func(t *testing.T) {
		functionName := "square"

		// Change multiplication sign to Addition sign
		kUserFunctionConfigCopy.Definitions[functionName].Code = `function(context,args) {return args.n + args.n}`
		requestBody, err := json.Marshal(kUserFunctionConfigCopy.Definitions[functionName])
		assert.NoError(t, err)

		response := rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_config/functions/%s", functionName), string(requestBody))
		assert.Equal(t, 200, response.Result().StatusCode)

		functionName = "squareN1QL"

		// Change multiplication sign to Addition sign
		kUserFunctionConfigCopy.Definitions[functionName].Code = `SELECT $args.n + $args.n AS square`
		requestBody, err = json.Marshal(kUserFunctionConfigCopy.Definitions[functionName])
		assert.NoError(t, err)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_config/functions/%s", functionName), string(requestBody))
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	// Get the Updated Function
	t.Run("GET The Updated Function", func(t *testing.T) {
		for fnName, fnBody := range kUserFunctionConfigCopy.Definitions {
			functionName := fnName
			response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_config/functions/%s", functionName), "")
			assert.NotNil(t, response)

			var responseUserFunctionConfig functions.FunctionConfig
			err := json.Unmarshal(response.BodyBytes(), &responseUserFunctionConfig)
			assert.NoError(t, err)
			assert.Equal(t, fnBody, &responseUserFunctionConfig)
		}
	})

	// GET : Evaluate the Updated Function
	t.Run("Run the Function and match evaluated result", func(t *testing.T) {
		functionNameToBeChecked := "square"
		response := rt.SendAdminRequest("GET", fmt.Sprintf("/db/_function/%s?n=4", functionNameToBeChecked), "")
		assert.NotNil(t, response)
		assert.Equal(t, "8", response.Body.String())

		functionNameToBeChecked = "squareN1QL"
		response = rt.SendAdminRequest("GET", fmt.Sprintf("/db/_function/%s?n=4", functionNameToBeChecked), "")
		assert.NotNil(t, response)

		expectedEvaluatedResponse := map[string]any{"square": float64(8)}
		var actualEvaluatedResponse []map[string]any
		err := json.Unmarshal(response.BodyBytes(), &actualEvaluatedResponse)
		assert.NoError(t, err)
		assert.Equal(t, expectedEvaluatedResponse["square"], actualEvaluatedResponse[0]["square"])
	})
}

/// ILLEGAL SYNTAX TESTS

func TestIllegalSyntax(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	response := rt.SendAdminRequest("PUT", "/db/_config/functions", `{
		"definitions": {
			"syntax_error": {
				"type": "javascript",
				"code": "returm )42(",
				"allow": {"channels": ["*"]}}
		}
	}`)
	assert.Equal(t, 400, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "Error compiling function")

	// Can only register SELECT queries
	response = rt.SendAdminRequest("PUT", "/db/_config/functions", `{
		"definitions": {
			"evil_n1ql_mutation": {
				"type": "query",
				"code": "DROP COLLECTION Students",
				"allow": {"channels": ["*"]}}
		}
	}`)
	assert.Equal(t, 400, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "only SELECT queries are allowed")

	// A bad SELECT query can be registered to config
	// But, executing it will result in an error
	response = rt.SendAdminRequest("PUT", "/db/_config/functions", `{
		"definitions": {
			"bad_n1ql_syntax": {
				"type": "query",
				"code": "SELECT )22( AS Students :",
				"allow": {"channels": ["*"]}}
		}
	}`)
	assert.Equal(t, 200, response.Result().StatusCode)

	response = rt.SendAdminRequest("GET", "/db/_function/bad_n1ql_syntax", "")
	assert.Equal(t, 500, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "Internal Server Error")
	assert.Contains(t, string(response.BodyBytes()), "syntax error")
}

//////// FUNCTIONS EXECUTION API TESTS

/// AUTH TESTS

var allowAll = &functions.Allow{Channels: []string{"*"}}

func createUserAlice(t *testing.T, rt *rest.RestTester) (string, string) {
	resp := rt.SendAdminRequest("POST", "/db/_role/", `{"name":"hero", "admin_channels":["heroes"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)
	resp = rt.SendAdminRequest("POST", "/db/_role/", `{"name":"villain", "admin_channels":["villains"]}`)
	rest.RequireStatus(t, resp, http.StatusCreated)

	username := "alice"
	password := "pass"

	userDetails := fmt.Sprintf(`{"name":"%s", "password":"%s", "admin_channels":["wonderland", "lookingglass", "city-London", "user-alice"], "admin_roles": ["hero"]}`, username, password)
	t.Logf("shaad: %+v\n", userDetails)
	resp = rt.SendAdminRequest("POST", "/db/_user/", userDetails)
	rest.RequireStatus(t, resp, http.StatusCreated)

	return username, password
}

func TestUserFunctions(t *testing.T) {
	kUserFunctionAuthTestConfig := functions.FunctionsConfig{
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

func testUserFunctionsCommon(t *testing.T, rt *rest.RestTester, sendReqFn func(string, string, string) *rest.TestResponse) {
	// Basic call passing a parameter:
	response := sendReqFn("POST", "/db/_function/square", `{"numero": 42}`)
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "1764", string(response.BodyBytes()))

	response = sendReqFn("GET", "/db/_function/square?numero=42", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "1764", string(response.BodyBytes()))

	// Function that calls a function:
	response = sendReqFn("GET", "/db/_function/call_fn", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "49", string(response.BodyBytes()))

	// `requireUser` test that passes:
	response = sendReqFn("GET", "/db/_function/alice_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	// `requireChannel` test that passes:
	response = sendReqFn("GET", "/db/_function/wonderland_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	// `requireRole` test that passes:
	response = sendReqFn("GET", "/db/_function/hero_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	// Max call depth:
	response = sendReqFn("GET", "/db/_function/factorial?n=20", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "2432902008176640000", string(response.BodyBytes()))
}

func testUserFunctionsAsAdmin(t *testing.T, rt *rest.RestTester) {
	testUserFunctionsCommon(t, rt, rt.SendAdminRequest)

	// Admin-only (success):
	response := rt.SendAdminRequest("GET", "/db/_function/admin_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	response = rt.SendAdminRequest("GET", "/db/_function/require_admin", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	response = rt.SendAdminRequest("GET", "/db/_function/pevensies_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	response = rt.SendAdminRequest("GET", "/db/_function/narnia_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	response = rt.SendAdminRequest("GET", "/db/_function/villain_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"OK\"", string(response.BodyBytes()))

	// ERRORS:
	// Checking `context.user.name`:
	response = rt.SendAdminRequest("GET", "/db/_function/user_only", "")
	assert.Equal(t, 500, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "No user")

	// No such function:
	response = rt.SendAdminRequest("GET", "/db/_function/xxxx", "")
	assert.Equal(t, 404, response.Result().StatusCode)
}

func testUserFunctionsAsUser(t *testing.T, rt *rest.RestTester) {
	username, password := createUserAlice(t, rt)
	sendReqFn := func(method, resource, body string) *rest.TestResponse {
		return rt.SendUserRequestWithHeaders(method, resource, body, nil, username, password)
	}
	testUserFunctionsCommon(t, rt, sendReqFn)

	// Checking `context.user.name`:
	response := sendReqFn("GET", "/db/_function/user_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "\"alice\"", string(response.BodyBytes()))

	// Checking `context.admin.func`
	response = sendReqFn("GET", "/db/_function/sudo_call_forbidden", "")
	assert.Equal(t, 200, response.Result().StatusCode)

	response = sendReqFn("GET", "/db/_function/xxxx", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	response = sendReqFn("GET", "/db/_function/great_and_terrible", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	response = sendReqFn("GET", "/db/_function/call_forbidden", "")
	assert.Equal(t, 403, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "great_and_terrible")

	response = sendReqFn("GET", "/db/_function/admin_only", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	response = sendReqFn("GET", "/db/_function/require_admin", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	response = sendReqFn("GET", "/db/_function/pevensies_only", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	response = sendReqFn("GET", "/db/_function/narnia_only", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	response = sendReqFn("GET", "/db/_function/villain_only", "")
	assert.Equal(t, 403, response.Result().StatusCode)
}

func TestUserN1QLQueries(t *testing.T) {
	var kUserN1QLFunctionsAuthTestConfig = functions.FunctionsConfig{
		Definitions: functions.FunctionsDefs{
			"airports_in_city": &functions.FunctionConfig{
				Type:  "query",
				Code:  `SELECT $$args.city AS city`,
				Args:  []string{"city"},
				Allow: &functions.Allow{Channels: []string{"city-$${args.city}", "allcities"}},
			},
			"square": &functions.FunctionConfig{
				Type:  "query",
				Code:  "SELECT $$args.numero * $$args.numero AS square",
				Args:  []string{"numero"},
				Allow: &functions.Allow{Channels: []string{"wonderland"}},
			},
			"user": &functions.FunctionConfig{
				Type: "query",
				//todo: if instead of our_user we want to use the word user
				// we can't use it direclty as "user" is a reserved keyword in n1ql
				// n1ql expects backticks for such cases to avoid conflict with reserved keywords
				// eg `user`
				// but, our JSON parsing for DB config preprocess backticks,
				// how to get around this ???
				Code:  "SELECT $$user AS our_user",
				Allow: allowAll,
			},
			"user_parts": &functions.FunctionConfig{
				Type:  "query",
				Code:  "SELECT $$user.name AS name, $$user.email AS email",
				Allow: &functions.Allow{Channels: []string{"user-$${context.user.name}"}},
			},
			"admin_only": &functions.FunctionConfig{
				Type:  "query",
				Code:  `SELECT "ok" AS status`,
				Allow: nil, // no 'allow' property means admin-only
			},
			"inject": &functions.FunctionConfig{
				Type:  "query",
				Code:  `SELECT $$args.foo`,
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

	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: &kUserN1QLFunctionsAuthTestConfig,
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("AsAdmin", func(t *testing.T) { testUserQueriesAsAdmin(t, rt) })
	t.Run("AsUser", func(t *testing.T) { testUserQueriesAsUser(t, rt) })
}

func testUserQueriesCommon(t *testing.T, rt *rest.RestTester, sendReqFn func(string, string, string) *rest.TestResponse) {
	// dynamic channel list
	response := sendReqFn("POST", "/db/_function/airports_in_city", `{"city": "London"}`)
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "[{\"city\":\"London\"}\n]\n", string(response.BodyBytes()))

	response = sendReqFn("POST", "/db/_function/square", `{"numero": 16}`)
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "[{\"square\":256}\n]\n", string(response.BodyBytes()))

	response = sendReqFn("POST", "/db/_function/inject", `{"foo": "1337 as pwned"}`)
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "[{\"foo\":\"1337 as pwned\"}\n]\n", string(response.BodyBytes()))

	// ERRORS:

	// Missing a parameter:
	response = sendReqFn("GET", "/db/_function/square", "")
	assert.Equal(t, 400, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "numero")
	assert.Contains(t, string(response.BodyBytes()), "square")

	// Extra parameter:
	response = sendReqFn("POST", "/db/_function/square", `{"numero": 42, "number": 0}`)
	assert.Equal(t, 400, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "number")
	assert.Contains(t, string(response.BodyBytes()), "square")

	// Function definition has a syntax error:
	response = sendReqFn("GET", "/db/_function/syntax_error", "")
	assert.Equal(t, 500, response.Result().StatusCode)
	assert.Contains(t, string(response.BodyBytes()), "syntax_error")
}

func testUserQueriesAsAdmin(t *testing.T, rt *rest.RestTester) {
	testUserQueriesCommon(t, rt, rt.SendAdminRequest)

	// Admin-only (success):
	response := rt.SendAdminRequest("GET", "/db/_function/user", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "[{\"our_user\":{}}\n]\n", string(response.BodyBytes()))

	response = rt.SendAdminRequest("GET", "/db/_function/user_parts", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "[{}\n]\n", string(response.BodyBytes()))

	// admin only:
	response = rt.SendAdminRequest("GET", "/db/_function/admin_only", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "[{\"status\":\"ok\"}\n]\n", string(response.BodyBytes()))

	// ERRORS:

	// No such query:
	response = rt.SendAdminRequest("GET", "/db/_function/xxxx", "")
	assert.Equal(t, 404, response.Result().StatusCode)
}

func testUserQueriesAsUser(t *testing.T, rt *rest.RestTester) {
	username, password := createUserAlice(t, rt)
	sendReqFn := func(method, resource, body string) *rest.TestResponse {
		return rt.SendUserRequestWithHeaders(method, resource, body, nil, username, password)
	}
	testUserQueriesCommon(t, rt, sendReqFn)

	response := sendReqFn("GET", "/db/_function/user", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.True(t, strings.HasPrefix(string(response.BodyBytes()), `[{"our_user":{"channels":["`))
	assert.True(t, strings.HasSuffix(string(response.BodyBytes()), "\"],\"email\":\"\",\"name\":\"alice\",\"roles\":[\"hero\"]}}\n]\n"))

	response = sendReqFn("GET", "/db/_function/user_parts", "")
	assert.Equal(t, 200, response.Result().StatusCode)
	assert.EqualValues(t, "[{\"email\":\"\",\"name\":\"alice\"}\n]\n", string(response.BodyBytes()))

	// ERRORS:

	// Not allowed (admin only):
	response = sendReqFn("GET", "/db/_function/admin_only", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	// Not allowed (dynamic channel list):
	response = sendReqFn("GET", "/db/_function/airports_in_city?city=Chicago", "")
	assert.Equal(t, 403, response.Result().StatusCode)

	response = sendReqFn("POST", "/db/_function/airports_in_city", `{"city": "Chicago"}`)
	assert.Equal(t, 403, response.Result().StatusCode)

	// No such query:
	response = sendReqFn("GET", "/db/_function/xxxx", "")
	assert.Equal(t, 403, response.Result().StatusCode)
}
