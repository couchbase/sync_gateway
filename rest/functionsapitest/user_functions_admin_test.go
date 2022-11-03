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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//////// ALL ADMIN USER QUERY APIS:

// When feature flag is not enabled, all API calls return 404:
func TestFunctionsConfigGetWithoutFeatureFlag(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{EnableUserQueries: false})
	defer rt.Close()

	t.Run("Functions, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("All Functions", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
	t.Run("Single Function", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}

// Test use of "Etag" and "If-Match" headers to safely update function/query/graphql config.
func TestFunctionsConfigMVCC(t *testing.T) {
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{
		UserFunctions: &functions.FunctionsConfig{
			Definitions: functions.FunctionsDefs{
				"xxx": {
					Type: "javascript",
					Code: "function(){return 42;}",
				},
				"xxxN1QL": {
					Type: "query",
					Code: "SELECT 42",
				},
				"yyy": {
					Type: "query",
					Code: "SELECT 999",
				},
			},
		},
	})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("Function", func(t *testing.T) {
		runTestFunctionsConfigMVCC(t, rt, "/db/_config/functions/xxx", `{
			"type": "javascript",
			"code": "function(){return 69;}"
		}`)
	})

	t.Run("Query", func(t *testing.T) {
		runTestFunctionsConfigMVCC(t, rt, "/db/_config/functions/xxxN1QL", `{
			"type": "query",
			"code": "select 69"
		}`)
	})

	t.Run("Functions", func(t *testing.T) {
		runTestFunctionsConfigMVCC(t, rt, "/db/_config/functions", `{
			"definitions": {
				"zzz": {"type": "javascript", "code": "function(){return 69;}"} } }`)
	})
}

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

func TestMaxRequestSize(t *testing.T) {
	var testFunctionConfig = functions.FunctionsConfig{
		Definitions: functions.FunctionsDefs{
			"multiply": &functions.FunctionConfig{
				Type:  "javascript",
				Code:  "function(context, args) {return args.first * args.second * args.third * args.fourth;}",
				Args:  []string{"first", "second", "third", "fourth"},
				Allow: allowAll,
			},
			"multiplyN1QL": {
				Type:  "query",
				Code:  "SELECT $args.`first` * $args.second * $args.third * $args.fourth AS result",
				Args:  []string{"first", "second", "third", "fourth"},
				Allow: allowAll,
			},
		},
	}

	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	// positive cases:
	t.Run("request size less than MaxRequestSize", func(t *testing.T) {
		testFunctionConfig.MaxRequestSize = base.IntPtr(1000)

		request, err := json.Marshal(testFunctionConfig)
		assert.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
		assert.Equal(t, 200, response.Result().StatusCode)

		t.Run("js function", func(t *testing.T) {
			response := rt.SendAdminRequest("POST", "/db/_function/multiply", `{"first":1,"second":2,"third":3,"fourth":4}`)
			assert.Equal(t, 200, response.Result().StatusCode)
			assert.Equal(t, "24", string(response.BodyBytes()))
		})

		t.Run("n1ql query", func(t *testing.T) {
			response := rt.SendAdminRequest("POST", "/db/_function/multiplyN1QL", `{"first":1,"second":2,"third":3,"fourth":4}`)
			assert.Equal(t, 200, response.Result().StatusCode)
			assert.Equal(t, "[{\"result\":24}\n]\n", string(response.BodyBytes()))
		})
	})

	// negative cases:
	t.Run("request size greater than MaxRequestSize", func(t *testing.T) {
		testFunctionConfig.MaxRequestSize = base.IntPtr(5)

		request, err := json.Marshal(testFunctionConfig)
		assert.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
		assert.Equal(t, 200, response.Result().StatusCode)

		t.Run("js function", func(t *testing.T) {
			response = rt.SendAdminRequest("POST", "/db/_function/multiply", `{"first":1,"second":2,"third":3,"fourth":4}`)
			assert.Equal(t, 413, response.Result().StatusCode)
			assert.Contains(t, string(response.BodyBytes()), "Arguments too large")
		})

		t.Run("n1ql query", func(t *testing.T) {
			response := rt.SendAdminRequest("POST", "/db/_function/multiplyN1QL", `{"first":1,"second":2,"third":3,"fourth":4}`)
			assert.Equal(t, 413, response.Result().StatusCode)
			assert.Contains(t, string(response.BodyBytes()), "Arguments too large")
		})
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

// // TESTING PATHS A DEVELOPER MIGHT TRY TO GO THROUGH WHILE INTERACTING WITH THE FUNCTIONS API.
// // these include both negative and positive cases
// // e.g. PUT -> GET -> DELETE, or GET(negative) -> PUT -> GET or DELETE(negative)->PUT
func TestSaveAndGet(t *testing.T) {
	// Setting up tester Config
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	request, err := json.Marshal(kUserFunctionConfig)
	assert.NoError(t, err)

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
	if rt == nil {
		return
	}
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

func TestSaveAndDeleteAndGet(t *testing.T) {
	// Setting up tester Config
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	request, err := json.Marshal(kUserFunctionConfig)
	assert.NoError(t, err)

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

	functionNameToBeDeleted := "square"

	t.Run("Delete A Specific Function", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", fmt.Sprintf("/db/_config/functions/%s", functionNameToBeDeleted), "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("Get remaining functions and check schema", func(t *testing.T) {
		var kUserFunctionConfigCopy = &functions.FunctionsConfig{
			MaxFunctionCount: kUserFunctionConfig.MaxFunctionCount,
			MaxCodeSize:      kUserFunctionConfig.MaxCodeSize,
			MaxRequestSize:   kUserFunctionConfig.MaxRequestSize,
			Definitions:      map[string]*functions.FunctionConfig{},
		}
		for functionName, functionConfig := range kUserFunctionConfig.Definitions {
			if functionName != functionNameToBeDeleted {
				kUserFunctionConfigCopy.Definitions[functionName] = functionConfig
			}
		}
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.NotNil(t, response)

		var responseUserFunctionsConfig functions.FunctionsConfig
		err := json.Unmarshal(response.BodyBytes(), &responseUserFunctionsConfig)
		assert.NoError(t, err)
		assert.Equal(t, kUserFunctionConfigCopy, &responseUserFunctionsConfig)

	})

	// Delete All functions
	t.Run("Delete all functions", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions", "")
		assert.Equal(t, 200, response.Result().StatusCode)
	})

	t.Run("Get All Non-exisitng Functions And Check HTTP Status", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)

	})
}
func TestDeleteNonExisting(t *testing.T) {
	// Setting up tester Config
	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	// NEGATIVE CASES
	t.Run("Delete All Non-existing functions and check HTTP Status Code", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})

	t.Run("Delete a non-existing function and check HTTP Status Code", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions/foo", "")
		assert.Equal(t, 404, response.Result().StatusCode)
	})
}
