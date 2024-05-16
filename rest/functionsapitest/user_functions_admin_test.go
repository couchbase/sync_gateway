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
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/couchbase/sync_gateway/rest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var allowAll = &functions.Allow{Channels: []string{"*"}}

//////// ALL ADMIN USER QUERY APIS:

// When feature flag is not enabled, all API calls return 404:
func TestFunctionsConfigGetWithoutFeatureFlag(t *testing.T) {
	rt := rest.NewRestTester(t, &rest.RestTesterConfig{EnableUserQueries: false})
	defer rt.Close()

	t.Run("Functions, Non-Admin", func(t *testing.T) {
		response := rt.SendRequest("GET", "/db/_config/functions", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
	t.Run("All Functions", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
	t.Run("Single Function", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
}

func runTestFunctionsConfigMVCC(t *testing.T, rt *rest.RestTester, uri string, newValue string) {
	// Get initial etag:
	response := rt.SendAdminRequest("GET", uri, "")
	rest.RequireStatus(t, response, http.StatusOK)
	etag := response.HeaderMap.Get("Etag")
	assert.Regexp(t, `"[^"]+"`, etag)

	// Update config, just to change its etag:
	response = rt.SendAdminRequest("PUT", uri, newValue)
	rest.RequireStatus(t, response, http.StatusOK)
	newEtag := response.HeaderMap.Get("Etag")
	assert.Regexp(t, `"[^"]+"`, newEtag)
	assert.NotEqual(t, etag, newEtag)

	// A GET should also return the new etag:
	response = rt.SendAdminRequest("GET", uri, "")
	rest.RequireStatus(t, response, http.StatusOK)
	assert.Equal(t, newEtag, response.HeaderMap.Get("Etag"))

	// Try to update using If-Match with the old etag:
	headers := map[string]string{"If-Match": etag}
	response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
	rest.RequireStatus(t, response, http.StatusPreconditionFailed)

	// Now update successfully using the current etag:
	headers["If-Match"] = newEtag
	response = rt.SendAdminRequestWithHeaders("PUT", uri, newValue, headers)
	rest.RequireStatus(t, response, http.StatusOK)
	newestEtag := response.HeaderMap.Get("Etag")
	assert.Regexp(t, `"[^"]+"`, newestEtag)
	assert.NotEqual(t, etag, newestEtag)
	assert.NotEqual(t, newEtag, newestEtag)

	// Try to delete using If-Match with the previous etag:
	response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
	rest.RequireStatus(t, response, http.StatusPreconditionFailed)

	// Now delete successfully using the current etag:
	headers["If-Match"] = newestEtag
	response = rt.SendAdminRequestWithHeaders("DELETE", uri, newValue, headers)
	rest.RequireStatus(t, response, http.StatusOK)
}

// Test use of "Etag" and "If-Match" headers to safely update function/query config.
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
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
	t.Run("All", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
	t.Run("Missing", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions/cube", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
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
		rest.RequireStatus(t, response, http.StatusNotFound)
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
		rest.RequireStatus(t, response, http.StatusNotFound)
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
		rest.RequireStatus(t, response, http.StatusNotFound)
		response = rt.SendRequest("DELETE", "/db/_config/functions", "{}")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
	t.Run("ReplaceAll", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions", `{
			"definitions": {
				"sum": {"type": "javascript",
						"code": "function(context,args){return args.numero + args.numero;}",
						"args": ["numero"],
						"allow": {"channels": ["*"]}} } }`)
		rest.RequireStatus(t, response, http.StatusOK)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.Nil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		rest.RequireStatus(t, response, http.StatusOK)
		assert.Equal(t, "26", response.BodyString())

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
	t.Run("DeleteAll", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions", "")
		rest.RequireStatus(t, response, http.StatusOK)

		assert.Nil(t, rt.GetDatabase().Options.UserFunctions)

		response = rt.SendAdminRequest("GET", "/db/_function/square?numero=13", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
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
		rest.RequireStatus(t, response, http.StatusNotFound)
		response = rt.SendRequest("DELETE", "/db/_config/function/square", "{}")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
	t.Run("Bogus", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `[]`)
		rest.RequireStatus(t, response, http.StatusBadRequest)
		response = rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{"ruby": "foo"}`)
		rest.RequireStatus(t, response, http.StatusBadRequest)
	})
	t.Run("Add", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/sum", `{
			"type": "javascript",
			"code": "function(context,args){return args.numero + args.numero;}",
			"args": ["numero"],
			"allow": {"channels": ["*"]}
		}`)
		rest.RequireStatus(t, response, http.StatusOK)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/sum?numero=13", "")
		rest.RequireStatus(t, response, http.StatusOK)
		assert.Equal(t, "26", response.BodyString())
	})
	t.Run("ReplaceOne", func(t *testing.T) {
		response := rt.SendAdminRequest("PUT", "/db/_config/functions/square", `{
			"type": "javascript",
			"code": "function(context,args){return -args.n * args.n;}",
			"args": ["n"],
			"allow": {"channels": ["*"]}
		}`)
		rest.RequireStatus(t, response, http.StatusOK)

		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["sum"])
		assert.NotNil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		rest.RequireStatus(t, response, http.StatusOK)
		assert.Equal(t, "-169", response.BodyString())
	})
	t.Run("DeleteOne", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions/square", "")
		rest.RequireStatus(t, response, http.StatusOK)

		assert.Nil(t, rt.GetDatabase().Options.UserFunctions.Definitions["square"])
		assert.Len(t, rt.GetDatabase().Options.UserFunctions.Definitions, 1)

		response = rt.SendAdminRequest("GET", "/db/_function/square?n=13", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
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
		},
	}

	rt := rest.NewRestTesterForUserQueries(t, rest.DbConfig{})
	if rt == nil {
		return
	}
	defer rt.Close()

	t.Run("check MaxRequestSize config set", func(t *testing.T) {
		testFunctionConfig.MaxRequestSize = base.IntPtr(20)

		request, err := json.Marshal(testFunctionConfig)
		assert.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
		rest.RequireStatus(t, response, http.StatusOK)

		response = rt.SendAdminRequest("GET", "/db/_config/functions", "")
		assert.NotNil(t, response)

		var responseUserFunctionsConfig functions.FunctionsConfig
		err = json.Unmarshal(response.BodyBytes(), &responseUserFunctionsConfig)
		assert.NoError(t, err)
		assert.Equal(t, testFunctionConfig.MaxRequestSize, responseUserFunctionsConfig.MaxRequestSize)
	})

	// positive cases:
	t.Run("request size less than MaxRequestSize", func(t *testing.T) {
		testFunctionConfig.MaxRequestSize = base.IntPtr(1000)

		request, err := json.Marshal(testFunctionConfig)
		assert.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
		rest.RequireStatus(t, response, http.StatusOK)

		t.Run("GET req", func(t *testing.T) {
			response := rt.SendAdminRequest("GET", "/db/_function/multiply?first=1&second=2&third=3&fourth=4", "")
			rest.RequireStatus(t, response, http.StatusOK)
			assert.Equal(t, "24", response.BodyString())
		})

		t.Run("POST req", func(t *testing.T) {
			response := rt.SendAdminRequest("POST", "/db/_function/multiply", `{"first":1,"second":2,"third":3,"fourth":4}`)
			rest.RequireStatus(t, response, http.StatusOK)
			assert.Equal(t, "24", response.BodyString())
		})
	})

	// negative cases:
	t.Run("request size greater than MaxRequestSize", func(t *testing.T) {
		testFunctionConfig.MaxRequestSize = base.IntPtr(5)

		request, err := json.Marshal(testFunctionConfig)
		assert.NoError(t, err)

		response := rt.SendAdminRequest("PUT", "/db/_config/functions", string(request))
		rest.RequireStatus(t, response, http.StatusOK)

		t.Run("GET req", func(t *testing.T) {
			response = rt.SendAdminRequest("GET", "/db/_function/multiply?first=1&second=2&third=3&fourth=4", "")
			rest.AssertStatus(t, response, http.StatusRequestEntityTooLarge)
			assert.Contains(t, string(response.BodyBytes()), "Arguments too large")
		})

		t.Run("POST req", func(t *testing.T) {
			response = rt.SendAdminRequest("POST", "/db/_function/multiply", `{"first":1,"second":2,"third":3,"fourth":4}`)
			rest.AssertStatus(t, response, http.StatusRequestEntityTooLarge)
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
		rest.RequireStatus(t, response, http.StatusOK)
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
		rest.RequireStatus(t, response, http.StatusNotFound)
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
		rest.RequireStatus(t, response, http.StatusCreated)

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
		rest.RequireStatus(t, response, http.StatusOK)
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
		rest.RequireStatus(t, response, http.StatusOK)

		functionName = "squareN1QL"

		// Change multiplication sign to Addition sign
		kUserFunctionConfigCopy.Definitions[functionName].Code = `SELECT $args.n + $args.n AS square`
		requestBody, err = json.Marshal(kUserFunctionConfigCopy.Definitions[functionName])
		assert.NoError(t, err)

		response = rt.SendAdminRequest("PUT", fmt.Sprintf("/db/_config/functions/%s", functionName), string(requestBody))
		rest.RequireStatus(t, response, http.StatusOK)
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
		rest.RequireStatus(t, response, http.StatusOK)
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
		rest.RequireStatus(t, response, http.StatusOK)
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
		rest.RequireStatus(t, response, http.StatusOK)
	})

	t.Run("Get All Non-existing Functions And Check HTTP Status", func(t *testing.T) {
		response := rt.SendAdminRequest("GET", "/db/_config/functions", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
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
		rest.RequireStatus(t, response, http.StatusNotFound)
	})

	t.Run("Delete a non-existing function and check HTTP Status Code", func(t *testing.T) {
		response := rt.SendAdminRequest("DELETE", "/db/_config/functions/foo", "")
		rest.RequireStatus(t, response, http.StatusNotFound)
	})
}
