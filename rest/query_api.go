/*
Copyright 2022-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package rest

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/graphql-go/graphql"
)

// Timeout for N1QL, JavaScript and GraphQL queries.
// TODO: Make this a configurable parameter?
const QueryTimeout = 60 * time.Second

// HTTP handler for GET or POST `/$db/_query/$name`
func (h *handler) handleUserQuery() error {
	queryName, queryParams, err := h.getUserFunctionParams()
	if err != nil {
		return err
	}
	// Run the query:
	var results sgbucket.QueryResultIterator
	err = h.db.WithTimeout(QueryTimeout, func() error {
		results, err = h.db.UserN1QLQuery(queryName, queryParams)
		return err
	})
	if err != nil {
		return err
	}

	// Write the query results to the response, as a JSON array of objects:
	h.setHeader("Content-Type", "application/json")
	_, _ = h.response.Write([]byte(`[`))
	first := true
	var row interface{}
	for results.Next(&row) {
		if first {
			first = false
		} else {
			h.response.Write([]byte(`,`))
		}
		if err := h.addJSON(row); err != nil {
			return err
		}
	}
	if err = results.Close(); err != nil {
		return err
	}
	_, _ = h.response.Write([]byte(`]` + "\n"))
	return nil
}

// HTTP handler for GET or POST `/$db/_function/$name`
func (h *handler) handleUserFunction() error {
	fnName, fnParams, err := h.getUserFunctionParams()
	if err != nil {
		return err
	}
	canMutate := h.rq.Method != "GET"
	var result interface{}
	err = h.db.WithTimeout(QueryTimeout, func() error {
		result, err = h.db.CallUserFunction(fnName, fnParams, canMutate)
		return err
	})
	if err == nil {
		h.writeJSON(result)
	}
	return err
}

// Common subroutine for reading query name and parameters from a request
func (h *handler) getUserFunctionParams() (name string, params map[string]interface{}, err error) {
	name = h.PathVar("name")
	params = map[string]interface{}{}
	if h.rq.Method == "POST" {
		// POST: Params come from the request body in JSON format:
		err = h.readJSONInto(&params)
	} else {
		// GET: Params come from the URL queries (`?key=value`):
		for key, values := range h.getQueryValues() {
			// `values` is an array of strings, one per instance of the key in the URL
			if len(values) > 1 {
				err = base.HTTPErrorf(http.StatusBadRequest, "Duplicate parameter '%s'", key)
				return
			}
			value := values[0]
			// Parse value as JSON if it looks like JSON, else just as a raw string:
			if len(value) > 0 && strings.IndexByte(`0123456789-"[{`, value[0]) >= 0 {
				var jsonValue interface{}
				if base.JSONUnmarshal([]byte(value), &jsonValue) != nil {
					err = base.HTTPErrorf(http.StatusBadRequest, "Value of ?%s is not valid JSON", key)
					return
				}
				params[key] = jsonValue
			} else {
				params[key] = value
			}
		}
	}
	return
}

// HTTP handler for GET or POST `/$db/_graphql`
// See <https://graphql.org/learn/serving-over-http/#http-methods-headers-and-body>
func (h *handler) handleGraphQL() error {
	var queryString string
	var operationName string
	var variables map[string]interface{}
	var err error
	canMutate := false

	if h.rq.Method == "POST" {
		canMutate = true
		if h.rq.Header.Get("Content-Type") == "application/graphql" {
			// POST graphql data: Request body contains the query string alone:
			query, err := h.readBody()
			if err != nil {
				return err
			}
			queryString = string(query)
		} else {
			// POST JSON: Get the "query", "operationName" and "variables" properties:
			body, err := h.readJSON()
			if err != nil {
				return err
			}
			queryString = body["query"].(string)
			operationName, _ = body["operationName"].(string)
			if variablesVal, _ := body["variables"]; variablesVal != nil {
				variables, _ = variablesVal.(map[string]interface{})
				if variables == nil {
					return base.HTTPErrorf(http.StatusBadRequest, "`variables` property must be an object")
				}
			}
		}
	} else {
		// GET: Params come from the URL queries (`?query=...&operationName=...&variables=...`):
		queryString = h.getQuery("query")
		operationName = h.getQuery("operationName")
		if varsJSON := h.getQuery("variables"); len(varsJSON) > 0 {
			if err := json.Unmarshal([]byte(varsJSON), &variables); err != nil {
				return err
			}
		}
	}

	if len(queryString) == 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing/empty `query` property")
	}

	var result *graphql.Result
	err = h.db.WithTimeout(QueryTimeout, func() error {
		result, err = h.db.UserGraphQLQuery(queryString, operationName, variables, canMutate)
		return err
	})
	if err == nil {
		h.writeJSON(result)
	}
	return err
}
