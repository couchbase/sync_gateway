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
	"context"
	"encoding/json"
	"net/http"
	"strings"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

const kFnNameParam = "name"
const kGraphQLQueryParam = "query"
const kGraphQLOperationNameParam = "operationName"
const kGraphQLVariablesParam = "variables"

//////// FUNCTIONS:

// HTTP handler for GET or POST `/$db/_function/$name`
func (h *handler) handleFunctionCall() error {
	var maxRequestSize *int
	if h.db.UserFunctions != nil {
		maxRequestSize = h.db.UserFunctions.MaxRequestSize
	}
	fnName, fnParams, err := h.getFunctionArgs(maxRequestSize)
	if err != nil {
		return err
	}
	canMutate := h.rq.Method != "GET"

	return db.WithTimeout(h.ctx(), db.UserFunctionTimeout, func(ctx context.Context) error {
		fn, err := h.db.GetUserFunction(fnName, fnParams, canMutate, ctx)
		if err != nil {
			return err
		} else if rows, err := fn.Iterate(); err != nil {
			return err
		} else if rows != nil {
			return h.writeQueryRows(rows)
		} else {
			// Write the single result to the response:
			result, err := fn.Run()
			if err == nil {
				h.writeJSON(result)
			}
			return err
		}
	})
}

// Subroutine for reading function name and arguments from a request
func (h *handler) getFunctionArgs(maxSize *int) (string, map[string]interface{}, error) {
	name := h.PathVar(kFnNameParam)
	args := map[string]interface{}{}
	var err error
	if h.rq.Method == "POST" {
		// POST: Args come from the request body in JSON format:
		input, err := processContentEncoding(h.rq.Header, h.requestBody, "application/json")
		if err != nil {
			return "", nil, err
		}
		if h.rq.ContentLength >= 0 {
			if err := db.CheckRequestSize(h.rq.ContentLength, maxSize); err != nil {
				return "", nil, err
			}
		}
		// Decode the body bytes into target structure.
		decoder := json.NewDecoder(input)
		err = decoder.Decode(&args)
		_ = input.Close()
		if err == nil {
			err = db.CheckRequestSize(decoder.InputOffset(), maxSize)
		}
	} else {
		// GET: Params come from the URL queries (`?key=value`):
		if err := db.CheckRequestSize(int64(len(h.rq.URL.RawQuery)), maxSize); err != nil {
			return "", nil, err
		}
		for key, values := range h.getQueryValues() {
			// `values` is an array of strings, one per instance of the key in the URL
			if len(values) > 1 {
				return "", nil, base.HTTPErrorf(http.StatusBadRequest, "Duplicate parameter '%s'", key)
			}
			value := values[0]
			// Parse value as JSON if it looks like JSON, else just as a raw string:
			if len(value) > 0 && strings.IndexByte(`0123456789-"[{`, value[0]) >= 0 {
				var jsonValue interface{}
				if base.JSONUnmarshal([]byte(value), &jsonValue) != nil {
					return "", nil, base.HTTPErrorf(http.StatusBadRequest, "Value of ?%s is not valid JSON", key)
				}
				args[key] = jsonValue
			} else {
				args[key] = value
			}
		}
	}
	return name, args, err
}

// Subroutine to write N1QL query results to a response
func (h *handler) writeQueryRows(rows sgbucket.QueryResultIterator) error {
	// Use iterator to write results one at a time to the response:
	defer func() {
		if rows != nil {
			_ = rows.Close()
		}
	}()

	// Write the query results to the response, as a JSON array of objects.
	h.setHeader("Content-Type", "application/json")
	var err error
	if _, err = h.response.Write([]byte(`[`)); err != nil {
		return err
	}
	first := true
	var row interface{}
	for rows.Next(&row) {
		if first {
			first = false
		} else {
			if _, err = h.response.Write([]byte(`,`)); err != nil {
				return err
			}
		}
		if err = h.addJSON(row); err != nil {
			return err
		}
		// The iterator streams results as the query engine produces them, so this loop may take most of the query's time; check for timeout after each iteration:
		if err = db.CheckTimeout(h.ctx()); err != nil {
			return err
		}
	}
	err = rows.Close()
	rows = nil // prevent 'defer' from closing again
	if err != nil {
		return err
	}
	_, err = h.response.Write([]byte("]\n"))
	return err
}

//////// GRAPHQL QUERIES:

// HTTP handler for GET or POST `/$db/_graphql`
// See <https://graphql.org/learn/serving-over-http/#http-methods-headers-and-body>
func (h *handler) handleGraphQL() error {
	var queryString string
	var operationName string
	var variables map[string]interface{}
	canMutate := false

	if h.db.GraphQL == nil {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "GraphQL is not configured")
	}
	maxSize := h.db.GraphQL.MaxRequestSize()

	if h.rq.Method == "POST" {
		if h.rq.ContentLength >= 0 {
			if err := db.CheckRequestSize(h.rq.ContentLength, maxSize); err != nil {
				return err
			}
		}
		canMutate = true
		if h.rq.Header.Get("Content-Type") == "application/graphql" {
			// POST graphql data: Request body contains the query string alone:
			query, err := h.readBody()
			if err != nil {
				return err
			}
			if err := db.CheckRequestSize(len(query), maxSize); err != nil {
				return err
			}
			queryString = string(query)
		} else {
			// POST JSON: Get the "query", "operationName" and "variables" properties.
			// go-graphql does not like the Number type, so decode leaving numbers as float64:
			body, err := h.readJSONWithoutNumber()
			if err != nil {
				return err
			}
			if h.rq.ContentLength < 0 {
				if err := db.CheckRequestSize(db.EstimateSizeOfJSON(body), maxSize); err != nil {
					return err
				}
			}
			queryString = body[kGraphQLQueryParam].(string)
			operationName, _ = body[kGraphQLOperationNameParam].(string)
			if variablesVal := body[kGraphQLVariablesParam]; variablesVal != nil {
				variables, _ = variablesVal.(map[string]interface{})
				if variables == nil {
					return base.HTTPErrorf(http.StatusBadRequest, "`variables` property must be an object")
				}
			}
		}
	} else {
		// GET: Params come from the URL queries (`?query=...&operationName=...&variables=...`):
		if err := db.CheckRequestSize(h.rq.ContentLength, maxSize); err != nil {
			return err
		}
		queryString = h.getQuery(kGraphQLQueryParam)
		operationName = h.getQuery(kGraphQLOperationNameParam)
		if varsJSON := h.getQuery(kGraphQLVariablesParam); len(varsJSON) > 0 {
			if err := json.Unmarshal([]byte(varsJSON), &variables); err != nil {
				return err
			}
		}
	}

	if len(queryString) == 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Missing/empty `query` property")
	}

	return db.WithTimeout(h.ctx(), db.UserFunctionTimeout, func(ctx context.Context) error {
		result, err := h.db.UserGraphQLQuery(queryString, operationName, variables, canMutate, ctx)
		if err == nil {
			h.writeJSON(result)
		}
		return err
	})
}
