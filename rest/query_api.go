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
	"net/http"
	"strings"

	"github.com/couchbase/sync_gateway/base"
)

// HTTP handler for GET or POST `/$db/_query/$name`
func (h *handler) handleUserQuery() error {
	queryName := h.PathVar("name")

	queryParams := map[string]interface{}{}
	if h.rq.Method == "POST" {
		// POST: Params come from the request body in JSON format:
		if err := h.readJSONInto(&queryParams); err != nil {
			return err
		}
	} else {
		// GET: Params come from the URL queries (`?key=value`):
		for key, values := range h.getQueryValues() {
			// `values` is an array of strings, one per instance of the key in the URL
			if len(values) > 1 {
				return base.HTTPErrorf(http.StatusBadRequest, "Duplicate parameter '%s'", key)
			}
			value := values[0]
			// Parse value as JSON if it looks like JSON, else just as a raw string:
			if len(value) > 0 && strings.IndexByte(`0123456789-"[{`, value[0]) >= 0 {
				var jsonValue interface{}
				if err := base.JSONUnmarshal([]byte(value), &jsonValue); err != nil {
					return base.HTTPErrorf(http.StatusBadRequest, "Value of ?%s is not valid JSON", key)
				}
				queryParams[key] = jsonValue
			} else {
				queryParams[key] = value
			}
		}
	}

	// Run the query:
	results, err := h.db.UserQuery(queryName, queryParams)
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

// HTTP handler for POST `/$db/_graphql`
func (h *handler) handleGraphQL() error {
	query, err := h.readBody()
	if err != nil {
		return err
	}
	result, err := h.db.UserGraphQLQuery(string(query))
	if err == nil {
		h.writeJSON(result)
	}
	return err
}
