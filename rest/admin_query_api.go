//  Copyright 2022-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"net/http"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// GET database config user queries (or a single query)
func (h *handler) handleGetDbConfigQueries() error {
	config, etagVersion, err := h.getDBConfig()
	if err != nil {
		return err
	} else if config.UserQueries == nil {
		return base.HTTPErrorf(http.StatusNotFound, "no queries configured")
	} else if queryName := h.PathVar("query"); queryName == "" {
		h.writeJSON(config.UserQueries)
	} else if queryConfig, found := config.UserQueries[queryName]; found {
		h.writeJSON(queryConfig)
	} else {
		return base.HTTPErrorf(http.StatusNotFound, "no such query")
	}
	h.response.Header().Set("ETag", etagVersion)
	return nil
}

// GET database config user functions (or a single function)
func (h *handler) handleGetDbConfigFunctions() error {
	config, etagVersion, err := h.getDBConfig()
	if err != nil {
		return err
	} else if config.UserFunctions == nil {
		return base.HTTPErrorf(http.StatusNotFound, "no functions configured")
	} else if functionName := h.PathVar("function"); functionName == "" {
		h.writeJSON(config.UserFunctions)
	} else if functionConfig, found := config.UserFunctions[functionName]; found {
		h.writeJSON(functionConfig)
	} else {
		return base.HTTPErrorf(http.StatusNotFound, "no such function")
	}
	h.response.Header().Set("ETag", etagVersion)
	return nil
}

// PUT database config user queries (or a single query)
func (h *handler) handlePutDbConfigQueries() error {
	// Read the new config, either the entire UserQueryMap, or a single UserQuery:
	var queriesConfig db.UserQueryMap
	var queryConfig db.UserQuery
	var err error
	queryName := h.PathVar("query")
	if queryName == "" {
		err = h.readJSONInto(&queriesConfig)
	} else {
		err = h.readJSONInto(&queryConfig)
	}
	if err != nil {
		return err
	}
	return h.mutateDbConfig(func(dbConfig *DbConfig) {
		if queryName == "" {
			dbConfig.UserQueries = queriesConfig
		} else {
			if dbConfig.UserQueries == nil {
				dbConfig.UserQueries = db.UserQueryMap{}
			}
			dbConfig.UserQueries[queryName] = &queryConfig
		}
	})
}

// PUT database config user functions (or a single function)
func (h *handler) handlePutDbConfigFunctions() error {
	// Read the new config, either the entire UserFunctionMap, or a single UserFunction:
	var functionsConfig db.UserFunctionMap
	var functionConfig db.UserFunctionConfig
	var err error
	functionName := h.PathVar("function")
	if functionName == "" {
		err = h.readJSONInto(&functionsConfig)
	} else {
		err = h.readJSONInto(&functionConfig)
	}
	if err != nil {
		return err
	}
	return h.mutateDbConfig(func(dbConfig *DbConfig) {
		if functionName == "" {
			dbConfig.UserFunctions = functionsConfig
		} else {
			if dbConfig.UserFunctions == nil {
				dbConfig.UserFunctions = db.UserFunctionMap{}
			}
			dbConfig.UserFunctions[functionName] = &functionConfig
		}
	})
}
