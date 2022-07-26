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

//////// JS FUNCTIONS:

// GET database config user functions.
func (h *handler) handleGetDbConfigFunctions() error {
	if config, etagVersion, err := h.getDBConfig(); err != nil {
		return err
	} else {
		if config.UserFunctions != nil {
			h.writeJSON(config.UserFunctions)
		} else {
			h.writeRawJSON([]byte("{}"))
		}
		h.response.Header().Set("ETag", etagVersion)
		return nil
	}
}

// GET database config, a single user function
func (h *handler) handleGetDbConfigFunction() error {
	functionName := h.PathVar("function")
	if config, etagVersion, err := h.getDBConfig(); err != nil {
		return err
	} else if functionConfig, found := config.UserFunctions[functionName]; !found {
		return base.HTTPErrorf(http.StatusNotFound, "")
	} else {
		h.writeJSON(functionConfig)
		h.response.Header().Set("ETag", etagVersion)
		return nil
	}
}

// PUT/DELETE database config user function(s)
func (h *handler) handlePutDbConfigFunctions() error {
	var functionsConfig db.UserFunctionMap
	if h.rq.Method != "DELETE" {
		if err := h.readJSONInto(&functionsConfig); err != nil {
			return err
		}
	}
	return h.mutateDbConfig(func(dbConfig *DbConfig) error {
		if functionsConfig == nil && dbConfig.UserFunctions == nil {
			return base.HTTPErrorf(http.StatusNotFound, "")
		}
		dbConfig.UserFunctions = functionsConfig
		return nil
	})
}

// PUT/DELETE database config, a single user function
func (h *handler) handlePutDbConfigFunction() error {
	functionName := h.PathVar("function")
	if h.rq.Method != "DELETE" {
		var functionConfig *db.UserFunctionConfig
		if err := h.readJSONInto(&functionConfig); err != nil {
			return err
		}
		return h.mutateDbConfig(func(dbConfig *DbConfig) error {
			if dbConfig.UserFunctions == nil {
				dbConfig.UserFunctions = db.UserFunctionMap{}
			}
			dbConfig.UserFunctions[functionName] = functionConfig
			return nil
		})
	} else {
		return h.mutateDbConfig(func(dbConfig *DbConfig) error {
			if dbConfig.UserFunctions[functionName] == nil {
				return base.HTTPErrorf(http.StatusNotFound, "")
			}
			delete(dbConfig.UserFunctions, functionName)
			return nil
		})
	}
}

//////// GRAPHQL:

// GET database GraphQL config.
func (h *handler) handleGetDbConfigGraphQL() error {
	if config, etagVersion, err := h.getDBConfig(); err != nil {
		return err
	} else if config.GraphQL == nil {
		return base.HTTPErrorf(http.StatusNotFound, "")
	} else {
		h.writeJSON(config.GraphQL)
		h.response.Header().Set("ETag", etagVersion)
		return nil
	}
}

// PUT/DELETE database GraphQL config.
func (h *handler) handlePutDbConfigGraphQL() error {
	var newConfig *db.GraphQLConfig
	if h.rq.Method != "DELETE" {
		if err := h.readJSONInto(&newConfig); err != nil {
			return err
		}
	}
	err := h.mutateDbConfig(func(dbConfig *DbConfig) error {
		if newConfig == nil && dbConfig.GraphQL == nil {
			return base.HTTPErrorf(http.StatusNotFound, "")
		}
		dbConfig.GraphQL = newConfig
		return nil
	})
	if _, ok := err.(*db.GraphQLConfigError); ok {
		return base.HTTPErrorf(http.StatusBadRequest, err.Error())
	}
	return err
}

//////// QUERIES:

// TODO: This is mostly a copy/paste of the functions code above; would be cleaner to use generics.

// GET database config user queries.
func (h *handler) handleGetDbConfigQueries() error {
	if config, etagVersion, err := h.getDBConfig(); err != nil {
		return err
	} else {
		if config.UserQueries != nil {
			h.writeJSON(config.UserQueries)
		} else {
			h.writeRawJSON([]byte("{}"))
		}
		h.response.Header().Set("ETag", etagVersion)
		return nil
	}
}

// GET database config, a single user query
func (h *handler) handleGetDbConfigQuery() error {
	queryName := h.PathVar("query")
	if config, etagVersion, err := h.getDBConfig(); err != nil {
		return err
	} else if queryConfig, found := config.UserQueries[queryName]; !found {
		return base.HTTPErrorf(http.StatusNotFound, "")
	} else {
		h.writeJSON(queryConfig)
		h.response.Header().Set("ETag", etagVersion)
		return nil
	}
}

// PUT/DELETE database config user query(s)
func (h *handler) handlePutDbConfigQueries() error {
	var queriesConfig db.UserQueryMap
	if h.rq.Method != "DELETE" {
		if err := h.readJSONInto(&queriesConfig); err != nil {
			return err
		}
	}
	return h.mutateDbConfig(func(dbConfig *DbConfig) error {
		if queriesConfig == nil && dbConfig.UserQueries == nil {
			return base.HTTPErrorf(http.StatusNotFound, "")
		}
		dbConfig.UserQueries = queriesConfig
		return nil
	})
}

// PUT/DELETE database config, a single user query
func (h *handler) handlePutDbConfigQuery() error {
	queryName := h.PathVar("query")
	if h.rq.Method != "DELETE" {
		var queryConfig *db.UserQueryConfig
		if err := h.readJSONInto(&queryConfig); err != nil {
			return err
		}
		return h.mutateDbConfig(func(dbConfig *DbConfig) error {
			if dbConfig.UserQueries == nil {
				dbConfig.UserQueries = db.UserQueryMap{}
			}
			dbConfig.UserQueries[queryName] = queryConfig
			return nil
		})
	} else {
		return h.mutateDbConfig(func(dbConfig *DbConfig) error {
			if dbConfig.UserQueries[queryName] == nil {
				return base.HTTPErrorf(http.StatusNotFound, "")
			}
			delete(dbConfig.UserQueries, queryName)
			return nil
		})
	}
}
