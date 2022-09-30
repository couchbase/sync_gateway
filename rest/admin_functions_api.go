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
	"github.com/couchbase/sync_gateway/db/functions"
)

//////// JS FUNCTIONS:

// GET config: user functions.
func (h *handler) handleGetDbConfigFunctions() error {
	if config, etagVersion, err := h.getDBConfig(); err != nil {
		return err
	} else if config.UserFunctions == nil {
		return base.HTTPErrorf(http.StatusNotFound, "")
	} else {
		h.writeJSON(config.UserFunctions)
		h.setEtag(etagVersion)
		return nil
	}
}

// GET config: a single user function
func (h *handler) handleGetDbConfigFunction() error {
	functionName := h.PathVar("function")
	if config, etagVersion, err := h.getDBConfig(); err != nil {
		return err
	} else if functionConfig, found := config.UserFunctions[functionName]; !found {
		return base.HTTPErrorf(http.StatusNotFound, "")
	} else {
		h.writeJSON(functionConfig)
		h.setEtag(etagVersion)
		return nil
	}
}

// PUT/DELETE config: user function(s)
func (h *handler) handlePutDbConfigFunctions() error {
	var functionsConfig functions.FunctionConfigMap
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

// PUT/DELETE config: a single user function
func (h *handler) handlePutDbConfigFunction() error {
	functionName := h.PathVar("function")
	if h.rq.Method != "DELETE" {
		var functionConfig *functions.FunctionConfig
		if err := h.readJSONInto(&functionConfig); err != nil {
			return err
		}
		return h.mutateDbConfig(func(dbConfig *DbConfig) error {
			if dbConfig.UserFunctions == nil {
				dbConfig.UserFunctions = functions.FunctionConfigMap{}
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
		h.setEtag(etagVersion)
		return nil
	}
}

// PUT/DELETE database GraphQL config.
func (h *handler) handlePutDbConfigGraphQL() error {
	var newConfig *functions.GraphQLConfig
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
	return err
}
