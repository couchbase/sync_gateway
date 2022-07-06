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

func (h *handler) getDBConfig() (config *DbConfig, etagVersion string, err error) {
	h.assertAdminOnly()
	if h.server.bootstrapContext.connection == nil {
		return h.server.GetDbConfig(h.db.Name), "", nil
	} else if found, databaseConfig, err := h.server.fetchDatabase(h.db.Name); err != nil {
		return nil, "", err
	} else if !found {
		return nil, "", base.HTTPErrorf(http.StatusNotFound, "database config not found")
	} else {
		return &databaseConfig.DbConfig, databaseConfig.Version, nil
	}
}

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

// PUT database config user queries (or a single query)
func (h *handler) handlePutDbConfigQueries() error {
	h.assertAdminOnly()

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

	bucket := h.db.Bucket.GetName()
	var updatedDbConfig *DatabaseConfig
	cas, err := h.server.bootstrapContext.connection.UpdateConfig(
		bucket, h.server.config.Bootstrap.ConfigGroupID,
		func(rawBucketConfig []byte) (newConfig []byte, err error) {
			var bucketDbConfig DatabaseConfig
			if err := base.JSONUnmarshal(rawBucketConfig, &bucketDbConfig); err != nil {
				return nil, err
			}

			headerVersion := h.rq.Header.Get("If-Match")
			if headerVersion != "" && headerVersion != bucketDbConfig.Version {
				return nil, base.HTTPErrorf(http.StatusPreconditionFailed, "Provided If-Match header does not match current config version")
			}

			// Update the config:
			if queryName == "" {
				bucketDbConfig.UserQueries = queriesConfig
			} else {
				if bucketDbConfig.UserQueries == nil {
					bucketDbConfig.UserQueries = db.UserQueryMap{}
				}
				bucketDbConfig.UserQueries[queryName] = &queryConfig
			}

			if err := bucketDbConfig.validate(h.ctx(), !h.getBoolQuery(paramDisableOIDCValidation)); err != nil {
				return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
			}

			bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(bucketDbConfig.Version, &bucketDbConfig.DbConfig)
			if err != nil {
				return nil, err
			}

			updatedDbConfig = &bucketDbConfig
			return base.JSONMarshal(bucketDbConfig)
		})
	if err != nil {
		return err
	}
	updatedDbConfig.cas = cas

	dbName := h.db.Name
	dbCreds := h.server.config.DatabaseCredentials[dbName]
	if err := updatedDbConfig.setup(dbName, h.server.config.Bootstrap, dbCreds); err != nil {
		return err
	}

	h.server.lock.Lock()
	defer h.server.lock.Unlock()

	// TODO: Dynamic update instead of reload
	if err := h.server._reloadDatabaseWithConfig(*updatedDbConfig, false); err != nil {
		return err
	}

	return base.HTTPErrorf(http.StatusOK, "updated")
}
