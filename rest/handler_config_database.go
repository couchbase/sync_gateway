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
)

// Returns the current database config object for this request
func (h *handler) getDBConfig() (config *DbConfig, etagVersion string, err error) {
	h.assertAdminOnly()
	if h.server.BootstrapContext.Connection == nil {
		if dbConfig := h.server.GetDatabaseConfig(h.db.Name); dbConfig != nil {
			etagVersion = dbConfig.Version
			if etagVersion == "" {
				etagVersion = "0-"
			}
			return &dbConfig.DbConfig, etagVersion, nil
		}
		return nil, "", nil
	} else if found, databaseConfig, err := h.server.fetchDatabase(h.ctx(), h.db.Name); err != nil {
		return nil, "", err
	} else if !found {
		return nil, "", base.HTTPErrorf(http.StatusNotFound, "database config not found")
	} else {
		return &databaseConfig.DbConfig, databaseConfig.Version, nil
	}
}

// Updates the database config via a callback function that can modify a `DbConfig`.
// Note: This always returns a non-nil error; on success it's an HTTPError with status OK.
// The calling handler method is expected to simply return the result.
func (h *handler) mutateDbConfig(mutator func(*DbConfig) error) error {
	h.assertAdminOnly()
	dbName := h.db.Name
	validateOIDC := !h.getBoolQuery(paramDisableOIDCValidation)

	if h.server.persistentConfig {
		// Update persistently-stored config:
		bucket := h.db.Bucket.GetName()
		var updatedDbConfig *DatabaseConfig
		cas, err := h.server.BootstrapContext.UpdateConfig(h.ctx(), bucket, h.server.Config.Bootstrap.ConfigGroupID, dbName, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {

			if h.headerDoesNotMatchEtag(bucketDbConfig.Version) {
				return nil, base.HTTPErrorf(http.StatusPreconditionFailed, "Provided If-Match header does not match current config version")
			}

			// Now call the mutator function:
			if err := mutator(&bucketDbConfig.DbConfig); err != nil {
				return nil, err
			}

			if err := bucketDbConfig.validate(h.ctx(), validateOIDC); err != nil {
				return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
			}

			bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(bucketDbConfig.Version, &bucketDbConfig.DbConfig)
			if err != nil {
				return nil, err
			}
			updatedDbConfig = bucketDbConfig
			return bucketDbConfig, nil
		})
		if err != nil {
			return err
		}
		updatedDbConfig.cfgCas = cas

		dbCreds := h.server.Config.DatabaseCredentials[dbName]
		bucketCreds := h.server.Config.BucketCredentials[bucket]
		if err := updatedDbConfig.setup(dbName, h.server.Config.Bootstrap, dbCreds, bucketCreds, h.server.Config.IsServerless()); err != nil {
			return err
		}

		h.server.lock.Lock()
		defer h.server.lock.Unlock()

		// TODO: Dynamic update instead of reload
		if err := h.server._reloadDatabaseWithConfig(h.ctx(), *updatedDbConfig, false, false); err != nil {
			return err
		}
		h.setEtag(updatedDbConfig.Version)
		return base.HTTPErrorf(http.StatusOK, "updated")

	} else {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Unavailable")
	}
}
