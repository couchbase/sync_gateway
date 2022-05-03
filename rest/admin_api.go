//  Copyright 2012-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/google/uuid"
	"github.com/gorilla/mux"
	pkgerrors "github.com/pkg/errors"
)

const kDefaultDBOnlineDelay = 0

const paramDisableOIDCValidation = "disable_oidc_validation"

// GetUsers  - GET /{db}/_user/
const paramNameOnly = "name_only"
const paramLimit = "limit"

// ////// DATABASE MAINTENANCE:

// "Create" a database (actually just register an existing bucket)
func (h *handler) handleCreateDB() error {
	h.assertAdminOnly()
	dbName := h.PathVar("newdb")
	config, err := h.readSanitizeDbConfigJSON()
	if err != nil {
		return err
	}
	config.Name = dbName

	validateOIDC := !h.getBoolQuery(paramDisableOIDCValidation)

	if h.server.persistentConfig {
		if err := config.validatePersistentDbConfig(); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		if err := config.validate(h.ctx(), validateOIDC); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		version, err := GenerateDatabaseConfigVersionID("", config)
		if err != nil {
			return err
		}

		bucket := dbName
		if config.Bucket != nil {
			bucket = *config.Bucket
		}

		// copy config before setup to persist the raw config the user supplied
		var persistedDbConfig DbConfig
		if err := base.DeepCopyInefficient(&persistedDbConfig, config); err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't create copy of db config: %v", err)
		}

		dbCreds, _ := h.server.config.DatabaseCredentials[dbName]
		if err := config.setup(dbName, h.server.config.Bootstrap, dbCreds); err != nil {
			return err
		}

		loadedConfig := DatabaseConfig{Version: version, DbConfig: *config}
		persistedConfig := DatabaseConfig{Version: version, DbConfig: persistedDbConfig}

		h.server.lock.Lock()
		defer h.server.lock.Unlock()

		if _, exists := h.server.databases_[dbName]; exists {
			return base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
				"Duplicate database name %q", dbName)
		}

		_, err = h.server._applyConfig(loadedConfig, true)
		if err != nil {
			if errors.Is(err, base.ErrAuthError) {
				return base.HTTPErrorf(http.StatusForbidden, "auth failure accessing provided bucket: %s", bucket)
			} else if errors.Is(err, base.ErrAlreadyExists) {
				return base.HTTPErrorf(http.StatusConflict, "couldn't load database: %s", err)
			}
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't load database: %v", err)
		}

		// now we've started the db successfully, we can persist it to the cluster
		cas, err := h.server.bootstrapContext.connection.InsertConfig(bucket, h.server.config.Bootstrap.ConfigGroupID, persistedConfig)
		if err != nil {
			// unload the requested database config to prevent the cluster being in an inconsistent state
			h.server._removeDatabase(dbName)
			if errors.Is(err, base.ErrAuthError) {
				return base.HTTPErrorf(http.StatusForbidden, "auth failure accessing provided bucket using bootstrap credentials: %s", bucket)
			} else if errors.Is(err, base.ErrAlreadyExists) {
				// on-demand config load if someone else beat us to db creation
				if _, err := h.server._fetchAndLoadDatabase(dbName); err != nil {
					base.WarnfCtx(h.rq.Context(), "Couldn't load database after conflicting create: %v", err)
				}
				return base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
					"Duplicate database name %q", dbName)
			}
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't save database config: %v", err)
		}
		// store the cas in the loaded config after a successful insert
		h.server.dbConfigs[dbName].cas = cas
	} else {
		// Intentionally pass in an empty BootstrapConfig to avoid inheriting any credentials or server when running with a legacy config (CBG-1764)
		if err := config.setup(dbName, BootstrapConfig{}, nil); err != nil {
			return err
		}

		// load database in-memory for non-persistent nodes
		if _, err := h.server.AddDatabaseFromConfigFailFast(DatabaseConfig{DbConfig: *config}); err != nil {
			if errors.Is(err, base.ErrAuthError) {
				return base.HTTPErrorf(http.StatusForbidden, "auth failure using provided bucket credentials for database %s", base.MD(config.Name))
			}
			return err
		}
	}

	return base.HTTPErrorf(http.StatusCreated, "created")
}

// getAuthScopeHandleCreateDB is used in the router to supply an auth scope for the admin api auth. Takes the JSON body
// from the payload, pulls out bucket and returns this as the auth scope.
func getAuthScopeHandleCreateDB(bodyJSON []byte) (string, error) {
	var body struct {
		Bucket string `json:"bucket"`
	}
	err := base.JSONUnmarshal(bodyJSON, &body)
	if err != nil {
		return "", err
	}

	if body.Bucket == "" {
		return "", nil
	}

	return body.Bucket, nil
}

// Take a DB online, first reload the DB config
func (h *handler) handleDbOnline() error {
	h.assertAdminOnly()
	dbState := atomic.LoadUint32(&h.db.State)
	// If the DB is already transitioning to: online or is online silently return
	if dbState == db.DBOnline || dbState == db.DBStarting {
		return nil
	}

	// If the DB is currently re-syncing return an error asking the user to retry later
	if dbState == db.DBResyncing {
		return base.HTTPErrorf(http.StatusServiceUnavailable, "Database _resync is in progress, this may take some time, try again later")
	}

	body, err := h.readBody()
	if err != nil {
		return err
	}

	var input struct {
		Delay int `json:"delay"`
	}

	input.Delay = kDefaultDBOnlineDelay

	_ = base.JSONUnmarshal(body, &input)

	base.InfofCtx(h.ctx(), base.KeyCRUD, "Taking Database : %v, online in %v seconds", base.MD(h.db.Name), input.Delay)
	go func() {
		time.Sleep(time.Duration(input.Delay) * time.Second)
		h.server.TakeDbOnline(h.db.DatabaseContext)
	}()

	return nil
}

// Take a DB offline
func (h *handler) handleDbOffline() error {
	h.assertAdminOnly()
	var err error
	if err = h.db.TakeDbOffline("ADMIN Request"); err != nil {
		base.InfofCtx(h.ctx(), base.KeyCRUD, "Unable to take Database : %v, offline", base.MD(h.db.Name))
	}

	return err
}

// Get admin database info
func (h *handler) handleGetDbConfig() error {
	if redact, _ := h.getOptBoolQuery("redact", true); !redact {
		return base.HTTPErrorf(http.StatusBadRequest, "redact=false is no longer supported")
	}

	// load config from bucket once for:
	// - Populate an up to date ETag header
	// - Applying if refresh_config is set
	// - Returning if include_runtime=false
	var responseConfig *DbConfig
	if h.server.bootstrapContext.connection != nil {
		found, dbConfig, err := h.server.fetchDatabase(h.db.Name)
		if err != nil {
			return err
		}

		if !found || dbConfig == nil {
			return base.HTTPErrorf(http.StatusNotFound, "database config not found")
		}

		h.response.Header().Set("ETag", dbConfig.Version)

		// refresh_config=true forces the config loaded out of the bucket to be applied on the node
		if h.getBoolQuery("refresh_config") && h.server.bootstrapContext.connection != nil {
			// set cas=0 to force a refresh
			dbConfig.cas = 0
			h.server.applyConfigs(map[string]DatabaseConfig{h.db.Name: *dbConfig})
		}

		responseConfig = &dbConfig.DbConfig

		// Strip out bootstrap credentials that are stamped into the config
		responseConfig.Username = ""
		responseConfig.Password = ""
		responseConfig.CACertPath = ""
		responseConfig.KeyPath = ""
		responseConfig.CertPath = ""
	} else {
		// non-persistent mode just returns running database config
		responseConfig = h.server.GetDbConfig(h.db.Name)
	}

	// include_runtime controls whether to return the raw bucketDbConfig, or the runtime version populated with default values, etc.
	includeRuntime, _ := h.getOptBoolQuery("include_runtime", false)
	if includeRuntime {

		var err error
		responseConfig, err = MergeDatabaseConfigWithDefaults(h.server.config, h.server.GetDbConfig(h.db.Name))
		if err != nil {
			return err
		}
	}

	// defensive check - there could've been an in-flight request to remove the database between entering the handler and getting the config above.
	if responseConfig == nil {
		return base.HTTPErrorf(http.StatusNotFound, "database config not found")
	}

	var err error
	responseConfig, err = responseConfig.Redacted()
	if err != nil {
		return err
	}

	// include_javascript=false omits config fields that contain javascript code
	includeJavascript, _ := h.getOptBoolQuery("include_javascript", true)
	if !includeJavascript {
		responseConfig.Sync = nil
		responseConfig.ImportFilter = nil
		if responseConfig.EventHandlers != nil {
			for _, evt := range responseConfig.EventHandlers.DocumentChanged {
				evt.Filter = ""
			}
			for _, evt := range responseConfig.EventHandlers.DBStateChanged {
				evt.Filter = ""
			}
		}
	}

	h.writeJSON(responseConfig)
	return nil
}

type RunTimeServerConfigResponse struct {
	*StartupConfig
	Databases map[string]*DbConfig `json:"databases"`
}

// Get admin config info
func (h *handler) handleGetConfig() error {
	if redact, _ := h.getOptBoolQuery("redact", true); !redact {
		return base.HTTPErrorf(http.StatusBadRequest, "redact=false is no longer supported")
	}

	includeRuntime, _ := h.getOptBoolQuery("include_runtime", false)
	if includeRuntime {
		cfg := RunTimeServerConfigResponse{}
		var err error

		allDbNames := h.server.AllDatabaseNames()
		databaseMap := make(map[string]*DbConfig, len(allDbNames))
		cfg.StartupConfig, err = h.server.config.Redacted()
		if err != nil {
			return err
		}

		for _, dbName := range allDbNames {
			// defensive check - in-flight requests could've removed this database since we got the name of it
			dbConfig := h.server.GetDbConfig(dbName)
			if dbConfig == nil {
				continue
			}

			dbConfig, err := MergeDatabaseConfigWithDefaults(h.server.config, dbConfig)
			if err != nil {
				return err
			}

			databaseMap[dbName], err = dbConfig.Redacted()
			if err != nil {
				return err
			}
		}

		for dbName, dbConfig := range databaseMap {
			database, err := h.server.GetDatabase(dbName)
			if err != nil {
				return err
			}

			replications, err := database.SGReplicateMgr.GetReplications()
			if err != nil {
				return err
			}

			dbConfig.Replications = make(map[string]*db.ReplicationConfig, len(replications))

			for replicationName, replicationConfig := range replications {
				dbConfig.Replications[replicationName] = replicationConfig.ReplicationConfig.Redacted()
			}
		}

		cfg.Logging = *base.BuildLoggingConfigFromLoggers(h.server.config.Logging.RedactionLevel, h.server.config.Logging.LogFilePath)
		cfg.Databases = databaseMap

		h.writeJSON(cfg)
	} else {
		cfg, err := h.server.initialStartupConfig.Redacted()
		if err != nil {
			return err
		}
		h.writeJSON(cfg)
	}

	return nil
}

func (h *handler) handlePutConfig() error {

	type FileLoggerPutConfig struct {
		Enabled *bool `json:"enabled,omitempty"`
	}

	type ConsoleLoggerPutConfig struct {
		LogLevel *base.LogLevel `json:"log_level,omitempty"`
		LogKeys  []string       `json:"log_keys,omitempty"`
	}

	// Probably need to make our own to remove log file path / redaction level
	type ServerPutConfig struct {
		Logging struct {
			Console *ConsoleLoggerPutConfig `json:"console,omitempty"`
			Error   FileLoggerPutConfig     `json:"error,omitempty"`
			Warn    FileLoggerPutConfig     `json:"warn,omitempty"`
			Info    FileLoggerPutConfig     `json:"info,omitempty"`
			Debug   FileLoggerPutConfig     `json:"debug,omitempty"`
			Trace   FileLoggerPutConfig     `json:"trace,omitempty"`
			Stats   FileLoggerPutConfig     `json:"stats,omitempty"`
		} `json:"logging"`
	}

	var config ServerPutConfig
	err := base.WrapJSONUnknownFieldErr(ReadJSONFromMIMERawErr(h.rq.Header, h.requestBody, &config))
	if err != nil {
		if pkgerrors.Cause(err) == base.ErrUnknownField {
			return base.HTTPErrorf(http.StatusBadRequest, "Unable to configure given options at runtime: %v", err)
		}
		return err
	}

	// Go over all loggers and use
	if config.Logging.Console != nil {
		if config.Logging.Console.LogLevel != nil {
			base.ConsoleLogLevel().Set(*config.Logging.Console.LogLevel)
		}

		if config.Logging.Console.LogKeys != nil {
			testMap := make(map[string]bool)
			for _, key := range config.Logging.Console.LogKeys {
				testMap[key] = true
			}

			base.UpdateLogKeys(testMap, true)
		}
	}

	if config.Logging.Error.Enabled != nil {
		base.EnableErrorLogger(*config.Logging.Error.Enabled)
	}

	if config.Logging.Warn.Enabled != nil {
		base.EnableWarnLogger(*config.Logging.Warn.Enabled)
	}

	if config.Logging.Info.Enabled != nil {
		base.EnableInfoLogger(*config.Logging.Info.Enabled)
	}

	if config.Logging.Debug.Enabled != nil {
		base.EnableDebugLogger(*config.Logging.Debug.Enabled)
	}

	if config.Logging.Trace.Enabled != nil {
		base.EnableTraceLogger(*config.Logging.Trace.Enabled)
	}

	if config.Logging.Stats.Enabled != nil {
		base.EnableStatsLogger(*config.Logging.Stats.Enabled)
	}

	return base.HTTPErrorf(http.StatusOK, "Updated")
}

// handlePutDbConfig Upserts a new database config
func (h *handler) handlePutDbConfig() (err error) {
	h.assertAdminOnly()

	var dbConfig *DbConfig

	if h.permissionsResults[PermUpdateDb.PermissionName] {
		// user authorized to change all fields
		dbConfig, err = h.readSanitizeDbConfigJSON()
		if err != nil {
			return err
		}
	} else {
		hasAuthPerm := h.permissionsResults[PermConfigureAuth.PermissionName]
		hasSyncPerm := h.permissionsResults[PermConfigureSyncFn.PermissionName]

		bodyContents, err := ioutil.ReadAll(h.requestBody)
		if err != nil {
			return err
		}

		var mapDbConfig map[string]interface{}
		err = ReadJSONFromMIMERawErr(h.rq.Header, ioutil.NopCloser(bytes.NewReader(bodyContents)), &mapDbConfig)
		if err != nil {
			return err
		}

		unknownFileKeys := make([]string, 0)
		for key, _ := range mapDbConfig {
			if key == "sync" && hasSyncPerm || key == "guest" && hasAuthPerm {
				continue
			}
			unknownFileKeys = append(unknownFileKeys, key)
		}

		if len(unknownFileKeys) > 0 {
			return base.HTTPErrorf(http.StatusForbidden, "not authorized to update field: %s", strings.Join(unknownFileKeys, ","))
		}

		err = ReadJSONFromMIMERawErr(h.rq.Header, ioutil.NopCloser(bytes.NewReader(bodyContents)), &dbConfig)
		if err != nil {
			return err
		}
	}

	bucket := h.db.Bucket.GetName()
	if dbConfig.Bucket != nil {
		bucket = *dbConfig.Bucket
	}

	// Set dbName based on path value (since db doesn't necessarily exist), and update in incoming config in case of insert
	dbName := h.PathVar("db")
	if dbConfig.Name != "" && dbName != dbConfig.Name {
		return base.HTTPErrorf(http.StatusBadRequest, "Cannot update database name. "+
			"This requires removing and re-creating the database with a new name")
	}

	if dbConfig.Name == "" {
		dbConfig.Name = dbName
	}

	validateOIDC := !h.getBoolQuery(paramDisableOIDCValidation)

	if !h.server.persistentConfig {
		updatedDbConfig := &DatabaseConfig{DbConfig: *dbConfig}
		err = updatedDbConfig.validate(h.ctx(), validateOIDC)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		dbCreds, _ := h.server.config.DatabaseCredentials[dbName]
		if err := updatedDbConfig.setup(dbName, h.server.config.Bootstrap, dbCreds); err != nil {
			return err
		}
		if err := h.server.ReloadDatabaseWithConfig(*updatedDbConfig); err != nil {
			return err
		}
		return base.HTTPErrorf(http.StatusCreated, "updated")
	}

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

			if h.rq.Method == http.MethodPost {
				base.TracefCtx(h.rq.Context(), base.KeyConfig, "merging upserted config into bucket config")
				if err := base.ConfigMerge(&bucketDbConfig.DbConfig, dbConfig); err != nil {
					return nil, err
				}
			} else {
				base.TracefCtx(h.rq.Context(), base.KeyConfig, "using config as-is without merge")
				bucketDbConfig.DbConfig = *dbConfig
			}

			if err := dbConfig.validatePersistentDbConfig(); err != nil {
				return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
			}
			if err := bucketDbConfig.validate(h.ctx(), validateOIDC); err != nil {
				return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
			}

			bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(bucketDbConfig.Version, &bucketDbConfig.DbConfig)
			if err != nil {
				return nil, err
			}

			updatedDbConfig = &bucketDbConfig

			// take a copy to stamp credentials and load before we persist
			var tmpConfig DatabaseConfig
			if err = base.DeepCopyInefficient(&tmpConfig, bucketDbConfig); err != nil {
				return nil, err
			}
			dbCreds, _ := h.server.config.DatabaseCredentials[dbName]
			if err := tmpConfig.setup(dbName, h.server.config.Bootstrap, dbCreds); err != nil {
				return nil, err
			}

			// Load the new dbConfig before we persist the update.
			err = h.server.ReloadDatabaseWithConfig(tmpConfig)
			if err != nil {
				return nil, err
			}

			return base.JSONMarshal(bucketDbConfig)
		})
	if err != nil {
		base.WarnfCtx(h.rq.Context(), "Couldn't update config for database - rolling back: %v", err)
		// failed to start the new database config - rollback and return the original error for the user
		if _, err := h.server.fetchAndLoadDatabase(dbName); err != nil {
			base.WarnfCtx(h.rq.Context(), "got error rolling back database %q after failed update: %v", base.UD(dbName), err)
		}
		return err
	}
	// store the cas in the loaded config after a successful update
	h.server.dbConfigs[dbName].cas = cas
	h.response.Header().Set("ETag", updatedDbConfig.Version)
	return base.HTTPErrorf(http.StatusCreated, "updated")

}

// GET database config sync function
func (h *handler) handleGetDbConfigSync() error {
	h.assertAdminOnly()
	var (
		etagVersion  string
		syncFunction string
	)

	if h.server.bootstrapContext.connection != nil {
		found, dbConfig, err := h.server.fetchDatabase(h.db.Name)
		if err != nil {
			return err
		}

		if !found {
			return base.HTTPErrorf(http.StatusNotFound, "database config not found")
		}

		etagVersion = dbConfig.Version
		if dbConfig.Sync != nil {
			syncFunction = *dbConfig.Sync
		}
	}

	h.response.Header().Set("ETag", etagVersion)
	h.writeJavascript(syncFunction)
	return nil
}

// DELETE a database config sync function
func (h *handler) handleDeleteDbConfigSync() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
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

			bucketDbConfig.Sync = nil
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
	dbCreds, _ := h.server.config.DatabaseCredentials[dbName]
	if err := updatedDbConfig.setup(dbName, h.server.config.Bootstrap, dbCreds); err != nil {
		return err
	}

	h.server.lock.Lock()
	defer h.server.lock.Unlock()

	// TODO: Dynamic update instead of reload
	if err := h.server._reloadDatabaseWithConfig(*updatedDbConfig, false); err != nil {
		return err
	}

	return base.HTTPErrorf(http.StatusOK, "sync function removed")
}

func (h *handler) handlePutDbConfigSync() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
	}

	js, err := h.readJavascript()
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

			bucketDbConfig.Sync = &js

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
	dbCreds, _ := h.server.config.DatabaseCredentials[dbName]
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

// GET database config import filter function
func (h *handler) handleGetDbConfigImportFilter() error {
	h.assertAdminOnly()
	var (
		etagVersion          string
		importFilterFunction string
	)

	if h.server.bootstrapContext.connection != nil {
		found, dbConfig, err := h.server.fetchDatabase(h.db.Name)
		if err != nil {
			return err
		}
		if !found {
			return base.HTTPErrorf(http.StatusNotFound, "database config not found")
		}
		etagVersion = dbConfig.Version
		if dbConfig.ImportFilter != nil {
			importFilterFunction = *dbConfig.ImportFilter
		}
	}

	h.response.Header().Set("ETag", etagVersion)
	h.writeJavascript(importFilterFunction)
	return nil
}

// DELETE a database config import filter
func (h *handler) handleDeleteDbConfigImportFilter() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
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

			bucketDbConfig.ImportFilter = nil
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
	dbCreds, _ := h.server.config.DatabaseCredentials[dbName]
	if err := updatedDbConfig.setup(dbName, h.server.config.Bootstrap, dbCreds); err != nil {
		return err
	}

	h.server.lock.Lock()
	defer h.server.lock.Unlock()

	// TODO: Dynamic update instead of reload
	if err := h.server._reloadDatabaseWithConfig(*updatedDbConfig, false); err != nil {
		return err
	}

	return base.HTTPErrorf(http.StatusOK, "import filter removed")
}

// PUT a new database config import filter function
func (h *handler) handlePutDbConfigImportFilter() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
	}

	js, err := h.readJavascript()
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

			bucketDbConfig.ImportFilter = &js

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
	dbCreds, _ := h.server.config.DatabaseCredentials[dbName]
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

// handleDeleteDB when running in persistent config mode, deletes a database config from the bucket and removes it from the current node.
// In non-persistent mode, the endpoint just removes the database from the node.
func (h *handler) handleDeleteDB() error {
	h.assertAdminOnly()

	if h.server.persistentConfig {
		bucket := h.db.Bucket.GetName()
		_, err := h.server.bootstrapContext.connection.UpdateConfig(bucket, h.server.config.Bootstrap.ConfigGroupID, func(rawBucketConfig []byte) (updatedConfig []byte, err error) {
			return nil, nil
		})
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't remove database %q from bucket %q: %s", base.MD(h.db.Name), base.MD(bucket), err.Error())
		}
	}

	if !h.server.RemoveDatabase(h.db.Name) {
		return base.HTTPErrorf(http.StatusNotFound, "missing")
	}
	_, _ = h.response.Write([]byte("{}"))
	return nil
}

// raw document access for admin api

func (h *handler) handleGetRawDoc() error {
	h.assertAdminOnly()
	docid := h.PathVar("docid")

	includeDoc, includeDocSet := h.getOptBoolQuery("include_doc", true)
	redact, _ := h.getOptBoolQuery("redact", false)
	salt := h.getQuery("salt")

	if redact && includeDoc && includeDocSet {
		return base.HTTPErrorf(http.StatusBadRequest, "redact and include_doc cannot be true at the same time. "+
			"If you want to redact you must specify include_doc=false")
	}

	if redact && !includeDocSet {
		includeDoc = false
	}

	doc, err := h.db.GetDocument(h.db.Ctx, docid, db.DocUnmarshalSync)
	if err != nil {
		return err
	}

	rawBytes := []byte(base.EmptyDocument)
	if includeDoc {
		if doc.IsDeleted() {
			rawBytes = []byte(db.DeletedDocument)
		} else {
			docRawBodyBytes, err := doc.BodyBytes()
			if err != nil {
				return err
			}
			rawBytes = docRawBodyBytes
		}
	}

	syncData := doc.SyncData
	if redact {
		if salt == "" {
			salt = uuid.New().String()
		}
		syncData = doc.SyncData.HashRedact(salt)
	}

	rawBytes, err = base.InjectJSONProperties(rawBytes, base.KVPair{Key: base.SyncPropertyName, Val: syncData})
	if err != nil {

		return err
	}

	if h.db.Options.UserXattrKey != "" {
		metaMap, err := doc.GetMetaMap(h.db.Options.UserXattrKey)
		if err != nil {
			return err
		}

		rawBytes, err = base.InjectJSONProperties(rawBytes, base.KVPair{Key: "_meta", Val: metaMap})
		if err != nil {
			return err
		}
	}

	h.writeRawJSON(rawBytes)
	return nil
}

func (h *handler) handleGetRevTree() error {
	h.assertAdminOnly()
	docid := h.PathVar("docid")
	doc, err := h.db.GetDocument(h.db.Ctx, docid, db.DocUnmarshalAll)

	if doc != nil {
		h.writeText([]byte(doc.History.RenderGraphvizDot()))
	}
	return err
}

func (h *handler) handleGetLogging() error {
	h.writeJSON(base.GetLogKeys())
	base.WarnfCtx(h.ctx(), "Deprecation notice: Current _logging endpoints are now deprecated. Using _config endpoints "+
		"instead")
	return nil
}

type DatabaseStatus struct {
	SequenceNumber    uint64                  `json:"seq"`
	ServerUUID        string                  `json:"server_uuid"`
	State             string                  `json:"state"`
	ReplicationStatus []*db.ReplicationStatus `json:"replication_status"`
	SGRCluster        *db.SGRCluster          `json:"cluster"`
}

type Status struct {
	Databases map[string]DatabaseStatus `json:"databases"`
	Version   string                    `json:"version"`
	Vendor    vendor                    `json:"vendor"`
}

func (h *handler) handleGetStatus() error {

	var status = Status{
		Databases: make(map[string]DatabaseStatus),
		Vendor:    vendor{Name: base.ProductNameString},
	}

	// This handler is supposed to be admin-only anyway, but being defensive if this is opened up in the routes file.
	if h.shouldShowProductVersion() {
		status.Version = base.LongVersionString
		status.Vendor.Version = base.ProductVersionNumber
	}

	for _, database := range h.server.databases_ {
		lastSeq := uint64(0)
		runState := db.RunStateString[atomic.LoadUint32(&database.State)]

		// Don't bother trying to lookup LastSequence() if offline
		if runState != db.RunStateString[db.DBOffline] {
			lastSeq, _ = database.LastSequence()
		}

		replicationsStatus, err := database.SGReplicateMgr.GetReplicationStatusAll(db.DefaultReplicationStatusOptions())
		if err != nil {
			return err
		}
		cluster, err := database.SGReplicateMgr.GetSGRCluster()
		if err != nil {
			return err
		}
		for _, replication := range cluster.Replications {
			replication.ReplicationConfig = *replication.Redacted()
		}

		status.Databases[database.Name] = DatabaseStatus{
			SequenceNumber:    lastSeq,
			State:             runState,
			ServerUUID:        database.GetServerUUID(),
			ReplicationStatus: replicationsStatus,
			SGRCluster:        cluster,
		}
	}

	h.writeJSON(status)
	return nil
}

func (h *handler) handleSetLogging() error {
	base.WarnfCtx(h.ctx(), "Deprecation notice: Current _logging endpoints are now deprecated. Using _config endpoints "+
		"instead")

	body, err := h.readBody()
	if err != nil {
		return nil
	}

	var newLogLevel base.LogLevel
	var setLogLevel bool
	if level := h.getQuery("logLevel"); level != "" {
		if err := newLogLevel.UnmarshalText([]byte(level)); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}
		setLogLevel = true
	} else if level := h.getIntQuery("level", 0); level != 0 {
		base.WarnfCtx(h.ctx(), "Using deprecated query parameter: %q. Use %q instead.", "level", "logLevel")
		switch base.GetRestrictedInt(&level, 0, 1, 3, false) {
		case 1:
			newLogLevel = base.LevelInfo
		case 2:
			newLogLevel = base.LevelWarn
		case 3:
			newLogLevel = base.LevelError
		}
		setLogLevel = true
	}

	if setLogLevel {
		base.InfofCtx(h.ctx(), base.KeyAll, "Setting log level to: %v", newLogLevel)
		base.ConsoleLogLevel().Set(newLogLevel)

		// empty body is OK if request is just setting the log level
		if len(body) == 0 {
			return nil
		}
	}

	var keys map[string]bool
	if err := base.JSONUnmarshal(body, &keys); err != nil {

		// return a better error if a user is setting log level inside the body
		var logLevel map[string]string
		if err := base.JSONUnmarshal(body, &logLevel); err == nil {
			if _, ok := logLevel["logLevel"]; ok {
				return base.HTTPErrorf(http.StatusBadRequest, "Can't set log level in body, please use \"logLevel\" query parameter instead.")
			}
		}

		return base.HTTPErrorf(http.StatusBadRequest, "Invalid JSON or non-boolean values for log key map")
	}

	base.UpdateLogKeys(keys, h.rq.Method == "PUT")
	return nil
}

func (h *handler) handleSGCollectStatus() error {
	status := "stopped"
	if sgcollectInstance.IsRunning() {
		status = "running"
	}

	h.writeRawJSONStatus(http.StatusOK, []byte(`{"status":"`+status+`"}`))
	return nil
}

func (h *handler) handleSGCollectCancel() error {
	err := sgcollectInstance.Stop()
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Error stopping sgcollect_info: %v", err)
	}

	h.writeRawJSONStatus(http.StatusOK, []byte(`{"status":"cancelled"}`))
	return nil
}

func (h *handler) handleSGCollect() error {
	body, err := h.readBody()
	if err != nil {
		return err
	}

	var params sgCollectOptions
	if err = base.JSONUnmarshal(body, &params); err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Unable to parse request body: %v", err)
	}

	if multiError := params.Validate(); multiError != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "Invalid options used for sgcollect_info: %v", multiError)
	}

	// Populate username and password used by sgcollect_info script for talking to Sync Gateway.
	params.syncGatewayUsername, params.syncGatewayPassword = h.getBasicAuth()

	zipFilename := sgcollectFilename()

	logFilePath := h.server.config.Logging.LogFilePath

	if err := sgcollectInstance.Start(logFilePath, h.serialNumber, zipFilename, params); err != nil {
		return base.HTTPErrorf(http.StatusInternalServerError, "Error running sgcollect_info: %v", err)
	}

	h.writeRawJSONStatus(http.StatusOK, []byte(`{"status":"started"}`))

	return nil
}

// ////// USERS & ROLES:

func internalUserName(name string) string {
	if name == base.GuestUsername {
		return ""
	}
	return name
}

func externalUserName(name string) string {
	if name == "" {
		return base.GuestUsername
	}
	return name
}

func marshalPrincipal(princ auth.Principal, includeDynamicGrantInfo bool) ([]byte, error) {
	name := externalUserName(princ.Name())
	info := db.PrincipalConfig{
		Name:             &name,
		ExplicitChannels: princ.ExplicitChannels().AsSet(),
	}
	if user, ok := princ.(auth.User); ok {
		info.Email = user.Email()
		info.Disabled = base.BoolPtr(user.Disabled())
		info.ExplicitRoleNames = user.ExplicitRoles().AllKeys()
		if includeDynamicGrantInfo {
			info.Channels = user.InheritedChannels().AsSet()
			info.RoleNames = user.RoleNames().AllKeys()
		}
	} else {
		if includeDynamicGrantInfo {
			info.Channels = princ.Channels().AsSet()
		}
	}
	return base.JSONMarshal(info)
}

// Handles PUT and POST for a user or a role.
func (h *handler) updatePrincipal(name string, isUser bool) error {
	h.assertAdminOnly()
	// Unmarshal the request body into a PrincipalConfig struct:
	body, _ := h.readBody()
	var newInfo db.PrincipalConfig
	var err error
	if err = base.JSONUnmarshal(body, &newInfo); err != nil {
		return err
	}

	if h.rq.Method == "POST" {
		// On POST, take the name from the "name" property in the request body:
		if newInfo.Name == nil {
			return base.HTTPErrorf(http.StatusBadRequest, "Missing name property")
		}
	} else {
		// ON PUT, verify the name matches, if given:
		if newInfo.Name == nil {
			newInfo.Name = &name
		} else if *newInfo.Name != name {
			return base.HTTPErrorf(http.StatusBadRequest, "Name mismatch (can't change name)")
		}
	}

	internalName := internalUserName(*newInfo.Name)
	newInfo.Name = &internalName
	replaced, err := h.db.UpdatePrincipal(h.db.Ctx, newInfo, isUser, h.rq.Method != "POST")
	if err != nil {
		return err
	} else if replaced {
		// on update with a new password, remove previous user sessions
		if newInfo.Password != nil {
			err = h.db.DeleteUserSessions(h.db.Ctx, *newInfo.Name)
			if err != nil {
				return err
			}
		}
		h.writeStatus(http.StatusOK, "OK")
	} else {
		h.writeStatus(http.StatusCreated, "Created")
	}
	return nil
}

// Handles PUT or POST to /_user/*
func (h *handler) putUser() error {
	username := mux.Vars(h.rq)["name"]
	return h.updatePrincipal(username, true)
}

// Handles PUT or POST to /_role/*
func (h *handler) putRole() error {
	rolename := mux.Vars(h.rq)["name"]
	return h.updatePrincipal(rolename, false)
}

func (h *handler) deleteUser() error {
	h.assertAdminOnly()
	username := mux.Vars(h.rq)["name"]

	// Can't delete the guest user, only disable.
	if username == base.GuestUsername {
		return base.HTTPErrorf(http.StatusMethodNotAllowed,
			"The %s user cannot be deleted. Only disabled via an update.", base.GuestUsername)
	}

	user, err := h.db.Authenticator(h.db.Ctx).GetUser(username)
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return h.db.Authenticator(h.db.Ctx).DeleteUser(user)
}

func (h *handler) deleteRole() error {
	h.assertAdminOnly()
	purge := h.getBoolQuery("purge")
	return h.db.DeleteRole(h.ctx(), mux.Vars(h.rq)["name"], purge)

}

func (h *handler) getUserInfo() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator(h.db.Ctx).GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	// If not specified will default to false
	includeDynamicGrantInfo := h.permissionsResults[PermReadPrincipalAppData.PermissionName]
	bytes, err := marshalPrincipal(user, includeDynamicGrantInfo)
	h.writeRawJSON(bytes)
	return err
}

func (h *handler) getRoleInfo() error {
	h.assertAdminOnly()
	role, err := h.db.Authenticator(h.db.Ctx).GetRole(mux.Vars(h.rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	// If not specified will default to false
	includeDynamicGrantInfo := h.permissionsResults[PermReadPrincipalAppData.PermissionName]
	bytes, err := marshalPrincipal(role, includeDynamicGrantInfo)
	_, _ = h.response.Write(bytes)
	return err
}

func (h *handler) getUsers() error {

	limit := h.getIntQuery(paramLimit, 0)
	nameOnly, _ := h.getOptBoolQuery(paramNameOnly, true)

	if limit > 0 && nameOnly {
		return base.HTTPErrorf(http.StatusBadRequest, fmt.Sprintf("Use of %s only supported when %s=false", paramLimit, paramNameOnly))
	}

	var bytes []byte
	var marshalErr error
	if nameOnly {
		users, _, err := h.db.AllPrincipalIDs(h.db.Ctx)
		if err != nil {
			return err
		}
		bytes, marshalErr = base.JSONMarshal(users)
	} else {
		if h.db.Options.UseViews {
			return base.HTTPErrorf(http.StatusBadRequest, fmt.Sprintf("Use of %s=false not supported when database has use_views=true", paramNameOnly))
		}
		users, err := h.db.GetUsers(h.db.Ctx, int(limit))
		if err != nil {
			return err
		}
		bytes, marshalErr = base.JSONMarshal(users)
	}

	if marshalErr != nil {
		return marshalErr
	}
	h.writeRawJSON(bytes)
	return nil
}

func (h *handler) getRoles() error {
	_, roles, err := h.db.AllPrincipalIDs(h.db.Ctx)
	if err != nil {
		return err
	}
	bytes, err := base.JSONMarshal(roles)
	h.writeRawJSON(bytes)
	return err
}

func (h *handler) handlePurge() error {
	h.assertAdminOnly()

	message := "OK"

	// Get the list of docs to purge

	input, err := h.readJSON()
	if err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, "_purge document ID's must be passed as a JSON")
	}

	startTime := time.Now()
	docIDs := make([]string, 0)

	h.setHeader("Content-Type", "application/json")
	h.setHeader("Cache-Control", "private, max-age=0, no-cache, no-store")
	_, _ = h.response.Write([]byte("{\"purged\":{\r\n"))
	var first bool = true

	for key, value := range input {
		// For each one validate that the revision list is set to ["*"], otherwise skip doc and log warning
		base.InfofCtx(h.db.Ctx, base.KeyCRUD, "purging document = %v", base.UD(key))

		if revisionList, ok := value.([]interface{}); ok {

			// There should only be a single revision entry of "*"
			if len(revisionList) != 1 {
				base.InfofCtx(h.db.Ctx, base.KeyCRUD, "Revision list for doc ID %v, should contain exactly one entry", base.UD(key))
				continue // skip this entry its not valid
			}

			if revisionList[0] != "*" {
				base.InfofCtx(h.db.Ctx, base.KeyCRUD, "Revision entry for doc ID %v, should be the '*' revison", base.UD(key))
				continue // skip this entry its not valid
			}

			// Attempt to delete document, if successful add to response, otherwise log warning
			err = h.db.Purge(key)
			if err == nil {

				docIDs = append(docIDs, key)

				if first {
					first = false
				} else {
					_, _ = h.response.Write([]byte(","))
				}

				s := fmt.Sprintf("\"%v\" : [\"*\"]\n", key)
				_, _ = h.response.Write([]byte(s))

			} else {
				base.InfofCtx(h.db.Ctx, base.KeyCRUD, "Failed to purge document %v, err = %v", base.UD(key), err)
				continue // skip this entry its not valid
			}

		} else {
			base.InfofCtx(h.db.Ctx, base.KeyCRUD, "Revision list for doc ID %v, is not an array, ", base.UD(key))
			continue // skip this entry its not valid
		}
	}

	if len(docIDs) > 0 {
		count := h.db.GetChangeCache().Remove(docIDs, startTime)
		base.DebugfCtx(h.db.Ctx, base.KeyCache, "Purged %d items from caches", count)
	}

	_, _ = h.response.Write([]byte("}\n}\n"))
	h.logStatusWithDuration(http.StatusOK, message)

	return nil
}

// sg-replicate endpoints
func (h *handler) getReplications() error {
	replications, err := h.db.SGReplicateMgr.GetReplications()
	if err != nil {
		return err
	}

	for _, replication := range replications {
		if replication.AssignedNode == h.db.UUID {
			replication.AssignedNode = replication.AssignedNode + " (local)"
		} else {
			replication.AssignedNode = replication.AssignedNode + " (non-local)"
		}
		replication.ReplicationConfig = *replication.Redacted()
	}

	h.writeJSON(replications)
	return nil
}

func (h *handler) getReplication() error {
	replicationID := mux.Vars(h.rq)["replicationID"]
	replication, err := h.db.SGReplicateMgr.GetReplication(replicationID)
	if replication == nil {
		if err == nil {
			return kNotFoundError
		}
		return err
	}

	h.writeJSON(replication.Redacted())
	return nil
}

func (h *handler) putReplication() error {

	body, readErr := h.readBody()
	if readErr != nil {
		return readErr
	}
	body = base.ConvertBackQuotedStrings(body)

	replicationConfig := &db.ReplicationUpsertConfig{}
	if err := base.JSONUnmarshal(body, replicationConfig); err != nil {
		return err
	}

	if h.rq.Method == "PUT" {
		replicationID := mux.Vars(h.rq)["replicationID"]
		if replicationConfig.ID != "" && replicationConfig.ID != replicationID {
			return base.HTTPErrorf(http.StatusBadRequest, "Replication ID in body %q does not match request URI", replicationConfig.ID)
		}
		replicationConfig.ID = replicationID
	}

	created, err := h.db.SGReplicateMgr.UpsertReplication(replicationConfig)
	if err != nil {
		return err
	}
	if created {
		h.writeStatus(http.StatusCreated, "Created")
	}

	return nil
}

func (h *handler) deleteReplication() error {
	replicationID := mux.Vars(h.rq)["replicationID"]
	return h.db.SGReplicateMgr.DeleteReplication(replicationID)
}

func (h *handler) getReplicationsStatus() error {
	replicationsStatus, err := h.db.SGReplicateMgr.GetReplicationStatusAll(h.getReplicationStatusOptions())
	if err != nil {
		return err
	}
	h.writeJSON(replicationsStatus)
	return nil
}

func (h *handler) getReplicationStatus() error {
	replicationID := mux.Vars(h.rq)["replicationID"]
	status, err := h.db.SGReplicateMgr.GetReplicationStatus(replicationID, h.getReplicationStatusOptions())
	if err != nil {
		return err
	}
	h.writeJSON(status)
	return nil
}

func (h *handler) getReplicationStatusOptions() db.ReplicationStatusOptions {
	activeOnly, _ := h.getOptBoolQuery("activeOnly", false)
	localOnly, _ := h.getOptBoolQuery("localOnly", false)
	includeError, _ := h.getOptBoolQuery("includeError", true)
	includeConfig, _ := h.getOptBoolQuery("includeConfig", false)
	return db.ReplicationStatusOptions{
		ActiveOnly:    activeOnly,
		LocalOnly:     localOnly,
		IncludeError:  includeError,
		IncludeConfig: includeConfig,
	}
}

func (h *handler) putReplicationStatus() error {
	replicationID := mux.Vars(h.rq)["replicationID"]

	action := h.getQuery("action")
	if action == "" {
		return base.HTTPErrorf(http.StatusBadRequest, "Query parameter 'action' must be specified")
	}

	updatedStatus, err := h.db.SGReplicateMgr.PutReplicationStatus(replicationID, action)
	if err != nil {
		return err
	}
	h.writeJSON(updatedStatus)
	return nil
}
