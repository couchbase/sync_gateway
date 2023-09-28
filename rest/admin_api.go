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
	"context"
	"errors"
	"fmt"
	"io"
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
const paramDeleted = "deleted"

// ////// DATABASE MAINTENANCE:

// "Create" a database (actually just register an existing bucket)
func (h *handler) handleCreateDB() error {
	contextNoCancel := base.NewNonCancelCtx()
	h.assertAdminOnly()
	dbName := h.PathVar("newdb")
	config, err := h.readSanitizeDbConfigJSON()
	if err != nil {
		return err
	}

	validateOIDC := !h.getBoolQuery(paramDisableOIDCValidation)

	if config.Name != "" && dbName != config.Name {
		return base.HTTPErrorf(http.StatusBadRequest, "When providing a name in the JSON body (%s), ensure it matches the name in the path (%s).", config.Name, dbName)
	}

	config.Name = dbName
	if h.server.persistentConfig {
		if err := config.validatePersistentDbConfig(); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		if err := config.validate(h.ctx(), validateOIDC); err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		version, err := GenerateDatabaseConfigVersionID(h.ctx(), "", config)
		if err != nil {
			return err
		}

		bucket := config.GetBucketName()

		// Before computing metadata ID or taking a copy of the "persistedDbConfig", stamp bucket if missing...
		// Ensures all newly persisted configs have a bucket field set to detect bucket moves via backup/restore or XDCR
		if config.Bucket == nil {
			config.Bucket = &bucket
		}

		metadataID, metadataIDError := h.server.BootstrapContext.ComputeMetadataIDForDbConfig(h.ctx(), config)
		if metadataIDError != nil {
			base.WarnfCtx(h.ctx(), "Unable to compute metadata ID - using standard metadataID.  Error: %v", metadataIDError)
			metadataID = h.server.BootstrapContext.standardMetadataID(config.Name)
		}

		// copy config before setup to persist the raw config the user supplied
		var persistedDbConfig DbConfig
		if err := base.DeepCopyInefficient(&persistedDbConfig, config); err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't create copy of db config: %v", err)
		}

		dbCreds, _ := h.server.Config.DatabaseCredentials[dbName]
		bucketCreds, _ := h.server.Config.BucketCredentials[bucket]
		if err := config.setup(h.ctx(), dbName, h.server.Config.Bootstrap, dbCreds, bucketCreds, h.server.Config.IsServerless()); err != nil {
			return err
		}

		loadedConfig := DatabaseConfig{
			Version:    version,
			MetadataID: metadataID,
			DbConfig:   *config,
		}

		persistedConfig := DatabaseConfig{
			Version:    version,
			MetadataID: metadataID,
			DbConfig:   persistedDbConfig,
			SGVersion:  base.ProductVersion.String(),
		}

		h.server.lock.Lock()
		defer h.server.lock.Unlock()

		if _, exists := h.server.databases_[dbName]; exists {
			return base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
				"Duplicate database name %q", dbName)
		}

		_, err = h.server._applyConfig(contextNoCancel, loadedConfig, true, false)
		if err != nil {
			var httpErr *base.HTTPError
			if errors.As(err, &httpErr) {
				return httpErr
			}
			if errors.Is(err, base.ErrAuthError) {
				return base.HTTPErrorf(http.StatusForbidden, "Provided credentials do not have access to specified bucket/scope/collection")
			}
			if errors.Is(err, base.ErrAlreadyExists) {
				return base.HTTPErrorf(http.StatusConflict, "couldn't load database: %s", err)
			}
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't load database: %v", err)
		}

		// now we've started the db successfully, we can persist it to the cluster first checking if this db used to be a corrupt db
		// if it used to be corrupt we need to remove it from the invalid database map on server context and remove the old corrupt config from the bucket
		err = h.removeCorruptConfigIfExists(contextNoCancel.Ctx, bucket, h.server.Config.Bootstrap.ConfigGroupID, dbName)
		if err != nil {
			return err
		}
		cas, err := h.server.BootstrapContext.InsertConfig(contextNoCancel.Ctx, bucket, h.server.Config.Bootstrap.ConfigGroupID, &persistedConfig)
		if err != nil {
			// unload the requested database config to prevent the cluster being in an inconsistent state
			h.server._removeDatabase(contextNoCancel.Ctx, dbName)
			var httpError *base.HTTPError
			if errors.As(err, &httpError) {
				// Collection conflict returned as http error with conflict details
				return err
			} else if errors.Is(err, base.ErrAuthError) {
				return base.HTTPErrorf(http.StatusForbidden, "auth failure accessing provided bucket using bootstrap credentials: %s", bucket)
			} else if errors.Is(err, base.ErrAlreadyExists) {
				// on-demand config load if someone else beat us to db creation
				if _, err := h.server._fetchAndLoadDatabase(contextNoCancel, dbName); err != nil {
					base.WarnfCtx(h.ctx(), "Couldn't load database after conflicting create: %v", err)
				}
				return base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
					"Duplicate database name %q", dbName)
			}
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't save database config: %v", err)
		}
		// store the cas in the loaded config after a successful insert
		h.server.dbConfigs[dbName].cfgCas = cas
	} else {
		// Intentionally pass in an empty BootstrapConfig to avoid inheriting any credentials or server when running with a legacy config (CBG-1764)
		if err := config.setup(h.ctx(), dbName, BootstrapConfig{}, nil, nil, false); err != nil {
			return err
		}

		// load database in-memory for non-persistent nodes
		if _, err := h.server.AddDatabaseFromConfigFailFast(contextNoCancel, DatabaseConfig{DbConfig: *config}); err != nil {
			if errors.Is(err, base.ErrAuthError) {
				return base.HTTPErrorf(http.StatusForbidden, "auth failure using provided bucket credentials for database %s", base.MD(config.Name))
			}
			return err
		}
	}

	return base.HTTPErrorf(http.StatusCreated, "created")
}

// getAuthScopeHandleCreateDB determines the auth scope for a PUT /{db}/ request.
// This is a special case because we don't have a database initialized yet, so we need to infer the bucket from db config.
func getAuthScopeHandleCreateDB(ctx context.Context, h *handler) (bucketName string, err error) {

	// grab a copy of the request body and restore body buffer for later handlers
	bodyJSON, err := h.readBody()
	if err != nil {
		return "", base.HTTPErrorf(http.StatusInternalServerError, "Unable to read body: %v", err)
	}
	// mark the body as already read to avoid double-counting bytes for stats once it gets read again
	h.requestBody.bodyRead = true
	h.requestBody.reader = io.NopCloser(bytes.NewReader(bodyJSON))

	var dbConfigBody struct {
		Bucket string `json:"bucket"`
	}
	reader := bytes.NewReader(bodyJSON)
	err = DecodeAndSanitiseConfig(ctx, reader, &dbConfigBody, false)
	if err != nil {
		return "", err
	}

	if dbConfigBody.Bucket == "" {
		// imply bucket name from db name in path if not in body
		return h.PathVar("newdb"), nil
	}

	return dbConfigBody.Bucket, nil
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
		h.server.TakeDbOnline(base.NewNonCancelCtx(), h.db.DatabaseContext)
	}()

	return nil
}

// Take a DB offline
func (h *handler) handleDbOffline() error {
	h.assertAdminOnly()
	var err error
	if err = h.db.TakeDbOffline(base.NewNonCancelCtx(), "ADMIN Request"); err != nil {
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
	if h.server.BootstrapContext.Connection != nil {
		found, dbConfig, err := h.server.fetchDatabase(h.ctx(), h.db.Name)
		if err != nil {
			return err
		}

		if !found || dbConfig == nil {
			return base.HTTPErrorf(http.StatusNotFound, "database config not found")
		}

		h.setEtag(dbConfig.Version)
		// refresh_config=true forces the config loaded out of the bucket to be applied on the node
		if h.getBoolQuery("refresh_config") && h.server.BootstrapContext.Connection != nil {
			// set cas=0 to force a refresh
			dbConfig.cfgCas = 0
			h.server.applyConfigs(h.ctx(), map[string]DatabaseConfig{h.db.Name: *dbConfig})
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
		responseConfig, err = MergeDatabaseConfigWithDefaults(h.server.Config, h.server.GetDbConfig(h.db.Name))
		if err != nil {
			return err
		}
	}

	// defensive check - there could've been an in-flight request to remove the database between entering the handler and getting the config above.
	if responseConfig == nil {
		return base.HTTPErrorf(http.StatusNotFound, "database config not found")
	}

	var err error
	responseConfig, err = responseConfig.Redacted(h.ctx())
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
		cfg.StartupConfig, err = h.server.Config.Redacted()
		if err != nil {
			return err
		}

		for _, dbName := range allDbNames {
			// defensive check - in-flight requests could've removed this database since we got the name of it
			dbConfig := h.server.GetDbConfig(dbName)
			if dbConfig == nil {
				continue
			}

			dbConfig, err := MergeDatabaseConfigWithDefaults(h.server.Config, dbConfig)
			if err != nil {
				return err
			}

			databaseMap[dbName], err = dbConfig.Redacted(h.ctx())
			if err != nil {
				return err
			}
		}

		for dbName, dbConfig := range databaseMap {
			database, err := h.server.GetDatabase(h.ctx(), dbName)
			if err != nil {
				return err
			}

			replications, err := database.SGReplicateMgr.GetReplications()
			if err != nil {
				return err
			}

			dbConfig.Replications = make(map[string]*db.ReplicationConfig, len(replications))

			for replicationName, replicationConfig := range replications {
				dbConfig.Replications[replicationName] = replicationConfig.ReplicationConfig.Redacted(h.ctx())
			}
		}

		cfg.Logging = *base.BuildLoggingConfigFromLoggers(h.server.Config.Logging.RedactionLevel, h.server.Config.Logging.LogFilePath)
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
		ReplicationLimit *int `json:"max_concurrent_replications,omitempty"`
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

			base.UpdateLogKeys(h.ctx(), testMap, true)
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

	if config.ReplicationLimit != nil {
		if *config.ReplicationLimit < 0 {
			return base.HTTPErrorf(http.StatusBadRequest, "replication limit cannot be less than 0")
		}
		h.server.Config.Replicator.MaxConcurrentReplications = *config.ReplicationLimit
		h.server.ActiveReplicationsCounter.lock.Lock()
		h.server.ActiveReplicationsCounter.activeReplicatorLimit = *config.ReplicationLimit
		h.server.ActiveReplicationsCounter.lock.Unlock()
	}

	return base.HTTPErrorf(http.StatusOK, "Updated")
}

// handlePutDbConfig Upserts a new database config
func (h *handler) handlePutDbConfig() (err error) {
	h.assertAdminOnly()
	contextNoCancel := base.NewNonCancelCtx()

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

		bodyContents, err := io.ReadAll(h.requestBody)
		if err != nil {
			return err
		}

		var mapDbConfig map[string]interface{}
		err = ReadJSONFromMIMERawErr(h.rq.Header, io.NopCloser(bytes.NewReader(bodyContents)), &mapDbConfig)
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

		err = ReadJSONFromMIMERawErr(h.rq.Header, io.NopCloser(bytes.NewReader(bodyContents)), &dbConfig)
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
		err := updatedDbConfig.validate(h.ctx(), validateOIDC)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}
		oldDBConfig := h.server.GetDatabaseConfig(h.db.Name).DatabaseConfig.DbConfig
		err = updatedDbConfig.validateConfigUpdate(h.ctx(), oldDBConfig,
			validateOIDC)
		if err != nil {
			return base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		dbCreds, _ := h.server.Config.DatabaseCredentials[dbName]
		if err := updatedDbConfig.setup(h.ctx(), dbName, h.server.Config.Bootstrap, dbCreds, nil, false); err != nil {
			return err
		}
		if err := h.server.ReloadDatabaseWithConfig(contextNoCancel, *updatedDbConfig, false); err != nil {
			return err
		}
		return base.HTTPErrorf(http.StatusCreated, "updated")
	}

	var updatedDbConfig *DatabaseConfig
	cas, err := h.server.BootstrapContext.UpdateConfig(h.ctx(), bucket, h.server.Config.Bootstrap.ConfigGroupID, dbConfig.Name, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		if h.headerDoesNotMatchEtag(bucketDbConfig.Version) {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, "Provided If-Match header does not match current config version")
		}
		oldBucketDbConfig := bucketDbConfig.DbConfig

		if h.rq.Method == http.MethodPost {
			base.TracefCtx(h.ctx(), base.KeyConfig, "merging upserted config into bucket config")
			if err := base.ConfigMerge(&bucketDbConfig.DbConfig, dbConfig); err != nil {
				return nil, err
			}
		} else {
			base.TracefCtx(h.ctx(), base.KeyConfig, "using config as-is without merge")
			bucketDbConfig.DbConfig = *dbConfig
		}

		if err := dbConfig.validatePersistentDbConfig(); err != nil {
			return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}
		if err := bucketDbConfig.validateConfigUpdate(h.ctx(), oldBucketDbConfig, validateOIDC); err != nil {
			return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(h.ctx(), bucketDbConfig.Version, &bucketDbConfig.DbConfig)
		if err != nil {
			return nil, err
		}

		bucketDbConfig.SGVersion = base.ProductVersion.String()
		updatedDbConfig = bucketDbConfig

		// take a copy to stamp credentials and load before we persist
		var tmpConfig DatabaseConfig
		if err = base.DeepCopyInefficient(&tmpConfig, bucketDbConfig); err != nil {
			return nil, err
		}
		tmpConfig.cfgCas = bucketDbConfig.cfgCas
		dbCreds, _ := h.server.Config.DatabaseCredentials[dbName]
		bucketCreds, _ := h.server.Config.BucketCredentials[bucket]
		if err := tmpConfig.setup(h.ctx(), dbName, h.server.Config.Bootstrap, dbCreds, bucketCreds, h.server.Config.IsServerless()); err != nil {
			return nil, err
		}

		// Load the new dbConfig before we persist the update.
		err = h.server.ReloadDatabaseWithConfig(contextNoCancel, tmpConfig, true)
		if err != nil {
			return nil, err
		}
		return bucketDbConfig, nil
	})
	if err != nil {
		base.WarnfCtx(h.ctx(), "Couldn't update config for database - rolling back: %v", err)
		// failed to start the new database config - rollback and return the original error for the user
		if _, err := h.server.fetchAndLoadDatabase(contextNoCancel, dbName); err != nil {
			base.WarnfCtx(h.ctx(), "got error rolling back database %q after failed update: %v", base.UD(dbName), err)
		}
		return err
	}
	// store the cas in the loaded config after a successful update
	h.setEtag(updatedDbConfig.Version)
	h.server.lock.Lock()
	defer h.server.lock.Unlock()
	h.server.dbConfigs[dbName].cfgCas = cas

	return base.HTTPErrorf(http.StatusCreated, "updated")

}

// GET collection config sync function
func (h *handler) handleGetCollectionConfigSync() error {
	h.assertAdminOnly()
	var (
		etagVersion  string
		syncFunction string
	)

	if h.server.BootstrapContext.Connection != nil {
		found, dbConfig, err := h.server.fetchDatabase(h.ctx(), h.db.Name)
		if err != nil {
			return err
		}

		if !found {
			return base.HTTPErrorf(http.StatusNotFound, "database config not found")
		}

		etagVersion = dbConfig.Version

		if dbConfig.Scopes != nil {
			scope, ok := dbConfig.Scopes[h.collection.ScopeName]
			if ok {
				collectionConfig, ok := scope.Collections[h.collection.Name]
				if ok && collectionConfig.SyncFn != nil {
					syncFunction = *collectionConfig.SyncFn

				}
			}
		} else if dbConfig.Sync != nil {
			syncFunction = *dbConfig.Sync
		}
	}

	h.setEtag(etagVersion)
	h.writeJavascript(syncFunction)
	return nil
}

// DELETE a collection sync function
func (h *handler) handleDeleteCollectionConfigSync() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
	}

	bucket := h.db.Bucket.GetName()
	var updatedDbConfig *DatabaseConfig
	cas, err := h.server.BootstrapContext.UpdateConfig(h.ctx(), bucket, h.server.Config.Bootstrap.ConfigGroupID, h.db.Name, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		if h.headerDoesNotMatchEtag(bucketDbConfig.Version) {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, "Provided If-Match header does not match current config version")
		}
		if bucketDbConfig.Scopes != nil {
			config := bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name]
			config.SyncFn = nil
			bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name] = config
		} else if base.IsDefaultCollection(h.collection.ScopeName, h.collection.Name) {
			bucketDbConfig.Sync = nil
		}

		bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(h.ctx(), bucketDbConfig.Version, &bucketDbConfig.DbConfig)
		if err != nil {
			return nil, err
		}

		bucketDbConfig.SGVersion = base.ProductVersion.String()
		updatedDbConfig = bucketDbConfig

		return bucketDbConfig, nil
	})
	if err != nil {
		return err
	}
	updatedDbConfig.cfgCas = cas

	dbName := h.db.Name
	dbCreds, _ := h.server.Config.DatabaseCredentials[dbName]
	bucketCreds, _ := h.server.Config.BucketCredentials[bucket]
	if err := updatedDbConfig.setup(h.ctx(), dbName, h.server.Config.Bootstrap, dbCreds, bucketCreds, h.server.Config.IsServerless()); err != nil {
		return err
	}

	h.server.lock.Lock()
	defer h.server.lock.Unlock()

	// TODO: Dynamic update instead of reload
	if err := h.server._reloadDatabaseWithConfig(h.ctx(), *updatedDbConfig, false, false); err != nil {
		return err
	}

	return base.HTTPErrorf(http.StatusOK, "sync function removed")
}

func (h *handler) handlePutCollectionConfigSync() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
	}

	js, err := h.readJavascript()
	if err != nil {
		return err
	}

	bucket := h.db.Bucket.GetName()
	dbName := h.db.Name

	var updatedDbConfig *DatabaseConfig
	cas, err := h.server.BootstrapContext.UpdateConfig(h.ctx(), bucket, h.server.Config.Bootstrap.ConfigGroupID, dbName, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {
		if h.headerDoesNotMatchEtag(bucketDbConfig.Version) {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, "Provided If-Match header does not match current config version")
		}

		if bucketDbConfig.Scopes != nil {
			config := bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name]
			config.SyncFn = &js
			bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name] = config
		} else if base.IsDefaultCollection(h.collection.ScopeName, h.collection.Name) {
			bucketDbConfig.Sync = &js
		}

		if err := bucketDbConfig.validate(h.ctx(), !h.getBoolQuery(paramDisableOIDCValidation)); err != nil {
			return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(h.ctx(), bucketDbConfig.Version, &bucketDbConfig.DbConfig)
		if err != nil {
			return nil, err
		}

		bucketDbConfig.SGVersion = base.ProductVersion.String()
		updatedDbConfig = bucketDbConfig
		return bucketDbConfig, nil
	})
	if err != nil {
		return err
	}
	updatedDbConfig.cfgCas = cas

	dbCreds, _ := h.server.Config.DatabaseCredentials[dbName]
	bucketCreds, _ := h.server.Config.BucketCredentials[bucket]
	if err := updatedDbConfig.setup(h.ctx(), dbName, h.server.Config.Bootstrap, dbCreds, bucketCreds, h.server.Config.IsServerless()); err != nil {
		return err
	}

	h.server.lock.Lock()
	defer h.server.lock.Unlock()

	// TODO: Dynamic update instead of reload
	if err := h.server._reloadDatabaseWithConfig(h.ctx(), *updatedDbConfig, false, false); err != nil {
		return err
	}

	return base.HTTPErrorf(http.StatusOK, "updated")
}

// GET collection config import filter function
func (h *handler) handleGetCollectionConfigImportFilter() error {
	h.assertAdminOnly()
	var (
		etagVersion          string
		importFilterFunction string
	)

	if h.server.BootstrapContext.Connection != nil {
		found, dbConfig, err := h.server.fetchDatabase(h.ctx(), h.db.Name)
		if err != nil {
			return err
		}
		if !found {
			return base.HTTPErrorf(http.StatusNotFound, "database config not found")
		}
		etagVersion = dbConfig.Version
		if dbConfig.Scopes != nil {
			scope, ok := dbConfig.Scopes[h.collection.ScopeName]
			if ok {
				collectionConfig, ok := scope.Collections[h.collection.Name]
				if ok && collectionConfig.ImportFilter != nil {
					importFilterFunction = *collectionConfig.ImportFilter

				}
			}
		} else {
			if dbConfig.ImportFilter != nil {
				importFilterFunction = *dbConfig.ImportFilter
			}
		}
	}

	h.setEtag(etagVersion)
	h.writeJavascript(importFilterFunction)
	return nil
}

// DELETE a collection config import filter
func (h *handler) handleDeleteCollectionConfigImportFilter() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
	}

	bucket := h.db.Bucket.GetName()
	dbName := h.db.Name

	var updatedDbConfig *DatabaseConfig
	cas, err := h.server.BootstrapContext.UpdateConfig(h.ctx(), bucket, h.server.Config.Bootstrap.ConfigGroupID, dbName, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {

		if h.headerDoesNotMatchEtag(bucketDbConfig.Version) {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, "Provided If-Match header does not match current config version")
		}

		if bucketDbConfig.Scopes != nil {
			config := bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name]
			config.ImportFilter = nil
			bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name] = config
		} else if base.IsDefaultCollection(h.collection.ScopeName, h.collection.Name) {
			bucketDbConfig.ImportFilter = nil
		}

		bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(h.ctx(), bucketDbConfig.Version, &bucketDbConfig.DbConfig)
		if err != nil {
			return nil, err
		}

		bucketDbConfig.SGVersion = base.ProductVersion.String()
		updatedDbConfig = bucketDbConfig
		return bucketDbConfig, nil
	})
	if err != nil {
		return err
	}
	updatedDbConfig.cfgCas = cas

	dbCreds, _ := h.server.Config.DatabaseCredentials[dbName]
	bucketCreds, _ := h.server.Config.BucketCredentials[bucket]
	if err := updatedDbConfig.setup(h.ctx(), dbName, h.server.Config.Bootstrap, dbCreds, bucketCreds, h.server.Config.IsServerless()); err != nil {
		return err
	}

	h.server.lock.Lock()
	defer h.server.lock.Unlock()

	// TODO: Dynamic update instead of reload
	if err := h.server._reloadDatabaseWithConfig(h.ctx(), *updatedDbConfig, false, false); err != nil {
		return err
	}

	return base.HTTPErrorf(http.StatusOK, "import filter removed")
}

// PUT a new database config import filter function
func (h *handler) handlePutCollectionConfigImportFilter() error {
	h.assertAdminOnly()

	if !h.server.persistentConfig {
		return base.HTTPErrorf(http.StatusBadRequest, "endpoint only supports persistent config mode")
	}

	js, err := h.readJavascript()
	if err != nil {
		return err
	}
	bucket := h.db.Bucket.GetName()
	dbName := h.db.Name

	var updatedDbConfig *DatabaseConfig
	cas, err := h.server.BootstrapContext.UpdateConfig(h.ctx(), bucket, h.server.Config.Bootstrap.ConfigGroupID, dbName, func(bucketDbConfig *DatabaseConfig) (updatedConfig *DatabaseConfig, err error) {

		if h.headerDoesNotMatchEtag(bucketDbConfig.Version) {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, "Provided If-Match header does not match current config version")
		}

		if bucketDbConfig.Scopes != nil {
			config := bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name]
			config.ImportFilter = &js
			bucketDbConfig.Scopes[h.collection.ScopeName].Collections[h.collection.Name] = config
		} else if base.IsDefaultCollection(h.collection.ScopeName, h.collection.Name) {
			bucketDbConfig.ImportFilter = &js
		}

		if err := bucketDbConfig.validate(h.ctx(), !h.getBoolQuery(paramDisableOIDCValidation)); err != nil {
			return nil, base.HTTPErrorf(http.StatusBadRequest, err.Error())
		}

		bucketDbConfig.Version, err = GenerateDatabaseConfigVersionID(h.ctx(), bucketDbConfig.Version, &bucketDbConfig.DbConfig)
		if err != nil {
			return nil, err
		}

		bucketDbConfig.SGVersion = base.ProductVersion.String()
		updatedDbConfig = bucketDbConfig
		return bucketDbConfig, nil
	})
	if err != nil {
		return err
	}
	updatedDbConfig.cfgCas = cas

	dbCreds, _ := h.server.Config.DatabaseCredentials[dbName]
	bucketCreds, _ := h.server.Config.BucketCredentials[bucket]
	if err := updatedDbConfig.setup(h.ctx(), dbName, h.server.Config.Bootstrap, dbCreds, bucketCreds, h.server.Config.IsServerless()); err != nil {
		return err
	}

	h.server.lock.Lock()
	defer h.server.lock.Unlock()

	// TODO: Dynamic update instead of reload
	if err := h.server._reloadDatabaseWithConfig(h.ctx(), *updatedDbConfig, false, false); err != nil {
		return err
	}

	return base.HTTPErrorf(http.StatusOK, "updated")
}

// handleDeleteDB when running in persistent config mode, deletes a database config from the bucket and removes it from the current node.
// In non-persistent mode, the endpoint just removes the database from the node.
func (h *handler) handleDeleteDB() error {
	h.assertAdminOnly()
	dbName := h.PathVar("db")

	var bucket string

	if h.server.persistentConfig {
		bucket, _ = h.server.bucketNameFromDbName(h.ctx(), dbName)
		err := h.server.BootstrapContext.DeleteConfig(h.ctx(), bucket, h.server.Config.Bootstrap.ConfigGroupID, dbName)
		if err != nil {
			return base.HTTPErrorf(http.StatusInternalServerError, "couldn't remove database %q from bucket %q: %s", base.MD(dbName), base.MD(bucket), err.Error())
		}
		h.server.RemoveDatabase(h.ctx(), dbName) // unhandled 404 to allow broken config deletion (CBG-2420)
		_, _ = h.response.Write([]byte("{}"))
		return nil
	}

	if !h.server.RemoveDatabase(h.ctx(), dbName) {
		return base.HTTPErrorf(http.StatusNotFound, "no such database %q", dbName)
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

	doc, err := h.collection.GetDocument(h.ctx(), docid, db.DocUnmarshalSync)
	if err != nil {
		return err
	}

	rawBytes := []byte(base.EmptyDocument)
	if includeDoc {
		if doc.IsDeleted() {
			rawBytes = []byte(db.DeletedDocument)
		} else {
			docRawBodyBytes, err := doc.BodyBytes(h.ctx())
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
	doc, err := h.collection.GetDocument(h.ctx(), docid, db.DocUnmarshalAll)

	if doc != nil {
		h.writeText([]byte(doc.History.RenderGraphvizDot()))
	}
	return err
}

func (h *handler) handleGetLogging() error {
	base.WarnfCtx(h.ctx(), "Using deprecated /_logging endpoint. Use /_config endpoints instead.")
	h.writeJSON(base.GetLogKeys())
	return nil
}

type DatabaseStatus struct {
	SequenceNumber    uint64                  `json:"seq"`
	ServerUUID        string                  `json:"server_uuid"`
	State             string                  `json:"state"`
	RequireResync     []string                `json:"require_resync"`
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
		status.Vendor.Version = base.ProductAPIVersion
	}

	for _, database := range h.server.databases_ {
		lastSeq := uint64(0)
		runState := db.RunStateString[atomic.LoadUint32(&database.State)]

		// Don't bother trying to lookup LastSequence() if offline
		if runState != db.RunStateString[db.DBOffline] {
			lastSeq, _ = database.LastSequence(h.ctx())
		}

		replicationsStatus, err := database.SGReplicateMgr.GetReplicationStatusAll(h.ctx(), db.DefaultReplicationStatusOptions())
		if err != nil {
			return err
		}
		cluster, err := database.SGReplicateMgr.GetSGRCluster()
		if err != nil {
			return err
		}
		for _, replication := range cluster.Replications {
			replication.ReplicationConfig = *replication.Redacted(h.ctx())
		}

		status.Databases[database.Name] = DatabaseStatus{
			SequenceNumber:    lastSeq,
			State:             runState,
			ServerUUID:        database.ServerUUID,
			ReplicationStatus: replicationsStatus,
			SGRCluster:        cluster,
			RequireResync:     database.RequireResync.ScopeAndCollectionNames(),
		}
	}

	h.writeJSON(status)
	return nil
}

func (h *handler) handleSetLogging() error {
	base.WarnfCtx(h.ctx(), "Using deprecated /_logging endpoint. Use /_config endpoints instead.")

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

	base.UpdateLogKeys(h.ctx(), keys, h.rq.Method == "PUT")
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

	logFilePath := h.server.Config.Logging.LogFilePath

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

// marshalPrincipal outputs a PrincipalConfig in a format for REST API endpoints.
func marshalPrincipal(database *db.Database, princ auth.Principal, includeDynamicGrantInfo bool) auth.PrincipalConfig {
	name := externalUserName(princ.Name())
	info := auth.PrincipalConfig{
		Name:             &name,
		ExplicitChannels: princ.ExplicitChannels().AsSet(),
	}

	collectionAccess := princ.GetCollectionsAccess()
	if collectionAccess != nil && !database.OnlyDefaultCollection() {
		info.CollectionAccess = make(map[string]map[string]*auth.CollectionAccessConfig)
		for scopeName, scope := range collectionAccess {
			scopeAccessConfig := make(map[string]*auth.CollectionAccessConfig)
			for collectionName, collection := range scope {
				_, err := database.GetDatabaseCollection(scopeName, collectionName)
				// collection doesn't exist anymore, but did at some point
				if err != nil {
					continue
				}
				collectionAccessConfig := &auth.CollectionAccessConfig{
					ExplicitChannels_: collection.ExplicitChannels().AsSet(),
				}
				if includeDynamicGrantInfo {
					if user, ok := princ.(auth.User); ok {
						collectionAccessConfig.Channels_ = user.InheritedCollectionChannels(scopeName, collectionName).AsSet()
						collectionAccessConfig.JWTChannels_ = user.CollectionJWTChannels(scopeName, collectionName).AsSet()
						lastUpdated := collection.JWTLastUpdated
						if lastUpdated != nil && !lastUpdated.IsZero() {
							collectionAccessConfig.JWTLastUpdated = lastUpdated
						}
					} else {
						collectionAccessConfig.Channels_ = princ.CollectionChannels(scopeName, collectionName).AsSet()
					}
				}
				scopeAccessConfig[collectionName] = collectionAccessConfig
			}
			info.CollectionAccess[scopeName] = scopeAccessConfig
		}
	}

	if user, ok := princ.(auth.User); ok {
		email := user.Email()
		info.Email = &email
		info.Disabled = base.BoolPtr(user.Disabled())
		info.ExplicitRoleNames = user.ExplicitRoles().AsSet()
		if includeDynamicGrantInfo {
			info.Channels = user.InheritedCollectionChannels(base.DefaultScope, base.DefaultCollection).AsSet()
			info.RoleNames = user.RoleNames().AllKeys()
			info.JWTIssuer = base.StringPtr(user.JWTIssuer())
			info.JWTRoles = user.JWTRoles().AsSet()
			info.JWTChannels = user.JWTChannels().AsSet()
			lastUpdated := user.JWTLastUpdated()
			if !lastUpdated.IsZero() {
				info.JWTLastUpdated = &lastUpdated
			}
		}
	} else {
		if includeDynamicGrantInfo {
			info.Channels = princ.Channels().AsSet()
		}
	}
	return info
}

// Handles PUT and POST for a user or a role.
func (h *handler) updatePrincipal(name string, isUser bool) error {
	h.assertAdminOnly()
	// Unmarshal the request body into a PrincipalConfig struct:
	body, _ := h.readBody()
	var newInfo auth.PrincipalConfig
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

	// NB: other read-only properties are ignored but no error is returned for backwards-compatibility
	if newInfo.JWTIssuer != nil || len(newInfo.JWTRoles) > 0 || len(newInfo.JWTChannels) > 0 {
		return base.HTTPErrorf(http.StatusBadRequest, "Can't change read-only properties")
	}

	internalName := internalUserName(*newInfo.Name)
	if err = auth.ValidatePrincipalName(internalName); err != nil {
		return base.HTTPErrorf(http.StatusBadRequest, err.Error())
	}

	newInfo.Name = &internalName
	replaced, err := h.db.UpdatePrincipal(h.ctx(), &newInfo, isUser, h.rq.Method != "POST")
	if err != nil {
		return err
	} else if replaced {
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

	user, err := h.db.Authenticator(h.ctx()).GetUser(username)
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	return h.db.Authenticator(h.ctx()).DeleteUser(user)
}

func (h *handler) deleteRole() error {
	h.assertAdminOnly()
	purge := h.getBoolQuery("purge")
	return h.db.DeleteRole(h.ctx(), mux.Vars(h.rq)["name"], purge)

}

func (h *handler) getUserInfo() error {
	h.assertAdminOnly()
	user, err := h.db.Authenticator(h.ctx()).GetUser(internalUserName(mux.Vars(h.rq)["name"]))
	if user == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	// If not specified will default to false
	includeDynamicGrantInfo := h.permissionsResults[PermReadPrincipalAppData.PermissionName]
	info := marshalPrincipal(h.db, user, includeDynamicGrantInfo)
	// If the user's OIDC issuer is no longer valid, remove the OIDC information to avoid confusing users
	// (it'll get removed permanently the next time the user signs in)
	if info.JWTIssuer != nil {
		issuerValid := false
		for _, provider := range h.db.OIDCProviders {
			if provider.Issuer == *info.JWTIssuer {
				issuerValid = true
				break
			}
		}
		if !issuerValid {
			info.JWTIssuer = nil
			info.JWTLastUpdated = nil
			info.JWTRoles = nil
			info.JWTChannels = nil
		}
	}
	bytes, err := base.JSONMarshal(info)
	h.writeRawJSON(bytes)
	return err
}

func (h *handler) getRoleInfo() error {
	h.assertAdminOnly()
	role, err := h.db.Authenticator(h.ctx()).GetRole(mux.Vars(h.rq)["name"])
	if role == nil {
		if err == nil {
			err = kNotFoundError
		}
		return err
	}
	// If not specified will default to false
	includeDynamicGrantInfo := h.permissionsResults[PermReadPrincipalAppData.PermissionName]
	info := marshalPrincipal(h.db, role, includeDynamicGrantInfo)
	bytes, err := base.JSONMarshal(info)
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
		var users []string
		var err error
		if h.db.Options.UseViews {
			users, _, err = h.db.AllPrincipalIDs(h.ctx())
		} else {
			users, err = h.db.GetUserNames(h.ctx())
		}
		if err != nil {
			return err
		}
		bytes, marshalErr = base.JSONMarshal(users)
	} else {
		if h.db.Options.UseViews {
			return base.HTTPErrorf(http.StatusBadRequest, fmt.Sprintf("Use of %s=false not supported when database has use_views=true", paramNameOnly))
		}
		users, err := h.db.GetUsers(h.ctx(), int(limit))
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
	includeDeleted, _ := h.getOptBoolQuery(paramDeleted, false)

	roles, err := h.db.GetRoleIDs(h.ctx(), h.db.Options.UseViews, includeDeleted)

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
		base.InfofCtx(h.ctx(), base.KeyCRUD, "purging document = %v", base.UD(key))

		if revisionList, ok := value.([]interface{}); ok {

			// There should only be a single revision entry of "*"
			if len(revisionList) != 1 {
				base.InfofCtx(h.ctx(), base.KeyCRUD, "Revision list for doc ID %v, should contain exactly one entry", base.UD(key))
				continue // skip this entry its not valid
			}

			if revisionList[0] != "*" {
				base.InfofCtx(h.ctx(), base.KeyCRUD, "Revision entry for doc ID %v, should be the '*' revison", base.UD(key))
				continue // skip this entry its not valid
			}

			// Attempt to delete document, if successful add to response, otherwise log warning
			err = h.collection.Purge(h.ctx(), key)
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
				base.InfofCtx(h.ctx(), base.KeyCRUD, "Failed to purge document %v, err = %v", base.UD(key), err)
				continue // skip this entry its not valid
			}

		} else {
			base.InfofCtx(h.ctx(), base.KeyCRUD, "Revision list for doc ID %v, is not an array, ", base.UD(key))
			continue // skip this entry its not valid
		}
	}

	if len(docIDs) > 0 {
		count := h.collection.RemoveFromChangeCache(h.ctx(), docIDs, startTime)
		base.DebugfCtx(h.ctx(), base.KeyCache, "Purged %d items from caches", count)
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
		replication.ReplicationConfig = *replication.Redacted(h.ctx())
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

	h.writeJSON(replication.Redacted(h.ctx()))
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

	created, err := h.db.SGReplicateMgr.UpsertReplication(h.ctx(), replicationConfig)
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
	replicationsStatus, err := h.db.SGReplicateMgr.GetReplicationStatusAll(h.ctx(), h.getReplicationStatusOptions())
	if err != nil {
		return err
	}
	h.writeJSON(replicationsStatus)
	return nil
}

func (h *handler) getReplicationStatus() error {
	replicationID := mux.Vars(h.rq)["replicationID"]
	status, err := h.db.SGReplicateMgr.GetReplicationStatus(h.ctx(), replicationID, h.getReplicationStatusOptions())
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

	updatedStatus, err := h.db.SGReplicateMgr.PutReplicationStatus(h.ctx(), replicationID, action)
	if err != nil {
		return err
	}
	h.writeJSON(updatedStatus)
	return nil
}

// Cluster information, returned by _cluster_info API request
type ClusterInfo struct {
	LegacyConfig bool                  `json:"legacy_config,omitempty"`
	Buckets      map[string]BucketInfo `json:"buckets,omitempty"`
}

type BucketInfo struct {
	Registry GatewayRegistry `json:"registry,omitempty"`
}

// Get SG cluster information.  Iterates over all buckets associated with the server, and returns cluster
// information (registry) for each
func (h *handler) handleGetClusterInfo() error {

	// If not using persistent config, returns legacy_config:true
	if h.server.persistentConfig == false {
		clusterInfo := ClusterInfo{
			LegacyConfig: true,
		}
		h.writeJSON(clusterInfo)
		return nil
	}

	clusterInfo := ClusterInfo{
		Buckets: make(map[string]BucketInfo),
	}

	bucketNames, err := h.server.GetBucketNames()
	if err != nil {
		return err
	}

	for _, bucketName := range bucketNames {
		registry, err := h.server.BootstrapContext.getGatewayRegistry(h.ctx(), bucketName)
		if err != nil {
			base.InfofCtx(h.ctx(), base.KeyAll, "Unable to retrieve registry for bucket %s during getClusterInfo: %v", base.MD(bucketName), err)
			continue
		}

		bucketInfo := BucketInfo{
			Registry: *registry,
		}
		clusterInfo.Buckets[bucketName] = bucketInfo
	}
	h.writeJSON(clusterInfo)
	return nil
}
