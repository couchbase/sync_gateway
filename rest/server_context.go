//  Copyright (c) 2013 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-couchbase"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	pkgerrors "github.com/pkg/errors"
)

// The URL that stats will be reported to if deployment_id is set in the config
const kStatsReportURL = "http://localhost:9999/stats"
const kStatsReportInterval = time.Hour
const kDefaultSlowQueryWarningThreshold = 500 // ms
const KDefaultNumShards = 16
const DefaultStatsLogFrequencySecs = 60

// Shared context of HTTP handlers: primarily a registry of databases by name. It also stores
// the configuration settings so handlers can refer to them.
// This struct is accessed from HTTP handlers running on multiple goroutines, so it needs to
// be thread-safe.
type ServerContext struct {
	config            *ServerConfig
	databases_        map[string]*db.DatabaseContext
	lock              sync.RWMutex
	statsContext      *statsContext
	HTTPClient        *http.Client
	replicator        *base.Replicator
	cpuPprofFileMutex sync.Mutex // Protect cpuPprofFile from concurrent Start and Stop CPU profiling requests
	cpuPprofFile      *os.File   // An open file descriptor holds the reference during CPU profiling
}

func (sc *ServerContext) SetCpuPprofFile(file *os.File) {
	sc.cpuPprofFileMutex.Lock()
	sc.cpuPprofFile = file
	sc.cpuPprofFileMutex.Unlock()
}

func (sc *ServerContext) CloseCpuPprofFile() {
	sc.cpuPprofFileMutex.Lock()
	if err := sc.cpuPprofFile.Close(); err != nil {
		base.Warnf("Error closing CPU profile file: %v", err)
	}
	sc.cpuPprofFile = nil
	sc.cpuPprofFileMutex.Unlock()
}

func NewServerContext(config *ServerConfig) *ServerContext {
	sc := &ServerContext{
		config:       config,
		databases_:   map[string]*db.DatabaseContext{},
		HTTPClient:   http.DefaultClient,
		replicator:   base.NewReplicator(),
		statsContext: &statsContext{},
	}
	if config.Databases == nil {
		config.Databases = DbConfigMap{}
	}

	if config.CouchbaseKeepaliveInterval != nil {
		couchbase.SetTcpKeepalive(true, *config.CouchbaseKeepaliveInterval)
	}

	slowQuery := kDefaultSlowQueryWarningThreshold
	if config.SlowQueryWarningThreshold != nil {
		slowQuery = *config.SlowQueryWarningThreshold
	}
	base.SlowQueryWarningThreshold = time.Duration(slowQuery) * time.Millisecond

	sc.startStatsLogger()

	return sc
}

// PostStartup runs anything that relies on SG being fully started (i.e. sgreplicate)
func (sc *ServerContext) PostStartup() {
	// Introduce a minor delay if there are any replications
	// (sc.startReplicators() might rely on SG being fully started)
	if len(sc.config.Replications) > 0 {
		time.Sleep(time.Second)
	}

	sc.startReplicators()

}

// startReplicators will start up any replicators for the ServerContext
func (sc *ServerContext) startReplicators() {

	for _, replicationConfig := range sc.config.Replications {

		params, _, _, err := validateReplicationParameters(*replicationConfig, true, *sc.config.AdminInterface)
		if err != nil {
			base.Errorf("Error validating replication parameters: %v", err)
			continue
		}

		// Force one-shot replications to run Async
		// to avoid blocking server startup
		params.Async = true

		// Run single replication, cancel parameter will always be false
		if _, err := sc.replicator.Replicate(params, false); err != nil {
			base.Warnf("Error starting replication %v: %v", base.UD(params.ReplicationId), err)
		}

	}

}

func (sc *ServerContext) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if err := sc.replicator.StopReplications(); err != nil {
		base.Warnf("Error stopping replications: %+v.  This could cause a resource leak.  Please restart Sync Gateway to cleanup leaked resources.", err)
	}

	sc.stopStatsLogger()

	for _, ctx := range sc.databases_ {
		ctx.Close()
		if ctx.EventMgr.HasHandlerForEvent(db.DBStateChange) {
			_ = ctx.EventMgr.RaiseDBStateChangeEvent(ctx.Name, "offline", "Database context closed", *sc.config.AdminInterface)
		}
	}

	sc.databases_ = nil

}

// Returns the DatabaseContext with the given name
func (sc *ServerContext) GetDatabase(name string) (*db.DatabaseContext, error) {
	sc.lock.RLock()
	dbc := sc.databases_[name]
	sc.lock.RUnlock()
	if dbc != nil {
		return dbc, nil
	} else if db.ValidateDatabaseName(name) != nil {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "invalid database name %q", name)
	} else if sc.config.ConfigServer == nil {
		return nil, base.HTTPErrorf(http.StatusNotFound, "no such database %q", name)
	} else {
		// Let's ask the config server if it knows this database:
		base.Infof(base.KeyAll, "Asking config server %q about db %q...", base.UD(*sc.config.ConfigServer), base.UD(name))
		config, err := sc.getDbConfigFromServer(name)
		if err != nil {
			return nil, err
		}
		if dbc, err = sc.getOrAddDatabaseFromConfig(config, true); err != nil {
			return nil, err
		}
	}
	return dbc, nil

}

func (sc *ServerContext) GetDatabaseConfig(name string) *DbConfig {
	sc.lock.RLock()
	config := sc.config.Databases[name]
	sc.lock.RUnlock()
	return config
}

func (sc *ServerContext) GetConfig() *ServerConfig {
	return sc.config
}

func (sc *ServerContext) AllDatabaseNames() []string {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	names := make([]string, 0, len(sc.databases_))
	for name := range sc.databases_ {
		names = append(names, name)
	}
	return names
}

// AllDatabases returns a copy of the databases_ map.
func (sc *ServerContext) AllDatabases() map[string]*db.DatabaseContext {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	databases := make(map[string]*db.DatabaseContext, len(sc.databases_))

	for name, database := range sc.databases_ {
		databases[name] = database
	}
	return databases
}

type PostUpgradeResult map[string]PostUpgradeDatabaseResult

type PostUpgradeDatabaseResult struct {
	RemovedDDocs   []string `json:"removed_design_docs"`
	RemovedIndexes []string `json:"removed_indexes"`
}

// PostUpgrade performs post-upgrade processing for each database
func (sc *ServerContext) PostUpgrade(preview bool) (postUpgradeResults PostUpgradeResult, err error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	postUpgradeResults = make(map[string]PostUpgradeDatabaseResult, len(sc.databases_))

	for name, database := range sc.databases_ {
		// View cleanup
		removedDDocs, _ := database.RemoveObsoleteDesignDocs(preview)

		// Index cleanup
		removedIndexes, _ := database.RemoveObsoleteIndexes(preview)

		postUpgradeResults[name] = PostUpgradeDatabaseResult{
			RemovedDDocs:   removedDDocs,
			RemovedIndexes: removedIndexes,
		}
	}
	return postUpgradeResults, nil
}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) ReloadDatabaseFromConfig(reloadDbName string) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()

	sc._removeDatabase(reloadDbName)

	config := sc.config.Databases[reloadDbName]

	dbContext, err := sc._getOrAddDatabaseFromConfig(config, true)
	sc.lock.Unlock()

	return dbContext, err
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) getOrAddDatabaseFromConfig(config *DbConfig, useExisting bool) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	dbContext, err := sc._getOrAddDatabaseFromConfig(config, useExisting)
	sc.lock.Unlock()

	return dbContext, err
}

func GetBucketSpec(config *DbConfig) (spec base.BucketSpec, err error) {

	spec = config.MakeBucketSpec()

	if spec.BucketName == "" {
		spec.BucketName = config.Name
	}

	spec.FeedType = strings.ToLower(config.FeedType)

	spec.CouchbaseDriver = base.ChooseCouchbaseDriver(base.DataBucket)

	if config.ViewQueryTimeoutSecs != nil {
		spec.ViewQueryTimeoutSecs = config.ViewQueryTimeoutSecs
	}

	spec.UseXattrs = config.UseXattrs()

	if config.BucketOpTimeoutMs != nil {
		operationTimeout := time.Millisecond * time.Duration(*config.BucketOpTimeoutMs)
		spec.BucketOpTimeout = &operationTimeout
	}
	return spec, nil
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) _getOrAddDatabaseFromConfig(config *DbConfig, useExisting bool) (context *db.DatabaseContext, err error) {

	oldRevExpirySeconds := base.DefaultOldRevExpirySeconds

	// Connect to the bucket and add the database:
	spec, err := GetBucketSpec(config)
	if err != nil {
		return nil, err
	}

	dbName := config.Name
	if dbName == "" {
		dbName = spec.BucketName
	}

	if config.OldRevExpirySeconds != nil {
		oldRevExpirySeconds = *config.OldRevExpirySeconds
	}

	localDocExpirySecs := base.DefaultLocalDocExpirySecs
	if config.LocalDocExpirySecs != nil {
		localDocExpirySecs = *config.LocalDocExpirySecs
	}

	if sc.databases_[dbName] != nil {
		if useExisting {
			return sc.databases_[dbName], nil
		} else {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
				"Duplicate database name %q", dbName)
		}
	}

	base.Infof(base.KeyAll, "Opening db /%s as bucket %q, pool %q, server <%s>",
		base.MD(dbName), base.MD(spec.BucketName), base.SD(spec.PoolName), base.SD(spec.Server))

	if err := db.ValidateDatabaseName(dbName); err != nil {
		return nil, err
	}

	autoImport, err := config.AutoImportEnabled()
	if err != nil {
		return nil, err
	}

	importOptions := db.ImportOptions{}
	if config.ImportFilter != nil {
		importOptions.ImportFilter = db.NewImportFilterFunction(*config.ImportFilter)
	}
	importOptions.BackupOldRev = config.ImportBackupOldRev

	if config.ImportPartitions == nil {
		importOptions.ImportPartitions = base.DefaultImportPartitions
	} else {
		importOptions.ImportPartitions = *config.ImportPartitions
	}

	// Check for deprecated cache options. If new are set they will take priority but will still log warnings
	warnings := config.deprecatedConfigCacheFallback()
	for _, warnLog := range warnings {
		base.Warnf(warnLog)
	}

	// Set cache properties, if present
	cacheOptions := db.DefaultCacheOptions()
	revCacheOptions := db.DefaultRevisionCacheOptions()
	if config.CacheConfig != nil {
		if config.CacheConfig.ChannelCacheConfig != nil {
			if config.CacheConfig.ChannelCacheConfig.MaxNumPending != nil {
				cacheOptions.CachePendingSeqMaxNum = *config.CacheConfig.ChannelCacheConfig.MaxNumPending
			}
			if config.CacheConfig.ChannelCacheConfig.MaxWaitPending != nil {
				cacheOptions.CachePendingSeqMaxWait = time.Duration(*config.CacheConfig.ChannelCacheConfig.MaxWaitPending) * time.Millisecond
			}
			if config.CacheConfig.ChannelCacheConfig.MaxWaitSkipped != nil {
				cacheOptions.CacheSkippedSeqMaxWait = time.Duration(*config.CacheConfig.ChannelCacheConfig.MaxWaitSkipped) * time.Millisecond
			}
			// set EnableStarChannelLog directly here (instead of via NewDatabaseContext), so that it's set when we create the channels view in ConnectToBucket
			if config.CacheConfig.ChannelCacheConfig.EnableStarChannel != nil {
				db.EnableStarChannelLog = *config.CacheConfig.ChannelCacheConfig.EnableStarChannel
			}
			if config.CacheConfig.ChannelCacheConfig.MaxLength != nil {
				cacheOptions.ChannelCacheMaxLength = *config.CacheConfig.ChannelCacheConfig.MaxLength
			}
			if config.CacheConfig.ChannelCacheConfig.MinLength != nil {
				cacheOptions.ChannelCacheMinLength = *config.CacheConfig.ChannelCacheConfig.MinLength
			}
			if config.CacheConfig.ChannelCacheConfig.ExpirySeconds != nil {
				cacheOptions.ChannelCacheAge = time.Duration(*config.CacheConfig.ChannelCacheConfig.ExpirySeconds) * time.Second
			}
			if config.CacheConfig.ChannelCacheConfig.MaxNumber != nil {
				cacheOptions.MaxNumChannels = *config.CacheConfig.ChannelCacheConfig.MaxNumber
			}
			if config.CacheConfig.ChannelCacheConfig.HighWatermarkPercent != nil && *config.CacheConfig.ChannelCacheConfig.HighWatermarkPercent > 0 {
				cacheOptions.CompactHighWatermarkPercent = *config.CacheConfig.ChannelCacheConfig.HighWatermarkPercent
			}
			if config.CacheConfig.ChannelCacheConfig.HighWatermarkPercent != nil && *config.CacheConfig.ChannelCacheConfig.HighWatermarkPercent > 0 {
				cacheOptions.CompactLowWatermarkPercent = *config.CacheConfig.ChannelCacheConfig.HighWatermarkPercent
			}
		}

		if config.CacheConfig.RevCacheConfig != nil {
			if config.CacheConfig.RevCacheConfig.Size != nil {
				revCacheOptions.Size = *config.CacheConfig.RevCacheConfig.Size
			}
			if config.CacheConfig.RevCacheConfig.ShardCount != nil {
				revCacheOptions.ShardCount = *config.CacheConfig.RevCacheConfig.ShardCount
			}
		}
	}

	bucket, err := db.ConnectToBucket(spec)
	if err != nil {
		return nil, err
	}

	// If using a walrus bucket, force use of views
	useViews := config.UseViews
	if !useViews && spec.IsWalrusBucket() {
		base.Warnf("Using GSI is not supported when using a walrus bucket - switching to use views.  Set 'use_views':true in Sync Gateway's database config to avoid this warning.")
		useViews = true
	}

	// Initialize Views or GSI indexes
	if !useViews {

		gsiSupported := bucket.IsSupported(sgbucket.BucketFeatureN1ql)

		if !gsiSupported {
			return nil, errors.New("Sync Gateway was unable to connect to a query node on the provided Couchbase Server cluster.  Ensure a query node is accessible, or set 'use_views':true in Sync Gateway's database config.")
		}

		numReplicas := DefaultNumIndexReplicas
		if config.NumIndexReplicas != nil {
			numReplicas = *config.NumIndexReplicas
		}

		indexErr := db.InitializeIndexes(bucket, config.UseXattrs(), numReplicas, false)
		if indexErr != nil {
			return nil, indexErr
		}
	} else {
		viewErr := db.InitializeViews(bucket)
		if viewErr != nil {
			return nil, viewErr
		}
	}

	// Create a callback function that will be invoked if the database goes offline and comes
	// back online again
	dbOnlineCallback := func(dbContext *db.DatabaseContext) {
		sc.TakeDbOnline(dbContext)
	}

	deltaSyncOptions := db.DeltaSyncOptions{
		Enabled:          db.DefaultDeltaSyncEnabled,
		RevMaxAgeSeconds: db.DefaultDeltaSyncRevMaxAge,
	}

	if config.DeltaSync != nil {
		if enable := config.DeltaSync.Enabled; enable != nil {
			deltaSyncOptions.Enabled = *enable
		}

		if revMaxAge := config.DeltaSync.RevMaxAgeSeconds; revMaxAge != nil {
			if *revMaxAge == 0 {
				// a setting of zero will fall back to the non-delta handling of revision body backups
			} else if *revMaxAge < oldRevExpirySeconds {
				return nil, fmt.Errorf("delta_sync.rev_max_age_seconds: %d must not be less than the configured old_rev_expiry_seconds: %d", *revMaxAge, oldRevExpirySeconds)
			}
			deltaSyncOptions.RevMaxAgeSeconds = *revMaxAge
		}
	}
	base.Infof(base.KeyAll, "delta_sync enabled=%t with rev_max_age_seconds=%d for database %s", deltaSyncOptions.Enabled, deltaSyncOptions.RevMaxAgeSeconds, dbName)

	if config.Unsupported.WarningThresholds.XattrSize == nil {
		val := uint32(base.DefaultWarnThresholdXattrSize)
		config.Unsupported.WarningThresholds.XattrSize = &val
	} else {
		lowerLimit := 0.1 * 1024 * 1024 // 0.1 MB
		upperLimit := 1 * 1024 * 1024   // 1 MB
		if *config.Unsupported.WarningThresholds.XattrSize < uint32(lowerLimit) {
			return nil, fmt.Errorf("xattr_size warning threshold cannot be lower than %d bytes", uint32(lowerLimit))
		} else if *config.Unsupported.WarningThresholds.XattrSize > uint32(upperLimit) {
			return nil, fmt.Errorf("xattr_size warning threshold cannot be higher than %d bytes", uint32(upperLimit))
		}
	}

	if config.Unsupported.WarningThresholds.ChannelsPerDoc == nil {
		config.Unsupported.WarningThresholds.ChannelsPerDoc = &base.DefaultWarnThresholdChannelsPerDoc
	} else {
		lowerLimit := 5
		if *config.Unsupported.WarningThresholds.ChannelsPerDoc < uint32(lowerLimit) {
			return nil, fmt.Errorf("channels_per_doc warning threshold cannot be lower than %d", lowerLimit)
		}
	}

	if config.Unsupported.WarningThresholds.GrantsPerDoc == nil {
		config.Unsupported.WarningThresholds.GrantsPerDoc = &base.DefaultWarnThresholdGrantsPerDoc
	} else {
		lowerLimit := 5
		if *config.Unsupported.WarningThresholds.GrantsPerDoc < uint32(lowerLimit) {
			return nil, fmt.Errorf("access_and_role_grants_per_doc warning threshold cannot be lower than %d", lowerLimit)
		}
	}

	compactIntervalDays := config.CompactIntervalDays
	var compactIntervalSecs uint32
	if compactIntervalDays == nil {
		compactIntervalSecs = db.DefaultCompactInterval
	} else {
		compactIntervalSecs = uint32(*compactIntervalDays * 60 * 60 * 24)
	}

	var secureCookieOverride bool
	if config.SecureCookieOverride != nil {
		secureCookieOverride = *config.SecureCookieOverride
	} else {
		secureCookieOverride = sc.config.SSLCert != nil
	}

	contextOptions := db.DatabaseContextOptions{
		CacheOptions:              &cacheOptions,
		RevisionCacheOptions:      revCacheOptions,
		OldRevExpirySeconds:       oldRevExpirySeconds,
		LocalDocExpirySecs:        localDocExpirySecs,
		AdminInterface:            sc.config.AdminInterface,
		UnsupportedOptions:        config.Unsupported,
		OIDCOptions:               config.OIDCConfig,
		DBOnlineCallback:          dbOnlineCallback,
		ImportOptions:             importOptions,
		EnableXattr:               config.UseXattrs(),
		SecureCookieOverride:      secureCookieOverride,
		SessionCookieName:         config.SessionCookieName,
		SessionCookieHttpOnly:     config.SessionCookieHTTPOnly,
		AllowConflicts:            config.ConflictsAllowed(),
		SendWWWAuthenticateHeader: config.SendWWWAuthenticateHeader,
		UseViews:                  useViews,
		DeltaSyncOptions:          deltaSyncOptions,
		CompactInterval:           compactIntervalSecs,
	}

	// Create the DB Context
	dbcontext, err := db.NewDatabaseContext(dbName, bucket, autoImport, contextOptions)
	if err != nil {
		return nil, err
	}
	dbcontext.BucketSpec = spec

	syncFn := ""
	if config.Sync != nil {
		syncFn = *config.Sync
	}
	if err := sc.applySyncFunction(dbcontext, syncFn); err != nil {
		return nil, err
	}

	if config.RevsLimit != nil {
		dbcontext.RevsLimit = *config.RevsLimit
		if dbcontext.AllowConflicts() {
			if dbcontext.RevsLimit < 20 {
				return nil, fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration cannot be set lower than 20.", dbcontext.RevsLimit)
			}

			if dbcontext.RevsLimit < db.DefaultRevsLimitConflicts {
				base.Warnf("Setting the revs_limit (%v) to less than %d, whilst having allow_conflicts set to true, may have unwanted results when documents are frequently updated. Please see documentation for details.", dbcontext.RevsLimit, db.DefaultRevsLimitConflicts)
			}
		} else {
			if dbcontext.RevsLimit <= 0 {
				return nil, fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration must be greater than zero.", dbcontext.RevsLimit)
			}
		}
	}

	dbcontext.AllowEmptyPassword = config.AllowEmptyPassword

	if dbcontext.ChannelMapper == nil {
		base.Infof(base.KeyAll, "Using default sync function 'channel(doc.channels)' for database %q", base.MD(dbName))
	}

	// Create default users & roles:
	if err := sc.installPrincipals(dbcontext, config.Roles, "role"); err != nil {
		return nil, err
	} else if err := sc.installPrincipals(dbcontext, config.Users, "user"); err != nil {
		return nil, err
	}

	// Note: disabling access-related warnings, because they potentially block startup during view reindexing trying to query the principals view, which outweighs the usability benefit
	//emitAccessRelatedWarnings(config, dbcontext)

	// Initialize event handlers
	if err := sc.initEventHandlers(dbcontext, config); err != nil {
		return nil, err
	}

	dbcontext.ExitChanges = make(chan struct{})

	// Register it so HTTP handlers can find it:
	sc.databases_[dbcontext.Name] = dbcontext

	// Save the config
	sc.config.Databases[dbName] = config

	if config.StartOffline {
		atomic.StoreUint32(&dbcontext.State, db.DBOffline)
		if dbcontext.EventMgr.HasHandlerForEvent(db.DBStateChange) {
			_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "offline", "DB loaded from config", *sc.config.AdminInterface)
		}
	} else {
		atomic.StoreUint32(&dbcontext.State, db.DBOnline)
		if dbcontext.EventMgr.HasHandlerForEvent(db.DBStateChange) {
			_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "online", "DB loaded from config", *sc.config.AdminInterface)
		}
	}

	return dbcontext, nil
}

func (sc *ServerContext) TakeDbOnline(database *db.DatabaseContext) {

	//Take a write lock on the Database context, so that we can cycle the underlying Database
	// without any other call running concurrently
	database.AccessLock.Lock()
	defer database.AccessLock.Unlock()

	//We can only transition to Online from Offline state
	if atomic.CompareAndSwapUint32(&database.State, db.DBOffline, db.DBStarting) {
		reloadedDb, err := sc.ReloadDatabaseFromConfig(database.Name)
		if err != nil {
			base.Errorf("Error reloading database from config: %v", err)
			return
		}

		// Reloaded DB should already be online in most cases, but force state to online to handle cases
		// where config specifies offline startup
		atomic.StoreUint32(&reloadedDb.State, db.DBOnline)

	} else {
		base.Infof(base.KeyCRUD, "Unable to take Database : %v online , database must be in Offline state", base.UD(database.Name))
	}

}

// Initialize event handlers, if present
func (sc *ServerContext) initEventHandlers(dbcontext *db.DatabaseContext, config *DbConfig) error {
	if config.EventHandlers != nil {

		// Temporary solution to do validation of invalid event types in config.EventHandlers.
		// config.EventHandlers is originally unmarshalled as interface{} so that we retain any
		// invalid keys during the original config unmarshalling.  We validate the expected entries
		// manually and throw an error for any invalid keys.  Then remarshal and
		// unmarshal as EventHandlerConfig (considered manual reflection, but was too painful).  Comes with
		// some overhead, but will only happen on startup/new config.
		// Should be replaced when we implement full schema validation on config.

		eventHandlers := &EventHandlerConfig{}
		eventHandlersMap, ok := config.EventHandlers.(map[string]interface{})
		if !ok {
			return errors.New(fmt.Sprintf("Unable to parse event_handlers definition in config for db %s", dbcontext.Name))
		}

		// validate event-related keys
		for k := range eventHandlersMap {
			if k != "max_processes" && k != "wait_for_process" && k != "document_changed" && k != "db_state_changed" {
				return errors.New(fmt.Sprintf("Unsupported event property '%s' defined for db %s", k, dbcontext.Name))
			}
		}

		eventHandlersJSON, err := base.JSONMarshal(eventHandlersMap)
		if err != nil {
			return pkgerrors.Wrapf(err, "Error calling base.JSONMarshal() in initEventHandlers")
		}
		if err := base.JSONUnmarshal(eventHandlersJSON, eventHandlers); err != nil {
			return pkgerrors.Wrapf(err, "Error calling base.JSONUnmarshal() in initEventHandlers")
		}

		// Process document commit event handlers
		if err = sc.processEventHandlersForEvent(eventHandlers.DocumentChanged, db.DocumentChange, dbcontext); err != nil {
			return err
		}

		// Process db state change event handlers
		if err = sc.processEventHandlersForEvent(eventHandlers.DBStateChanged, db.DBStateChange, dbcontext); err != nil {
			return err
		}
		// WaitForProcess uses string, to support both omitempty and zero values
		customWaitTime := int64(-1)
		if eventHandlers.WaitForProcess != "" {
			customWaitTime, err = strconv.ParseInt(eventHandlers.WaitForProcess, 10, 0)
			if err != nil {
				customWaitTime = -1
				base.Warnf("Error parsing wait_for_process from config, using default %s", err)
			}
		}
		dbcontext.EventMgr.Start(eventHandlers.MaxEventProc, int(customWaitTime))

	}
	return nil
}

// Adds a database to the ServerContext given its configuration.  If an existing config is found
// for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfig(config *DbConfig) (*db.DatabaseContext, error) {
	return sc.getOrAddDatabaseFromConfig(config, false)
}

func (sc *ServerContext) processEventHandlersForEvent(events []*EventConfig, eventType db.EventType, dbcontext *db.DatabaseContext) error {

	for _, event := range events {
		switch event.HandlerType {
		case "webhook":
			wh, err := db.NewWebhook(event.Url, event.Filter, event.Timeout)
			if err != nil {
				base.Warnf("Error creating webhook %v", err)
				return err
			}
			dbcontext.EventMgr.RegisterEventHandler(wh, eventType)
		default:
			return errors.New(fmt.Sprintf("Unknown event handler type %s", event.HandlerType))
		}

	}
	return nil
}

func (sc *ServerContext) applySyncFunction(dbcontext *db.DatabaseContext, syncFn string) error {
	changed, err := dbcontext.UpdateSyncFun(syncFn)
	if err != nil || !changed {
		return err
	}
	// Sync function has changed:
	base.Infof(base.KeyAll, "**NOTE:** %q's sync function has changed. The new function may assign different channels to documents, or permissions to users. You may want to re-sync the database to update these.", base.MD(dbcontext.Name))
	return nil
}

func (sc *ServerContext) RemoveDatabase(dbName string) bool {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	return sc._removeDatabase(dbName)
}

func (sc *ServerContext) _removeDatabase(dbName string) bool {

	context := sc.databases_[dbName]
	if context == nil {
		return false
	}
	base.Infof(base.KeyAll, "Closing db /%s (bucket %q)", base.MD(context.Name), base.MD(context.Bucket.GetName()))
	context.Close()
	delete(sc.databases_, dbName)
	return true
}

func (sc *ServerContext) installPrincipals(context *db.DatabaseContext, spec map[string]*db.PrincipalConfig, what string) error {
	for name, princ := range spec {
		isGuest := name == base.GuestUsername
		if isGuest {
			internalName := ""
			princ.Name = &internalName
		} else {
			n := name
			princ.Name = &n
		}

		createdPrincipal := true
		worker := func() (shouldRetry bool, err error, value interface{}) {
			_, err = context.UpdatePrincipal(*princ, (what == "user"), isGuest)
			if err != nil {
				if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusConflict {
					// Ignore and absorb this error if it's a conflict error, which just means that updatePrincipal didn't overwrite an existing user.
					// Since if there's an existing user it's "mission accomplished", this can be treated as a success case.
					createdPrincipal = false
					return false, nil, nil
				}

				if err == base.ErrViewTimeoutError {
					// Timeout error, possibly due to view re-indexing, so retry
					base.Infof(base.KeyAuth, "Error calling UpdatePrincipal(): %v.  Will retry in case this is a temporary error", err)
					return true, err, nil
				}

				// Unexpected error, return error don't retry
				return false, err, nil
			}

			// No errors, assume it worked
			return false, nil, nil

		}

		err, _ := base.RetryLoop("installPrincipals", worker, base.CreateDoublingSleeperFunc(16, 10))
		if err != nil {
			return err
		}

		if isGuest {
			base.Infof(base.KeyAll, "Reset guest user to config")
		} else if createdPrincipal {
			base.Infof(base.KeyAll, "Created %s %q", what, base.UD(name))
		}

	}
	return nil
}

// Fetch a configuration for a database from the ConfigServer
func (sc *ServerContext) getDbConfigFromServer(dbName string) (*DbConfig, error) {
	if sc.config.ConfigServer == nil {
		return nil, base.HTTPErrorf(http.StatusNotFound, "not_found")
	}

	urlStr := *sc.config.ConfigServer
	if !strings.HasSuffix(urlStr, "/") {
		urlStr += "/"
	}
	urlStr += url.QueryEscape(dbName)
	resp, err := sc.HTTPClient.Get(urlStr)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			"Error contacting config server: %v", err)
	} else if resp.StatusCode >= 300 {
		return nil, base.HTTPErrorf(resp.StatusCode, http.StatusText(resp.StatusCode))
	}

	var config DbConfig
	defer func() { _ = resp.Body.Close() }()

	if err := decodeAndSanitiseConfig(resp.Body, &config); err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			"Bad response from config server: %v", err)
	}

	if err = config.setup(dbName); err != nil {
		return nil, err
	}
	return &config, nil
}

//////// STATS LOGGING

type statsWrapper struct {
	Stats              json.RawMessage `json:"stats"`
	UnixEpochTimestamp int64           `json:"unix_epoch_timestamp"`
	RFC3339            string          `json:"rfc3339_timestamp"`
}

func (sc *ServerContext) startStatsLogger() {

	statsLogFrequencySecs := uint(DefaultStatsLogFrequencySecs)
	if sc.config.Unsupported != nil && sc.config.Unsupported.StatsLogFrequencySecs != nil {
		if *sc.config.Unsupported.StatsLogFrequencySecs == 0 {
			// don't start the stats logger when explicitly zero
			return
		} else if *sc.config.Unsupported.StatsLogFrequencySecs > 0 {
			statsLogFrequencySecs = *sc.config.Unsupported.StatsLogFrequencySecs
		}
	}

	interval := time.Second * time.Duration(statsLogFrequencySecs)

	sc.statsContext.statsLoggingTicker = time.NewTicker(interval)
	go func() {
		for range sc.statsContext.statsLoggingTicker.C {
			err := sc.logStats()
			if err != nil {
				base.Warnf("Error logging stats: %v", err)
			}
		}
	}()
	base.Infof(base.KeyAll, "Logging stats with frequency: %v", interval)

}

func (sc *ServerContext) stopStatsLogger() {
	if sc.statsContext.statsLoggingTicker != nil {
		sc.statsContext.statsLoggingTicker.Stop()
	}
}

func (sc *ServerContext) logStats() error {

	AddGoRuntimeStats()

	sc.logNetworkInterfaceStats()

	if err := sc.statsContext.addGoSigarStats(); err != nil {
		base.Warnf("Error getting sigar based system resource stats: %v", err)
	}

	sc.updateCalculatedStats()
	// Create wrapper expvar map in order to add a timestamp field for logging purposes
	currentTime := time.Now()
	wrapper := statsWrapper{
		Stats:              []byte(base.Stats.String()),
		UnixEpochTimestamp: currentTime.Unix(),
		RFC3339:            currentTime.Format(time.RFC3339),
	}

	marshalled, err := base.JSONMarshal(wrapper)
	if err != nil {
		return err
	}

	// Marshal expvar map w/ timestamp to string and write to logs
	base.RecordStats(string(marshalled))

	return nil

}

func (sc *ServerContext) logNetworkInterfaceStats() {

	publicListenInterface := DefaultInterface
	if sc.config.Interface != nil {
		publicListenInterface = *sc.config.Interface
	}
	if err := sc.statsContext.addPublicNetworkInterfaceStatsForHostnamePort(publicListenInterface); err != nil {
		base.Warnf("Error getting public network interface resource stats: %v", err)
	}

	adminListenInterface := DefaultAdminInterface
	if sc.config.AdminInterface != nil {
		adminListenInterface = *sc.config.AdminInterface
	}
	if err := sc.statsContext.addAdminNetworkInterfaceStatsForHostnamePort(adminListenInterface); err != nil {
		base.Warnf("Error getting admin network interface resource stats: %v", err)
	}

}

// Updates stats that are more efficient to calculate at stats collection time
func (sc *ServerContext) updateCalculatedStats() {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	for _, dbContext := range sc.databases_ {
		dbContext.UpdateCalculatedStats()
	}

}

// For test use
func (sc *ServerContext) Database(name string) *db.DatabaseContext {
	db, err := sc.GetDatabase(name)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error getting db %q: %v", name, err))
	}
	return db
}
