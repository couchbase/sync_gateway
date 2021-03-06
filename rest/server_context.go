//  Copyright 2013-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package rest

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/gocbcore"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/hashicorp/go-multierror"
)

// The URL that stats will be reported to if deployment_id is set in the config
const kStatsReportURL = "http://localhost:9999/stats"
const kStatsReportInterval = time.Hour
const kDefaultSlowQueryWarningThreshold = 500 // ms
const KDefaultNumShards = 16

// Shared context of HTTP handlers: primarily a registry of databases by name. It also stores
// the configuration settings so handlers can refer to them.
// This struct is accessed from HTTP handlers running on multiple goroutines, so it needs to
// be thread-safe.
type ServerContext struct {
	config              *StartupConfig
	persistentConfig    bool
	bucketDbName        map[string]string              // bucketDbName is a map of bucket to database name
	dbConfigs           map[string]*DatabaseConfig     // dbConfigs is a map of db name to DatabaseConfig
	databases_          map[string]*db.DatabaseContext // databases_ is a map of dbname to db.DatabaseContext
	lock                sync.RWMutex
	statsContext        *statsContext
	bootstrapConnection base.BootstrapConnection
	HTTPClient          *http.Client
	cpuPprofFileMutex   sync.Mutex     // Protect cpuPprofFile from concurrent Start and Stop CPU profiling requests
	cpuPprofFile        *os.File       // An open file descriptor holds the reference during CPU profiling
	_httpServers        []*http.Server // A list of HTTP servers running under the ServerContext
}

type DatabaseConfig struct {
	// TODO: Copy non-legacy properties into this struct?
	cas uint64
	DbConfig
}

func (dbs DbConfigMap) SetupAndValidate() error {
	for name, dbConfig := range dbs {
		if err := dbConfig.setup(name); err != nil {
			return err
		}
		if err := dbConfig.validateSgDbConfig(); err != nil {
			return err
		}
	}
	return nil
}

func (sc *ServerContext) CreateLocalDatabase(dbs DbConfigMap) error {
	for _, dbConfig := range dbs {
		dbc := DatabaseConfig{DbConfig: *dbConfig}
		_, err := sc._getOrAddDatabaseFromConfig(dbc, false)
		if err != nil {
			return err
		}
	}
	return nil
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

func NewServerContext(config *StartupConfig, persistentConfig bool) *ServerContext {
	sc := &ServerContext{
		config:           config,
		persistentConfig: persistentConfig,
		bucketDbName:     map[string]string{},
		dbConfigs:        map[string]*DatabaseConfig{},
		databases_:       map[string]*db.DatabaseContext{},
		HTTPClient:       http.DefaultClient,
		statsContext:     &statsContext{},
	}

	if base.ServerIsWalrus(sc.config.Bootstrap.Server) {
		sc.persistentConfig = false

		// Disable Admin API authentication when running as walrus on the default admin interface to support dev
		// environments.
		if sc.config.API.AdminInterface == DefaultAdminInterface {
			sc.config.API.AdminInterfaceAuthentication = base.BoolPtr(false)
			sc.config.API.MetricsInterfaceAuthentication = base.BoolPtr(false)
		}
	}

	// TODO: Remove with GoCB DCP switch
	// if config.CouchbaseKeepaliveInterval != nil {
	// 	couchbase.SetTcpKeepalive(true, *config.CouchbaseKeepaliveInterval)
	// }

	// TODO: Moved to dbConfig?
	// if config.SlowQueryWarningThreshold == nil {
	// 	config.SlowQueryWarningThreshold = base.IntPtr(kDefaultSlowQueryWarningThreshold)
	// }

	sc.startStatsLogger()

	return sc
}

// PostStartup runs anything that relies on SG being fully started (i.e. sgreplicate)
func (sc *ServerContext) PostStartup() {
	// Start sg-replicate2 replications per-database.  sg-replicate2 replications aren't
	// started until at least 5 seconds after SG node start, to avoid replication reassignment churn
	// when a Sync Gateway Cluster is being initialized
	time.Sleep(5 * time.Second)

	sc.lock.RLock()
	for _, dbContext := range sc.databases_ {
		base.Infof(base.KeyReplicate, "Starting sg-replicate replications...")
		err := dbContext.SGReplicateMgr.StartReplications()
		if err != nil {
			base.Errorf("Error starting sg-replicate replications: %v", err)
		}
	}
	sc.lock.RUnlock()

}

func (sc *ServerContext) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	sc.stopStatsLogger()

	for _, ctx := range sc.databases_ {
		ctx.Close()
		_ = ctx.EventMgr.RaiseDBStateChangeEvent(ctx.Name, "offline", "Database context closed", &sc.config.API.AdminInterface)
	}
	sc.databases_ = nil

	for _, s := range sc._httpServers {
		base.Infof(base.KeyHTTP, "Closing HTTP Server: %v", s.Addr)
		if err := s.Close(); err != nil {
			base.Warnf("Error closing HTTP server %q: %v", s.Addr, err)
		}
	}
	sc._httpServers = nil

	if sc.bootstrapConnection != nil {
		if err := sc.bootstrapConnection.Close(); err != nil {
			base.Warnf("Error closing bootstrap cluster connection: %v", err)
		}
	}

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
	}
	return nil, base.HTTPErrorf(http.StatusNotFound, "no such database %q", name)
}

func (sc *ServerContext) GetDatabaseConfig(name string) *DatabaseConfig {
	sc.lock.RLock()
	config, ok := sc.dbConfigs[name]
	sc.lock.RUnlock()
	if !ok {
		return nil
	}
	return config
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
		var removedIndexes []string
		if !base.TestsDisableGSI() {
			removedIndexes, _ = database.RemoveObsoleteIndexes(preview)
		}

		postUpgradeResults[name] = PostUpgradeDatabaseResult{
			RemovedDDocs:   removedDDocs,
			RemovedIndexes: removedIndexes,
		}
	}
	return postUpgradeResults, nil
}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) _reloadDatabaseFromConfig(reloadDbName string) (*db.DatabaseContext, error) {
	sc._removeDatabase(reloadDbName)
	config := sc.dbConfigs[reloadDbName]
	return sc._getOrAddDatabaseFromConfig(*config, true)
}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) ReloadDatabaseFromConfig(reloadDbName string) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	dbContext, err := sc._reloadDatabaseFromConfig(reloadDbName)
	sc.lock.Unlock()

	return dbContext, err
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) getOrAddDatabaseFromConfig(config DatabaseConfig, useExisting bool) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	dbContext, err := sc._getOrAddDatabaseFromConfig(config, useExisting)
	sc.lock.Unlock()

	return dbContext, err
}

func GetBucketSpec(config *DbConfig, serverConfig *StartupConfig) (spec base.BucketSpec, err error) {

	spec = config.MakeBucketSpec()

	if serverConfig.Bootstrap.ServerTLSSkipVerify != nil {
		spec.TLSSkipVerify = *serverConfig.Bootstrap.ServerTLSSkipVerify
	}

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
func (sc *ServerContext) _getOrAddDatabaseFromConfig(config DatabaseConfig, useExisting bool) (context *db.DatabaseContext, err error) {

	// Generate bucket spec and validate whether db already exists
	spec, err := GetBucketSpec(&config.DbConfig, sc.config)
	if err != nil {
		return nil, err
	}

	dbName := config.Name
	if dbName == "" {
		dbName = spec.BucketName
	}

	if sc.databases_[dbName] != nil {
		if useExisting {
			return sc.databases_[dbName], nil
		} else {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
				"Duplicate database name %q", dbName)
		}
	}

	if err := db.ValidateDatabaseName(dbName); err != nil {
		return nil, err
	}

	// Connect to bucket
	base.Infof(base.KeyAll, "Opening db /%s as bucket %q, pool %q, server <%s>",
		base.MD(dbName), base.MD(spec.BucketName), base.SD(base.DefaultPool), base.SD(spec.Server))
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

	// Initialize Views or GSI indexes for the bucket
	if !useViews {
		gsiSupported := bucket.IsSupported(sgbucket.DataStoreFeatureN1ql)
		if !gsiSupported {
			return nil, errors.New("Sync Gateway was unable to connect to a query node on the provided Couchbase Server cluster.  Ensure a query node is accessible, or set 'use_views':true in Sync Gateway's database config.")
		}

		numReplicas := DefaultNumIndexReplicas
		if config.NumIndexReplicas != nil {
			numReplicas = *config.NumIndexReplicas
		}

		indexErr := db.InitializeIndexes(bucket, config.UseXattrs(), numReplicas)
		if indexErr != nil {
			return nil, indexErr
		}
	} else {
		viewErr := db.InitializeViews(bucket)
		if viewErr != nil {
			return nil, viewErr
		}
	}

	// Process unsupported config options
	if config.Unsupported.WarningThresholds.XattrSize == nil {
		config.Unsupported.WarningThresholds.XattrSize = base.Uint32Ptr(uint32(base.DefaultWarnThresholdXattrSize))
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

	if config.Unsupported.WarningThresholds.ChannelsPerUser == nil {
		config.Unsupported.WarningThresholds.ChannelsPerUser = &base.DefaultWarnThresholdChannelsPerUser
	}

	if config.Unsupported.WarningThresholds.GrantsPerDoc == nil {
		config.Unsupported.WarningThresholds.GrantsPerDoc = &base.DefaultWarnThresholdGrantsPerDoc
	} else {
		lowerLimit := 5
		if *config.Unsupported.WarningThresholds.GrantsPerDoc < uint32(lowerLimit) {
			return nil, fmt.Errorf("access_and_role_grants_per_doc warning threshold cannot be lower than %d", lowerLimit)
		}
	}

	if config.Unsupported.WarningThresholds.ChannelNameSize == nil {
		config.Unsupported.WarningThresholds.ChannelNameSize = &base.DefaultWarnThresholdChannelNameSize
	}

	autoImport, err := config.AutoImportEnabled()
	if err != nil {
		return nil, err
	}

	// Generate database context options from config and server context
	contextOptions, err := dbcOptionsFromConfig(sc, &config.DbConfig, dbName)
	if err != nil {
		return nil, err
	}
	contextOptions.UseViews = useViews

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
	dbcontext.ServeInsecureAttachmentTypes = config.ServeInsecureAttachmentTypes

	if dbcontext.ChannelMapper == nil {
		base.Infof(base.KeyAll, "Using default sync function 'channel(doc.channels)' for database %q", base.MD(dbName))
	}

	// Create default users & roles:
	if err := sc.installPrincipals(dbcontext, config.Roles, "role"); err != nil {
		return nil, err
	} else if err := sc.installPrincipals(dbcontext, config.Users, "user"); err != nil {
		return nil, err
	}

	// Initialize event handlers
	if err := sc.initEventHandlers(dbcontext, &config.DbConfig); err != nil {
		return nil, err
	}

	// Upsert replications
	replicationErr := dbcontext.SGReplicateMgr.PutReplications(config.Replications)
	if replicationErr != nil {
		return nil, replicationErr
	}

	// Register it so HTTP handlers can find it:
	sc.databases_[dbcontext.Name] = dbcontext

	// Save the config
	sc.dbConfigs[dbcontext.Name] = &config

	if config.StartOffline {
		atomic.StoreUint32(&dbcontext.State, db.DBOffline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "offline", "DB loaded from config", &sc.config.API.AdminInterface)
	} else {
		atomic.StoreUint32(&dbcontext.State, db.DBOnline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "online", "DB loaded from config", &sc.config.API.AdminInterface)
	}

	return dbcontext, nil
}

func dbcOptionsFromConfig(sc *ServerContext, config *DbConfig, dbName string) (db.DatabaseContextOptions, error) {

	// Identify import options
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

	// Create a callback function that will be invoked if the database goes offline and comes
	// back online again
	dbOnlineCallback := func(dbContext *db.DatabaseContext) {
		sc.TakeDbOnline(dbContext)
	}

	oldRevExpirySeconds := base.DefaultOldRevExpirySeconds
	if config.OldRevExpirySeconds != nil {
		oldRevExpirySeconds = *config.OldRevExpirySeconds
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
				return db.DatabaseContextOptions{}, fmt.Errorf("delta_sync.rev_max_age_seconds: %d must not be less than the configured old_rev_expiry_seconds: %d", *revMaxAge, oldRevExpirySeconds)
			}
			deltaSyncOptions.RevMaxAgeSeconds = *revMaxAge
		}
	}
	base.Infof(base.KeyAll, "delta_sync enabled=%t with rev_max_age_seconds=%d for database %s", deltaSyncOptions.Enabled, deltaSyncOptions.RevMaxAgeSeconds, dbName)

	compactIntervalSecs := db.DefaultCompactInterval
	if config.CompactIntervalDays != nil {
		compactIntervalSecs = uint32(*config.CompactIntervalDays * 60 * 60 * 24)
	}

	var queryPaginationLimit int

	// If QueryPaginationLimit has been set use that first
	if config.QueryPaginationLimit != nil {
		queryPaginationLimit = *config.QueryPaginationLimit
	}

	// If DeprecatedQueryLimit is set we need to handle this
	if config.CacheConfig != nil && config.CacheConfig.ChannelCacheConfig != nil && config.CacheConfig.ChannelCacheConfig.DeprecatedQueryLimit != nil {
		// If QueryPaginationLimit has not been set use the deprecated option
		if queryPaginationLimit == 0 {
			base.Warnf("Using deprecated config parameter 'cache.channel_cache.query_limit'. Use 'query_pagination_limit' instead")
			queryPaginationLimit = *config.CacheConfig.ChannelCacheConfig.DeprecatedQueryLimit
		} else {
			base.Warnf("Both query_pagination_limit and the deprecated cache.channel_cache.query_limit have been specified in config - using query_pagination_limit")
		}
	}

	// If no limit has been set (or is set to 0) we will now choose the default
	if queryPaginationLimit == 0 {
		queryPaginationLimit = db.DefaultQueryPaginationLimit
	}

	if queryPaginationLimit < 2 {
		return db.DatabaseContextOptions{}, fmt.Errorf("query_pagination_limit: %d must be greater than 1", queryPaginationLimit)
	}
	cacheOptions.ChannelQueryLimit = queryPaginationLimit

	secureCookieOverride := sc.config.API.HTTPS.TLSCertPath != ""
	if config.SecureCookieOverride != nil {
		secureCookieOverride = *config.SecureCookieOverride
	}

	sgReplicateEnabled := db.DefaultSGReplicateEnabled
	if config.SGReplicateEnabled != nil {
		sgReplicateEnabled = *config.SGReplicateEnabled
	}

	sgReplicateWebsocketPingInterval := db.DefaultSGReplicateWebsocketPingInterval
	if config.SGReplicateWebsocketPingInterval != nil {
		sgReplicateWebsocketPingInterval = time.Second * time.Duration(*config.SGReplicateWebsocketPingInterval)
	}

	localDocExpirySecs := base.DefaultLocalDocExpirySecs
	if config.LocalDocExpirySecs != nil {
		localDocExpirySecs = *config.LocalDocExpirySecs
	}

	if config.UserXattrKey != "" && !config.UseXattrs() {
		return db.DatabaseContextOptions{}, fmt.Errorf("use of user_xattr_key requires shared_bucket_access to be enabled")
	}

	clientPartitionWindow := base.DefaultClientPartitionWindow
	if config.ClientPartitionWindowSecs != nil {
		clientPartitionWindow = time.Duration(*config.ClientPartitionWindowSecs) * time.Second
	}

	contextOptions := db.DatabaseContextOptions{
		CacheOptions:              &cacheOptions,
		RevisionCacheOptions:      revCacheOptions,
		OldRevExpirySeconds:       oldRevExpirySeconds,
		LocalDocExpirySecs:        localDocExpirySecs,
		AdminInterface:            &sc.config.API.AdminInterface,
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
		DeltaSyncOptions:          deltaSyncOptions,
		CompactInterval:           compactIntervalSecs,
		QueryPaginationLimit:      queryPaginationLimit,
		UserXattrKey:              config.UserXattrKey,
		SGReplicateOptions: db.SGReplicateOptions{
			Enabled:               sgReplicateEnabled,
			WebsocketPingInterval: sgReplicateWebsocketPingInterval,
		},
		// FIXME?
		// SlowQueryWarningThreshold: time.Duration(*sc.config.SlowQueryWarningThreshold) * time.Millisecond,
		ClientPartitionWindow: clientPartitionWindow,
	}

	return contextOptions, nil
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

// validateEventConfigOptions returns errors for all invalid event type options.
func validateEventConfigOptions(eventType db.EventType, eventConfig *EventConfig) error {
	if eventConfig == nil || eventConfig.Options == nil {
		return nil
	}

	var errs *multierror.Error

	switch eventType {
	case db.DocumentChange:
		for k, v := range eventConfig.Options {
			switch k {
			case db.EventOptionDocumentChangedWinningRevOnly:
				if _, ok := v.(bool); !ok {
					errs = multierror.Append(errs, fmt.Errorf("Event option %q must be of type bool", db.EventOptionDocumentChangedWinningRevOnly))
				}
			default:
				errs = multierror.Append(errs, fmt.Errorf("unknown option %q found for event type %q", k, eventType))
			}
		}
	default:
		errs = multierror.Append(errs, fmt.Errorf("unknown options %v found for event type %q", eventConfig.Options, eventType))
	}

	// If we only have 1 error, return it as-is for clarity in the logs.
	if errs != nil {
		if errs.Len() == 1 {
			return errs.Errors[0]
		}
		return errs
	}

	return nil
}

// Initialize event handlers, if present
func (sc *ServerContext) initEventHandlers(dbcontext *db.DatabaseContext, config *DbConfig) (err error) {
	if config.EventHandlers == nil {
		return nil
	}

	// Load Webhook Filter Function.
	eventHandlersByType := map[db.EventType][]*EventConfig{
		db.DocumentChange: config.EventHandlers.DocumentChanged,
		db.DBStateChange:  config.EventHandlers.DBStateChanged,
	}

	for eventType, handlers := range eventHandlersByType {
		for _, conf := range handlers {
			if err := validateEventConfigOptions(eventType, conf); err != nil {
				return err
			}

			// Load external webhook filter function
			filter, err := loadJavaScript(conf.Filter, config.Unsupported.RemoteConfigTlsSkipVerify)
			if err != nil {
				return &JavaScriptLoadError{
					JSLoadType: WebhookFilter,
					Path:       conf.Filter,
					Err:        err,
				}
			}
			conf.Filter = filter
		}

		// Register event handlers
		if err = sc.processEventHandlersForEvent(handlers, eventType, dbcontext); err != nil {
			return err
		}
	}

	// WaitForProcess uses string, to support both omitempty and zero values
	customWaitTime := int64(-1)
	if config.EventHandlers.WaitForProcess != "" {
		customWaitTime, err = strconv.ParseInt(config.EventHandlers.WaitForProcess, 10, 0)
		if err != nil {
			customWaitTime = -1
			base.Warnf("Error parsing wait_for_process from config, using default %s", err)
		}
	}
	dbcontext.EventMgr.Start(config.EventHandlers.MaxEventProc, int(customWaitTime))

	return nil
}

// Adds a database to the ServerContext given its configuration.  If an existing config is found
// for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfig(config DatabaseConfig) (*db.DatabaseContext, error) {
	return sc.getOrAddDatabaseFromConfig(config, false)
}

func (sc *ServerContext) processEventHandlersForEvent(events []*EventConfig, eventType db.EventType, dbcontext *db.DatabaseContext) error {

	for _, event := range events {
		switch event.HandlerType {
		case "webhook":
			wh, err := db.NewWebhook(event.Url, event.Filter, event.Timeout, event.Options)
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

//////// STATS LOGGING

type statsWrapper struct {
	Stats              json.RawMessage `json:"stats"`
	UnixEpochTimestamp int64           `json:"unix_epoch_timestamp"`
	RFC3339            string          `json:"rfc3339_timestamp"`
}

func (sc *ServerContext) startStatsLogger() {

	if sc.config.Unsupported.StatsLogFrequency == nil || sc.config.Unsupported.StatsLogFrequency.Duration == 0 {
		// don't start the stats logger when explicitly zero
		return
	}

	interval := sc.config.Unsupported.StatsLogFrequency

	sc.statsContext.statsLoggingTicker = time.NewTicker(interval.Duration)
	sc.statsContext.terminator = make(chan struct{})
	sc.statsContext.doneChan = make(chan struct{})
	go func() {
		defer close(sc.statsContext.doneChan)
		for {
			select {
			case <-sc.statsContext.statsLoggingTicker.C:
				err := sc.logStats()
				if err != nil {
					base.Warnf("Error logging stats: %v", err)
				}
			case <-sc.statsContext.terminator:
				base.Debugf(base.KeyAll, "Stopping stats logging goroutine")
				sc.statsContext.statsLoggingTicker.Stop()
				return
			}
		}
	}()
	base.Infof(base.KeyAll, "Logging stats with frequency: %v", interval)

}

// StatsLoggerStopMaxWait is the maximum amount of time to wait for
// stats logger goroutine to terminate before the server is stopped.
const StatsLoggerStopMaxWait = 30 * time.Second

// stopStatsLogger stops the stats logger.
func (sc *ServerContext) stopStatsLogger() {
	if sc.statsContext.terminator != nil {
		close(sc.statsContext.terminator)

		waitTime := StatsLoggerStopMaxWait
		select {
		case <-sc.statsContext.doneChan:
			// Stats logger goroutine is terminated and doneChan is already closed.
		case <-time.After(waitTime):
			base.Infof(base.KeyAll, "Timeout after %v of waiting for stats logger to terminate", waitTime)
		}
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
		Stats:              []byte(base.SyncGatewayStats.String()),
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

	if err := sc.statsContext.addPublicNetworkInterfaceStatsForHostnamePort(sc.config.API.PublicInterface); err != nil {
		base.Warnf("Error getting public network interface resource stats: %v", err)
	}

	if err := sc.statsContext.addAdminNetworkInterfaceStatsForHostnamePort(sc.config.API.AdminInterface); err != nil {
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

func initClusterAgent(clusterAddress, clusterUser, clusterPass, certPath, keyPath, caCertPath string, tlsSkipVerify *bool, timeout time.Duration) (*gocbcore.Agent, error) {
	authenticator, err := base.GoCBCoreAuthConfig(clusterUser, clusterPass, certPath, keyPath)
	if err != nil {
		return nil, err
	}

	tlsRootCAProvider, err := base.GoCBCoreTLSRootCAProvider(tlsSkipVerify, caCertPath)
	if err != nil {
		return nil, err
	}

	config := gocbcore.AgentConfig{
		SecurityConfig: gocbcore.SecurityConfig{
			TLSRootCAProvider: tlsRootCAProvider,
			Auth:              authenticator,
		},
	}

	err = config.FromConnStr(clusterAddress)
	if err != nil {
		return nil, err
	}

	agent, err := gocbcore.CreateAgent(&config)
	if err != nil {
		return nil, err
	}

	agentWaitUntilReadyTimeout := 5 * time.Second
	if timeout != 0 {
		agentWaitUntilReadyTimeout = timeout
	}

	agentReadyErr := make(chan error)
	_, err = agent.WaitUntilReady(time.Now().Add(agentWaitUntilReadyTimeout), gocbcore.WaitUntilReadyOptions{ServiceTypes: []gocbcore.ServiceType{gocbcore.MgmtService}}, func(result *gocbcore.WaitUntilReadyResult, err error) {
		agentReadyErr <- err
	})

	if err != nil {
		return nil, err
	}

	if err := <-agentReadyErr; err != nil {
		return nil, err
	}

	return agent, nil
}

func (sc *ServerContext) ObtainManagementEndpointsAndHTTPClient() ([]string, *http.Client, error) {
	agent, err := initClusterAgent(
		sc.config.Bootstrap.Server, sc.config.Bootstrap.Username, sc.config.Bootstrap.Password,
		sc.config.Bootstrap.X509CertPath, sc.config.Bootstrap.X509KeyPath, sc.config.Bootstrap.CACertPath, sc.config.Bootstrap.ServerTLSSkipVerify,
		sc.config.API.ServerReadTimeout.Value())
	if err != nil {
		return nil, nil, err
	}

	managementEndpoints := agent.MgmtEps()

	httpClient := &http.Client{
		Transport: agent.HTTPClient().Transport,
	}

	err = agent.Close()
	if err != nil {
		return nil, nil, err
	}

	return managementEndpoints, httpClient, nil
}

// CheckPermissions is used for Admin authentication to check a CBS RBAC user.
// It performs two jobs: Authentication and then attempts Authorization.
// For Authorization it checks whether the user has any ONE of the supplied accessPermissions
// If the user is authorized it will also check the responsePermissions and return the results for these. These can be
// used by handlers to determine different responses based on the permissions the user has.
func CheckPermissions(httpClient *http.Client, managementEndpoints []string, bucketName, username, password string, accessPermissions []Permission, responsePermissions []Permission) (statusCode int, permissionResults map[string]bool, err error) {
	combinedPermissions := append(accessPermissions, responsePermissions...)
	body := []byte(strings.Join(FormatPermissionNames(combinedPermissions, bucketName), ","))
	statusCode, bodyResponse, err := doHTTPAuthRequest(httpClient, username, password, "POST", "/pools/default/checkPermissions", managementEndpoints, body)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	if statusCode != http.StatusOK {
		if statusCode == http.StatusUnauthorized {
			return http.StatusUnauthorized, nil, nil
		}

		// If we don't provide permissions we get a BadRequest but know we have successfully authenticated
		if statusCode == http.StatusBadRequest && len(combinedPermissions) > 0 {
			return statusCode, nil, nil
		}
	}

	// At this point we know the user exists, now check whether they have the required permissions
	if len(combinedPermissions) > 0 {
		var permissions map[string]bool

		err = base.JSONUnmarshal(bodyResponse, &permissions)
		if err != nil {
			return http.StatusInternalServerError, nil, err
		}

		if len(responsePermissions) > 0 {
			permissionResults = make(map[string]bool)
			for _, responsePermission := range responsePermissions {
				hasPermission, ok := permissions[responsePermission.FormattedName(bucketName)]
				// This should always be true but better to be safe to avoid panic
				if ok {
					permissionResults[responsePermission.PermissionName] = hasPermission
				}
			}
		}

		for _, accessPermission := range accessPermissions {
			if hasPermission, ok := permissions[accessPermission.FormattedName(bucketName)]; ok && hasPermission {
				return http.StatusOK, permissionResults, nil
			}
		}
	}

	return http.StatusForbidden, nil, nil
}

func CheckRoles(httpClient *http.Client, managementEndpoints []string, username, password string, requestedRoles []RouteRole, bucketName string) (statusCode int, err error) {
	statusCode, bodyResponse, err := doHTTPAuthRequest(httpClient, username, password, "GET", "/whoami", managementEndpoints, nil)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	if statusCode != http.StatusOK {
		return statusCode, nil
	}

	var whoAmIResults struct {
		Roles []struct {
			RoleName   string `json:"role"`
			BucketName string `json:"bucket_name"`
		} `json:"roles"`
	}

	err = base.JSONUnmarshal(bodyResponse, &whoAmIResults)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	for _, roleResult := range whoAmIResults.Roles {
		for _, requireRole := range requestedRoles {
			requireBucket := ""
			if requireRole.DatabaseScoped {
				requireBucket = bucketName
			}

			if roleResult.BucketName == requireBucket && roleResult.RoleName == requireRole.RoleName {
				return http.StatusOK, nil
			}
		}
	}

	return http.StatusForbidden, nil
}

func doHTTPAuthRequest(httpClient *http.Client, username, password, method, path string, endpoints []string, requestBody []byte) (statusCode int, responseBody []byte, err error) {
	retryCount := 0

	worker := func() (shouldRetry bool, err error, value interface{}) {
		var httpResponse *http.Response

		endpointIdx := retryCount % len(endpoints)
		req, err := http.NewRequest(method, endpoints[endpointIdx]+path, bytes.NewBuffer(requestBody))
		if err != nil {
			return false, err, nil
		}

		req.SetBasicAuth(username, password)

		httpResponse, err = httpClient.Do(req)
		if err == nil {
			return false, nil, httpResponse
		}

		if err, ok := err.(net.Error); ok && err.Timeout() {
			retryCount++
			return true, err, nil
		}

		return false, err, nil
	}

	err, result := base.RetryLoop("", worker, base.CreateSleeperFunc(10, 100))
	if err != nil {
		return 0, nil, err
	}

	httpResponse, ok := result.(*http.Response)
	if !ok {
		return 0, nil, fmt.Errorf("unexpected response type from doHTTPAuthRequest")
	}

	bodyString, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		return 0, nil, err
	}

	err = httpResponse.Body.Close()
	if err != nil {
		return 0, nil, err
	}

	return httpResponse.StatusCode, bodyString, nil
}

// For test use
func (sc *ServerContext) Database(name string) *db.DatabaseContext {
	db, err := sc.GetDatabase(name)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error getting db %q: %v", name, err))
	}
	return db
}
