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
	"context"
	"crypto/tls"
	"crypto/x509"
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

	"github.com/couchbase/sync_gateway/auth"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
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
	config               *StartupConfig // The current runtime configuration of the node
	initialStartupConfig *StartupConfig // The configuration at startup of the node. Built from config file + flags
	persistentConfig     bool
	bucketDbName         map[string]string              // bucketDbName is a map of bucket to database name
	dbConfigs            map[string]*DatabaseConfig     // dbConfigs is a map of db name to DatabaseConfig
	databases_           map[string]*db.DatabaseContext // databases_ is a map of dbname to db.DatabaseContext
	lock                 sync.RWMutex
	statsContext         *statsContext
	bootstrapContext     *bootstrapContext
	HTTPClient           *http.Client
	cpuPprofFileMutex    sync.Mutex      // Protect cpuPprofFile from concurrent Start and Stop CPU profiling requests
	cpuPprofFile         *os.File        // An open file descriptor holds the reference during CPU profiling
	_httpServers         []*http.Server  // A list of HTTP servers running under the ServerContext
	GoCBAgent            *gocbcore.Agent // GoCB Agent to use when obtaining management endpoints
	NoX509HTTPClient     *http.Client    // httpClient for the cluster that doesn't include x509 credentials, even if they are configured for the cluster
	hasStarted           chan struct{}   // A channel that is closed via PostStartup once the ServerContext has fully started
}

type bootstrapContext struct {
	connection base.BootstrapConnection
	terminator chan struct{} // Used to stop the goroutine handling the stats logging
	doneChan   chan struct{} // doneChan is closed when the stats logger goroutine finishes.
}

func (sc *ServerContext) CreateLocalDatabase(dbs DbConfigMap) error {
	for _, dbConfig := range dbs {
		dbc := dbConfig.ToDatabaseConfig()
		_, err := sc._getOrAddDatabaseFromConfig(*dbc, false, false)
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
		base.WarnfCtx(context.TODO(), "Error closing CPU profile file: %v", err)
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
		bootstrapContext: &bootstrapContext{},
		hasStarted:       make(chan struct{}),
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

	sc.startStatsLogger()

	return sc
}

func (sc *ServerContext) waitForRESTAPIs() error {
	timeout := 30 * time.Second
	interval := time.Millisecond * 100
	numAttempts := int(timeout / interval)
	timeoutCtx, cancelFn := context.WithTimeout(context.Background(), timeout)
	defer cancelFn()
	err, _ := base.RetryLoopCtx("Wait for REST APIs", func() (shouldRetry bool, err error, value interface{}) {
		sc.lock.RLock()
		defer sc.lock.RUnlock()
		if len(sc._httpServers) == 3 {
			return false, nil, nil
		}
		return true, nil, nil
	}, base.CreateSleeperFunc(numAttempts, int(interval.Milliseconds())), timeoutCtx)
	return err
}

// PostStartup runs anything that relies on SG being fully started (i.e. sgreplicate)
func (sc *ServerContext) PostStartup() {
	// Delay DatabaseContext processes starting up, e.g. to avoid replication reassignment churn when a Sync Gateway Cluster is being initialized
	// TODO: Consider sc.waitForRESTAPIs for faster startup?
	time.Sleep(5 * time.Second)
	close(sc.hasStarted)
}

// serverContextStopMaxWait is the maximum amount of time to wait for
// background goroutines to terminate before the server is stopped.
const serverContextStopMaxWait = 30 * time.Second

func (sc *ServerContext) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	logCtx := context.TODO()
	err := base.TerminateAndWaitForClose(sc.statsContext.terminator, sc.statsContext.doneChan, serverContextStopMaxWait)
	if err != nil {
		base.InfofCtx(logCtx, base.KeyAll, "Couldn't stop stats logger: %v", err)
	}

	err = base.TerminateAndWaitForClose(sc.bootstrapContext.terminator, sc.bootstrapContext.doneChan, serverContextStopMaxWait)
	if err != nil {
		base.InfofCtx(logCtx, base.KeyAll, "Couldn't stop background config update worker: %v", err)
	}

	for _, ctx := range sc.databases_ {
		ctx.Close()
		_ = ctx.EventMgr.RaiseDBStateChangeEvent(ctx.Name, "offline", "Database context closed", &sc.config.API.AdminInterface)
	}
	sc.databases_ = nil

	for _, s := range sc._httpServers {
		base.InfofCtx(logCtx, base.KeyHTTP, "Closing HTTP Server: %v", s.Addr)
		if err := s.Close(); err != nil {
			base.WarnfCtx(logCtx, "Error closing HTTP server %q: %v", s.Addr, err)
		}
	}
	sc._httpServers = nil

	if agent := sc.GoCBAgent; agent != nil {
		if err := agent.Close(); err != nil {
			base.WarnfCtx(logCtx, "Error closing agent connection: %v", err)
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

	if sc.bootstrapContext.connection != nil {
		sc.lock.Lock()
		defer sc.lock.Unlock()
		// database not loaded, go look for it in the cluster
		found, err := sc._fetchAndLoadDatabase(name)
		if err != nil {
			return nil, base.HTTPErrorf(http.StatusInternalServerError, "couldn't load database: %v", err)
		}
		if found {
			dbc := sc.databases_[name]
			if dbc != nil {
				return dbc, nil
			}
		}
	}

	return nil, base.HTTPErrorf(http.StatusNotFound, "no such database %q", name)
}

func (sc *ServerContext) GetDbConfig(name string) *DbConfig {
	if dbConfig := sc.GetDatabaseConfig(name); dbConfig != nil {
		return &dbConfig.DbConfig
	}
	return nil
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
func (sc *ServerContext) _reloadDatabase(reloadDbName string, failFast bool) (*db.DatabaseContext, error) {
	sc._unloadDatabase(reloadDbName)
	config := sc.dbConfigs[reloadDbName]
	return sc._getOrAddDatabaseFromConfig(*config, true, failFast)
}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) ReloadDatabase(reloadDbName string) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	dbContext, err := sc._reloadDatabase(reloadDbName, false)
	sc.lock.Unlock()

	return dbContext, err
}

func (sc *ServerContext) ReloadDatabaseWithConfig(config DatabaseConfig) error {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc._reloadDatabaseWithConfig(config, true)
}

func (sc *ServerContext) _reloadDatabaseWithConfig(config DatabaseConfig, failFast bool) error {
	sc._removeDatabase(config.Name)
	_, err := sc._getOrAddDatabaseFromConfig(config, false, failFast)
	return err
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) getOrAddDatabaseFromConfig(config DatabaseConfig, useExisting bool, failFast bool) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc._getOrAddDatabaseFromConfig(config, useExisting, failFast)
}

func GetBucketSpec(config *DatabaseConfig, serverConfig *StartupConfig) (spec base.BucketSpec, err error) {

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
	if !spec.UseXattrs {
		base.WarnfCtx(context.TODO(), "Running Sync Gateway without shared bucket access is deprecated. Recommendation: set enable_shared_bucket_access=true")
	}

	if config.BucketOpTimeoutMs != nil {
		operationTimeout := time.Millisecond * time.Duration(*config.BucketOpTimeoutMs)
		spec.BucketOpTimeout = &operationTimeout
	}
	return spec, nil
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) _getOrAddDatabaseFromConfig(config DatabaseConfig, useExisting, failFast bool) (*db.DatabaseContext, error) {

	// Generate bucket spec and validate whether db already exists
	spec, err := GetBucketSpec(&config, sc.config)
	if err != nil {
		return nil, err
	}

	dbName := config.Name
	if dbName == "" {
		dbName = spec.BucketName
	}
	if spec.Server == "" {
		spec.Server = sc.config.Bootstrap.Server
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
	base.InfofCtx(context.TODO(), base.KeyAll, "Opening db /%s as bucket %q, pool %q, server <%s>",
		base.MD(dbName), base.MD(spec.BucketName), base.SD(base.DefaultPool), base.SD(spec.Server))
	connectToBucketFn := db.ConnectToBucket
	if failFast {
		connectToBucketFn = db.ConnectToBucketFailFast
	}
	bucket, err := connectToBucketFn(spec)
	if err != nil {
		return nil, err
	}

	// If using a walrus bucket, force use of views
	useViews := base.BoolDefault(config.UseViews, false)
	if !useViews && spec.IsWalrusBucket() {
		base.WarnfCtx(context.TODO(), "Using GSI is not supported when using a walrus bucket - switching to use views.  Set 'use_views':true in Sync Gateway's database config to avoid this warning.")
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
		n1qlStore, ok := base.AsN1QLStore(bucket)
		if !ok {
			return nil, errors.New("Cannot create indexes on non-Couchbase data store.")

		}
		indexErr := db.InitializeIndexes(n1qlStore, config.UseXattrs(), numReplicas)
		if indexErr != nil {
			return nil, indexErr
		}
	} else {
		viewErr := db.InitializeViews(bucket)
		if viewErr != nil {
			return nil, viewErr
		}
	}

	// Process unsupported config options or store runtime defaults if not set
	if config.Unsupported == nil {
		config.Unsupported = &db.UnsupportedOptions{}
	}
	if config.Unsupported.WarningThresholds == nil {
		config.Unsupported.WarningThresholds = &db.WarningThresholds{}
	}

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
	dbcontext.ServerContextHasStarted = sc.hasStarted
	dbcontext.NoX509HTTPClient = sc.NoX509HTTPClient

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
				base.WarnfCtx(context.TODO(), "Setting the revs_limit (%v) to less than %d, whilst having allow_conflicts set to true, may have unwanted results when documents are frequently updated. Please see documentation for details.", dbcontext.RevsLimit, db.DefaultRevsLimitConflicts)
			}
		} else {
			if dbcontext.RevsLimit <= 0 {
				return nil, fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration must be greater than zero.", dbcontext.RevsLimit)
			}
		}
	}

	dbcontext.AllowEmptyPassword = base.BoolDefault(config.AllowEmptyPassword, false)
	dbcontext.ServeInsecureAttachmentTypes = base.BoolDefault(config.ServeInsecureAttachmentTypes, false)

	if dbcontext.ChannelMapper == nil {
		base.InfofCtx(context.TODO(), base.KeyAll, "Using default sync function 'channel(doc.channels)' for database %q", base.MD(dbName))
	}

	// Create default users & roles:
	if err := sc.installPrincipals(dbcontext, config.Roles, "role"); err != nil {
		return nil, err
	}
	if err := sc.installPrincipals(dbcontext, config.Users, "user"); err != nil {
		return nil, err
	}

	if config.Guest != nil {
		guest := map[string]*db.PrincipalConfig{base.GuestUsername: config.Guest}
		if err := sc.installPrincipals(dbcontext, guest, "user"); err != nil {
			return nil, err
		}
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
	sc.dbConfigs[dbcontext.Name] = &config
	sc.bucketDbName[spec.BucketName] = dbName

	if base.BoolDefault(config.StartOffline, false) {
		atomic.StoreUint32(&dbcontext.State, db.DBOffline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "offline", "DB loaded from config", &sc.config.API.AdminInterface)
	} else {
		atomic.StoreUint32(&dbcontext.State, db.DBOnline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "online", "DB loaded from config", &sc.config.API.AdminInterface)
	}

	dbcontext.StartReplications()

	return dbcontext, nil
}

func dbcOptionsFromConfig(sc *ServerContext, config *DbConfig, dbName string) (db.DatabaseContextOptions, error) {

	// Identify import options
	importOptions := db.ImportOptions{}
	if config.ImportFilter != nil {
		importOptions.ImportFilter = db.NewImportFilterFunction(*config.ImportFilter)
	}
	importOptions.BackupOldRev = base.BoolDefault(config.ImportBackupOldRev, false)

	if config.ImportPartitions == nil {
		importOptions.ImportPartitions = base.DefaultImportPartitions
	} else {
		importOptions.ImportPartitions = *config.ImportPartitions
	}

	// Check for deprecated cache options. If new are set they will take priority but will still log warnings
	warnings := config.deprecatedConfigCacheFallback()
	for _, warnLog := range warnings {
		base.WarnfCtx(context.TODO(), warnLog)
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
	base.InfofCtx(context.TODO(), base.KeyAll, "delta_sync enabled=%t with rev_max_age_seconds=%d for database %s", deltaSyncOptions.Enabled, deltaSyncOptions.RevMaxAgeSeconds, dbName)

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
			base.WarnfCtx(context.TODO(), "Using deprecated config parameter 'cache.channel_cache.query_limit'. Use 'query_pagination_limit' instead")
			queryPaginationLimit = *config.CacheConfig.ChannelCacheConfig.DeprecatedQueryLimit
		} else {
			base.WarnfCtx(context.TODO(), "Both query_pagination_limit and the deprecated cache.channel_cache.query_limit have been specified in config - using query_pagination_limit")
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

	if config.UserXattrKey != "" {
		if !base.IsEnterpriseEdition() {
			return db.DatabaseContextOptions{}, fmt.Errorf("user_xattr_key is only supported in enterpise edition")
		}

		if !config.UseXattrs() {
			return db.DatabaseContextOptions{}, fmt.Errorf("use of user_xattr_key requires shared_bucket_access to be enabled")
		}
	}

	clientPartitionWindow := base.DefaultClientPartitionWindow
	if config.ClientPartitionWindowSecs != nil {
		clientPartitionWindow = time.Duration(*config.ClientPartitionWindowSecs) * time.Second
	}

	bcryptCost := sc.config.Auth.BcryptCost
	if bcryptCost <= 0 {
		bcryptCost = auth.DefaultBcryptCost
	}

	slowQueryWarningThreshold := kDefaultSlowQueryWarningThreshold * time.Millisecond
	if config.SlowQueryWarningThresholdMs != nil {
		slowQueryWarningThreshold = time.Duration(*config.SlowQueryWarningThresholdMs) * time.Millisecond
	}

	groupID := ""
	if sc.config.Bootstrap.ConfigGroupID != persistentConfigDefaultGroupID {
		groupID = sc.config.Bootstrap.ConfigGroupID
	}

	if config.AllowConflicts != nil && *config.AllowConflicts {
		base.WarnfCtx(context.TODO(), `Deprecation notice: setting database configuration option "allow_conflicts" to true is due to be removed. In the future, conflicts will not be allowed.`)
	}

	// If basic auth is disabled, it doesn't make sense to send WWW-Authenticate
	sendWWWAuthenticate := config.SendWWWAuthenticateHeader
	if config.DisablePasswordAuth {
		sendWWWAuthenticate = base.BoolPtr(false)
	}

	contextOptions := db.DatabaseContextOptions{
		CacheOptions:                  &cacheOptions,
		RevisionCacheOptions:          revCacheOptions,
		OldRevExpirySeconds:           oldRevExpirySeconds,
		LocalDocExpirySecs:            localDocExpirySecs,
		AdminInterface:                &sc.config.API.AdminInterface,
		UnsupportedOptions:            config.Unsupported,
		OIDCOptions:                   config.OIDCConfig,
		DBOnlineCallback:              dbOnlineCallback,
		ImportOptions:                 importOptions,
		EnableXattr:                   config.UseXattrs(),
		SecureCookieOverride:          secureCookieOverride,
		SessionCookieName:             config.SessionCookieName,
		SessionCookieHttpOnly:         base.BoolDefault(config.SessionCookieHTTPOnly, false),
		AllowConflicts:                config.ConflictsAllowed(),
		SendWWWAuthenticateHeader:     sendWWWAuthenticate,
		DisablePasswordAuthentication: config.DisablePasswordAuth,
		DeltaSyncOptions:              deltaSyncOptions,
		CompactInterval:               compactIntervalSecs,
		QueryPaginationLimit:          queryPaginationLimit,
		UserXattrKey:                  config.UserXattrKey,
		SGReplicateOptions: db.SGReplicateOptions{
			Enabled:               sgReplicateEnabled,
			WebsocketPingInterval: sgReplicateWebsocketPingInterval,
		},
		SlowQueryWarningThreshold: slowQueryWarningThreshold,
		ClientPartitionWindow:     clientPartitionWindow,
		BcryptCost:                bcryptCost,
		GroupID:                   groupID,
	}

	return contextOptions, nil
}

func (sc *ServerContext) TakeDbOnline(database *db.DatabaseContext) {

	// Take a write lock on the Database context, so that we can cycle the underlying Database
	// without any other call running concurrently
	database.AccessLock.Lock()
	defer database.AccessLock.Unlock()

	// We can only transition to Online from Offline state
	if atomic.CompareAndSwapUint32(&database.State, db.DBOffline, db.DBStarting) {
		reloadedDb, err := sc.ReloadDatabase(database.Name)
		if err != nil {
			base.ErrorfCtx(context.TODO(), "Error reloading database from config: %v", err)
			return
		}

		// Reloaded DB should already be online in most cases, but force state to online to handle cases
		// where config specifies offline startup
		atomic.StoreUint32(&reloadedDb.State, db.DBOnline)

	} else {
		base.InfofCtx(context.TODO(), base.KeyCRUD, "Unable to take Database : %v online , database must be in Offline state", base.UD(database.Name))
	}

}

// validateEventConfigOptions returns errors for all invalid event type options.
func validateEventConfigOptions(eventType db.EventType, eventConfig *EventConfig) error {
	if eventConfig == nil || eventConfig.Options == nil {
		return nil
	}

	var errs *base.MultiError

	switch eventType {
	case db.DocumentChange:
		for k, v := range eventConfig.Options {
			switch k {
			case db.EventOptionDocumentChangedWinningRevOnly:
				if _, ok := v.(bool); !ok {
					errs = errs.Append(fmt.Errorf("Event option %q must be of type bool", db.EventOptionDocumentChangedWinningRevOnly))
				}
			default:
				errs = errs.Append(fmt.Errorf("unknown option %q found for event type %q", k, eventType))
			}
		}
	default:
		errs = errs.Append(fmt.Errorf("unknown options %v found for event type %q", eventConfig.Options, eventType))
	}

	// If we only have 1 error, return it as-is for clarity in the logs.
	if errs.ErrorOrNil() != nil {
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
			insecureSkipVerify := false
			if config.Unsupported != nil {
				insecureSkipVerify = config.Unsupported.RemoteConfigTlsSkipVerify
			}
			filter, err := loadJavaScript(conf.Filter, insecureSkipVerify)
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
			base.WarnfCtx(context.TODO(), "Error parsing wait_for_process from config, using default %s", err)
		}
	}
	dbcontext.EventMgr.Start(config.EventHandlers.MaxEventProc, int(customWaitTime))

	return nil
}

// Adds a database to the ServerContext given its configuration.  If an existing config is found
// for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfig(config DatabaseConfig) (*db.DatabaseContext, error) {
	return sc.getOrAddDatabaseFromConfig(config, false, false)
}

// AddDatabaseFromConfigFailFast adds a database to the ServerContext given its configuration and fails fast.
// If an existing config is found for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfigFailFast(config DatabaseConfig) (*db.DatabaseContext, error) {
	return sc.getOrAddDatabaseFromConfig(config, false, true)
}

func (sc *ServerContext) processEventHandlersForEvent(events []*EventConfig, eventType db.EventType, dbcontext *db.DatabaseContext) error {

	for _, event := range events {
		switch event.HandlerType {
		case "webhook":
			wh, err := db.NewWebhook(event.Url, event.Filter, event.Timeout, event.Options)
			if err != nil {
				base.WarnfCtx(context.TODO(), "Error creating webhook %v", err)
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
	base.InfofCtx(context.TODO(), base.KeyAll, "**NOTE:** %q's sync function has changed. The new function may assign different channels to documents, or permissions to users. You may want to re-sync the database to update these.", base.MD(dbcontext.Name))
	return nil
}

func (sc *ServerContext) RemoveDatabase(dbName string) bool {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	return sc._removeDatabase(dbName)
}

// _unloadDatabase unloads and stops the database, but does not remove the in-memory config.
func (sc *ServerContext) _unloadDatabase(dbName string) bool {
	dbCtx := sc.databases_[dbName]
	if dbCtx == nil {
		return false
	}
	base.InfofCtx(context.TODO(), base.KeyAll, "Closing db /%s (bucket %q)", base.MD(dbCtx.Name), base.MD(dbCtx.Bucket.GetName()))
	dbCtx.Close()
	delete(sc.databases_, dbName)
	return true
}

// _removeDatabase unloads and removes all references to the given database.
func (sc *ServerContext) _removeDatabase(dbName string) bool {
	dbCtx := sc.databases_[dbName]
	if dbCtx == nil {
		return false
	}
	bucket := dbCtx.Bucket.GetName()
	if ok := sc._unloadDatabase(dbName); !ok {
		return ok
	}
	delete(sc.dbConfigs, dbName)
	delete(sc.bucketDbName, bucket)
	return true
}

func (sc *ServerContext) installPrincipals(dbc *db.DatabaseContext, spec map[string]*db.PrincipalConfig, what string) error {
	for name, princ := range spec {
		isGuest := name == base.GuestUsername
		if isGuest {
			internalName := ""
			princ.Name = &internalName
		} else {
			n := name
			princ.Name = &n
		}

		logCtx := context.TODO()
		createdPrincipal := true
		worker := func() (shouldRetry bool, err error, value interface{}) {
			_, err = dbc.UpdatePrincipal(context.Background(), *princ, (what == "user"), isGuest)
			if err != nil {
				if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusConflict {
					// Ignore and absorb this error if it's a conflict error, which just means that updatePrincipal didn't overwrite an existing user.
					// Since if there's an existing user it's "mission accomplished", this can be treated as a success case.
					createdPrincipal = false
					return false, nil, nil
				}

				if err == base.ErrViewTimeoutError {
					// Timeout error, possibly due to view re-indexing, so retry
					base.InfofCtx(logCtx, base.KeyAuth, "Error calling UpdatePrincipal(): %v.  Will retry in case this is a temporary error", err)
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
			base.InfofCtx(logCtx, base.KeyAll, "Reset guest user to config")
		} else if createdPrincipal {
			base.InfofCtx(logCtx, base.KeyAll, "Created %s %q", what, base.UD(name))
		}

	}
	return nil
}

// ////// STATS LOGGING

type statsWrapper struct {
	Stats              json.RawMessage `json:"stats"`
	UnixEpochTimestamp int64           `json:"unix_epoch_timestamp"`
	RFC3339            string          `json:"rfc3339_timestamp"`
}

func (sc *ServerContext) startStatsLogger() {

	if sc.config.Unsupported.StatsLogFrequency == nil || sc.config.Unsupported.StatsLogFrequency.Value() == 0 {
		// don't start the stats logger when explicitly zero
		return
	}

	interval := sc.config.Unsupported.StatsLogFrequency

	sc.statsContext.statsLoggingTicker = time.NewTicker(interval.Value())
	sc.statsContext.terminator = make(chan struct{})
	sc.statsContext.doneChan = make(chan struct{})
	go func() {
		defer close(sc.statsContext.doneChan)
		for {
			select {
			case <-sc.statsContext.statsLoggingTicker.C:
				err := sc.logStats()
				if err != nil {
					base.WarnfCtx(context.TODO(), "Error logging stats: %v", err)
				}
			case <-sc.statsContext.terminator:
				base.DebugfCtx(context.TODO(), base.KeyAll, "Stopping stats logging goroutine")
				sc.statsContext.statsLoggingTicker.Stop()
				return
			}
		}
	}()
	base.InfofCtx(context.TODO(), base.KeyAll, "Logging stats with frequency: %v", interval)

}

func (sc *ServerContext) logStats() error {

	AddGoRuntimeStats()

	sc.logNetworkInterfaceStats()

	if err := sc.statsContext.addGoSigarStats(); err != nil {
		base.WarnfCtx(context.TODO(), "Error getting sigar based system resource stats: %v", err)
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
		base.WarnfCtx(context.TODO(), "Error getting public network interface resource stats: %v", err)
	}

	if err := sc.statsContext.addAdminNetworkInterfaceStatsForHostnamePort(sc.config.API.AdminInterface); err != nil {
		base.WarnfCtx(context.TODO(), "Error getting admin network interface resource stats: %v", err)
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

func initClusterAgent(clusterAddress, clusterUser, clusterPass, certPath, keyPath, caCertPath string, tlsSkipVerify *bool) (*gocbcore.Agent, error) {
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

	shouldCloseAgent := true
	defer func() {
		if shouldCloseAgent {
			if err := agent.Close(); err != nil {
				base.WarnfCtx(context.TODO(), "unable to close gocb agent: %v", err)
			}
		}
	}()

	agentReadyErr := make(chan error)
	_, err = agent.WaitUntilReady(
		time.Now().Add(5*time.Second),
		gocbcore.WaitUntilReadyOptions{
			ServiceTypes: []gocbcore.ServiceType{gocbcore.MgmtService},
		},
		func(result *gocbcore.WaitUntilReadyResult, err error) {
			agentReadyErr <- err
		},
	)

	if err != nil {
		return nil, err
	}

	if err := <-agentReadyErr; err != nil {
		if _, ok := errors.Unwrap(err).(x509.UnknownAuthorityError); ok {
			err = fmt.Errorf("%w - Provide a CA cert, or set tls_skip_verify to true in config", err)
		}

		return nil, err
	}

	shouldCloseAgent = false
	return agent, nil
}

// initializeGoCBAgent Obtains a gocb agent from the current server connection. Requires the agent to be closed after use.
// Uses retry loop
func (sc *ServerContext) initializeGoCBAgent() (*gocbcore.Agent, error) {
	err, a := base.RetryLoop("Initialize Cluster Agent", func() (shouldRetry bool, err error, value interface{}) {
		agent, err := initClusterAgent(
			sc.config.Bootstrap.Server, sc.config.Bootstrap.Username, sc.config.Bootstrap.Password,
			sc.config.Bootstrap.X509CertPath, sc.config.Bootstrap.X509KeyPath, sc.config.Bootstrap.CACertPath, sc.config.Bootstrap.ServerTLSSkipVerify)
		if err != nil {
			base.InfofCtx(context.TODO(), base.KeyConfig, "Couldn't initialize cluster agent: %v - will retry...", err)
			return true, err, nil
		}

		return false, nil, agent
	}, base.CreateSleeperFunc(27, 1000)) // ~2 mins total - 5 second gocb WaitUntilReady timeout and 1 second interval
	if err != nil {
		return nil, err
	}

	base.InfofCtx(context.TODO(), base.KeyConfig, "Successfully initialized cluster agent")
	agent := a.(*gocbcore.Agent)
	return agent, nil
}

// initializeNoX509HttpClient() returns an http client based on the bootstrap connection information, but
// without any x509 keypair included in the tls config.  This client can be used to perform basic
// authentication checks against the server.
// Client creation otherwise clones the approach used by gocb.
func (sc *ServerContext) initializeNoX509HttpClient() (*http.Client, error) {

	// baseTlsConfig defines the tlsConfig except for ServerName, which is updated based
	// on addr in DialTLS
	baseTlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	var rootCAs *x509.CertPool
	tlsRootCAProvider, err := base.GoCBCoreTLSRootCAProvider(sc.config.Bootstrap.ServerTLSSkipVerify, sc.config.Bootstrap.CACertPath)
	if err != nil {
		return nil, err
	}
	rootCAs = tlsRootCAProvider()
	if rootCAs != nil {
		baseTlsConfig.RootCAs = rootCAs
		baseTlsConfig.InsecureSkipVerify = false
	} else {
		baseTlsConfig.InsecureSkipVerify = true
	}

	httpDialer := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	maxIdleConns, _ := strconv.Atoi(base.DefaultHttpMaxIdleConns)
	idleConnTimeoutMs, _ := strconv.Atoi(base.DefaultHttpIdleConnTimeoutMilliseconds)

	// gocbcore: We set ForceAttemptHTTP2, which will update the base-config to support HTTP2
	// automatically, so that all configs from it will look for that.
	httpTransport := &http.Transport{
		ForceAttemptHTTP2: true,

		Dial: func(network, addr string) (net.Conn, error) {
			return httpDialer.Dial(network, addr)
		},
		DialTLS: func(network, addr string) (net.Conn, error) {
			tcpConn, err := httpDialer.Dial(network, addr)
			if err != nil {
				return nil, err
			}

			// Update tlsConfig.ServerName based on addr
			tlsConfig := baseTlsConfig.Clone()
			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			tlsConfig.ServerName = host
			tlsConn := tls.Client(tcpConn, tlsConfig)
			return tlsConn, nil
		},
		MaxIdleConns:        maxIdleConns,
		MaxIdleConnsPerHost: base.DefaultHttpMaxIdleConnsPerHost,
		IdleConnTimeout:     time.Duration(idleConnTimeoutMs) * time.Millisecond,
	}

	httpCli := &http.Client{
		Transport: httpTransport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// gocbcore: All that we're doing here is setting auth on any redirects.
			// For that reason we can just pull it off the oldest (first) request.
			if len(via) >= 10 {
				// Just duplicate the default behaviour for maximum redirects.
				return errors.New("stopped after 10 redirects")
			}

			oldest := via[0]
			auth := oldest.Header.Get("Authorization")
			if auth != "" {
				req.Header.Set("Authorization", auth)
			}

			return nil
		},
	}

	return httpCli, nil
}

func (sc *ServerContext) ObtainManagementEndpointsAndHTTPClient() ([]string, *http.Client, error) {
	if sc.GoCBAgent == nil {
		return nil, nil, fmt.Errorf("unable to obtain agent")
	}

	return sc.GoCBAgent.MgmtEps(), sc.NoX509HTTPClient, nil
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
			requireBucketOptions := []string{""}
			if requireRole.DatabaseScoped {
				requireBucketOptions = []string{bucketName, RoleBucketWildcard}
			}

			for _, requireBucket := range requireBucketOptions {
				if (roleResult.BucketName == requireBucket) && roleResult.RoleName == requireRole.RoleName {
					return http.StatusOK, nil
				}
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

func (sc *ServerContext) initializeCouchbaseServerConnections() error {
	goCBAgent, err := sc.initializeGoCBAgent()
	if err != nil {
		return err
	}
	sc.GoCBAgent = goCBAgent

	sc.NoX509HTTPClient, err = sc.initializeNoX509HttpClient()
	if err != nil {
		return err
	}

	// Fetch database configs from bucket and start polling for new buckets and config updates.
	if sc.persistentConfig {
		couchbaseCluster, err := createCouchbaseClusterFromStartupConfig(sc.config)
		if err != nil {
			return err
		}

		sc.bootstrapContext.connection = couchbaseCluster

		count, err := sc.fetchAndLoadConfigs(true)
		if err != nil {
			return err
		}

		logCtx := context.TODO()
		if count > 0 {
			base.InfofCtx(logCtx, base.KeyConfig, "Successfully fetched %d database configs from buckets in cluster", count)
		} else {
			base.WarnfCtx(logCtx, "Config: No database configs for group %q. Continuing startup to allow REST API database creation", sc.config.Bootstrap.ConfigGroupID)
		}

		if sc.config.Bootstrap.ConfigUpdateFrequency.Value() > 0 {
			sc.bootstrapContext.terminator = make(chan struct{})
			sc.bootstrapContext.doneChan = make(chan struct{})

			base.InfofCtx(logCtx, base.KeyConfig, "Starting background polling for new configs/buckets: %s", sc.config.Bootstrap.ConfigUpdateFrequency.Value().String())
			go func() {
				defer close(sc.bootstrapContext.doneChan)
				t := time.NewTicker(sc.config.Bootstrap.ConfigUpdateFrequency.Value())
				for {
					select {
					case <-sc.bootstrapContext.terminator:
						base.InfofCtx(logCtx, base.KeyConfig, "Stopping background config polling loop")
						t.Stop()
						return
					case <-t.C:
						base.DebugfCtx(logCtx, base.KeyConfig, "Fetching configs from buckets in cluster for group %q", sc.config.Bootstrap.ConfigGroupID)
						count, err := sc.fetchAndLoadConfigs(false)
						if err != nil {
							base.WarnfCtx(logCtx, "Couldn't load configs from bucket when polled: %v", err)
						}
						if count > 0 {
							base.InfofCtx(logCtx, base.KeyConfig, "Successfully fetched %d database configs from buckets in cluster", count)
						}
					}
				}
			}()
		} else {
			base.InfofCtx(logCtx, base.KeyConfig, "Disabled background polling for new configs/buckets")
		}
	}

	return nil
}
