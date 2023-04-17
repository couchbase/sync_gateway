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
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/db/functions"

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

var errCollectionsUnsupported = base.HTTPErrorf(http.StatusBadRequest, "Named collections specified in database config, but not supported by connected Couchbase Server.")

var ErrSuspendingDisallowed = errors.New("database does not allow suspending")

// Shared context of HTTP handlers: primarily a registry of databases by name. It also stores
// the configuration settings so handlers can refer to them.
// This struct is accessed from HTTP handlers running on multiple goroutines, so it needs to
// be thread-safe.
type ServerContext struct {
	Config                        *StartupConfig // The current runtime configuration of the node
	initialStartupConfig          *StartupConfig // The configuration at startup of the node. Built from config file + flags
	persistentConfig              bool
	dbRegistry                    map[string]struct{}               // registry of dbNames, used to ensure uniqueness even when db isn't active
	collectionRegistry            map[string]string                 // map of fully qualified collection name to db name, used for local uniqueness checks
	dbConfigs                     map[string]*RuntimeDatabaseConfig // dbConfigs is a map of db name to the RuntimeDatabaseConfig
	databases_                    map[string]*db.DatabaseContext    // databases_ is a map of dbname to db.DatabaseContext
	lock                          sync.RWMutex
	statsContext                  *statsContext
	BootstrapContext              *bootstrapContext
	HTTPClient                    *http.Client
	cpuPprofFileMutex             sync.Mutex      // Protect cpuPprofFile from concurrent Start and Stop CPU profiling requests
	cpuPprofFile                  *os.File        // An open file descriptor holds the reference during CPU profiling
	_httpServers                  []*http.Server  // A list of HTTP servers running under the ServerContext
	GoCBAgent                     *gocbcore.Agent // GoCB Agent to use when obtaining management endpoints
	NoX509HTTPClient              *http.Client    // httpClient for the cluster that doesn't include x509 credentials, even if they are configured for the cluster
	hasStarted                    chan struct{}   // A channel that is closed via PostStartup once the ServerContext has fully started
	LogContextID                  string          // ID to differentiate log messages from different server context
	fetchConfigsLastUpdate        time.Time       // The last time fetchConfigsWithTTL() updated dbConfigs
	allowScopesInPersistentConfig bool            // Test only backdoor to allow scopes in persistent config, not supported for multiple databases with different collections targeting the same bucket
}

// defaultConfigRetryTimeout is the total retry time when waiting for in-flight config updates.  Set as a multiple of kv op timeout,
// based on the maximum of 3 kv ops for a successful config update
const defaultConfigRetryTimeout = 3 * base.DefaultGocbV2OperationTimeout

type bootstrapContext struct {
	Connection         base.BootstrapConnection
	configRetryTimeout time.Duration // configRetryTimeout defines the total amount of time to retry on a registry/config mismatch
	terminator         chan struct{} // Used to stop the goroutine handling the stats logging
	doneChan           chan struct{} // doneChan is closed when the stats logger goroutine finishes.
}

func (sc *ServerContext) CreateLocalDatabase(ctx context.Context, dbs DbConfigMap) error {
	for _, dbConfig := range dbs {
		dbc := dbConfig.ToDatabaseConfig()
		_, err := sc._getOrAddDatabaseFromConfig(ctx, *dbc, false, db.GetConnectToBucketFn(false))
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

func (sc *ServerContext) CloseCpuPprofFile(ctx context.Context) {
	sc.cpuPprofFileMutex.Lock()
	if err := sc.cpuPprofFile.Close(); err != nil {
		base.WarnfCtx(ctx, "Error closing CPU profile file: %v", err)
	}
	sc.cpuPprofFile = nil
	sc.cpuPprofFileMutex.Unlock()
}

func NewServerContext(ctx context.Context, config *StartupConfig, persistentConfig bool) *ServerContext {
	sc := &ServerContext{
		Config:             config,
		persistentConfig:   persistentConfig,
		dbRegistry:         map[string]struct{}{},
		collectionRegistry: map[string]string{},
		dbConfigs:          map[string]*RuntimeDatabaseConfig{},
		databases_:         map[string]*db.DatabaseContext{},
		HTTPClient:         http.DefaultClient,
		statsContext:       &statsContext{},
		BootstrapContext:   &bootstrapContext{},
		hasStarted:         make(chan struct{}),
	}

	if base.ServerIsWalrus(sc.Config.Bootstrap.Server) {
		sc.persistentConfig = false

		// Disable Admin API authentication when running as walrus on the default admin interface to support dev
		// environments.
		if sc.Config.API.AdminInterface == DefaultAdminInterface {
			sc.Config.API.AdminInterfaceAuthentication = base.BoolPtr(false)
			sc.Config.API.MetricsInterfaceAuthentication = base.BoolPtr(false)
		}
	}

	sc.startStatsLogger(ctx)

	return sc
}

func (sc *ServerContext) WaitForRESTAPIs() error {
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
	// TODO: Consider sc.WaitForRESTAPIs for faster startup?
	time.Sleep(5 * time.Second)
	close(sc.hasStarted)
}

// serverContextStopMaxWait is the maximum amount of time to wait for
// background goroutines to terminate before the server is stopped.
const serverContextStopMaxWait = 30 * time.Second

func (sc *ServerContext) Close(ctx context.Context) {

	err := base.TerminateAndWaitForClose(sc.statsContext.terminator, sc.statsContext.doneChan, serverContextStopMaxWait)
	if err != nil {
		base.InfofCtx(ctx, base.KeyAll, "Couldn't stop stats logger: %v", err)
	}

	err = base.TerminateAndWaitForClose(sc.BootstrapContext.terminator, sc.BootstrapContext.doneChan, serverContextStopMaxWait)
	if err != nil {
		base.InfofCtx(ctx, base.KeyAll, "Couldn't stop background config update worker: %v", err)
	}

	sc.lock.Lock()
	defer sc.lock.Unlock()

	for _, db := range sc.databases_ {
		db.Close(ctx)
		_ = db.EventMgr.RaiseDBStateChangeEvent(db.Name, "offline", "Database context closed", &sc.Config.API.AdminInterface)
	}
	sc.databases_ = nil

	for _, s := range sc._httpServers {
		base.InfofCtx(ctx, base.KeyHTTP, "Closing HTTP Server: %v", s.Addr)
		if err := s.Close(); err != nil {
			base.WarnfCtx(ctx, "Error closing HTTP server %q: %v", s.Addr, err)
		}
	}
	sc._httpServers = nil

	if agent := sc.GoCBAgent; agent != nil {
		if err := agent.Close(); err != nil {
			base.WarnfCtx(ctx, "Error closing agent connection: %v", err)
		}
	}
}

// GetDatabase attempts to return the DatabaseContext of the database. It will load the database if necessary.
func (sc *ServerContext) GetDatabase(ctx context.Context, name string) (*db.DatabaseContext, error) {
	dbc, err := sc.GetActiveDatabase(name)
	if err == base.ErrNotFound {
		return sc.GetInactiveDatabase(ctx, name)
	}
	return dbc, err
}

// GetActiveDatabase attempts to return the DatabaseContext of a loaded database. If not found, the database name will be
// validated to make sure it's valid and then an error returned.
func (sc *ServerContext) GetActiveDatabase(name string) (*db.DatabaseContext, error) {
	sc.lock.RLock()
	dbc := sc.databases_[name]
	sc.lock.RUnlock()
	if dbc != nil {
		return dbc, nil
	} else if db.ValidateDatabaseName(name) != nil {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "invalid database name %q", name)
	}
	return nil, base.ErrNotFound
}

// GetInactiveDatabase attempts to load the database and return it's DatabaseContext. It will first attempt to unsuspend the
// database, and if that fails, try to load the database from the buckets.
// This should be used if GetActiveDatabase fails.
func (sc *ServerContext) GetInactiveDatabase(ctx context.Context, name string) (*db.DatabaseContext, error) {
	dbc, err := sc.unsuspendDatabase(ctx, name)
	if err != nil && err != base.ErrNotFound && err != ErrSuspendingDisallowed {
		return nil, err
	} else if err == nil {
		return dbc, nil
	}

	// database not loaded, fallback to fetching it from cluster
	if sc.BootstrapContext.Connection != nil {
		var found bool
		if sc.Config.IsServerless() {
			found, err = sc.fetchAndLoadDatabaseSince(ctx, name, sc.Config.Unsupported.Serverless.MinConfigFetchInterval)

		} else {
			found, err = sc.fetchAndLoadDatabase(base.NewNonCancelCtx(), name)
		}
		if found {
			sc.lock.RLock()
			defer sc.lock.RUnlock()
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

func (sc *ServerContext) GetDatabaseConfig(name string) *RuntimeDatabaseConfig {
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
func (sc *ServerContext) PostUpgrade(ctx context.Context, preview bool) (postUpgradeResults PostUpgradeResult, err error) {
	sc.lock.RLock()
	defer sc.lock.RUnlock()

	postUpgradeResults = make(map[string]PostUpgradeDatabaseResult, len(sc.databases_))

	for name, database := range sc.databases_ {
		// View cleanup
		removedDDocs, _ := database.RemoveObsoleteDesignDocs(preview)

		// Index cleanup
		var removedIndexes []string
		if !base.TestsDisableGSI() {
			removedIndexes, _ = database.RemoveObsoleteIndexes(ctx, preview)
		}

		postUpgradeResults[name] = PostUpgradeDatabaseResult{
			RemovedDDocs:   removedDDocs,
			RemovedIndexes: removedIndexes,
		}
	}
	return postUpgradeResults, nil
}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) _reloadDatabase(ctx context.Context, reloadDbName string, failFast bool) (*db.DatabaseContext, error) {
	sc._unloadDatabase(ctx, reloadDbName)
	config := sc.dbConfigs[reloadDbName]
	return sc._getOrAddDatabaseFromConfig(ctx, config.DatabaseConfig, true, db.GetConnectToBucketFn(failFast))
}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) ReloadDatabase(ctx context.Context, reloadDbName string) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	dbContext, err := sc._reloadDatabase(ctx, reloadDbName, false)
	sc.lock.Unlock()

	return dbContext, err
}

func (sc *ServerContext) ReloadDatabaseWithConfig(nonContextStruct base.NonCancellableContext, config DatabaseConfig) error {
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc._reloadDatabaseWithConfig(nonContextStruct.Ctx, config, true)
}

func (sc *ServerContext) _reloadDatabaseWithConfig(ctx context.Context, config DatabaseConfig, failFast bool) error {
	sc._removeDatabase(ctx, config.Name)
	_, err := sc._getOrAddDatabaseFromConfig(ctx, config, false, db.GetConnectToBucketFn(failFast))
	return err
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) getOrAddDatabaseFromConfig(ctx context.Context, config DatabaseConfig, useExisting bool, openBucketFn db.OpenBucketFn) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	defer sc.lock.Unlock()
	return sc._getOrAddDatabaseFromConfig(ctx, config, useExisting, openBucketFn)
}

func GetBucketSpec(ctx context.Context, config *DatabaseConfig, serverConfig *StartupConfig) (spec base.BucketSpec, err error) {

	spec = config.MakeBucketSpec()

	if serverConfig.Bootstrap.ServerTLSSkipVerify != nil {
		spec.TLSSkipVerify = *serverConfig.Bootstrap.ServerTLSSkipVerify
	}

	if spec.BucketName == "" {
		spec.BucketName = config.Name
	}

	spec.FeedType = strings.ToLower(config.FeedType)

	if config.ViewQueryTimeoutSecs != nil {
		spec.ViewQueryTimeoutSecs = config.ViewQueryTimeoutSecs
	}

	if config.Unsupported != nil && config.Unsupported.KVBufferSize != 0 {
		spec.KvBufferSize = config.Unsupported.KVBufferSize
	}
	if config.Unsupported != nil && config.Unsupported.DCPReadBuffer != 0 {
		spec.DcpBuffer = config.Unsupported.DCPReadBuffer
	}

	spec.UseXattrs = config.UseXattrs()
	if !spec.UseXattrs {
		base.WarnfCtx(ctx, "Running Sync Gateway without shared bucket access is deprecated. Recommendation: set enable_shared_bucket_access=true")
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
// Pass in a bucketFromBucketSpecFn to replace the default ConnectToBucket function. This will cause the failFast argument to be ignored
func (sc *ServerContext) _getOrAddDatabaseFromConfig(ctx context.Context, config DatabaseConfig, useExisting bool, openBucketFn db.OpenBucketFn) (*db.DatabaseContext, error) {

	// Generate bucket spec and validate whether db already exists
	spec, err := GetBucketSpec(ctx, &config, sc.Config)
	if err != nil {
		return nil, err
	}

	dbName := config.Name
	if dbName == "" {
		dbName = spec.BucketName
	}
	if spec.Server == "" {
		spec.Server = sc.Config.Bootstrap.Server
	}
	if sc.Config.IsServerless() {
		params := &base.GoCBConnStringParams{
			KVPoolSize:    base.DefaultGocbKvPoolSizeServerless,
			KVBufferSize:  base.DefaultKvBufferSizeServerless,
			DCPBufferSize: base.DefaultDCPBufferServerless,
		}
		if spec.KvBufferSize != 0 {
			params.KVBufferSize = spec.KvBufferSize
		}
		if spec.DcpBuffer != 0 {
			params.DCPBufferSize = spec.DcpBuffer
		}
		connStr, err := spec.GetGoCBConnString(params)
		if err != nil {
			return nil, err
		}
		spec.Server = connStr
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
	base.InfofCtx(ctx, base.KeyAll, "Opening db /%s as bucket %q, pool %q, server <%s>",
		base.MD(dbName), base.MD(spec.BucketName), base.SD(base.DefaultPool), base.SD(spec.Server))
	bucket, err := openBucketFn(ctx, spec)
	if err != nil {
		return nil, err
	}

	// If using a walrus bucket, force use of views
	useViews := base.BoolDefault(config.UseViews, false)
	if !useViews && spec.IsWalrusBucket() {
		base.WarnfCtx(ctx, "Using GSI is not supported when using a walrus bucket - switching to use views.  Set 'use_views':true in Sync Gateway's database config to avoid this warning.")
		useViews = true
	}

	// initDataStore is a function to initialize Views or GSI indexes for a datastore
	initDataStore := func(ds base.DataStore, metadataIndexes db.CollectionIndexesType, verifySyncInfo bool) (resyncRequired bool, err error) {

		// If this collection uses syncInfo, verify the collection isn't associated with a different database's metadataID
		if verifySyncInfo {
			resyncRequired, err = base.InitSyncInfo(ds, config.MetadataID)
			if err != nil {
				return true, err
			}
		}

		if useViews {
			viewErr := db.InitializeViews(ctx, ds)
			if viewErr != nil {
				return false, viewErr
			}
			return resyncRequired, nil
		}

		gsiSupported := bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql)
		if !gsiSupported {
			return false, errors.New("Sync Gateway was unable to connect to a query node on the provided Couchbase Server cluster.  Ensure a query node is accessible, or set 'use_views':true in Sync Gateway's database config.")
		}

		numReplicas := DefaultNumIndexReplicas
		if config.NumIndexReplicas != nil {
			numReplicas = *config.NumIndexReplicas
		}
		n1qlStore, ok := base.AsN1QLStore(ds)
		if !ok {
			return false, errors.New("Cannot create indexes on non-Couchbase data store.")

		}
		options := db.InitializeIndexOptions{
			FailFast:        false,
			NumReplicas:     numReplicas,
			Serverless:      sc.Config.IsServerless(),
			MetadataIndexes: metadataIndexes,
			UseXattrs:       config.UseXattrs(),
		}
		dsName, ok := base.AsDataStoreName(ds)
		if !ok {
			return false, fmt.Errorf("Could not get datastore name from %s", base.MD(ds.GetName()))
		}
		ctx := base.KeyspaceLogCtx(ctx, bucket.GetName(), dsName.ScopeName(), dsName.CollectionName())
		indexErr := db.InitializeIndexes(ctx, n1qlStore, options)
		if indexErr != nil {
			return false, indexErr
		}

		return resyncRequired, nil
	}

	collectionsRequiringResync := make([]base.ScopeAndCollectionName, 0)
	if len(config.Scopes) > 0 {
		if !bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) {
			return nil, errCollectionsUnsupported
		}

		hasDefaultCollection := false
		for scopeName, scopeConfig := range config.Scopes {
			for collectionName, _ := range scopeConfig.Collections {
				var dataStore sgbucket.DataStore
				err := base.WaitForNoError(func() error {
					dataStore, err = bucket.NamedDataStore(base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName})
					return err
				})
				if err != nil {
					return nil, fmt.Errorf("error attempting to create/update database: %w", err)
				}
				// Check if scope/collection specified exists. Will enter retry loop if connection unsuccessful
				if err := base.WaitUntilDataStoreExists(dataStore); err != nil {
					return nil, fmt.Errorf("attempting to create/update database with a scope/collection that is not found")
				}
				metadataIndexOption := db.IndexesWithoutMetadata
				if base.IsDefaultCollection(scopeName, collectionName) {
					hasDefaultCollection = true
					metadataIndexOption = db.IndexesAll
				}
				requiresResync, err := initDataStore(dataStore, metadataIndexOption, true)
				if err != nil {
					return nil, err
				}
				if requiresResync {
					collectionsRequiringResync = append(collectionsRequiringResync, base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName})
				}
			}
		}
		if !hasDefaultCollection {
			if _, err := initDataStore(bucket.DefaultDataStore(), db.IndexesMetadataOnly, false); err != nil {
				return nil, err
			}
		}
	} else {
		// no scopes configured - init the default data store
		requiresResync, err := initDataStore(bucket.DefaultDataStore(), db.IndexesAll, true)
		if err != nil {
			return nil, err
		}
		if requiresResync {
			collectionsRequiringResync = append(collectionsRequiringResync, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: base.DefaultCollection})
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
	contextOptions, err := dbcOptionsFromConfig(ctx, sc, &config.DbConfig, dbName)
	if err != nil {
		return nil, err
	}
	contextOptions.UseViews = useViews

	javascriptTimeout := getJavascriptTimeout(&config.DbConfig)

	fqCollections := make([]string, 0)
	if len(config.Scopes) > 0 {
		if !sc.persistentConfig && !sc.allowScopesInPersistentConfig {
			return nil, base.HTTPErrorf(http.StatusBadRequest, "scopes are not allowed with legacy config")
		}
		contextOptions.Scopes = make(db.ScopesOptions, len(config.Scopes))
		for scopeName, scopeCfg := range config.Scopes {
			contextOptions.Scopes[scopeName] = db.ScopeOptions{
				Collections: make(map[string]db.CollectionOptions, len(scopeCfg.Collections)),
			}
			for collName, collCfg := range scopeCfg.Collections {
				var importFilter *db.ImportFilterFunction
				if collCfg.ImportFilter != nil {
					importFilter = db.NewImportFilterFunction(*collCfg.ImportFilter, javascriptTimeout)
				}

				contextOptions.Scopes[scopeName].Collections[collName] = db.CollectionOptions{
					Sync:         collCfg.SyncFn,
					ImportFilter: importFilter,
				}
				fqCollections = append(fqCollections, base.FullyQualifiedCollectionName(spec.BucketName, scopeName, collName))
			}
		}
	} else {
		// Set up default import filter
		var importFilter *db.ImportFilterFunction
		if config.ImportFilter != nil {
			importFilter = db.NewImportFilterFunction(*config.ImportFilter, javascriptTimeout)
		}

		contextOptions.Scopes = map[string]db.ScopeOptions{
			base.DefaultScope: db.ScopeOptions{
				Collections: map[string]db.CollectionOptions{
					base.DefaultCollection: {
						Sync:         config.Sync,
						ImportFilter: importFilter,
					},
				},
			},
		}
	}

	// For now, we'll continue writing metadata into `_default`.`_default`, for a few reasons:
	// - we know it is supported in all server versions
	// - it cannot be dropped by customers, we always know it exists
	// - it simplifies RBAC in terms of not having to create a metadata collection
	// Once system scope/collection is well-supported, and we have a migration path, we can consider using those.
	// contextOptions.MetadataStore = bucket.NamedDataStore(base.ScopeAndCollectionName{base.MobileMetadataScope, base.MobileMetadataCollection})
	contextOptions.MetadataStore = bucket.DefaultDataStore()
	err = validateMetadataStore(ctx, contextOptions.MetadataStore)
	if err != nil {
		return nil, err
	}

	// If identified as default database, use metadataID of "" so legacy non namespaced docs are used
	if config.MetadataID != defaultMetadataID {
		contextOptions.MetadataID = config.MetadataID
	}

	// Create the DB Context
	dbcontext, err := db.NewDatabaseContext(ctx, dbName, bucket, autoImport, contextOptions)
	if err != nil {
		return nil, err
	}
	dbcontext.BucketSpec = spec
	dbcontext.ServerContextHasStarted = sc.hasStarted
	dbcontext.NoX509HTTPClient = sc.NoX509HTTPClient
	dbcontext.RequireResync = collectionsRequiringResync

	if config.CORS != nil {
		dbcontext.CORS = config.DbConfig.CORS
	} else {
		dbcontext.CORS = sc.Config.API.CORS
	}

	if config.RevsLimit != nil {
		dbcontext.RevsLimit = *config.RevsLimit
		if dbcontext.AllowConflicts() {
			if dbcontext.RevsLimit < 20 {
				return nil, fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration cannot be set lower than 20.", dbcontext.RevsLimit)
			}

			if dbcontext.RevsLimit < db.DefaultRevsLimitConflicts {
				base.WarnfCtx(ctx, "Setting the revs_limit (%v) to less than %d, whilst having allow_conflicts set to true, may have unwanted results when documents are frequently updated. Please see documentation for details.", dbcontext.RevsLimit, db.DefaultRevsLimitConflicts)
			}
		} else {
			if dbcontext.RevsLimit <= 0 {
				return nil, fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration must be greater than zero.", dbcontext.RevsLimit)
			}
		}
	}

	dbcontext.AllowEmptyPassword = base.BoolDefault(config.AllowEmptyPassword, false)
	dbcontext.ServeInsecureAttachmentTypes = base.BoolDefault(config.ServeInsecureAttachmentTypes, false)

	// Create default users & roles:
	if err := sc.installPrincipals(ctx, dbcontext, config.Roles, "role"); err != nil {
		return nil, err
	}
	if err := sc.installPrincipals(ctx, dbcontext, config.Users, "user"); err != nil {
		return nil, err
	}

	if config.Guest != nil {
		guest := map[string]*auth.PrincipalConfig{base.GuestUsername: config.Guest}
		if err := sc.installPrincipals(ctx, dbcontext, guest, "user"); err != nil {
			return nil, err
		}
	}

	// Initialize event handlers
	if err := sc.initEventHandlers(ctx, dbcontext, &config.DbConfig); err != nil {
		return nil, err
	}

	// Upsert replications
	replicationErr := dbcontext.SGReplicateMgr.PutReplications(config.Replications)
	if replicationErr != nil {
		return nil, replicationErr
	}

	// Register it so HTTP handlers can find it:
	sc.databases_[dbcontext.Name] = dbcontext
	sc.dbConfigs[dbcontext.Name] = &RuntimeDatabaseConfig{DatabaseConfig: config}
	sc.dbRegistry[dbName] = struct{}{}
	for _, name := range fqCollections {
		sc.collectionRegistry[name] = dbName
	}

	if base.BoolDefault(config.StartOffline, false) {
		atomic.StoreUint32(&dbcontext.State, db.DBOffline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "offline", "DB loaded from config", &sc.Config.API.AdminInterface)
	} else if len(dbcontext.RequireResync) > 0 {
		atomic.StoreUint32(&dbcontext.State, db.DBOffline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "offline", "Resync required for collections", &sc.Config.API.AdminInterface)
	} else {
		atomic.StoreUint32(&dbcontext.State, db.DBOnline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "online", "DB loaded from config", &sc.Config.API.AdminInterface)
	}

	dbcontext.StartReplications(ctx)

	return dbcontext, nil
}

// getJavascriptTimeout returns the duration javascript functions can run.
func getJavascriptTimeout(config *DbConfig) time.Duration {
	javascriptTimeout := time.Duration(base.DefaultJavascriptTimeoutSecs) * time.Second
	if config.JavascriptTimeoutSecs != nil {
		javascriptTimeout = time.Duration(*config.JavascriptTimeoutSecs) * time.Second
	}
	return javascriptTimeout
}

// newBaseImportOptions returns a prepopulated ImportOptions struct with values that are database wide.
func newBaseImportOptions(config *DbConfig, serverless bool) *db.ImportOptions {
	// Identify import options
	importOptions := &db.ImportOptions{
		BackupOldRev: base.BoolDefault(config.ImportBackupOldRev, false),
	}

	if config.ImportPartitions == nil {
		importOptions.ImportPartitions = base.GetDefaultImportPartitions(serverless)
	} else {
		importOptions.ImportPartitions = *config.ImportPartitions
	}
	return importOptions

}

func dbcOptionsFromConfig(ctx context.Context, sc *ServerContext, config *DbConfig, dbName string) (db.DatabaseContextOptions, error) {

	// Get timeout to use for import filter function and db context
	javascriptTimeout := getJavascriptTimeout(config)

	// Identify import options
	importOptions := newBaseImportOptions(config, sc.Config.IsServerless())

	// Check for deprecated cache options. If new are set they will take priority but will still log warnings
	warnings := config.deprecatedConfigCacheFallback()
	for _, warnLog := range warnings {
		base.WarnfCtx(ctx, warnLog)
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
		sc.TakeDbOnline(base.NewNonCancelCtx(), dbContext)
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
	base.InfofCtx(ctx, base.KeyAll, "delta_sync enabled=%t with rev_max_age_seconds=%d for database %s", deltaSyncOptions.Enabled, deltaSyncOptions.RevMaxAgeSeconds, dbName)

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
			base.WarnfCtx(ctx, "Using deprecated config parameter 'cache.channel_cache.query_limit'. Use 'query_pagination_limit' instead")
			queryPaginationLimit = *config.CacheConfig.ChannelCacheConfig.DeprecatedQueryLimit
		} else {
			base.WarnfCtx(ctx, "Both query_pagination_limit and the deprecated cache.channel_cache.query_limit have been specified in config - using query_pagination_limit")
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

	secureCookieOverride := sc.Config.API.HTTPS.TLSCertPath != ""
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

	bcryptCost := sc.Config.Auth.BcryptCost
	if bcryptCost <= 0 {
		bcryptCost = auth.DefaultBcryptCost
	}

	slowQueryWarningThreshold := kDefaultSlowQueryWarningThreshold * time.Millisecond
	if config.SlowQueryWarningThresholdMs != nil {
		slowQueryWarningThreshold = time.Duration(*config.SlowQueryWarningThresholdMs) * time.Millisecond
	}

	groupID := ""
	if sc.Config.Bootstrap.ConfigGroupID != PersistentConfigDefaultGroupID {
		groupID = sc.Config.Bootstrap.ConfigGroupID
	}

	if config.AllowConflicts != nil && *config.AllowConflicts {
		base.WarnfCtx(ctx, `Deprecation notice: setting database configuration option "allow_conflicts" to true is due to be removed. In the future, conflicts will not be allowed.`)
	}

	if config.Unsupported.DisableCleanSkippedQuery {
		base.WarnfCtx(ctx, `Deprecation notice: setting databse configuration option "disable_clean_skipped_query" no longer has any functionality. In the future, this option will be removed.`)
	}
	// If basic auth is disabled, it doesn't make sense to send WWW-Authenticate
	sendWWWAuthenticate := config.SendWWWAuthenticateHeader
	if base.BoolDefault(config.DisablePasswordAuth, false) {
		sendWWWAuthenticate = base.BoolPtr(false)
	}

	contextOptions := db.DatabaseContextOptions{
		CacheOptions:                  &cacheOptions,
		RevisionCacheOptions:          revCacheOptions,
		OldRevExpirySeconds:           oldRevExpirySeconds,
		LocalDocExpirySecs:            localDocExpirySecs,
		AdminInterface:                &sc.Config.API.AdminInterface,
		UnsupportedOptions:            config.Unsupported,
		OIDCOptions:                   config.OIDCConfig,
		LocalJWTConfig:                config.LocalJWTConfig,
		DBOnlineCallback:              dbOnlineCallback,
		ImportOptions:                 *importOptions,
		EnableXattr:                   config.UseXattrs(),
		SecureCookieOverride:          secureCookieOverride,
		SessionCookieName:             config.SessionCookieName,
		SessionCookieHttpOnly:         base.BoolDefault(config.SessionCookieHTTPOnly, false),
		AllowConflicts:                config.ConflictsAllowed(),
		SendWWWAuthenticateHeader:     sendWWWAuthenticate,
		DisablePasswordAuthentication: base.BoolDefault(config.DisablePasswordAuth, false),
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
		JavascriptTimeout:         javascriptTimeout,
		Serverless:                sc.Config.IsServerless(),
		// UserQueries:               config.UserQueries,   // behind feature flag (see below)
		// UserFunctions:             config.UserFunctions, // behind feature flag (see below)
		// GraphQL:                   config.GraphQL,       // behind feature flag (see below)
	}

	if sc.Config.Unsupported.UserQueries != nil && *sc.Config.Unsupported.UserQueries {
		var err error
		if config.UserFunctions != nil {
			contextOptions.UserFunctions, err = functions.CompileFunctions(*config.UserFunctions)
			if err != nil {
				return contextOptions, err
			}
		}
		if config.GraphQL != nil {
			contextOptions.GraphQL, err = functions.CompileGraphQL(config.GraphQL)
			if err != nil {
				return contextOptions, err
			}
		}
	} else if config.UserFunctions != nil || config.GraphQL != nil {
		base.WarnfCtx(context.TODO(), `Database config options "functions" and "graphql" ignored because unsupported.user_queries feature flag is not enabled`)
	}

	return contextOptions, nil
}

func (sc *ServerContext) TakeDbOnline(nonContextStruct base.NonCancellableContext, database *db.DatabaseContext) {

	// Take a write lock on the Database context, so that we can cycle the underlying Database
	// without any other call running concurrently
	database.AccessLock.Lock()
	defer database.AccessLock.Unlock()

	// We can only transition to Online from Offline state
	if atomic.CompareAndSwapUint32(&database.State, db.DBOffline, db.DBStarting) {
		reloadedDb, err := sc.ReloadDatabase(nonContextStruct.Ctx, database.Name)
		if err != nil {
			base.ErrorfCtx(nonContextStruct.Ctx, "Error reloading database from config: %v", err)
			return
		}

		if len(reloadedDb.RequireResync) > 0 {
			base.ErrorfCtx(nonContextStruct.Ctx, "Database has collections that require resync before it can go online: %v", reloadedDb.RequireResync)
			return
		}
		// Reloaded DB should already be online in most cases, but force state to online to handle cases
		// where config specifies offline startup
		atomic.StoreUint32(&reloadedDb.State, db.DBOnline)

	} else {
		base.InfofCtx(nonContextStruct.Ctx, base.KeyCRUD, "Unable to take Database : %v online , database must be in Offline state", base.UD(database.Name))
	}

}

// validateMetadataStore will
func validateMetadataStore(ctx context.Context, metadataStore base.DataStore) error {
	// Check if scope/collection specified exists. Will enter retry loop if connection unsuccessful
	err := base.WaitUntilDataStoreExists(metadataStore)
	if err == nil {
		return nil
	}
	metadataStoreName, ok := base.AsDataStoreName(metadataStore)
	if ok {
		keyspace := strings.Join([]string{metadataStore.GetName(), metadataStoreName.ScopeName(), metadataStoreName.CollectionName()}, base.ScopeCollectionSeparator)
		if base.IsDefaultCollection(metadataStoreName.ScopeName(), metadataStoreName.CollectionName()) {
			base.WarnfCtx(ctx, "_default._default has been deleted from the server for bucket %s, to recover recreate the bucket", metadataStore.GetName())
		}
		return fmt.Errorf("metadata store %s does not exist on couchbase server: %w", base.MD(keyspace), err)
	}
	return fmt.Errorf("metadata store %s does not exist on couchbase server: %w", base.MD(metadataStore.GetName()), err)
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
func (sc *ServerContext) initEventHandlers(ctx context.Context, dbcontext *db.DatabaseContext, config *DbConfig) (err error) {
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
			if conf.Filter == "" {
				base.InfofCtx(ctx, base.KeyEvents, "No filter function defined for event handler %s - everything will be processed", eventType.String())
			}
		}

		// Register event handlers
		if err = sc.processEventHandlersForEvent(ctx, handlers, eventType, dbcontext); err != nil {
			return err
		}
	}

	// WaitForProcess uses string, to support both omitempty and zero values
	customWaitTime := int64(-1)
	if config.EventHandlers.WaitForProcess != "" {
		customWaitTime, err = strconv.ParseInt(config.EventHandlers.WaitForProcess, 10, 0)
		if err != nil {
			customWaitTime = -1
			base.WarnfCtx(ctx, "Error parsing wait_for_process from config, using default %s", err)
		}
	}
	dbcontext.EventMgr.Start(config.EventHandlers.MaxEventProc, int(customWaitTime))

	return nil
}

// Adds a database to the ServerContext given its configuration.  If an existing config is found
// for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfig(ctx context.Context, config DatabaseConfig) (*db.DatabaseContext, error) {
	return sc.getOrAddDatabaseFromConfig(ctx, config, false, db.GetConnectToBucketFn(false))
}

// AddDatabaseFromConfigFailFast adds a database to the ServerContext given its configuration and fails fast.
// If an existing config is found for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfigFailFast(nonContextStruct base.NonCancellableContext, config DatabaseConfig) (*db.DatabaseContext, error) {
	return sc.getOrAddDatabaseFromConfig(nonContextStruct.Ctx, config, false, db.GetConnectToBucketFn(true))
}

func (sc *ServerContext) processEventHandlersForEvent(ctx context.Context, events []*EventConfig, eventType db.EventType, dbcontext *db.DatabaseContext) error {

	for _, event := range events {
		switch event.HandlerType {
		case "webhook":
			wh, err := db.NewWebhook(event.Url, event.Filter, event.Timeout, event.Options)
			if err != nil {
				base.WarnfCtx(ctx, "Error creating webhook %v", err)
				return err
			}
			dbcontext.EventMgr.RegisterEventHandler(wh, eventType)
		default:
			return errors.New(fmt.Sprintf("Unknown event handler type %s", event.HandlerType))
		}

	}
	return nil
}

func (sc *ServerContext) RemoveDatabase(ctx context.Context, dbName string) bool {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	return sc._removeDatabase(ctx, dbName)
}

// _unloadDatabase unloads and stops the database, but does not remove the in-memory config.
func (sc *ServerContext) _unloadDatabase(ctx context.Context, dbName string) bool {
	dbCtx := sc.databases_[dbName]
	if dbCtx == nil {
		return false
	}
	base.InfofCtx(ctx, base.KeyAll, "Closing db /%s (bucket %q)", base.MD(dbCtx.Name), base.MD(dbCtx.Bucket.GetName()))
	dbCtx.Close(ctx)
	delete(sc.databases_, dbName)
	return true
}

// _removeDatabase unloads and removes all references to the given database.
func (sc *ServerContext) _removeDatabase(ctx context.Context, dbName string) bool {
	dbCtx := sc.databases_[dbName]
	if dbCtx == nil {
		return false
	}
	if ok := sc._unloadDatabase(ctx, dbName); !ok {
		return ok
	}
	delete(sc.dbConfigs, dbName)
	delete(sc.dbRegistry, dbName)
	for fqCollection, registryDbName := range sc.collectionRegistry {
		if dbName == registryDbName {
			delete(sc.collectionRegistry, fqCollection)
		}
	}
	return true
}

func (sc *ServerContext) _isDatabaseSuspended(dbName string) bool {
	if config, loaded := sc.dbConfigs[dbName]; loaded && config.isSuspended {
		return true
	}
	return false
}

func (sc *ServerContext) _suspendDatabase(ctx context.Context, dbName string) error {
	dbCtx := sc.databases_[dbName]
	if dbCtx == nil {
		return base.ErrNotFound
	}

	config := sc.dbConfigs[dbName]
	if config != nil && !base.BoolDefault(config.Suspendable, sc.Config.IsServerless()) {
		return ErrSuspendingDisallowed
	}

	bucket := dbCtx.Bucket.GetName()
	base.InfofCtx(context.TODO(), base.KeyAll, "Suspending db %q (bucket %q)", base.MD(dbName), base.MD(bucket))

	if !sc._unloadDatabase(ctx, dbName) {
		return base.ErrNotFound
	}

	config.isSuspended = true
	return nil
}

func (sc *ServerContext) unsuspendDatabase(ctx context.Context, dbName string) (*db.DatabaseContext, error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	return sc._unsuspendDatabase(ctx, dbName)
}

func (sc *ServerContext) _unsuspendDatabase(ctx context.Context, dbName string) (*db.DatabaseContext, error) {
	dbCtx := sc.databases_[dbName]
	if dbCtx != nil {
		return dbCtx, nil
	}

	// Check if database is in dbConfigs so no need to search through buckets
	if dbConfig, ok := sc.dbConfigs[dbName]; ok {
		if !dbConfig.isSuspended {
			base.WarnfCtx(ctx, "attempting to unsuspend database %q that is not suspended", base.MD(dbName))
		}
		if !base.BoolDefault(dbConfig.Suspendable, sc.Config.IsServerless()) {
			base.InfofCtx(ctx, base.KeyAll, "attempting to unsuspend db %q while not configured to be suspendable", base.MD(dbName))
		}

		bucket := dbName
		if dbConfig.Bucket != nil {
			bucket = *dbConfig.Bucket
		}

		cas, err := sc.BootstrapContext.GetConfig(bucket, sc.Config.Bootstrap.ConfigGroupID, dbName, &dbConfig.DatabaseConfig)
		if err == base.ErrNotFound {
			// Database no longer exists, so clean up dbConfigs
			base.InfofCtx(ctx, base.KeyConfig, "Database %q has been removed while suspended from bucket %q", base.MD(dbName), base.MD(bucket))
			delete(sc.dbConfigs, dbName)
			return nil, err
		} else if err != nil {
			return nil, fmt.Errorf("unsuspending db %q failed due to an error while trying to retrieve latest config from bucket %q: %w", base.MD(dbName).Redact(), base.MD(bucket).Redact(), err)
		}
		dbConfig.cfgCas = cas
		dbCtx, err = sc._getOrAddDatabaseFromConfig(ctx, dbConfig.DatabaseConfig, false, db.GetConnectToBucketFn(false))
		if err != nil {
			return nil, err
		}
		return dbCtx, nil
	}

	return nil, base.ErrNotFound
}

func (sc *ServerContext) installPrincipals(ctx context.Context, dbc *db.DatabaseContext, spec map[string]*auth.PrincipalConfig, what string) error {
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
			_, err = dbc.UpdatePrincipal(ctx, princ, (what == "user"), isGuest)
			if err != nil {
				if status, _ := base.ErrorAsHTTPStatus(err); status == http.StatusConflict {
					// Ignore and absorb this error if it's a conflict error, which just means that updatePrincipal didn't overwrite an existing user.
					// Since if there's an existing user it's "mission accomplished", this can be treated as a success case.
					createdPrincipal = false
					return false, nil, nil
				}

				if err == base.ErrViewTimeoutError {
					// Timeout error, possibly due to view re-indexing, so retry
					base.InfofCtx(ctx, base.KeyAuth, "Error calling UpdatePrincipal(): %v.  Will retry in case this is a temporary error", err)
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
			base.InfofCtx(ctx, base.KeyAll, "Reset guest user to config")
		} else if createdPrincipal {
			base.InfofCtx(ctx, base.KeyAll, "Created %s %q", what, base.UD(name))
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

func (sc *ServerContext) startStatsLogger(ctx context.Context) {

	if sc.Config.Unsupported.StatsLogFrequency == nil || sc.Config.Unsupported.StatsLogFrequency.Value() == 0 {
		// don't start the stats logger when explicitly zero
		return
	}

	interval := sc.Config.Unsupported.StatsLogFrequency

	sc.statsContext.statsLoggingTicker = time.NewTicker(interval.Value())
	sc.statsContext.terminator = make(chan struct{})
	sc.statsContext.doneChan = make(chan struct{})
	go func() {
		defer close(sc.statsContext.doneChan)
		for {
			select {
			case <-sc.statsContext.statsLoggingTicker.C:
				err := sc.logStats(ctx)
				if err != nil {
					base.WarnfCtx(ctx, "Error logging stats: %v", err)
				}
			case <-sc.statsContext.terminator:
				base.DebugfCtx(ctx, base.KeyAll, "Stopping stats logging goroutine")
				sc.statsContext.statsLoggingTicker.Stop()
				return
			}
		}
	}()
	base.InfofCtx(ctx, base.KeyAll, "Logging stats with frequency: %v", interval)

}

func (sc *ServerContext) logStats(ctx context.Context) error {

	AddGoRuntimeStats()

	sc.logNetworkInterfaceStats(ctx)

	if err := sc.statsContext.addGoSigarStats(); err != nil {
		base.WarnfCtx(ctx, "Error getting sigar based system resource stats: %v", err)
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

func (sc *ServerContext) logNetworkInterfaceStats(ctx context.Context) {

	if err := sc.statsContext.addPublicNetworkInterfaceStatsForHostnamePort(sc.Config.API.PublicInterface); err != nil {
		base.WarnfCtx(ctx, "Error getting public network interface resource stats: %v", err)
	}

	if err := sc.statsContext.addAdminNetworkInterfaceStatsForHostnamePort(sc.Config.API.AdminInterface); err != nil {
		base.WarnfCtx(ctx, "Error getting admin network interface resource stats: %v", err)
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

func initClusterAgent(ctx context.Context, clusterAddress, clusterUser, clusterPass, certPath, keyPath, caCertPath string, tlsSkipVerify *bool) (*gocbcore.Agent, error) {
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
				base.WarnfCtx(ctx, "unable to close gocb agent: %v", err)
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
func (sc *ServerContext) initializeGoCBAgent(ctx context.Context) (*gocbcore.Agent, error) {
	err, a := base.RetryLoop("Initialize Cluster Agent", func() (shouldRetry bool, err error, value interface{}) {
		agent, err := initClusterAgent(
			ctx,
			sc.Config.Bootstrap.Server, sc.Config.Bootstrap.Username, sc.Config.Bootstrap.Password,
			sc.Config.Bootstrap.X509CertPath, sc.Config.Bootstrap.X509KeyPath, sc.Config.Bootstrap.CACertPath, sc.Config.Bootstrap.ServerTLSSkipVerify)
		if err != nil {
			base.InfofCtx(ctx, base.KeyConfig, "Couldn't initialize cluster agent: %v - will retry...", err)
			return true, err, nil
		}

		return false, nil, agent
	}, base.CreateSleeperFunc(27, 1000)) // ~2 mins total - 5 second gocb WaitUntilReady timeout and 1 second interval
	if err != nil {
		return nil, err
	}

	base.InfofCtx(ctx, base.KeyConfig, "Successfully initialized cluster agent")
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
	tlsRootCAProvider, err := base.GoCBCoreTLSRootCAProvider(sc.Config.Bootstrap.ServerTLSSkipVerify, sc.Config.Bootstrap.CACertPath)
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

	bodyString, err := io.ReadAll(httpResponse.Body)
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
func (sc *ServerContext) Database(ctx context.Context, name string) *db.DatabaseContext {
	db, err := sc.GetDatabase(ctx, name)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error getting db %q: %v", name, err))
	}
	return db
}

func (sc *ServerContext) initializeCouchbaseServerConnections(ctx context.Context) error {
	base.InfofCtx(ctx, base.KeyAll, "initializing server connections")
	defer func() {
		base.InfofCtx(ctx, base.KeyAll, "finished initializing server connections")
	}()
	goCBAgent, err := sc.initializeGoCBAgent(ctx)
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
		couchbaseCluster, err := CreateCouchbaseClusterFromStartupConfig(sc.Config, base.CachedClusterConnections)
		if err != nil {
			return err
		}
		if sc.Config.IsServerless() {
			err := couchbaseCluster.SetConnectionStringServerless()
			if err != nil {
				return err
			}
		}

		sc.BootstrapContext.Connection = couchbaseCluster

		// Check for v3.0 persisted configs, migrate to registry format if found
		err = sc.migrateV30Configs(ctx)
		if err != nil {
			base.InfofCtx(ctx, base.KeyConfig, "Unable to migrate v3.0 config to registry - will not be migrated: %v", err)
		}

		count, err := sc.fetchAndLoadConfigs(ctx, true)
		if err != nil {
			return err
		}

		if count > 0 {
			base.InfofCtx(ctx, base.KeyConfig, "Successfully fetched %d database configs from buckets in cluster", count)
		} else {
			base.WarnfCtx(ctx, "Config: No database configs for group %q. Continuing startup to allow REST API database creation", sc.Config.Bootstrap.ConfigGroupID)
		}

		if sc.Config.Bootstrap.ConfigUpdateFrequency.Value() > 0 {
			sc.BootstrapContext.terminator = make(chan struct{})
			sc.BootstrapContext.doneChan = make(chan struct{})

			base.InfofCtx(ctx, base.KeyConfig, "Starting background polling for new configs/buckets: %s", sc.Config.Bootstrap.ConfigUpdateFrequency.Value().String())
			go func() {
				defer close(sc.BootstrapContext.doneChan)
				defer sc.BootstrapContext.Connection.Close()
				t := time.NewTicker(sc.Config.Bootstrap.ConfigUpdateFrequency.Value())
				for {
					select {
					case <-sc.BootstrapContext.terminator:
						base.InfofCtx(ctx, base.KeyConfig, "Stopping background config polling loop")
						t.Stop()
						return
					case <-t.C:
						base.DebugfCtx(ctx, base.KeyConfig, "Fetching configs from buckets in cluster for group %q", sc.Config.Bootstrap.ConfigGroupID)
						count, err := sc.fetchAndLoadConfigs(ctx, false)
						if err != nil {
							base.WarnfCtx(ctx, "Couldn't load configs from bucket when polled: %v", err)
						}
						if count > 0 {
							base.InfofCtx(ctx, base.KeyConfig, "Successfully fetched %d database configs from buckets in cluster", count)
						}
					}
				}
			}()
		} else {
			base.InfofCtx(ctx, base.KeyConfig, "Disabled background polling for new configs/buckets")
		}
	}

	return nil
}

func (sc *ServerContext) AddServerLogContext(parent context.Context) context.Context {
	// ServerLogContext is separate from standard LogContext, so this does not reset the log context
	if sc != nil && sc.LogContextID != "" {
		return base.LogContextWith(parent, &base.ServerLogContext{LogContextID: sc.LogContextID})
	}
	return parent
}

func (sc *ServerContext) SetContextLogID(parent context.Context, id string) context.Context {
	if sc != nil {
		sc.LogContextID = id
		// ServerLogContext is separate from standard LogContext, so this does not reset the log context
		return base.LogContextWith(parent, &base.ServerLogContext{LogContextID: sc.LogContextID})
	}
	return parent
}
