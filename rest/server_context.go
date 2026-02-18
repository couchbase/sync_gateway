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
	"maps"
	"net"
	"net/http"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbase/sync_gateway/auth"
	"github.com/couchbase/sync_gateway/db/functions"
	"github.com/shirou/gopsutil/v4/mem"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

const kDefaultSlowQueryWarningThreshold uint32 = 500 // ms
const KDefaultNumShards = 16

// defaultBytesStatsReportingInterval is the default interval when to report bytes transferred stats
const defaultBytesStatsReportingInterval = 30 * time.Second

const dbLoadedStateChangeMsg = "DB loaded from config"

const (
	CBXDCRCompatibleMajorVersion = 7
	CBXDCRCompatibleMinorVersion = 6
)

var errCollectionsUnsupported = base.HTTPErrorf(http.StatusBadRequest, "Named collections specified in database config, but not supported by connected Couchbase Server.")

var ErrSuspendingDisallowed = errors.New("database does not allow suspending")

var allServers = []serverType{publicServer, adminServer, metricsServer, diagnosticServer}

// serverInfo represents an instance of an HTTP server from sync gateway
type serverInfo struct {
	server *http.Server // server is the HTTP server instance
	addr   net.Addr     // addr is the addr of the listener, which will be resolvable always, whereas server.Addr can be :0
}

// Shared context of HTTP handlers: primarily a registry of databases by name. It also stores
// the configuration settings so handlers can refer to them.
// This struct is accessed from HTTP handlers running on multiple goroutines, so it needs to
// be thread-safe.
type ServerContext struct {
	Config               *StartupConfig // The current runtime configuration of the node
	initialStartupConfig *StartupConfig // The configuration at startup of the node. Built from config file + flags
	persistentConfig     bool

	_dbRegistry         map[string]struct{}               // _dbRegistry is a map of db names, used to ensure uniqueness even when db isn't active
	_collectionRegistry map[string]string                 // _collectionRegistry is a map of fully qualified collection name to db name, used for local uniqueness checks
	_dbConfigs          map[string]*RuntimeDatabaseConfig // _dbConfigs is a map of db name to the RuntimeDatabaseConfig
	_databases          map[string]*db.DatabaseContext    // _databases is a map of dbname to db.DatabaseContext
	_databasesLock      sync.RWMutex                      // Lock for _databases and other db-specific maps above

	statsContext      *statsContext
	BootstrapContext  *bootstrapContext
	HTTPClient        *http.Client
	cpuPprofFileMutex sync.Mutex // Protect cpuPprofFile from concurrent Start and Stop CPU profiling requests
	cpuPprofFile      *os.File   // An open file descriptor holds the reference during CPU profiling

	_httpServers     map[serverType]*serverInfo // A list of HTTP servers running under the ServerContext
	_httpServersLock sync.RWMutex               // Lock for managing access to _httpServers

	GoCBAgent                     *gocbcore.Agent      // GoCB Agent to use when obtaining management endpoints
	NoX509HTTPClient              *http.Client         // httpClient for the cluster that doesn't include x509 credentials, even if they are configured for the cluster
	hasStarted                    chan struct{}        // A channel that is closed via PostStartup once the ServerContext has fully started
	LogContextID                  string               // ID to differentiate log messages from different server context
	fetchConfigsLastUpdate        time.Time            // The last time fetchConfigsWithTTL() updated dbConfigs
	allowScopesInPersistentConfig bool                 // Test only backdoor to allow scopes in persistent config, not supported for multiple databases with different collections targeting the same bucket
	DatabaseInitManager           *DatabaseInitManager // Manages database initialization (index creation and readiness) independent of database stop/start/reload, when using persistent config
	ActiveReplicationsCounter
	invalidDatabaseConfigTracking invalidDatabaseConfigs
	SGCollect                     *sgCollect // singleton instance for this server's sgcollect_info process
}

type ActiveReplicationsCounter struct {
	activeReplicatorCount int          // The count of concurrent active replicators
	activeReplicatorLimit int          // The limit on number of active replicators allowed
	lock                  sync.RWMutex // Lock for managing access to shared memory location
}

// defaultConfigRetryTimeout is the total retry time when waiting for in-flight config updates.  Set as a multiple of kv op timeout,
// based on the maximum of 3 kv ops for a successful config update
const defaultConfigRetryTimeout = 3 * base.DefaultGocbV2OperationTimeout

type bootstrapContext struct {
	Connection         base.BootstrapConnection
	configRetryTimeout time.Duration               // configRetryTimeout defines the total amount of time to retry on a registry/config mismatch
	terminator         chan struct{}               // Used to stop the goroutine handling the bootstrap polling
	doneChan           chan struct{}               // doneChan is closed when the bootstrap polling goroutine finishes.
	sgVersion          base.ComparableBuildVersion // version of Sync Gateway
}

type getOrAddDatabaseConfigOptions struct {
	failFast          bool            // if set, a failure to connect to a bucket of collection will immediately fail
	useExisting       bool            //  if true, return an existing DatabaseContext vs return an error
	connectToBucketFn db.OpenBucketFn // supply a custom function for buckets, used for testing only
	forceOnline       bool            // force the database to come online, even if startOffline is set
	asyncOnline       bool            // Whether getOrAddDatabaseConfig should block until database is ready, when startOffline=false
	loadFromBucket    bool            // If this is load config from bucket operation
}

func (sc *ServerContext) CreateLocalDatabase(ctx context.Context, dbs DbConfigMap) error {
	for _, dbConfig := range dbs {
		dbc := dbConfig.ToDatabaseConfig()
		_, err := sc._getOrAddDatabaseFromConfig(ctx, *dbc, getOrAddDatabaseConfigOptions{
			useExisting: false,
			failFast:    false,
		})
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

func (sc *ServerContext) CloseCpuPprofFile(ctx context.Context) (filename string) {
	sc.cpuPprofFileMutex.Lock()
	if sc.cpuPprofFile != nil {
		filename = sc.cpuPprofFile.Name()
	}
	if err := sc.cpuPprofFile.Close(); err != nil {
		base.WarnfCtx(ctx, "Error closing CPU profile file: %v", err)
	}
	sc.cpuPprofFile = nil
	sc.cpuPprofFileMutex.Unlock()
	return filename
}

func NewServerContext(ctx context.Context, config *StartupConfig, persistentConfig bool) *ServerContext {

	sc := &ServerContext{
		Config:              config,
		persistentConfig:    persistentConfig,
		_dbRegistry:         map[string]struct{}{},
		_collectionRegistry: map[string]string{},
		_dbConfigs:          map[string]*RuntimeDatabaseConfig{},
		_databases:          map[string]*db.DatabaseContext{},
		DatabaseInitManager: &DatabaseInitManager{},
		HTTPClient:          http.DefaultClient,
		statsContext:        &statsContext{heapProfileEnabled: !config.HeapProfileDisableCollection},
		BootstrapContext:    &bootstrapContext{sgVersion: *base.ProductVersion},
		hasStarted:          make(chan struct{}),
		_httpServers:        map[serverType]*serverInfo{},
		SGCollect:           newSGCollect(ctx),
	}
	sc.invalidDatabaseConfigTracking = invalidDatabaseConfigs{
		dbNames: map[string]*invalidConfigInfo{},
	}

	if base.ServerIsWalrus(sc.Config.Bootstrap.Server) {
		// Disable Admin API authentication when running as walrus on the default admin interface to support dev
		// environments.
		if sc.Config.API.AdminInterface == DefaultAdminInterface {
			sc.Config.API.AdminInterfaceAuthentication = base.Ptr(false)
			sc.Config.API.MetricsInterfaceAuthentication = base.Ptr(false)
		}
	}
	if config.Replicator.MaxConcurrentReplications != 0 {
		sc.ActiveReplicationsCounter.activeReplicatorLimit = config.Replicator.MaxConcurrentReplications
	}

	if config.HeapProfileCollectionThreshold != nil {
		sc.statsContext.heapProfileCollectionThreshold = *config.HeapProfileCollectionThreshold
	} else {
		memoryTotal := getTotalMemory(ctx)
		if memoryTotal != 0 {
			sc.statsContext.heapProfileCollectionThreshold = uint64(float64(memoryTotal) * 0.85)
		} else {
			base.WarnfCtx(ctx, "Could not determine system memory, disabling automatic heap profile collection")
			sc.statsContext.heapProfileEnabled = false
		}
	}

	sc.startStatsLogger(ctx)

	return sc
}

func (sc *ServerContext) WaitForRESTAPIs(ctx context.Context) error {
	timeout := 30 * time.Second
	interval := time.Millisecond * 100
	numAttempts := int(timeout / interval)
	ctx, cancelFn := context.WithTimeout(ctx, timeout)
	defer cancelFn()
	err, _ := base.RetryLoop(ctx, "Wait for REST APIs", func() (shouldRetry bool, err error, value any) {
		sc._httpServersLock.RLock()
		defer sc._httpServersLock.RUnlock()
		if len(sc._httpServers) == len(allServers) {
			return false, nil, nil
		}
		return true, nil, nil
	}, base.CreateSleeperFunc(numAttempts, int(interval.Milliseconds())))
	return err
}

// getServerAddr returns the address as assigned by the listener. This will return an addressable address, whereas ":0" is a valid value to pass to server.
func (sc *ServerContext) getServerAddr(s serverType) (string, error) {
	server, err := sc.getHTTPServer(s)
	if err != nil {
		return "", err
	}
	return server.addr.String(), nil
}

func (sc *ServerContext) addHTTPServer(t serverType, s *serverInfo) {
	sc._httpServersLock.Lock()
	defer sc._httpServersLock.Unlock()
	sc._httpServers[t] = s
}

// getHTTPServer returns information about the given HTTP server.
func (sc *ServerContext) getHTTPServer(t serverType) (*serverInfo, error) {
	sc._httpServersLock.RLock()
	defer sc._httpServersLock.RUnlock()
	s, ok := sc._httpServers[t]
	if !ok {
		return nil, fmt.Errorf("server type %q not found running in server context", t)
	}
	return s, nil
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
	// stop HTTP servers - prevents any further requests from coming in before we continue with tearing down everything else
	sc.stopHTTPServers(ctx)

	// stop the config polling
	if err := base.TerminateAndWaitForClose(sc.BootstrapContext.terminator, sc.BootstrapContext.doneChan, serverContextStopMaxWait); err != nil {
		base.InfofCtx(ctx, base.KeyAll, "Couldn't stop background config update worker: %v", err)
	}

	// close cached bootstrap bucket connections for config polling
	if sc.BootstrapContext != nil && sc.BootstrapContext.Connection != nil {
		sc.BootstrapContext.Connection.Close()
	}

	if agent := sc.GoCBAgent; agent != nil {
		if err := agent.Close(); err != nil {
			base.WarnfCtx(ctx, "Error closing agent connection: %v", err)
		}
	}

	if err := base.TerminateAndWaitForClose(sc.statsContext.terminator, sc.statsContext.doneChan, serverContextStopMaxWait); err != nil {
		base.InfofCtx(ctx, base.KeyAll, "Couldn't stop stats logger: %v", err)
	}

	// close all databases
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()
	for _, db := range sc._databases {
		db.Close(ctx)
		_ = db.EventMgr.RaiseDBStateChangeEvent(ctx, db.Name, "offline", "Database context closed", &sc.Config.API.AdminInterface)
	}
	sc._databases = nil
	sc.invalidDatabaseConfigTracking.dbNames = nil
}

func (sc *ServerContext) stopHTTPServers(ctx context.Context) {
	sc._httpServersLock.Lock()
	defer sc._httpServersLock.Unlock()
	for _, s := range sc._httpServers {
		if s.server != nil {
			base.InfofCtx(ctx, base.KeyHTTP, "Closing HTTP Server: %v", s.addr)
			if err := s.server.Close(); err != nil {
				base.WarnfCtx(ctx, "Error closing HTTP server %q: %v", s.addr, err)
			}
		}
	}
	sc._httpServers = nil
}

// GetDatabase attempts to return the DatabaseContext of the database. It will load the database if necessary.
func (sc *ServerContext) GetDatabase(ctx context.Context, name string) (*db.DatabaseContext, error) {
	dbc, err := sc.GetActiveDatabase(name)
	if err == base.ErrNotFound {
		dbc, _, err := sc.GetInactiveDatabase(ctx, name)
		return dbc, err
	}
	return dbc, err
}

// GetActiveDatabase attempts to return the DatabaseContext of a loaded database. If not found, the database name will be
// validated to make sure it's valid and then an error returned.
func (sc *ServerContext) GetActiveDatabase(name string) (*db.DatabaseContext, error) {
	sc._databasesLock.RLock()
	dbc := sc._databases[name]
	sc._databasesLock.RUnlock()
	if dbc != nil {
		return dbc, nil
	} else if db.ValidateDatabaseName(name) != nil {
		return nil, base.HTTPErrorf(http.StatusBadRequest, "invalid database name %q", name)
	}
	return nil, base.ErrNotFound
}

// GetInactiveDatabase attempts to load the database and return it's DatabaseContext. It will first attempt to unsuspend the
// database, and if that fails, try to load the database from the buckets.
// This should be used if GetActiveDatabase fails. Turns the database context, a variable to say if the config exists, and an error.
func (sc *ServerContext) GetInactiveDatabase(ctx context.Context, name string) (*db.DatabaseContext, bool, error) {
	dbc, err := sc.unsuspendDatabase(ctx, name)
	if err != nil && err != base.ErrNotFound && err != ErrSuspendingDisallowed {
		return nil, false, err
	} else if err == nil {
		return dbc, true, nil
	}

	var dbConfigFound bool
	// database not loaded, fallback to fetching it from cluster
	if sc.BootstrapContext.Connection != nil {
		if sc.Config.IsServerless() {
			dbConfigFound, _ = sc.fetchAndLoadDatabaseSince(ctx, name, sc.Config.Unsupported.Serverless.MinConfigFetchInterval)

		} else {
			dbConfigFound, _ = sc.fetchAndLoadDatabase(base.NewNonCancelCtx(), name, false)
		}
		if dbConfigFound {
			sc._databasesLock.RLock()
			defer sc._databasesLock.RUnlock()
			dbc := sc._databases[name]
			if dbc != nil {
				return dbc, dbConfigFound, nil
			}
		}
	}
	// handle the correct error message being returned for a corrupt database config
	var httpErr *base.HTTPError
	invalidConfig, ok := sc.invalidDatabaseConfigTracking.exists(name)
	if !dbConfigFound && ok {
		httpErr = sc.buildErrorMessage(name, invalidConfig)
	} else {
		httpErr = base.HTTPErrorf(http.StatusNotFound, "no such database %q", name)
	}

	return nil, dbConfigFound, httpErr
}

// buildErrorMessage will build appropriate http error message based on the invalidConfig
func (sc *ServerContext) buildErrorMessage(dbName string, invalidConfig *invalidConfigInfo) *base.HTTPError {
	if invalidConfig.databaseError != nil {
		err := base.HTTPErrorf(http.StatusNotFound, "Database %s has an invalid configuration: %v. You must update database config immediately through create db process", dbName, invalidConfig.databaseError.ErrMsg)
		return err
	}
	if invalidConfig.collectionConflicts {
		err := base.HTTPErrorf(http.StatusNotFound, "Database %s has conflicting collections. You must update database config immediately through create db process", dbName)
		return err
	}
	return base.HTTPErrorf(http.StatusNotFound, "Mismatch in database config for database %s bucket name: %s and backend bucket: %s groupID: %s You must update database config immediately", base.MD(dbName), base.MD(invalidConfig.configBucketName), base.MD(invalidConfig.persistedBucketName), base.MD(sc.Config.Bootstrap.ConfigGroupID))
}

func (sc *ServerContext) GetDbConfig(name string) *DbConfig {
	if dbConfig := sc.GetDatabaseConfig(name); dbConfig != nil {
		return &dbConfig.DbConfig
	}
	return nil
}

func (sc *ServerContext) GetDatabaseConfig(name string) *RuntimeDatabaseConfig {
	sc._databasesLock.RLock()
	config, ok := sc._dbConfigs[name]
	sc._databasesLock.RUnlock()
	if !ok {
		return nil
	}
	return config
}

func (sc *ServerContext) AllDatabaseNames() []string {
	sc._databasesLock.RLock()
	defer sc._databasesLock.RUnlock()

	names := make([]string, 0, len(sc._databases))
	for name := range sc._databases {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

func (sc *ServerContext) allDatabaseSummaries() []DbSummary {
	sc._databasesLock.RLock()
	defer sc._databasesLock.RUnlock()

	dbs := make([]DbSummary, 0, len(sc._databases))
	for name, dbctx := range sc._databases {
		state := db.RunStateString[atomic.LoadUint32(&dbctx.State)]
		summary := DbSummary{
			DBName:        name,
			Bucket:        dbctx.Bucket.GetName(),
			State:         state,
			DatabaseError: dbctx.DatabaseStartupError,
		}
		if state == db.RunStateString[db.DBOffline] {
			if len(dbctx.RequireResync.ScopeAndCollectionNames()) > 0 {
				summary.RequireResync = true
			}
		}
		if sc.DatabaseInitManager.HasActiveInitialization(name) {
			summary.InitializationActive = true
		}
		dbs = append(dbs, summary)
	}
	sc.invalidDatabaseConfigTracking.m.RLock()
	defer sc.invalidDatabaseConfigTracking.m.RUnlock()
	for name, invalidConfig := range sc.invalidDatabaseConfigTracking.dbNames {
		// skip adding any invalid dbs with no error associated with them or that exist in above list
		if invalidConfig.databaseError == nil || sc._databases[name] != nil {
			continue
		}
		summary := DbSummary{
			DBName:        name,
			Bucket:        invalidConfig.configBucketName,
			State:         db.RunStateString[db.DBOffline], // db can always be reported as offline if we have an invalid config
			DatabaseError: invalidConfig.databaseError,
		}
		dbs = append(dbs, summary)
	}
	sort.Slice(dbs, func(i, j int) bool {
		return dbs[i].DBName < dbs[j].DBName
	})
	return dbs
}

// AllDatabases returns a copy of the _databases map.
func (sc *ServerContext) AllDatabases() map[string]*db.DatabaseContext {
	sc._databasesLock.RLock()
	defer sc._databasesLock.RUnlock()

	databases := make(map[string]*db.DatabaseContext, len(sc._databases))

	maps.Copy(databases, sc._databases)
	return databases
}

type PostUpgradeResult map[string]PostUpgradeDatabaseResult

type PostUpgradeDatabaseResult struct {
	RemovedDDocs   []string `json:"removed_design_docs"`
	RemovedIndexes []string `json:"removed_indexes"`
}

// PostUpgrade performs post-upgrade processing for each database
func (sc *ServerContext) PostUpgrade(ctx context.Context, preview bool) (postUpgradeResults PostUpgradeResult, err error) {
	sc._databasesLock.RLock()
	defer sc._databasesLock.RUnlock()

	var errs *base.MultiError
	buckets := make(map[string]base.Bucket)                       // map of bucket name to bucket object
	dbs := make(map[string]string, len(sc._databases))            // map of db name to bucket name
	dbDesignDocs := make(map[string][]string, len(sc._databases)) // map of db name to removed design docs
	bucketInUseIndexes := make(map[string]db.CollectionIndexes)   // map of buckets to in use index names
	bucketRemovedIndexes := make(map[string][]string)             // map of bucket name to removed index names
	for dbName, database := range sc._databases {
		bucketName := database.Bucket.GetName()
		dbs[dbName] = bucketName
		buckets[bucketName] = database.Bucket
		// View cleanup
		removedDDocs, err := database.RemoveObsoleteDesignDocs(ctx, preview)
		if err != nil {
			errs = errs.Append(fmt.Errorf("Error removing obsolete design docs for database %q: %v", dbName, err))
			continue
		}
		dbDesignDocs[dbName] = removedDDocs

		// Index cleanup
		inUseIndexes := database.GetInUseIndexes()
		if _, ok := bucketInUseIndexes[database.Bucket.GetName()]; !ok {
			bucketInUseIndexes[database.Bucket.GetName()] = make(db.CollectionIndexes)
		}
		for dsName, indexes := range inUseIndexes {
			if _, ok := bucketInUseIndexes[database.Bucket.GetName()][dsName]; !ok {
				bucketInUseIndexes[database.Bucket.GetName()][dsName] = make(map[string]struct{})
			}
			for indexName := range indexes {
				bucketInUseIndexes[database.Bucket.GetName()][dsName][indexName] = struct{}{}
			}
		}
	}

	for bucketName, indexes := range bucketInUseIndexes {
		bucket, ok := buckets[bucketName]
		if !ok {
			errs = errs.Append(fmt.Errorf("Error getting bucket %q for index cleanup: %v", bucketName, err))
			continue
		}
		if !bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql) {
			continue
		}
		removedIndexes, err := db.RemoveUnusedIndexes(ctx, bucket, indexes, preview)
		if err != nil {
			errs = errs.Append(fmt.Errorf("Error removing obsolete indexes for bucket %q: %v", bucketName, err))
			continue
		}
		bucketRemovedIndexes[bucketName] = removedIndexes
	}
	postUpgradeResults = make(map[string]PostUpgradeDatabaseResult, len(sc._databases))
	for dbName, database := range sc._databases {
		postUpgradeResults[dbName] = PostUpgradeDatabaseResult{
			RemovedDDocs:   dbDesignDocs[dbName],
			RemovedIndexes: bucketRemovedIndexes[database.Bucket.GetName()],
		}
	}
	return postUpgradeResults, errs.ErrorOrNil()

}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) _reloadDatabase(ctx context.Context, reloadDbName string, failFast bool, forceOnline bool) (*db.DatabaseContext, error) {
	sc._unloadDatabase(ctx, reloadDbName)
	config := sc._dbConfigs[reloadDbName]
	return sc._getOrAddDatabaseFromConfig(ctx, config.DatabaseConfig, getOrAddDatabaseConfigOptions{
		useExisting: true,
		failFast:    failFast,
		forceOnline: forceOnline,
	})
}

// Removes and re-adds a database to the ServerContext.
func (sc *ServerContext) ReloadDatabase(ctx context.Context, reloadDbName string, forceOnline bool) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()
	return sc._reloadDatabase(ctx, reloadDbName, false, forceOnline)
}

func (sc *ServerContext) ReloadDatabaseWithConfig(nonContextStruct base.NonCancellableContext, config DatabaseConfig) error {
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()
	return sc._reloadDatabaseWithConfig(nonContextStruct.Ctx, config, true, false)
}

func (sc *ServerContext) _reloadDatabaseWithConfig(ctx context.Context, config DatabaseConfig, failFast bool, loadFromBucket bool) error {
	sc._removeDatabase(ctx, config.Name)
	// use async initialization whenever using persistent config
	asyncOnline := sc.persistentConfig
	_, err := sc._getOrAddDatabaseFromConfig(ctx, config, getOrAddDatabaseConfigOptions{
		useExisting:    false,
		failFast:       failFast,
		asyncOnline:    asyncOnline,
		loadFromBucket: loadFromBucket,
	})
	return err
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) getOrAddDatabaseFromConfig(ctx context.Context, config DatabaseConfig, options getOrAddDatabaseConfigOptions) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()
	return sc._getOrAddDatabaseFromConfig(ctx, config, options)
}

// GetBucketSpec returns a BucketSpec from a given DatabaseConfig and StartupConfig.
func GetBucketSpec(ctx context.Context, config *DatabaseConfig, serverConfig *StartupConfig) (base.BucketSpec, error) {

	var server string
	if config.Server != nil && *config.Server != "" {
		server = *config.Server
	} else {
		server = serverConfig.Bootstrap.Server
	}

	if !base.ServerIsWalrus(server) {
		var params *base.GoCBConnStringParams
		if serverConfig.IsServerless() {
			params = base.DefaultServerlessGoCBConnStringParams()
		} else {
			params = base.DefaultGoCBConnStringParams()
		}
		if config.Unsupported != nil {
			if config.Unsupported.DCPReadBuffer != 0 {
				params.DcpBufferSize = config.Unsupported.DCPReadBuffer
			}
			if config.Unsupported.KVBufferSize != 0 {
				params.KvBufferSize = config.Unsupported.KVBufferSize
			}
		}
		connStr, err := base.GetGoCBConnStringWithDefaults(server, params)
		if err != nil {
			return base.BucketSpec{}, err
		}
		server = connStr
	}

	spec := config.MakeBucketSpec(server)

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
func (sc *ServerContext) _getOrAddDatabaseFromConfig(ctx context.Context, config DatabaseConfig, options getOrAddDatabaseConfigOptions) (dbcontext *db.DatabaseContext, returnedError error) {
	var bucket base.Bucket
	// Generate bucket spec and validate whether db already exists
	spec, err := GetBucketSpec(ctx, &config, sc.Config)
	if err != nil {
		return nil, err
	}

	dbName := config.Name
	if dbName == "" {
		dbName = spec.BucketName
	}

	// we do not have per database logging parameters, but it is still useful to have the database name in the log context. This must be set again after dbcOptionsFromConfig is called.
	ctx = base.DatabaseLogCtx(ctx, dbName, nil)

	defer func() {
		if returnedError == nil {
			return
		}
		// database exists in global map, management is deferred to REST api
		_, dbRegistered := sc._databases[dbName]
		if dbRegistered {
			return
		}
		if dbcontext != nil {
			dbcontext.Close(ctx) // will close underlying bucket
		} else if bucket != nil {
			bucket.Close(ctx)
		}
	}()

	if err := db.ValidateDatabaseName(dbName); err != nil {
		return nil, err
	}

	previousDatabase := sc._databases[dbName]
	if previousDatabase != nil {
		if options.useExisting {
			return previousDatabase, nil
		}

		return nil, base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
			"Duplicate database name %q", dbName)
	}

	if config.DbConfig.CacheConfig != nil {
		if config.DbConfig.CacheConfig.ChannelCacheConfig != nil {
			if config.DbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel != nil && !*config.DbConfig.CacheConfig.ChannelCacheConfig.EnableStarChannel {
				base.WarnfCtx(ctx, `enable_star_channel config option is set to false, set it to true`)
				sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseEnableStarChannelFalseError))
				return nil, errors.New("enable_star_channel in cache config is set to false, please set the value to true")
			}
		}
	}

	// Generate database context options from config and server context
	contextOptions, err := dbcOptionsFromConfig(ctx, sc, &config.DbConfig, dbName)
	if err != nil {
		return nil, err
	}
	// set this early so we have dbName available in db-init related logging, before we have an actual database
	ctx = base.DatabaseLogCtx(ctx, dbName, contextOptions.LoggingConfig)

	// Connect to bucket
	base.InfofCtx(ctx, base.KeyAll, "Opening db /%s as bucket %q, pool %q, server <%s>",
		base.MD(dbName), base.MD(spec.BucketName), base.SD(base.DefaultPool), base.SD(spec.Server))

	// the connectToBucketFn is used for testing seam
	if options.connectToBucketFn != nil {
		// the connectToBucketFn is used for testing seam
		bucket, err = options.connectToBucketFn(ctx, spec, options.failFast)
	} else {
		bucket, err = db.ConnectToBucket(ctx, spec, options.failFast)
	}
	if err != nil {
		if options.loadFromBucket {
			sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseBucketConnectionError))
		}
		return nil, err
	}

	// If using a walrus bucket, force use of views
	contextOptions.UseViews = base.ValDefault(config.UseViews, false)
	if !contextOptions.UseViews && spec.IsWalrusBucket() {
		base.WarnfCtx(ctx, "Using GSI is not supported when using a walrus bucket - switching to use views.  Set 'use_views':true in Sync Gateway's database config to avoid this warning.")
		contextOptions.UseViews = true
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
		if options.loadFromBucket {
			sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseInvalidDatastore))
		}
		return nil, err
	}

	hasDefaultCollection := false
	collectionsRequiringResync := make([]base.ScopeAndCollectionName, 0)
	collectionsRequiringAttachmentMigration := make([]base.ScopeAndCollectionName, 0)
	if len(config.Scopes) > 0 {
		if !bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) {
			return nil, errCollectionsUnsupported
		}

		for scopeName, scopeConfig := range config.Scopes {
			for collectionName := range scopeConfig.Collections {
				scName := base.ScopeAndCollectionName{Scope: scopeName, Collection: collectionName}
				if scName.IsDefault() {
					hasDefaultCollection = true
				}

				dataStore, err := base.GetAndWaitUntilDataStoreReady(ctx, bucket, scName, options.failFast)
				if err != nil {
					if options.loadFromBucket {
						sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseInvalidDatastore))
					}
					return nil, fmt.Errorf("error attempting to create/update database: %w", err)
				}

				// Init views now. If we're using GSI we'll use DatabaseInitManager to handle creation later.
				if contextOptions.UseViews {
					if err := db.InitializeViews(ctx, dataStore); err != nil {
						return nil, err
					}
				}

				// Verify whether the collection is associated with a different database's metadataID - if so, add to set requiring resync
				resyncRequired, requiresAttachmentMigration, err := base.InitSyncInfo(ctx, dataStore, config.MetadataID)
				if err != nil {
					if options.loadFromBucket {
						sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseInitSyncInfoError))
					}
					return nil, err
				}
				if resyncRequired {
					collectionsRequiringResync = append(collectionsRequiringResync, scName)
				}

				if requiresAttachmentMigration {
					collectionsRequiringAttachmentMigration = append(collectionsRequiringAttachmentMigration, scName)
				}

			}
		}
	}
	// no scopes, or the set of collections didn't include `_default` and we'll need to initialize it for metadata.
	if !hasDefaultCollection {
		ds := bucket.DefaultDataStore()
		// No explicitly defined scopes means we'll initialize this as a usable default collection, otherwise it's for metadata only
		if len(config.Scopes) == 0 {
			scName := base.DefaultScopeAndCollectionName()
			resyncRequired, requiresAttachmentMigration, err := base.InitSyncInfo(ctx, ds, config.MetadataID)
			if err != nil {
				if options.loadFromBucket {
					sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseInitSyncInfoError))
				}
				return nil, err
			}
			if resyncRequired {
				collectionsRequiringResync = append(collectionsRequiringResync, scName)
			}

			if requiresAttachmentMigration {
				collectionsRequiringAttachmentMigration = append(collectionsRequiringAttachmentMigration, base.ScopeAndCollectionName{Scope: base.DefaultScope, Collection: base.DefaultCollection})
			}
		}
		if contextOptions.UseViews {
			if err := db.InitializeViews(ctx, ds); err != nil {
				return nil, err
			}
		}

	}

	var (
		dbInitDoneChan chan error
		isAsync        bool // blocks reading dbInitDoneChan if false
	)
	startOffline := base.ValDefault(config.StartOffline, false)
	if !contextOptions.UseViews {
		// Initialize any required indexes
		if gsiSupported := bucket.IsSupported(sgbucket.BucketStoreFeatureN1ql); !gsiSupported {
			return nil, errors.New("Sync Gateway was unable to connect to a query node on the provided Couchbase Server cluster.  Ensure a query node is accessible, or set 'use_views':true in Sync Gateway's database config.")
		}

		metadataStore, ok := contextOptions.MetadataStore.(base.N1QLStore)
		if !ok {
			return nil, errors.New("Bucket %s is not %T and does not support N1QL.")
		}
		contextOptions.UseLegacySyncDocsIndex = db.ShouldUseLegacySyncDocsIndex(ctx, metadataStore, config.UseXattrs())
		if sc.DatabaseInitManager == nil {
			base.AssertfCtx(ctx, "DatabaseInitManager should always be initialized")
			return nil, errors.New("DatabaseInitManager not initialized")
		}

		// If database has been requested to start offline, or there's an active async initialization, use async initialization
		isAsync = startOffline || sc.DatabaseInitManager.HasActiveInitialization(dbName)

		// Initialize indexes using DatabaseInitManager.
		dbInitDoneChan, err = sc.DatabaseInitManager.InitializeDatabase(ctx, sc.Config, &config, contextOptions.UseLegacySyncDocsIndex)
		if err != nil {
			if options.loadFromBucket {
				sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseInitializationIndexError))
			}
			return nil, err
		}
	}

	autoImport, err := config.AutoImportEnabled(ctx)
	if err != nil {
		return nil, err
	}

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
				ctx := base.CollectionLogCtx(ctx, scopeName, collName)

				var importFilter *db.ImportFilterFunction
				if collCfg.ImportFilter != nil {
					importFilter = db.NewImportFilterFunction(ctx, *collCfg.ImportFilter, javascriptTimeout)
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
			importFilter = db.NewImportFilterFunction(ctx, *config.ImportFilter, javascriptTimeout)
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

	// If identified as default database, use metadataID of "" so legacy non namespaced docs are used
	if config.MetadataID != defaultMetadataID {
		contextOptions.MetadataID = config.MetadataID
	}

	contextOptions.BlipStatsReportingInterval = defaultBytesStatsReportingInterval.Milliseconds()
	contextOptions.ImportVersion = config.ImportVersion

	// Create the DB Context
	dbcontext, err = db.NewDatabaseContext(ctx, dbName, bucket, autoImport, contextOptions)
	if err != nil {
		if options.loadFromBucket {
			sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseCreateDatabaseContextError))
		}
		return nil, err
	}
	dbcontext.BucketSpec = spec
	dbcontext.ServerContextHasStarted = sc.hasStarted
	dbcontext.NoX509HTTPClient = sc.NoX509HTTPClient
	dbcontext.RequireResync = collectionsRequiringResync
	dbcontext.RequireAttachmentMigration = collectionsRequiringAttachmentMigration

	if config.CORS != nil {
		dbcontext.CORS = config.DbConfig.CORS
	} else {
		dbcontext.CORS = sc.Config.API.CORS
	}
	if !dbcontext.CORS.IsEmpty() && dbcontext.Options.SecureCookieOverride {
		dbcontext.SameSiteCookieMode = http.SameSiteNoneMode
	}
	if config.Unsupported != nil && config.Unsupported.SameSiteCookie != nil {
		var err error
		dbcontext.SameSiteCookieMode, err = config.Unsupported.GetSameSiteCookieMode()
		if err != nil {
			return nil, err
		}
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
		} else if dbcontext.RevsLimit <= 0 {
			return nil, fmt.Errorf("The revs_limit (%v) value in your Sync Gateway configuration must be greater than zero.", dbcontext.RevsLimit)
		}
	}

	dbcontext.AllowEmptyPassword = base.ValDefault(config.AllowEmptyPassword, false)
	dbcontext.ServeInsecureAttachmentTypes = base.ValDefault(config.ServeInsecureAttachmentTypes, false)

	dbcontext.Options.ConfigPrincipals = &db.ConfigPrincipals{
		Users: config.Users,
		Roles: config.Roles,
		Guest: config.Guest,
	}

	// Initialize event handlers
	if err := sc.initEventHandlers(ctx, dbcontext, &config.DbConfig); err != nil {
		return nil, err
	}

	cfgReplications, err := dbcontext.SGReplicateMgr.GetReplications()
	if err != nil {
		if options.loadFromBucket {
			sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseSGRClusterError))
		}
		return nil, err
	}
	// PUT replications that do not exist
	newReplications := make(map[string]*db.ReplicationConfig)
	for name, replication := range config.Replications {
		_, ok := cfgReplications[name]
		if ok {
			continue
		}
		newReplications[name] = replication
	}
	replicationErr := dbcontext.SGReplicateMgr.PutReplications(ctx, newReplications)
	if replicationErr != nil {
		if options.loadFromBucket {
			sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError(db.DatabaseCreateReplicationError))
		}
		return nil, replicationErr
	}

	if config.AllowConflicts != nil {
		base.WarnfCtx(ctx, "allow_conflicts option is no longer supported. Do not use it in the config")
		if *config.AllowConflicts {
			sc._handleInvalidDatabaseConfig(ctx, spec.BucketName, config, db.NewDatabaseError((db.DatabaseAllowConflictsError)))
			return nil, errors.New("allow_conflicts options is set to true, please remove allow_conflicts option from the database config")
		}
	}

	// startOnlineProcesses will be set to true if the database should be started, either synchronously or asynchronously
	startOnlineProcesses := true
	needsResync := len(collectionsRequiringResync) > 0
	if needsResync || (startOffline && !options.forceOnline) {
		startOnlineProcesses = false
		var stateChangeMsg string
		if needsResync {
			stateChangeMsg = "Resync required for collections"
		} else {
			stateChangeMsg = dbLoadedStateChangeMsg + " in offline state"
		}
		// Defer state change event to after the databases are registered. This should not matter because this function holds ServerContext._databasesLock and _databases can't be read while the lock is present.
		defer func() {
			atomic.StoreUint32(&dbcontext.State, db.DBOffline)
			_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(ctx, dbName, "offline", stateChangeMsg, &sc.Config.API.AdminInterface)
		}()
	} else {
		atomic.StoreUint32(&dbcontext.State, db.DBStarting)
	}

	// Register it so HTTP handlers can find it:
	sc._databases[dbcontext.Name] = dbcontext
	sc._dbConfigs[dbcontext.Name] = &RuntimeDatabaseConfig{DatabaseConfig: config}
	sc._dbRegistry[dbName] = struct{}{}
	for _, name := range fqCollections {
		sc._collectionRegistry[name] = dbName
	}

	if !startOnlineProcesses {
		return dbcontext, nil
	}

	// If asyncOnline wasn't specified, block until db init is completed, then start online processes
	if !options.asyncOnline || !isAsync {
		dbcontext.WasInitializedSynchronously = true
		base.InfofCtx(ctx, base.KeyAll, "Waiting for database init to complete...")
		if dbInitDoneChan != nil {
			initError := <-dbInitDoneChan
			if initError != nil {
				dbcontext.DbStats.DatabaseStats.TotalInitFatalErrors.Add(1)
				// report error in building/creating indexes
				dbcontext.DatabaseStartupError = db.NewDatabaseError(db.DatabaseInitializationIndexError)
				atomic.StoreUint32(&dbcontext.State, db.DBOffline)
				_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(ctx, dbName, "offline", dbLoadedStateChangeMsg, &sc.Config.API.AdminInterface)
				return nil, initError
			}
		}
		base.InfofCtx(ctx, base.KeyAll, "Database init completed, starting online processes")
		if err := dbcontext.StartOnlineProcesses(ctx); err != nil {
			dbcontext.DbStats.DatabaseStats.TotalOnlineFatalErrors.Add(1)
			atomic.StoreUint32(&dbcontext.State, db.DBOffline)
			_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(ctx, dbName, "offline", dbLoadedStateChangeMsg, &sc.Config.API.AdminInterface)
			return nil, err
		}
		atomic.StoreUint32(&dbcontext.State, db.DBOnline)
		_ = dbcontext.EventMgr.RaiseDBStateChangeEvent(ctx, dbName, "online", dbLoadedStateChangeMsg, &sc.Config.API.AdminInterface)
		return dbcontext, nil
	}

	// If asyncOnline is requested, set state to Starting and spawn a separate goroutine to wait for init completion
	// before going online
	base.InfofCtx(ctx, base.KeyAll, "Waiting for database init to complete asynchonously...")
	nonCancelCtx := base.NewNonCancelCtxForDatabase(ctx)
	go sc.asyncDatabaseOnline(nonCancelCtx, dbcontext, dbInitDoneChan, config.Version)
	return dbcontext, nil
}

// asyncDatabaseOnline waits for async initialization to complete (based on doneChan).  On successful completion, brings the database online.
// Checks to ensure the database config hasn't been updated while waiting for init to complete - if that happens, doesn't attempt to bring
// db online (it may have been set to offline=true)
func (sc *ServerContext) asyncDatabaseOnline(nonCancelCtx base.NonCancellableContext, dbc *db.DatabaseContext, doneChan chan error, version string) {

	ctx := nonCancelCtx.Ctx
	if doneChan != nil {
		initError := <-doneChan
		if initError != nil {
			dbc.DbStats.DatabaseStats.TotalInitFatalErrors.Add(1)
			base.WarnfCtx(ctx, "Async database init returned error: %v", initError)
			dbc.DatabaseStartupError = db.NewDatabaseError(db.DatabaseInitializationIndexError)
			atomic.CompareAndSwapUint32(&dbc.State, db.DBStarting, db.DBOffline)
			return
		}
	}

	// Before bringing the database online, ensure that the database hasn't been modified while we waited for initialization to complete
	currentDbVersion := sc.GetDbVersion(dbc.Name)
	if currentDbVersion != version {
		base.InfofCtx(ctx, base.KeyConfig, "Database version changed while waiting for async init - cancelling obsolete online request. Old version: %s New version: %s", version, currentDbVersion)
		atomic.CompareAndSwapUint32(&dbc.State, db.DBStarting, db.DBOffline)
		return
	}

	base.InfofCtx(ctx, base.KeyAll, "Async database initialization complete, starting online processes...")
	err := dbc.StartOnlineProcesses(ctx)
	if err != nil {
		dbc.DbStats.DatabaseStats.TotalOnlineFatalErrors.Add(1)
		base.ErrorfCtx(ctx, "Error starting online processes after async initialization: %v", err)
		atomic.CompareAndSwapUint32(&dbc.State, db.DBStarting, db.DBOffline)
		return
	}

	if !atomic.CompareAndSwapUint32(&dbc.State, db.DBStarting, db.DBOnline) {
		// 2nd atomic might end up being Starting here if there's a legitimate race, but it's the most we can do for CAS
		base.PanicfCtx(ctx, "database state wasn't Starting during asyncDatabaseOnline Online transition... now %q", db.RunStateString[atomic.LoadUint32(&dbc.State)])
	}

	_ = dbc.EventMgr.RaiseDBStateChangeEvent(ctx, dbc.Name, "online", dbLoadedStateChangeMsg, &sc.Config.API.AdminInterface)
}

func (sc *ServerContext) GetDbVersion(dbName string) string {
	sc._databasesLock.RLock()
	defer sc._databasesLock.RUnlock()
	currentDbConfig, ok := sc._dbConfigs[dbName]
	if !ok {
		return ""
	}
	return currentDbConfig.Version
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
		BackupOldRev: base.ValDefault(config.ImportBackupOldRev, false),
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
		base.WarnfCtx(ctx, "%s", warnLog)
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
			if config.CacheConfig.RevCacheConfig.MaxItemCount != nil {
				revCacheOptions.MaxItemCount = *config.CacheConfig.RevCacheConfig.MaxItemCount
			}
			if config.CacheConfig.RevCacheConfig.MaxMemoryCountMB != nil {
				maxMemoryConfigValue := *config.CacheConfig.RevCacheConfig.MaxMemoryCountMB
				if maxMemoryConfigValue != uint32(0) && maxMemoryConfigValue < uint32(50) {
					return db.DatabaseContextOptions{}, fmt.Errorf("maximum rev cache memory size cannot be lower than 50 MB")
				}
				revCacheOptions.MaxBytes = int64(*config.CacheConfig.RevCacheConfig.MaxMemoryCountMB * 1024 * 1024) // Convert MB input to bytes
			}
			if config.CacheConfig.RevCacheConfig.ShardCount != nil {
				revCacheOptions.ShardCount = *config.CacheConfig.RevCacheConfig.ShardCount
			}
			if config.CacheConfig.RevCacheConfig.InsertOnWrite != nil {
				revCacheOptions.InsertOnWrite = *config.CacheConfig.RevCacheConfig.InsertOnWrite
			}
		}
	}

	// In sync gateway version 4.0+ we do not support the disabling of use of xattrs
	if !config.UseXattrs() {
		return db.DatabaseContextOptions{}, fmt.Errorf("sync gateway requires enable_shared_bucket_access=true")
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

	compactIntervalSecs := uint32(db.DefaultCompactInterval.Seconds())
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

	var userXattrKey string
	if config.UserXattrKey != nil && *config.UserXattrKey != "" {
		if !base.IsEnterpriseEdition() {
			return db.DatabaseContextOptions{}, fmt.Errorf("user_xattr_key is only supported in enterpise edition")
		}

		if !config.UseXattrs() {
			return db.DatabaseContextOptions{}, fmt.Errorf("use of user_xattr_key requires shared_bucket_access to be enabled")
		}

		userXattrKey = *config.UserXattrKey
	}

	clientPartitionWindow := base.DefaultClientPartitionWindow
	if config.ClientPartitionWindowSecs != nil {
		clientPartitionWindow = time.Duration(*config.ClientPartitionWindowSecs) * time.Second
	}

	bcryptCost := sc.Config.Auth.BcryptCost
	if bcryptCost <= 0 {
		bcryptCost = auth.DefaultBcryptCost
	}

	slowQueryWarningThreshold := time.Duration(kDefaultSlowQueryWarningThreshold) * time.Millisecond
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

	// Process unsupported config options or store runtime defaults if not set
	if config.Unsupported == nil {
		config.Unsupported = &db.UnsupportedOptions{}
	}
	if config.Unsupported.WarningThresholds == nil {
		config.Unsupported.WarningThresholds = &db.WarningThresholds{}
	}

	if config.Unsupported.WarningThresholds.XattrSize == nil {
		config.Unsupported.WarningThresholds.XattrSize = base.Ptr(uint32(base.DefaultWarnThresholdXattrSize))
	} else {
		lowerLimit := 0.1 * 1024 * 1024 // 0.1 MB
		upperLimit := 1 * 1024 * 1024   // 1 MB
		if *config.Unsupported.WarningThresholds.XattrSize < uint32(lowerLimit) {
			return db.DatabaseContextOptions{}, fmt.Errorf("xattr_size warning threshold cannot be lower than %d bytes", uint32(lowerLimit))
		} else if *config.Unsupported.WarningThresholds.XattrSize > uint32(upperLimit) {
			return db.DatabaseContextOptions{}, fmt.Errorf("xattr_size warning threshold cannot be higher than %d bytes", uint32(upperLimit))
		}
	}

	if config.Unsupported.WarningThresholds.ChannelsPerDoc == nil {
		config.Unsupported.WarningThresholds.ChannelsPerDoc = &base.DefaultWarnThresholdChannelsPerDoc
	} else {
		lowerLimit := 5
		if *config.Unsupported.WarningThresholds.ChannelsPerDoc < uint32(lowerLimit) {
			return db.DatabaseContextOptions{}, fmt.Errorf("channels_per_doc warning threshold cannot be lower than %d", lowerLimit)
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
			return db.DatabaseContextOptions{}, fmt.Errorf("access_and_role_grants_per_doc warning threshold cannot be lower than %d", lowerLimit)
		}
	}

	if config.Unsupported.WarningThresholds.ChannelNameSize == nil {
		config.Unsupported.WarningThresholds.ChannelNameSize = &base.DefaultWarnThresholdChannelNameSize
	}

	if config.Unsupported.RejectWritesWithSkippedSequences {
		base.InfofCtx(ctx, base.KeyConfig, "Setting database configuration option 'reject_writes_with_skipped_sequences' to true")
	}

	if config.Unsupported.DisableCleanSkippedQuery {
		base.WarnfCtx(ctx, `Deprecation notice: setting database configuration option "disable_clean_skipped_query" no longer has any functionality. In the future, this option will be removed.`)
	}
	// If basic auth is disabled, it doesn't make sense to send WWW-Authenticate
	sendWWWAuthenticate := config.SendWWWAuthenticateHeader
	if base.ValDefault(config.DisablePasswordAuth, false) {
		sendWWWAuthenticate = base.Ptr(false)
	}

	disablePublicAllDocs := base.ValDefault(config.DisablePublicAllDocs, false)
	if !disablePublicAllDocs {
		base.WarnfCtx(ctx, `Deprecation notice: setting database configuration option "disable_public_all_docs" to false is deprecated. In the future, public access to the all_docs API will be disabled by default.`)
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
		ImportOptions:                 *importOptions,
		EnableXattr:                   config.UseXattrs(),
		SecureCookieOverride:          secureCookieOverride,
		SessionCookieName:             config.SessionCookieName,
		SessionCookieHttpOnly:         base.ValDefault(config.SessionCookieHTTPOnly, false),
		AllowConflicts:                config.ConflictsAllowed(),
		SendWWWAuthenticateHeader:     sendWWWAuthenticate,
		DisablePasswordAuthentication: base.ValDefault(config.DisablePasswordAuth, false),
		DeltaSyncOptions:              deltaSyncOptions,
		CompactInterval:               compactIntervalSecs,
		QueryPaginationLimit:          queryPaginationLimit,
		UserXattrKey:                  userXattrKey,
		SGReplicateOptions: db.SGReplicateOptions{
			Enabled:               sgReplicateEnabled,
			WebsocketPingInterval: sgReplicateWebsocketPingInterval,
		},
		SlowQueryWarningThreshold: slowQueryWarningThreshold,
		ClientPartitionWindow:     clientPartitionWindow,
		BcryptCost:                bcryptCost,
		GroupID:                   groupID,
		JavascriptTimeout:         javascriptTimeout,
		ChangesRequestPlus:        base.ValDefault(config.ChangesRequestPlus, false),
		// UserFunctions:             config.UserFunctions, // behind feature flag (see below)
		MaxConcurrentChangesBatches: sc.Config.Replicator.MaxConcurrentChangesBatches,
		MaxConcurrentRevs:           sc.Config.Replicator.MaxConcurrentRevs,
		NumIndexReplicas:            config.numIndexReplicas(),
		DisablePublicAllDocs:        disablePublicAllDocs,
		StoreLegacyRevTreeData:      base.Ptr(base.ValDefault(config.StoreLegacyRevTreeData, db.DefaultStoreLegacyRevTreeData)),
	}

	if config.Index != nil && config.Index.NumPartitions != nil {
		contextOptions.NumIndexPartitions = config.Index.NumPartitions
	}
	// Per-database logging config overrides
	contextOptions.LoggingConfig = config.toDbLogConfig(ctx)

	if sc.Config.Unsupported.UserQueries != nil && *sc.Config.Unsupported.UserQueries {
		var err error
		if config.UserFunctions != nil {
			contextOptions.UserFunctions, err = functions.CompileFunctions(ctx, *config.UserFunctions)
			if err != nil {
				return contextOptions, err
			}
		}
	} else if config.UserFunctions != nil {
		base.WarnfCtx(ctx, `Database config option "functions" ignored because unsupported.user_queries feature flag is not enabled`)
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
		reloadedDb, err := sc.ReloadDatabase(nonContextStruct.Ctx, database.Name, true)
		if err != nil {
			base.ErrorfCtx(nonContextStruct.Ctx, "Error reloading database from config: %v", err)
			return
		}

		if len(reloadedDb.RequireResync) > 0 {
			base.ErrorfCtx(nonContextStruct.Ctx, "Database has collections that require regenerate_sequences resync before it can go online: %v", reloadedDb.RequireResync)
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
	err := base.WaitUntilDataStoreReady(ctx, metadataStore)
	if err == nil {
		return nil
	}
	keyspace := strings.Join([]string{metadataStore.GetName(), metadataStore.ScopeName(), metadataStore.CollectionName()}, base.ScopeCollectionSeparator)
	if base.IsDefaultCollection(metadataStore.ScopeName(), metadataStore.CollectionName()) {
		base.WarnfCtx(ctx, "_default._default has been deleted from the server for bucket %s, to recover recreate the bucket", metadataStore.GetName())
	}
	return fmt.Errorf("metadata store %s does not exist on couchbase server: %w", base.MD(keyspace), err)
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
			filter, err := loadJavaScript(ctx, conf.Filter, insecureSkipVerify)
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
	dbcontext.EventMgr.Start(ctx, config.EventHandlers.MaxEventProc, int(customWaitTime))

	return nil
}

// Adds a database to the ServerContext given its configuration.  If an existing config is found
// for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfig(ctx context.Context, config DatabaseConfig) (*db.DatabaseContext, error) {
	failFast := false
	return sc.getOrAddDatabaseFromConfig(ctx, config, getOrAddDatabaseConfigOptions{useExisting: false, failFast: failFast})
}

// AddDatabaseFromConfigFailFast adds a database to the ServerContext given its configuration and fails fast.
// If an existing config is found for the name, returns an error.
func (sc *ServerContext) AddDatabaseFromConfigFailFast(nonContextStruct base.NonCancellableContext, config DatabaseConfig) (*db.DatabaseContext, error) {
	failFast := true
	return sc.getOrAddDatabaseFromConfig(nonContextStruct.Ctx, config, getOrAddDatabaseConfigOptions{useExisting: false, failFast: failFast})
}

func (sc *ServerContext) processEventHandlersForEvent(ctx context.Context, events []*EventConfig, eventType db.EventType, dbcontext *db.DatabaseContext) error {

	for _, event := range events {
		switch event.HandlerType {
		case "webhook":
			wh, err := db.NewWebhook(ctx, event.Url, event.Filter, event.Timeout, event.Options)
			if err != nil {
				base.WarnfCtx(ctx, "Error creating webhook %v", err)
				return err
			}
			dbcontext.EventMgr.RegisterEventHandler(ctx, wh, eventType)
		default:
			return errors.New(fmt.Sprintf("Unknown event handler type %s", event.HandlerType))
		}

	}
	return nil
}

// RemoveDatabase is called when an external request is made to delete the database
func (sc *ServerContext) RemoveDatabase(ctx context.Context, dbName string, reason string) bool {
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()

	// If async init is running for the database, cancel it for an external remove.  (cannot be
	// done in _removeDatabase, as this is called during reload)
	if sc.DatabaseInitManager != nil && sc.DatabaseInitManager.HasActiveInitialization(dbName) {
		sc.DatabaseInitManager.Cancel(dbName, reason)
	}

	return sc._removeDatabase(ctx, dbName)
}

// _unloadDatabase unloads and stops the database, but does not remove the in-memory config.
func (sc *ServerContext) _unloadDatabase(ctx context.Context, dbName string) bool {
	dbCtx := sc._databases[dbName]
	if dbCtx == nil {
		return false
	}
	base.InfofCtx(ctx, base.KeyAll, "Closing db /%s (bucket %q)", base.MD(dbCtx.Name), base.MD(dbCtx.Bucket.GetName()))
	dbCtx.Close(ctx)
	delete(sc._databases, dbName)
	return true
}

// _removeDatabase unloads and removes all references to the given database.
func (sc *ServerContext) _removeDatabase(ctx context.Context, dbName string) bool {
	dbCtx := sc._databases[dbName]
	if dbCtx == nil {
		return false
	}

	if ok := sc._unloadDatabase(ctx, dbName); !ok {
		return ok
	}
	delete(sc._dbConfigs, dbName)
	delete(sc._dbRegistry, dbName)
	for fqCollection, registryDbName := range sc._collectionRegistry {
		if dbName == registryDbName {
			delete(sc._collectionRegistry, fqCollection)
		}
	}
	return true
}

func (sc *ServerContext) _isDatabaseSuspended(dbName string) bool {
	if config, loaded := sc._dbConfigs[dbName]; loaded && config.isSuspended {
		return true
	}
	return false
}

func (sc *ServerContext) _suspendDatabase(ctx context.Context, dbName string) error {
	dbCtx := sc._databases[dbName]
	if dbCtx == nil {
		return base.ErrNotFound
	}

	config := sc._dbConfigs[dbName]
	if config != nil && !base.ValDefault(config.Suspendable, sc.Config.IsServerless()) {
		return ErrSuspendingDisallowed
	}

	bucket := dbCtx.Bucket.GetName()
	base.InfofCtx(ctx, base.KeyAll, "Suspending db %q (bucket %q)", base.MD(dbName), base.MD(bucket))

	if !sc._unloadDatabase(ctx, dbName) {
		return base.ErrNotFound
	}

	config.isSuspended = true
	return nil
}

func (sc *ServerContext) unsuspendDatabase(ctx context.Context, dbName string) (*db.DatabaseContext, error) {
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()

	return sc._unsuspendDatabase(ctx, dbName)
}

func (sc *ServerContext) _unsuspendDatabase(ctx context.Context, dbName string) (*db.DatabaseContext, error) {
	dbCtx := sc._databases[dbName]
	if dbCtx != nil {
		return dbCtx, nil
	}

	// Check if database is in dbConfigs so no need to search through buckets
	if dbConfig, ok := sc._dbConfigs[dbName]; ok {
		if !dbConfig.isSuspended {
			base.WarnfCtx(ctx, "attempting to unsuspend database %q that is not suspended", base.MD(dbName))
		}
		if !base.ValDefault(dbConfig.Suspendable, sc.Config.IsServerless()) {
			base.InfofCtx(ctx, base.KeyAll, "attempting to unsuspend db %q while not configured to be suspendable", base.MD(dbName))
		}

		bucket := dbName
		if dbConfig.Bucket != nil {
			bucket = *dbConfig.Bucket
		}

		cas, err := sc.BootstrapContext.GetConfig(ctx, bucket, sc.Config.Bootstrap.ConfigGroupID, dbName, &dbConfig.DatabaseConfig)
		if err == base.ErrNotFound {
			// Database no longer exists, so clean up dbConfigs
			base.InfofCtx(ctx, base.KeyConfig, "Database %q has been removed while suspended from bucket %q", base.MD(dbName), base.MD(bucket))
			delete(sc._dbConfigs, dbName)
			return nil, err
		} else if err != nil {
			return nil, fmt.Errorf("unsuspending db %q failed due to an error while trying to retrieve latest config from bucket %q: %w", base.MD(dbName).Redact(), base.MD(bucket).Redact(), err)
		}
		dbConfig.cfgCas = cas
		failFast := false
		dbCtx, err = sc._getOrAddDatabaseFromConfig(ctx, dbConfig.DatabaseConfig, getOrAddDatabaseConfigOptions{
			useExisting: false,
			failFast:    failFast,
		})
		if err != nil {
			return nil, err
		}
		return dbCtx, nil
	}

	return nil, base.ErrNotFound
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

	if err := sc.statsContext.addNodeCpuStats(); err != nil {
		base.WarnfCtx(ctx, "Error getting system resource stats: %v", err)
	}

	sc.updateCalculatedStats(ctx)
	// Create wrapper expvar map in order to add a timestamp field for logging purposes
	currentTime := time.Now()
	timestamp := currentTime.Format(time.RFC3339)
	wrapper := statsWrapper{
		Stats:              []byte(base.SyncGatewayStats.String()),
		UnixEpochTimestamp: currentTime.Unix(),
		RFC3339:            timestamp,
	}

	marshalled, err := base.JSONMarshal(wrapper)
	if err != nil {
		return err
	}

	// Marshal expvar map w/ timestamp to string and write to logs
	base.RecordStats(string(marshalled))

	return sc.statsContext.collectMemoryProfile(ctx, sc.Config.Logging.LogFilePath, timestamp)

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
func (sc *ServerContext) updateCalculatedStats(ctx context.Context) {
	sc._databasesLock.RLock()
	defer sc._databasesLock.RUnlock()
	for _, dbContext := range sc._databases {
		dbState := atomic.LoadUint32(&dbContext.State)
		if dbState == db.DBOnline {
			dbContext.UpdateCalculatedStats(ctx)
		}
	}

}

// initializeGoCBAgent Obtains a gocb agent from the current server connection. Requires the agent to be closed after use.
// Uses retry loop
func (sc *ServerContext) initializeGoCBAgent(ctx context.Context) (*gocbcore.Agent, error) {
	err, agent := base.RetryLoop(ctx, "Initialize Cluster Agent", func() (shouldRetry bool, err error, agent *gocbcore.Agent) {
		agent, err = base.NewClusterAgent(
			ctx,
			base.CouchbaseClusterSpec{
				Server:        sc.Config.Bootstrap.Server,
				Username:      sc.Config.Bootstrap.Username,
				Password:      sc.Config.Bootstrap.Password,
				X509Certpath:  sc.Config.Bootstrap.X509CertPath,
				X509Keypath:   sc.Config.Bootstrap.X509KeyPath,
				CACertpath:    sc.Config.Bootstrap.CACertPath,
				TLSSkipVerify: base.ValDefault(sc.Config.Bootstrap.ServerTLSSkipVerify, false),
			},
			base.CouchbaseClusterWaitUntilReadyOptions{
				// this timeout is pretty short since we are doing external retries
				Timeout: 5 * time.Second,
			})
		if err != nil {
			// since we're starting up - let's be verbose (on console) about these retries happening ... otherwise it looks like nothing is happening ...
			base.ConsolefCtx(ctx, base.LevelInfo, base.KeyConfig, "Couldn't initialize cluster agent: %v - will retry...", err)
			return true, err, nil
		}

		return false, nil, agent
	}, base.CreateSleeperFunc(27, 1000)) // ~2 mins total - 5 second gocb WaitUntilReady timeout and 1 second interval
	if err != nil {
		// warn and bubble up error for further handling
		base.ConsolefCtx(ctx, base.LevelWarn, base.KeyConfig, "Giving up initializing cluster agent after retry: %v", err)
		return nil, err
	}

	base.InfofCtx(ctx, base.KeyConfig, "Successfully initialized cluster agent")
	return agent, nil
}

// initializeNoX509HttpClient() returns an http client based on the bootstrap connection information, but
// without any x509 keypair included in the tls config.  This client can be used to perform basic
// authentication checks against the server.
// Client creation otherwise clones the approach used by gocb.
func (sc *ServerContext) initializeNoX509HttpClient(ctx context.Context) (*http.Client, error) {

	// baseTlsConfig defines the tlsConfig except for ServerName, which is updated based
	// on addr in DialTLS
	baseTlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	var rootCAs *x509.CertPool
	tlsRootCAProvider, err := base.GoCBCoreTLSRootCAProvider(ctx, sc.Config.Bootstrap.ServerTLSSkipVerify, sc.Config.Bootstrap.CACertPath)
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
		MaxIdleConns:        base.DefaultHttpMaxIdleConns,
		MaxIdleConnsPerHost: base.DefaultHttpMaxIdleConnsPerHost,
		IdleConnTimeout:     base.DefaultHttpIdleConnTimeout,
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
func CheckPermissions(ctx context.Context, httpClient *http.Client, managementEndpoints []string, bucketName, username, password string, accessPermissions []Permission, responsePermissions []Permission) (statusCode int, permissionResults map[string]bool, err error) {
	combinedPermissions := append(accessPermissions, responsePermissions...)
	body := []byte(strings.Join(FormatPermissionNames(combinedPermissions, bucketName), ","))
	statusCode, bodyResponse, err := doHTTPAuthRequest(ctx, httpClient, username, password, "POST", "/pools/default/checkPermissions", managementEndpoints, body)
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

type WhoAmIResponse struct {
	Roles []struct {
		RoleName   string `json:"role"`
		BucketName string `json:"bucket_name"`
	} `json:"roles"`
}

func cbRBACWhoAmI(ctx context.Context, httpClient *http.Client, managementEndpoints []string, username, password string) (response *WhoAmIResponse, statusCode int, err error) {
	statusCode, bodyResponse, err := doHTTPAuthRequest(ctx, httpClient, username, password, "GET", "/whoami", managementEndpoints, nil)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	if statusCode != http.StatusOK {
		return nil, statusCode, nil
	}

	err = base.JSONUnmarshal(bodyResponse, &response)
	if err != nil {
		return nil, http.StatusInternalServerError, err
	}

	return response, statusCode, nil
}

func CheckRoles(ctx context.Context, httpClient *http.Client, managementEndpoints []string, username, password string, requestedRoles []RouteRole, bucketName string) (statusCode int, err error) {
	whoAmIResults, statusCode, err := cbRBACWhoAmI(ctx, httpClient, managementEndpoints, username, password)
	if err != nil || statusCode != http.StatusOK {
		return statusCode, err
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

func doHTTPAuthRequest(ctx context.Context, httpClient *http.Client, username, password, method, path string, endpoints []string, requestBody []byte) (statusCode int, responseBody []byte, err error) {
	retryCount := 0

	worker := func() (shouldRetry bool, err error, value any) {
		endpointIdx := retryCount % len(endpoints)
		responseBody, statusCode, err = base.MgmtRequest(httpClient, endpoints[endpointIdx], method, path, "", username, password, bytes.NewBuffer(requestBody))

		if err, ok := err.(net.Error); ok && err.Timeout() {
			retryCount++
			return true, err, nil
		}

		return false, err, nil
	}

	err, _ = base.RetryLoop(ctx, "doHTTPAuthRequest", worker, base.CreateSleeperFunc(10, 100))
	if err != nil {
		return 0, nil, err
	}

	return statusCode, responseBody, nil
}

// For test use
func (sc *ServerContext) Database(ctx context.Context, name string) *db.DatabaseContext {
	db, err := sc.GetDatabase(ctx, name)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error getting db %q: %v", name, err))
	}
	return db
}

func (sc *ServerContext) initializeGocbAdminConnection(ctx context.Context) error {
	base.ConsolefCtx(ctx, base.LevelInfo, base.KeyAll, "Initializing server admin connection...")

	goCBAgent, err := sc.initializeGoCBAgent(ctx)
	if err != nil {
		return err
	}
	sc.GoCBAgent = goCBAgent

	sc.NoX509HTTPClient, err = sc.initializeNoX509HttpClient(ctx)
	base.InfofCtx(ctx, base.KeyAll, "Finished initializing server admin connection")
	return err
}

func (sc *ServerContext) initializeBootstrapConnection(ctx context.Context) error {
	if !sc.persistentConfig {
		return nil
	}
	base.InfofCtx(ctx, base.KeyAll, "Initializing bootstrap connection..")
	// Fetch database configs from bucket and start polling for new buckets and config updates.
	couchbaseCluster, err := CreateBootstrapConnectionFromStartupConfig(ctx, sc.Config, base.CachedClusterConnections)
	if err != nil {
		return err
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
		base.InfofCtx(ctx, base.KeyConfig, "Successfully fetched %d database configs for group %q from buckets in cluster", count, sc.Config.Bootstrap.ConfigGroupID)
	} else {
		base.WarnfCtx(ctx, "Config: No database configs for group %q. Continuing startup to allow REST API database creation", sc.Config.Bootstrap.ConfigGroupID)
	}

	if sc.Config.Bootstrap.ConfigUpdateFrequency.Value() > 0 {
		sc.BootstrapContext.terminator = make(chan struct{})
		sc.BootstrapContext.doneChan = make(chan struct{})

		base.InfofCtx(ctx, base.KeyConfig, "Starting background polling for new configs/buckets: %s", sc.Config.Bootstrap.ConfigUpdateFrequency.Value().String())
		go func() {
			defer close(sc.BootstrapContext.doneChan)
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
						base.WarnfCtx(ctx, "Couldn't load configs from bucket for group %q when polled: %v", sc.Config.Bootstrap.ConfigGroupID, err)
					}
					if count > 0 {
						base.InfofCtx(ctx, base.KeyConfig, "Successfully fetched %d database configs for group %q from buckets in cluster", count, sc.Config.Bootstrap.ConfigGroupID)
					}
				}
			}
		}()
	} else {
		base.InfofCtx(ctx, base.KeyConfig, "Disabled background polling for new configs/buckets")
	}
	base.InfofCtx(ctx, base.KeyAll, "Finished initializing bootstrap connection")

	return nil
}

func (sc *ServerContext) CheckSupportedCouchbaseVersion(ctx context.Context) error {
	clusterSpec := base.CouchbaseClusterSpec{
		Server:        sc.Config.Bootstrap.Server,
		Username:      sc.Config.Bootstrap.Username,
		Password:      sc.Config.Bootstrap.Password,
		X509Certpath:  sc.Config.Bootstrap.X509CertPath,
		X509Keypath:   sc.Config.Bootstrap.X509KeyPath,
		CACertpath:    sc.Config.Bootstrap.CACertPath,
		TLSSkipVerify: base.ValDefault(sc.Config.Bootstrap.ServerTLSSkipVerify, false),
	}

	securityConfig, err := base.GoCBv2SecurityConfig(ctx, base.Ptr(clusterSpec.TLSSkipVerify), clusterSpec.CACertpath)
	if err != nil {
		return fmt.Errorf("failed to create security config: %v", err)
	}

	authenticator, err := base.GoCBv2Authenticator(clusterSpec.Username, clusterSpec.Password, clusterSpec.X509Certpath, clusterSpec.X509Keypath)
	if err != nil {
		return fmt.Errorf("failed to create authenticator: %v", err)
	}

	cluster, err := gocb.Connect(clusterSpec.Server,
		gocb.ClusterOptions{
			Authenticator:  authenticator,
			SecurityConfig: securityConfig,
		})
	if err != nil {
		return fmt.Errorf("failed to create cluster: %v", err)
	}

	err = cluster.WaitUntilReady(5*time.Second, &gocb.WaitUntilReadyOptions{
		ServiceTypes: []gocb.ServiceType{gocb.ServiceTypeManagement},
	})
	if err != nil {
		return fmt.Errorf("failed to wait for cluster to become ready: %v", err)
	}

	major, minor, err := base.GetClusterVersion(cluster)
	if err != nil {
		return fmt.Errorf("failed to get cluster version: %v", err)
	}

	errMsg := fmt.Sprintf(
		"Sync Gateway requires Couchbase Server %d.%d or later, but found cluster version %d.%d",
		CBXDCRCompatibleMajorVersion,
		CBXDCRCompatibleMinorVersion,
		major,
		minor,
	)

	if !base.IsMinimumVersion(uint64(major), uint64(minor), CBXDCRCompatibleMajorVersion, CBXDCRCompatibleMinorVersion) {
		base.ErrorfCtx(ctx, "%s", errMsg)
		return errors.New(errMsg)
	}

	err = cluster.Close(&gocb.ClusterCloseOptions{})
	if err != nil {
		base.WarnfCtx(ctx, "Couldn't close cluster: %v", err)
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

// getTotalMemory returns the total memory available on the system. If a cgroup is detected, it will use the cgroup memory max.
func getTotalMemory(ctx context.Context) uint64 {
	memoryTotal, err := memlimit.FromCgroup()
	if err == nil {
		return memoryTotal
	}
	base.TracefCtx(ctx, base.KeyAll, "Did not detect a cgroup for a memory limit")
	memory, err := mem.VirtualMemory()
	if err != nil {
		base.WarnfCtx(ctx, "Error getting total memory from gopsutil: %v", err)
		return 0
	}
	return memory.Total
}

// getClusterUUID returns the cluster UUID. rosmar does not have a ClusterUUID, so this will return an empty cluster UUID and no error in this case.
func (sc *ServerContext) getClusterUUID(ctx context.Context) (string, error) {
	allDbNames := sc.AllDatabaseNames()
	// we can use db context to retrieve clusterUUID
	if len(allDbNames) > 0 {
		db, err := sc.GetDatabase(ctx, allDbNames[0])
		if err == nil {
			return db.ServerUUID, nil
		}
	}
	// no cluster uuid for rosmar cluster
	if base.ServerIsWalrus(sc.Config.Bootstrap.Server) {
		return "", nil
	}
	// request server for cluster uuid
	eps, client, err := sc.ObtainManagementEndpointsAndHTTPClient()
	if err != nil {
		return "", err
	}
	statusCode, output, err := doHTTPAuthRequest(ctx, client, sc.Config.Bootstrap.Username, sc.Config.Bootstrap.Password, http.MethodGet, "/pools", eps, nil)
	if err != nil {
		return "", err
	}
	if statusCode != http.StatusOK {
		return "", fmt.Errorf("unable to get cluster UUID from server: %s", output)
	}
	return base.ParseClusterUUID(output)
}

// removeBucketAndRecreateDatabase will flush all data from the backing bucket associated with this database. Note, this will take down
// other databases backed by the same bucket. It will recreate an empty database from the existing configuration.
func (sc *ServerContext) removeBucketAndRecreateDatabase(ctx context.Context, dbName string, deleteFunc func(base.BucketSpec) error) error {
	sc._databasesLock.Lock()
	defer sc._databasesLock.Unlock()
	config, ok := sc._dbConfigs[dbName]
	if !ok {
		return fmt.Errorf("no config found for database %q", dbName)
	}
	for otherDBName, dbCtx := range sc._databases {
		if dbCtx.Bucket.GetName() == *config.Bucket {
			// If async init is running for the database, cancel it for an external remove.  (cannot be
			// done in _removeDatabase, as this is called during reload)
			if sc.DatabaseInitManager != nil && sc.DatabaseInitManager.HasActiveInitialization(otherDBName) {
				sc.DatabaseInitManager.Cancel(otherDBName, fmt.Sprintf("flush db %s with same backing bucket", dbName))
			}
			if !sc._removeDatabase(ctx, otherDBName) {
				return base.RedactErrorf("could not remove database %s as part of flushing %s. Bucket %s is left in an unstable state", base.UD(otherDBName), base.UD(dbName), base.UD(*config.Bucket))
			}
		}
	}

	spec, err := GetBucketSpec(ctx, &config.DatabaseConfig, sc.Config)
	if err != nil {
		return err
	}

	err = deleteFunc(spec)
	if err != nil {
		return err
	}

	if sc.persistentConfig {
		ctx := context.WithoutCancel(ctx)
		cas, err := sc.BootstrapContext.InsertConfig(ctx, spec.BucketName, sc.Config.Bootstrap.ConfigGroupID, &config.DatabaseConfig)
		if err != nil {
			base.WarnfCtx(ctx, "Could not re-insert database config after flush and re-adding bucket: %v", err)
			return err
		}
		_, err = sc._fetchAndLoadDatabase(base.NewNonCancelCtxForDatabase(ctx), dbName, true)
		if err != nil {
			base.WarnfCtx(ctx, "Could not reload database after flush and re-adding bucket: %v", err)
			return err
		}
		// store cas in db config after update
		sc._dbConfigs[dbName].cfgCas = cas
	} else {
		// Re-open database and add to Sync Gateway
		_, err := sc._getOrAddDatabaseFromConfig(ctx, config.DatabaseConfig,
			getOrAddDatabaseConfigOptions{
				useExisting: false,
				failFast:    false,
			})
		if err != nil {
			return err
		}
	}
	return nil
}

func (sc *ServerContext) getBucketCCVSettings() map[string]bool {
	bucketCCVSettings := make(map[string]bool)
	for _, _db := range sc._databases {
		bucketName := _db.BucketSpec.BucketName
		bucketCCVSettings[bucketName] = _db.CachedCCVEnabled.Load()
	}
	return bucketCCVSettings
}
