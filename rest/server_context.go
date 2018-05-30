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
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	pkgerrors "github.com/pkg/errors"
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
	config      *ServerConfig
	databases_  map[string]*db.DatabaseContext
	lock        sync.RWMutex
	statsTicker *time.Ticker
	HTTPClient  *http.Client
	replicator  *base.Replicator
}

func NewServerContext(config *ServerConfig) *ServerContext {
	sc := &ServerContext{
		config:     config,
		databases_: map[string]*db.DatabaseContext{},
		HTTPClient: http.DefaultClient,
		replicator: base.NewReplicator(),
	}
	if config.Databases == nil {
		config.Databases = DbConfigMap{}
	}

	// Initialize the go-couchbase library's global configuration variables:
	couchbase.PoolSize = DefaultMaxCouchbaseConnections
	couchbase.PoolOverflow = DefaultMaxCouchbaseOverflowConnections
	if config.MaxCouchbaseConnections != nil {
		couchbase.PoolSize = *config.MaxCouchbaseConnections
	}
	if config.MaxCouchbaseOverflow != nil {
		couchbase.PoolOverflow = *config.MaxCouchbaseOverflow
	}

	if config.CouchbaseKeepaliveInterval != nil {
		couchbase.SetTcpKeepalive(true, *config.CouchbaseKeepaliveInterval)
	}

	slowQuery := kDefaultSlowQueryWarningThreshold
	if config.SlowQueryWarningThreshold != nil {
		slowQuery = *config.SlowQueryWarningThreshold
	}
	base.SlowQueryWarningThreshold = time.Duration(slowQuery) * time.Millisecond

	if config.DeploymentID != nil {
		sc.startStatsReporter()
	}

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
			base.Errorf(base.KeyAll, "Error validating replication parameters: %v", err)
			continue
		}

		// Force one-shot replications to run Async
		// to avoid blocking server startup
		params.Async = true

		// Run single replication, cancel parameter will always be false
		if _, err := sc.replicator.Replicate(params, false); err != nil {
			base.Warnf(base.KeyAll, "Error starting replication %v: %v", base.UD(params.ReplicationId), err)
		}

	}

}

func (sc *ServerContext) FindDbByBucketName(bucketName string) string {

	// Loop through all known database contexts and return the first one
	// that has the bucketName specified above.
	for dbName, dbContext := range sc.databases_ {
		if dbContext.Bucket.GetName() == bucketName {
			return dbName
		}
	}
	return ""

}

func (sc *ServerContext) Close() {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	if err := sc.replicator.StopReplications(); err != nil {
		base.Warnf(base.KeyAll, "Error stopping replications: %+v.  This could cause a resource leak.  Please restart Sync Gateway to cleanup leaked resources.", err)
	}

	sc.stopStatsReporter()
	for _, ctx := range sc.databases_ {
		ctx.Close()
		if ctx.EventMgr.HasHandlerForEvent(db.DBStateChange) {
			ctx.EventMgr.RaiseDBStateChangeEvent(ctx.Name, "offline", "Database context closed", *sc.config.AdminInterface)
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
	sc.lock.Lock()
	defer sc.lock.Unlock()

	names := make([]string, 0, len(sc.databases_))
	for name := range sc.databases_ {
		names = append(names, name)
	}
	return names
}

// AllDatabases returns a copy of the databases_ map.
func (sc *ServerContext) AllDatabases() map[string]*db.DatabaseContext {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	databases := make(map[string]*db.DatabaseContext, len(sc.databases_))

	for name, database := range sc.databases_ {
		databases[name] = database
	}
	return databases
}

// Make sure that for all nodes that are feedtype=DCPSHARD, either all of them
// are IndexWriters, or none of them are IndexWriters, otherwise return false.
func (sc *ServerContext) numIndexWriters() (numIndexWriters, numIndexNonWriters int) {

	for _, dbContext := range sc.databases_ {
		if strings.ToLower(dbContext.BucketSpec.FeedType) != base.DcpShardFeedType {
			continue
		}
		if dbContext.Options.IndexOptions.Writer {
			numIndexWriters += 1
		} else {
			numIndexNonWriters += 1
		}
	}

	return numIndexWriters, numIndexNonWriters

}

func (sc *ServerContext) HasIndexWriters() bool {
	numIndexWriters, _ := sc.numIndexWriters()
	return numIndexWriters > 0
}

type PostUpgradeResult map[string]PostUpgradeDatabaseResult

type PostUpgradeDatabaseResult struct {
	RemovedDDocs   []string `json:"removed_design_docs"`
	RemovedIndexes []string `json:"removed_indexes"`
}

// PostUpgrade performs post-upgrade processing for each database
func (sc *ServerContext) PostUpgrade(preview bool) (postUpgradeResults PostUpgradeResult, err error) {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	postUpgradeResults = make(map[string]PostUpgradeDatabaseResult, len(sc.databases_))

	for name, database := range sc.databases_ {
		// View cleanup
		removedDDocs, err := database.RemoveObsoleteDesignDocs(preview)
		if err != nil {
			return nil, err
		}

		// Index cleanup
		removedIndexes, err := database.RemoveObsoleteIndexes(preview)
		if err != nil {
			return nil, err
		}

		postUpgradeResults[name] = PostUpgradeDatabaseResult{
			RemovedDDocs:   removedDDocs,
			RemovedIndexes: removedIndexes,
		}
	}
	return postUpgradeResults, nil
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) ReloadDatabaseFromConfig(reloadDbName string, useExisting bool) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	defer sc.lock.Unlock()

	sc._removeDatabase(reloadDbName)

	config := sc.config.Databases[reloadDbName]

	return sc._getOrAddDatabaseFromConfig(config, useExisting)
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) getOrAddDatabaseFromConfig(config *DbConfig, useExisting bool) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	defer sc.lock.Unlock()

	return sc._getOrAddDatabaseFromConfig(config, useExisting)
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

	if config.Unsupported.LeakyBucketConfig != nil {
		spec.LeakyBucketConfig = config.Unsupported.LeakyBucketConfig
	}

	return spec, nil
}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) _getOrAddDatabaseFromConfig(config *DbConfig, useExisting bool) (*db.DatabaseContext, error) {

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

	if config.OldRevExpirySeconds != nil && *config.OldRevExpirySeconds >= 0 {
		oldRevExpirySeconds = *config.OldRevExpirySeconds
	}

	localDocExpirySecs := base.DefaultLocalDocExpirySecs
	if config.LocalDocExpirySecs != nil && *config.LocalDocExpirySecs >= 0 {
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

	// Set cache properties, if present
	cacheOptions := db.CacheOptions{}
	if config.CacheConfig != nil {
		if config.CacheConfig.CachePendingSeqMaxNum != nil && *config.CacheConfig.CachePendingSeqMaxNum > 0 {
			cacheOptions.CachePendingSeqMaxNum = *config.CacheConfig.CachePendingSeqMaxNum
		}
		if config.CacheConfig.CachePendingSeqMaxWait != nil && *config.CacheConfig.CachePendingSeqMaxWait > 0 {
			cacheOptions.CachePendingSeqMaxWait = time.Duration(*config.CacheConfig.CachePendingSeqMaxWait) * time.Millisecond
		}
		if config.CacheConfig.CacheSkippedSeqMaxWait != nil && *config.CacheConfig.CacheSkippedSeqMaxWait > 0 {
			cacheOptions.CacheSkippedSeqMaxWait = time.Duration(*config.CacheConfig.CacheSkippedSeqMaxWait) * time.Millisecond
		}
		// set EnableStarChannelLog directly here (instead of via NewDatabaseContext), so that it's set when we create the channels view in ConnectToBucket
		if config.CacheConfig.EnableStarChannel != nil {
			db.EnableStarChannelLog = *config.CacheConfig.EnableStarChannel
		}

		if config.CacheConfig.ChannelCacheMaxLength != nil && *config.CacheConfig.ChannelCacheMaxLength > 0 {
			cacheOptions.ChannelCacheMaxLength = *config.CacheConfig.ChannelCacheMaxLength
		}
		if config.CacheConfig.ChannelCacheMinLength != nil && *config.CacheConfig.ChannelCacheMinLength > 0 {
			cacheOptions.ChannelCacheMinLength = *config.CacheConfig.ChannelCacheMinLength
		}
		if config.CacheConfig.ChannelCacheAge != nil && *config.CacheConfig.ChannelCacheAge > 0 {
			cacheOptions.ChannelCacheAge = time.Duration(*config.CacheConfig.ChannelCacheAge) * time.Second
		}

	}

	bucket, err := db.ConnectToBucket(spec, func(bucket string, err error) {

		msgFormatStr := "%v dropped Mutation feed (TAP/DCP) due to error: %v, taking offline"
		base.Warnf(base.KeyAll, msgFormatStr, base.UD(bucket), err)

		if dc := sc.databases_[dbName]; dc != nil {

			err := dc.TakeDbOffline(fmt.Sprintf(msgFormatStr, bucket, err))
			if err == nil {

				//start a retry loop to pick up tap feed again backing off double the delay each time
				worker := func() (shouldRetry bool, err error, value interface{}) {
					//If DB is going online via an admin request Bucket will be nil
					if dc.Bucket != nil {
						err = dc.Bucket.Refresh()
					} else {
						err = base.HTTPErrorf(http.StatusPreconditionFailed, "Database %q, bucket is not available", dbName)
						return false, err, nil
					}
					return err != nil, err, nil
				}

				sleeper := base.CreateDoublingSleeperFunc(
					20, //MaxNumRetries
					5,  //InitialRetrySleepTimeMS
				)

				description := fmt.Sprintf("Attempt reconnect to lost Mutation (TAP/DCP) Feed for : %v", dc.Name)
				err, _ := base.RetryLoop(description, worker, sleeper)

				if err == nil {
					base.Infof(base.KeyCRUD, "Connection to Mutation (TAP/DCP) feed for %v re-established, bringing DB back online", base.UD(dc.Name))

					// The 10 second wait was introduced because the bucket was not fully initialised
					// after the return of the retry loop.
					timer := time.NewTimer(time.Duration(10) * time.Second)
					<-timer.C

					sc.TakeDbOnline(dc)
				}
			}
		}
	})

	if err != nil {
		return nil, err
	}

	// If using a walrus bucket, force use of views
	useViews := config.UseViews
	if !useViews && spec.IsWalrusBucket() {
		base.Warnf(base.KeyAll, "Using GSI is not supported when using a walrus bucket - switching to use views.  Set 'use_views':true in Sync Gateway's database config to avoid this warning.")
		useViews = true
	}

	// If using accel, force use of views
	if !useViews && config.ChannelIndex != nil {
		base.Warnf(base.KeyAll, "Using GSI is not supported when using Sync Gateway Accelerator - switching to use views.  Set 'use_views':true in Sync Gateway's database config to avoid this warning.")
		useViews = true
	}

	// Initialize Views or GSI indexes
	if !useViews {

		// Couchbase Server version must be 5.5 or higher to use GSI
		gsiSupported, errServerVersion := base.IsMinimumServerVersion(bucket, 5, 5)
		if errServerVersion != nil {
			return nil, errServerVersion
		}

		if !gsiSupported {
			return nil, errors.New("Couchbase Server version must be 5.5 or higher for Sync Gateway to use GSI.  Upgrade the server, or set 'use_views':true in Sync Gateway's database config.")
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

	// Channel index definition, if present
	channelIndexOptions := &db.ChannelIndexOptions{}
	sequenceHashOptions := &db.SequenceHashOptions{}
	if config.ChannelIndex != nil {

		// Index buckets always use DCP feed type
		couchbaseDriverIndexBucket := base.ChooseCouchbaseDriver(base.IndexBucket)

		indexSpec := config.ChannelIndex.MakeBucketSpec()
		indexSpec.CouchbaseDriver = couchbaseDriverIndexBucket

		if config.ChannelIndex.NumShards != 0 {
			channelIndexOptions.NumShards = config.ChannelIndex.NumShards
		} else {
			channelIndexOptions.NumShards = KDefaultNumShards
		}

		channelIndexOptions.ValidateOrPanic()

		channelIndexOptions.Spec = indexSpec
		channelIndexOptions.Writer = config.ChannelIndex.IndexWriter
		channelIndexOptions.TombstoneCompactFrequency = config.ChannelIndex.TombstoneCompactFrequency

		// Hash bucket defaults to index bucket, but can be customized.
		sequenceHashOptions.Size = 32
		sequenceHashBucketSpec := indexSpec
		hashConfig := config.ChannelIndex.SequenceHashConfig
		if hashConfig != nil {
			sequenceHashBucketSpec = config.ChannelIndex.SequenceHashConfig.MakeBucketSpec()
			sequenceHashBucketSpec.CouchbaseDriver = couchbaseDriverIndexBucket
			sequenceHashOptions.Expiry = hashConfig.Expiry
			sequenceHashOptions.HashFrequency = hashConfig.Frequency
		}

		sequenceHashOptions.Bucket, err = base.GetBucket(sequenceHashBucketSpec, nil)
		if err != nil {
			base.Warnf(base.KeyAll, "Error opening sequence hash bucket %q, pool %q, server <%s>",
				base.MD(sequenceHashBucketSpec.BucketName), base.SD(sequenceHashBucketSpec.PoolName), base.SD(sequenceHashBucketSpec.Server))
			return nil, err
		}

	} else {
		channelIndexOptions = nil
	}

	var revCacheSize uint32
	if config.RevCacheSize != nil && *config.RevCacheSize > 0 {
		revCacheSize = *config.RevCacheSize
	} else {
		revCacheSize = db.KDefaultRevisionCacheCapacity
	}

	// Enable doc tracking if needed for autoImport or shadowing.  Only supported for non-xattr configurations
	trackDocs := false
	if !config.UseXattrs() {
		trackDocs = autoImport || config.Deprecated.Shadow != nil
	}

	// Create a callback function that will be invoked if the database goes offline and comes
	// back online again
	dbOnlineCallback := func(dbContext *db.DatabaseContext) {
		sc.TakeDbOnline(dbContext)
	}

	contextOptions := db.DatabaseContextOptions{
		CacheOptions:              &cacheOptions,
		IndexOptions:              channelIndexOptions,
		SequenceHashOptions:       sequenceHashOptions,
		RevisionCacheCapacity:     revCacheSize,
		OldRevExpirySeconds:       oldRevExpirySeconds,
		LocalDocExpirySecs:        localDocExpirySecs,
		AdminInterface:            sc.config.AdminInterface,
		UnsupportedOptions:        config.Unsupported,
		TrackDocs:                 trackDocs,
		OIDCOptions:               config.OIDCConfig,
		DBOnlineCallback:          dbOnlineCallback,
		ImportOptions:             importOptions,
		EnableXattr:               config.UseXattrs(),
		SessionCookieName:         config.SessionCookieName,
		AllowConflicts:            config.ConflictsAllowed(),
		SendWWWAuthenticateHeader: config.SendWWWAuthenticateHeader,
		UseViews:                  useViews,
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

			if dbcontext.RevsLimit < 100 {
				base.Warnf(base.KeyAll, "Setting the revs_limit (%v) to less than 100 may have unwanted results when documents are frequently updated. Please see documentation for details.", dbcontext.RevsLimit)
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

	// Install bucket-shadower if any:
	if shadow := config.Deprecated.Shadow; shadow != nil {
		if err := sc.startShadowing(dbcontext, shadow); err != nil {
			base.Warnf(base.KeyAll, "Database %q: unable to connect to external bucket for shadowing: %v",
				base.MD(dbName), err)
		}
	}

	// Initialize event handlers
	if err := sc.initEventHandlers(dbcontext, config); err != nil {
		return nil, err
	}

	dbcontext.ExitChanges = make(chan struct{})

	// Register it so HTTP handlers can find it:
	sc.databases_[dbcontext.Name] = dbcontext

	// Save the config
	sc.config.Databases[config.Name] = config

	if config.StartOffline {
		atomic.StoreUint32(&dbcontext.State, db.DBOffline)
		if dbcontext.EventMgr.HasHandlerForEvent(db.DBStateChange) {
			dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "offline", "DB loaded from config", *sc.config.AdminInterface)
		}
	} else {
		atomic.StoreUint32(&dbcontext.State, db.DBOnline)
		if dbcontext.EventMgr.HasHandlerForEvent(db.DBStateChange) {
			dbcontext.EventMgr.RaiseDBStateChangeEvent(dbName, "online", "DB loaded from config", *sc.config.AdminInterface)
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
		reloadedDb, err := sc.ReloadDatabaseFromConfig(database.Name, true)
		if err != nil {
			base.Errorf(base.KeyAll, "Error reloading database from config: %v", err)
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

		eventHandlersJSON, err := json.Marshal(eventHandlersMap)
		if err != nil {
			return pkgerrors.Wrapf(err, "Error calling json.Marshal() in initEventHandlers")
		}
		if err := json.Unmarshal(eventHandlersJSON, eventHandlers); err != nil {
			return pkgerrors.Wrapf(err, "Error calling json.Unmarshal() in initEventHandlers")
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
				base.Warnf(base.KeyAll, "Error parsing wait_for_process from config, using default %s", err)
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
				base.Warnf(base.KeyAll, "Error creating webhook %v", err)
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

func (sc *ServerContext) startShadowing(dbcontext *db.DatabaseContext, shadow *ShadowConfig) error {

	base.Warnf(base.KeyAll, "Bucket Shadowing feature comes with a number of limitations and caveats. See https://github.com/couchbase/sync_gateway/issues/1363 for more details.")

	var pattern *regexp.Regexp
	if shadow.Doc_id_regex != nil {
		var err error
		pattern, err = regexp.Compile(*shadow.Doc_id_regex)
		if err != nil {
			base.Warnf(base.KeyAll, "Invalid shadow doc_id_regex: %s", base.UD(*shadow.Doc_id_regex))
			return err
		}
	}

	shadowBucketCouchbaseDriver := base.ChooseCouchbaseDriver(base.DataBucket)

	spec := base.BucketSpec{
		Server:          *shadow.Server,
		PoolName:        "default",
		BucketName:      *shadow.Bucket,
		CouchbaseDriver: shadowBucketCouchbaseDriver,
		FeedType:        shadow.FeedType,
	}
	if shadow.Pool != nil {
		spec.PoolName = *shadow.Pool
	}
	if shadow.Username != "" {
		spec.Auth = shadow
	}

	bucket, err := base.GetBucket(spec, nil)

	if err != nil {
		err = base.HTTPErrorf(http.StatusBadGateway,
			"Unable to connect to shadow bucket: %s", err)
		return err
	}
	shadower, err := db.NewShadower(dbcontext, bucket, pattern)
	if err != nil {
		bucket.Close()
		return err
	}
	dbcontext.Shadower = shadower

	//Remove credentials from server URL before logging
	url, err := couchbase.ParseURL(spec.Server)
	if err == nil {
		base.Infof(base.KeyAll, "Database %q shadowing remote bucket %q, pool %q, server <%s:%s/%s>", base.MD(dbcontext.Name), base.MD(spec.BucketName), base.SD(spec.PoolName), url.Scheme, base.SD(url.Host), url.Path)
	}
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
			base.Infof(base.KeyAll, "    Reset guest user to config")
		} else if createdPrincipal {
			base.Infof(base.KeyAll, "    Created %s %q", what, base.UD(name))
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
	res, err := sc.HTTPClient.Get(urlStr)
	if err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			"Error contacting config server: %v", err)
	} else if res.StatusCode >= 300 {
		return nil, base.HTTPErrorf(res.StatusCode, res.Status)
	}

	var config DbConfig

	var bodyBytes []byte
	defer res.Body.Close()
	if bodyBytes, err = ioutil.ReadAll(res.Body); err != nil {
		return nil, err
	}

	j := json.NewDecoder(bytes.NewReader(base.ConvertBackQuotedStrings(bodyBytes)))
	if err = j.Decode(&config); err != nil {
		return nil, base.HTTPErrorf(http.StatusBadGateway,
			"Bad response from config server: %v", err)
	}

	if err = config.setup(dbName); err != nil {
		return nil, err
	}
	return &config, nil
}

//////// STATISTICS REPORT:

func (sc *ServerContext) startStatsReporter() {
	interval := kStatsReportInterval
	if sc.config.StatsReportInterval != nil {
		if *sc.config.StatsReportInterval <= 0 {
			return
		}
		interval = time.Duration(*sc.config.StatsReportInterval) * time.Second
	}
	sc.statsTicker = time.NewTicker(interval)
	go func() {
		for range sc.statsTicker.C {
			sc.reportStats()
		}
	}()
	base.Infof(base.KeyAll, "Will report server stats for %q every %v",
		base.UD(*sc.config.DeploymentID), interval)
}

func (sc *ServerContext) stopStatsReporter() {
	if sc.statsTicker != nil {
		sc.statsTicker.Stop()
		sc.reportStats() // Report stuff since the last tick
	}
}

// POST a report of database statistics
func (sc *ServerContext) reportStats() {
	if sc.config.DeploymentID == nil {
		panic("Can't reportStats without DeploymentID")
	}
	stats := sc.Stats()
	if stats == nil {
		return // No activity
	}
	base.Infof(base.KeyAll, "Reporting server stats to %s ...", base.SD(kStatsReportURL))
	body, _ := json.Marshal(stats)
	bodyReader := bytes.NewReader(body)
	_, err := sc.HTTPClient.Post(kStatsReportURL, "application/json", bodyReader)
	if err != nil {
		base.Warnf(base.KeyAll, "Error posting stats: %v", err)
	}
}

func (sc *ServerContext) Stats() map[string]interface{} {
	sc.lock.RLock()
	defer sc.lock.RUnlock()
	var stats []map[string]interface{}
	any := false
	for _, dbc := range sc.databases_ {
		max := dbc.ChangesClientStats.MaxCount()
		total := dbc.ChangesClientStats.TotalCount()
		dbc.ChangesClientStats.Reset()
		stats = append(stats, map[string]interface{}{
			"max_connections":   max,
			"total_connections": total,
		})
		any = any || total > 0
	}
	if !any {
		return nil
	}
	return map[string]interface{}{
		"deploymentID": *sc.config.DeploymentID,
		"databases":    stats,
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
