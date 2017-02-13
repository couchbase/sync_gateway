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
	"github.com/couchbaselabs/sg-replicate"
)

// The URL that stats will be reported to if deployment_id is set in the config
const kStatsReportURL = "http://localhost:9999/stats"
const kStatsReportInterval = time.Hour
const kDefaultSlowServerCallWarningThreshold = 200 // ms
const kOneShotLocalDbReplicateWait = 10 * time.Second
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

	slow := kDefaultSlowServerCallWarningThreshold
	if config.SlowServerCallWarningThreshold != nil {
		slow = *config.SlowServerCallWarningThreshold
	}
	couchbase.SlowServerCallWarningThreshold = time.Duration(slow) * time.Millisecond

	if config.DeploymentID != nil {
		sc.startStatsReporter()
	}

	if config.Replications != nil {

		for _, replicationConfig := range config.Replications {

			params, _, localdb, err := validateReplicationParameters(*replicationConfig, true, *config.AdminInterface)

			if err != nil {
				base.LogError(err)
				continue
			}

			//Force one-shot replications to run Async
			//to avoid blocking server startup
			params.Async = true

			//Run single replication, cancel parameter will always be false
			go func() {
				//Delay the start of the replication if its a oneshot that
				//uses a localdb reference to allow the REST API's to come up
				if params.Lifecycle == sgreplicate.ONE_SHOT && localdb {
					base.Warn("Delaying start of local database one-shot replication, source %v, target %v for %v seconds", params.SourceDb, params.TargetDb, kOneShotLocalDbReplicateWait)
					time.Sleep(kOneShotLocalDbReplicateWait)
				}
				sc.replicator.Replicate(params, false)
			}()
		}

	}

	return sc
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
		base.Logf("Asking config server %q about db %q...", *sc.config.ConfigServer, name)
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
		if dbContext.BucketSpec.FeedType != base.DcpShardFeedType {
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

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) _getOrAddDatabaseFromConfig(config *DbConfig, useExisting bool) (*db.DatabaseContext, error) {

	server := "http://localhost:8091"
	pool := "default"
	bucketName := config.Name

	if config.Server != nil {
		server = *config.Server
	}
	if config.Pool != nil {
		pool = *config.Pool
	}
	if config.Bucket != nil {
		bucketName = *config.Bucket
	}
	dbName := config.Name
	if dbName == "" {
		dbName = bucketName
	}

	if sc.databases_[dbName] != nil {
		if useExisting {
			return sc.databases_[dbName], nil
		} else {
			return nil, base.HTTPErrorf(http.StatusPreconditionFailed, // what CouchDB returns
				"Duplicate database name %q", dbName)
		}
	}

	base.Logf("Opening db /%s as bucket %q, pool %q, server <%s>",
		dbName, bucketName, pool, server)

	if err := db.ValidateDatabaseName(dbName); err != nil {
		return nil, err
	}

	var importDocs, autoImport bool
	switch config.ImportDocs {
	case nil, false:
	case true:
		importDocs = true
	case "continuous":
		importDocs = true
		autoImport = true
	default:
		return nil, fmt.Errorf("Unrecognized value for ImportDocs: %#v", config.ImportDocs)
	}

	feedType := strings.ToLower(config.FeedType)

	// Connect to the bucket and add the database:
	spec := base.BucketSpec{
		Server:     server,
		PoolName:   pool,
		BucketName: bucketName,
		FeedType:   feedType,
		Auth:       config,
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
		base.Warn("Lost TAP feed for bucket %s, with error: %v", bucket, err)

		if dc := sc.databases_[dbName]; dc != nil {
			err := dc.TakeDbOffline("Lost TAP feed")
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

				description := fmt.Sprintf("Attempt reconnect to lost TAP Feed for : %v", dc.Name)
				err, _ := base.RetryLoop(description, worker, sleeper)

				if err == nil {
					base.LogTo("CRUD", "Connection to TAP feed for %v re-established, bringing DB back online", dc.Name)
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

	// Channel index definition, if present
	channelIndexOptions := &db.ChangeIndexOptions{} // TODO: this is confusing!  why is it called both a "change index" and a "channel index"?
	sequenceHashOptions := &db.SequenceHashOptions{}
	if config.ChannelIndex != nil {
		indexServer := "http://localhost:8091"
		indexPool := "default"
		indexBucketName := ""

		if config.ChannelIndex.Server != nil {
			indexServer = *config.ChannelIndex.Server
		}
		if config.ChannelIndex.Pool != nil {
			indexPool = *config.ChannelIndex.Pool
		}
		if config.ChannelIndex.Bucket != nil {
			indexBucketName = *config.ChannelIndex.Bucket
		}
		indexSpec := base.BucketSpec{
			Server:          indexServer,
			PoolName:        indexPool,
			BucketName:      indexBucketName,
			CouchbaseDriver: base.GoCB,
		}
		if config.ChannelIndex.Username != "" {
			indexSpec.Auth = config.ChannelIndex
		}

		if config.ChannelIndex.NumShards != 0 {
			channelIndexOptions.NumShards = config.ChannelIndex.NumShards
		} else {
			channelIndexOptions.NumShards = KDefaultNumShards
		}

		channelIndexOptions.ValidateOrPanic()

		channelIndexOptions.Spec = indexSpec
		channelIndexOptions.Writer = config.ChannelIndex.IndexWriter

		// Hash bucket defaults to index bucket, but can be customized.
		sequenceHashOptions.Size = 32
		sequenceHashBucketSpec := indexSpec
		hashConfig := config.ChannelIndex.SequenceHashConfig
		if hashConfig != nil {
			if hashConfig.Server != nil {
				sequenceHashBucketSpec.Server = *hashConfig.Server
			}
			if hashConfig.Pool != nil {
				sequenceHashBucketSpec.PoolName = *hashConfig.Pool
			}
			if hashConfig.Bucket != nil {
				sequenceHashBucketSpec.BucketName = *hashConfig.Bucket
			}
			sequenceHashOptions.Expiry = hashConfig.Expiry
			sequenceHashOptions.HashFrequency = hashConfig.Frequency
		}

		sequenceHashOptions.Bucket, err = base.GetBucket(sequenceHashBucketSpec, nil)
		if err != nil {
			base.Logf("Error opening sequence hash bucket %q, pool %q, server <%s>",
				sequenceHashBucketSpec.BucketName, sequenceHashBucketSpec.PoolName, sequenceHashBucketSpec.Server)
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

	unsupportedOptions := &db.UnsupportedOptions{}
	if config.Unsupported != nil {
		if config.Unsupported.UserViews != nil {
			if config.Unsupported.UserViews.Enabled != nil {
				unsupportedOptions.EnableUserViews = *config.Unsupported.UserViews.Enabled
			}
		}
		if config.Unsupported.OidcTestProvider != nil {
			unsupportedOptions.OidcTestProvider = *config.Unsupported.OidcTestProvider
		}
	}

	// Enable doc tracking if needed for autoImport or shadowing
	trackDocs := autoImport || config.Shadow != nil

	contextOptions := db.DatabaseContextOptions{
		CacheOptions:          &cacheOptions,
		IndexOptions:          channelIndexOptions,
		SequenceHashOptions:   sequenceHashOptions,
		RevisionCacheCapacity: revCacheSize,
		AdminInterface:        sc.config.AdminInterface,
		UnsupportedOptions:    unsupportedOptions,
		TrackDocs:             trackDocs,
		OIDCOptions:           config.OIDCConfig,
	}

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

	if importDocs {
		db, _ := db.GetDatabase(dbcontext, nil)
		if _, err := db.UpdateAllDocChannels(false, true); err != nil {
			return nil, err
		}
	}

	if config.RevsLimit != nil && *config.RevsLimit > 0 {
		dbcontext.RevsLimit = *config.RevsLimit
	}

	dbcontext.AllowEmptyPassword = config.AllowEmptyPassword

	if dbcontext.ChannelMapper == nil {
		base.Logf("Using default sync function 'channel(doc.channels)' for database %q", dbName)
	}

	// Create default users & roles:
	if err := sc.installPrincipals(dbcontext, config.Roles, "role"); err != nil {
		return nil, err
	} else if err := sc.installPrincipals(dbcontext, config.Users, "user"); err != nil {
		return nil, err
	}

	emitAccessRelatedWarnings(config, dbcontext)

	// Install bucket-shadower if any:
	if shadow := config.Shadow; shadow != nil {
		if err := sc.startShadowing(dbcontext, shadow); err != nil {
			base.Warn("Database %q: unable to connect to external bucket for shadowing: %v",
				dbName, err)
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

		if _, err := sc.ReloadDatabaseFromConfig(database.Name, true); err != nil {
			base.LogError(err)
			return
		}

		//Set DB state to DBOnline, this wil cause new API requests to be be accepted
		atomic.StoreUint32(&sc.databases_[database.Name].State, db.DBOnline)

	} else {
		base.LogTo("CRUD", "Unable to take Database : %v online , database must be in Offline state", database.Name)
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
			return err
		}
		if err := json.Unmarshal(eventHandlersJSON, eventHandlers); err != nil {
			return err
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
				base.Warn("Error parsing wait_for_process from config, using default %s", err)
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
				base.Warn("Error creating webhook %v", err)
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
	base.Logf("**NOTE:** %q's sync function has changed. The new function may assign different channels to documents, or permissions to users. You may want to re-sync the database to update these.", dbcontext.Name)
	return nil
}

func (sc *ServerContext) startShadowing(dbcontext *db.DatabaseContext, shadow *ShadowConfig) error {

	base.Warn("Bucket Shadowing feature comes with a number of limitations and caveats. See https://github.com/couchbase/sync_gateway/issues/1363 for more details.")

	var pattern *regexp.Regexp
	if shadow.Doc_id_regex != nil {
		var err error
		pattern, err = regexp.Compile(*shadow.Doc_id_regex)
		if err != nil {
			base.Warn("Invalid shadow doc_id_regex: %s", *shadow.Doc_id_regex)
			return err
		}
	}

	spec := base.BucketSpec{
		Server:     *shadow.Server,
		PoolName:   "default",
		BucketName: *shadow.Bucket,
		FeedType:   shadow.FeedType,
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
		base.Logf("Database %q shadowing remote bucket %q, pool %q, server <%s:%s/%s>", dbcontext.Name, spec.BucketName, spec.PoolName, url.Scheme, url.Host, url.Path)
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
	base.Logf("Closing db /%s (bucket %q)", context.Name, context.Bucket.GetName())
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
			princ.Name = &name
		}
		_, err := context.UpdatePrincipal(*princ, (what == "user"), isGuest)
		if err != nil {
			// A conflict error just means updatePrincipal didn't overwrite an existing user.
			if status, _ := base.ErrorAsHTTPStatus(err); status != http.StatusConflict {
				return fmt.Errorf("Couldn't create %s %q: %v", what, name, err)
			}
		} else if isGuest {
			base.Log("    Reset guest user to config")
		} else {
			base.Logf("    Created %s %q", what, name)
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
	base.Logf("Will report server stats for %q every %v",
		*sc.config.DeploymentID, interval)
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
	base.Logf("Reporting server stats to %s ...", kStatsReportURL)
	body, _ := json.Marshal(stats)
	bodyReader := bytes.NewReader(body)
	_, err := sc.HTTPClient.Post(kStatsReportURL, "application/json", bodyReader)
	if err != nil {
		base.Warn("Error posting stats: %v", err)
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

///////// ACCESS WARNINGS

// If no users defined (config or bucket), issue a warning + give tips to fix
// If guest user defined, but has no access to channels .. issue warning + tips to fix
func emitAccessRelatedWarnings(config *DbConfig, context *db.DatabaseContext) {
	for _, warning := range collectAccessRelatedWarnings(config, context) {
		base.Warn("%v", warning)
	}

}

func collectAccessRelatedWarnings(config *DbConfig, context *db.DatabaseContext) []string {

	currentDb, err := db.GetDatabase(context, nil)
	if err != nil {
		base.Warn("Could not get database, skipping access related warnings")
	}

	numUsersInDb := 0

	// If no users defined in config, and no users were returned from the view, add warning.
	// NOTE: currently ignoring the fact that the config could contain only disabled=true users.
	if len(config.Users) == 0 {

		// There are no users in the config, but there might be users in the db.  Find out
		// by querying the "view principals" view which will return users and roles.  We only want to
		// find out if there is at least one user (or role) defined, so set limit == 1 to minimize
		// performance hit of query.
		viewOptions := db.Body{
			"stale": false,
			"limit": 1,
		}
		vres, err := currentDb.Bucket.View(db.DesignDocSyncGateway, db.ViewPrincipals, viewOptions)
		if err != nil {
			base.Warn("Error trying to query ViewPrincipals: %v", err)
			return []string{}
		}

		numUsersInDb = len(vres.Rows)

		if len(vres.Rows) == 0 {
			noUsersWarning := fmt.Sprintf("No users have been defined in the '%v' database, which means that you will not be able to get useful data out of the sync gateway over the standard port.  FIX: define users in the configuration json or via the REST API on the admin port, and grant users to channels via the admin_channels parameter.", currentDb.Name)

			return []string{noUsersWarning}

		}

	}

	// If the GUEST user is the *only* user defined, but it is disabled or has no access to channels, add warning
	guestUser, ok := config.Users[base.GuestUsername]
	if ok == true {
		// Do we have any other users?  If so, we're done.
		if len(config.Users) > 1 || numUsersInDb > 1 {
			return []string{}
		}
		if guestUser.Disabled == true || len(guestUser.ExplicitChannels) == 0 {
			noGuestChannelsWarning := fmt.Sprintf("The GUEST user is the only user defined in the '%v' database, but is either disabled or has no access to any channels.  This means that you will not be able to get useful data out of the sync gateway over the standard port.  FIX: enable and/or grant access to the GUEST user to channels via the admin_channels parameter.", currentDb.Name)
			return []string{noGuestChannelsWarning}
		}
	}

	return []string{}

}
