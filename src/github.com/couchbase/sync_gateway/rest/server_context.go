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
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/cb-heartbeat"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbaselabs/cbgt"
	"github.com/couchbaselabs/cbgt/cmd"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
)

// The URL that stats will be reported to if deployment_id is set in the config
const kStatsReportURL = "http://localhost:9999/stats"
const kStatsReportInterval = time.Hour
const kDefaultSlowServerCallWarningThreshold = 200 // ms

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
	CbgtContext base.CbgtContext
}

func NewServerContext(config *ServerConfig) *ServerContext {
	sc := &ServerContext{
		config:     config,
		databases_: map[string]*db.DatabaseContext{},
		HTTPClient: http.DefaultClient,
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

	if err := sc.registerCbgtPindexType(); err != nil {
		log.Fatalf("Fatal error initializing CBGT: %v", err)
	}

	return sc
}

func (sc *ServerContext) registerCbgtPindexType() error {

	// The Description format is:
	//
	//     $categoryName/$indexType - short descriptive string
	//
	// where $categoryName is something like "advanced", or "general".
	description := fmt.Sprintf("%v/%v - Sync Gateway", base.IndexCategorySyncGateway, base.IndexTypeSyncGateway)

	// Register with CBGT
	cbgt.RegisterPIndexImplType(base.IndexTypeSyncGateway, &cbgt.PIndexImplType{
		New:         sc.NewSyncGatewayPIndexFactory,
		Open:        sc.OpenSyncGatewayPIndexFactory,
		Count:       nil,
		Query:       nil,
		Description: description,
	})
	return nil

}

// When CBGT is opening a PIndex for the first time, it will call back this method.
func (sc *ServerContext) NewSyncGatewayPIndexFactory(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	os.MkdirAll(path, 0700)

	indexParamsStruct := base.SyncGatewayIndexParams{}
	err := json.Unmarshal([]byte(indexParams), &indexParamsStruct)
	if err != nil {
		return nil, nil, fmt.Errorf("Error unmarshalling indexParams: %v.  indexParams: %v", err, indexParams)
	}

	pathMeta := path + string(os.PathSeparator) + "SyncGatewayIndexParams.json"
	err = ioutil.WriteFile(pathMeta, []byte(indexParams), 0600)
	if err != nil {
		errWrap := fmt.Errorf("Error writing %v to %v. Err: %v", indexParams, pathMeta, err)
		log.Fatalf("%v", errWrap)
		return nil, nil, errWrap
	}

	pindexImpl, dest, err := sc.SyncGatewayPIndexFactoryCommon(indexParamsStruct)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, nil, err
	}

	return pindexImpl, dest, nil

}

// When CBGT is re-opening an existing PIndex (after a Sync Gw restart for example), it will call back this method.
func (sc *ServerContext) OpenSyncGatewayPIndexFactory(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	buf, err := ioutil.ReadFile(path +
		string(os.PathSeparator) + "SyncGatewayIndexParams.json")
	if err != nil {
		return nil, nil, err
	}

	indexParams := base.SyncGatewayIndexParams{}
	err = json.Unmarshal(buf, &indexParams)
	if err != nil {
		errWrap := fmt.Errorf("Could not unmarshal buf: %v err: %v", buf, err)
		log.Fatalf("%v", errWrap)
		return nil, nil, errWrap
	}

	pindexImpl, dest, err := sc.SyncGatewayPIndexFactoryCommon(indexParams)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, nil, err
	}

	return pindexImpl, dest, nil

}

func (sc *ServerContext) SyncGatewayPIndexFactoryCommon(indexParams base.SyncGatewayIndexParams) (cbgt.PIndexImpl, cbgt.Dest, error) {

	bucketName := indexParams.BucketName

	// lookup the database context based on the indexParams
	dbName := sc.findDbByBucketName(bucketName)
	if dbName == "" {
		err := fmt.Errorf("Could not find database for bucket name: %v", bucketName)
		log.Fatalf("%v", err)
		return nil, nil, err
	}

	dbContext, ok := sc.databases_[dbName]
	if !ok {
		err := fmt.Errorf("Could not find database for name: %v", dbName)
		log.Fatalf("%v", err)
		return nil, nil, err
	}

	// get the bucket, and type assert to a couchbase bucket
	baseBucket := dbContext.Bucket
	bucket, ok := baseBucket.(base.CouchbaseBucket)
	if !ok {
		err := fmt.Errorf("Type assertion fail to Couchbasebucket: %+v", baseBucket)
		log.Fatalf("%v", err)
		return nil, nil, err
	}

	tapListener := dbContext.TapListener()

	eventFeed := tapListener.TapFeed().WriteEvents()

	stableClock, err := dbContext.GetStableClock()
	if err != nil {
		errWrap := fmt.Errorf("Error getting stable clock: %v", err)
		log.Fatalf("%v", errWrap)
		return nil, nil, errWrap
	}

	result := base.NewSyncGatewayPIndex(eventFeed, bucket, tapListener.TapArgs, stableClock)

	return result, result, nil

}

func (sc *ServerContext) findDbByBucketName(bucketName string) string {

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

func (sc *ServerContext) AllDatabaseNames() []string {
	sc.lock.Lock()
	defer sc.lock.Unlock()

	names := make([]string, 0, len(sc.databases_))
	for name, _ := range sc.databases_ {
		names = append(names, name)
	}
	return names
}

func (sc *ServerContext) InitCBGT() error {

	cbgtContext, err := sc.InitCBGTManager()
	if err != nil {
		return err
	}
	sc.CbgtContext = cbgtContext

	// loop through all databases and init CBGT index
	for _, dbContext := range sc.databases_ {
		dbContext.BucketSpec.CbgtContext = sc.CbgtContext
		if err := dbContext.CreateCBGTIndex(); err != nil {
			return err
		}
	}

	// Since we've created new indexes via the dbContext.InitCBGT() calls above,
	// we need to kick the manager to recognize them.
	cbgtContext.Manager.Kick("NewIndexesCreated")

	// Make sure that we don't have multiple partitions subscribed to the same DCP shard
	// for a given bucket.
	if err := sc.validateCBGTPartitionMap(); err != nil {
		return err
	}

	return nil

}

// This was created in reaction to a development bug that was found:
//    https://github.com/couchbase/sync_gateway/issues/1129
// What this method does is to loop through all the databases and build
// up a map of what the CBGT index names _should be_.  Then, it loops over
// all of the known CBGT indexes, and makes sure the CBGT index name is a valid
// name by checking if it's in the map.  If not, return an error.
func (sc *ServerContext) validateCBGTPartitionMap() error {

	validCBGTIndexNames := map[string]string{}
	for _, dbContext := range sc.databases_ {
		cbgtIndexName := dbContext.GetCBGTIndexNameForBucket(dbContext.Bucket)
		validCBGTIndexNames[cbgtIndexName] = "n/a"
	}

	_, planPIndexesByName, _ := sc.CbgtContext.Manager.GetPlanPIndexes(true)

	for cbgtIndexName, _ := range planPIndexesByName {
		_, ok := validCBGTIndexNames[cbgtIndexName]
		if !ok {
			return fmt.Errorf(
				"CBGT contains an invalid index name: %v.  Aborting. "+
					"Valid index names: %v",
				cbgtIndexName,
				validCBGTIndexNames,
			)
		}
	}

	return nil
}

func (sc *ServerContext) InitCBGTManager() (base.CbgtContext, error) {

	couchbaseUrl, err := base.CouchbaseUrlWithAuth(
		*sc.config.ClusterConfig.Server,
		sc.config.ClusterConfig.Username,
		sc.config.ClusterConfig.Password,
		*sc.config.ClusterConfig.Bucket,
	)
	if err != nil {
		return base.CbgtContext{}, err
	}

	uuid, err := cmd.MainUUID(base.IndexTypeSyncGateway, sc.config.ClusterConfig.DataDir)
	if err != nil {
		return base.CbgtContext{}, err
	}

	// this tells CBGT that we are brining a new CBGT node online
	register := "wanted"

	// use the uuid as the bindHttp so that we don't have to make the user
	// configure this, and since as far as the REST Api interface, we'll be using
	// whatever is configured in adminInterface anyway.
	// More info here:
	//   https://github.com/couchbaselabs/cbgt/issues/1
	//   https://github.com/couchbaselabs/cbgt/issues/25
	bindHttp := uuid

	options := map[string]interface{}{
		"keyPrefix": db.KSyncKeyPrefix,
	}

	cfgCb, err := cbgt.NewCfgCBEx(
		couchbaseUrl,
		*sc.config.ClusterConfig.Bucket,
		options,
	)
	if err != nil {
		return base.CbgtContext{}, err
	}

	tags := []string{"feed", "janitor", "pindex", "planner"}
	container := ""
	weight := 1                               // this would allow us to have more pindexes serviced by this node
	server := *sc.config.ClusterConfig.Server // or use "." (don't bother checking)
	extras := ""
	var managerEventHandlers cbgt.ManagerEventHandlers

	// refresh it so we have a fresh copy
	if err := cfgCb.Refresh(); err != nil {
		return base.CbgtContext{}, err
	}

	manager := cbgt.NewManager(
		cbgt.VERSION,
		cfgCb,
		uuid,
		tags,
		container,
		weight,
		extras,
		bindHttp,
		sc.config.ClusterConfig.DataDir,
		server,
		managerEventHandlers,
	)

	err = manager.Start(register)
	if err != nil {
		return base.CbgtContext{}, err
	}

	if err := sc.enableCBGTAutofailover(
		cbgt.VERSION,
		manager,
		cfgCb,
		uuid,
		couchbaseUrl,
		db.KSyncKeyPrefix,
	); err != nil {
		return base.CbgtContext{}, err
	}

	cbgtContext := base.CbgtContext{
		Manager: manager,
		Cfg:     cfgCb,
	}

	return cbgtContext, nil

}

func (sc *ServerContext) enableCBGTAutofailover(version string, mgr *cbgt.Manager, cfg cbgt.Cfg, uuid, couchbaseUrl, keyPrefix string) error {

	cbHeartbeater, err := cbheartbeat.NewCouchbaseHeartbeater(
		couchbaseUrl,
		*sc.config.ClusterConfig.Bucket,
		keyPrefix,
		uuid,
	)
	if err != nil {
		return err
	}

	intervalMs := 10000

	if sc.config.ClusterConfig.HeartbeatIntervalSeconds != nil {
		intervalMs = int(*sc.config.ClusterConfig.HeartbeatIntervalSeconds) * 1000
	}

	cbHeartbeater.StartSendingHeartbeats(intervalMs)

	deadNodeHandler := base.HeartbeatStoppedHandler{
		Cfg:         cfg,
		CbgtVersion: version,
		Manager:     mgr,
	}

	staleThresholdMs := intervalMs * 10
	if err := cbHeartbeater.StartCheckingHeartbeats(staleThresholdMs, deadNodeHandler); err != nil {
		return err
	}

	return nil

}

// Adds a database to the ServerContext.  Attempts a read after it gets the write
// lock to see if it's already been added by another process. If so, returns either the
// existing DatabaseContext or an error based on the useExisting flag.
func (sc *ServerContext) getOrAddDatabaseFromConfig(config *DbConfig, useExisting bool) (*db.DatabaseContext, error) {
	// Obtain write lock during add database, to avoid race condition when creating based on ConfigServer
	sc.lock.Lock()
	defer sc.lock.Unlock()

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
	}

	// If we are using DCPSHARD feed type, set CbgtContext on bucket spec
	if feedType == strings.ToLower(base.DcpShardFeedType) {
		spec.CbgtContext = sc.CbgtContext
	}

	if config.Username != "" {
		spec.Auth = config
	}

	if config.FeedParams != nil {
		config.FeedParams.SetDefaultValues()
		spec.FeedParams = *config.FeedParams
		spec.FeedParams.ValidateOrPanic()
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
	}

	bucket, err := db.ConnectToBucket(spec)
	if err != nil {
		return nil, err
	}

	// Channel index definition, if present
	channelIndexOptions := &db.ChangeIndexOptions{}
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
		base.Logf("Opening index bucket %q, pool %q, server <%s>",
			indexBucketName, indexPool, indexServer)

		channelIndexOptions.Bucket, err = base.GetBucket(indexSpec)
		if err != nil {
			base.Logf("error Opening index bucket %q, pool %q, server <%s>",
				indexBucketName, indexPool, indexServer)
			// TODO: revert to local index?
			return nil, err
		}
		// TODO: separate config of hash bucket
		sequenceHashOptions.Bucket = channelIndexOptions.Bucket
		sequenceHashOptions.Size = 32
	} else {
		channelIndexOptions = nil
	}

	contextOptions := db.DatabaseContextOptions{
		CacheOptions:        &cacheOptions,
		IndexOptions:        channelIndexOptions,
		SequenceHashOptions: sequenceHashOptions,
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

	// Register it so HTTP handlers can find it:
	sc.databases_[dbcontext.Name] = dbcontext

	// Save the config
	sc.config.Databases[config.Name] = config
	return dbcontext, nil
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
		for k, _ := range eventHandlersMap {
			if k != "max_processes" && k != "wait_for_process" && k != "document_changed" {
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

	bucket, err := base.GetBucket(spec)
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
	j := json.NewDecoder(res.Body)
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
		for _ = range sc.statsTicker.C {
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
