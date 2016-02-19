package sg_accel

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"

	"github.com/couchbase/cb-heartbeat"
	"github.com/couchbase/cbgt"
	"github.com/couchbase/cbgt/cmd"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/db"
	"github.com/couchbase/sync_gateway/indexwriter"
	"github.com/couchbase/sync_gateway/rest"
)

// Context for sg_accel.  Embeds a Sync Gateway server context for config-based management of server and databases.
type SGAccelContext struct {
	serverContext *rest.ServerContext
	cbgtContext   *indexwriter.CbgtContext
	indexWriter   *indexwriter.IndexWriter
}

func NewSGAccelContext(config *rest.ServerConfig) *SGAccelContext {

	// Initialize server context, which includes initial database configurations
	ac := &SGAccelContext{
		serverContext: rest.NewServerContext(config),
	}

	// Initialize databases
	var databases []*db.DatabaseContext
	for _, dbConfig := range config.Databases {
		database, err := ac.serverContext.AddDatabaseFromConfig(dbConfig)
		if err != nil {
			base.LogFatal("Error opening database: %v", err)
		}
		databases = append(databases, database)
	}

	// Instantiate CBGT Manager
	cbgtContext, err := ac.InitCBGTManager(config)
	if err != nil {
		base.LogFatal("Error initializing CBGT Manager: %v", err)
	}
	ac.cbgtContext = &cbgtContext

	// Initialize index writers for initial set of databases.
	err = ac.InitIndexWriters(databases)
	if err != nil {
		base.LogFatal("Error initializing index writer: %v", err)
	}

	// Initialize CBGT context
	err = ac.InitCBGT(databases)
	if err != nil {
		base.LogFatal("Error initializing CBGT: %v", err)
	}

	if err := ac.registerCbgtPindexType(); err != nil {
		log.Fatalf("Fatal error initializing CBGT: %v", err)
	}

	return ac
}

// Initialize index writer.   Initializes a writer for each database, and directs change notifications for
// that database to the writer.
func (ac *SGAccelContext) InitIndexWriters(databases []*db.DatabaseContext) error {

	// loop through all databases and init index writer for each
	for _, dbContext := range databases {
		if _, err := ac.indexWriter.AddDbIndexWriter(dbContext, ac.cbgtContext); err != nil {
			return err
		}
	}
	return nil
}

func (ac *SGAccelContext) InitCBGT(databases []*db.DatabaseContext) error {

	// loop through all databases and init CBGT index
	for _, dbContext := range databases {
		if err := indexwriter.CreateCBGTIndexForDatabase(dbContext, ac.cbgtContext); err != nil {
			return err
		}
	}

	// Since we've created new indexes via the dbContext.InitCBGT() calls above,
	// we need to kick the manager to recognize them.
	ac.cbgtContext.Manager.Kick("NewIndexesCreated")

	// Make sure that we don't have multiple partitions subscribed to the same DCP shard
	// for a given bucket.
	if err := ac.validateCBGTPartitionMap(databases); err != nil {
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
func (ac *SGAccelContext) validateCBGTPartitionMap(databases []*db.DatabaseContext) error {

	validCBGTIndexNames := map[string]string{}
	for _, dbContext := range databases {
		cbgtIndexName := indexwriter.GetCBGTIndexNameForBucket(dbContext.Bucket)
		validCBGTIndexNames[cbgtIndexName] = "n/a"
	}

	_, planPIndexesByName, _ := ac.cbgtContext.Manager.GetPlanPIndexes(true)

	for cbgtIndexName, _ := range planPIndexesByName {
		_, ok := validCBGTIndexNames[cbgtIndexName]
		if !ok {
			base.Warn("Deleting CBGT index %v since it is invalid for the current Sync Gateway configuration.  Valid index names: %v", cbgtIndexName, validCBGTIndexNames)
			ac.cbgtContext.Manager.DeleteIndex(cbgtIndexName)
		}
	}

	return nil
}

func (ac *SGAccelContext) InitCBGTManager(config *rest.ServerConfig) (indexwriter.CbgtContext, error) {

	base.LogTo("DIndex+", "Initializing CBGT")

	couchbaseUrl, err := base.CouchbaseUrlWithAuth(
		*config.ClusterConfig.Server,
		config.ClusterConfig.Username,
		config.ClusterConfig.Password,
		*config.ClusterConfig.Bucket,
	)
	if err != nil {
		return indexwriter.CbgtContext{}, err
	}

	uuid, err := cmd.MainUUID(indexwriter.IndexTypeSyncGateway, config.ClusterConfig.DataDir)
	if err != nil {
		return indexwriter.CbgtContext{}, err
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
		*config.ClusterConfig.Bucket,
		options,
	)
	if err != nil {
		return indexwriter.CbgtContext{}, err
	}

	tags := []string{"feed", "janitor", "pindex", "planner"}
	container := ""
	weight := 1                            // this would allow us to have more pindexes serviced by this node
	server := *config.ClusterConfig.Server // or use "." (don't bother checking)
	extras := ""
	var managerEventHandlers cbgt.ManagerEventHandlers

	// refresh it so we have a fresh copy
	if err := cfgCb.Refresh(); err != nil {
		return indexwriter.CbgtContext{}, err
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
		config.ClusterConfig.DataDir,
		server,
		managerEventHandlers,
	)

	err = manager.Start(register)
	if err != nil {
		return indexwriter.CbgtContext{}, err
	}

	if err := ac.enableCBGTAutofailover(
		cbgt.VERSION,
		manager,
		cfgCb,
		uuid,
		couchbaseUrl,
		db.KSyncKeyPrefix,
		config,
	); err != nil {
		return indexwriter.CbgtContext{}, err
	}

	cbgtContext := indexwriter.CbgtContext{
		Manager: manager,
		Cfg:     cfgCb,
	}

	return cbgtContext, nil

}

func (ac *SGAccelContext) enableCBGTAutofailover(version string, mgr *cbgt.Manager, cfg cbgt.Cfg, uuid, couchbaseUrl, keyPrefix string, config *rest.ServerConfig) error {

	base.LogTo("DIndex+", "Enabling CBGT auto-failover")
	cbHeartbeater, err := cbheartbeat.NewCouchbaseHeartbeater(
		couchbaseUrl,
		*config.ClusterConfig.Bucket,
		keyPrefix,
		uuid,
	)
	if err != nil {
		return err
	}

	intervalMs := 10000

	if config.ClusterConfig.HeartbeatIntervalSeconds != nil {
		intervalMs = int(*config.ClusterConfig.HeartbeatIntervalSeconds) * 1000
	}

	base.LogTo("DIndex+", "Sending CBGT node heartbeats at interval: %v ms", intervalMs)
	cbHeartbeater.StartSendingHeartbeats(intervalMs)

	deadNodeHandler := indexwriter.HeartbeatStoppedHandler{
		Cfg:         cfg,
		CbgtVersion: version,
		Manager:     mgr,
	}

	staleThresholdMs := intervalMs * 10
	if err := cbHeartbeater.StartCheckingHeartbeats(staleThresholdMs, deadNodeHandler); err != nil {
		return err
	}
	base.LogTo("DIndex+", "Checking CBGT node heartbeats with stale threshold: %v ms", staleThresholdMs)

	return nil

}

func (ac *SGAccelContext) registerCbgtPindexType() error {

	// The Description format is:
	//
	//     $categoryName/$indexType - short descriptive string
	//
	// where $categoryName is something like "advanced", or "general".
	description := fmt.Sprintf("%v/%v - Sync Gateway", indexwriter.IndexCategorySyncGateway, indexwriter.IndexTypeSyncGateway)

	// Register with CBGT
	cbgt.RegisterPIndexImplType(indexwriter.IndexTypeSyncGateway, &cbgt.PIndexImplType{
		New:         ac.NewSyncGatewayPIndexFactory,
		Open:        ac.OpenSyncGatewayPIndexFactory,
		Count:       nil,
		Query:       nil,
		Description: description,
	})
	return nil

}

// When CBGT is opening a PIndex for the first time, it will call back this method.
func (ac *SGAccelContext) NewSyncGatewayPIndexFactory(indexType, indexParams, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	os.MkdirAll(path, 0700)

	indexParamsStruct := indexwriter.SyncGatewayIndexParams{}
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

	pindexImpl, dest, err := ac.SyncGatewayPIndexFactoryCommon(indexParamsStruct)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, nil, err
	}

	if pindexImpl == nil {
		base.Warn("PIndex for dest %v is nil", dest)
		return nil, nil, fmt.Errorf("PIndex for dest %v is nil.  This could be due to the pindex stored on disk not corresponding to any configured databases.", dest)
	}

	return pindexImpl, dest, nil

}

// When CBGT is re-opening an existing PIndex (after a Sync Gw restart for example), it will call back this method.
func (ac *SGAccelContext) OpenSyncGatewayPIndexFactory(indexType, path string, restart func()) (cbgt.PIndexImpl, cbgt.Dest, error) {

	buf, err := ioutil.ReadFile(path +
		string(os.PathSeparator) + "SyncGatewayIndexParams.json")
	if err != nil {
		return nil, nil, err
	}

	indexParams := indexwriter.SyncGatewayIndexParams{}
	err = json.Unmarshal(buf, &indexParams)
	if err != nil {
		errWrap := fmt.Errorf("Could not unmarshal buf: %v err: %v", buf, err)
		log.Fatalf("%v", errWrap)
		return nil, nil, errWrap
	}

	pindexImpl, dest, err := ac.SyncGatewayPIndexFactoryCommon(indexParams)
	if err != nil {
		log.Fatalf("%v", err)
		return nil, nil, err
	}

	if pindexImpl == nil {
		base.Warn("PIndex for dest %v is nil", dest)
		return nil, nil, fmt.Errorf("PIndex for dest %v is nil.  This could be due to the pindex stored on disk not corresponding to any configured databases.", dest)
	}

	return pindexImpl, dest, nil

}

func (ac *SGAccelContext) SyncGatewayPIndexFactoryCommon(indexParams indexwriter.SyncGatewayIndexParams) (cbgt.PIndexImpl, cbgt.Dest, error) {

	bucketName := indexParams.BucketName

	// lookup the database context based on the indexParams
	dbName := ac.serverContext.FindDbByBucketName(bucketName)
	if dbName == "" {
		base.Warn("Could not find database for bucket name: %v.  Unable to instantiate this pindex from disk.", bucketName)
		return nil, nil, nil
	}

	dbContext, err := ac.serverContext.GetDatabase(dbName)
	if err != nil || dbContext == nil {
		err := fmt.Errorf("Could not find database for name: %v : %v", dbName, err)
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

	result := indexwriter.NewSyncGatewayPIndex(eventFeed, bucket, tapListener.TapArgs, stableClock)

	return result, result, nil

}
