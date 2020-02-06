package base

import (
	"crypto/tls"
	"strings"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/pkg/errors"
)

const CBGTIndexTypeSyncGatewayImport = "syncGateway-import-"
const DefaultImportPartitions = 16

// CbgtContext holds the two handles we have for CBGT-related functionality.
type CbgtContext struct {
	Manager     *cbgt.Manager // Manager is main entry point for initialization, registering indexes
	Cfg         *CfgSG        // Cfg manages storage of the current pindex set and node assignment
	Heartbeater Heartbeater   // Detects failed nodes when running in multi-node configuration
}

// StartShardedDCPFeed initializes and starts a CBGT Manager targeting the provided bucket.
// dbName is used to define a unique path name for local file storage of pindex files
func StartShardedDCPFeed(dbName string, bucket Bucket, spec BucketSpec, numPartitions uint16) (*CbgtContext, error) {

	cbgtContext, err := initCBGTManager(dbName, bucket, spec)
	if err != nil {
		return nil, err
	}

	// Start Manager.  Registers this node in the cfg
	err = cbgtContext.StartManager(dbName, bucket, spec, numPartitions)
	if err != nil {
		return nil, err
	}

	// Start heartbeater.  Sends heartbeats for this SG node, and triggers removal from cfg when
	// other SG nodes stop sending heartbeats.
	cbgtContext.Heartbeater, err = startHeartbeater(bucket, cbgtContext)
	if err != nil {
		return nil, err
	}

	return cbgtContext, nil
}

// cbgtFeedParams returns marshalled cbgt.DCPFeedParams as string, to be passed as feedparams during cbgt.Manager init.
// Used to pass basic auth credentials and xattr flag to cbgt.
func cbgtFeedParams(spec BucketSpec) (string, error) {
	feedParams := cbgt.NewDCPFeedParams()

	// check for basic auth
	if spec.Certpath == "" && spec.Auth != nil {
		username, password, _ := spec.Auth.GetCredentials()
		feedParams.AuthUser = username
		feedParams.AuthPassword = password
	}

	if spec.UseXattrs {
		// TODO: This is always being set in NewGocbDCPFeed, review whether we actually need ability to run w/ false
		feedParams.IncludeXAttrs = true
	}

	paramBytes, err := JSONMarshal(feedParams)
	if err != nil {
		return "", err
	}
	return string(paramBytes), nil
}

// Given a dbName, generate a unique and length-constrained index name for CBGT to use as part of their DCP name.
func GenerateIndexName(dbName string) string {
	// Index names *must* start with a letter, so we'll prepend 'db' before the per-database checksum (which starts with '0x')
	return "db" + Crc32cHashString([]byte(dbName)) + "_index"
}

// Creates a CBGT index definition for the specified bucket.  This adds the index definition
// to the manager's cbgt cfg.  Nodes that have registered for this indexType with the manager via
// RegisterPIndexImplType (see importListener.RegisterImportPindexImpl)
// will receive PIndexImpl callbacks (New, Open) for assigned PIndex to initiate DCP processing.
// TODO: If the index definition already exists in the cfg, it appears like this step can be bypassed,
//       as we don't currently have a scenario where we want to update an existing index def.  Needs
//       additional testing to validate.
func createCBGTIndex(manager *cbgt.Manager, dbName string, bucket Bucket, spec BucketSpec, numPartitions uint16) error {

	// sourceType is based on cbgt.source_gocouchbase non-public constant.
	// TODO: Request that cbgt make this and source_gocb public.
	sourceType := "gocb"
	if feedType == cbgtFeedType_cbdatasource {
		sourceType = "couchbase"
	}

	bucketUUID, err := bucket.UUID()
	if err != nil {
		return err
	}

	sourceParams, err := cbgtFeedParams(spec)
	if err != nil {
		return err
	}

	vbNo, err := bucket.GetMaxVbno()
	if err != nil {
		return errors.Wrapf(err, "Unable to retrieve maxVbNo for bucket %s", MD(bucket.GetName()).Redact())
	}

	// Calculate partitionsPerPIndex required to hit target DefaultImportPartitions
	if numPartitions == 0 || numPartitions > vbNo {
		numPartitions = DefaultImportPartitions
	}

	partitionsPerPIndex := vbNo / numPartitions

	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: int(partitionsPerPIndex), // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
		NumReplicas:            0,                        // No replicas required for SG sharded feed
	}

	// TODO: If this isn't well-formed JSON, cbgt emits errors when opening locally persisted pindex files.  Review
	//       how this can be optimized if we're not actually using it in the indexImpl
	indexParams := `{"name": "` + dbName + `"}`
	indexName := GenerateIndexName(dbName)
	Infof(KeyDCP, "Creating cbgt index %q for db %q", indexName, MD(dbName))

	// Required for initial pools request, before BucketDataSourceOptions kick in
	if spec.Certpath != "" {
		couchbase.SetCertFile(spec.Certpath)
		couchbase.SetKeyFile(spec.Keypath)
		couchbase.SetRootFile(spec.CACertPath)
		couchbase.SetSkipVerify(false)
	}

	// Register bucketDataSource callback for new index if we need to configure TLS
	cbgt.RegisterBucketDataSourceOptionsCallback(indexName, manager.UUID(), func(options *cbdatasource.BucketDataSourceOptions) *cbdatasource.BucketDataSourceOptions {
		if spec.IsTLS() {
			options.TLSConfig = func() *tls.Config {
				return spec.TLSConfig()
			}
		}
		options.ConnectBucket, options.Connect, options.ConnectTLS = alternateAddressShims(spec.IsTLS())
		return options
	})

	_, previousIndexUUID, err := getCBGTIndexUUID(manager, indexName)
	indexType := CBGTIndexTypeSyncGatewayImport + dbName
	err = manager.CreateIndex(
		sourceType,        // sourceType
		bucket.GetName(),  // sourceName
		bucketUUID,        // sourceUUID
		sourceParams,      // sourceParams
		indexType,         // indexType
		indexName,         // indexName
		indexParams,       // indexParams
		planParams,        // planParams
		previousIndexUUID, // prevIndexUUID
	)
	manager.Kick("NewIndexesCreated")

	Infof(KeyDCP, "Initialized sharded DCP feed %s with %d partitions.", indexName, numPartitions)
	return err

}

// Check if this CBGT index already exists.
func getCBGTIndexUUID(manager *cbgt.Manager, indexName string) (exists bool, previousUUID string, err error) {

	_, indexDefsMap, err := manager.GetIndexDefs(true)
	if err != nil {
		return false, "", errors.Wrapf(err, "Error calling CBGT GetIndexDefs() on index: %s", indexName)
	}

	indexDef, ok := indexDefsMap[indexName]
	if ok {
		return true, indexDef.UUID, nil
	} else {
		return false, "", nil
	}
}

// createCBGTManager creates a new manager for a given bucket and bucketSpec
// Inline comments below provide additional detail on how cbgt uses each manager
// parameter, and the implications for SG
func initCBGTManager(dbName string, bucket Bucket, spec BucketSpec) (*CbgtContext, error) {

	// uuid: Unique identifier for the node. Used to identify the node in the config.
	//       Without UUID persistence across SG restarts, a restarted SG node relies on heartbeater to remove
	// 		 the previous version of that node from the cfg, and assign pindexes to the new one.
	// 		(note that in a single node scenario, this can result in latency removing previous version of itself
	//  	on restart, if time between restarts is less than heartbeat expiry time).
	uuid := cbgt.NewUUID()

	gocbBucket, ok := AsGoCBBucket(bucket)
	if !ok {
		return nil, errors.New("gocbBucket required to initCBGTManager")
	}

	cfgCB, err := NewCfgSG(gocbBucket.Bucket)
	if err != nil {
		Warnf("Error initializing cfg for import sharding: %v", err)
		return nil, err
	}

	// tags: Every node participating in sharded DCP has feed, janitor, pindex and planner roles
	//   	"feed": runs dcp feed
	//   	"janitor": maintains feed instances, attempts to match to plan
	//   	"pindex": handles DCP events
	//   	"planner": assigns partitions to nodes
	// TODO: Every participating node acts as planner, which means every node is going to be trying to
	//       reassign partitions (vbuckets) to nodes when a cfg change is detected, and relying on cas
	//       failure in the cfg to avoid rework.  cbft doesn't do this -
	//       it leverages ns-server to identify a master node.
	//       Could revisit in future, as leader-elect style functionality is
	//       required for sg-replicate HA anyway
	tags := []string{"feed", "janitor", "pindex", "planner"}

	// weight: Allows for weighted distribution of vbuckets across participating nodes.  Not
	//         used by Sync Gateway
	weight := 1

	// container: Used by cbgt to determine node hierarchy.  Not needed by Sync Gateway
	container := ""

	// extras: Can be used to pass Sync Gateway specific node information to callbacks.  Not
	//         currently needed by Sync Gateway.
	extras := ""

	// bindHttp: Used for REST binding (not needed by Sync Gateway), but also as a unique identifier
	//           in some circumstances.  See below for additional information.
	// 		(from accel): use the uuid as the bindHttp so that we don't have to make the user
	// 		configure this, since we're just using for uniqueness and not for REST API
	// 		More info here:
	//   		https://github.com/couchbaselabs/cbgt/issues/1
	//   		https://github.com/couchbaselabs/cbgt/issues/25
	bindHttp := uuid

	// serverURL: Passing gocb connect string.
	var serverURL string
	if feedType == cbgtFeedType_cbdatasource {
		// cbdatasource expects server URL in http format
		serverURLs, errConvertServerSpec := CouchbaseURIToHttpURL(bucket, spec.Server)
		if errConvertServerSpec != nil {
			return nil, errConvertServerSpec
		}
		serverURL = strings.Join(serverURLs, ";")
	} else {
		serverURL, err = spec.GetGoCBConnString()
		if err != nil {
			return nil, err
		}
	}

	// dataDir: file system location for files persisted by cbgt.  Not required by SG, setting to empty
	//   avoids file system usage, in conjunction with managerLoadDataDir=false in options.
	dataDir := ""

	// eventHandlers: SG doesn't currently do any processing on manager events:
	//   - OnRegisterPIndex
	//   - OnUnregisterPIndex
	//   - OnFeedError
	var eventHandlers cbgt.ManagerEventHandlers

	// Specify one feed per pindex
	options := make(map[string]string)
	options[cbgt.FeedAllotmentOption] = cbgt.FeedAllotmentOnePerPIndex
	options["managerLoadDataDir"] = "false"

	// Creates a new cbgt manager.
	mgr := cbgt.NewManagerEx(
		cbgt.VERSION, // cbgt metadata version
		cfgCB,
		uuid,
		tags,
		container,
		weight,
		extras,
		bindHttp,
		dataDir,
		serverURL,
		eventHandlers,
		options)

	cbgtContext := &CbgtContext{
		Manager: mgr,
		Cfg:     cfgCB,
	}
	return cbgtContext, nil
}

// StartManager registers this node with cbgt, and the janitor will start feeds on this node.
func (c *CbgtContext) StartManager(dbName string, bucket Bucket, spec BucketSpec, numPartitions uint16) (err error) {

	// TODO: Clarify the functional difference between registering the manager as 'wanted' vs 'known'.
	registerType := cbgt.NODE_DEFS_WANTED
	if err := c.Manager.Start(registerType); err != nil {
		Errorf("cbgt Manager start failed: %v", err)
		return err
	}

	// Add the index definition for this feed to the cbgt cfg, in case it's not already present.
	err = createCBGTIndex(c.Manager, dbName, bucket, spec, numPartitions)
	if err != nil {
		Errorf("cbgt index creation failed: %v", err)
		return err
	}

	return nil
}

func initCfgCB(bucket Bucket, spec BucketSpec) (*cbgt.CfgCB, error) {

	// cfg: Implementation of cbgt.Cfg interface.  Responsible for configuration management
	//      Sync Gateway uses bucket-based config management
	options := map[string]interface{}{
		"keyPrefix": SyncPrefix,
	}
	urls, errConvertServerSpec := CouchbaseURIToHttpURL(bucket, spec.Server)
	if errConvertServerSpec != nil {
		return nil, errConvertServerSpec
	}

	// TODO: Until we switch to a gocb-backed CfgCB, need to manage credentials in URL
	basicAuthUrls, err := ServerUrlsWithAuth(urls, spec)
	if err != nil {
		return nil, err
	}

	cfgCB, err := cbgt.NewCfgCBEx(
		strings.Join(basicAuthUrls, ";"),
		spec.BucketName,
		options,
	)
	if err != nil {
		return nil, err
	}

	return cfgCB, nil
}

func startHeartbeater(bucket Bucket, cbgtContext *CbgtContext) (Heartbeater, error) {

	if cbgtContext == nil || cbgtContext.Manager == nil || cbgtContext.Cfg == nil {
		return nil, errors.New("Unable to start heartbeater with nil manager or cfg")
	}
	// Initialize handler to use cfg to determine the set of participating nodes
	nodeListHandler, err := NewCBGTNodeListHandler(cbgtContext.Cfg)
	if err != nil {
		return nil, err
	}

	// Create heartbeater
	heartbeater, err := NewCouchbaseHeartbeater(bucket, SyncPrefix, cbgtContext.Manager.UUID(), nodeListHandler)
	if err != nil {
		return nil, errors.Wrapf(err, "Error starting heartbeater for bucket %s", MD(bucket.GetName()).Redact())
	}

	// TODO: Allow customization of heartbeat interval
	intervalSeconds := 1
	Debugf(KeyDCP, "Sending CBGT node heartbeats at interval: %vs", intervalSeconds)
	err = heartbeater.StartSendingHeartbeats(intervalSeconds)
	if err != nil {
		return nil, err
	}

	deadNodeHandler := HeartbeatStoppedHandler{
		Cfg:     cbgtContext.Cfg,
		Manager: cbgtContext.Manager,
	}

	staleThresholdMs := intervalSeconds * 10 * 1000
	if err := heartbeater.StartCheckingHeartbeats(staleThresholdMs, deadNodeHandler); err != nil {
		return nil, errors.Wrapf(err, "Error calling StartCheckingHeartbeats() during startHeartbeater for bucket %s", MD(bucket.GetName).Redact())
	}
	Debugf(KeyDCP, "Checking CBGT node heartbeats with stale threshold: %v ms", staleThresholdMs)

	return heartbeater, nil

}

// When we detect other nodes have stopped pushing heartbeats, remove from CBGT cfg
type HeartbeatStoppedHandler struct {
	Cfg     cbgt.Cfg
	Manager *cbgt.Manager
}

func (h HeartbeatStoppedHandler) StaleHeartBeatDetected(nodeUUID string) {

	Debugf(KeyDCP, "StaleHeartBeatDetected for node: %v", nodeUUID)
	err := cbgt.UnregisterNodes(h.Cfg, h.Manager.Version(), []string{nodeUUID})
	if err != nil {
		Warnf("base.Warning: attempt to unregister %v from CBGT got error: %v", nodeUUID, err)
	}
}
