/*
Copyright 2019-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package base

import (
	"context"
	"crypto/tls"
	"strings"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/pkg/errors"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

const CBGTIndexTypeSyncGatewayImport = "syncGateway-import-"
const DefaultImportPartitions = 16

// CbgtContext holds the two handles we have for CBGT-related functionality.
type CbgtContext struct {
	Manager           *cbgt.Manager            // Manager is main entry point for initialization, registering indexes
	Cfg               cbgt.Cfg                 // Cfg manages storage of the current pindex set and node assignment
	heartbeater       Heartbeater              // Heartbeater used for failed node detection
	heartbeatListener *importHeartbeatListener // Listener subscribed to failed node alerts from heartbeater
	loggingCtx        context.Context          // Context for cbgt logging
}

// StartShardedDCPFeed initializes and starts a CBGT Manager targeting the provided bucket.
// dbName is used to define a unique path name for local file storage of pindex files
func StartShardedDCPFeed(dbName string, uuid string, heartbeater Heartbeater, bucket Bucket, spec BucketSpec, numPartitions uint16, cfg cbgt.Cfg) (*CbgtContext, error) {

	cbgtContext, err := initCBGTManager(bucket, spec, cfg, uuid)
	if err != nil {
		return nil, err
	}

	dcpContextID := MD(spec.BucketName).Redact() + "-" + DCPImportFeedID
	cbgtContext.loggingCtx = context.WithValue(context.Background(), LogContextKey{},
		LogContext{CorrelationID: dcpContextID},
	)

	// Start Manager.  Registers this node in the cfg
	err = cbgtContext.StartManager(dbName, bucket, spec, numPartitions)
	if err != nil {
		return nil, err
	}

	// Register heartbeat listener to trigger removal from cfg when
	// other SG nodes stop sending heartbeats.
	listener, err := registerHeartbeatListener(heartbeater, cbgtContext)
	if err != nil {
		return nil, err
	}

	cbgtContext.heartbeater = heartbeater
	cbgtContext.heartbeatListener = listener

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

// Given a dbName, generates a name based on the approach used prior to CBG-626.  Used for upgrade handling
func GenerateLegacyIndexName(dbName string) string {
	return dbName + "_import"
}

// Creates a CBGT index definition for the specified bucket.  This adds the index definition
// to the manager's cbgt cfg.  Nodes that have registered for this indexType with the manager via
// RegisterPIndexImplType (see importListener.RegisterImportPindexImpl)
// will receive PIndexImpl callbacks (New, Open) for assigned PIndex to initiate DCP processing.
// TODO: If the index definition already exists in the cfg, it appears like this step can be bypassed,
//       as we don't currently have a scenario where we want to update an existing index def.  Needs
//       additional testing to validate.
func createCBGTIndex(c *CbgtContext, dbName string, bucket Bucket, spec BucketSpec, numPartitions uint16) error {

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

	// Required for initial pools request, before BucketDataSourceOptions kick in
	if spec.Certpath != "" && spec.Keypath != "" {
		couchbase.SetCertFile(spec.Certpath)
		couchbase.SetKeyFile(spec.Keypath)
	}
	if spec.CACertPath != "" {
		couchbase.SetRootFile(spec.CACertPath)
		couchbase.SetSkipVerify(false)
	}

	connSpec, err := gocbconnstr.Parse(spec.Server)
	if err != nil {
		return err
	}

	// Determine index name and UUID
	indexName, previousIndexUUID := dcpSafeIndexName(c, dbName)
	InfofCtx(c.loggingCtx, KeyDCP, "Creating cbgt index %q for db %q", indexName, MD(dbName))

	// Register bucketDataSource callback for new index if we need to configure TLS
	cbgt.RegisterBucketDataSourceOptionsCallback(indexName, c.Manager.UUID(), func(options *cbdatasource.BucketDataSourceOptions) *cbdatasource.BucketDataSourceOptions {
		if spec.IsTLS() {
			options.TLSConfig = func() *tls.Config {
				return spec.TLSConfig()
			}
		}
		options.ConnectBucket, options.Connect, options.ConnectTLS = alternateAddressShims(c.loggingCtx, spec.IsTLS(), connSpec.Addresses)
		return options
	})

	indexType := CBGTIndexTypeSyncGatewayImport + dbName
	err = c.Manager.CreateIndex(
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
	c.Manager.Kick("NewIndexesCreated")

	InfofCtx(c.loggingCtx, KeyDCP, "Initialized sharded DCP feed %s with %d partitions.", indexName, numPartitions)
	return err

}

// dcpSafeIndexName returns an index name and previousIndexUUID to handle upgrade scenarios from the
// legacy index name format ("dbname_import") to the new length-safe format ("db[crc32]_index").
// Handles removal of legacy index definitions, except for the case where the legacy index is
// the only index defined, and the name is safe.  In that case, continue using legacy index name
// to avoid restarting the import processing from zero
func dcpSafeIndexName(c *CbgtContext, dbName string) (safeIndexName, previousUUID string) {

	indexName := GenerateIndexName(dbName)
	legacyIndexName := GenerateLegacyIndexName(dbName)

	_, indexUUID, _ := getCBGTIndexUUID(c.Manager, indexName)
	_, legacyIndexUUID, _ := getCBGTIndexUUID(c.Manager, legacyIndexName)

	// 200 is the recommended maximum DCP stream name length
	// cbgt adds 41 characters to index name we provide when naming the DCP stream, rounding up to 50 defensively
	safeIndexNameLen := 200 - 50

	// Check for the case where we want to continue using legacy index name:
	if legacyIndexUUID != "" && indexUUID == "" && len(legacyIndexName) < safeIndexNameLen {
		return legacyIndexName, legacyIndexUUID
	}

	// Otherwise, remove legacy if it exists, and return new format
	if legacyIndexUUID != "" {
		deleteErr := c.Manager.DeleteIndexEx(legacyIndexName, "")
		if deleteErr != nil {
			Warnf("Error removing legacy import feed index: %v", deleteErr)
		}
	}
	return indexName, indexUUID
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
func initCBGTManager(bucket Bucket, spec BucketSpec, cfgSG cbgt.Cfg, dbUUID string) (*CbgtContext, error) {

	// uuid: Unique identifier for the node. Used to identify the node in the config.
	//       Without UUID persistence across SG restarts, a restarted SG node relies on heartbeater to remove
	// 		 the previous version of that node from the cfg, and assign pindexes to the new one.
	// 		(note that in a single node scenario, this can result in latency removing previous version of itself
	//  	on restart, if time between restarts is less than heartbeat expiry time).
	uuid := dbUUID

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
	var err error
	if feedType == cbgtFeedType_cbdatasource {
		// cbdatasource expects server URL in http format
		serverURLs, errConvertServerSpec := CouchbaseURIToHttpURL(bucket, spec.Server, nil)
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
		cfgSG,
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
		Cfg:     cfgSG,
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
	err = createCBGTIndex(c, dbName, bucket, spec, numPartitions)
	if err != nil {
		if strings.Contains(err.Error(), "an index with the same name already exists") {
			Infof(KeyCluster, "Duplicate cbgt index detected during index creation (concurrent creation), using existing")
		} else {
			Errorf("cbgt index creation failed: %v", err)
			return err
		}
	}

	return nil
}

// StopHeartbeatListener unregisters the listener from the heartbeater, and stops it.
func (c *CbgtContext) StopHeartbeatListener() {

	if c.heartbeatListener != nil {
		c.heartbeater.UnregisterListener(c.heartbeatListener.Name())
		c.heartbeatListener.Stop()
	}
}

func initCfgCB(bucket Bucket, spec BucketSpec) (*cbgt.CfgCB, error) {

	// cfg: Implementation of cbgt.Cfg interface.  Responsible for configuration management
	//      Sync Gateway uses bucket-based config management
	options := map[string]interface{}{
		"keyPrefix": SyncPrefix,
	}
	urls, errConvertServerSpec := CouchbaseURIToHttpURL(bucket, spec.Server, nil)
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

func registerHeartbeatListener(heartbeater Heartbeater, cbgtContext *CbgtContext) (*importHeartbeatListener, error) {

	if cbgtContext == nil || cbgtContext.Manager == nil || cbgtContext.Cfg == nil || heartbeater == nil {
		return nil, errors.New("Unable to register import heartbeat listener with nil manager, cfg or heartbeater")
	}

	// Register listener for import, uses cfg and manager to manage set of participating nodes
	importHeartbeatListener, err := NewImportHeartbeatListener(cbgtContext.Cfg, cbgtContext.Manager)
	if err != nil {
		return nil, err
	}

	err = heartbeater.RegisterListener(importHeartbeatListener)
	if err != nil {
		return nil, err
	}

	return importHeartbeatListener, nil
}

// ImportHeartbeatListener uses cbgt's cfg to manage node list
type importHeartbeatListener struct {
	cfg        cbgt.Cfg      // cbgt cfg being used for import
	mgr        *cbgt.Manager // cbgt manager associated with this import node
	terminator chan struct{} // close cfg subscription on close
	nodeIDs    []string      // Set of nodes from the latest retrieval
	lock       sync.RWMutex  // lock for nodeIDs access
}

func NewImportHeartbeatListener(cfg cbgt.Cfg, mgr *cbgt.Manager) (*importHeartbeatListener, error) {

	if cfg == nil {
		return nil, errors.New("Cfg must not be nil for ImportHeartbeatListener")
	}

	listener := &importHeartbeatListener{
		cfg:        cfg,
		mgr:        mgr,
		terminator: make(chan struct{}),
	}

	// Initialize the node set
	_, err := listener.reloadNodes()
	if err != nil {
		return nil, err
	}

	// Subscribe to changes to the known node set key
	err = listener.subscribeNodeChanges()
	if err != nil {
		return nil, err
	}

	return listener, nil
}

func (l *importHeartbeatListener) Name() string {
	return "importListener"
}

// When we detect other nodes have stopped pushing heartbeats, use manager to remove from cfg
func (l *importHeartbeatListener) StaleHeartbeatDetected(nodeUUID string) {

	Infof(KeyCluster, "StaleHeartbeatDetected by import listener for node: %v", nodeUUID)
	err := cbgt.UnregisterNodes(l.cfg, l.mgr.Version(), []string{nodeUUID})
	if err != nil {
		Warnf("Attempt to unregister %v from CBGT got error: %v", nodeUUID, err)
	}
}

// subscribeNodeChanges registers with the manager's cfg implementation for notifications on changes to the
// NODE_DEFS_KNOWN key.  When notified, refreshes the handlers nodeIDs.
func (l *importHeartbeatListener) subscribeNodeChanges() error {

	cfgEvents := make(chan cbgt.CfgEvent)
	err := l.cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_KNOWN), cfgEvents)
	if err != nil {
		Debugf(KeyCluster, "Error subscribing NODE_DEFS_KNOWN changes: %v", err)
		return err
	}
	err = l.cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_WANTED), cfgEvents)
	if err != nil {
		Debugf(KeyCluster, "Error subscribing NODE_DEFS_WANTED changes: %v", err)
		return err
	}
	go func() {
		defer FatalPanicHandler()
		for {
			select {
			case <-cfgEvents:
				localNodeRegistered, err := l.reloadNodes()
				if err != nil {
					Warnf("Error while reloading heartbeat node definitions: %v", err)
				}
				if !localNodeRegistered {
					registerErr := l.mgr.Register(cbgt.NODE_DEFS_WANTED)
					if registerErr != nil {
						Warnf("Error attempting to re-register node, node will not participate in import until restarted or cbgt cfg is next updated: %v", registerErr)
					}
				}

			case <-l.terminator:
				return
			}
		}
	}()
	return nil
}

func (l *importHeartbeatListener) reloadNodes() (localNodePresent bool, err error) {

	nodeUUIDs := make([]string, 0)
	nodeDefTypes := []string{cbgt.NODE_DEFS_KNOWN, cbgt.NODE_DEFS_WANTED}
	for _, nodeDefType := range nodeDefTypes {
		nodeSet, _, err := cbgt.CfgGetNodeDefs(l.cfg, nodeDefType)
		if err != nil {
			return false, err
		}
		if nodeSet != nil {
			for _, nodeDef := range nodeSet.NodeDefs {
				nodeUUIDs = append(nodeUUIDs, nodeDef.UUID)
				if nodeDef.UUID == l.mgr.UUID() {
					localNodePresent = true
				}
			}
		}
	}
	l.lock.Lock()
	l.nodeIDs = nodeUUIDs
	l.lock.Unlock()

	return localNodePresent, nil
}

// GetNodes returns a copy of the in-memory node set
func (l *importHeartbeatListener) GetNodes() ([]string, error) {

	l.lock.RLock()
	nodeIDsCopy := make([]string, len(l.nodeIDs))
	copy(nodeIDsCopy, l.nodeIDs)
	l.lock.RUnlock()
	return nodeIDsCopy, nil
}

func (l *importHeartbeatListener) Stop() {
	close(l.terminator)
}
