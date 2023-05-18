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
	"crypto/sha256"
	"crypto/tls"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/pkg/errors"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

const CBGTIndexTypeSyncGatewayImport = "syncGateway-import-"
const DefaultImportPartitions = 16
const DefaultImportPartitionsServerless = 6

// firstVersionToSupportCollections represents the earliest Sync Gateway release that supports collections.
var firstVersionToSupportCollections = &ComparableVersion{
	epoch: 0,
	major: 3,
	minor: 1,
	patch: 0,
}

// nodeExtras is the contents of the JSON value of the cbgt.NodeDef.Extras field as used by Sync Gateway.
type nodeExtras struct {
	// Version is the node's version.
	Version *ComparableVersion `json:"v"`
}

// CbgtContext holds the two handles we have for CBGT-related functionality.
type CbgtContext struct {
	Manager           *cbgt.Manager            // Manager is main entry point for initialization, registering indexes
	Cfg               cbgt.Cfg                 // Cfg manages storage of the current pindex set and node assignment
	heartbeater       Heartbeater              // Heartbeater used for failed node detection
	heartbeatListener *importHeartbeatListener // Listener subscribed to failed node alerts from heartbeater
	eventHandlers     *sgMgrEventHandlers      // Event handler callbacks
	ctx               context.Context          // Log context
	dbName            string                   // Database name
}

// StartShardedDCPFeed initializes and starts a CBGT Manager targeting the provided bucket.
// dbName is used to define a unique path name for local file storage of pindex files
func StartShardedDCPFeed(ctx context.Context, dbName string, configGroup string, uuid string, heartbeater Heartbeater, bucket Bucket, spec BucketSpec, scope string, collections []string, numPartitions uint16, cfg cbgt.Cfg) (*CbgtContext, error) {
	// Ensure we don't try to start collections-enabled feed if there are any pre-collection SG nodes in the cluster.
	minVersion, err := getMinNodeVersion(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to get minimum node version in cluster: %w", err)
	}
	if minVersion.Less(firstVersionToSupportCollections) {
		// DefaultScope is allowed by older versions of CBGT as long as no collections are specified.
		if len(collections) > 0 {
			return nil, fmt.Errorf("cannot start DCP feed on non-default collection with legacy nodes present in the cluster")
		}
	}

	cbgtContext, err := initCBGTManager(ctx, bucket, spec, cfg, uuid, dbName)
	if err != nil {
		return nil, err
	}

	// Add logging info before passing ctx down
	ctx = CorrelationIDLogCtx(ctx, DCPImportFeedID)

	// Start Manager.  Registers this node in the cfg
	err = cbgtContext.StartManager(ctx, dbName, configGroup, bucket, spec, scope, collections, numPartitions)
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

// Given a dbName, generate a unique and length-constrained index name for CBGT to use as part of their DCP name.
func GenerateIndexName(dbName string) string {
	// Index names *must* start with a letter, so we'll prepend 'db' before the per-database checksum (which starts with '0x')
	// Don't use Crc32cHashString here becuase this is intentionally non zero padded to match
	// existing values.
	return fmt.Sprintf("db0x%x_index", Crc32cHash([]byte(dbName)))
}

// Given a dbName, generates a name based on the approach used prior to CBG-626.  Used for upgrade handling
func GenerateLegacyIndexName(dbName string) string {
	return dbName + "_import"
}

// Creates a CBGT index definition for the specified bucket.  This adds the index definition
// to the manager's cbgt cfg.  Nodes that have registered for this indexType with the manager via
// RegisterPIndexImplType (see importListener.RegisterImportPindexImpl)
// will receive PIndexImpl callbacks (New, Open) for assigned PIndex to initiate DCP processing.
func createCBGTIndex(ctx context.Context, c *CbgtContext, dbName string, configGroupID string, bucket Bucket, spec BucketSpec, scope string, collections []string, numPartitions uint16) error {
	sourceType := SOURCE_DCP_SG

	bucketUUID, err := bucket.UUID()
	if err != nil {
		return err
	}

	sourceParams, err := cbgtFeedParams(spec, scope, collections, dbName)
	if err != nil {
		return err
	}

	indexParams, err := cbgtIndexParams(ImportDestKey(dbName, scope, collections))
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

	// Required for initial pools request, before BucketDataSourceOptions kick in.
	// go-couchbase doesn't support handling x509 auth and root ca verification as separate concerns.
	if spec.Certpath != "" && spec.Keypath != "" {
		couchbase.SetCertFile(spec.Certpath)
		couchbase.SetKeyFile(spec.Keypath)
		couchbase.SetRootFile(spec.CACertPath)
		couchbase.SetSkipVerify(false)
	}

	connSpec, err := gocbconnstr.Parse(spec.Server)
	if err != nil {
		return err
	}

	// Determine index name and UUID
	indexName, previousIndexUUID := dcpSafeIndexName(ctx, c, dbName)
	InfofCtx(ctx, KeyDCP, "Creating cbgt index %q for db %q", indexName, MD(dbName))

	// Register bucketDataSource callback for new index if we need to configure TLS
	cbgt.RegisterBucketDataSourceOptionsCallback(indexName, c.Manager.UUID(), func(options *cbdatasource.BucketDataSourceOptions) *cbdatasource.BucketDataSourceOptions {
		if spec.IsTLS() {
			options.TLSConfig = func() *tls.Config {
				return spec.TLSConfig()
			}
		}

		networkType := getNetworkTypeFromConnSpec(connSpec)
		InfofCtx(ctx, KeyDCP, "Using network type: %s", networkType)

		// default (aka internal) networking is handled by cbdatasource, so we can avoid the shims altogether in this case, for all other cases we need shims to remap hosts.
		if networkType != clusterNetworkDefault {
			// A lookup of host dest to external alternate address hostnames
			options.ConnectBucket, options.Connect, options.ConnectTLS = alternateAddressShims(ctx, spec.IsTLS(), connSpec.Addresses, networkType)
		}

		return options
	})

	// Index types are namespaced by configGroupID to support delete and create of a database targeting the
	// same bucket in a config group
	indexType := CBGTIndexTypeSyncGatewayImport + configGroupID
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

	InfofCtx(ctx, KeyDCP, "Initialized sharded DCP feed %s with %d partitions.", indexName, numPartitions)
	return err

}

// dcpSafeIndexName returns an index name and previousIndexUUID to handle upgrade scenarios from the
// legacy index name format ("dbname_import") to the new length-safe format ("db[crc32]_index").
// Handles removal of legacy index definitions, except for the case where the legacy index is
// the only index defined, and the name is safe.  In that case, continue using legacy index name
// to avoid restarting the import processing from zero
func dcpSafeIndexName(ctx context.Context, c *CbgtContext, dbName string) (safeIndexName, previousUUID string) {

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
		_, deleteErr := c.Manager.DeleteIndexEx(legacyIndexName, "")
		if deleteErr != nil {
			WarnfCtx(ctx, "Error removing legacy import feed index: %v", deleteErr)
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
func initCBGTManager(ctx context.Context, bucket Bucket, spec BucketSpec, cfgSG cbgt.Cfg, dbUUID string, dbName string) (*CbgtContext, error) {
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

	// extras: Can be used to pass Sync Gateway specific node information to callbacks. Used to store the node version
	// from Helium (3.1.0) onwards, empty for older versions.
	extras, err := JSONMarshal(&nodeExtras{Version: ProductVersion})
	if err != nil {
		return nil, err
	}

	// bindHttp: Used for REST binding (not needed by Sync Gateway), but also as a unique identifier
	//           in some circumstances.  See below for additional information.
	// 		(from accel): use the uuid as the bindHttp so that we don't have to make the user
	// 		configure this, since we're just using for uniqueness and not for REST API
	// 		More info here:
	//   		https://github.com/couchbaselabs/cbgt/issues/1
	//   		https://github.com/couchbaselabs/cbgt/issues/25
	bindHttp := uuid
	defaultValues := &GoCBConnStringParams{
		KVPoolSize:    GoCBPoolSizeDCP,
		KVBufferSize:  spec.KvBufferSize,
		DCPBufferSize: spec.DcpBuffer,
	}

	serverURL, err := spec.GetGoCBConnString(defaultValues)
	if err != nil {
		return nil, err
	}

	// dataDir: file system location for files persisted by cbgt.  Not required by SG, setting to empty
	//   avoids file system usage, in conjunction with managerLoadDataDir=false in options.
	dataDir := ""

	eventHandlersCtx, eventHandlersCancel := context.WithCancel(ctx)
	eventHandlers := &sgMgrEventHandlers{ctx: eventHandlersCtx, ctxCancel: eventHandlersCancel}

	// Specify one feed per pindex
	options := make(map[string]string)
	options[cbgt.FeedAllotmentOption] = cbgt.FeedAllotmentOnePerPIndex
	options["managerLoadDataDir"] = "false"
	// TLS is controlled by the connection string.
	// cbgt uses this parameter to run in mixed mode - non-TLS for CCCP but TLS for memcached. Sync Gateway does not need to set this parameter.
	options["feedInitialBootstrapNonTLS"] = "false"

	// Disable collections if unsupported
	if !bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		options["disableCollectionsSupport"] = "true"
		options["disableStreamIDs"] = "true"
	}

	// Creates a new cbgt manager.
	mgr := cbgt.NewManagerEx(
		SGCbgtMetadataVersion, // cbgt metadata version, matching 3.0 clients
		cfgSG,
		uuid,
		tags,
		container,
		weight,
		string(extras),
		bindHttp,
		dataDir,
		serverURL,
		eventHandlers,
		options)

	cbgtContext := &CbgtContext{
		Manager:       mgr,
		Cfg:           cfgSG,
		eventHandlers: eventHandlers,
		ctx:           ctx,
		dbName:        dbName,
	}

	if spec.Auth != nil || (spec.Certpath != "" && spec.Keypath != "") {
		username, password, _ := spec.Auth.GetCredentials()
		addCbgtCredentials(dbName, bucket.GetName(), username, password, spec.Certpath, spec.Keypath)
	}

	if spec.IsTLS() {
		bucketUUID, err := bucket.UUID()
		if err != nil {
			return nil, fmt.Errorf("failed to fetch UUID of bucket %v: %w", MD(bucket.GetName()).Redact(), err)
		}
		if spec.TLSSkipVerify {
			setCbgtRootCertsForBucket(bucketUUID, nil)
		} else {
			certs, err := getRootCAs(spec.CACertPath)
			if err != nil {
				return nil, fmt.Errorf("failed to load root CAs: %w", err)
			}
			setCbgtRootCertsForBucket(bucketUUID, certs)
		}
	}

	return cbgtContext, nil
}

// StartManager registers this node with cbgt, and the janitor will start feeds on this node.
func (c *CbgtContext) StartManager(ctx context.Context, dbName string, configGroup string, bucket Bucket, spec BucketSpec, scope string, collections []string, numPartitions uint16) (err error) {
	// TODO: Clarify the functional difference between registering the manager as 'wanted' vs 'known'.
	registerType := cbgt.NODE_DEFS_WANTED
	if err := c.Manager.Start(registerType); err != nil {
		ErrorfCtx(ctx, "cbgt Manager start failed: %v", err)
		return err
	}

	// Add the index definition for this feed to the cbgt cfg, in case it's not already present.
	err = createCBGTIndex(ctx, c, dbName, configGroup, bucket, spec, scope, collections, numPartitions)
	if err != nil {
		if strings.Contains(err.Error(), "an index with the same name already exists") {
			InfofCtx(ctx, KeyCluster, "Duplicate cbgt index detected during index creation (concurrent creation), using existing")
		} else if strings.Contains(err.Error(), "concurrent index definition update") {
			InfofCtx(ctx, KeyCluster, "Index update failed due to concurrent update, using existing")
		} else {
			ErrorfCtx(ctx, "cbgt index creation failed: %v", err)
			return err
		}
	}

	return nil
}

// getNodeVersion returns the version of the node from its Extras field, or nil if none is stored. Returns an error if
// the extras could not be parsed.
func getNodeVersion(def *cbgt.NodeDef) (*ComparableVersion, error) {
	if len(def.Extras) == 0 {
		return nil, nil
	}
	var extras nodeExtras
	if err := JSONUnmarshal([]byte(def.Extras), &extras); err != nil {
		return nil, fmt.Errorf("parsing node extras: %w", err)
	}
	return extras.Version, nil
}

// getMinNodeVersion returns the version of the oldest node currently in the cluster.
func getMinNodeVersion(cfg cbgt.Cfg) (*ComparableVersion, error) {
	nodes, _, err := cbgt.CfgGetNodeDefs(cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		return nil, err
	}
	if nodes == nil || len(nodes.NodeDefs) == 0 {
		// If there are no nodes at all, it's likely we're the first node in the cluster.
		return ProductVersion, nil
	}
	var minVersion *ComparableVersion
	for _, node := range nodes.NodeDefs {
		nodeVersion, err := getNodeVersion(node)
		if err != nil {
			return nil, fmt.Errorf("failed to get version of node %v: %w", MD(node.HostPort).Redact(), err)
		}
		if nodeVersion == nil {
			nodeVersion = zeroComparableVersion
		}
		if minVersion == nil || nodeVersion.Less(minVersion) {
			minVersion = nodeVersion
		}
	}
	return minVersion, nil
}

// Stop unregisters the listener from the heartbeater, and stops it and associated handlers.
func (c *CbgtContext) Stop() {
	if c.eventHandlers != nil {
		c.eventHandlers.ctxCancel()
	}

	if c.heartbeatListener != nil {
		c.heartbeater.UnregisterListener(c.heartbeatListener.Name())
		c.heartbeatListener.Stop()
	}

	// Close open PIndexes before stopping the manager.
	_, pindexes := c.Manager.CurrentMaps()
	for _, pIndex := range pindexes {
		err := c.Manager.ClosePIndex(pIndex)
		if err != nil {
			DebugfCtx(c.ctx, KeyImport, "Error closing pindex: %v", err)
		}
	}
	// ClosePIndex calls are synchronous, so can stop manager once they've completed
	c.Manager.Stop()
	c.RemoveFeedCredentials(c.dbName)
}

func (c *CbgtContext) RemoveFeedCredentials(dbName string) {
	removeCbgtCredentials(dbName)
}

// Format of dest key for retrieval of import dest from cbgtDestFactories
func ImportDestKey(dbName string, scope string, collections []string) string {
	sort.Strings(collections)
	collectionString := ""
	onlyDefault := true
	for _, collection := range collections {
		if collection != DefaultCollection {
			onlyDefault = false
		}
		collectionString += fmt.Sprintf("%s.%s:", scope, collection)
	}
	// format for _default._default
	if collectionString == "" || (scope == DefaultScope && onlyDefault) {
		return fmt.Sprintf("%s_import", dbName)
	}
	return fmt.Sprintf("%s_import_%x", dbName, sha256.Sum256([]byte(collectionString)))
}

func initCfgCB(bucket Bucket, spec BucketSpec) (*cbgt.CfgCB, error) {

	// cfg: Implementation of cbgt.Cfg interface.  Responsible for configuration management
	//      Sync Gateway uses bucket-based config management
	options := map[string]interface{}{
		"keyPrefix": SyncDocPrefix,
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
	importHeartbeatListener, err := NewImportHeartbeatListener(cbgtContext)
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
	ctx        *CbgtContext
	terminator chan struct{} // close cfg subscription on close
	nodeIDs    []string      // Set of nodes from the latest retrieval
	lock       sync.RWMutex  // lock for nodeIDs access
}

func NewImportHeartbeatListener(ctx *CbgtContext) (*importHeartbeatListener, error) {

	if ctx == nil {
		return nil, errors.New("ctx must not be nil for ImportHeartbeatListener")
	}

	listener := &importHeartbeatListener{
		ctx:        ctx,
		mgr:        ctx.Manager,
		cfg:        ctx.Cfg,
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

	InfofCtx(context.TODO(), KeyCluster, "StaleHeartbeatDetected by import listener for node: %v", nodeUUID)
	err := cbgt.UnregisterNodes(l.cfg, l.mgr.Version(), []string{nodeUUID})
	if err != nil {
		WarnfCtx(context.TODO(), "Attempt to unregister %v from CBGT got error: %v", nodeUUID, err)
	}
}

// subscribeNodeChanges registers with the manager's cfg implementation for notifications on changes to the
// NODE_DEFS_KNOWN key.  When notified, refreshes the handlers nodeIDs.
func (l *importHeartbeatListener) subscribeNodeChanges() error {
	logCtx := context.TODO()

	cfgEvents := make(chan cbgt.CfgEvent)
	err := l.cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_KNOWN), cfgEvents)
	if err != nil {
		DebugfCtx(logCtx, KeyCluster, "Error subscribing NODE_DEFS_KNOWN changes: %v", err)
		return err
	}
	err = l.cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_WANTED), cfgEvents)
	if err != nil {
		DebugfCtx(logCtx, KeyCluster, "Error subscribing NODE_DEFS_WANTED changes: %v", err)
		return err
	}
	go func() {
		defer FatalPanicHandler()
		for {
			select {
			case <-cfgEvents:
				localNodeRegistered, err := l.reloadNodes()
				if err != nil {
					WarnfCtx(logCtx, "Error while reloading heartbeat node definitions: %v", err)
				}
				if !localNodeRegistered {
					registerErr := l.mgr.Register(cbgt.NODE_DEFS_WANTED)
					if registerErr != nil {
						WarnfCtx(logCtx, "Error attempting to re-register node, node will not participate in import until restarted or cbgt cfg is next updated: %v", registerErr)
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

// cbgtDestFactories map DCP feed keys (destKey) to a function that will generate cbgt.Dest.  Need to be stored in a
// global map to avoid races between db creation and db addition to the server context database set

type CbgtDestFactoryFunc = func() (cbgt.Dest, error)

var cbgtDestFactories = make(map[string]CbgtDestFactoryFunc)
var cbgtDestFactoriesLock sync.Mutex

func StoreDestFactory(ctx context.Context, destKey string, dest CbgtDestFactoryFunc) {
	cbgtDestFactoriesLock.Lock()
	_, ok := cbgtDestFactories[destKey]

	// We don't expect duplicate destKey registration - log a warning if it already exists
	if ok {
		WarnfCtx(ctx, "destKey %s already exists in cbgtDestFactories - new value will replace the existing dest", destKey)
	}
	cbgtDestFactories[destKey] = dest
	cbgtDestFactoriesLock.Unlock()
}

func FetchDestFactory(destKey string) (CbgtDestFactoryFunc, error) {
	cbgtDestFactoriesLock.Lock()
	defer cbgtDestFactoriesLock.Unlock()
	listener, ok := cbgtDestFactories[destKey]
	if !ok {
		return nil, ErrNotFound
	}
	return listener, nil
}

func RemoveDestFactory(destKey string) {
	cbgtDestFactoriesLock.Lock()
	delete(cbgtDestFactories, destKey)
	cbgtDestFactoriesLock.Unlock()
}

func GetDefaultImportPartitions(serverless bool) uint16 {
	if serverless {
		return DefaultImportPartitionsServerless
	} else {
		return DefaultImportPartitions
	}
}

type sgMgrEventHandlers struct {
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func (meh *sgMgrEventHandlers) OnRefreshManagerOptions(options map[string]string) {
	// No-op for SG
}

func (meh *sgMgrEventHandlers) OnRegisterPIndex(pindex *cbgt.PIndex) {
	// No-op for SG
}

func (meh *sgMgrEventHandlers) OnUnregisterPIndex(pindex *cbgt.PIndex) {
	// No-op for SG
}

// OnFeedError is required to trigger reconnection to a feed on an closed connection (EOF).
// Handling below based on cbft implementation - checks whether the underlying source (bucket)
// still exists with VerifySourceNotExists, and if it exists, calls NotifyMgrOnClose.
// This will trigger cbgt closing and then attempting to reconnect to the feed.
func (meh *sgMgrEventHandlers) OnFeedError(srcType string, r cbgt.Feed, feedErr error) {

	DebugfCtx(meh.ctx, KeyDCP, "cbgt Mgr OnFeedError, srcType: %s, feed name: %s, err: %v",
		srcType, r.Name(), feedErr)

	dcpFeed, ok := r.(cbgt.FeedEx)
	if !ok {
		return
	}

	gone, indexUUID, err := dcpFeed.VerifySourceNotExists()
	DebugfCtx(meh.ctx, KeyDCP, "cbgt Mgr OnFeedError, VerifySourceNotExists,"+
		" srcType: %s, gone: %t, indexUUID: %s, err: %v",
		srcType, gone, indexUUID, err)
	if !gone {
		// If we get an EOF error from the feeds and the bucket is still alive,
		// then there could at the least two potential error scenarios.
		//
		// 1. Faulty kv node is failed over.
		// 2. Ephemeral network connection issues with the host.
		//
		// In either case, the current feed instance turns dangling.
		// Hence we can close the feeds so that they get refreshed to fix
		// the connectivity problems either during the next rebalance
		// (new kv node after failover-recovery rebalance) or
		// on the next janitor work cycle(ephemeral network issue to the same node).
		if strings.Contains(feedErr.Error(), "EOF") {
			// If this wasn't an intentional close, log about the EOF
			if meh.ctx.Err() != context.Canceled {
				InfofCtx(meh.ctx, KeyDCP, "Handling EOF on cbgt feed - notifying manager to trigger reconnection to feed.  indexUUID: %v, err: %v", indexUUID, feedErr)
			}
			dcpFeed.NotifyMgrOnClose()
		}
	}

}
