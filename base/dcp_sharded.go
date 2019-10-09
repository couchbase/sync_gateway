package base

import (
	"log"
	"os"
	"strings"

	"github.com/couchbase/cbgt"
	"github.com/pkg/errors"
)

const CBGTIndexTypeSyncGatewayImport = "syncGateway-import"

// The two "handles" we have for CBGT are the manager and Cfg objects.
// This struct makes it easy to pass them around together as a unit.
type CbgtContext struct {
	Manager *cbgt.Manager
	Cfg     cbgt.Cfg
}

func StartShardedDCPFeed(dbName string, bucket Bucket, spec BucketSpec) (*CbgtContext, error) {

	cbgtContext, err := initCBGTManager(dbName, bucket, spec)
	if err != nil {
		return nil, err
	}

	//TODO: start heartbeater

	cbgtContext.StartManager(dbName, bucket, spec)

	return cbgtContext, nil

}

// cbgtFeedParams builds feed params for dest-based feed.  cbgt expects marshalled json, as string.
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

// Create database-specific instance of CBGT Index.
func createCBGTIndex(manager *cbgt.Manager, dbName string, bucket Bucket, spec BucketSpec) error {

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

	// TODO: Review uniqueness requirements for index definition, to determine if it should include 'import'
	indexName := dbName + "_import"

	// IndexParams are metadata that get passed back to us when ImportPIndex is created.  ImportPIndex is server context
	// scoped, so only needs dbName, but needs to be JSON.

	// TODO: Turn into proper struct
	indexParams := `{"name": "` + dbName + `"}`

	// TODO: MaxPartitionsPerPIndex could be configurable
	// TODO: what if this is inconsistent between SG nodes?
	planParams := cbgt.PlanParams{
		MaxPartitionsPerPIndex: 16, // num vbuckets per Pindex.  Multiple Pindexes could be assigned per node.
		NumReplicas:            0,  // No replicas required for SG sharded feed
	}

	// TODO: The manager
	exists, previousIndexUUID, err := getCBGTIndexUUID(manager, indexName)
	if exists {
		log.Printf("Hey, this index already exists.  Another node must have registered it with the same cfgCB")
	}

	err = manager.CreateIndex(
		sourceType,                     // sourceType
		bucket.GetName(),               // sourceName
		bucketUUID,                     // sourceUUID
		sourceParams,                   // sourceParams
		CBGTIndexTypeSyncGatewayImport, // indexType
		indexName,                      // indexName
		indexParams,                    // indexParams
		planParams,                     // planParams
		previousIndexUUID,              // prevIndexUUID
	)
	return err

}

// Check if this CBGT index already exists.
// TODO: The manager goes and looks up the index from the config.
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

	// uuid: Unique identifier for the node. Used to identify the node in the config
	// TODO: review whether SG actually needs UUID state across restarts, in order
	//       to minimize config churn
	uuid := cbgt.NewUUID()

	// cfg: Implementation of cbgt.Cfg interface.  Responsible for configuration management
	//      Sync Gateway uses bucket-based config management

	// TODO: Replace CfgMem with CfgCB
	//cfgCB := cbgt.NewCfgMem()

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
		log.Printf("Hey there's an error initializing the cfgCB: %v", err)
		return nil, err
	}

	// tags: Every node participating in sharded DCP has feed, janitor, pindex and planner roles
	//   	"feed": runs dcp feed
	//   	"janitor": maintains feed instances, attempts to match to plan
	//   	"pindex": handles DCP events
	//   	"planner": assigns partitions to nodes
	// TODO: Every participating node acts as planner, which means every node is going to be trying to
	//       reassign partitions (vbuckets) to nodes when a cfg change is detected.  cbft doesn't do this -
	//       it leverages ns-server to identify a master node.
	//       Assumption is that cbgt handles multi-planner collisions gracefully, and that SG
	//       doesn't need to set up master election.  Could revisit in future, as this functionality is
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
	serverURL, err := spec.GetGoCBConnString()
	if err != nil {
		return nil, err
	}

	// dataDir: file system location for files persisted by cbgt.  Needs to be unique
	// per database

	dataDir := os.TempDir() + "sg_" + dbName
	// TODO: Would really not have to persist here
	// Create the db-scoped path
	err = os.MkdirAll(dataDir, 0700)
	if err != nil {
		return nil, err
	}

	// eventHandlers: SG doesn't currently do any processing on manager events:
	//   - OnRegisterPIndex
	//   - OnUnregisterPIndex
	//   - OnFeedError
	var eventHandlers cbgt.ManagerEventHandlers

	// Creates a new cbgt manager.  NewManagerEx is available if we need to specify advanced options,
	// but isn't needed today.
	mgr := cbgt.NewManager(
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
		eventHandlers)

	cbgtContext := &CbgtContext{
		Manager: mgr,
		Cfg:     cfgCB,
	}
	return cbgtContext, nil
}

func (c *CbgtContext) StartManager(dbName string, bucket Bucket, spec BucketSpec) (err error) {
	// TODO: always wanted?
	// Start the manager.  Will retrieve config, and janitor will start feeds on this node.

	registerType := "wanted"
	if err := c.Manager.Start(registerType); err != nil {
		log.Printf("Manager start failed with error: %v", err)
		return err
	}
	// Add the index definition for this feed to the cbgt cfg, in case it's not already present.
	err = createCBGTIndex(c.Manager, dbName, bucket, spec)
	if err != nil {
		return err
	}
	c.Manager.Kick("NewIndexesCreated")
	return nil
}
