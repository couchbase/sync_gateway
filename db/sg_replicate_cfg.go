package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

const (
	cfgKeySGRCluster        = "sgrCluster" // key used for sgrCluster information in a cbgt.Cfg-based key value store
	maxSGRClusterCasRetries = 100          // Maximum number of CAS retries when attempting to update the sgr cluster configuration
	sgrClusterMgrContextID  = "sgr-mgr-"   // logging context ID prefix for sgreplicate manager
	defaultChangesBatchSize = 200          // default changes batch size if replication batch_size is unset
)

const (
	ReplicationStateStopped   = "stopped"
	ReplicationStateRunning   = "running"
	ReplicationStateResetting = "resetting"
	ReplicationStateError     = "error"
)

// ClusterUpdateFunc is callback signature used when updating the cluster configuration
type ClusterUpdateFunc func(cluster *SGRCluster) (cancel bool, err error)

// SGRCluster defines sg-replicate configuration and distribution for a collection of Sync Gateway nodes
type SGRCluster struct {
	Replications map[string]*ReplicationCfg `json:"replications"` // Set of replications defined for the cluster, indexed by replicationID
	Nodes        map[string]*SGNode         `json:"nodes"`        // Set of nodes, indexed by host name
	loggingCtx   context.Context            // logging context for cluster operations
}

func NewSGRCluster() *SGRCluster {
	return &SGRCluster{
		Replications: make(map[string]*ReplicationCfg),
		Nodes:        make(map[string]*SGNode),
	}
}

// SGNode represents a single Sync Gateway node in the cluster
type SGNode struct {
	UUID string `json:"uuid"` // Node UUID
	Host string `json:"host"` // Host name
}

func NewSGNode(uuid string, host string) *SGNode {
	return &SGNode{
		UUID: uuid,
		Host: host,
	}
}

// ReplicationConfig is a replication definition as stored in the Sync Gateway config
type ReplicationConfig struct {
	ID                     string                    `json:"replication_id"`
	Remote                 string                    `json:"remote"`
	Direction              ActiveReplicatorDirection `json:"direction"`
	ConflictResolutionType ConflictResolverType      `json:"conflict_resolution_type,omitempty"`
	ConflictResolutionFn   string                    `json:"custom_conflict_resolver,omitempty"`
	PurgeOnRemoval         bool                      `json:"purge_on_removal,omitempty"`
	DeltaSyncEnabled       bool                      `json:"enable_delta_sync,omitempty"`
	MaxBackoff             int                       `json:"max_backoff_time,omitempty"`
	State                  string                    `json:"state,omitempty"`
	Continuous             bool                      `json:"continuous"`
	Filter                 string                    `json:"filter,omitempty"`
	QueryParams            interface{}               `json:"query_params,omitempty"`
	Cancel                 bool                      `json:"cancel,omitempty"`
	Adhoc                  bool                      `json:"adhoc,omitempty"`
	BatchSize              int                       `json:"batch_size,omitempty"`
}

func DefaultReplicationConfig() ReplicationConfig {
	return ReplicationConfig{
		DeltaSyncEnabled:       false,
		PurgeOnRemoval:         false,
		MaxBackoff:             5,
		ConflictResolutionType: ConflictResolverDefault,
		State:                  ReplicationStateRunning,
		Continuous:             false,
		Adhoc:                  false,
		BatchSize:              defaultChangesBatchSize,
	}
}

// ReplicationCfg represents a replication definition as stored in the cluster config.
type ReplicationCfg struct {
	ReplicationConfig
	AssignedNode string `json:"assigned_node"` // UUID of node assigned to this replication
}

// ReplicationUpsertConfig is used for operations that support upsert of a subset of replication properties.
type ReplicationUpsertConfig struct {
	ID                     string      `json:"replication_id"`
	Remote                 *string     `json:"remote"`
	Direction              *string     `json:"direction"`
	ConflictResolutionType *string     `json:"conflict_resolution_type,omitempty"`
	ConflictResolutionFn   *string     `json:"custom_conflict_resolver,omitempty"`
	PurgeOnRemoval         *bool       `json:"purge_on_removal,omitempty"`
	DeltaSyncEnabled       *bool       `json:"enable_delta_sync,omitempty"`
	MaxBackoff             *int        `json:"max_backoff_time,omitempty"`
	State                  *string     `json:"state,omitempty"`
	Continuous             *bool       `json:"continuous"`
	Filter                 *string     `json:"filter,omitempty"`
	QueryParams            interface{} `json:"query_params,omitempty"`
	Cancel                 *bool       `json:"cancel,omitempty"`
	Adhoc                  *bool       `json:"adhoc,omitempty"`
	BatchSize              *int        `json:"batch_size,omitempty"`
}

func (rc *ReplicationConfig) ValidateReplication(fromConfig bool) (err error) {

	// Cancel is only supported via the REST API
	if rc.Cancel {
		if fromConfig {
			err = base.HTTPErrorf(http.StatusBadRequest, "cancel=true is invalid for replication in Sync Gateway configuration")
			return
		}
	}

	// Adhoc is only supported via the REST API
	if rc.Adhoc {
		if fromConfig {
			err = base.HTTPErrorf(http.StatusBadRequest, "adhoc=true is invalid for replication in Sync Gateway configuration")
			return
		}
	}

	if rc.Remote == "" {
		err = base.HTTPErrorf(http.StatusBadRequest, "Replication remote must be specified.")
		return err
	}

	if rc.Direction == "" {
		err = base.HTTPErrorf(http.StatusBadRequest, "Replication direction must be specified")
		return err
	}

	remoteURL, err := url.Parse(rc.Remote)
	if err != nil {
		err = base.HTTPErrorf(http.StatusBadRequest, "Replication remote URL [%s] is invalid: %v", remoteURL, err)
		return err
	}

	if rc.Filter == base.ByChannelFilter {
		if rc.QueryParams == nil {
			err = base.HTTPErrorf(http.StatusBadRequest, "Replication specifies sync_gateway/bychannel filter but is missing query_params")
			return
		}

		//The Channels may be passed as a JSON array of strings directly
		//or embedded in a JSON object with the "channels" property and array value
		var chanarray []interface{}

		if paramsmap, ok := rc.QueryParams.(map[string]interface{}); ok {
			if chanarray, ok = paramsmap["channels"].([]interface{}); !ok {
				err = base.HTTPErrorf(http.StatusBadRequest, "Replication specifies sync_gateway/bychannel filter but is missing channels property in query_params")
				return
			}
		} else if chanarray, ok = rc.QueryParams.([]interface{}); ok {
			// query params is an array and chanarray has been set, now drop out of if-then-else for processing
		} else {
			err = base.HTTPErrorf(http.StatusBadRequest, "Bad channels array in query_params for sync_gateway/bychannel filter.")
			return
		}
		if len(chanarray) > 0 {
			channels := make([]string, len(chanarray))
			for i := range chanarray {
				if channel, ok := chanarray[i].(string); ok {
					channels[i] = channel
				} else {
					err = base.HTTPErrorf(http.StatusBadRequest, "Bad channel name in query_params for sync_gateway/bychannel filter")
					return
				}
			}
		}
	} else if rc.Filter != "" {
		err = base.HTTPErrorf(http.StatusBadRequest, "Unknown replication filter; try sync_gateway/bychannel")
		return
	}
	return nil
}

// Upsert updates ReplicationConfig with any non-empty properties specified in the incoming replication config.
// Note that if the intention is to reset the value to default, empty values must be specified.
func (rc *ReplicationConfig) Upsert(c *ReplicationUpsertConfig) {

	if c.Remote != nil {
		rc.Remote = *c.Remote
	}

	if c.Direction != nil {
		rc.Direction = ActiveReplicatorDirection(*c.Direction)
	}

	if c.ConflictResolutionType != nil {
		rc.ConflictResolutionType = ConflictResolverType(*c.ConflictResolutionType)
	}
	if c.ConflictResolutionFn != nil {
		rc.ConflictResolutionFn = *c.ConflictResolutionFn
	}
	if c.PurgeOnRemoval != nil {
		rc.PurgeOnRemoval = *c.PurgeOnRemoval
	}

	if c.DeltaSyncEnabled != nil {
		rc.DeltaSyncEnabled = *c.DeltaSyncEnabled
	}
	if c.MaxBackoff != nil {
		rc.MaxBackoff = *c.MaxBackoff
	}
	if c.State != nil {
		rc.State = *c.State
	}
	if c.Continuous != nil {
		rc.Continuous = *c.Continuous
	}
	if c.Filter != nil {
		rc.Filter = *c.Filter
	}
	if c.Cancel != nil {
		rc.Cancel = *c.Cancel
	}
	if c.Adhoc != nil {
		rc.Adhoc = *c.Adhoc
	}
	if c.QueryParams != nil {
		rc.QueryParams = c.QueryParams
	}

	if c.BatchSize != nil {
		rc.BatchSize = *c.BatchSize
	}

	if c.QueryParams != nil {
		// QueryParams can be either []interface{} or map[string]interface{}, so requires type-specific copying
		// avoid later mutating c.QueryParams
		switch qp := c.QueryParams.(type) {
		case []interface{}:
			newParams := make([]interface{}, len(qp))
			copy(newParams, qp)
			rc.QueryParams = newParams
		case map[string]interface{}:
			newParamMap := make(map[string]interface{})
			for k, v := range qp {
				newParamMap[k] = v
			}
			rc.QueryParams = newParamMap
		default:
			// unsupported query params type, don't upsert
			base.Warnf("Unexpected QueryParams type found during upsert, will be ignored (%T): %v", c.QueryParams, c.QueryParams)
		}
	}
}

// Equals is doing a relatively expensive json-based equality computation, so shouldn't be used in performance-sensitive scenarios
func (rc *ReplicationConfig) Equals(compareToCfg *ReplicationConfig) (bool, error) {
	currentBytes, err := base.JSONMarshalCanonical(rc)
	if err != nil {
		return false, err
	}
	compareToBytes, err := base.JSONMarshalCanonical(compareToCfg)
	if err != nil {
		return false, err
	}
	return bytes.Equal(currentBytes, compareToBytes), nil
}

// sgReplicateManager should be used for all interactions with the stored cluster definition.
type sgReplicateManager struct {
	cfg                   cbgt.Cfg                      // Key-value store implementation
	loggingCtx            context.Context               // logging context for manager operations
	heartbeatListener     *ReplicationHeartbeatListener // node heartbeat listener for replication distribution
	localNodeUUID         string                        // nodeUUID for this SG node
	activeReplicators     map[string]*ActiveReplicator  // currently assigned replications
	activeReplicatorsLock sync.RWMutex                  // Mutex for activeReplications
	terminator            chan struct{}
	dbContext             *DatabaseContext
}

// alignState attempts to update the current replicator state to align with the provided targetState, if
// it's a valid state transition.
//  Valid:
//     stopped -> running
//     stopped -> resetting
//     resetting -> stopped
//     running -> stopped
func (ar *ActiveReplicator) alignState(targetState string) error {
	if ar == nil {
		return nil
	}

	currentState, _ := ar.State()
	if targetState == currentState {
		return nil
	}

	switch targetState {
	case ReplicationStateStopped:
		base.Infof(base.KeyReplicate, "Stopping replication %s - previous state %s", ar.ID, currentState)
		stopErr := ar.Stop()
		if stopErr != nil {
			return fmt.Errorf("Unable to gracefully stop active replicator for replication %s: %v", ar.ID, stopErr)
		}
	case ReplicationStateRunning:
		base.Infof(base.KeyReplicate, "Starting replication %s - previous state %s", ar.ID, currentState)
		startErr := ar.Start()
		if startErr != nil {
			return fmt.Errorf("Unable to start active replicator for replication %s: %v", ar.ID, startErr)
		}
	case ReplicationStateResetting:
		if currentState != ReplicationStateStopped {
			return fmt.Errorf("Replication must be stopped before it can be reset")
		}
		base.Infof(base.KeyReplicate, "Resetting replication %s - previous state %s", ar.ID, currentState)
		resetErr := ar.Reset()
		if resetErr != nil {
			return fmt.Errorf("Unable to reset active replicator for replication %s: %v", ar.ID, resetErr)
		}
	}
	return nil

}

func NewSGReplicateManager(dbContext *DatabaseContext, cfg *base.CfgSG) (*sgReplicateManager, error) {
	if cfg == nil {
		return nil, errors.New("Cfg must be provided for SGReplicateManager")
	}

	return &sgReplicateManager{
		cfg: cfg,
		loggingCtx: context.WithValue(context.Background(), base.LogContextKey{},
			base.LogContext{CorrelationID: sgrClusterMgrContextID + dbContext.Name}),
		terminator:        make(chan struct{}),
		dbContext:         dbContext,
		activeReplicators: make(map[string]*ActiveReplicator),
	}, nil

}

// StartLocalNode registers the local node into the replication config, and registers an
// sg-replicate heartbeat listener with the provided heartbeater
func (m *sgReplicateManager) StartLocalNode(nodeUUID string, heartbeater base.Heartbeater) error {

	hostName, hostErr := os.Hostname()
	if hostErr != nil {
		base.Infof(base.KeyCluster, "Unable to retrieve hostname, registering node for sgreplicate without host specified.  Error: %v", hostErr)
	}

	err := m.RegisterNode(nodeUUID, hostName)
	if err != nil {
		return err
	}

	m.localNodeUUID = nodeUUID

	m.heartbeatListener, err = NewReplicationHeartbeatListener(m)
	if err != nil {
		return err
	}

	base.Infof(base.KeyCluster, "Started local node for sg-replicate as %s", nodeUUID)
	return heartbeater.RegisterListener(m.heartbeatListener)
}

// StartReplications performs an initial retrieval of the cluster config, starts any replications
// assigned to this node, and starts the process to monitor future changes to the cluster config.
func (m *sgReplicateManager) StartReplications() error {

	replications, err := m.GetReplications()
	if err != nil {
		return err
	}
	for replicationID, replicationCfg := range replications {
		base.Debugf(base.KeyCluster, "Replication %s is assigned to node %s (local node is %s)", replicationID, replicationCfg.AssignedNode, m.localNodeUUID)
		if replicationCfg.AssignedNode == m.localNodeUUID {
			activeReplicator, err := m.InitializeReplication(replicationCfg)
			if err != nil {
				base.Warnf("Error initializing replication %s: %v", err)
				continue
			}
			m.activeReplicatorsLock.Lock()
			m.activeReplicators[replicationID] = activeReplicator
			m.activeReplicatorsLock.Unlock()
			if replicationCfg.State == "" || replicationCfg.State == ReplicationStateRunning {
				if startErr := activeReplicator.Start(); startErr != nil {
					base.Warnf("Unable to start replication %s: %v", replicationID, startErr)
				}
			}
		}
	}
	return m.SubscribeCfgChanges()
}

func (m *sgReplicateManager) InitializeReplication(config *ReplicationCfg) (replicator *ActiveReplicator, err error) {
	// TODO: the following properties on ActiveReplicator aren't currently supported in ReplicationCfg.  Some could be taken
	//    out until they are implemented?
	//    - docIDs  (future enhancement)
	//    - activeOnly (P1)
	//    - checkpointInterval (not externally configurable)

	base.Infof(base.KeyReplicate, "Initializing replication %v", config.ID)

	rc := &ActiveReplicatorConfig{
		ID:                 config.ID,
		Continuous:         config.Continuous,
		ActiveDB:           &Database{DatabaseContext: m.dbContext}, // sg-replicate interacts with local as admin
		PurgeOnRemoval:     config.PurgeOnRemoval,
		DeltasEnabled:      config.DeltaSyncEnabled,
		InsecureSkipVerify: m.dbContext.Options.UnsupportedOptions.SgrTlsSkipVerify,
	}

	rc.ChangesBatchSize = defaultChangesBatchSize
	if config.BatchSize > 0 {
		rc.ChangesBatchSize = uint16(config.BatchSize)
	}

	// TODO: Move all this config validation to the point of creation, so that
	//  InitializeReplication doesn't need to report errors based on user data
	if config.Filter == base.ByChannelFilter {
		rc.Filter = base.ByChannelFilter
		rc.FilterChannels, err = ChannelsFromQueryParams(config.QueryParams)
		if err != nil {
			return nil, err
		}
	} else if config.Filter != "" {
		return nil, fmt.Errorf("Unknown replication filter: %v", config.Filter)
	}

	rc.Direction = config.Direction
	if !rc.Direction.IsValid() {
		return nil, fmt.Errorf("Unknown replication direction: %v", config.Direction)
	}

	// Set conflict resolver for pull replications
	if rc.Direction == ActiveReplicatorTypePull || rc.Direction == ActiveReplicatorTypePushAndPull {
		if config.ConflictResolutionType == "" {
			rc.ConflictResolver, err = NewConflictResolverFunc(ConflictResolverDefault, "")
		} else {
			rc.ConflictResolver, err = NewConflictResolverFunc(config.ConflictResolutionType, config.ConflictResolutionFn)
		}
		if err != nil {
			return nil, err
		}
	}

	if config.Remote == "" {
		return nil, fmt.Errorf("Replication remote must not be empty")
	}

	rc.PassiveDBURL, err = url.Parse(config.Remote)
	if err != nil {
		return nil, err
	}

	rc.WebsocketPingInterval = m.dbContext.Options.SGReplicateOptions.WebsocketPingInterval

	rc.onComplete = m.replicationComplete

	// TODO: review whether there's a more appropriate context to use here
	replicator = NewActiveReplicator(rc)

	return replicator, nil
}

// replicationComplete updates the replication status.
func (m *sgReplicateManager) replicationComplete(replicationID string) {
	err := m.UpdateReplicationState(replicationID, ReplicationStateStopped)
	if err != nil {
		base.Warnf("Unable to update replication state to stopped on completion: %v", err)
	}
}

func (m *sgReplicateManager) Stop() {
	close(m.terminator)
}

// RefreshReplicationCfg is called when the cfg changes.  Checks whether replications
// have been added to or removed from this node
func (m *sgReplicateManager) RefreshReplicationCfg() error {

	base.Infof(base.KeyCluster, "Replication definitions changed - refreshing...")
	configReplications, err := m.GetReplications()
	if err != nil {
		return err
	}

	m.activeReplicatorsLock.Lock()
	defer m.activeReplicatorsLock.Unlock()

	// check for active replications that should be stopped
	for replicationID, activeReplicator := range m.activeReplicators {
		replicationCfg, ok := configReplications[replicationID]

		// Check for active replications removed from this node
		if !ok || replicationCfg.AssignedNode != m.localNodeUUID {
			base.Infof(base.KeyReplicate, "Stopping reassigned replication %s", replicationID)
			err := activeReplicator.Stop()
			if err != nil {
				base.Warnf("Unable to gracefully close active replication: %v", err)
			}
			delete(m.activeReplicators, replicationID)
		} else {
			// Check for replications assigned to this node with updated state
			base.Debugf(base.KeyReplicate, "Aligning state for existing replication %s", replicationID)
			stateErr := activeReplicator.alignState(replicationCfg.State)
			if stateErr != nil {
				base.Warnf("Error updating active replication %s to state %s", replicationID, replicationCfg.State)
			}
		}
	}

	// Check for replications newly assigned to this node
	for replicationID, replicationCfg := range configReplications {
		if replicationCfg.AssignedNode == m.localNodeUUID {
			replicator, exists := m.activeReplicators[replicationID]
			if !exists {
				var initError error
				replicator, initError = m.InitializeReplication(replicationCfg)
				if initError != nil {
					base.Warnf("Error initializing replication %s: %v", initError)
					continue
				}
				m.activeReplicators[replicationID] = replicator

				if replicationCfg.State == "" || replicationCfg.State == ReplicationStateRunning {
					base.Infof(base.KeyReplicate, "Starting newly assigned replication %s", replicationID)
					if startErr := replicator.Start(); startErr != nil {
						base.Warnf("Unable to start replication %s: %v", replicationID, startErr)
					}
				}
			}
		}
	}
	return nil
}

func (m *sgReplicateManager) SubscribeCfgChanges() error {
	cfgEvents := make(chan cbgt.CfgEvent)

	err := m.cfg.Subscribe(cfgKeySGRCluster, cfgEvents)
	if err != nil {
		base.Debugf(base.KeyCluster, "Error subscribing to %s key changes: %v", cfgKeySGRCluster, err)
		return err
	}
	go func() {
		defer base.FatalPanicHandler()
		for {
			select {
			case <-cfgEvents:
				err := m.RefreshReplicationCfg()
				if err != nil {
					base.Warnf("Error while updating replications based on latest cfg: %v", err)
				}
			case <-m.terminator:
				return
			}
		}
	}()
	return nil
}

// Loads the SGReplicate config from the config store
func (m *sgReplicateManager) loadSGRCluster() (sgrCluster *SGRCluster, cas uint64, err error) {
	var cfgBytes []byte
	cfgBytes, cas, err = m.cfg.Get(cfgKeySGRCluster, 0)
	if err != nil {
		return nil, 0, err
	}

	if cfgBytes == nil {
		sgrCluster = NewSGRCluster()
		cas = 0
	} else {
		sgrCluster = &SGRCluster{}
		err := base.JSONUnmarshal(cfgBytes, sgrCluster)
		if err != nil {
			return nil, 0, err
		}
	}
	sgrCluster.loggingCtx = m.loggingCtx
	return sgrCluster, cas, nil
}

// updateCluster manages CAS retry for SGRCluster updates.
func (m *sgReplicateManager) updateCluster(callback ClusterUpdateFunc) error {
	for i := 1; i <= maxSGRClusterCasRetries; i++ {
		sgrCluster, cas, err := m.loadSGRCluster()
		if err != nil {
			return err
		}
		cancel, callbackError := callback(sgrCluster)
		if callbackError != nil {
			return callbackError
		}
		if cancel {
			return nil
		}

		updatedBytes, err := base.JSONMarshal(sgrCluster)
		if err != nil {
			return err
		}

		_, err = m.cfg.Set(cfgKeySGRCluster, updatedBytes, cas)
		if err == base.ErrCfgCasError {
			base.DebugfCtx(m.loggingCtx, base.KeyReplicate, "CAS Retry updating sg-replicate cluster definition (%d/%d)", i, maxSGRClusterCasRetries)
		} else if err != nil {
			base.InfofCtx(m.loggingCtx, base.KeyReplicate, "Error persisting sg-replicate cluster definition - update cancelled: %v", err)
			return err
		} else {
			base.DebugfCtx(m.loggingCtx, base.KeyReplicate, "Successfully persisted sg-replicate cluster definition.")
			return nil
		}
	}
	return errors.New("CAS Retry count (%d) exceeded when attempting to update sg-replicate cluster configuration")
}

// Register node adds a node to the cluster config, then triggers replication rebalance.
// Retries on CAS failure, is no-op if node already exists in config
func (m *sgReplicateManager) RegisterNode(nodeUUID string, host string) error {
	registerNodeCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Nodes[nodeUUID]
		if exists {
			return true, nil
		}
		cluster.Nodes[nodeUUID] = NewSGNode(nodeUUID, host)
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(registerNodeCallback)
}

// RemoveNode removes a node from the cluster config, then triggers replication rebalance.
// Retries on CAS failure, is no-op if node doesn't exist in config
func (m *sgReplicateManager) RemoveNode(nodeUUID string) error {
	removeNodeCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Nodes[nodeUUID]
		if !exists {
			return true, nil
		}
		delete(cluster.Nodes, nodeUUID)
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(removeNodeCallback)
}

func (m *sgReplicateManager) getNodes() (nodes map[string]*SGNode, err error) {
	sgrCluster, _, err := m.loadSGRCluster()
	if err != nil {
		return nil, err
	}
	return sgrCluster.Nodes, nil
}

// GET _replication/{replicationID}
func (m *sgReplicateManager) GetReplication(replicationID string) (*ReplicationCfg, error) {
	sgrCluster, _, err := m.loadSGRCluster()
	if err != nil {
		return nil, err
	}
	replication, exists := sgrCluster.Replications[replicationID]
	if !exists {
		return nil, base.ErrNotFound
	}
	return replication, nil
}

// GET _replication
func (m *sgReplicateManager) GetReplications() (replications map[string]*ReplicationCfg, err error) {

	sgrCluster, _, err := m.loadSGRCluster()
	if err != nil {
		return nil, err
	}
	return sgrCluster.Replications, nil
}

// PUT _replication/replicationID
func (m *sgReplicateManager) AddReplication(replication *ReplicationCfg) error {
	addReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Replications[replication.ID]
		if exists {
			return false, base.ErrAlreadyExists
		}
		cluster.Replications[replication.ID] = replication
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(addReplicationCallback)
}

// PutReplications sets the value of one or more replications in the config
func (m *sgReplicateManager) PutReplications(replications map[string]*ReplicationConfig) error {
	addReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		if len(replications) == 0 {
			return true, nil
		}
		for replicationID, replication := range replications {
			existingCfg, exists := cluster.Replications[replicationID]
			replicationCfg := &ReplicationCfg{}
			if exists {
				replicationCfg.AssignedNode = existingCfg.AssignedNode
			}
			replicationCfg.ReplicationConfig = *replication
			cluster.Replications[replicationID] = replicationCfg
		}
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(addReplicationCallback)
}

// PUT _replication/replicationID
func (m *sgReplicateManager) UpsertReplication(replication *ReplicationUpsertConfig) (created bool, err error) {

	created = true
	addReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Replications[replication.ID]
		if exists {
			created = false
		} else {
			replicationConfig := DefaultReplicationConfig()
			replicationConfig.ID = replication.ID
			cluster.Replications[replication.ID] = &ReplicationCfg{ReplicationConfig: replicationConfig}
		}
		cluster.Replications[replication.ID].Upsert(replication)

		validateErr := cluster.Replications[replication.ID].ValidateReplication(false)
		if validateErr != nil {
			return true, validateErr
		}

		cluster.RebalanceReplications()
		return false, nil
	}
	return created, m.updateCluster(addReplicationCallback)
}

func (m *sgReplicateManager) UpdateReplicationState(replicationID string, state string) error {

	updateReplicationStatusCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		replicationCfg, exists := cluster.Replications[replicationID]
		if !exists {
			return true, base.ErrNotFound
		}

		stateChangeErr := isValidStateChange(replicationCfg.State, state)
		if stateChangeErr != nil {
			return true, stateChangeErr
		}

		if state == ReplicationStateStopped && replicationCfg.Adhoc == true {
			delete(cluster.Replications, replicationID)
			cluster.RebalanceReplications()
			return false, nil
		}

		upsertReplication := &ReplicationUpsertConfig{
			ID:    replicationID,
			State: &state,
		}
		cluster.Replications[replicationID].Upsert(upsertReplication)

		validateErr := cluster.Replications[replicationID].ValidateReplication(false)
		if validateErr != nil {
			return true, validateErr
		}

		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(updateReplicationStatusCallback)
}

func isValidStateChange(currentState, newState string) error {

	switch newState {
	case ReplicationStateRunning:
		if currentState == ReplicationStateResetting {
			return fmt.Errorf("Replication cannot be started until reset is complete")
		}
		return nil
	case ReplicationStateStopped:
		return nil
	case ReplicationStateResetting:
		if currentState == ReplicationStateRunning {
			return fmt.Errorf("Replication must be stopped before it can be reset")
		}
	}
	return nil
}

func transitionStateName(currentState, targetState string) string {
	transitionState := currentState
	switch targetState {
	case ReplicationStateRunning:
		if currentState != targetState {
			transitionState = "starting"
		}
	case ReplicationStateStopped:
		if currentState != targetState {
			transitionState = "stopping"
		}
	}
	return transitionState
}

// DELETE _replication
func (m *sgReplicateManager) DeleteReplication(replicationID string) error {
	deleteReplicationCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Replications[replicationID]
		if !exists {
			return false, base.ErrNotFound
		}
		delete(cluster.Replications, replicationID)
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(deleteReplicationCallback)
}

func (c *SGRCluster) GetReplicationIDsForNode(nodeUUID string) (replicationIDs []string) {
	replicationIDs = make([]string, 0)
	for id, replication := range c.Replications {
		if replication.AssignedNode == nodeUUID {
			replicationIDs = append(replicationIDs, id)
		}
	}
	return replicationIDs
}

// RebalanceReplications distributes the set of defined replications across the set of available nodes
func (c *SGRCluster) RebalanceReplications() {

	base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Initiating replication rebalance.  Nodes: %d  Replications: %d", len(c.Nodes), len(c.Replications))
	defer base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication rebalance complete, persistence is pending...")

	// Identify unassigned replications that need to be distributed.
	// This includes both replications not assigned to a node, as well as replications
	// assigned to a node that is no longer part of the cluster.
	unassignedReplications := make(map[string]*ReplicationCfg)
	for replicationID, replication := range c.Replications {
		if replication.AssignedNode == "" {
			unassignedReplications[replicationID] = replication
		} else {
			// If replication has an assigned node, remove that assignment if node no longer exists in cluster
			_, ok := c.Nodes[replication.AssignedNode]
			if !ok {
				replication.AssignedNode = ""
				unassignedReplications[replicationID] = replication
				base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s unassigned, previous node no longer active.", replicationID)
			}
		}
	}

	// If there aren't any nodes available, there's nothing more to be done
	if len(c.Nodes) == 0 {
		return
	}

	// Construct a slice of sortableSGNodes, for sorting nodes by the number of assigned replications
	nodesByReplicationCount := make(NodesByReplicationCount, 0)
	for host := range c.Nodes {
		sortableNode := &sortableSGNode{
			host:                   host,
			assignedReplicationIDs: c.GetReplicationIDsForNode(host),
		}
		nodesByReplicationCount = append(nodesByReplicationCount, sortableNode)
	}
	sort.Sort(nodesByReplicationCount)

	// Assign unassigned replications to nodes with the fewest replications already assigned
	for replicationID, replication := range unassignedReplications {
		replication.AssignedNode = nodesByReplicationCount[0].host
		nodesByReplicationCount[0].assignedReplicationIDs = append(nodesByReplicationCount[0].assignedReplicationIDs, replicationID)
		base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s assigned to %s.", replicationID, nodesByReplicationCount[0].host)
		sort.Sort(nodesByReplicationCount)
	}

	// If there is more than one node, balance replications across nodes.
	// When the difference in the number of replications between any two nodes
	// is greater than 1, move a replication from the higher to lower count node.
	numNodes := len(nodesByReplicationCount)
	if len(nodesByReplicationCount) <= 1 {
		return
	}

	for {
		lowCountNode := nodesByReplicationCount[0]
		highCountNode := nodesByReplicationCount[numNodes-1]
		if len(highCountNode.assignedReplicationIDs)-len(lowCountNode.assignedReplicationIDs) < 2 {
			break
		}
		replicationToMove := highCountNode.assignedReplicationIDs[0]
		highCountNode.assignedReplicationIDs = highCountNode.assignedReplicationIDs[1:]
		base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s unassigned from %s.", replicationToMove, highCountNode.host)
		lowCountNode.assignedReplicationIDs = append(lowCountNode.assignedReplicationIDs, replicationToMove)
		c.Replications[replicationToMove].AssignedNode = lowCountNode.host
		base.DebugfCtx(c.loggingCtx, base.KeyReplicate, "Replication %s assigned to %s.", replicationToMove, lowCountNode.host)
		sort.Sort(nodesByReplicationCount)
	}

}

// sortableSGNode and NodesByReplicationCount are used to sort a set of nodes based on the number of replications
// assigned to each node.
type sortableSGNode struct {
	host                   string
	assignedReplicationIDs []string
}

type NodesByReplicationCount []*sortableSGNode

func (a NodesByReplicationCount) Len() int      { return len(a) }
func (a NodesByReplicationCount) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a NodesByReplicationCount) Less(i, j int) bool {
	return len(a[i].assignedReplicationIDs) < len(a[j].assignedReplicationIDs)
}

// ReplicationStatus is used by the _replicationStatus REST API endpoints
type ReplicationStatus struct {
	ID               string             `json:"replication_id"`
	DocsRead         int64              `json:"docs_read"`
	DocsWritten      int64              `json:"docs_written"`
	DocsPurged       int64              `json:"docs_purged,omitempty"`
	DocWriteFailures int64              `json:"doc_write_failures"`
	DocWriteConflict int64              `json:"doc_write_conflict"`
	Status           string             `json:"status"`
	RejectedRemote   int64              `json:"rejected_by_remote"`
	RejectedLocal    int64              `json:"rejected_by_local"`
	LastSeqPull      string             `json:"last_seq_pull,omitempty"`
	LastSeqPush      string             `json:"last_seq_push,omitempty"`
	ErrorMessage     string             `json:"error_message,omitempty"`
	DeltasSent       int64              `json:"deltas_sent,omitempty"`
	DeltasRecv       int64              `json:"deltas_recv,omitempty"`
	DeltasRequested  int64              `json:"deltas_requested,omitempty"`
	Config           *ReplicationConfig `json:"config,omitempty"`
}

func (m *sgReplicateManager) GetReplicationStatus(replicationID string, includeConfig bool) (*ReplicationStatus, error) {

	// Check if replication is assigned locally
	m.activeReplicatorsLock.RLock()
	replication, isLocal := m.activeReplicators[replicationID]
	m.activeReplicatorsLock.RUnlock()

	var status *ReplicationStatus
	if isLocal {
		status = replication.GetStatus()
		if includeConfig {
			config, err := m.GetReplication(replicationID)
			if err != nil {
				return nil, err
			}
			status.Config = &config.ReplicationConfig
		}
		return status, nil
	}

	// Check if replication is remote
	// TODO: generation of remote status pending CBG-909
	remoteCfg, err := m.GetReplication(replicationID)
	if err != nil {
		return nil, err
	}
	status = &ReplicationStatus{
		ID:     replicationID,
		Status: remoteCfg.State,
	}
	if includeConfig {
		status.Config = &remoteCfg.ReplicationConfig
	}
	return status, nil
}

func (m *sgReplicateManager) PutReplicationStatus(replicationID, action string) (status *ReplicationStatus, err error) {

	targetState := ""
	switch action {
	case "reset":
		targetState = ReplicationStateResetting
	case "stop":
		targetState = ReplicationStateStopped
	case "start":
		targetState = ReplicationStateRunning
	default:
		return nil, base.HTTPErrorf(http.StatusBadRequest, "Unrecognized action %q.  Valid values are start/stop/reset.", action)
	}

	err = m.UpdateReplicationState(replicationID, targetState)
	if err != nil {
		return nil, err
	}

	updatedStatus, err := m.GetReplicationStatus(replicationID, false)
	if err != nil {
		// Not found is expected when adhoc replication is stopped, return removed status instead of error
		// since UpdateReplicationState was successful
		if err == base.ErrNotFound {
			replicationStatus := &ReplicationStatus{
				ID:     replicationID,
				Status: "removed",
			}
			return replicationStatus, nil
		} else {
			return nil, err
		}
	}

	// Modify the returned replication state to align with the requested state
	updatedStatus.Status = transitionStateName(updatedStatus.Status, targetState)
	return updatedStatus, nil
}

func (m *sgReplicateManager) GetReplicationStatusAll(includeConfig bool) ([]*ReplicationStatus, error) {

	statuses := make([]*ReplicationStatus, 0)

	// Include persisted replications
	persistedReplications, err := m.GetReplications()
	if err != nil {
		return nil, err
	}

	for replicationID, _ := range persistedReplications {
		status, err := m.GetReplicationStatus(replicationID, includeConfig)
		if err != nil {
			base.Warnf("Unable to retrieve replication status for replication %s", replicationID)
		}
		statuses = append(statuses, status)
	}

	return statuses, nil
}

// ImportHeartbeatListener uses replication cfg to manage node list
type ReplicationHeartbeatListener struct {
	mgr        *sgReplicateManager
	terminator chan struct{} // close manager subscription on close
	nodeIDs    []string      // Set of nodes from the latest retrieval
	lock       sync.RWMutex  // lock for nodeIDs access
}

func NewReplicationHeartbeatListener(mgr *sgReplicateManager) (*ReplicationHeartbeatListener, error) {

	if mgr == nil {
		return nil, errors.New("sgReplicateManager must not be nil for ReplicationHeartbeatListener")
	}

	listener := &ReplicationHeartbeatListener{
		mgr:        mgr,
		terminator: make(chan struct{}),
	}

	// Initialize the node set
	err := listener.reloadNodes()
	if err != nil {
		return nil, err
	}

	// Subscribe to changes to the known node set key
	err = listener.subscribeNodeSetChanges()
	if err != nil {
		return nil, err
	}

	return listener, nil
}

func (l *ReplicationHeartbeatListener) Name() string {
	return "sgReplicateListener"
}

// When we detect other nodes have stopped pushing heartbeats, use manager to remove from cfg
func (l *ReplicationHeartbeatListener) StaleHeartbeatDetected(nodeUUID string) {

	base.Debugf(base.KeyCluster, "StaleHeartbeatDetected by sg-replicate listener for node: %v", nodeUUID)
	err := l.mgr.RemoveNode(nodeUUID)
	if err != nil {
		base.Warnf("Attempt to remove node %v from sg-replicate cfg got error: %v", nodeUUID, err)
	}
}

// subscribeNodeChanges registers with the manager's cfg implementation for notifications on changes to the
// cfgKeySGRCluster key.  When notified, refreshes the handlers nodeIDs.
func (l *ReplicationHeartbeatListener) subscribeNodeSetChanges() error {

	cfgEvents := make(chan cbgt.CfgEvent)

	err := l.mgr.cfg.Subscribe(cfgKeySGRCluster, cfgEvents)
	if err != nil {
		base.Debugf(base.KeyCluster, "Error subscribing to %s key changes: %v", cfgKeySGRCluster, err)
		return err
	}
	go func() {
		defer base.FatalPanicHandler()
		for {
			select {
			case <-cfgEvents:
				err := l.reloadNodes()
				if err != nil {
					base.Warnf("Error while reloading heartbeat node definitions: %v", err)
				}
			case <-l.terminator:
				return
			}
		}
	}()
	return nil
}

func (l *ReplicationHeartbeatListener) reloadNodes() error {

	nodeSet, err := l.mgr.getNodes()
	if err != nil {
		return err
	}

	nodeUUIDs := make([]string, 0)
	if nodeSet != nil {
		for _, nodeDef := range nodeSet {
			nodeUUIDs = append(nodeUUIDs, nodeDef.UUID)
		}
	}

	l.lock.Lock()
	l.nodeIDs = nodeUUIDs
	l.lock.Unlock()

	return nil
}

// GetNodes returns a copy of the in-memory node set
func (l *ReplicationHeartbeatListener) GetNodes() ([]string, error) {

	l.lock.RLock()
	nodeIDsCopy := make([]string, len(l.nodeIDs))
	copy(nodeIDsCopy, l.nodeIDs)
	l.lock.RUnlock()
	return nodeIDsCopy, nil
}

func (l *ReplicationHeartbeatListener) Stop() {
	close(l.terminator)
}
