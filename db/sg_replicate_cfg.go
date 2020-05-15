package db

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/url"
	"sort"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

const (
	cfgKeySGRCluster        = "sgrCluster" // key used for sgrCluster information in a cbgt.Cfg-based key value store
	maxSGRClusterCasRetries = 100          // Maximum number of CAS retries when attempting to update the sgr cluster configuration
	sgrClusterMgrContextID  = "sgr-mgr-"   // logging context ID prefix for sgreplicate manager
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
	Host string `json:"host"` // Host name
}

func NewSGNode(host string) *SGNode {
	return &SGNode{
		Host: host,
	}
}

// ReplicationConfig is a replication definition as stored in the Sync Gateway config
type ReplicationConfig struct {
	ID                     string      `json:"replication_id"`
	Remote                 string      `json:"remote"`
	Direction              string      `json:"direction"`
	ConflictResolutionType string      `json:"conflict_resolution_type,omitempty"`
	ConflictResolutionFn   string      `json:"custom_conflict_resolver,omitempty"`
	PurgeOnRemoval         bool        `json:"purge_on_removal,omitempty"`
	DeltaSyncEnabled       bool        `json:"enable_delta_sync,omitempty"`
	MaxBackoff             int         `json:"max_backoff_time,omitempty"`
	State                  string      `json:"state,omitempty"`
	Continuous             bool        `json:"continuous,omitempty"`
	Filter                 string      `json:"filter,omitempty"`
	QueryParams            interface{} `json:"query_params,omitempty"`
	Cancel                 bool        `json:"cancel,omitempty"`
}

// ReplicationCfg represents a replication definition as stored in the cluster config.
type ReplicationCfg struct {
	ReplicationConfig
	AssignedNode string `json:"assigned_node"` // host name of node assigned to this replication
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
	Continuous             *bool       `json:"continuous,omitempty"`
	Filter                 *string     `json:"filter,omitempty"`
	QueryParams            interface{} `json:"query_params,omitempty"`
	Cancel                 *bool       `json:"cancel,omitempty"`
}

func (rc *ReplicationConfig) ValidateReplication(fromConfig bool) (err error) {

	// Cancel is only supported via the REST API
	if rc.Cancel {
		if fromConfig {
			err = base.HTTPErrorf(http.StatusBadRequest, "cancel=true is invalid for replication in Sync Gateway configuration")
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

	remoteUrl, err := url.Parse(rc.Remote)
	if err != nil {
		err = base.HTTPErrorf(http.StatusBadRequest, "Replication remote URL [%s] is invalid: %v", remoteUrl, err)
		return err
	}

	if rc.Filter == "sync_gateway/bychannel" {
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
		rc.Direction = *c.Direction
	}

	if c.ConflictResolutionType != nil {
		rc.ConflictResolutionType = *c.ConflictResolutionType
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
	if c.QueryParams != nil {
		rc.QueryParams = c.QueryParams
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
	cfg        cbgt.Cfg        // Key-value store implementation
	loggingCtx context.Context // logging context for manager operations
}

func NewSGReplicateManager(cfg cbgt.Cfg, dbName string) (*sgReplicateManager, error) {
	if cfg == nil {
		return nil, errors.New("Cfg must be provided for SGReplicateManager")
	}

	return &sgReplicateManager{
		cfg: cfg,
		loggingCtx: context.WithValue(context.Background(), base.LogContextKey{},
			base.LogContext{CorrelationID: sgrClusterMgrContextID + dbName}),
	}, nil

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
func (m *sgReplicateManager) RegisterNode(host string) error {
	registerNodeCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Nodes[host]
		if exists {
			return true, nil
		}
		cluster.Nodes[host] = NewSGNode(host)
		cluster.RebalanceReplications()
		return false, nil
	}
	return m.updateCluster(registerNodeCallback)
}

// RemoveNode removes a node from the cluster config, then triggers replication rebalance.
// Retries on CAS failure, is no-op if node doesn't exist in config
func (m *sgReplicateManager) RemoveNode(host string) error {
	removeNodeCallback := func(cluster *SGRCluster) (cancel bool, err error) {
		_, exists := cluster.Nodes[host]
		if !exists {
			return true, nil
		}
		delete(cluster.Nodes, host)
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
	if exists {
		return replication, nil
	} else {
		return nil, base.ErrNotFound
	}
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
			cluster.Replications[replication.ID] = &ReplicationCfg{ReplicationConfig: ReplicationConfig{ID: replication.ID}}
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

func (c *SGRCluster) GetReplicationIDsForNode(host string) (replicationIDs []string) {
	replicationIDs = make([]string, 0)
	for id, replication := range c.Replications {
		if replication.AssignedNode == host {
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
	for host, _ := range c.Nodes {
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
	ID               string     `json:"replication_id"`
	DocsRead         int64      `json:"docs_read"`
	DocsWritten      int64      `json:"docs_written"`
	DocWriteFailures int64      `json:"doc_write_failures"`
	Status           string     `json:"status"`
	RejectedRemote   int64      `json:"rejected_by_remote"`
	RejectedLocal    int64      `json:"rejected_by_local"`
	LastSeqPull      SequenceID `json:"last_seq_pull,omitempty"`
	LastSeqPush      SequenceID `json:"last_seq_push,omitempty"`
	ErrorMessage     string     `json:"error_message,omitempty"`
}

func (m *sgReplicateManager) GetReplicationStatus(replicationID string) *ReplicationStatus {
	// TODO: CBG-768
	return &ReplicationStatus{
		ID:               replicationID,
		DocsRead:         100,
		DocsWritten:      100,
		DocWriteFailures: 5,
		Status:           "running",
		RejectedRemote:   10,
		RejectedLocal:    7,
		LastSeqPull:      SequenceID{Seq: 500},
		ErrorMessage:     "",
	}
}

func (m *sgReplicateManager) PutReplicationStatus(replicationID, action string) error {
	// TODO: CBG-768
	return nil
}

func (m *sgReplicateManager) GetReplicationStatusAll() []*ReplicationStatus {
	// TODO: CBG-768
	return []*ReplicationStatus{
		{
			ID:               "sampleReplication1",
			DocsRead:         100,
			DocsWritten:      100,
			DocWriteFailures: 5,
			Status:           "running",
			RejectedRemote:   10,
			RejectedLocal:    7,
			LastSeqPull:      SequenceID{Seq: 500},
			ErrorMessage:     "",
		},
		{
			ID:               "sampleReplication2",
			DocsRead:         150,
			DocsWritten:      150,
			DocWriteFailures: 3,
			Status:           "stopped",
			RejectedRemote:   10,
			RejectedLocal:    7,
			LastSeqPull:      SequenceID{Seq: 50},
			LastSeqPush:      SequenceID{Seq: 75},
			ErrorMessage:     "",
		},
	}
}
