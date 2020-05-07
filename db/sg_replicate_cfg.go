package db

import (
	"context"
	"errors"
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

// ReplicationCfg represents a replication that has been defined for the cluster.
type ReplicationCfg struct {
	ID           string `json:"replication_id"`
	AssignedNode string // host name of node assigned to this replication
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
