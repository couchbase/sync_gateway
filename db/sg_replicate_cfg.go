package db

import (
	"context"
	"errors"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/sync_gateway/base"
)

const (
	cfgKeySGRCluster        = "sgrCluster" // key used for sgrCluster information in a cbgt.Cfg-based key value store
	maxSGRClusterCasRetries = 100          // Maximum number of CAS retries when attempting to update the sgr cluster configuration
	sgrClusterMgrContextID  = "sgr-mgr"    // logging context ID for sgreplicate manager
)

// ClusterUpdateFunc is callback signature used when updating the cluster configuration
type ClusterUpdateFunc func(cluster *SGRCluster) (cancel bool, err error)

// SGRCluster defines sg-replicate configuration and distribution for a collection of Sync Gateway nodes
type SGRCluster struct {
	Replications map[string]*ReplicationCfg `json:"replications"` // Set of replications defined for the cluster, indexed by replicationID
	Nodes        map[string]*SGNode         `json:"nodes"`        // Set of nodes, indexed by host name
}

func NewSGRCluster() *SGRCluster {
	return &SGRCluster{
		Replications: make(map[string]*ReplicationCfg),
		Nodes:        make(map[string]*SGNode),
	}
}

// SGNode represents a single Sync Gateway node in the cluster
type SGNode struct {
	Host                 string   `json:"host"`                  // Host name
	AssignedReplications []string `json:"assigned_replications"` // Set of replications assigned to this node
}

func NewSGNode(host string) *SGNode {
	return &SGNode{
		Host:                 host,
		AssignedReplications: make([]string, 0),
	}
}

// ReplicationCfg represents a replication that has been defined for the cluster.
type ReplicationCfg struct {
	ID           string `json:"replication_id"`
	assignedNode string // host name of node assigned to this replication
	// TODO: embed full replication config as part of CBG-765
}

// sgReplicateManager should be used for all interactions with the stored cluster definition.
type sgReplicateManager struct {
	cfg        cbgt.Cfg        // Key-value store implementation
	loggingCtx context.Context // logging context for manager operations
}

func NewSGReplicateManager(cfg cbgt.Cfg) (*sgReplicateManager, error) {
	if cfg == nil {
		return nil, errors.New("Cfg must be provided for SGReplicateManager")
	}

	return &sgReplicateManager{
		cfg: cfg,
		loggingCtx: context.WithValue(context.Background(), base.LogContextKey{},
			base.LogContext{CorrelationID: sgrClusterMgrContextID}),
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
			base.Debugf(base.KeyReplicate, "CAS Retry updating cluster definition (%d/%d)", i, maxSGRClusterCasRetries)
		} else {
			return err
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

func (c *SGRCluster) RebalanceReplications() {
	// TODO: CBG-769
}
