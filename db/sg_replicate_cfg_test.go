package db

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/couchbase/sync_gateway/base"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test node operations on SGReplicateManager
func TestReplicateManagerReplications(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket

	testCfg, err := base.NewCfgSG(bucket)
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(testCfg, "test")
	require.NoError(t, err)

	replication1_id := "replication1"
	err = manager.AddReplication(&ReplicationCfg{ID: replication1_id})

	r, err := manager.GetReplication(replication1_id)
	require.NoError(t, err)
	assert.Equal(t, replication1_id, r.ID)

	// Request non-existent replication
	r, err = manager.GetReplication("dne")
	require.Error(t, err, base.ErrNotFound)

	// Attempt to add existing replication
	err = manager.AddReplication(&ReplicationCfg{ID: replication1_id})
	require.Error(t, err, base.ErrAlreadyExists)

	// Add a second replication
	replication2_id := "replication2"
	err = manager.AddReplication(&ReplicationCfg{ID: replication2_id})
	require.NoError(t, err)

	r, err = manager.GetReplication(replication1_id)
	require.NoError(t, err)
	assert.Equal(t, replication1_id, r.ID)

	replications, err := manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 2, len(replications))

	// Remove replication
	err = manager.DeleteReplication(replication1_id)
	require.NoError(t, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 1, len(replications))

	// Remove non-existent replication
	err = manager.DeleteReplication(replication1_id)
	require.Error(t, base.ErrNotFound, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 1, len(replications))

	// Remove last replication
	err = manager.DeleteReplication(replication2_id)
	require.NoError(t, err)
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 0, len(replications))
}

// Test node operations on SGReplicateManager
func TestReplicateManagerNodes(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket

	testCfg, err := base.NewCfgSG(bucket)
	require.NoError(t, err)

	manager, err := NewSGReplicateManager(testCfg, "test")
	require.NoError(t, err)

	err = manager.RegisterNode("node1")
	require.NoError(t, err)

	nodes, err := manager.getNodes()
	require.NoError(t, err)
	assert.Equal(t, 1, len(nodes))

	err = manager.RegisterNode("node2")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	assert.Equal(t, 2, len(nodes))

	// re-adding an existing node is a no-op
	err = manager.RegisterNode("node1")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	assert.Equal(t, 2, len(nodes))

	// Remove node
	err = manager.RemoveNode("node1")
	require.NoError(t, err)

	nodes, err = manager.getNodes()
	require.NoError(t, err)
	require.Equal(t, 1, len(nodes))
	node2, ok := nodes["node2"]
	require.True(t, ok)
	require.Equal(t, node2.Host, "node2")

	// Removing an already removed node is a no-op
	err = manager.RemoveNode("node1")
	require.NoError(t, err)

	replications, err := manager.GetReplications()
	require.NoError(t, err)
	assert.Equal(t, 0, len(replications))
}

// Test concurrent node operations on SGReplicateManager
func TestReplicateManagerConcurrentNodeOperations(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket

	testCfg, err := base.NewCfgSG(bucket)
	require.NoError(t, err)
	manager, err := NewSGReplicateManager(testCfg, "test")
	require.NoError(t, err)

	var nodeWg sync.WaitGroup

	for i := 0; i < 20; i++ {
		nodeWg.Add(1)
		go func(i int) {
			defer nodeWg.Done()
			err := manager.RegisterNode(fmt.Sprintf("node_%d", i))
			assert.NoError(t, err)
		}(i)
	}

	nodeWg.Wait()
	nodes, err := manager.getNodes()
	require.NoError(t, err)
	require.Equal(t, 20, len(nodes))

	for i := 0; i < 20; i++ {
		nodeWg.Add(1)
		go func(i int) {
			defer nodeWg.Done()
			err := manager.RemoveNode(fmt.Sprintf("node_%d", i))
			assert.NoError(t, err)
		}(i)
	}

	nodeWg.Wait()
	nodes, err = manager.getNodes()
	require.NoError(t, err)
	require.Equal(t, 0, len(nodes))
}

// Test concurrent replication operations on SGReplicateManager
func TestReplicateManagerConcurrentReplicationOperations(t *testing.T) {

	testBucket := base.GetTestBucket(t)
	defer testBucket.Close()
	bucket := testBucket.Bucket

	testCfg, err := base.NewCfgSG(bucket)
	require.NoError(t, err)
	manager, err := NewSGReplicateManager(testCfg, "test")
	require.NoError(t, err)

	var replicationWg sync.WaitGroup

	for i := 0; i < 20; i++ {
		replicationWg.Add(1)
		go func(i int) {
			defer replicationWg.Done()
			err := manager.AddReplication(&ReplicationCfg{ID: fmt.Sprintf("r_%d", i)})
			assert.NoError(t, err)
		}(i)
	}

	replicationWg.Wait()
	replications, err := manager.GetReplications()
	require.NoError(t, err)
	require.Equal(t, 20, len(replications))

	for i := 0; i < 20; i++ {
		replicationWg.Add(1)
		go func(i int) {
			defer replicationWg.Done()
			err := manager.DeleteReplication(fmt.Sprintf("r_%d", i))
			assert.NoError(t, err)
		}(i)
	}

	replicationWg.Wait()
	replications, err = manager.GetReplications()
	require.NoError(t, err)
	require.Equal(t, 0, len(replications))
}

func TestRebalanceReplications(t *testing.T) {

	defer base.SetUpTestLogging(base.LevelDebug, base.KeyReplicate)()

	type rebalanceTest struct {
		name                  string                     // Test name
		nodes                 map[string]*SGNode         // Initial node set
		replications          map[string]*ReplicationCfg // Initial replication assignment
		expectedMinPerNode    int                        // Minimum replications per node after rebalance
		expectedMaxPerNode    int                        // Maximum replications per node after rebalance
		expectedTotalAssigned int                        // Expected total number of assigned replications post-rebalance
	}
	testCases := []rebalanceTest{
		{
			name: "new nodes",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
				"n2": {Host: "n2"},
				"n3": {Host: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: "n1"},
				"r2": {ID: "r2", AssignedNode: "n1"},
				"r3": {ID: "r3", AssignedNode: "n1"},
			},
			expectedMinPerNode:    1,
			expectedMaxPerNode:    1,
			expectedTotalAssigned: 3,
		},
		{
			name: "new replications",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
				"n2": {Host: "n2"},
				"n3": {Host: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: ""},
				"r2": {ID: "r2", AssignedNode: ""},
				"r3": {ID: "r3", AssignedNode: ""},
			},
			expectedMinPerNode:    1,
			expectedMaxPerNode:    1,
			expectedTotalAssigned: 3,
		},
		{
			name: "remove nodes",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
				"n2": {Host: "n2"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: "n1"},
				"r2": {ID: "r2", AssignedNode: "n2"},
				"r3": {ID: "r3", AssignedNode: "n3"},
				"r4": {ID: "r4", AssignedNode: "n4"},
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 4,
		},
		{
			name:  "no nodes",
			nodes: map[string]*SGNode{},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: "n1"},
				"r2": {ID: "r2", AssignedNode: "n1"},
				"r3": {ID: "r3", AssignedNode: "n2"},
			},
			expectedMinPerNode:    0,
			expectedMaxPerNode:    0,
			expectedTotalAssigned: 0,
		},
		{
			name:  "single node",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: "n1"},
				"r2": {ID: "r2", AssignedNode: "n2"},
				"r3": {ID: "r3", AssignedNode: ""},
			},
			expectedMinPerNode:    3,
			expectedMaxPerNode:    3,
			expectedTotalAssigned: 3,
		},
		{
			name: "unbalanced distribution",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
				"n2": {Host: "n2"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: "n1"},
				"r2": {ID: "r2", AssignedNode: "n1"},
				"r3": {ID: "r3", AssignedNode: "n1"},
			},
			expectedMinPerNode:    1,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 3,
		},
		{
			name: "multiple reassignments new nodes",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
				"n2": {Host: "n2"},
				"n3": {Host: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: "n1"},
				"r2": {ID: "r2", AssignedNode: "n1"},
				"r3": {ID: "r3", AssignedNode: "n1"},
				"r4": {ID: "r4", AssignedNode: "n1"},
				"r5": {ID: "r5", AssignedNode: "n1"},
				"r6": {ID: "r6", AssignedNode: "n1"},
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 6,
		},
		{
			name: "multiple reassignments new replications",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
				"n2": {Host: "n2"},
				"n3": {Host: "n3"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: ""},
				"r2": {ID: "r2", AssignedNode: ""},
				"r3": {ID: "r3", AssignedNode: ""},
				"r4": {ID: "r4", AssignedNode: ""},
				"r5": {ID: "r5", AssignedNode: "n1"},
				"r6": {ID: "r6", AssignedNode: "n2"},
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 6,
		},
		{
			name: "reassignment from unknown host",
			nodes: map[string]*SGNode{
				"n1": {Host: "n1"},
				"n2": {Host: "n2"},
			},
			replications: map[string]*ReplicationCfg{
				"r1": {ID: "r1", AssignedNode: "n3"},
				"r2": {ID: "r2", AssignedNode: "n3"},
				"r3": {ID: "r3", AssignedNode: "n3"},
				"r4": {ID: "r4", AssignedNode: "n3"},
			},
			expectedMinPerNode:    2,
			expectedMaxPerNode:    2,
			expectedTotalAssigned: 4,
		},
	}

	for _, testCase := range testCases {
		t.Run(fmt.Sprintf("%s", testCase.name), func(t *testing.T) {

			cluster := NewSGRCluster()
			cluster.loggingCtx = context.WithValue(context.Background(), base.LogContextKey{},
				base.LogContext{CorrelationID: sgrClusterMgrContextID + "test"})
			cluster.Nodes = testCase.nodes
			cluster.Replications = testCase.replications
			cluster.RebalanceReplications()

			// Verify post-rebalance distribution
			for host, _ := range cluster.Nodes {
				nodeReplications := cluster.GetReplicationIDsForNode(host)
				assert.True(t, len(nodeReplications) >= testCase.expectedMinPerNode)
				assert.True(t, len(nodeReplications) <= testCase.expectedMaxPerNode)
			}

			// Verify replications are all assigned
			assignedCount := 0
			for _, replication := range cluster.Replications {
				if replication.AssignedNode != "" {
					assignedCount++
				}
			}
			assert.Equal(t, testCase.expectedTotalAssigned, assignedCount)
		})
	}
}
