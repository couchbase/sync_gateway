package db

import (
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

	manager, err := NewSGReplicateManager(testCfg)
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

	manager, err := NewSGReplicateManager(testCfg)
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
	manager, err := NewSGReplicateManager(testCfg)
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
	manager, err := NewSGReplicateManager(testCfg)
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
