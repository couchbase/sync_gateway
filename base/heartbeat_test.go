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
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/couchbase/cbgt"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHeartbeatListener uses a shared node store, and maintains a count of stale notifications
type TestHeartbeatListener struct {
	name             string
	nodeStore        *TestHeartbeatNodeStore // shared definition of nodes
	nodeUUIDs        map[string]struct{}
	staleDetectCount uint32
}

type TestHeartbeatNodeStore struct {
	nodes map[string]struct{}
	lock  sync.Mutex
}

func NewTestHeartbeatNodeStore() *TestHeartbeatNodeStore {
	return &TestHeartbeatNodeStore{
		nodes: make(map[string]struct{}),
	}
}

func (ns *TestHeartbeatNodeStore) GetNodes() []string {
	nodeSet := make([]string, 0)
	ns.lock.Lock()
	for uuid, _ := range ns.nodes {
		nodeSet = append(nodeSet, uuid)
	}
	ns.lock.Unlock()
	return nodeSet

}
func (ns *TestHeartbeatNodeStore) AddNode(nodeUUID string) {
	ns.lock.Lock()
	ns.nodes[nodeUUID] = struct{}{}
	ns.lock.Unlock()
}

func (ns *TestHeartbeatNodeStore) RemoveNode(nodeUUID string) {
	ns.lock.Lock()
	_, ok := ns.nodes[nodeUUID]
	if ok {
		delete(ns.nodes, nodeUUID)
	}
	ns.lock.Unlock()
}

func NewTestHeartbeatListener(handlerID string, nodeStore *TestHeartbeatNodeStore) *TestHeartbeatListener {
	testListener := &TestHeartbeatListener{
		name:      handlerID,
		nodeStore: nodeStore,
	}
	return testListener
}

func (th *TestHeartbeatListener) StaleHeartbeatDetected(nodeUUID string) {
	log.Printf("Handler %s detected stale heartbeat for %v, will be removed", th.name, nodeUUID)
	th.nodeStore.RemoveNode(nodeUUID)
	atomic.AddUint32(&th.staleDetectCount, 1)
}

func (th *TestHeartbeatListener) GetNodes() (nodeUUIDs []string, err error) {
	nodeSet := th.nodeStore.GetNodes()
	return nodeSet, nil
}

func (th *TestHeartbeatListener) Stop() {
	return
}

// TestNewCouchbaseHeartbeater starts three heartbeaters (simulating three nodes), then stops a node
// and validates the node triggers heartbeat-based removal on one of the remaining nodes.
func TestCouchbaseHeartbeaters(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus - no expiry, required for heartbeats")
	}

	if testing.Short() {
		t.Skip("Skipping heartbeattest in short mode")
	}

	SetUpTestLogging(t, LevelDebug, KeyDCP)

	keyprefix := SyncPrefix + t.Name()

	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	// Setup heartbeaters and listeners
	nodeCount := 3
	nodes := make([]*couchbaseHeartBeater, nodeCount)
	listeners := make([]*documentBackedListener, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeUUID := fmt.Sprintf("node%d", i)
		node, err := NewCouchbaseHeartbeater(testBucket, keyprefix, nodeUUID)
		assert.NoError(t, err)

		// Lower heartbeat expiry to avoid long-running test
		assert.NoError(t, node.SetExpirySeconds(2))

		// Start node
		assert.NoError(t, node.Start())

		// Create and register listener.
		// Simulates service starting on node, and self-registering the nodeUUID to that listener's node set
		listener, err := NewDocumentBackedListener(testBucket, keyprefix)
		require.NoError(t, err)
		assert.NoError(t, listener.AddNode(nodeUUID))
		assert.NoError(t, node.RegisterListener(listener))

		nodes[i] = node
		listeners[i] = listener
	}

	// Wait for node0 to start running (and persist initial heartbeat docs) before stopping
	retryUntilFunc := func() bool {
		return nodes[0].checkCount > 0 && nodes[0].sendCount > 0
	}
	testRetryUntilTrue(t, retryUntilFunc)
	assert.True(t, nodes[0].checkCount > 0)
	assert.True(t, nodes[0].sendCount > 0)

	// Stop node 0
	nodes[0].Stop()

	// Wait for another node to detect node0 has stopped sending heartbeats
	retryUntilFunc = func() bool {
		return listeners[1].StaleNotificationCount() >= 1 ||
			listeners[2].StaleNotificationCount() >= 1
	}
	testRetryUntilTrue(t, retryUntilFunc)

	// Validate that at least one node detected the stopped node 0
	h2staleDetectCount := listeners[1].StaleNotificationCount()
	h3staleDetectCount := listeners[2].StaleNotificationCount()
	assert.True(t, h2staleDetectCount >= 1 || h3staleDetectCount >= 1,
		fmt.Sprintf("Expected stale detection counts (1) not found in either handler2 (%d) or handler3 (%d)", h2staleDetectCount, h3staleDetectCount))

	// Validate current node list
	activeNodes, err := listeners[0].GetNodes()
	require.NoError(t, err, "Error getting node list")
	require.Len(t, activeNodes, 2)
	assert.NotContains(t, activeNodes, "node0")
	assert.Contains(t, activeNodes, "node1")
	assert.Contains(t, activeNodes, "node2")

	// Stop heartbeaters
	nodes[1].Stop()
	nodes[2].Stop()
}

// TestNewCouchbaseHeartbeater simulates three nodes, with two services (listeners).
func TestCouchbaseHeartbeatersMultipleListeners(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus - no expiry, required for heartbeats")
	}

	if testing.Short() {
		t.Skip("Skipping heartbeattest in short mode")
	}

	keyprefix := SyncPrefix + t.Name()
	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	// Setup heartbeaters and listeners
	nodeCount := 3
	nodes := make([]*couchbaseHeartBeater, nodeCount)
	importListeners := make([]*documentBackedListener, nodeCount)
	sgrListeners := make([]*documentBackedListener, nodeCount)
	for i := 0; i < nodeCount; i++ {
		nodeUUID := fmt.Sprintf("node%d", i)
		node, err := NewCouchbaseHeartbeater(testBucket, keyprefix, nodeUUID)
		assert.NoError(t, err)

		// Lower heartbeat expiry to avoid long-running test
		assert.NoError(t, node.SetExpirySeconds(2))

		// Start node
		assert.NoError(t, node.Start())

		// Create and register import listener on all nodes.
		// Simulates service starting on node, and self-registering the nodeUUID to that listener's node set
		importListener, err := NewDocumentBackedListener(testBucket, keyprefix+":import")
		require.NoError(t, err)
		assert.NoError(t, importListener.AddNode(nodeUUID))
		assert.NoError(t, node.RegisterListener(importListener))
		importListeners[i] = importListener

		//Create and register sgr listener on two nodes
		if i < 2 {
			sgrListener, err := NewDocumentBackedListener(testBucket, keyprefix+":sgr")
			require.NoError(t, err)
			assert.NoError(t, sgrListener.AddNode(nodeUUID))
			assert.NoError(t, node.RegisterListener(sgrListener))
			sgrListeners[i] = sgrListener
		}

		nodes[i] = node
	}

	// Wait for node1 to start running (and persist initial heartbeat docs) before stopping
	retryUntilFunc := func() bool {
		return nodes[0].checkCount > 0 && nodes[0].sendCount > 0
	}
	testRetryUntilTrue(t, retryUntilFunc)
	assert.True(t, nodes[0].checkCount > 0)
	assert.True(t, nodes[0].sendCount > 0)

	// Stop node 1
	nodes[0].Stop()

	// Wait for both listener types node to detect node1 has stopped sending heartbeats
	retryUntilFunc = func() bool {
		return importListeners[1].StaleNotificationCount() >= 1 ||
			importListeners[2].StaleNotificationCount() >= 1
	}
	testRetryUntilTrue(t, retryUntilFunc)

	retryUntilFunc = func() bool {
		return sgrListeners[1].StaleNotificationCount() >= 1
	}
	testRetryUntilTrue(t, retryUntilFunc)

	log.Printf("checking for dropped node detection")
	// Validate that at least one node detected the stopped node 1
	h2staleDetectCount := importListeners[1].StaleNotificationCount()
	h3staleDetectCount := importListeners[2].StaleNotificationCount()
	assert.True(t, h2staleDetectCount >= 1 || h3staleDetectCount >= 1,
		fmt.Sprintf("Expected stale detection counts (1) not found in either handler2 (%d) or handler3 (%d)", h2staleDetectCount, h3staleDetectCount))

	// Validate current node list for import with one of the import listeners
	activeImportNodes, err := importListeners[1].GetNodes()
	log.Printf("import listener nodes: %+v", activeImportNodes)
	require.NoError(t, err, "Error getting node list")
	require.Len(t, activeImportNodes, 2)
	assert.NotContains(t, activeImportNodes, "node0")
	assert.Contains(t, activeImportNodes, "node1")
	assert.Contains(t, activeImportNodes, "node2")

	// Validate current node list for sgr with one of the sgr listeners
	activeReplicateNodes, err := sgrListeners[1].GetNodes()
	log.Printf("replicate listener nodes: %+v", activeReplicateNodes)
	require.NoError(t, err, "Error getting node list")
	require.Len(t, activeReplicateNodes, 1)
	assert.NotContains(t, activeReplicateNodes, "node0")
	assert.Contains(t, activeReplicateNodes, "node1")
	assert.NotContains(t, activeReplicateNodes, "node2")

	// Stop heartbeaters
	nodes[1].Stop()
	nodes[2].Stop()
}

// TestNewCouchbaseHeartbeater simulates three nodes.  The minimum time window for failed node
// detection is 2 seconds, based on Couchbase Server's minimum document expiry TTL of
// one second, so retry polling is required.
func TestCBGTManagerHeartbeater(t *testing.T) {

	SetUpTestLogging(t, LevelDebug, KeyDCP)

	if UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus - no expiry, required for heartbeats")
	}

	if testing.Short() {
		t.Skip("Skipping heartbeattest in short mode")
	}

	keyprefix := SyncPrefix + t.Name()

	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	// Initialize cfgCB
	cfgCB, err := initCfgCB(testBucket, testBucket.BucketSpec)
	require.NoError(t, err)

	// Simulate the three nodes self-registering into the cfg
	nodeDefs := cbgt.NewNodeDefs("1.0.0")
	nodeDefs.NodeDefs["node1"] = &cbgt.NodeDef{UUID: "node1"}
	nodeDefs.NodeDefs["node2"] = &cbgt.NodeDef{UUID: "node2"}
	nodeDefs.NodeDefs["node3"] = &cbgt.NodeDef{UUID: "node3"}
	_, err = cbgt.CfgSetNodeDefs(cfgCB, cbgt.NODE_DEFS_KNOWN, nodeDefs, 0)
	require.NoError(t, err)

	// Create three heartbeaters (representing three nodes)
	node1, err := NewCouchbaseHeartbeater(testBucket, keyprefix, "node1")
	assert.NoError(t, err)
	node2, err := NewCouchbaseHeartbeater(testBucket, keyprefix, "node2")
	assert.NoError(t, err)
	node3, err := NewCouchbaseHeartbeater(testBucket, keyprefix, "node3")
	assert.NoError(t, err)

	assert.NoError(t, node1.SetExpirySeconds(2))
	assert.NoError(t, node2.SetExpirySeconds(2))
	assert.NoError(t, node3.SetExpirySeconds(2))

	assert.NoError(t, node1.Start())
	assert.NoError(t, node2.Start())
	assert.NoError(t, node3.Start())

	// Create three heartbeat listeners, associate one with each node
	testUUID := cbgt.NewUUID()
	var eventHandlers cbgt.ManagerEventHandlers
	options := make(map[string]string)
	options[cbgt.FeedAllotmentOption] = cbgt.FeedAllotmentOnePerPIndex
	options["managerLoadDataDir"] = "false"
	testManager := cbgt.NewManagerEx(
		cbgt.VERSION,
		cbgt.NewCfgMem(),
		testUUID,
		nil,
		"",
		1,
		"",
		testUUID,
		"",
		"some-datasource",
		eventHandlers,
		options)
	listener1, err := NewImportHeartbeatListener(cfgCB, testManager)
	assert.NoError(t, err)
	assert.NoError(t, node1.RegisterListener(listener1))

	listener2, err := NewImportHeartbeatListener(cfgCB, testManager)
	assert.NoError(t, err)
	assert.NoError(t, node2.RegisterListener(listener2))

	listener3, err := NewImportHeartbeatListener(cfgCB, testManager)
	assert.NoError(t, err)
	assert.NoError(t, node3.RegisterListener(listener3))

	// Wait for node1 to start running (and persist initial heartbeat docs) before stopping
	retryUntilFunc := func() bool {
		return node1.checkCount > 0 && node1.sendCount > 0
	}
	testRetryUntilTrue(t, retryUntilFunc)
	assert.True(t, node1.checkCount > 0)
	assert.True(t, node1.sendCount > 0)

	// Stop node 1
	node1.Stop()

	// Wait for another node to detect node1 has stopped sending heartbeats
	retryUntilFunc = func() bool {
		nodeSet, err := listener2.GetNodes()
		if err != nil {
			log.Printf("getNodes error: %v", err)
			return false
		}
		return len(nodeSet) < 3
	}
	testRetryUntilTrue(t, retryUntilFunc)

	// Validate current node list
	activeNodes, err := listener2.GetNodes()
	require.NoError(t, err, "Error getting node list")
	require.Len(t, activeNodes, 2)
	assert.NotContains(t, activeNodes, "node1")
	assert.Contains(t, activeNodes, "node2")
	assert.Contains(t, activeNodes, "node3")

	// Stop heartbeaters
	node2.Stop()
	node3.Stop()
}
