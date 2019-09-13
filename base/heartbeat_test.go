package base

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type TestHeartbeatStoppedHandler struct {
	handlerID        string
	staleDetectCount int
}

func (th *TestHeartbeatStoppedHandler) StaleHeartBeatDetected(nodeUuid string) {
	log.Printf("Handler %s detected stale heartbeat for %v, will be removed", th.handlerID, nodeUuid)
	th.staleDetectCount++
}

// TestNewCouchbaseHeartbeater simulates three nodes.  The minimum time window for failed node
// detection is 2 seconds, based on Couchbase Server's minimum document expiry TTL of
// one second, so retry polling is required.
func TestCouchbaseHeartbeaters(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus - no expiry, required for heartbeats")
	}

	if testing.Short() {
		t.Skip("Skipping heartbeattest in short mode")
	}

	keyprefix := "hbtest"

	heartbeaters := []struct {
		name                      string
		nodeSetHandlerConstructor func(bucket Bucket) HeartbeatNodeSetHandler
	}{
		{
			"documentBackedNodeListHandler",
			func(bucket Bucket) HeartbeatNodeSetHandler {
				handler, _ := NewDocumentBackedNodeListHandler(bucket, keyprefix)
				return handler
			},
		},
		{
			"viewBackedNodeListHandler",
			func(bucket Bucket) HeartbeatNodeSetHandler {
				handler, _ := NewViewBackedNodeListHandler(bucket, keyprefix)
				return handler
			},
		},
	}
	for _, h := range heartbeaters {
		t.Run(h.name, func(tt *testing.T) {
			testBucket := GetTestBucket(t)
			defer testBucket.Close()

			// Create three heartbeaters (representing three nodes)
			handler1 := h.nodeSetHandlerConstructor(testBucket)
			assert.NotNil(tt, handler1)
			node1, err := NewCouchbaseHeartbeater(testBucket, keyprefix, "node1", handler1)
			assert.NoError(tt, err)
			handler2 := h.nodeSetHandlerConstructor(testBucket)
			assert.NotNil(tt, handler2)
			node2, err := NewCouchbaseHeartbeater(testBucket, keyprefix, "node2", handler2)
			assert.NoError(tt, err)
			handler3 := h.nodeSetHandlerConstructor(testBucket)
			assert.NotNil(tt, handler3)
			node3, err := NewCouchbaseHeartbeater(testBucket, keyprefix, "node3", handler3)
			assert.NoError(tt, err)

			assert.NoError(t, node1.StartSendingHeartbeats(1))
			assert.NoError(t, node2.StartSendingHeartbeats(1))
			assert.NoError(t, node3.StartSendingHeartbeats(1))

			heartbeatStoppedHandler1 := &TestHeartbeatStoppedHandler{handlerID: "handler1"}
			heartbeatStoppedHandler2 := &TestHeartbeatStoppedHandler{handlerID: "handler2"}
			heartbeatStoppedHandler3 := &TestHeartbeatStoppedHandler{handlerID: "handler3"}

			assert.NoError(t, node1.StartCheckingHeartbeats(1000, heartbeatStoppedHandler1))
			assert.NoError(t, node2.StartCheckingHeartbeats(1000, heartbeatStoppedHandler2))
			assert.NoError(t, node3.StartCheckingHeartbeats(1000, heartbeatStoppedHandler3))

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
				return heartbeatStoppedHandler2.staleDetectCount >= 1 || heartbeatStoppedHandler3.staleDetectCount >= 1
			}
			testRetryUntilTrue(t, retryUntilFunc)

			// Validate that at least one node detected the stopped node 1
			assert.True(t, heartbeatStoppedHandler2.staleDetectCount >= 1 || heartbeatStoppedHandler3.staleDetectCount >= 1,
				fmt.Sprintf("Expected stale detection counts (1) not found in either handler2 (%d) or handler3 (%d)", heartbeatStoppedHandler2.staleDetectCount, heartbeatStoppedHandler3.staleDetectCount))

			// Validate current node list
			activeNodes, err := node2.GetNodeList()
			require.NoError(t, err, "Error getting node list")
			assert.Equal(t, 2, len(activeNodes))
			assert.NotContains(t, activeNodes, "node1")
			assert.Contains(t, activeNodes, "node2")
			assert.Contains(t, activeNodes, "node3")

			// Stop heartbeaters
			node2.Stop()
			node3.Stop()
		})
	}

}
