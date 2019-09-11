package base

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
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
func TestNewCouchbaseHeartbeater(t *testing.T) {

	if UnitTestUrlIsWalrus() {
		t.Skip("This test won't work under walrus - no expiry, required for heartbeats")
	}

	if testing.Short() {
		t.Skip("Skipping heartbeattest in short mode")
	}

	testBucket := GetTestBucket(t)
	defer testBucket.Close()

	// Create three heartbeaters (representing three nodes)
	node1, err := NewCouchbaseHeartbeater(testBucket, "hbtest", "node1")
	assert.NoError(t, err)
	node2, err := NewCouchbaseHeartbeater(testBucket, "hbtest", "node2")
	assert.NoError(t, err)
	node3, err := NewCouchbaseHeartbeater(testBucket, "hbtest", "node3")
	assert.NoError(t, err)

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

	// Wait for other nodes to detect node1 has stopped sending heartbeats
	retryUntilFunc = func() bool {
		return heartbeatStoppedHandler2.staleDetectCount >= 1 && heartbeatStoppedHandler3.staleDetectCount >= 1
	}
	testRetryUntilTrue(t, retryUntilFunc)

	// Validate that both active nodes were notified of removal exactly once
	assert.Equal(t, 1, heartbeatStoppedHandler2.staleDetectCount)
	assert.Equal(t, 1, heartbeatStoppedHandler2.staleDetectCount)

	// Stop heartbeaters
	node2.Stop()
	node3.Stop()

}
