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
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultHeartbeatSendInterval  = 1 * time.Second
	defaultHeartbeatExpirySeconds = 10
	defaultHeartbeatPollInterval  = 2 * time.Second
)

// Heartbeater defines the interface for heartbeat management
type Heartbeater interface {
	RegisterListener(listener HeartbeatListener) error
	UnregisterListener(name string)
	Start() error
	StartSendingHeartbeats() error
	StartCheckingHeartbeats() error
	Stop()
}

// A HeartbeatListener defines the set of nodes it wants to monitor, and a callback when one of those nodes stops
// sending heartbeats.
type HeartbeatListener interface {
	Name() string
	GetNodes() (nodeUUIDs []string, err error)
	StaleHeartbeatDetected(nodeUUID string)
	Stop()
}

// couchbaseHeartBeater is a Heartbeater implementation that uses Couchbase document expiry for heartbeat detection.
// Each active node maintains a heartbeat document with expiry = heartbeatExpirySeconds, and performs a touch to refresh
// the expiry on that document every heartbeatSendInterval.  Heartbeater polls for existence of other nodes' heartbeats
// every heartbeatPollInterval.  The set of nodes to poll is the union of nodes returned by GetNodes call on all
// heartbeatListeners.
//
// The default timing intervals are defined to balance the following:
//
//    Network latency tolerance = heartbeatExpirySeconds - heartbeatSendInterval   (default = 10-1= 9s)
//       Network latency tolerance is the minimum amount of time before this node may be flagged as offline
//       by another node. Must be large enough to avoid triggering false positives during network load spikes on this node.
//
//    Rebalance latency = heartbeatExpirySeconds + heartbeatPollInterval   (default = 10+2 = 12s)
//       The maximum amount of time between a node going offline, and rebalance being triggered for that node.
//
//    Heartbeat ops/second/cluster = n/heartbeatSendInterval + (n^2)/heartbeatPollInterval (default = n + (n^2)/2)
//       Number of heartbeat ops/second for a cluster of n nodes - one heartbeat touch per node per heartbeatSendInterval,
//       n heartbeat reads per node per heartbeatPollInterval
//       e.g  Default for a 4 node cluster: 12 ops/second
type couchbaseHeartBeater struct {
	bucket                  Bucket
	nodeUUID                string
	keyPrefix               string
	groupID                 string
	heartbeatSendInterval   time.Duration                // Heartbeat send interval
	heartbeatExpirySeconds  uint32                       // Heartbeat expiry time (seconds)
	heartbeatPollInterval   time.Duration                // Frequency of polling for other nodes' heartbeat documents
	terminator              chan struct{}                // terminator for send and check goroutines
	heartbeatListeners      map[string]HeartbeatListener // Handlers to be notified when dropped nodes are detected
	heartbeatListenersMutex sync.RWMutex                 // mutex for heartbeatsStoppedHandlers
	sendCount               int                          // Monitoring stat - number of heartbeats sent
	checkCount              int                          // Monitoring stat - number of checks issued
	sendActive              AtomicBool                   // Monitoring state of send goroutine
	checkActive             AtomicBool                   // Monitoring state of check goroutine
}

// Create a new CouchbaseHeartbeater, passing in an authenticated bucket connection,
// the keyPrefix which will be prepended to the heartbeat doc keys,
// and the nodeUUID, which is an opaque identifier for the "thing" that is using this
// library.  nodeUUID will be passed to listeners on stale node detection.
func NewCouchbaseHeartbeater(bucket Bucket, keyPrefix, nodeUUID string) (heartbeater *couchbaseHeartBeater, err error) {

	heartbeater = &couchbaseHeartBeater{
		bucket:                 bucket,
		keyPrefix:              keyPrefix,
		nodeUUID:               nodeUUID,
		terminator:             make(chan struct{}),
		heartbeatListeners:     make(map[string]HeartbeatListener),
		heartbeatSendInterval:  defaultHeartbeatSendInterval,
		heartbeatExpirySeconds: defaultHeartbeatExpirySeconds,
		heartbeatPollInterval:  defaultHeartbeatPollInterval,
	}

	return heartbeater, err

}

// Start the heartbeater.  Underlying methods performs the first heartbeat send and check synchronously, then
// starts scheduled goroutines for ongoing processing.
func (h *couchbaseHeartBeater) Start() error {

	if err := h.StartSendingHeartbeats(); err != nil {
		return err
	}

	if err := h.StartCheckingHeartbeats(); err != nil {
		return err
	}

	DebugfCtx(context.TODO(), KeyCluster, "Sending node heartbeats at interval: %v", h.heartbeatSendInterval)

	return nil

}

// Stop terminates the send and check goroutines, and blocks for up to 1s
// until goroutines are actually terminated.
func (h *couchbaseHeartBeater) Stop() {

	if h == nil {
		return
	}
	// Stop send and check goroutines
	close(h.terminator)

	maxWaitTimeMs := 1000
	waitTimeMs := 0
	for h.sendActive.IsTrue() || h.checkActive.IsTrue() {
		waitTimeMs += 10
		if waitTimeMs > maxWaitTimeMs {
			WarnfCtx(context.Background(), "couchbaseHeartBeater didn't complete Stop() within expected elapsed time")
			return
		}
		time.Sleep(10 * time.Millisecond)

	}
}

// Send initial heartbeat, and start goroutine to schedule sendHeartbeat invocation
func (h *couchbaseHeartBeater) StartSendingHeartbeats() error {
	if err := h.sendHeartbeat(); err != nil {
		return err
	}

	ticker := time.NewTicker(h.heartbeatSendInterval)
	go func() {
		defer FatalPanicHandler()
		defer func() {
			h.sendActive.Set(false)
		}()
		for {
			select {
			case <-h.terminator:
				ticker.Stop()
				return
			case <-ticker.C:
				if err := h.sendHeartbeat(); err != nil {
					WarnfCtx(context.Background(), "Unexpected error sending heartbeat - will be retried: %v", err)
				}
			}
		}
	}()
	h.sendActive.Set(true)
	return nil
}

// Perform initial heartbeat check, then start goroutine to schedule check for stale heartbeats
func (h *couchbaseHeartBeater) StartCheckingHeartbeats() error {

	if err := h.checkStaleHeartbeats(); err != nil {
		WarnfCtx(context.Background(), "Error checking for stale heartbeats: %v", err)
	}

	ticker := time.NewTicker(h.heartbeatPollInterval)
	go func() {
		defer FatalPanicHandler()
		defer func() { h.checkActive.Set(false) }()
		for {
			select {
			case <-h.terminator:
				ticker.Stop()
				return
			case <-ticker.C:
				if err := h.checkStaleHeartbeats(); err != nil {
					WarnfCtx(context.Background(), "Error checking for stale heartbeats: %v", err)
				}
			}
		}
	}()
	h.checkActive.Set(true)
	return nil

}

// Register a new HeartbeatListener.  Listeners must be registered after the heartbeater has been started,
// to avoid the situation where a new node triggers immediate removal/rebalance because it hasn't started sending
// heartbeats yet
func (h *couchbaseHeartBeater) RegisterListener(handler HeartbeatListener) error {

	if !h.sendActive.IsTrue() {
		return errors.New("Heartbeater must be started before registering listeners, to avoid node removal")
	}

	h.heartbeatListenersMutex.Lock()
	defer h.heartbeatListenersMutex.Unlock()
	_, exists := h.heartbeatListeners[handler.Name()]
	if exists {
		return ErrAlreadyExists
	}
	h.heartbeatListeners[handler.Name()] = handler
	return nil
}

// Unregister a HeartbeatListener, if a matching listener is found
func (h *couchbaseHeartBeater) UnregisterListener(handlerName string) {
	h.heartbeatListenersMutex.Lock()
	defer h.heartbeatListenersMutex.Unlock()
	_, exists := h.heartbeatListeners[handlerName]
	if !exists {
		return
	}
	delete(h.heartbeatListeners, handlerName)
}

type ListenerMap map[string][]HeartbeatListener

// Custom string format for ListenerMap logging to only print map keys
// as slice when logging ListenerMap contents
func (l ListenerMap) String() string {
	if len(l) == 0 {
		return "[]"
	}
	keys := make([]string, 0, len(l))
	for k, _ := range l {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return fmt.Sprintf("%v", keys)
}

// getAllNodes returns all nodes from all registered listeners as a map from nodeUUID to the listeners
// registered for that node
func (h *couchbaseHeartBeater) getNodeListenerMap() ListenerMap {
	nodeToListenerMap := make(ListenerMap)
	h.heartbeatListenersMutex.RLock()
	for _, listener := range h.heartbeatListeners {
		listenerNodes, err := listener.GetNodes()
		if err != nil {
			WarnfCtx(context.Background(), "Error obtaining node set for listener %s - will be omitted for this heartbeat iteration.  Error: %v", listener.Name(), err)
		}
		for _, nodeUUID := range listenerNodes {
			_, ok := nodeToListenerMap[nodeUUID]
			if !ok {
				nodeToListenerMap[nodeUUID] = make([]HeartbeatListener, 0)
			}
			nodeToListenerMap[nodeUUID] = append(nodeToListenerMap[nodeUUID], listener)
		}
	}
	h.heartbeatListenersMutex.RUnlock()
	return nodeToListenerMap
}

func (h *couchbaseHeartBeater) checkStaleHeartbeats() error {

	// Build set of all nodes
	nodeListenerMap := h.getNodeListenerMap()
	TracefCtx(context.Background(), KeyCluster, "Checking heartbeats for node set: %s", nodeListenerMap)

	for heartbeatNodeUUID, listeners := range nodeListenerMap {
		if heartbeatNodeUUID == h.nodeUUID {
			// that's us, and we don't care about ourselves
			continue
		}
		if heartbeatNodeUUID == "" {
			continue
		}

		timeoutDocID := heartbeatTimeoutDocID(heartbeatNodeUUID, h.keyPrefix)
		_, _, err := h.bucket.GetRaw(timeoutDocID)
		if err != nil {
			if !IsKeyNotFoundError(h.bucket, err) {
				// unexpected error
				return err
			}

			// doc not found, which means the heartbeat doc expired.
			// Notify listeners for this node
			for _, listener := range listeners {
				listener.StaleHeartbeatDetected(heartbeatNodeUUID)
			}
		}
	}
	h.checkCount++
	return nil
}

func heartbeatTimeoutDocID(nodeUuid, keyPrefix string) string {
	return keyPrefix + "heartbeat_timeout:" + nodeUuid
}

func (h *couchbaseHeartBeater) sendHeartbeat() error {

	docID := heartbeatTimeoutDocID(h.nodeUUID, h.keyPrefix)

	_, touchErr := h.bucket.Touch(docID, h.heartbeatExpirySeconds)
	if touchErr == nil {
		h.sendCount++
		return nil
	}

	// On KeyNotFound, recreate heartbeat timeout doc
	if IsKeyNotFoundError(h.bucket, touchErr) {
		heartbeatDocBody := []byte(h.nodeUUID)
		setErr := h.bucket.SetRaw(docID, h.heartbeatExpirySeconds, nil, heartbeatDocBody)
		if setErr != nil {
			return setErr
		}
		h.sendCount++
		return nil
	} else {
		return touchErr
	}
}

// Accessors to modify heartbeatSendInterval, heartbeatPollInterval, heartbeatExpirySeconds must be invoked prior to Start().
// No consistency checking is done across values, callers that don't use default values must validate that their
// combination is valid (e.g. sendInterval is more frequent than expiry)
func (h *couchbaseHeartBeater) SetSendInterval(duration time.Duration) error {
	if h.sendActive.IsTrue() || h.checkActive.IsTrue() {
		return errors.New("Cannot modify send interval while heartbeater is running - must be set prior to calling Start()")
	}
	h.heartbeatSendInterval = duration
	return nil
}

func (h *couchbaseHeartBeater) SetPollInterval(duration time.Duration) error {
	if h.sendActive.IsTrue() || h.checkActive.IsTrue() {
		return errors.New("Cannot modify polling interval while heartbeater is running - must be set prior to calling Start()")
	}
	h.heartbeatPollInterval = duration
	return nil
}

func (h *couchbaseHeartBeater) SetExpirySeconds(expiry uint32) error {
	if h.sendActive.IsTrue() || h.checkActive.IsTrue() {
		return errors.New("Cannot modify heartbeat expiry value while heartbeater is running - must be set prior to calling Start()")
	}
	h.heartbeatExpirySeconds = expiry
	return nil
}

// documentBackedListener stores set of nodes in a single node list document.  On stale notification,
// removes node from the list.  Primarily intended for test usage.
type documentBackedListener struct {
	nodeListKey            string     // key for the tracking document
	bucket                 Bucket     // bucket used for document storage
	nodeIDs                []string   // Set of nodes from the latest retrieval
	cas                    uint64     // CAS from latest retrieval of tracking document
	lock                   sync.Mutex // lock for nodes access
	staleNotificationCount uint64     // stats - counter for stale heartbeat notifications
}

func NewDocumentBackedListener(bucket Bucket, keyPrefix string) (*documentBackedListener, error) {

	handler := &documentBackedListener{
		nodeListKey: keyPrefix + ":HeartbeatNodeList",
		bucket:      bucket,
	}
	return handler, nil
}

func (dh *documentBackedListener) Name() string {
	return dh.nodeListKey
}

func (dh *documentBackedListener) GetNodes() ([]string, error) {
	dh.lock.Lock()
	err := dh.loadNodeIDs()
	dh.lock.Unlock()
	return dh.nodeIDs, err
}

func (dh *documentBackedListener) Stop() {
	return
}

func (dh *documentBackedListener) StaleHeartbeatDetected(nodeUUID string) {
	_ = dh.RemoveNode(nodeUUID)
	atomic.AddUint64(&dh.staleNotificationCount, 1)
}

func (dh *documentBackedListener) StaleNotificationCount() uint64 {
	return atomic.LoadUint64(&dh.staleNotificationCount)
}

// Adds the node to the tracking document
func (dh *documentBackedListener) AddNode(nodeID string) error {
	return dh.updateNodeList(nodeID, false)
}

// Removes the node to the tracking document
func (dh *documentBackedListener) RemoveNode(nodeID string) error {
	return dh.updateNodeList(nodeID, true)
}

// Adds or removes a nodeID from the node list document
func (dh *documentBackedListener) updateNodeList(nodeID string, remove bool) error {

	dh.lock.Lock()
	defer dh.lock.Unlock()

	// Retry loop handles CAS failure
	for {
		// Reload the node set if it hasn't been initialized (or has been marked out of date by previous CAS write failure)
		if dh.cas == 0 {
			if err := dh.loadNodeIDs(); err != nil {
				return err
			}
		}

		// Check whether nodeID already exists in the set
		nodeIndex := -1
		for index, existingNodeID := range dh.nodeIDs {
			if existingNodeID == nodeID {
				nodeIndex = index
				break
			}
		}

		if remove { // RemoveNode handling
			if nodeIndex == -1 {
				return nil // NodeID isn't part of set, doesn't need to be removed
			}
			dh.nodeIDs = append(dh.nodeIDs[:nodeIndex], dh.nodeIDs[nodeIndex+1:]...)
		} else { // AddNode handling
			if nodeIndex > -1 {
				return nil // NodeID is already part of set, doesn't need to be added
			}
			dh.nodeIDs = append(dh.nodeIDs, nodeID)
		}

		InfofCtx(context.TODO(), KeyCluster, "Updating nodeList document (%s) with node IDs: %v", dh.nodeListKey, dh.nodeIDs)

		casOut, err := dh.bucket.WriteCas(dh.nodeListKey, 0, 0, dh.cas, dh.nodeIDs, 0)

		if err == nil { // Successful update
			dh.cas = casOut
			return nil
		}

		if !IsCasMismatch(err) { // Unexpected error
			return err
		}

		// CAS mismatch - reset cas to trigger reload and try again
		dh.cas = 0
	}

}

func (dh *documentBackedListener) loadNodeIDs() error {

	docBytes, cas, err := dh.bucket.GetRaw(dh.nodeListKey)
	if err != nil {
		dh.cas = 0
		dh.nodeIDs = []string{}
		if !IsKeyNotFoundError(dh.bucket, err) {
			return err
		}
	}

	if cas == dh.cas {
		// node list hasn't changed since the last load
		return nil
	}

	// Update the in-memory list and cas
	if unmarshalErr := JSONUnmarshal(docBytes, &dh.nodeIDs); unmarshalErr != nil {
		return unmarshalErr
	}
	dh.cas = cas

	return nil

}
