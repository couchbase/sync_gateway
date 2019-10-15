package base

import (
	"errors"
	"sync"
	"time"

	"github.com/couchbase/cbgt"

	sgbucket "github.com/couchbase/sg-bucket"
)

const (
	docTypeHeartbeat        = "heartbeat"
	docTypeHeartbeatTimeout = "heartbeat_timeout"
)

// A Heartbeater is something that can both send and check for heartbeats that
// are stored as documents in a Couchbase bucket
type Heartbeater interface {
	HeartbeatChecker
	HeartbeatSender
}

// A HeartbeatChecker checks _other_ nodes in the cluster for stale heartbeats
// and reacts by calling back the HeartbeatsStoppedHandler
type HeartbeatChecker interface {
	StartCheckingHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error
	StopCheckingHeartbeats()
	GetNodeList() []string
}

// A HeartbeatSender sends heartbeats
type HeartbeatSender interface {
	StartSendingHeartbeats(intervalSeconds int) error
	StopSendingHeartbeats()
}

// This is the callback interface that clients of this library
// need to pass in to be notified when other nodes have appeared to have
// stopped sending heartbeats.
type HeartbeatsStoppedHandler interface {
	StaleHeartBeatDetected(nodeUuid string)
}

// HeartbeatNodeSetHandler defines the interface to manage the list of nodes
// participating in heartbeat processing.  This list is used by CouchbaseHeartbeater
// instances to determine which heartbeat docs are monitored.
// CouchbaseHeartbeater has an internal implementation (DocumentBackedNodeListHandler), but
// accepts custom implementations.
type HeartbeatNodeSetHandler interface {
	AddNode(nodeID string) error
	RemoveNode(nodeID string) error
	GetNodes() ([]string, error)
	Stop()
}

type heartbeatMeta struct {
	Type     string `json:"type"`
	NodeUUID string `json:"node_uuid"`
}

type heartbeatTimeout struct {
	Type     string `json:"type"`
	NodeUUID string `json:"node_uuid"`
}

type couchbaseHeartBeater struct {
	bucket               Bucket
	nodeUuid             string
	heartbeatHandler     HeartbeatNodeSetHandler
	keyPrefix            string
	heartbeatSendCloser  chan struct{} // break out of heartbeat sender goroutine
	heartbeatCheckCloser chan struct{} // break out of heartbeat checker goroutine
	sendCount            int           // Monitoring stat - number of heartbeats sent
	checkCount           int           // Monitoring stat - number of checks issued
	sendActive           bool          // Monitoring state of send goroutine
	checkActive          bool          // Monitoring state of check goroutine
}

// Create a new CouchbaseHeartbeater, passing in an authenticated bucket connection,
// the keyPrefix which will be prepended to the heartbeat doc keys,
// and the nodeUuid, which is an opaque identifier for the "thing" that is using this
// library.  You can think of nodeUuid as a generic token, so put whatever you want there
// as long as it is unique to the node where this is running.  (eg, an ip address could work)
func NewCouchbaseHeartbeater(bucket Bucket, keyPrefix, nodeUuid string, handler HeartbeatNodeSetHandler) (heartbeater *couchbaseHeartBeater, err error) {

	heartbeater = &couchbaseHeartBeater{
		bucket:               bucket,
		nodeUuid:             nodeUuid,
		heartbeatHandler:     handler,
		keyPrefix:            keyPrefix,
		heartbeatSendCloser:  make(chan struct{}),
		heartbeatCheckCloser: make(chan struct{}),
	}

	// If custom handler not specified, default to document-based handler
	if handler == nil {
		heartbeater.heartbeatHandler, err = NewDocumentBackedNodeListHandler(bucket, keyPrefix)
	}

	return heartbeater, err

}

// Kick off the heartbeat sender with the given interval, in milliseconds.
// This method will BLOCK until the first heartbeat is sent, and the rest
// will happen asynchronously.
func (h *couchbaseHeartBeater) StartSendingHeartbeats(intervalSeconds int) error {

	err := h.heartbeatHandler.AddNode(h.nodeUuid)
	if err != nil {
		return err
	}

	// send the first heartbeat in the current goroutine and return
	// an error if it fails
	if err := h.sendHeartbeat(intervalSeconds); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Duration(intervalSeconds) * time.Second)

	go func() {
		defer func() { h.sendActive = false }()
		h.sendActive = true
		for {
			select {
			case _ = <-h.heartbeatSendCloser:
				ticker.Stop()
				return
			case <-ticker.C:
				if err := h.sendHeartbeat(intervalSeconds); err != nil {
				}
			}
		}
	}()
	return nil

}

// Stop terminates the send and check goroutines, and blocks for up to 1s
// until goroutines are actually terminated
func (h *couchbaseHeartBeater) Stop() {
	h.StopSendingHeartbeats()
	h.StopCheckingHeartbeats()

	maxWaitTimeMs := 1000
	waitTimeMs := 0
	for h.sendActive || h.checkActive {
		waitTimeMs += 10
		if waitTimeMs > maxWaitTimeMs {
			Warnf(KeyImport, "couchbaseHeartBeater didn't complete Stop() within expected elapsed time")
			return
		}
		time.Sleep(10 * time.Millisecond)

	}
}

// Stop sending heartbeats
func (h *couchbaseHeartBeater) StopSendingHeartbeats() {
	close(h.heartbeatSendCloser)
}

// Kick off the heartbeat checker and pass in the amount of time in milliseconds before
// a node has been considered to stop sending heartbeats.  Also pass in the handler which
// will be called back in that case (and passed the opaque node uuid)
func (h *couchbaseHeartBeater) StartCheckingHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error {

	ticker := time.NewTicker(time.Duration(staleThresholdMs) * time.Millisecond)

	go func() {
		defer func() { h.checkActive = false }()
		h.checkActive = true
		for {
			select {
			case _ = <-h.heartbeatCheckCloser:
				ticker.Stop()
				h.heartbeatHandler.Stop()
				return
			case <-ticker.C:
				if err := h.checkStaleHeartbeats(staleThresholdMs, handler); err != nil {
					Warnf(KeyImport, "Error checking for stale heartbeats: %v", err)
				}
			}
		}
	}()
	return nil

}

// Stop the heartbeat checker
func (h *couchbaseHeartBeater) StopCheckingHeartbeats() {
	close(h.heartbeatCheckCloser)
}

func (h *couchbaseHeartBeater) GetNodeList() ([]string, error) {
	return h.heartbeatHandler.GetNodes()
}

func (h *couchbaseHeartBeater) checkStaleHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error {

	// Get node set
	heartbeatNodes, err := h.heartbeatHandler.GetNodes()
	if err != nil {
		return err
	}
	Debugf(KeyDCP, "Checking heartbeats for nodes: %v", heartbeatNodes)

	for _, heartbeatNodeUUID := range heartbeatNodes {
		if heartbeatNodeUUID == h.nodeUuid {
			// that's us, and we don't care about ourselves
			continue
		}
		if heartbeatNodeUUID == "" {
			continue
		}

		timeoutDocId := heartbeatTimeoutDocId(heartbeatNodeUUID, h.keyPrefix)
		_, _, err := h.bucket.GetRaw(timeoutDocId)
		if err != nil {
			if !IsKeyNotFoundError(h.bucket, err) {
				// unexpected error
				return err
			}

			// doc not found, which means the heartbeat doc expired.
			// call back the handler.
			handler.StaleHeartBeatDetected(heartbeatNodeUUID)

			// delete the heartbeat doc itself so we don't have unwanted
			// repeated callbacks to the stale heartbeat handler
			err := h.heartbeatHandler.RemoveNode(heartbeatNodeUUID)
			if err != nil {
				Infof(KeyImport, "Failed to remove node for node ID:%v err: %v", heartbeatNodeUUID, err)
			}
		}

	}
	h.checkCount++
	return nil
}

func heartbeatTimeoutDocId(nodeUuid, keyPrefix string) string {
	return keyPrefix + "heartbeat_timeout:" + nodeUuid
}

func heartbeatDocId(nodeUuid, keyPrefix string) string {
	return keyPrefix + "heartbeat:" + nodeUuid
}

func (h *couchbaseHeartBeater) sendHeartbeat(intervalSeconds int) error {

	if err := h.upsertHeartbeatTimeoutDoc(intervalSeconds); err != nil {
		return err
	}
	h.sendCount++
	return nil
}

func (h *couchbaseHeartBeater) upsertHeartbeatDoc() error {

	heartbeatDoc := heartbeatMeta{
		Type:     docTypeHeartbeat,
		NodeUUID: h.nodeUuid,
	}
	docId := heartbeatDocId(h.nodeUuid, h.keyPrefix)

	if err := h.bucket.Set(docId, 0, heartbeatDoc); err != nil {
		return err
	}
	return nil

}

func (h *couchbaseHeartBeater) upsertHeartbeatTimeoutDoc(intervalSeconds int) error {

	docId := heartbeatTimeoutDocId(h.nodeUuid, h.keyPrefix)

	// make the expire time double the touch interval time, to ensure there is
	// always a heartbeat timeout document present under normal operation
	expireTimeSeconds := intervalSeconds * 2

	_, touchErr := h.bucket.Touch(docId, uint32(expireTimeSeconds))
	if touchErr == nil {
		return nil
	}

	// On KeyNotFound, recreate heartbeat timeout doc
	if IsKeyNotFoundError(h.bucket, touchErr) {

		heartbeatDocBody := []byte(h.nodeUuid)

		setErr := h.bucket.SetRaw(docId, uint32(expireTimeSeconds), heartbeatDocBody)
		if setErr != nil {
			return setErr
		}
		return nil
	} else {
		return touchErr
	}

}

// documentBackedNodeListHandler tracks nodes in a single node list document
type documentBackedNodeListHandler struct {
	nodeListKey string     // key for the tracking document
	bucket      Bucket     // bucket used for document storage
	nodeIDs     []string   // Set of nodes from the latest retrieval
	cas         uint64     // CAS from latest retrieval of tracking document
	lock        sync.Mutex // lock for nodes access
}

func NewDocumentBackedNodeListHandler(bucket Bucket, keyPrefix string) (*documentBackedNodeListHandler, error) {

	handler := &documentBackedNodeListHandler{
		nodeListKey: keyPrefix + "HeartbeatNodeList",
		bucket:      bucket,
	}
	return handler, nil
}

// Adds the node to the tracking document
func (dh *documentBackedNodeListHandler) AddNode(nodeID string) error {
	return dh.updateNodeList(nodeID, false)
}

// Removes the node to the tracking document
func (dh *documentBackedNodeListHandler) RemoveNode(nodeID string) error {
	return dh.updateNodeList(nodeID, true)
}

func (dh *documentBackedNodeListHandler) GetNodes() ([]string, error) {
	dh.lock.Lock()
	err := dh.loadNodeIDs()
	dh.lock.Unlock()
	return dh.nodeIDs, err
}

func (dh *documentBackedNodeListHandler) Stop() {
	return
}

// Adds or removes a nodeID from the node list document
func (dh *documentBackedNodeListHandler) updateNodeList(nodeID string, remove bool) error {

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

func (dh *documentBackedNodeListHandler) loadNodeIDs() error {

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

// viewBackedNodeListHandler tracks nodes as individual documents, and uses a view query to
// identify the full set of documents
// TODO: Currently being used to validate pluggable node handlers.  Can be removed to when this functionality
//       has test coverage with a cbgt-based handler
type viewBackedNodeListHandler struct {
	keyPrefix string
	bucket    Bucket
}

func NewViewBackedNodeListHandler(bucket Bucket, keyPrefix string) (*viewBackedNodeListHandler, error) {

	handler := &viewBackedNodeListHandler{
		keyPrefix: keyPrefix,
		bucket:    bucket,
	}
	err := handler.addHeartbeatCheckView(bucket)
	return handler, err
}

// Writes the heartbeat doc used to register the node
func (vh *viewBackedNodeListHandler) AddNode(nodeID string) error {

	heartbeatDoc := heartbeatMeta{
		Type:     docTypeHeartbeat,
		NodeUUID: nodeID,
	}
	docId := heartbeatDocId(nodeID, vh.keyPrefix)

	if err := vh.bucket.Set(docId, 0, heartbeatDoc); err != nil {
		return err
	}
	return nil

}

// Deletes the heartbeat doc used to register the node
func (vh *viewBackedNodeListHandler) RemoveNode(nodeID string) error {
	docId := heartbeatDocId(nodeID, vh.keyPrefix)
	return vh.bucket.Delete(docId)
}

// Issues a view query to identify the node set
func (vh *viewBackedNodeListHandler) GetNodes() ([]string, error) {
	return vh.viewQueryHeartbeatDocs()
}

func (vh *viewBackedNodeListHandler) addHeartbeatCheckView(bucket Bucket) error {

	heartbeatsMap := `function (doc, meta) { if (doc.type == 'heartbeat') { emit(meta.id, doc.node_uuid); }}`

	designDoc := sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"heartbeats": sgbucket.ViewDef{Map: heartbeatsMap},
		},
	}

	return vh.bucket.PutDDoc("cbgt", designDoc)
}

func (vh *viewBackedNodeListHandler) viewQueryHeartbeatDocs() ([]string, error) {

	viewRes := struct {
		Rows []struct {
			Id    string
			Value string
		}
		Errors []sgbucket.ViewError
	}{}

	err := vh.bucket.ViewCustom("cbgt", "heartbeats",
		map[string]interface{}{
			"stale": false,
		}, &viewRes)
	if err != nil {
		return nil, err
	}

	nodeIDs := []string{}
	for _, row := range viewRes.Rows {
		nodeIDs = append(nodeIDs, row.Value)
	}

	return nodeIDs, nil

}

func (vh *viewBackedNodeListHandler) Stop() {
	return
}

// cbgtNodeListHandler uses cbgt's cfg to manage node list
// TODO: should this build a superset of KNOWN and WANTED?
type cbgtNodeListHandler struct {
	cfg        cbgt.Cfg      // cbgt Config
	terminator chan struct{} // close cfg subscription on close
	nodeIDs    []string      // Set of nodes from the latest retrieval
	lock       sync.RWMutex  // lock for nodeIDs access
}

func NewCBGTNodeListHandler(cfg cbgt.Cfg) (*cbgtNodeListHandler, error) {

	if cfg == nil {
		return nil, errors.New("Manager Cfg must not be nil for CBGTNodeListHandler")
	}

	handler := &cbgtNodeListHandler{
		cfg:        cfg,
		terminator: make(chan struct{}),
	}

	// Initialize the node set
	err := handler.reloadNodes()
	if err != nil {
		return nil, err
	}

	// Subscribe to changes to the known node set key
	err = handler.subscribeNodeChanges()
	if err != nil {
		return nil, err
	}

	return handler, nil
}

// subscribeNodeChanges registers with the manager's cfg implementation for notifications on changes to the
// NODE_DEFS_KNOWN key.  When notified, refreshes the handlers nodeIDs.
func (ch *cbgtNodeListHandler) subscribeNodeChanges() error {

	cfgEvents := make(chan cbgt.CfgEvent)
	ch.cfg.Subscribe(cbgt.CfgNodeDefsKey(cbgt.NODE_DEFS_KNOWN), cfgEvents)
	go func() {
		for {
			select {
			case <-cfgEvents:
				err := ch.reloadNodes()
				if err != nil {
					Warnf(KeyDCP, "Error while reloading heartbeat node definitions: %v", err)
				}
			case <-ch.terminator:
				return
			}
		}
	}()
	return nil
}

func (ch *cbgtNodeListHandler) reloadNodes() error {

	nodeSet, _, err := cbgt.CfgGetNodeDefs(ch.cfg, cbgt.NODE_DEFS_KNOWN)
	if err != nil {
		return err
	}

	nodeUUIDs := make([]string, 0)
	if nodeSet != nil {
		for _, nodeDef := range nodeSet.NodeDefs {
			nodeUUIDs = append(nodeUUIDs, nodeDef.UUID)
		}
	}

	ch.lock.Lock()
	ch.nodeIDs = nodeUUIDs
	ch.lock.Unlock()

	return nil
}

// AddNode is a no-op for cbgtNodeListHandler.  Nodes self-register with the cfg on startup
func (ch *cbgtNodeListHandler) AddNode(nodeID string) error {
	return nil
}

// RemoveNode is a no-op for cbgtNodeListHandler.  cbgt manages removal via the associated HeartbeatsStoppedHandler
func (ch *cbgtNodeListHandler) RemoveNode(nodeID string) error {
	return nil
}

// GetNodes returns a copy of the in-memory node set
func (ch *cbgtNodeListHandler) GetNodes() ([]string, error) {

	ch.lock.RLock()
	nodeIDsCopy := make([]string, len(ch.nodeIDs))
	copy(nodeIDsCopy, ch.nodeIDs)
	ch.lock.RUnlock()
	return nodeIDsCopy, nil
}

func (ch *cbgtNodeListHandler) Stop() {
	close(ch.terminator)
}
