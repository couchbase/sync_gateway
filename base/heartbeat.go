package base

import (
	"fmt"
	"time"

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
func NewCouchbaseHeartbeater(bucket Bucket, keyPrefix, nodeUuid string) (*couchbaseHeartBeater, error) {

	heartbeater := &couchbaseHeartBeater{
		bucket:               bucket,
		nodeUuid:             nodeUuid,
		keyPrefix:            keyPrefix,
		heartbeatSendCloser:  make(chan struct{}),
		heartbeatCheckCloser: make(chan struct{}),
	}

	return heartbeater, nil

}

// Kick off the heartbeat sender with the given interval, in milliseconds.
// This method will BLOCK until the first heartbeat is sent, and the rest
// will happen asynchronously.
func (h *couchbaseHeartBeater) StartSendingHeartbeats(intervalSeconds int) error {

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

	if err := h.addHeartbeatCheckView(); err != nil {
		return err
	}

	ticker := time.NewTicker(time.Duration(staleThresholdMs) * time.Millisecond)

	go func() {
		defer func() { h.checkActive = false }()
		h.checkActive = true
		for {
			select {
			case _ = <-h.heartbeatCheckCloser:
				ticker.Stop()
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

func (h *couchbaseHeartBeater) checkStaleHeartbeats(staleThresholdMs int, handler HeartbeatsStoppedHandler) error {

	// query view to get all heartbeat docs
	heartbeatDocs, err := h.viewQueryHeartbeatDocs()
	if err != nil {
		return err
	}

	for _, heartbeatDoc := range heartbeatDocs {
		if heartbeatDoc.NodeUUID == h.nodeUuid {
			// that's us, and we don't care about ourselves
			continue
		}
		if heartbeatDoc.NodeUUID == "" {
			continue
		}

		timeoutDocId := h.heartbeatTimeoutDocId(heartbeatDoc.NodeUUID)
		heartbeatTimeoutDoc := heartbeatTimeout{}
		_, err := h.bucket.Get(timeoutDocId, &heartbeatTimeoutDoc)
		if err != nil {
			if !IsKeyNotFoundError(h.bucket, err) {
				// unexpected error
				return err
			}

			// doc not found, which means the heartbeat doc expired.
			// call back the handler.
			handler.StaleHeartBeatDetected(heartbeatDoc.NodeUUID)

			// delete the heartbeat doc itself so we don't have unwanted
			// repeated callbacks to the stale heartbeat handler
			docId := h.heartbeatDocId(heartbeatDoc.NodeUUID)
			if err := h.bucket.Delete(docId); err != nil {
				Infof(KeyImport, "Failed to delete heartbeat doc: %v err: %v", docId, err)
			}

		}

	}
	h.checkCount++
	return nil
}

func (h *couchbaseHeartBeater) heartbeatTimeoutDocId(nodeUuid string) string {
	return fmt.Sprintf("%vheartbeat_timeout:%v", h.keyPrefix, nodeUuid)
}

func (h *couchbaseHeartBeater) heartbeatDocId(nodeUuid string) string {
	return fmt.Sprintf("%vheartbeat:%v", h.keyPrefix, nodeUuid)
}

func (h *couchbaseHeartBeater) viewQueryHeartbeatDocs() ([]heartbeatMeta, error) {

	viewRes := struct {
		Rows []struct {
			Id    string
			Value string
		}
		Errors []sgbucket.ViewError
	}{}

	err := h.bucket.ViewCustom("cbgt", "heartbeats",
		map[string]interface{}{
			"stale": false,
		}, &viewRes)
	if err != nil {
		return nil, err
	}

	heartbeats := []heartbeatMeta{}
	for _, row := range viewRes.Rows {
		heartbeat := heartbeatMeta{
			Type:     docTypeHeartbeat,
			NodeUUID: row.Value,
		}
		heartbeats = append(heartbeats, heartbeat)
	}

	return heartbeats, nil

}

func (h *couchbaseHeartBeater) sendHeartbeat(intervalSeconds int) error {

	if err := h.upsertHeartbeatDoc(); err != nil {
		return err
	}
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
	docId := h.heartbeatDocId(h.nodeUuid)

	if err := h.bucket.Set(docId, 0, heartbeatDoc); err != nil {
		return err
	}
	return nil

}

func (h *couchbaseHeartBeater) upsertHeartbeatTimeoutDoc(intervalSeconds int) error {

	heartbeatTimeoutDoc := heartbeatTimeout{
		Type:     docTypeHeartbeatTimeout,
		NodeUUID: h.nodeUuid,
	}

	docId := h.heartbeatTimeoutDocId(h.nodeUuid)

	// make the expire time double the interval time, to ensure there is
	// always a heartbeat timeout document present under normal operation
	expireTimeSeconds := intervalSeconds * 2

	if err := h.bucket.Set(docId, uint32(expireTimeSeconds), heartbeatTimeoutDoc); err != nil {
		return err
	}
	return nil

}

func (h *couchbaseHeartBeater) addHeartbeatCheckView() error {

	heartbeatsMap := `function (doc, meta) { if (doc.type == 'heartbeat') { emit(meta.id, doc.node_uuid); }}`

	designDoc := sgbucket.DesignDoc{
		Views: sgbucket.ViewMap{
			"heartbeats": sgbucket.ViewDef{Map: heartbeatsMap},
		},
	}

	return h.bucket.PutDDoc("cbgt", designDoc)
}
