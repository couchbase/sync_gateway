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
	"bytes"
	"context"
	"expvar"
	"fmt"
	"sync"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/google/uuid"
)

// Number of non-checkpoint updates per vbucket required to trigger metadata persistence.  Must be greater than zero to avoid
// retriggering persistence solely based on checkpoint doc echo.
// Based on ad-hoc testing w/ travel-sample bucket, increasing this value doesn't significantly improve performance, since under load
// DCP will already be sending more documents per snapshot.
const kCheckpointThreshold = 1

// Only persist checkpoint once per kCheckpointTimeThreshold (per vbucket)
const kCheckpointTimeThreshold = 1 * time.Minute

// DCP Feed IDs are used to build unique DCP identifiers
const DCPCachingFeedID = "SG"
const DCPImportFeedID = "SGI"

type DCPCommon struct {
	dbStatsExpvars         *expvar.Map
	m                      sync.Mutex
	metaStore              DataStore                      // For metadata persistence/retrieval
	persistCheckpoints     bool                           // Whether this DCPReceiver should persist metadata to the bucket
	seqs                   []uint64                       // To track max seq #'s we received per vbucketId.
	meta                   [][]byte                       // To track metadata blob's per vbucketId.
	updatesSinceCheckpoint []uint64                       // Number of updates since the last checkpoint. Used to avoid checkpoint persistence feedback loop
	lastCheckpointTime     []time.Time                    // Time of last checkpoint persistence, per vbucket.  Used to manage checkpoint persistence volume
	callback               sgbucket.FeedEventCallbackFunc // Function to callback for mutation processing
	loggingCtx             context.Context                // Logging context, prefixes feedID
	checkpointPrefix       string                         // DCP checkpoint key prefix
}

// NewDCPCommon creates a new DCPCommon which manages updates coming from a cbgt-based DCP feed. The callback function will receive events from a DCP feed. It stores checkpoints in the metaStore starting with checkpointPrefix if persistCheckpoints is true.
// Specific stats for DCP are stored in expvars rather than SgwStats.
func NewDCPCommon(
	ctx context.Context,
	callback sgbucket.FeedEventCallbackFunc,
	metaStore DataStore,
	maxVbNo uint16,
	persistCheckpoints bool,
	dbStats *expvar.Map,
	checkpointPrefix string) (*DCPCommon, error) {

	c := &DCPCommon{
		dbStatsExpvars:         dbStats,
		metaStore:              metaStore,
		persistCheckpoints:     persistCheckpoints,
		seqs:                   make([]uint64, maxVbNo),
		meta:                   make([][]byte, maxVbNo),
		updatesSinceCheckpoint: make([]uint64, maxVbNo),
		callback:               callback,
		lastCheckpointTime:     make([]time.Time, maxVbNo),
		checkpointPrefix:       checkpointPrefix,
		loggingCtx:             ctx,
	}

	return c, nil
}

func (c *DCPCommon) dataUpdate(seq uint64, event sgbucket.FeedEvent) {
	shouldPersistCheckpoint := c.callback(event)
	c.updateSeq(event.VbNo, seq, true)
	if c.persistCheckpoints && shouldPersistCheckpoint {
		c.incrementCheckpointCount(event.VbNo)
	}
}

func (c *DCPCommon) snapshotStart(vbNo uint16, snapStart, snapEnd uint64) {
}

// setMetaData and getMetaData may used internally by dcp clients.  Expects send/receive of opaque
// []byte data.  May be invoked from multiple goroutines, so need to manage synchronization.
// Setting mustPersist=true bypasses checkpoint threshold checks and forces persistence as long as
// persistCheckpoints=true
func (c *DCPCommon) setMetaData(vbucketId uint16, value []byte, mustPersist bool) error {

	c.m.Lock()
	defer c.m.Unlock()

	c.meta[vbucketId] = value

	// Check persistMeta to avoids persistence if the only feed events we've seen are the DCP echo of DCP checkpoint docs
	if c.persistCheckpoints && (mustPersist || c.updatesSinceCheckpoint[vbucketId] >= kCheckpointThreshold) {

		// Don't checkpoint more frequently than kCheckpointTimeThreshold
		if !mustPersist && time.Since(c.lastCheckpointTime[vbucketId]) < kCheckpointTimeThreshold {
			return nil
		}

		err := c.persistCheckpoint(vbucketId, value)
		if err != nil {
			WarnfCtx(c.loggingCtx, "Unable to persist DCP metadata - will retry next snapshot. Error: %v", err)
			return fmt.Errorf("Unable to persist DCP metadata")
		}
		c.updatesSinceCheckpoint[vbucketId] = 0
		c.lastCheckpointTime[vbucketId] = time.Now()
	}
	return nil
}

func (c *DCPCommon) getMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {

	c.m.Lock()
	defer c.m.Unlock()

	value = []byte(nil)
	if c.meta != nil {
		value = c.meta[vbucketId]
	}

	if c.seqs != nil {
		lastSeq = c.seqs[vbucketId]
	}

	return value, lastSeq, nil
}

// rollbackEx is called when a DCP open stream issues a rollback. The metadata persisted for a given uuid and sequence number and stream reopening is deferred to cbgt via AutoReconnectAfterRollback feed parameter.
func (c *DCPCommon) rollbackEx(vbucketId uint16, vbucketUUID uint64, rollbackSeq uint64, rollbackMetaData []byte) error {
	InfofCtx(c.loggingCtx, KeyDCP, "DCP RollbackEx request - rolling back DCP feed for: vbucketId: %d, rollbackSeq: %x.", vbucketId, rollbackSeq)
	c.dbStatsExpvars.Add("dcp_rollback_count", 1)
	c.updateSeq(vbucketId, rollbackSeq, false)
	err := c.setMetaData(vbucketId, rollbackMetaData, true)
	if err != nil {
		WarnfCtx(c.loggingCtx, "Error setting metadata during DCP rollback for vBucket %d: %v", vbucketId, err)
	}
	return err
}

func (c *DCPCommon) incrementCheckpointCount(vbucketId uint16) {
	c.m.Lock()
	defer c.m.Unlock()
	c.updatesSinceCheckpoint[vbucketId]++
}

// loadCheckpoint retrieves previously persisted DCP metadata.  Need to unmarshal metadata to determine last sequence processed.
// We always restart the feed from the last persisted snapshot start (as opposed to a sequence we may have processed
// midway through the checkpoint), because:
//   - We don't otherwise persist the last sequence we processed
//   - For SG feed processing, there's no harm if we receive feed events for mutations we've previously seen
//   - The ongoing performance overhead of persisting last sequence outweighs the minor performance benefit of not reprocessing a few
//     sequences in a checkpoint on startup
func (c *DCPCommon) loadCheckpoint(vbNo uint16) (vbMetadata []byte, snapshotStartSeq uint64, snapshotEndSeq uint64, err error) {
	rawValue, _, err := c.metaStore.GetRaw(fmt.Sprintf("%s%d", c.checkpointPrefix, vbNo))
	if err != nil {
		// On a key not found error, metadata hasn't been persisted for this vbucket
		if IsDocNotFoundError(err) {
			return []byte{}, 0, 0, nil
		} else {
			return []byte{}, 0, 0, err
		}
	}

	var snapshotMetadata ShardedImportDCPMetadata
	unmarshalErr := JSONUnmarshal(rawValue, &snapshotMetadata)
	if unmarshalErr != nil {
		return []byte{}, 0, 0, err
	}
	return rawValue, snapshotMetadata.SnapStart, snapshotMetadata.SnapEnd, nil

}

func (c *DCPCommon) InitVbMeta(vbNo uint16) {
	metadata, snapStart, _, err := c.loadCheckpoint(vbNo)
	c.m.Lock()
	if err != nil {
		WarnfCtx(c.loggingCtx, "Unexpected error attempting to load DCP checkpoint for vbucket %d.  Will restart DCP for that vbucket from zero.  Error: %v", vbNo, err)
		c.meta[vbNo] = []byte{}
		c.seqs[vbNo] = 0
	} else {
		c.meta[vbNo] = metadata
		c.seqs[vbNo] = snapStart
	}
	c.m.Unlock()
}

// TODO: Convert checkpoint persistence to an asynchronous batched process, since
//
//	restarting w/ an older checkpoint:
//	  - Would only result in some repeated entry processing, which is already handled by the indexer
//	  - Is a relatively infrequent operation
func (c *DCPCommon) persistCheckpoint(vbNo uint16, value []byte) error {
	TracefCtx(c.loggingCtx, KeyDCP, "Persisting checkpoint for vbno %d", vbNo)
	return c.metaStore.SetRaw(fmt.Sprintf("%s%d", c.checkpointPrefix, vbNo), 0, nil, value)
}

// This updates the value stored in r.seqs with the given seq number for the given partition
// Setting warnOnLowerSeqNo to true will check
// if we are setting the seq number to a _lower_ value than we already have stored for that
// vbucket and log a warning in that case.  The valid case for setting warnOnLowerSeqNo to
// false is when it's a rollback scenario.  See https://github.com/couchbase/sync_gateway/issues/1098 for dev notes.
func (c *DCPCommon) updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool) {
	c.m.Lock()
	defer c.m.Unlock()

	previousSequence := c.seqs[vbucketId]

	if seq < previousSequence && warnOnLowerSeqNo == true {
		WarnfCtx(c.loggingCtx, "Setting to _lower_ sequence number than previous: %v -> %v", c.seqs[vbucketId], seq)
	}

	// Update c.seqs for use by GetMetaData()
	c.seqs[vbucketId] = seq

}

// DCP-related utilities

// isMetadataDocument returns true if the document is not a metadata document.
func isMetadataDocumentName(key []byte) bool {

	// If it's a _txn doc, don't process
	if bytes.HasPrefix(key, []byte(TxnPrefix)) {
		return true
	}

	return bytes.HasPrefix(key, []byte(SyncDocPrefix))
}

// Makes a feedEvent that can be passed to a FeedEventCallbackFunc implementation
// The byte slices must be copied to ensure that memory associated with the memd mutationEvent and Packet are independent and can be released or reused by gocbcore as needed.
func makeFeedEvent(key []byte, value []byte, dataType uint8, cas uint64, expiry uint32, vbNo uint16, collectionID uint32, revNo uint64, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {

	event := sgbucket.FeedEvent{
		RevNo:        revNo,
		Opcode:       opcode,
		Key:          EfficientBytesClone(key),
		Value:        EfficientBytesClone(value),
		CollectionID: collectionID,
		DataType:     dataType,
		Cas:          cas,
		Expiry:       expiry,
		Synchronous:  true,
		TimeReceived: time.Now(),
		VbNo:         vbNo,
	}
	return event
}

// Create a prefix that will be used to create the dcp stream name, which must be globally unique
// in order to avoid https://issues.couchbase.com/browse/MB-24237.  It's also useful to have the Sync Gateway
// version number / commit for debugging purposes
func GenerateDcpStreamName(feedID string) (string, error) {

	// Create a time-based UUID for uniqueness of DCP Stream Names
	u, err := uuid.NewUUID()
	if err != nil {
		return "", err
	}

	commitTruncated := StringPrefix(GitCommit, 7)

	return fmt.Sprintf(
		"%v-v-%v-commit-%v-uuid-%v",
		feedID,
		ProductAPIVersion,
		commitTruncated,
		u.String(),
	), nil

}
