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
	"crypto/tls"
	"errors"
	"expvar"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	memcached "github.com/couchbase/gomemcached/client"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/google/uuid"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
)

// Number of non-checkpoint updates per vbucket required to trigger metadata persistence.  Must be greater than zero to avoid
// retriggering persistence solely based on checkpoint doc echo.
// Based on ad-hoc testing w/ travel-sample bucket, increasing this value doesn't significantly improve performance, since under load
// DCP will already be sending more documents per snapshot.
const kCheckpointThreshold = 1

// Only persist checkpoint once per kCheckpointTimeThreshold (per vbucket)
const kCheckpointTimeThreshold = 1 * time.Minute

// Persist backfill progress every 10s
const kBackfillPersistInterval = 10 * time.Second

// DCP Feed IDs are used to build unique DCP identifiers
const DCPCachingFeedID = "SG"
const DCPImportFeedID = "SGI"

type SimpleFeed struct {
	eventFeed  chan sgbucket.FeedEvent
	terminator chan bool
}

func (s *SimpleFeed) Events() <-chan sgbucket.FeedEvent {
	return s.eventFeed
}

func (s *SimpleFeed) WriteEvents() chan<- sgbucket.FeedEvent {
	return s.eventFeed
}

func (s *SimpleFeed) Close() error {
	close(s.terminator)
	return nil
}

type DCPCommon struct {
	dbStatsExpvars         *expvar.Map
	m                      sync.Mutex
	bucket                 Bucket                         // For metadata persistence/retrieval
	maxVbNo                uint16                         // Number of vbuckets being used for this feed
	persistCheckpoints     bool                           // Whether this DCPReceiver should persist metadata to the bucket
	seqs                   []uint64                       // To track max seq #'s we received per vbucketId.
	meta                   [][]byte                       // To track metadata blob's per vbucketId.
	vbuuids                map[uint16]uint64              // Map of vbucket uuids, by vbno.  Used in cases of manual vbucket metadata creation
	updatesSinceCheckpoint []uint64                       // Number of updates since the last checkpoint. Used to avoid checkpoint persistence feedback loop
	lastCheckpointTime     []time.Time                    // Time of last checkpoint persistence, per vbucket.  Used to manage checkpoint persistence volume
	callback               sgbucket.FeedEventCallbackFunc // Function to callback for mutation processing
	backfill               *backfillStatus                // Backfill state and stats
	feedID                 string                         // Unique feed ID, used for logging
	loggingCtx             context.Context                // Logging context, prefixes feedID
	checkpointPrefix       string                         // DCP checkpoint key prefix
}

func NewDCPCommon(callback sgbucket.FeedEventCallbackFunc, bucket Bucket, maxVbNo uint16, persistCheckpoints bool, dbStats *expvar.Map, feedID, checkpointPrefix string) *DCPCommon {
	newBackfillStatus := backfillStatus{}

	c := &DCPCommon{
		dbStatsExpvars:         dbStats,
		bucket:                 bucket,
		maxVbNo:                maxVbNo,
		persistCheckpoints:     persistCheckpoints,
		seqs:                   make([]uint64, maxVbNo),
		meta:                   make([][]byte, maxVbNo),
		vbuuids:                make(map[uint16]uint64, maxVbNo),
		updatesSinceCheckpoint: make([]uint64, maxVbNo),
		callback:               callback,
		lastCheckpointTime:     make([]time.Time, maxVbNo),
		backfill:               &newBackfillStatus,
		feedID:                 feedID,
		checkpointPrefix:       checkpointPrefix,
	}

	dcpContextID := fmt.Sprintf("%s-%s", MD(bucket.GetName()).Redact(), feedID)
	c.loggingCtx = context.WithValue(context.Background(), LogContextKey{},
		LogContext{CorrelationID: dcpContextID},
	)

	return c
}

func (c *DCPCommon) dataUpdate(seq uint64, event sgbucket.FeedEvent) {
	c.updateSeq(event.VbNo, seq, true)
	shouldPersistCheckpoint := c.callback(event)
	if c.persistCheckpoints && shouldPersistCheckpoint {
		c.incrementCheckpointCount(event.VbNo)
	}
}

func (c *DCPCommon) snapshotStart(vbNo uint16, snapStart, snapEnd uint64) {
	// During initial backfill, we persist snapshot information to support resuming the DCP
	// stream midway through a snapshot.  This is primarily for the import when initially
	// connection to a populated bucket, to avoid restarting the import from
	// zero if SG is terminated before completing processing of the initial snapshots.
	if c.backfill.isActive() && c.backfill.isVbActive(vbNo) {
		c.backfill.snapshotStart(vbNo, snapStart, snapEnd)
	}
}

// setMetaData and getMetaData may used internally by dcp clients.  Expects send/receive of opaque
// []byte data.  May be invoked from multiple goroutines, so need to manage synchronization
func (c *DCPCommon) setMetaData(vbucketId uint16, value []byte) {

	c.m.Lock()
	defer c.m.Unlock()

	c.meta[vbucketId] = value

	// Check persistMeta to avoids persistence if the only feed events we've seen are the DCP echo of DCP checkpoint docs
	if c.persistCheckpoints && c.updatesSinceCheckpoint[vbucketId] >= kCheckpointThreshold {

		// Don't checkpoint more frequently than kCheckpointTimeThreshold
		if time.Since(c.lastCheckpointTime[vbucketId]) < kCheckpointTimeThreshold {
			return
		}

		err := c.persistCheckpoint(vbucketId, value)
		if err != nil {
			WarnfCtx(c.loggingCtx, "Unable to persist DCP metadata - will retry next snapshot. Error: %v", err)
		}
		c.updatesSinceCheckpoint[vbucketId] = 0
		c.lastCheckpointTime[vbucketId] = time.Now()
	}
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

// RollbackEx should be called by cbdatasource - Rollback required to maintain the interface.  In the event
// it's called, logs warning and does a hard reset on metadata for the vbucket
func (c *DCPCommon) rollback(vbucketId uint16, rollbackSeq uint64) error {
	WarnfCtx(c.loggingCtx, "DCP Rollback request.  Expected RollbackEx call - resetting vbucket %d to 0.", vbucketId)
	c.dbStatsExpvars.Add("dcp_rollback_count", 1)
	c.updateSeq(vbucketId, 0, false)
	c.setMetaData(vbucketId, nil)

	return nil
}

// RollbackEx includes the vbucketUUID needed to reset the metadata correctly
func (c *DCPCommon) rollbackEx(vbucketId uint16, vbucketUUID uint64, rollbackSeq uint64, rollbackMetaData []byte) error {
	WarnfCtx(c.loggingCtx, "DCP RollbackEx request - rolling back DCP feed for: vbucketId: %d, rollbackSeq: %x.", vbucketId, rollbackSeq)
	c.dbStatsExpvars.Add("dcp_rollback_count", 1)
	c.updateSeq(vbucketId, rollbackSeq, false)
	c.setMetaData(vbucketId, rollbackMetaData)
	return nil
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
//    sequences in a checkpoint on startup
func (c *DCPCommon) loadCheckpoint(vbNo uint16) (vbMetadata []byte, snapshotStartSeq uint64, snapshotEndSeq uint64, err error) {
	rawValue, _, err := c.bucket.GetRaw(fmt.Sprintf("%s%d", c.checkpointPrefix, vbNo))
	if err != nil {
		// On a key not found error, metadata hasn't been persisted for this vbucket
		if IsKeyNotFoundError(c.bucket, err) {
			return []byte{}, 0, 0, nil
		} else {
			return []byte{}, 0, 0, err
		}
	}

	var snapshotMetadata cbdatasource.VBucketMetaData
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

func (c *DCPCommon) initMetadata(maxVbNo uint16) {
	c.m.Lock()
	defer c.m.Unlock()

	// Check for persisted backfill sequences
	backfillSeqs, err := c.backfill.loadBackfillSequences(c.bucket)
	if err != nil {
		// Backfill sequences not present or invalid - will use metadata only
		backfillSeqs = nil
	}

	// Load persisted metadata
	for i := uint16(0); i < maxVbNo; i++ {
		metadata, snapStart, snapEnd, err := c.loadCheckpoint(i)
		if err != nil {
			WarnfCtx(c.loggingCtx, "Unexpected error attempting to load DCP checkpoint for vbucket %d.  Will restart DCP for that vbucket from zero.  Error: %v", i, err)
			c.meta[i] = []byte{}
			c.seqs[i] = 0
		} else {
			c.meta[i] = metadata
			c.seqs[i] = snapStart
			// Check whether we persisted a sequence midway through a previous incomplete backfill
			if backfillSeqs != nil {
				var partialBackfillSequence uint64
				if backfillSeqs.Seqs[i] < backfillSeqs.SnapEnd[i] {
					partialBackfillSequence = backfillSeqs.Seqs[i]
				}
				// If we have a backfill sequence later than the DCP checkpoint's snapStart, start from there
				if partialBackfillSequence > snapStart {
					InfofCtx(c.loggingCtx, KeyDCP, "Restarting vb %d using backfill sequence %d ([%d-%d])", i, partialBackfillSequence, backfillSeqs.SnapStart[i], backfillSeqs.SnapEnd[i])
					c.seqs[i] = partialBackfillSequence
					c.meta[i] = makeVbucketMetadata(c.vbuuids[i], partialBackfillSequence, backfillSeqs.SnapStart[i], backfillSeqs.SnapEnd[i])
				} else {
					InfofCtx(c.loggingCtx, KeyDCP, "Restarting vb %d using metadata sequence %d  (backfill %d not in [%d-%d])", i, snapStart, partialBackfillSequence, snapStart, snapEnd)
				}
			}
		}
	}

}

// TODO: Convert checkpoint persistence to an asynchronous batched process, since
//       restarting w/ an older checkpoint:
//         - Would only result in some repeated entry processing, which is already handled by the indexer
//         - Is a relatively infrequent operation
func (c *DCPCommon) persistCheckpoint(vbNo uint16, value []byte) error {
	TracefCtx(c.loggingCtx, KeyDCP, "Persisting checkpoint for vbno %d", vbNo)
	return c.bucket.SetRaw(fmt.Sprintf("%s%d", c.checkpointPrefix, vbNo), 0, nil, value)
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

	// If in backfill, update backfill tracking
	if c.backfill.isActive() {
		c.backfill.updateStats(vbucketId, previousSequence, c.seqs, c.bucket)
	}

}

// Initializes DCP Feed.  Determines starting position based on feed type.
func (c *DCPCommon) initFeed(backfillType uint64) (highSeqnos map[uint16]uint64, err error) {

	couchbaseBucket, ok := AsCouchbaseStore(c.bucket)
	if !ok {
		return nil, errors.New("DCP not supported for non-Couchbase data source")
	}

	var statsUuids map[uint16]uint64
	statsUuids, highSeqnos, err = couchbaseBucket.GetStatsVbSeqno(c.maxVbNo, false)
	if err != nil {
		return nil, pkgerrors.Wrap(err, "Error retrieving stats-vbseqno - DCP not supported")
	}

	c.vbuuids = statsUuids

	switch backfillType {
	case sgbucket.FeedNoBackfill:
		// For non-backfill, use vbucket uuids, high sequence numbers
		DebugfCtx(c.loggingCtx, KeyDCP, "Initializing DCP with no backfill - seeding seqnos: %v", highSeqnos)
		c.seedSeqnos(statsUuids, highSeqnos)
	case sgbucket.FeedResume:
		// For resume case, load previously persisted checkpoints from bucket
		c.initMetadata(c.maxVbNo)
		// Track backfill (from persisted checkpoints to current high seqno)
		c.backfill.init(c.seqs, highSeqnos, c.maxVbNo, c.dbStatsExpvars)
		DebugfCtx(c.loggingCtx, KeyDCP, "Initializing DCP feed based on persisted checkpoints")
	default:
		// Otherwise, start feed from zero
		startSeqnos := make(map[uint16]uint64, c.maxVbNo)
		vbuuids := make(map[uint16]uint64, c.maxVbNo)
		c.seedSeqnos(vbuuids, startSeqnos)
		// Track backfill (from zero to current high seqno)
		c.backfill.init(c.seqs, highSeqnos, c.maxVbNo, c.dbStatsExpvars)
		DebugfCtx(c.loggingCtx, KeyDCP, "Initializing DCP feed to start from zero")
	}

	return highSeqnos, nil
}

// Seeds the sequence numbers returned by GetMetadata to support starting DCP from a particular
// sequence.
func (c *DCPCommon) seedSeqnos(uuids map[uint16]uint64, seqs map[uint16]uint64) {
	c.m.Lock()
	defer c.m.Unlock()

	// Set the high seqnos as-is
	for vbNo, seq := range seqs {
		c.seqs[vbNo] = seq
	}

	// For metadata, we need to do more work to build metadata based on uuid and map values.  This
	// isn't strictly to the design of cbdatasource.Receiver, which intends metadata to be opaque, but
	// is required in order to have the BucketDataSource start the UPRStream as needed.
	// The implementation has been reviewed with the cbdatasource owners and they agree this is a
	// reasonable approach, as the structure of VBucketMetaData is expected to rarely change.
	for vbucketId, uuid := range uuids {
		c.meta[vbucketId] = makeVbucketMetadataForSequence(uuid, seqs[vbucketId])
	}
}

// BackfillStatus

// BackfillStatus manages tracking of DCP backfill progress, to provide diagnostics and mid-snapshot restart capability
type backfillStatus struct {
	active            bool        // Whether this DCP feed is in backfill
	vbActive          []bool      // Whether a vbucket is in backfill
	receivedSequences uint64      // Number of backfill sequences received
	expectedSequences uint64      // Expected number of sequences in backfill
	endSeqs           []uint64    // Backfill complete sequences, indexed by vbno
	snapStart         []uint64    // Start sequence of current backfill snapshot
	snapEnd           []uint64    // End sequence of current backfill snapshot
	lastPersistTime   time.Time   // The last time backfill stats were emitted (log, expvar)
	statsMap          *expvar.Map // Stats map for backfill
}

func (b *backfillStatus) init(start []uint64, end map[uint16]uint64, maxVbNo uint16, statsMap *expvar.Map) {
	b.vbActive = make([]bool, maxVbNo)
	b.snapStart = make([]uint64, maxVbNo)
	b.snapEnd = make([]uint64, maxVbNo)
	b.endSeqs = make([]uint64, maxVbNo)
	b.statsMap = statsMap

	// Calculate total sequences in backfill
	b.expectedSequences = 0
	for vbNo := uint16(0); vbNo < maxVbNo; vbNo++ {
		b.endSeqs[vbNo] = end[vbNo]
		if end[vbNo] > start[vbNo] {
			b.expectedSequences += end[vbNo] - start[vbNo]
			b.vbActive[vbNo] = true
			// Set backfill as active if any vb is in backfill
			b.active = true
		}
	}

	// Initialize backfill expvars
	// NOTE: this is a legacy stat, but cannot be removed b/c there are unit tests that depend on these stats
	totalVar := &expvar.Int{}
	completedVar := &expvar.Int{}
	totalVar.Set(int64(b.expectedSequences))
	completedVar.Set(0)
	statsMap.Set("dcp_backfill_expected", totalVar)
	statsMap.Set("dcp_backfill_completed", completedVar)

}

func (b *backfillStatus) isActive() bool {
	return b.active
}

func (b *backfillStatus) isVbActive(vbNo uint16) bool {
	return b.vbActive[vbNo]
}

func (b *backfillStatus) snapshotStart(vbNo uint16, snapStart uint64, snapEnd uint64) {
	b.snapStart[vbNo] = snapStart
	b.snapEnd[vbNo] = snapEnd
}
func (b *backfillStatus) updateStats(vbno uint16, previousVbSequence uint64, currentSequences []uint64, bucket Bucket) {
	if !b.vbActive[vbno] {
		return
	}

	logCtx := context.TODO()
	currentVbSequence := currentSequences[vbno]

	// Update backfill progress.  If this vbucket has run past the end of the backfill, only include up to
	// the backfill target for progress tracking.
	var backfillDelta uint64
	if currentVbSequence >= b.endSeqs[vbno] {
		backfillDelta = b.endSeqs[vbno] - previousVbSequence
		b.vbActive[vbno] = false
	} else {
		backfillDelta = currentVbSequence - previousVbSequence
	}

	b.receivedSequences += backfillDelta

	// NOTE: this is a legacy stat, but cannot be removed b/c there are unit tests that depend on these stats
	b.statsMap.Add("dcp_backfill_completed", int64(backfillDelta))

	// Check if it's time to persist and log backfill progress
	if time.Since(b.lastPersistTime) > kBackfillPersistInterval {
		b.lastPersistTime = time.Now()
		err := b.persistBackfillSequences(bucket, currentSequences)
		if err != nil {
			WarnfCtx(logCtx, "Error persisting back-fill sequences: %v", err)
		}
		b.logBackfillProgress()
	}

	// If backfill is complete, log and do backfill inactivation/cleanup
	if b.receivedSequences >= b.expectedSequences {
		InfofCtx(logCtx, KeyDCP, "Backfill complete")
		b.active = false
		err := b.purgeBackfillSequences(bucket)
		if err != nil {
			WarnfCtx(logCtx, "Error purging back-fill sequences: %v", err)
		}
	}
}

// Logs current backfill progress.  Expects caller to have the lock on r.m
func (b *backfillStatus) logBackfillProgress() {
	if !b.active {
		return
	}
	InfofCtx(context.TODO(), KeyDCP, "Backfill in progress: %d%% (%d / %d)", int(b.receivedSequences*100/b.expectedSequences), b.receivedSequences, b.expectedSequences)
}

// BackfillSequences defines the format used to persist snapshot information to the _sync:dcp_backfill document
// to support mid-snapshot restart
type BackfillSequences struct {
	Seqs      []uint64
	SnapStart []uint64
	SnapEnd   []uint64
}

func (b *backfillStatus) persistBackfillSequences(bucket Bucket, currentSeqs []uint64) error {
	backfillSeqs := &BackfillSequences{
		Seqs:      currentSeqs,
		SnapStart: b.snapStart,
		SnapEnd:   b.snapEnd,
	}
	return bucket.Set(DCPBackfillSeqKey, 0, nil, backfillSeqs)
}

func (b *backfillStatus) loadBackfillSequences(bucket Bucket) (*BackfillSequences, error) {
	var backfillSeqs BackfillSequences
	_, err := bucket.Get(DCPBackfillSeqKey, &backfillSeqs)
	if err != nil {
		return nil, err
	}
	InfofCtx(context.TODO(), KeyDCP, "Previously persisted backfill sequences found - will resume")
	return &backfillSeqs, nil
}

func (b *backfillStatus) purgeBackfillSequences(bucket Bucket) error {
	return bucket.Delete(DCPBackfillSeqKey)
}

// DCP-related utilities

// Only a subset of Sync Gateway's internal documents need to be included during DCP processing: user, role, and
// unused sequence documents.  Any other documents with the leading '_sync' prefix can be ignored.
// dcpKeyFilter returns true for documents that should be processed, false for those that do not need processing.
// c is used to get the SG Cfg prefix
func dcpKeyFilter(key []byte) bool {

	// If it's a _txn doc, don't process
	if bytes.HasPrefix(key, []byte(TxnPrefix)) {
		return false
	}

	// If it's not a _sync doc, process
	if !bytes.HasPrefix(key, []byte(SyncPrefix)) {
		return true
	}

	// User, role, unused sequence markers and cbgt cfg (regardless of group ID) docs should be processed
	if bytes.HasPrefix(key, []byte(UnusedSeqPrefix)) ||
		bytes.HasPrefix(key, []byte(UnusedSeqRangePrefix)) ||
		bytes.HasPrefix(key, []byte(UserPrefix)) ||
		bytes.HasPrefix(key, []byte(RolePrefix)) ||
		bytes.HasPrefix(key, []byte(SGCfgPrefix)) {
		return true
	}

	return false
}

// Makes a feedEvent that can be passed to a FeedEventCallbackFunc implementation
func makeFeedEvent(key []byte, value []byte, dataType uint8, cas uint64, expiry uint32, vbNo uint16, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {

	// not currently doing rq.Extras handling (as in gocouchbase/upr_feed, makeUprEvent) as SG doesn't use
	// expiry/flags information, and snapshot handling is done by cbdatasource and sent as
	// SnapshotStart, SnapshotEnd
	event := sgbucket.FeedEvent{
		Opcode:       opcode,
		Key:          key,
		Value:        value,
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
		ProductVersionNumber,
		commitTruncated,
		u.String(),
	), nil

}

// getExternalAlternateAddress returns a external alternate address for a given dest
func getExternalAlternateAddress(loggingCtx context.Context, alternateAddressMap map[string]string, dest string) (string, error) {
	if len(alternateAddressMap) == 0 {
		// early exit if we know we've got nothing to find
		return dest, nil
	}

	destHost, destPort, err := SplitHostPort(dest)
	if err != nil {
		return "", err
	}

	// Map the given destination to an external alternate address hostname if available
	if extHostname, foundAltAddress := alternateAddressMap[destHost]; foundAltAddress {
		host, port, _ := SplitHostPort(extHostname)
		if port == "" {
			port = destPort
		}
		if host == "" {
			host = extHostname
		}

		InfofCtx(loggingCtx, KeyDCP, "Using alternate address %s => %s", MD(dest), MD(host+":"+port))
		dest = host + ":" + port
	}

	return dest, nil
}

type clusterNetworkType string

const (
	// clusterNetworkAuto applies a heuristic to determine which network to use (based on bootstrap hosts)
	clusterNetworkAuto clusterNetworkType = "auto"
	// clusterNetworkDefault uses the default (internal) network
	clusterNetworkDefault clusterNetworkType = "default"
	// clusterNetworkExternal will use the external network
	clusterNetworkExternal clusterNetworkType = "external"
)

// getNetworkTypeFromConnSpec returns the configured network type, or clusterNetworkAuto if nothing is defined.
func getNetworkTypeFromConnSpec(spec gocbconnstr.ConnSpec) clusterNetworkType {
	networkType := clusterNetworkAuto
	if networkOpt, ok := spec.Options["network"]; ok && len(networkOpt) > 0 {
		if len(networkOpt) > 1 {
			WarnfCtx(context.TODO(), "multiple 'network' options found in connection string - using first one: %q", networkOpt[0])
		}
		networkType = clusterNetworkType(networkOpt[0])
	}
	return networkType
}

// alternateAddressShims returns the 3 functions that wrap around ConnectBucket/Connect/ConnectTLS to provide alternate address support.
func alternateAddressShims(loggingCtx context.Context, bucketSpecTLS bool, connSpecAddresses []gocbconnstr.Address, networkType clusterNetworkType) (
	connectBucketShim func(serverURL, poolName, bucketName string, auth couchbase.AuthHandler) (cbdatasource.Bucket, error),
	connectShim func(protocol, dest string) (*memcached.Client, error),
	connectTLSShim func(protocol, dest string, tlsConfig *tls.Config) (*memcached.Client, error),
) {

	// A map of dest URL (which may be an internal-only address) to external alternate address.
	var externalAlternateAddresses map[string]string

	// Copy of cbdatasource's default ConnectBucket function, which maps internal addresses to alternate addresses
	connectBucketShim = func(serverURL, poolName, bucketName string, auth couchbase.AuthHandler) (cbdatasource.Bucket, error) {
		TracefCtx(loggingCtx, KeyDCP, "ConnectBucket callback: %s %s %s", MD(serverURL), poolName, MD(bucketName))

		var (
			err    error
			client couchbase.Client
		)

		if auth != nil {
			client, err = couchbase.ConnectWithAuth(serverURL, auth)
		} else {
			client, err = couchbase.Connect(serverURL)
		}
		if err != nil {
			return nil, err
		}

		// Fetch any alternate external addresses/ports and store them in the externalAlternateAddresses map
		poolServices, err := client.GetPoolServices(poolName)
		if err != nil {
			return nil, err
		}

		connSpecAddressesHostMap := make(map[string]struct{}, len(connSpecAddresses))
		for _, connSpecAddress := range connSpecAddresses {
			connSpecAddressesHostMap[connSpecAddress.Host] = struct{}{}
		}

		// Recreate the map to forget about previous clustermap information.
		externalAlternateAddresses = make(map[string]string, len(poolServices.NodesExt))
		for _, node := range poolServices.NodesExt {

			// apply heuristic if auto to select between "default" and "external"
			if networkType == clusterNetworkAuto {
				if _, ok := connSpecAddressesHostMap[node.Hostname]; ok {
					DebugfCtx(loggingCtx, KeyDCP, "Matched host %s in connection string - using default/internal networking.", MD(node.Hostname))
					// Found default hostname in connSpec - abort all alternate address behaviour.
					// The client MUST use the default/internal network.
					externalAlternateAddresses = nil
					break
				}
				// select external network now heuristic failed
				networkType = clusterNetworkExternal
			}

			DebugfCtx(loggingCtx, KeyDCP, "Finding alternate addresses for network %s", networkType)

			// only try to map alternate addresses if an alternate hostname is present
			if alt, ok := node.AlternateNames[string(networkType)]; ok && alt.Hostname != "" {
				var port string
				if bucketSpecTLS {
					extPort, ok := alt.Ports["kvSSL"]
					if !ok {
						TracefCtx(loggingCtx, KeyDCP, "kvSSL port was not exposed for %s alternate address. Skipping remapping of this node.", networkType)
						continue
					}

					// found exposed kvSSL port, use when connecting
					port = ":" + strconv.Itoa(extPort)
					DebugfCtx(loggingCtx, KeyDCP, "Storing alternate address for kvSSL: %s => %s", MD(node.Hostname), MD(alt.Hostname+port))
				} else {
					extPort, ok := alt.Ports["kv"]
					if !ok {
						TracefCtx(loggingCtx, KeyDCP, "kv port was not exposed for %s alternate address. Skipping remapping of this node.", networkType)
						continue
					}

					// found exposed kv port, use when connecting
					port = ":" + strconv.Itoa(extPort)
					DebugfCtx(loggingCtx, KeyDCP, "Storing alternate address for kv: %s => %s", MD(node.Hostname), MD(alt.Hostname+port))
				}

				externalAlternateAddresses[node.Hostname] = alt.Hostname + port
			}
		}

		var bucket *couchbase.Bucket
		if auth != nil {
			bucket, err = couchbase.ConnectWithAuthAndGetBucket(serverURL, poolName, bucketName, auth)
		} else {
			bucket, err = couchbase.GetBucket(serverURL, poolName, bucketName)
		}
		if err != nil {
			return nil, err
		}

		if bucket == nil {
			return nil, fmt.Errorf("unknown bucket,"+
				" serverURL: %s, bucketName: %s", serverURL, bucketName)
		}

		return bucket, nil
	}

	// Copy of cbdatasource's default Connect function, which swaps the given destination, with alternate addresses we found in ConnectBucket.
	connectShim = func(protocol, dest string) (client *memcached.Client, err error) {
		TracefCtx(loggingCtx, KeyDCP, "Connect mutationCallback: %s %s", protocol, MD(dest))

		dest, err = getExternalAlternateAddress(loggingCtx, externalAlternateAddresses, dest)
		if err != nil {
			return nil, err
		}

		return memcached.Connect(protocol, dest)
	}

	// Copy of cbdatasource's default ConnectTLS function, which swaps the given destination, with alternate addresses we found in ConnectBucket.
	connectTLSShim = func(protocol, dest string, tlsConfig *tls.Config) (client *memcached.Client, err error) {
		TracefCtx(loggingCtx, KeyDCP, "ConnectTLS mutationCallback: %s %s", protocol, MD(dest))

		newDest, err := getExternalAlternateAddress(loggingCtx, externalAlternateAddresses, dest)
		if err != nil {
			return nil, err
		}
		if newDest == dest {
			// skip unnecessary tls reconfiguration if no alternate was found
			return memcached.ConnectTLS(protocol, dest, tlsConfig)
		}

		// extract the new host and insert into the tlsConfig
		host, _, err := SplitHostPort(dest)
		if err != nil {
			return nil, err
		}
		tlsConfigCopy := tlsConfig.Clone()
		tlsConfigCopy.ServerName = host

		return memcached.ConnectTLS(protocol, dest, tlsConfigCopy)
	}

	return connectBucketShim, connectShim, connectTLSShim
}
