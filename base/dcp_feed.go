//  Copyright (c) 2014 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package base

import (
	"encoding/json"
	"expvar"
	"fmt"
	"sync"
	"time"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	"github.com/couchbase/sg-bucket"
	"github.com/google/uuid"
	pkgerrors "github.com/pkg/errors"
)

var dcpExpvars *expvar.Map

func init() {
	dcpExpvars = expvar.NewMap("syncGateway_dcp")
}

// Memcached binary protocol datatype bit flags (https://github.com/couchbase/memcached/blob/master/docs/BinaryProtocol.md#data-types),
// used in MCRequest.DataType
const (
	MemcachedDataTypeJSON = 1 << iota
	MemcachedDataTypeSnappy
	MemcachedDataTypeXattr
)

// Memcached datatype for raw (binary) document (non-flag)
const MemcachedDataTypeRaw = 0

const DCPCheckpointPrefix = "_sync:dcp_ck:"  // Prefix used for DCP checkpoint persistence (is appended with vbno)
const DCPBackfillSeqs = "_sync:dcp_backfill" // Bucket doc used for DCP sequence persistence during backfill

// Number of non-checkpoint updates per vbucket required to trigger metadata persistence.  Must be greater than zero to avoid
// retriggering persistence solely based on checkpoint doc echo.
// Based on ad-hoc testing w/ travel-sample bucket, increasing this value doesn't significantly improve performance, since under load
// DCP will already be sending more documents per snapshot.
const kCheckpointThreshold = 1

// Persist backfill progress every 10s
const kBackfillPersistInterval = 10 * time.Second

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

type Receiver interface {
	cbdatasource.Receiver
	SeedSeqnos(map[uint16]uint64, map[uint16]uint64)
	updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool)
	SetBucketNotifyFn(sgbucket.BucketNotifyFn)
	GetBucketNotifyFn() sgbucket.BucketNotifyFn
	initFeed(feedType uint64) error
}

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPReceiver struct {
	m                      sync.Mutex
	bucket                 Bucket                         // For metadata persistence/retrieval
	maxVbNo                uint16                         // Number of vbuckets being used for this feed
	persistCheckpoints     bool                           // Whether this DCPReceiver should persist metadata to the bucket
	seqs                   []uint64                       // To track max seq #'s we received per vbucketId.
	meta                   [][]byte                       // To track metadata blob's per vbucketId.
	vbuuids                map[uint16]uint64              // Map of vbucket uuids, by vbno.  Used in cases of manual vbucket metadata creation
	updatesSinceCheckpoint []uint64                       // Number of updates since the last checkpoint. Used to avoid checkpoint persistence feedback loop
	notify                 sgbucket.BucketNotifyFn        // Function to callback when we lose our dcp feed
	callback               sgbucket.FeedEventCallbackFunc // Function to callback for mutation processing
	backfill               backfillStatus                 // Backfill state and stats
}

func NewDCPReceiver(callback sgbucket.FeedEventCallbackFunc, bucket Bucket, maxVbNo uint16, persistCheckpoints bool, backfillType uint64) (Receiver, error) {

	r := &DCPReceiver{
		bucket:                 bucket,
		maxVbNo:                maxVbNo,
		persistCheckpoints:     persistCheckpoints,
		seqs:                   make([]uint64, maxVbNo),
		meta:                   make([][]byte, maxVbNo),
		vbuuids:                make(map[uint16]uint64, maxVbNo),
		updatesSinceCheckpoint: make([]uint64, maxVbNo),
	}

	r.callback = callback
	initErr := r.initFeed(backfillType)

	if initErr != nil {
		return nil, initErr
	}

	if LogDebugEnabled(KeyDCP) {
		Infof(KeyDCP, "Using DCP Logging Receiver.")
		logRec := &DCPLoggingReceiver{rec: r}
		return logRec, nil
	}

	return r, nil
}

func (r *DCPReceiver) SetBucketNotifyFn(notify sgbucket.BucketNotifyFn) {
	r.notify = notify
}

func (r *DCPReceiver) GetBucketNotifyFn() sgbucket.BucketNotifyFn {
	return r.notify
}

func (r *DCPReceiver) OnError(err error) {
	Warnf(KeyAll, "Error processing DCP stream - will attempt to restart/reconnect if appropriate: %v.", err)
	// From cbdatasource:
	//  Invoked in advisory fashion by the BucketDataSource when it
	//  encounters an error.  The BucketDataSource will continue to try
	//  to "heal" and restart connections, etc, as necessary.  The
	//  Receiver has a recourse during these error notifications of
	//  simply Close()'ing the BucketDataSource.

	// Given this, we don't need to restart the feed/take the
	// database offline, particularly since this only represents an error for a single
	// vbucket stream, not the entire feed.
	// bucketName := "unknown" // this is currently ignored anyway
	// r.notify(bucketName, err)
	dcpExpvars.Add("onError_count", 1)
}

func (r *DCPReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	dcpExpvars.Add("dataUpdate_count", 1)
	r.updateSeq(vbucketId, seq, true)
	shouldPersistCheckpoint := r.callback(makeFeedEvent(req, vbucketId, sgbucket.FeedOpMutation))
	if shouldPersistCheckpoint {
		r.incrementCheckpointCount(vbucketId)
	}
	return nil
}

func (r *DCPReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	dcpExpvars.Add("dataDelete_count", 1)
	r.updateSeq(vbucketId, seq, true)
	shouldPersistCheckpoint := r.callback(makeFeedEvent(req, vbucketId, sgbucket.FeedOpDeletion))
	if shouldPersistCheckpoint {
		r.incrementCheckpointCount(vbucketId)
	}
	return nil
}

func (r *DCPReceiver) incrementCheckpointCount(vbucketId uint16) {
	r.m.Lock()
	defer r.m.Unlock()
	r.updatesSinceCheckpoint[vbucketId]++
}

func makeFeedEvent(rq *gomemcached.MCRequest, vbucketId uint16, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {

	// not currently doing rq.Extras handling (as in gocouchbase/upr_feed, makeUprEvent) as SG doesn't use
	// expiry/flags information, and snapshot handling is done by cbdatasource and sent as
	// SnapshotStart, SnapshotEnd
	event := sgbucket.FeedEvent{
		Opcode:      opcode,
		Key:         rq.Key,
		Value:       rq.Body,
		DataType:    rq.DataType,
		Cas:         rq.Cas,
		Expiry:      ExtractExpiryFromDCPMutation(rq),
		Synchronous: true,
	}
	return event
}

func (r *DCPReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	// During initial backfill, we persist snapshot information to support resuming the DCP
	// stream midway through a snapshot.  This is primarily for the import when initially
	// connection to a populated bucket, to avoid restarting the import from
	// zero if SG is terminated before completing processing of the initial snapshots.
	if r.backfill.isActive() && r.backfill.isVbActive(vbucketId) {
		r.backfill.snapshotStart(vbucketId, snapStart, snapEnd)
	}
	return nil
}

// SetMetaData and GetMetaData used internally by cbdatasource.  Expects send/recieve of opaque
// []byte data.  cbdatasource is multithreaded so need to manage synchronization
func (r *DCPReceiver) SetMetaData(vbucketId uint16, value []byte) error {

	r.m.Lock()
	defer r.m.Unlock()

	dcpExpvars.Add("setMetadata_count", 1)
	r.meta[vbucketId] = value

	// Check persistMeta to avoids persistence if the only feed events we've seen are the DCP echo of DCP checkpoint docs
	if r.persistCheckpoints && r.updatesSinceCheckpoint[vbucketId] >= kCheckpointThreshold {
		err := r.persistCheckpoint(vbucketId, value)
		if err != nil {
			Warnf(KeyAll, "Unable to persist DCP metadata - will retry next snapshot. Error: %v", err)
		}
		r.updatesSinceCheckpoint[vbucketId] = 0
	}
	return nil
}

func (r *DCPReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {

	r.m.Lock()
	defer r.m.Unlock()

	value = []byte(nil)
	if r.meta != nil {
		value = r.meta[vbucketId]
	}

	if r.seqs != nil {
		lastSeq = r.seqs[vbucketId]
	}

	return value, lastSeq, nil
}

// RollbackEx should be called by cbdatasource - Rollback required to maintain the interface.  In the event
// it's called, logs warning and does a hard reset on metadata for the vbucket
func (r *DCPReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	Warnf(KeyAll, "DCP Rollback request.  Expected RollbackEx call - resetting vbucket %d to 0.", vbucketId)
	dcpExpvars.Add("rollback_count", 1)
	r.updateSeq(vbucketId, 0, false)
	r.SetMetaData(vbucketId, nil)

	return nil
}

// RollbackEx includes the vbucketUUID needed to reset the metadata correctly
func (r *DCPReceiver) RollbackEx(vbucketId uint16, vbucketUUID uint64, rollbackSeq uint64) error {
	Warnf(KeyAll, "DCP RollbackEx request - rolling back DCP feed for: vbucketId: %d, rollbackSeq: %x.", vbucketId, rollbackSeq)

	dcpExpvars.Add("rollback_count", 1)
	r.updateSeq(vbucketId, rollbackSeq, false)
	r.SetMetaData(vbucketId, makeVbucketMetadataForSequence(vbucketUUID, rollbackSeq))
	return nil
}

// This updates the value stored in r.seqs with the given seq number for the given partition
// (whic.  Setting warnOnLowerSeqNo to true will check
// if we are setting the seq number to a _lower_ value than we already have stored for that
// vbucket and log a warning in that case.  The valid case for setting warnOnLowerSeqNo to
// false is when it's a rollback scenario.  See https://github.com/couchbase/sync_gateway/issues/1098 for dev notes.
func (r *DCPReceiver) updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool) {
	r.m.Lock()
	defer r.m.Unlock()

	previousSequence := r.seqs[vbucketId]

	if seq < previousSequence && warnOnLowerSeqNo == true {
		Warnf(KeyAll, "Setting to _lower_ sequence number than previous: %v -> %v", r.seqs[vbucketId], seq)
	}

	// Update r.seqs for use by GetMetaData()
	r.seqs[vbucketId] = seq

	// If in backfill, update backfill tracking
	if r.backfill.isActive() {
		r.backfill.updateStats(vbucketId, previousSequence, r.seqs, r.bucket)
	}

}

// Seeds the sequence numbers returned by GetMetadata to support starting DCP from a particular
// sequence.
func (r *DCPReceiver) SeedSeqnos(uuids map[uint16]uint64, seqs map[uint16]uint64) {
	r.m.Lock()
	defer r.m.Unlock()

	// Set the high seqnos as-is
	for vbNo, seq := range seqs {
		r.seqs[vbNo] = seq
	}

	// For metadata, we need to do more work to build metadata based on uuid and map values.  This
	// isn't strictly to the design of cbdatasource.Receiver, which intends metadata to be opaque, but
	// is required in order to have the BucketDataSource start the UPRStream as needed.
	// The implementation has been reviewed with the cbdatasource owners and they agree this is a
	// reasonable approach, as the structure of VBucketMetaData is expected to rarely change.
	for vbucketId, uuid := range uuids {
		r.meta[vbucketId] = makeVbucketMetadataForSequence(uuid, seqs[vbucketId])
	}
}

func makeVbucketMetadata(vbucketUUID uint64, sequence uint64, snapStart uint64, snapEnd uint64) []byte {
	failOver := make([][]uint64, 1)
	failOverEntry := []uint64{vbucketUUID, 0}
	failOver[0] = failOverEntry
	metadata := &cbdatasource.VBucketMetaData{
		SeqStart:    sequence,
		SeqEnd:      uint64(0xFFFFFFFFFFFFFFFF),
		SnapStart:   snapStart,
		SnapEnd:     snapEnd,
		FailOverLog: failOver,
	}
	metadataBytes, err := json.Marshal(metadata)
	if err == nil {
		return metadataBytes
	} else {
		return []byte{}
	}
}

// Create VBucketMetadata, marshalled to []byte
func makeVbucketMetadataForSequence(vbucketUUID uint64, sequence uint64) []byte {
	return makeVbucketMetadata(vbucketUUID, sequence, sequence, sequence)

}

// TODO: Convert checkpoint persistence to an asynchronous batched process, since
//       restarting w/ an older checkpoint:
//         - Would only result in some repeated entry processing, which is already handled by the indexer
//         - Is a relatively infrequent operation (occurs when vbuckets are initially assigned to an accel node)
func (r *DCPReceiver) persistCheckpoint(vbNo uint16, value []byte) error {
	dcpExpvars.Add("persistCheckpoint_count", 1)
	Tracef(KeyDCP, "Persisting checkpoint for vbno %d", vbNo)
	return r.bucket.SetRaw(fmt.Sprintf("%s%d", DCPCheckpointPrefix, vbNo), 0, value)
}

// loadCheckpoint retrieves previously persisted DCP metadata.  Need to unmarshal metadata to determine last sequence processed.
// We always restart the feed from the last persisted snapshot start (as opposed to a sequence we may have processed
// midway through the checkpoint), because:
//   - We don't otherwise persist the last sequence we processed
//   - For SG feed processing, there's no harm if we receive feed events for mutations we've previously seen
//   - The ongoing performance overhead of persisting last sequence outweighs the minor performance benefit of not reprocessing a few
//    sequences in a checkpoint on startup
func (r *DCPReceiver) loadCheckpoint(vbNo uint16) (vbMetadata []byte, snapshotStartSeq uint64, snapshotEndSeq uint64, err error) {
	dcpExpvars.Add("loadCheckpoint_count", 1)
	rawValue, _, err := r.bucket.GetRaw(fmt.Sprintf("%s%d", DCPCheckpointPrefix, vbNo))
	if err != nil {
		// On a key not found error, metadata hasn't been persisted for this vbucket
		if IsKeyNotFoundError(r.bucket, err) {
			return []byte{}, 0, 0, nil
		} else {
			return []byte{}, 0, 0, err
		}
	}

	var snapshotMetadata cbdatasource.VBucketMetaData
	unmarshalErr := json.Unmarshal(rawValue, &snapshotMetadata)
	if unmarshalErr != nil {
		return []byte{}, 0, 0, err
	}
	return rawValue, snapshotMetadata.SnapStart, snapshotMetadata.SnapEnd, nil

}

func (r *DCPReceiver) initMetadata(maxVbNo uint16) {
	r.m.Lock()
	defer r.m.Unlock()

	// Check for persisted backfill sequences
	backfillSeqs, err := r.backfill.loadBackfillSequences(r.bucket)
	if err != nil {
		// Backfill sequences not present or invalid - will use metadata only
		backfillSeqs = nil
	}

	// Load persisted metadata
	for i := uint16(0); i < maxVbNo; i++ {
		metadata, snapStart, snapEnd, err := r.loadCheckpoint(i)
		if err != nil {
			Warnf(KeyAll, "Unexpected error attempting to load DCP checkpoint for vbucket %d.  Will restart DCP for that vbucket from zero.  Error: %v", i, err)
			r.meta[i] = []byte{}
			r.seqs[i] = 0
		} else {
			r.meta[i] = metadata
			r.seqs[i] = snapStart
			// Check whether we persisted a sequence midway through a previous incomplete backfill
			if backfillSeqs != nil {
				var partialBackfillSequence uint64
				if backfillSeqs.Seqs[i] < backfillSeqs.SnapEnd[i] {
					partialBackfillSequence = backfillSeqs.Seqs[i]
				}
				// If we have a backfill sequence later than the DCP checkpoint's snapStart, start from there
				if partialBackfillSequence > snapStart {
					Infof(KeyDCP, "Restarting vb %d using backfill sequence %d ([%d-%d])", i, partialBackfillSequence, backfillSeqs.SnapStart[i], backfillSeqs.SnapEnd[i])
					r.seqs[i] = partialBackfillSequence
					r.meta[i] = makeVbucketMetadata(r.vbuuids[i], partialBackfillSequence, backfillSeqs.SnapStart[i], backfillSeqs.SnapEnd[i])
				} else {
					Infof(KeyDCP, "Restarting vb %d using metadata sequence %d  (backfill %d not in [%d-%d])", i, snapStart, partialBackfillSequence, snapStart, snapEnd)
				}
			}
		}
	}

}

// Initializes DCP Feed.  Determines starting position based on feed type.
func (r *DCPReceiver) initFeed(backfillType uint64) error {

	statsUuids, highSeqnos, err := r.bucket.GetStatsVbSeqno(r.maxVbNo, false)
	if err != nil {
		return pkgerrors.Wrap(err, "Error retrieving stats-vbseqno - DCP not supported")
	}

	r.vbuuids = statsUuids

	switch backfillType {
	case sgbucket.FeedNoBackfill:
		// For non-backfill, use vbucket uuids, high sequence numbers
		Debugf(KeyDCP, "Initializing DCP with no backfill - seeding seqnos: %v", highSeqnos)
		r.SeedSeqnos(statsUuids, highSeqnos)
	case sgbucket.FeedResume:
		// For resume case, load previously persisted checkpoints from bucket
		r.initMetadata(r.maxVbNo)
		// Track backfill (from persisted checkpoints to current high seqno)
		r.backfill.init(r.seqs, highSeqnos, r.maxVbNo)
		Debugf(KeyDCP, "Initializing DCP feed based on persisted checkpoints")
	default:
		// Otherwise, start feed from zero
		startSeqnos := make(map[uint16]uint64, r.maxVbNo)
		vbuuids := make(map[uint16]uint64, r.maxVbNo)
		r.SeedSeqnos(vbuuids, startSeqnos)
		// Track backfill (from zero to current high seqno)
		r.backfill.init(r.seqs, highSeqnos, r.maxVbNo)
		Debugf(KeyDCP, "Initializing DCP feed to start from zero")
	}
	return nil
}

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPLoggingReceiver struct {
	rec Receiver
}

func (r *DCPLoggingReceiver) OnError(err error) {
	Infof(KeyDCP, "OnError: %v", err)
	r.rec.OnError(err)
}

func (r *DCPLoggingReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	Tracef(KeyDCP, "DataUpdate:%d, %s, %d, %v", vbucketId, UD(string(key)), seq, UD(req))
	return r.rec.DataUpdate(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) SetBucketNotifyFn(notify sgbucket.BucketNotifyFn) {
	Tracef(KeyDCP, "SetBucketNotifyFn()")
	r.rec.SetBucketNotifyFn(notify)
}

func (r *DCPLoggingReceiver) GetBucketNotifyFn() sgbucket.BucketNotifyFn {
	Tracef(KeyDCP, "GetBucketNotifyFn()")
	return r.rec.GetBucketNotifyFn()
}

func (r *DCPLoggingReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	Tracef(KeyDCP, "DataDelete:%d, %s, %d, %v", vbucketId, UD(string(key)), seq, UD(req))
	return r.rec.DataDelete(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	Infof(KeyDCP, "Rollback:%d, %d", vbucketId, rollbackSeq)
	return r.rec.Rollback(vbucketId, rollbackSeq)
}

func (r *DCPLoggingReceiver) SetMetaData(vbucketId uint16, value []byte) error {
	Tracef(KeyDCP, "SetMetaData:%d, %s", vbucketId, value)
	return r.rec.SetMetaData(vbucketId, value)
}

func (r *DCPLoggingReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	Tracef(KeyDCP, "GetMetaData:%d", vbucketId)
	return r.rec.GetMetaData(vbucketId)
}

func (r *DCPLoggingReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	Tracef(KeyDCP, "SnapshotStart:%d, %d, %d, %d", vbucketId, snapStart, snapEnd, snapType)
	return r.rec.SnapshotStart(vbucketId, snapStart, snapEnd, snapType)
}

func (r *DCPLoggingReceiver) SeedSeqnos(uuids map[uint16]uint64, seqs map[uint16]uint64) {
	Tracef(KeyDCP, "SeedSeqnos:%v, %v", uuids, seqs)
	r.rec.SeedSeqnos(uuids, seqs)
}

func (r *DCPLoggingReceiver) updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool) {
	r.rec.updateSeq(vbucketId, seq, warnOnLowerSeqNo)
}

func (r *DCPLoggingReceiver) initFeed(feedType uint64) error {
	return r.rec.initFeed(feedType)
}

// NoPasswordAuthHandler is used for client cert-based auth
type NoPasswordAuthHandler struct {
	handler AuthHandler
}

func (nph NoPasswordAuthHandler) GetCredentials() (username string, password string, bucketname string) {
	_, _, bucketname = nph.handler.GetCredentials()
	return "", "", bucketname
}

// This starts a cbdatasource powered DCP Feed using an entirely separate connection to Couchbase Server than anything the existing
// bucket is using, and it uses the go-couchbase cbdatasource DCP abstraction layer
func StartDCPFeed(bucket Bucket, spec BucketSpec, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc) error {

	// Recommended usage of cbdatasource is to let it manage it's own dedicated connection, so we're not
	// reusing the bucket connection we've already established.
	urls, errConvertServerSpec := CouchbaseURIToHttpURL(bucket, spec.Server)
	if errConvertServerSpec != nil {
		return errConvertServerSpec
	}

	poolName := spec.PoolName
	if poolName == "" {
		poolName = "default"
	}
	bucketName := spec.BucketName

	vbucketIdsArr := []uint16(nil) // nil means get all the vbuckets.

	maxVbno, err := bucket.GetMaxVbno()
	if err != nil {
		return err
	}

	persistCheckpoints := false
	if args.Backfill == sgbucket.FeedResume {
		persistCheckpoints = true
	}

	dcpReceiver, err := NewDCPReceiver(callback, bucket, maxVbno, persistCheckpoints, args.Backfill)
	if err != nil {
		return err
	}

	dcpReceiver.SetBucketNotifyFn(args.Notify)

	// Initialize the feed based on the backfill type
	feedInitErr := dcpReceiver.initFeed(args.Backfill)
	if feedInitErr != nil {
		return feedInitErr
	}

	dataSourceOptions := cbdatasource.DefaultBucketDataSourceOptions
	if spec.UseXattrs {
		dataSourceOptions.IncludeXAttrs = true
	}

	dataSourceOptions.Logf = func(fmt string, v ...interface{}) {
		Debugf(KeyDCP, fmt, v...)
	}

	dataSourceOptions.Name = GenerateDcpStreamName("SG")

	auth := spec.Auth

	// If using client certificate for authentication, configure go-couchbase for cbdatasource's initial
	// connection to retrieve cluster configuration.
	if spec.Certpath != "" {
		couchbase.SetCertFile(spec.Certpath)
		couchbase.SetKeyFile(spec.Keypath)
		couchbase.SetRootFile(spec.CACertPath)
		couchbase.SetSkipVerify(false)
		auth = NoPasswordAuthHandler{handler: spec.Auth}
	}

	// If using TLS, pass a custom connect method to support using TLS for cbdatasource's memcached connections
	if spec.IsTLS() {
		dataSourceOptions.Connect = spec.TLSConnect

	}

	Debugf(KeyDCP, "Connecting to new bucket datasource.  URLs:%s, pool:%s, bucket:%s", MD(urls), MD(poolName), MD(bucketName))

	bds, err := cbdatasource.NewBucketDataSource(
		urls,
		poolName,
		bucketName,
		"",
		vbucketIdsArr,
		auth,
		dcpReceiver,
		dataSourceOptions,
	)
	if err != nil {
		return pkgerrors.WithStack(RedactErrorf("Error connecting to new bucket cbdatasource.  URLs:%s, pool:%s, bucket:%s.  Error: %v", MD(urls), MD(poolName), MD(bucketName), err))
	}

	if err = bds.Start(); err != nil {
		return pkgerrors.WithStack(RedactErrorf("Error starting bucket cbdatasource.  URLs:%s, pool:%s, bucket:%s.  Error: %v", MD(urls), MD(poolName), MD(bucketName), err))
	}

	// Close the data source if feed terminator is closed
	if args.Terminator != nil {
		go func() {
			<-args.Terminator
			Tracef(KeyDCP, "Closing DCP Feed based on termination notification")
			bds.Close()
		}()
	}

	return nil

}

// Create a prefix that will be used to create the dcp stream name, which must be globally unique
// in order to avoid https://issues.couchbase.com/browse/MB-24237.  It's also useful to have the Sync Gateway
// version number / commit for debugging purposes
func GenerateDcpStreamName(product string) string {

	// Create a time-based UUID for uniqueness of DCP Stream Names
	u, err := uuid.NewUUID()
	if err != nil {
		// Current implementation of uuid.NewUUID *never* returns an error.
		Errorf(KeyAll, "Error generating UUID for DCP Stream Name: %v", err)
	}

	commitTruncated := StringPrefix(GitCommit, 7)

	return fmt.Sprintf(
		"%v-v-%v-commit-%v-uuid-%v",
		product,
		VersionNumber,
		commitTruncated,
		u.String(),
	)

}

// BackfillStatus manages tracking of DCP backfill progress, to provide diagnostics and mid-snapshot restart capability
type backfillStatus struct {
	active            bool      // Whether this DCP feed is in backfill
	vbActive          []bool    // Whether a vbucket is in backfill
	receivedSequences uint64    // Number of backfill sequences received
	expectedSequences uint64    // Expected number of sequences in backfill
	endSeqs           []uint64  // Backfill complete sequences, indexed by vbno
	snapStart         []uint64  // Start sequence of current backfill snapshot
	snapEnd           []uint64  // End sequence of current backfill snapshot
	lastPersistTime   time.Time // The last time backfill stats were emitted (log, expvar)
}

func (b *backfillStatus) init(start []uint64, end map[uint16]uint64, maxVbNo uint16) {
	b.vbActive = make([]bool, maxVbNo)
	b.snapStart = make([]uint64, maxVbNo)
	b.snapEnd = make([]uint64, maxVbNo)
	b.endSeqs = make([]uint64, maxVbNo)

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
	totalVar := &expvar.Int{}
	completedVar := &expvar.Int{}
	totalVar.Set(int64(b.expectedSequences))
	completedVar.Set(0)
	dcpExpvars.Set("backfill_expected", totalVar)
	dcpExpvars.Set("backfill_completed", completedVar)
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
	dcpExpvars.Add("backfill_completed", int64(backfillDelta))

	// Check if it's time to persist and log backfill progress
	if time.Since(b.lastPersistTime) > kBackfillPersistInterval {
		b.lastPersistTime = time.Now()
		b.persistBackfillSequences(bucket, currentSequences)
		b.logBackfillProgress()
	}

	// If backfill is complete, log and do backfill inactivation/cleanup
	if b.receivedSequences >= b.expectedSequences {
		Infof(KeyDCP, "Backfill complete")
		b.active = false
		b.purgeBackfillSequences(bucket)
	}
}

// Logs current backfill progress.  Expects caller to have the lock on r.m
func (b *backfillStatus) logBackfillProgress() {
	if !b.active {
		return
	}
	Infof(KeyDCP, "Backfill in progress: %d%% (%d / %d)", int(b.receivedSequences*100/b.expectedSequences), b.receivedSequences, b.expectedSequences)
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
	return bucket.Set(DCPBackfillSeqs, 0, backfillSeqs)
}

func (b *backfillStatus) loadBackfillSequences(bucket Bucket) (*BackfillSequences, error) {
	var backfillSeqs BackfillSequences
	_, err := bucket.Get(DCPBackfillSeqs, &backfillSeqs)
	if err != nil {
		return nil, err
	}
	Infof(KeyDCP, "Previously persisted backfill sequences found - will resume")
	return &backfillSeqs, nil
}

func (b *backfillStatus) purgeBackfillSequences(bucket Bucket) error {
	return bucket.Delete(DCPBackfillSeqs)
}
