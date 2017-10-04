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
	"errors"
	"expvar"
	"fmt"
	"sync"

	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/satori/go.uuid"
	"sync/atomic"
)

var dcpExpvars *expvar.Map

var dcpReceiverId int32

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

const DCPCheckpointPrefix = "_sync:dcp_ck:" // Prefix used for DCP checkpoint persistence (is appended with vbno)

// Number of non-checkpoint updates per vbucket required to trigger metadata persistence.  Must be greater than zero to avoid
// retriggering persistence solely based on checkpoint doc echo.
// Based on ad-hoc testing w/ travel-sample bucket, increasing this value doesn't significantly improve performance, since under load
// DCP will already be sending more documents per snapshot.
const kCheckpointThreshold = 1

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
	initMetadata(maxVbNo uint16)
}

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPReceiver struct {
	id int32
	m                      sync.Mutex
	bucket                 Bucket                         // For metadata persistence/retrieval
	persistCheckpoints     bool                           // Whether this DCPReceiver should persist metadata to the bucket
	seqs                   []uint64                       // To track max seq #'s we received per vbucketId.
	meta                   [][]byte                       // To track metadata blob's per vbucketId.
	updatesSinceCheckpoint []uint64                       // Number of updates since the last checkpoint. Used to avoid checkpoint persistence feedback loop
	notify                 sgbucket.BucketNotifyFn        // Function to callback when we lose our dcp feed
	callback               sgbucket.FeedEventCallbackFunc // Function to callback for mutation processing
}

func NewDCPReceiver(callback sgbucket.FeedEventCallbackFunc, bucket Bucket, maxVbNo uint16, persistCheckpoints bool) Receiver {

	dcpReceiverId := atomic.AddInt32(&dcpReceiverId, 1)

	// TODO: set using maxvbno
	r := &DCPReceiver{
		id: dcpReceiverId,
		bucket:             bucket,
		persistCheckpoints: persistCheckpoints,
		seqs:               make([]uint64, maxVbNo),
		meta:               make([][]byte, maxVbNo),
		updatesSinceCheckpoint: make([]uint64, maxVbNo),
	}
	r.callback = callback

	if LogEnabledExcludingLogStar("DCP") {
		LogTo("DCP", "Using DCP Logging Receiver.  Receiver ID: %d", r.id)
		logRec := &DCPLoggingReceiver{rec: r}
		return logRec
	}
	return r
}

func (r *DCPReceiver) SetBucketNotifyFn(notify sgbucket.BucketNotifyFn) {
	r.notify = notify
}

func (r *DCPReceiver) GetBucketNotifyFn() sgbucket.BucketNotifyFn {
	return r.notify
}

func (r *DCPReceiver) OnError(err error) {
	Warn("Error processing DCP stream - will attempt to restart/reconnect: %v.  Receiver ID: %d", err, r.id)

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
		Sequence:    rq.Cas,
		DataType:    rq.DataType,
		Cas:         rq.Cas,
		Synchronous: true,
	}
	return event
}

func (r *DCPReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	// TODO: On snapshot start, could persist high sequence information when in a bucket shadowing
	// scenario, to support restart.  Not yet implemented due to concerns about impact of persistence
	// on the shadowing DCP feed, as the SnapshotStart gets issued per vbucket.  It's not clear that the
	// performance benefit on SG restart outweighs the performance impact during regular processing.
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
			Warn("Unable to persist DCP metadata - will retry next snapshot. Error: %v", err)
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
	Warn("DCP Rollback request.  Expected RollbackEx call - resetting vbucket %d to 0.  Receiver: %d", vbucketId, r.id)
	dcpExpvars.Add("rollback_count", 1)
	r.updateSeq(vbucketId, 0, false)
	r.SetMetaData(vbucketId, nil)

	return nil
}

// RollbackEx includes the vbucketUUID needed to reset the metadata correctly
func (r *DCPReceiver) RollbackEx(vbucketId uint16, vbucketUUID uint64, rollbackSeq uint64) error {
	Warn("DCP RollbackEx request - rolling back DCP feed for: vbucketId: %d, rollbackSeq: %x.  Receiver: %d", vbucketId, rollbackSeq, r.id)

	dcpExpvars.Add("rollback_count", 1)
	r.updateSeq(vbucketId, rollbackSeq, false)
	r.SetMetaData(vbucketId, makeVbucketMetadata(vbucketUUID, rollbackSeq))
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

	if seq < r.seqs[vbucketId] && warnOnLowerSeqNo == true {
		Warn("Setting to _lower_ sequence number than previous: %v -> %v", r.seqs[vbucketId], seq)
	}

	r.seqs[vbucketId] = seq // Remember the max seq for GetMetaData().

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
		r.meta[vbucketId] = makeVbucketMetadata(uuid, seqs[vbucketId])
	}
}

// Create VBucketMetadata, marshalled to []byte
func makeVbucketMetadata(vbucketUUUID uint64, sequence uint64) []byte {
	failOver := make([][]uint64, 1)
	failOverEntry := []uint64{vbucketUUUID, 0}
	failOver[0] = failOverEntry
	metadata := &cbdatasource.VBucketMetaData{
		SeqStart:    sequence,
		SeqEnd:      uint64(0xFFFFFFFFFFFFFFFF),
		SnapStart:   sequence,
		SnapEnd:     sequence,
		FailOverLog: failOver,
	}
	metadataBytes, err := json.Marshal(metadata)
	if err == nil {
		return metadataBytes
	} else {
		return []byte{}
	}
}

// TODO: Convert checkpoint persistence to an asynchronous batched process, since
//       restarting w/ an older checkpoint:
//         - Would only result in some repeated entry processing, which is already handled by the indexer
//         - Is a relatively infrequent operation (occurs when vbuckets are initially assigned to an accel node)
func (r *DCPReceiver) persistCheckpoint(vbNo uint16, value []byte) error {
	dcpExpvars.Add("persistCheckpoint_count", 1)
	return r.bucket.SetRaw(fmt.Sprintf("%s%d", DCPCheckpointPrefix, vbNo), 0, value)
}

// loadCheckpoint retrieves previously persisted DCP metadata.  Need to unmarshal metadata to determine last sequence processed.
// We always restart the feed from the last persisted snapshot start (as opposed to a sequence we may have processed
// midway through the checkpoint), because:
//   - We don't otherwise persist the last sequence we processed
//   - For SG feed processing, there's no harm if we receive feed events for mutations we've previously seen
//   - The ongoing performance overhead of persisting last sequence outweighs the minor performance benefit of not reprocessing a few
//    sequences in a checkpoint on startup
func (r *DCPReceiver) loadCheckpoint(vbNo uint16) (vbMetadata []byte, snapshotStartSeq uint64, err error) {
	dcpExpvars.Add("loadCheckpoint_count", 1)
	rawValue, _, err := r.bucket.GetRaw(fmt.Sprintf("%s%d", DCPCheckpointPrefix, vbNo))
	if err != nil {
		// On a key not found error, metadata hasn't been persisted for this vbucket
		if IsKeyNotFoundError(r.bucket, err) {
			return []byte{}, 0, nil
		} else {
			return []byte{}, 0, err
		}
	}

	var snapshotMetadata cbdatasource.VBucketMetaData
	unmarshalErr := json.Unmarshal(rawValue, &snapshotMetadata)
	if unmarshalErr != nil {
		return []byte{}, 0, err
	}
	return rawValue, snapshotMetadata.SnapStart, nil

}

func (r *DCPReceiver) initMetadata(maxVbNo uint16) {
	r.m.Lock()
	defer r.m.Unlock()
	for i := uint16(0); i < maxVbNo; i++ {
		metadata, lastSeq, err := r.loadCheckpoint(i)
		if err != nil {
			// TODO: instead of failing here, should log warning and init zero metadata/seq
			Warn("Unexpected error attempting to load DCP checkpoint for vbucket %d.  Will restart DCP for that checkpoint from zero.  Error: %v", err)
			r.meta[i] = []byte{}
			r.seqs[i] = 0
		} else {
			r.meta[i] = metadata
			r.seqs[i] = lastSeq
		}
	}
}

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPLoggingReceiver struct {
	rec Receiver
}

func (r *DCPLoggingReceiver) OnError(err error) {
	LogTo("DCP", "OnError: %v", err)
	r.rec.OnError(err)
}

func (r *DCPLoggingReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	LogTo("DCP+", "DataUpdate:%d, %s, %d, %v", vbucketId, key, seq, req)
	return r.rec.DataUpdate(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) SetBucketNotifyFn(notify sgbucket.BucketNotifyFn) {
	LogTo("DCP", "SetBucketNotifyFn()")
	r.rec.SetBucketNotifyFn(notify)
}

func (r *DCPLoggingReceiver) GetBucketNotifyFn() sgbucket.BucketNotifyFn {
	LogTo("DCP", "GetBucketNotifyFn()")
	return r.rec.GetBucketNotifyFn()
}

func (r *DCPLoggingReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	LogTo("DCP", "DataDelete:%d, %s, %d, %v", vbucketId, key, seq, req)
	return r.rec.DataDelete(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	LogTo("DCP", "Rollback:%d, %d", vbucketId, rollbackSeq)
	return r.rec.Rollback(vbucketId, rollbackSeq)
}

func (r *DCPLoggingReceiver) SetMetaData(vbucketId uint16, value []byte) error {

	LogTo("DCP+", "SetMetaData:%d, %s", vbucketId, value)
	return r.rec.SetMetaData(vbucketId, value)
}

func (r *DCPLoggingReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	LogTo("DCP+", "GetMetaData:%d", vbucketId)
	return r.rec.GetMetaData(vbucketId)
}

func (r *DCPLoggingReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	LogTo("DCP+", "SnapshotStart:%d, %d, %d, %d", vbucketId, snapStart, snapEnd, snapType)
	return r.rec.SnapshotStart(vbucketId, snapStart, snapEnd, snapType)
}

func (r *DCPLoggingReceiver) SeedSeqnos(uuids map[uint16]uint64, seqs map[uint16]uint64) {
	LogTo("DCP+", "SeedSeqnos:%v, %v", uuids, seqs)
	r.rec.SeedSeqnos(uuids, seqs)
}

func (r *DCPLoggingReceiver) updateSeq(vbucketId uint16, seq uint64, warnOnLowerSeqNo bool) {
	r.rec.updateSeq(vbucketId, seq, warnOnLowerSeqNo)
}

func (r *DCPLoggingReceiver) initMetadata(maxVbNo uint16) {
	LogTo("DCP", "Initializing metadata:%d", maxVbNo)
	r.rec.initMetadata(maxVbNo)
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

	dcpReceiver := NewDCPReceiver(callback, bucket, maxVbno, persistCheckpoints)
	dcpReceiver.SetBucketNotifyFn(args.Notify)

	// GetStatsVbSeqno retrieves high sequence number for each vbucket, to enable starting
	// DCP stream from that position.  Also being used as a check on whether the server supports
	// DCP.

	switch args.Backfill {
	case sgbucket.FeedNoBackfill:
		// For non-backfill, use vbucket uuids, high sequence numbers
		statsUuids, highSeqnos, err := bucket.GetStatsVbSeqno(maxVbno, false)
		if err != nil {
			return errors.New("Error retrieving stats-vbseqno - DCP not supported")
		}
		LogTo("Feed+", "Seeding seqnos: %v", highSeqnos)
		dcpReceiver.SeedSeqnos(statsUuids, highSeqnos)
	case sgbucket.FeedResume:
		// For resume case, load previously persisted checkpoints from bucket
		dcpReceiver.initMetadata(maxVbno)
	default:
		// Otherwise, start feed from zero
		startSeqnos := make(map[uint16]uint64, maxVbno)
		vbuuids := make(map[uint16]uint64, maxVbno)
		dcpReceiver.SeedSeqnos(vbuuids, startSeqnos)
	}

	dataSourceOptions := cbdatasource.DefaultBucketDataSourceOptions
	if spec.UseXattrs {
		dataSourceOptions.IncludeXAttrs = true
	}

	dataSourceOptions.Name = GenerateDcpStreamName("SG")

	LogTo("Feed+", "Connecting to new bucket datasource.  URLs:%s, pool:%s, bucket:%s", urls, poolName, bucketName)
	bds, err := cbdatasource.NewBucketDataSource(
		urls,
		poolName,
		bucketName,
		"",
		vbucketIdsArr,
		spec.Auth,
		dcpReceiver,
		dataSourceOptions,
	)
	if err != nil {
		return err
	}

	if err = bds.Start(); err != nil {
		return err
	}

	// Close the data source if feed terminator is closed
	if args.Terminator != nil {
		go func() {
			<-args.Terminator
			LogTo("Feed+", "Closing DCP Feed based on termination notification")
			bds.Close()
		}()
	}

	return nil

}

// Create a prefix that will be used to create the dcp stream name, which must be globally unique
// in order to avoid https://issues.couchbase.com/browse/MB-24237.  It's also useful to have the Sync Gateway
// version number / commit for debugging purposes
func GenerateDcpStreamName(product string) string {

	// Use V2 since it takes the timestamp into account (as opposed to V4 which appears that it would
	// require the random number generator to be seeded), so that it's more likely to be unique across different processes.
	uuidComponent := uuid.NewV2(uuid.DomainPerson).String()

	commitTruncated := StringPrefix(GitCommit, 7)

	return fmt.Sprintf(
		"%v-v-%v-commit-%v-uuid-%v",
		product,
		VersionNumber,
		commitTruncated,
		uuidComponent,
	)

}
