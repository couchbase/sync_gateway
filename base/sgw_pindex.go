package base

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"strconv"
	"sync"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/go-couchbase/cbdatasource"
	sgbucket "github.com/couchbase/sg-bucket"
)

// The two "handles" we have for CBGT are the manager and Cfg objects.
// This struct makes it easy to pass them around together as a unit.
type CbgtContext struct {
	Manager *cbgt.Manager
	Cfg     cbgt.Cfg
}

type SyncGatewayIndexParams struct {
	BucketName string `json:"bucket_name"`
}

const (
	SourceTypeCouchbase      = "couchbase"
	IndexTypeSyncGateway     = "sync_gateway" // Used by CBGT for its data path
	IndexCategorySyncGateway = "general"      // CBGT expects this index to fit into a category (general vs advanced)
)

type CBGTDCPFeed struct {
	eventFeed chan sgbucket.TapEvent
}

func (c *CBGTDCPFeed) Events() <-chan sgbucket.TapEvent {
	return c.eventFeed
}

func (c *CBGTDCPFeed) WriteEvents() chan<- sgbucket.TapEvent {
	return c.eventFeed
}

func (c *CBGTDCPFeed) Close() error { // TODO
	log.Fatalf("CBGTDCPFeed.Close() called but not implemented")
	return nil
}

type SyncGatewayPIndex struct {
	mutex        sync.Mutex               // mutex used to protect meta and seqs
	seqs         map[uint16]uint64        // To track max seq #'s we received per partition (vbucketId).
	meta         map[uint16][]byte        // To track metadata blob's per partition (vbucketId).
	feedEvents   chan<- sgbucket.TapEvent // The channel to forward TapEvents
	bucket       CouchbaseBucket          // the couchbase bucket
	tapArguments sgbucket.TapArguments    // tap args
	stableClock  SequenceClock            // The stable clock when this PIndex object was created
}

func NewSyncGatewayPIndex(feedEvents chan<- sgbucket.TapEvent, bucket CouchbaseBucket, args sgbucket.TapArguments, stableClock SequenceClock) *SyncGatewayPIndex {
	pindex := &SyncGatewayPIndex{
		feedEvents:   feedEvents,
		bucket:       bucket,
		tapArguments: args,
		stableClock:  stableClock,
	}

	if err := pindex.SeedSeqnos(); err != nil {
		log.Fatalf("Error calling SeedSeqnos for pindex: %v", err)
	}

	return pindex
}

func (s *SyncGatewayPIndex) SeedSeqnos() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	maxVbno, err := s.bucket.GetMaxVbno()
	if err != nil {
		return err
	}

	startSeqnos := make(map[uint16]uint64, maxVbno)
	vbuuids := make(map[uint16]uint64, maxVbno)

	highSeqnos := s.stableClock.ValueAsMap()

	// GetStatsVbSeqno retrieves high sequence number for each vbucket, to enable starting
	// DCP stream from that position.  Also being used as a check on whether the server supports
	// DCP.
	statsUuids, highSeqnosFromBucket, err := s.bucket.GetStatsVbSeqno(maxVbno, true)
	if err != nil {
		return errors.New("Error retrieving stats-vbseqno - DCP not supported")
	}

	if s.tapArguments.Backfill == sgbucket.TapNoBackfill {
		// For non-backfill, use vbucket uuids, high sequence numbers
		LogTo("Feed+", "Seeding seqnos: %v", highSeqnos)
		vbuuids = statsUuids
		startSeqnos = highSeqnos
	}

	// Set the high seqnos as-is
	s.seqs = startSeqnos

	// For metadata, we need to do more work to build metadata based on uuid and map values.  This
	// isn't strictly to the design of cbdatasource.Receiver, which intends metadata to be opaque, but
	// is required in order to have the BucketDataSource start the UPRStream as needed.
	// The implementation has been reviewed with the cbdatasource owners and they agree this is a
	// reasonable approach, as the structure of VBucketMetaData is expected to rarely change.
	for vbucketId, vbuuid := range vbuuids {

		failOver := make([][]uint64, 1)
		failOverEntry := []uint64{vbuuid, 0}
		failOver[0] = failOverEntry

		highSeqnoFromStableClock := s.seqs[vbucketId]
		highSeqnoFromBucket := highSeqnosFromBucket[vbucketId]

		if highSeqnoFromStableClock > highSeqnoFromBucket {
			Warn("issue_1259 highSeqnoFromStableClock (%d) > highSeqnoFromBucket (%d) for vb %d", highSeqnoFromStableClock, highSeqnoFromBucket, vbucketId)
		}

		metadata := &cbdatasource.VBucketMetaData{
			SeqStart:    s.seqs[vbucketId],
			SeqEnd:      uint64(0xFFFFFFFFFFFFFFFF),
			SnapStart:   s.seqs[vbucketId],
			SnapEnd:     s.seqs[vbucketId],
			FailOverLog: failOver,
		}
		buf, err := json.Marshal(metadata)
		if err == nil {
			if s.meta == nil {
				s.meta = make(map[uint16][]byte)
			}
			s.meta[vbucketId] = buf
		}
	}

	return nil

}

func (s *SyncGatewayPIndex) Close() error {
	return nil
}

// CBGT gives us "partition" which is a more generic version of "VbucketId".
// The partition is in string form (to be more generic), but we want numeric VBucketId's,
// so convert here.
func partitionToVbucketId(partition string) uint16 {

	vbucketNumber, err := strconv.ParseUint(partition, 10, 16) // base 10, 16 bit uint
	if err != nil {
		log.Fatalf("Expected a numeric vbucket (partition), got %v.  Err: %v", partition, err)
	}
	return uint16(vbucketNumber)

}

func (s *SyncGatewayPIndex) DataUpdate(partition string, key []byte, seq uint64, val []byte,
	cas uint64, extrasType cbgt.DestExtrasType, extras []byte) error {

	LogTo("DCP", "DataUpdate for pindex %p called with vbucket: %v.  key: %v seq: %v", s, partition, string(key), seq)

	vbucketNumber := partitionToVbucketId(partition)

	s.updateSeq(partition, seq, true)

	event := sgbucket.TapEvent{
		Opcode:   sgbucket.TapMutation,
		Key:      key,
		Value:    val,
		Sequence: seq,
		VbNo:     vbucketNumber,
	}

	s.feedEvents <- event

	return nil
}

func (s *SyncGatewayPIndex) DataDelete(partition string, key []byte, seq uint64,
	cas uint64, extrasType cbgt.DestExtrasType, extras []byte) error {

	LogTo("DCP", "DataDelete called with vbucket: %v.  key: %v", partition, string(key))

	s.updateSeq(partition, seq, true)

	event := sgbucket.TapEvent{
		Opcode:   sgbucket.TapDeletion,
		Key:      key,
		Sequence: seq,
	}

	s.feedEvents <- event

	return nil
}

func (s *SyncGatewayPIndex) SnapshotStart(partition string, snapStart, snapEnd uint64) error {

	return nil

}

// OpaqueGet() should return the opaque value previously
// provided by an earlier call to OpaqueSet().  If there was no
// previous call to OpaqueSet(), such as in the case of a brand
// new instance of a Dest (as opposed to a restarted or reloaded
// Dest), the Dest should return (nil, 0, nil) for (value,
// lastSeq, err), respectively.  The lastSeq should be the last
// sequence number received and persisted during calls to the
// Dest's DataUpdate() & DataDelete() methods.
func (s *SyncGatewayPIndex) OpaqueGet(partition string) (value []byte, lastSeq uint64, err error) {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	vbucketNumber := partitionToVbucketId(partition)

	value = []byte(nil)
	if s.meta != nil {
		value = s.meta[vbucketNumber]
	}

	if s.seqs != nil {
		lastSeq = s.seqs[vbucketNumber]
	}

	return value, lastSeq, nil

}

// The Dest implementation should persist the value parameter of
// OpaqueSet() for retrieval during some future call to
// OpaqueGet() by the system.  The metadata value should be
// considered "in-stream", or as part of the sequence history of
// mutations.  That is, a later Rollback() to some previous
// sequence number for a particular partition should rollback
// both persisted metadata and regular data.  The Dest
// implementation should make its own copy of the value data.
func (s *SyncGatewayPIndex) OpaqueSet(partition string, value []byte) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	vbucketNumber := partitionToVbucketId(partition)

	if s.meta == nil {
		s.meta = make(map[uint16][]byte)
	}
	s.meta[vbucketNumber] = value
	return nil
}

func (s *SyncGatewayPIndex) rollbackSeq(partition string, seq uint64) {

	s.updateSeq(partition, seq, false)

	if err := s.updateMeta(partition, seq); err != nil {
		Warn("RollbackSeq() unable to update meta: %v", err)
	}

}

func (s *SyncGatewayPIndex) updateMeta(partition string, seq uint64) error {

	s.mutex.Lock()
	defer s.mutex.Unlock()

	maxVbno, err := s.bucket.GetMaxVbno()
	if err != nil {
		return err
	}

	// GetStatsVbSeqno retrieves high sequence number for each vbucket, to enable starting
	// DCP stream from that position.
	vbuuids, _, err := s.bucket.GetStatsVbSeqno(maxVbno, true)
	if err != nil {
		return errors.New("Error retrieving stats-vbseqno - DCP not supported")
	}

	vbucketId := partitionToVbucketId(partition)
	vbuuid := vbuuids[vbucketId]

	failOver := make([][]uint64, 1)
	failOverEntry := []uint64{vbuuid, 0}
	failOver[0] = failOverEntry
	metadata := &cbdatasource.VBucketMetaData{
		SeqStart:    seq,
		SeqEnd:      uint64(0xFFFFFFFFFFFFFFFF),
		SnapStart:   seq,
		SnapEnd:     seq,
		FailOverLog: failOver,
	}
	buf, err := json.Marshal(metadata)
	if err == nil {
		if s.meta == nil {
			s.meta = make(map[uint16][]byte)
		}
		s.meta[vbucketId] = buf
	}

	return nil

}

// This updates the value stored in s.seqs with the given seq number for the given partition
// (which is a string value of vbucket id).  Setting warnOnLowerSeqNo to true will check
// if we are setting the seq number to a _lower_ value than we already have stored for that
// vbucket and log a warning in that case.  The valid case for setting warnOnLowerSeqNo to
// false is when it's a rollback scenario.  See https://github.com/couchbase/sync_gateway/issues/1098 for dev notes.
func (s *SyncGatewayPIndex) updateSeq(partition string, seq uint64, warnOnLowerSeqNo bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	vbucketNumber := partitionToVbucketId(partition)

	if s.seqs == nil {
		s.seqs = make(map[uint16]uint64)
	}
	if seq < s.seqs[vbucketNumber] && warnOnLowerSeqNo == true {
		Warn("Setting to _lower_ sequence number than previous: %v -> %v", s.seqs[vbucketNumber], seq)
	}

	s.seqs[vbucketNumber] = seq // Remember the max seq for GetMetaData().

}

func (s *SyncGatewayPIndex) Rollback(partition string, rollbackSeq uint64) error {

	// TODO: this should rollback the relevant metadata too (vals set via OpaqueSet())
	// As of the time of this writing, I believe this is also broken in the master branch
	// of Sync Gateway.

	Warn("DCP Rollback request SyncGatewayPIndex - rolling back DCP feed for: vbucketId: %s, rollbackSeq: %x", partition, rollbackSeq)

	s.rollbackSeq(partition, rollbackSeq)

	return nil
}

func (s *SyncGatewayPIndex) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string,
	consistencySeq uint64,
	cancelCh <-chan bool) error {
	return nil
}

func (s *SyncGatewayPIndex) Count(pindex *cbgt.PIndex, cancelCh <-chan bool) (uint64, error) {
	return 0, nil
}

func (s *SyncGatewayPIndex) Query(pindex *cbgt.PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	return nil
}

func (s *SyncGatewayPIndex) Stats(w io.Writer) error {
	_, err := w.Write(cbgt.JsonNULL)
	return err
}

// When we detect other nodes have stopped pushing heartbeats, remove from CBGT cluster
type HeartbeatStoppedHandler struct {
	Cfg         cbgt.Cfg
	Manager     *cbgt.Manager
	CbgtVersion string
}

func (h HeartbeatStoppedHandler) StaleHeartBeatDetected(nodeUuid string) {

	LogTo("DIndex+", "StaleHeartBeatDetected for node: %v", nodeUuid)

	kinds := []string{cbgt.NODE_DEFS_KNOWN, cbgt.NODE_DEFS_WANTED}
	for _, kind := range kinds {
		LogTo("DIndex+", "Telling CBGT to remove node: %v (kind: %v, cbgt version: %v)", nodeUuid, kind, h.CbgtVersion)
		if err := cbgt.CfgRemoveNodeDef(
			h.Cfg,
			kind,
			nodeUuid,
			h.CbgtVersion,
		); err != nil {
			Warn("Warning: attempted to remove %v (%v) from CBGT but failed: %v", nodeUuid, kind, err)
		}

	}

}

func CBGTPlanParams(numShards, numVbuckets uint16) cbgt.PlanParams {

	// Make sure the number of vbuckets is a power of two, since it's possible
	// (but not common) to configure the number of vbuckets as such.
	if !IsPowerOfTwo(numVbuckets) {
		LogPanic("The number of vbuckets is %v, but Sync Gateway expects this to be a power of two", numVbuckets)
	}

	// We can't allow more shards than vbuckets, that makes no sense because each
	// shard would be responsible for less than one vbucket.
	if numShards > numVbuckets {
		LogPanic("The number of shards (%v) must be less than the number of vbuckets (%v)", numShards, numVbuckets)
	}

	// Calculate numVbucketsPerShard based on numVbuckets and num_shards.
	// Due to the guarantees above and the ValidateOrPanic() method, this
	// is guaranteed to divide evenly.
	numVbucketsPerShard := numVbuckets / numShards

	return cbgt.PlanParams{
		MaxPartitionsPerPIndex: int(numVbucketsPerShard),
		NumReplicas:            0, // no use case for Sync Gateway to have pindex replicas
	}

}
