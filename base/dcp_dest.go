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
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"strconv"

	"github.com/couchbase/cbgt"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
)

// vbucketIdStrings is a memorized array of 1024 entries for fast
// conversion of vbucketId's to partition strings via an index lookup.
// (Atoi is faster than map lookup when going in the other direction)
var vbucketIdStrings [1024]string

type cbgtFeedType int8

const (
	cbgtFeedType_gocb cbgtFeedType = iota
)

type destFeedType int8

const (
	DestFullFeed destFeedType = iota
	DestShardedFeed
)

func init() {
	for i := 0; i < len(vbucketIdStrings); i++ {
		vbucketIdStrings[i] = fmt.Sprintf("%d", i)
	}
	cbgt.DCPFeedPrefix = "sg:"
}

type SGDest interface {
	cbgt.Dest
	initFeed(backfillType uint64) (map[uint16]uint64, error)
}

// DCPDest implements SGDest (superset of cbgt.Dest) interface to manage updates coming from a
// cbgt-based DCP feed.  Embeds DCPCommon for underlying feed event processing.  Metadata initialization
// is done on-demand per vbucket, as a given Dest isn't expected to manage the full set of vbuckets for a bucket.
type DCPDest struct {
	*DCPCommon
	feedType           destFeedType
	stats              *expvar.Map // DCP feed stats (rollback, backfill)
	partitionCountStat *SgwIntStat // Stat for partition count.  Stored outside the DCP feed stats map
	metaInitComplete   []bool      // Whether metadata initialization has been completed, per vbNo
}

func NewDCPDest(ctx context.Context, callback sgbucket.FeedEventCallbackFunc, bucket Bucket, maxVbNo uint16, persistCheckpoints bool,
	dcpStats *expvar.Map, feedID string, importPartitionStat *SgwIntStat, checkpointPrefix string, metaKeys *MetadataKeys) (SGDest, context.Context, error) {

	// TODO: Metadata store?
	metadataStore := bucket.DefaultDataStore()
	dcpCommon, err := NewDCPCommon(ctx, callback, bucket, metadataStore, maxVbNo, persistCheckpoints, dcpStats, feedID, checkpointPrefix, metaKeys)
	if err != nil {
		return nil, nil, err
	}

	d := &DCPDest{
		DCPCommon:          dcpCommon,
		stats:              dcpStats,
		partitionCountStat: importPartitionStat,
		metaInitComplete:   make([]bool, maxVbNo),
	}

	if d.partitionCountStat != nil {
		d.partitionCountStat.Add(1)
		InfofCtx(d.loggingCtx, KeyDCP, "Starting sharded feed for %s.  Total partitions:%v", d.feedID, d.partitionCountStat.String())
	}

	if LogDebugEnabled(KeyDCP) {
		InfofCtx(d.loggingCtx, KeyDCP, "Using DCP Logging Receiver")
		logRec := &DCPLoggingDest{dest: d}
		return logRec, d.loggingCtx, nil
	}

	return d, d.loggingCtx, nil
}

func (d *DCPDest) Close() error {
	if d.partitionCountStat != nil {
		d.partitionCountStat.Add(-1)
		InfofCtx(d.loggingCtx, KeyDCP, "Closing sharded feed for %s. Total partitions:%v", d.feedID, d.partitionCountStat.String())
	}
	DebugfCtx(d.loggingCtx, KeyDCP, "Closing DCPDest for %s", d.feedID)
	return nil
}

func (d *DCPDest) DataUpdate(partition string, key []byte, seq uint64,
	val []byte, cas uint64, extrasType cbgt.DestExtrasType, extras []byte) error {

	if !dcpKeyFilter(key, d.metaKeys) {
		return nil
	}
	event := makeFeedEventForDest(key, val, cas, partitionToVbNo(d.loggingCtx, partition), collectionIDFromExtras(extras), 0, 0, sgbucket.FeedOpMutation)
	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) DataUpdateEx(partition string, key []byte, seq uint64, val []byte,
	cas uint64, extrasType cbgt.DestExtrasType, req interface{}) error {

	if !dcpKeyFilter(key, d.metaKeys) {
		return nil
	}

	var event sgbucket.FeedEvent
	if extrasType == cbgt.DEST_EXTRAS_TYPE_MCREQUEST {
		mcReq, ok := req.(*gomemcached.MCRequest)
		if !ok {
			return errors.New("Unable to cast extras of type DEST_EXTRAS_TYPE_MCREQUEST to *gomemcached.MCRequest")
		}
		event = makeFeedEventForMCRequest(mcReq, sgbucket.FeedOpMutation)
	} else if extrasType == cbgt.DEST_EXTRAS_TYPE_GOCBCORE_DCP {
		dcpExtras, ok := req.(cbgt.GocbcoreDCPExtras)
		if !ok {
			return errors.New("Unable to cast extras of type DEST_EXTRAS_TYPE_GOCB_DCP to cbgt.GocbExtras")
		}
		event = makeFeedEventForDest(key, val, cas, partitionToVbNo(d.loggingCtx, partition), dcpExtras.CollectionId, dcpExtras.Expiry, dcpExtras.Datatype, sgbucket.FeedOpMutation)

	}

	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) DataDelete(partition string, key []byte, seq uint64,
	cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	if !dcpKeyFilter(key, d.metaKeys) {
		return nil
	}

	event := makeFeedEventForDest(key, nil, cas, partitionToVbNo(d.loggingCtx, partition), collectionIDFromExtras(extras), 0, 0, sgbucket.FeedOpDeletion)
	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) DataDeleteEx(partition string, key []byte, seq uint64,
	cas uint64, extrasType cbgt.DestExtrasType, req interface{}) error {
	if !dcpKeyFilter(key, d.metaKeys) {
		return nil
	}

	var event sgbucket.FeedEvent
	if extrasType == cbgt.DEST_EXTRAS_TYPE_MCREQUEST {
		mcReq, ok := req.(*gomemcached.MCRequest)
		if !ok {
			return errors.New("Unable to cast extras of type DEST_EXTRAS_TYPE_MCREQUEST to gomemcached.MCRequest")
		}
		event = makeFeedEventForMCRequest(mcReq, sgbucket.FeedOpDeletion)
	} else if extrasType == cbgt.DEST_EXTRAS_TYPE_GOCBCORE_DCP {
		dcpExtras, ok := req.(cbgt.GocbcoreDCPExtras)
		if !ok {
			return errors.New("Unable to cast extras of type DEST_EXTRAS_TYPE_GOCB_DCP to cbgt.GocbExtras")
		}
		event = makeFeedEventForDest(key, dcpExtras.Value, cas, partitionToVbNo(d.loggingCtx, partition), dcpExtras.CollectionId, dcpExtras.Expiry, dcpExtras.Datatype, sgbucket.FeedOpDeletion)

	}
	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	d.snapshotStart(partitionToVbNo(d.loggingCtx, partition), snapStart, snapEnd)
	return nil
}

func (d *DCPDest) OpaqueGet(partition string) (value []byte, lastSeq uint64, err error) {
	vbNo := partitionToVbNo(d.loggingCtx, partition)
	if !d.metaInitComplete[vbNo] {
		d.InitVbMeta(vbNo)
		d.metaInitComplete[vbNo] = true
	}

	metadata, lastSeq, err := d.getMetaData(vbNo)
	if len(metadata) == 0 {
		return nil, lastSeq, err
	}
	return metadata, lastSeq, err
}

func (d *DCPDest) OpaqueSet(partition string, value []byte) error {
	vbNo := partitionToVbNo(d.loggingCtx, partition)
	if !d.metaInitComplete[vbNo] {
		d.InitVbMeta(vbNo)
		d.metaInitComplete[vbNo] = true
	}
	_ = d.setMetaData(vbNo, value, false)
	return nil
}

func (d *DCPDest) Rollback(partition string, rollbackSeq uint64) error {
	return d.rollback(partitionToVbNo(d.loggingCtx, partition), rollbackSeq)
}

func (d *DCPDest) RollbackEx(partition string, vbucketUUID uint64, rollbackSeq uint64) error {
	cbgtMeta := makeVbucketMetadataForSequence(vbucketUUID, rollbackSeq)
	return d.rollbackEx(partitionToVbNo(d.loggingCtx, partition), vbucketUUID, rollbackSeq, cbgtMeta)
}

// TODO: Not implemented, review potential usage
func (d *DCPDest) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string, consistencySeq uint64, cancelCh <-chan bool) error {
	WarnfCtx(d.loggingCtx, "Dest.ConsistencyWait being invoked by cbgt - not supported by Sync Gateway")
	return nil
}

func (d *DCPDest) Count(pindex *cbgt.PIndex, cancelCh <-chan bool) (uint64, error) {
	WarnfCtx(d.loggingCtx, "Dest.Count being invoked by cbgt - not supported by Sync Gateway")
	return 0, nil
}

func (d *DCPDest) Query(pindex *cbgt.PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	WarnfCtx(d.loggingCtx, "Dest.Query being invoked by cbgt - not supported by Sync Gateway")
	return nil
}

// Stats would allow SG to return SG-specific stats to cbgt's stats reporting - not currently used.
func (d *DCPDest) Stats(io.Writer) error {
	return nil
}

func partitionToVbNo(ctx context.Context, partition string) uint16 {
	vbNo, err := strconv.Atoi(partition)
	if err != nil {
		ErrorfCtx(ctx, "Unexpected non-numeric partition value %s, ignoring: %v", partition, err)
		return 0
	}
	return uint16(vbNo)
}

func vbNoToPartition(vbNo uint16) string {
	return vbucketIdStrings[vbNo]
}

func collectionIDFromExtras(extras []byte) uint32 {
	if len(extras) != 8 {
		return 0
	}
	return binary.LittleEndian.Uint32(extras[4:])
}

func makeFeedEventForDest(key []byte, val []byte, cas uint64, vbNo uint16, collectionID uint32, expiry uint32, dataType uint8, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {
	return makeFeedEvent(key, val, dataType, cas, expiry, vbNo, collectionID, opcode)
}

// DCPLoggingDest wraps DCPDest to provide per-callback logging
type DCPLoggingDest struct {
	dest *DCPDest
}

func (d *DCPLoggingDest) Close() error {
	return d.dest.Close()
}

func (d *DCPLoggingDest) DataUpdate(partition string, key []byte, seq uint64,
	val []byte, cas uint64, extrasType cbgt.DestExtrasType, extras []byte) error {

	TracefCtx(d.dest.loggingCtx, KeyDCP, "DataUpdate:%s, %s, %d", partition, UD(string(key)), seq)
	return d.dest.DataUpdate(partition, key, seq, val, cas, extrasType, extras)
}

func (d *DCPLoggingDest) DataUpdateEx(partition string, key []byte, seq uint64, val []byte,
	cas uint64, extrasType cbgt.DestExtrasType, req interface{}) error {

	TracefCtx(d.dest.loggingCtx, KeyDCP, "DataUpdateEx:%s, %s, %d", partition, UD(string(key)), seq)
	return d.dest.DataUpdateEx(partition, key, seq, val, cas, extrasType, req)
}

func (d *DCPLoggingDest) DataDelete(partition string, key []byte, seq uint64,
	cas uint64, extrasType cbgt.DestExtrasType, extras []byte) error {
	TracefCtx(d.dest.loggingCtx, KeyDCP, "DataDelete:%s, %s, %d", partition, UD(string(key)), seq)
	return d.dest.DataDelete(partition, key, seq, cas, extrasType, extras)
}

func (d *DCPLoggingDest) DataDeleteEx(partition string, key []byte, seq uint64,
	cas uint64, extrasType cbgt.DestExtrasType, req interface{}) error {
	TracefCtx(d.dest.loggingCtx, KeyDCP, "DataDeleteEx:%s, %s, %d", partition, UD(string(key)), seq)
	return d.dest.DataDeleteEx(partition, key, seq, cas, extrasType, req)
}

func (d *DCPLoggingDest) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	TracefCtx(d.dest.loggingCtx, KeyDCP, "SnapshotStart:%d, %d, %d", partition, snapStart, snapEnd)
	return d.dest.SnapshotStart(partition, snapStart, snapEnd)
}

func (d *DCPLoggingDest) OpaqueGet(partition string) (value []byte, lastSeq uint64, err error) {
	TracefCtx(d.dest.loggingCtx, KeyDCP, "OpaqueGet:%s", partition)
	return d.dest.OpaqueGet(partition)
}

func (d *DCPLoggingDest) OpaqueSet(partition string, value []byte) error {
	TracefCtx(d.dest.loggingCtx, KeyDCP, "OpaqueSet:%s, %s", partition, value)
	return d.dest.OpaqueSet(partition, value)
}

func (d *DCPLoggingDest) Rollback(partition string, rollbackSeq uint64) error {
	InfofCtx(d.dest.loggingCtx, KeyDCP, "Rollback:%s, %d", partition, rollbackSeq)
	return d.dest.Rollback(partition, rollbackSeq)
}

func (d *DCPLoggingDest) RollbackEx(partition string, vbucketUUID uint64, rollbackSeq uint64) error {
	InfofCtx(d.dest.loggingCtx, KeyDCP, "RollbackEx:%s, %v, %d", partition, vbucketUUID, rollbackSeq)
	return d.dest.RollbackEx(partition, vbucketUUID, rollbackSeq)
}

func (d *DCPLoggingDest) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string, consistencySeq uint64, cancelCh <-chan bool) error {
	return d.dest.ConsistencyWait(partition, partitionUUID, consistencyLevel, consistencySeq, cancelCh)
}

func (d *DCPLoggingDest) Count(pindex *cbgt.PIndex, cancelCh <-chan bool) (uint64, error) {
	return d.dest.Count(pindex, cancelCh)
}

func (d *DCPLoggingDest) Query(pindex *cbgt.PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	return d.dest.Query(pindex, req, w, cancelCh)
}

func (d *DCPLoggingDest) Stats(w io.Writer) error {
	return d.dest.Stats(w)
}

func (d *DCPLoggingDest) initFeed(backfillType uint64) (map[uint16]uint64, error) {
	return d.dest.initFeed(backfillType)
}
