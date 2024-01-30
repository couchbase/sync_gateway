//  Copyright 2014-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package base

import (
	"context"
	"expvar"

	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
)

// Memcached binary protocol datatype bit flags (https://github.com/couchbase/memcached/blob/master/docs/BinaryProtocol.md#data-types),
// used in MCRequest.DataType
const (
	MemcachedDataTypeJSON = 1 << iota
	MemcachedDataTypeSnappy
	MemcachedDataTypeXattr
)

// Memcached datatype for raw (binary) document (non-flag)
const MemcachedDataTypeRaw = 0

// DCPReceiver implements cbdatasource.Receiver to manage updates coming from a
// cbdatasource BucketDataSource.  See go-couchbase/cbdatasource for
// additional details
type DCPReceiver struct {
	*DCPCommon
}

func NewDCPReceiver(ctx context.Context, callback sgbucket.FeedEventCallbackFunc, bucket Bucket, maxVbNo uint16, persistCheckpoints bool, dbStats *expvar.Map, feedID string, checkpointPrefix string, metaKeys *MetadataKeys) (cbdatasource.Receiver, context.Context, error) {

	metadataStore := bucket.DefaultDataStore()
	dcpCommon, err := NewDCPCommon(ctx, callback, bucket, metadataStore, maxVbNo, persistCheckpoints, dbStats, feedID, checkpointPrefix, metaKeys)
	if err != nil {
		return nil, nil, err
	}

	r := &DCPReceiver{
		DCPCommon: dcpCommon,
	}

	if LogDebugEnabled(KeyDCP) {
		InfofCtx(r.loggingCtx, KeyDCP, "Using DCP Logging Receiver")
		logRec := &DCPLoggingReceiver{rec: r}
		return logRec, r.loggingCtx, nil
	}

	return r, r.loggingCtx, nil
}

func (r *DCPReceiver) OnError(err error) {
	WarnfCtx(r.loggingCtx, "Error processing DCP stream - will attempt to restart/reconnect if appropriate: %v.", err)
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
}

func (r *DCPReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if !dcpKeyFilter(key, r.metaKeys) {
		return nil
	}
	event := makeFeedEventForMCRequest(req, sgbucket.FeedOpMutation)
	r.dataUpdate(seq, event)
	return nil
}

func (r *DCPReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if !dcpKeyFilter(key, r.metaKeys) {
		return nil
	}
	event := makeFeedEventForMCRequest(req, sgbucket.FeedOpDeletion)
	r.dataUpdate(seq, event)
	return nil
}

// Make a feed event for a gomemcached request.  Extracts expiry from extras
func makeFeedEventForMCRequest(rq *gomemcached.MCRequest, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {
	return makeFeedEvent(rq.Key, rq.Body, rq.DataType, rq.Cas, ExtractExpiryFromDCPMutation(rq), rq.VBucket, 0, opcode)
}

// ShardedImportDCPMetadata is an internal struct that is exposed to enable json marshaling, used by sharded import feed. It differs from DCPMetadata because it must match the private struct used by cbgt.metadata.
type ShardedImportDCPMetadata struct {
	FailOverLog [][]uint64 `json:"failOverLog"`
	SeqStart    uint64     `json:"seqStart"`
	SeqEnd      uint64     `json:"seqEnd"`
	SnapStart   uint64     `json:"snapStart"`
	SnapEnd     uint64     `json:"snapEnd"`
}

func (r *DCPReceiver) GetMetaData(vbNo uint16) (
	value []byte, lastSeq uint64, err error) {

	return r.getMetaData(vbNo)
}

// RollbackEx should be called by cbdatasource - Rollback required to maintain the interface.  In the event
// it's called, logs warning and does a hard reset on metadata for the vbucket
func (r *DCPReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	return r.rollback(vbucketId, rollbackSeq)
}

// RollbackEx includes the vbucketUUID needed to reset the metadata correctly
func (r *DCPReceiver) RollbackEx(vbucketId uint16, vbucketUUID uint64, rollbackSeq uint64) error {
	return r.rollbackEx(vbucketId, vbucketUUID, rollbackSeq, makeVbucketMetadataForSequence(vbucketUUID, rollbackSeq))
}

// Generate cbdatasource's VBucketMetadata for a vbucket from underlying components
func makeVbucketMetadata(vbucketUUID uint64, sequence uint64, snapStart uint64, snapEnd uint64) []byte {
	failOver := make([][]uint64, 1)
	failOverEntry := []uint64{vbucketUUID, 0}
	failOver[0] = failOverEntry
	metadata := &ShardedImportDCPMetadata{
		SeqStart:    sequence,
		SeqEnd:      uint64(0xFFFFFFFFFFFFFFFF),
		SnapStart:   snapStart,
		SnapEnd:     snapEnd,
		FailOverLog: failOver,
	}
	metadataBytes, err := JSONMarshal(metadata)
	if err == nil {
		return metadataBytes
	} else {
		return []byte{}
	}
}

// Create cbdatasource.VBucketMetadata, marshalled to []byte
func makeVbucketMetadataForSequence(vbucketUUID uint64, sequence uint64) []byte {
	return makeVbucketMetadata(vbucketUUID, sequence, sequence, sequence)

}

// DCPLoggingReceiver wraps DCPReceiver to provide per-callback logging
type DCPLoggingReceiver struct {
	rec *DCPReceiver
}

func (r *DCPLoggingReceiver) OnError(err error) {
	InfofCtx(r.rec.loggingCtx, KeyDCP, "OnError: %v", err)
	r.rec.OnError(err)
}

func (r *DCPLoggingReceiver) DataUpdate(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	TracefCtx(r.rec.loggingCtx, KeyDCP, "DataUpdate:%d, %s, %d, %v", vbucketId, UD(string(key)), seq, UD(req))
	return r.rec.DataUpdate(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	TracefCtx(r.rec.loggingCtx, KeyDCP, "DataDelete:%d, %s, %d, %v", vbucketId, UD(string(key)), seq, UD(req))
	return r.rec.DataDelete(vbucketId, key, seq, req)
}

func (r *DCPLoggingReceiver) Rollback(vbucketId uint16, rollbackSeq uint64) error {
	InfofCtx(r.rec.loggingCtx, KeyDCP, "Rollback:%d, %d", vbucketId, rollbackSeq)
	return r.rec.Rollback(vbucketId, rollbackSeq)
}

func (r *DCPLoggingReceiver) SetMetaData(vbucketId uint16, value []byte) error {
	TracefCtx(r.rec.loggingCtx, KeyDCP, "SetMetaData:%d, %s", vbucketId, value)
	return r.rec.SetMetaData(vbucketId, value)
}

func (r *DCPLoggingReceiver) GetMetaData(vbucketId uint16) (
	value []byte, lastSeq uint64, err error) {
	TracefCtx(r.rec.loggingCtx, KeyDCP, "GetMetaData:%d", vbucketId)
	return r.rec.GetMetaData(vbucketId)
}

func (r *DCPLoggingReceiver) SnapshotStart(vbucketId uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	TracefCtx(r.rec.loggingCtx, KeyDCP, "SnapshotStart:%d, %d, %d, %d", vbucketId, snapStart, snapEnd, snapType)
	return r.rec.SnapshotStart(vbucketId, snapStart, snapEnd, snapType)
}

// NoPasswordAuthHandler is used for client cert-based auth by cbdatasource
type NoPasswordAuthHandler struct {
	Handler AuthHandler
}

func (nph NoPasswordAuthHandler) GetCredentials() (username string, password string, bucketname string) {
	_, _, bucketname = nph.Handler.GetCredentials()

	return "", "", bucketname
}
