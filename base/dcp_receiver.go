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
	"crypto/tls"
	"errors"
	"expvar"

	"github.com/couchbase/go-couchbase"
	"github.com/couchbase/go-couchbase/cbdatasource"
	"github.com/couchbase/gomemcached"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
	"gopkg.in/couchbaselabs/gocbconnstr.v1"
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

func NewDCPReceiver(callback sgbucket.FeedEventCallbackFunc, bucket Bucket, maxVbNo uint16, persistCheckpoints bool, dbStats *expvar.Map, feedID string, checkpointPrefix string) (cbdatasource.Receiver, context.Context) {

	dcpCommon := NewDCPCommon(callback, bucket, maxVbNo, persistCheckpoints, dbStats, feedID, checkpointPrefix)
	r := &DCPReceiver{
		DCPCommon: dcpCommon,
	}

	if LogDebugEnabled(KeyDCP) {
		InfofCtx(r.loggingCtx, KeyDCP, "Using DCP Logging Receiver")
		logRec := &DCPLoggingReceiver{rec: r}
		return logRec, r.loggingCtx
	}

	return r, r.loggingCtx
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
	if !dcpKeyFilter(key) {
		return nil
	}
	event := makeFeedEventForMCRequest(req, sgbucket.FeedOpMutation)
	r.dataUpdate(seq, event)
	return nil
}

func (r *DCPReceiver) DataDelete(vbucketId uint16, key []byte, seq uint64,
	req *gomemcached.MCRequest) error {
	if !dcpKeyFilter(key) {
		return nil
	}
	event := makeFeedEventForMCRequest(req, sgbucket.FeedOpDeletion)
	r.dataUpdate(seq, event)
	return nil
}

// Make a feed event for a gomemcached request.  Extracts expiry from extras
func makeFeedEventForMCRequest(rq *gomemcached.MCRequest, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {
	return makeFeedEvent(rq.Key, rq.Body, rq.DataType, rq.Cas, ExtractExpiryFromDCPMutation(rq), rq.VBucket, opcode)
}

func (r *DCPReceiver) SnapshotStart(vbNo uint16,
	snapStart, snapEnd uint64, snapType uint32) error {
	r.snapshotStart(vbNo, snapStart, snapEnd)
	return nil
}

func (r *DCPReceiver) SetMetaData(vbucketId uint16, value []byte) error {
	r.setMetaData(vbucketId, value)
	return nil
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
	metadata := &cbdatasource.VBucketMetaData{
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

// This starts a cbdatasource powered DCP Feed using an entirely separate connection to Couchbase Server than anything the existing
// bucket is using, and it uses the go-couchbase cbdatasource DCP abstraction layer
func StartDCPFeed(bucket Bucket, spec BucketSpec, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {

	connSpec, err := gocbconnstr.Parse(spec.Server)
	if err != nil {
		return err
	}

	// Recommended usage of cbdatasource is to let it manage it's own dedicated connection, so we're not
	// reusing the bucket connection we've already established.
	urls, errConvertServerSpec := CouchbaseURIToHttpURL(bucket, spec.Server, &connSpec)

	if errConvertServerSpec != nil {
		return errConvertServerSpec
	}

	poolName := DefaultPool
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

	feedID := args.ID
	if feedID == "" {
		InfofCtx(context.TODO(), KeyDCP, "DCP feed started without feedID specified - defaulting to %s", DCPCachingFeedID)
		feedID = DCPCachingFeedID
	}
	receiver, loggingCtx := NewDCPReceiver(callback, bucket, maxVbno, persistCheckpoints, dbStats, feedID, args.CheckpointPrefix)

	var dcpReceiver *DCPReceiver
	switch v := receiver.(type) {
	case *DCPReceiver:
		dcpReceiver = v
	case *DCPLoggingReceiver:
		dcpReceiver = v.rec
	default:
		return errors.New("NewDCPReceiver returned unexpected receiver implementation")
	}

	// Initialize the feed based on the backfill type
	_, feedInitErr := dcpReceiver.initFeed(args.Backfill)
	if feedInitErr != nil {
		return feedInitErr
	}

	dataSourceOptions := CopyDefaultBucketDatasourceOptions()
	if spec.UseXattrs {
		dataSourceOptions.IncludeXAttrs = true
	}

	dataSourceOptions.Logf = func(fmt string, v ...interface{}) {
		DebugfCtx(loggingCtx, KeyDCP, fmt, v...)
	}

	dataSourceOptions.Name, err = GenerateDcpStreamName(feedID)
	InfofCtx(loggingCtx, KeyDCP, "DCP feed starting with name %s", dataSourceOptions.Name)
	if err != nil {
		return pkgerrors.Wrap(err, "unable to generate DCP stream name")
	}

	auth := spec.Auth

	// If using client certificate for authentication, configure go-couchbase for cbdatasource's initial
	// connection to retrieve cluster configuration.  go-couchbase doesn't support handling
	// x509 auth and root ca verification as separate concerns.
	if spec.Certpath != "" && spec.Keypath != "" {
		couchbase.SetCertFile(spec.Certpath)
		couchbase.SetKeyFile(spec.Keypath)
		auth = NoPasswordAuthHandler{Handler: spec.Auth}
		couchbase.SetRootFile(spec.CACertPath)
		couchbase.SetSkipVerify(false)
	}

	if spec.IsTLS() {
		dataSourceOptions.TLSConfig = func() *tls.Config {
			return spec.TLSConfig()
		}
	}

	networkType := getNetworkTypeFromConnSpec(connSpec)
	InfofCtx(loggingCtx, KeyDCP, "Using network type: %s", networkType)

	// default (aka internal) networking is handled by cbdatasource, so we can avoid the shims altogether in this case, for all other cases we need shims to remap hosts.
	if networkType != clusterNetworkDefault {
		// A lookup of host dest to external alternate address hostnames
		dataSourceOptions.ConnectBucket, dataSourceOptions.Connect, dataSourceOptions.ConnectTLS = alternateAddressShims(loggingCtx, spec.IsTLS(), connSpec.Addresses, networkType)
	}

	DebugfCtx(loggingCtx, KeyDCP, "Connecting to new bucket datasource.  URLs:%s, pool:%s, bucket:%s", MD(urls), MD(poolName), MD(bucketName))

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
		return pkgerrors.WithStack(RedactErrorf("Error connecting to new bucket cbdatasource.  FeedID:%s URLs:%s, pool:%s, bucket:%s.  Error: %v", feedID, MD(urls), MD(poolName), MD(bucketName), err))
	}

	if err = bds.Start(); err != nil {
		return pkgerrors.WithStack(RedactErrorf("Error starting bucket cbdatasource.  FeedID:%s URLs:%s, pool:%s, bucket:%s.  Error: %v", feedID, MD(urls), MD(poolName), MD(bucketName), err))
	}

	// Close the data source if feed terminator is closed
	if args.Terminator != nil {
		go func() {
			<-args.Terminator
			TracefCtx(loggingCtx, KeyDCP, "Closing DCP Feed [%s-%s] based on termination notification", MD(bucketName), feedID)
			if err := bds.Close(); err != nil {
				DebugfCtx(loggingCtx, KeyDCP, "Error closing DCP Feed [%s-%s] based on termination notification, Error: %v", MD(bucketName), feedID, err)
			}
			if args.DoneChan != nil {
				close(args.DoneChan)
			}
		}()
	}

	return nil

}

// CopyDefaultBucketDatasourceOptions makes a copy of cbdatasource.DefaultBucketDataSourceOptions.
// DeepCopyInefficient can't be used here due to function definitions present on BucketDataSourceOptions (ConnectBucket, etc)
func CopyDefaultBucketDatasourceOptions() *cbdatasource.BucketDataSourceOptions {
	return &cbdatasource.BucketDataSourceOptions{
		ClusterManagerBackoffFactor: cbdatasource.DefaultBucketDataSourceOptions.ClusterManagerBackoffFactor,
		ClusterManagerSleepInitMS:   cbdatasource.DefaultBucketDataSourceOptions.ClusterManagerSleepInitMS,
		ClusterManagerSleepMaxMS:    cbdatasource.DefaultBucketDataSourceOptions.ClusterManagerSleepMaxMS,

		DataManagerBackoffFactor: cbdatasource.DefaultBucketDataSourceOptions.DataManagerBackoffFactor,
		DataManagerSleepInitMS:   cbdatasource.DefaultBucketDataSourceOptions.DataManagerSleepInitMS,
		DataManagerSleepMaxMS:    cbdatasource.DefaultBucketDataSourceOptions.DataManagerSleepMaxMS,

		FeedBufferSizeBytes:    cbdatasource.DefaultBucketDataSourceOptions.FeedBufferSizeBytes,
		FeedBufferAckThreshold: cbdatasource.DefaultBucketDataSourceOptions.FeedBufferAckThreshold,

		NoopTimeIntervalSecs: cbdatasource.DefaultBucketDataSourceOptions.NoopTimeIntervalSecs,

		TraceCapacity: cbdatasource.DefaultBucketDataSourceOptions.TraceCapacity,

		PingTimeoutMS: cbdatasource.DefaultBucketDataSourceOptions.PingTimeoutMS,

		IncludeXAttrs: cbdatasource.DefaultBucketDataSourceOptions.IncludeXAttrs,
	}
}
