package base

import (
	"context"
	"errors"
	"expvar"
	"fmt"
	"io"
	"strconv"

	"github.com/couchbase/gomemcached"

	"github.com/couchbase/cbgt"
	sgbucket "github.com/couchbase/sg-bucket"
	pkgerrors "github.com/pkg/errors"
)

// vbucketIdStrings is a memorized array of 1024 entries for fast
// conversion of vbucketId's to partition strings via an index lookup.
// (Atoi is faster than map lookup when going in the other direction)
var vbucketIdStrings []string

func init() {
	vbucketIdStrings = make([]string, 1024)
	for i := 0; i < len(vbucketIdStrings); i++ {
		vbucketIdStrings[i] = fmt.Sprintf("%d", i)
	}
}

type SGDest interface {
	cbgt.Dest
	initFeed(backfillType uint64) error
}

// DCPDest implements SGDest (superset of cbgt.Dest) interface to manage updates coming from a
// cbgt-based DCP feed.  Embeds DCPCommon for underlying feed event processing
type DCPDest struct {
	DCPCommon
}

func NewDCPDest(callback sgbucket.FeedEventCallbackFunc, bucket Bucket, maxVbNo uint16, persistCheckpoints bool, dbStats *expvar.Map, feedID string) (SGDest, context.Context) {

	dcpCommon := NewDCPCommon(callback, bucket, maxVbNo, persistCheckpoints, dbStats, feedID)

	d := &DCPDest{
		DCPCommon: dcpCommon,
	}

	if LogDebugEnabled(KeyDCP) {
		InfofCtx(d.loggingCtx, KeyDCP, "Using DCP Logging Receiver")
		logRec := &DCPLoggingDest{dest: d}
		return logRec, d.loggingCtx
	}

	return d, d.loggingCtx
}

func (d *DCPDest) Close() error {
	// TODO: Do we have any cleanup we should be doing on close?
	return nil
}

func (d *DCPDest) DataUpdate(partition string, key []byte, seq uint64,
	val []byte, cas uint64, extrasType cbgt.DestExtrasType, extras []byte) error {

	if !dcpKeyFilter(key) {
		return nil
	}
	event := makeFeedEventForDest(key, val, cas, partitionToVbNo(partition), 0, 0, sgbucket.FeedOpMutation)
	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) DataUpdateEx(partition string, key []byte, seq uint64, val []byte,
	cas uint64, extrasType cbgt.DestExtrasType, req interface{}) error {

	if !dcpKeyFilter(key) {
		return nil
	}

	var event sgbucket.FeedEvent
	if extrasType == cbgt.DEST_EXTRAS_TYPE_MCREQUEST {
		mcReq, ok := req.(*gomemcached.MCRequest)
		if !ok {
		}
		event = makeFeedEventForMCRequest(mcReq, sgbucket.FeedOpMutation)
	} else if extrasType == cbgt.DEST_EXTRAS_TYPE_GOCB_DCP {
		dcpExtras, ok := req.(cbgt.GocbDCPExtras)
		if !ok {
			return errors.New("Unable to cast extras of type DEST_EXTRAS_TYPE_GOCB_DCP to cbgt.GocbExtras")
		}
		// TODO: has a dependency on cbgt enhancement
		event = makeFeedEventForDest(key, val, cas, partitionToVbNo(partition), dcpExtras.Expiry, dcpExtras.Datatype, sgbucket.FeedOpMutation)
		//event = makeFeedEventForDest(key, val, cas, partitionToVbNo(partition), 0, 0, sgbucket.FeedOpMutation)

	}

	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) DataDelete(partition string, key []byte, seq uint64,
	cas uint64,
	extrasType cbgt.DestExtrasType, extras []byte) error {
	if !dcpKeyFilter(key) {
		return nil
	}

	event := makeFeedEventForDest(key, nil, cas, partitionToVbNo(partition), 0, 0, sgbucket.FeedOpDeletion)
	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) DataDeleteEx(partition string, key []byte, seq uint64,
	cas uint64, extrasType cbgt.DestExtrasType, req interface{}) error {
	if !dcpKeyFilter(key) {
		return nil
	}

	var event sgbucket.FeedEvent
	if extrasType == cbgt.DEST_EXTRAS_TYPE_MCREQUEST {
		mcReq, ok := req.(*gomemcached.MCRequest)
		if !ok {
			return errors.New("Unable to cast extras of type DEST_EXTRAS_TYPE_MCREQUEST to gomemcached.MCRequest")
		}
		event = makeFeedEventForMCRequest(mcReq, sgbucket.FeedOpDeletion)
	} else if extrasType == cbgt.DEST_EXTRAS_TYPE_GOCB_DCP {
		dcpExtras, ok := req.(cbgt.GocbDCPExtras)
		if !ok {
			return errors.New("Unable to cast extras of type DEST_EXTRAS_TYPE_GOCB_DCP to cbgt.GocbExtras")
		}
		// TODO: has a dependency on cbgt enhancement
		event = makeFeedEventForDest(key, dcpExtras.Value, cas, partitionToVbNo(partition), dcpExtras.Expiry, dcpExtras.Datatype, sgbucket.FeedOpDeletion)
		//event = makeFeedEventForDest(key, nil, cas, partitionToVbNo(partition), 0, 0, sgbucket.FeedOpDeletion)

	}
	d.dataUpdate(seq, event)
	return nil
}

func (d *DCPDest) SnapshotStart(partition string,
	snapStart, snapEnd uint64) error {
	d.snapshotStart(partitionToVbNo(partition), snapStart, snapEnd)
	return nil
}

func (d *DCPDest) OpaqueGet(partition string) (value []byte, lastSeq uint64, err error) {
	metadata, lastSeq, err := d.getMetaData(partitionToVbNo(partition))
	if len(metadata) == 0 {
		return nil, lastSeq, err
	}
	return metadata, lastSeq, err
}

func (d *DCPDest) OpaqueSet(partition string, value []byte) error {

	return d.setMetaData(partitionToVbNo(partition), value)
}

func (d *DCPDest) Rollback(partition string, rollbackSeq uint64) error {
	return d.rollback(partitionToVbNo(partition), rollbackSeq)
}

func (d *DCPDest) RollbackEx(partition string, vbucketUUID uint64, rollbackSeq uint64) error {
	// TODO: need to craft meta appropriately based on cbdatasource vs. gocb DCP client
	cbgtMeta := makeVbucketMetadataForSequence(vbucketUUID, rollbackSeq)
	return d.rollbackEx(partitionToVbNo(partition), vbucketUUID, rollbackSeq, cbgtMeta)
}

// TODO: Not implemented, review potential usage
func (d *DCPDest) ConsistencyWait(partition, partitionUUID string,
	consistencyLevel string, consistencySeq uint64, cancelCh <-chan bool) error {
	Warnf(KeyAll, "Dest.ConsistencyWait being invoked by cbgt - not supported by Sync Gateway")
	return nil
}

func (d *DCPDest) Count(pindex *cbgt.PIndex, cancelCh <-chan bool) (uint64, error) {
	Warnf(KeyAll, "Dest.Count being invoked by cbgt - not supported by Sync Gateway")
	return 0, nil
}

func (d *DCPDest) Query(pindex *cbgt.PIndex, req []byte, w io.Writer,
	cancelCh <-chan bool) error {
	Warnf(KeyAll, "Dest.Query being invoked by cbgt - not supported by Sync Gateway")
	return nil
}

func (d *DCPDest) Stats(io.Writer) error {
	Warnf(KeyAll, "Dest.Stats being invoked by cbgt - not supported by Sync Gateway")
	return nil
}

func partitionToVbNo(partition string) uint16 {
	vbNo, err := strconv.Atoi(partition)
	if err != nil {
		Errorf(KeyAll, "Unexpected non-numeric partition value %s, ignoring: %v", partition, err)
		return 0
	}
	return uint16(vbNo)
}

func vbNoToPartition(vbNo uint16) string {
	return vbucketIdStrings[vbNo]
}

// This starts a cbdatasource powered DCP Feed using an entirely separate connection to Couchbase Server than anything the existing
// bucket is using, and it uses the go-couchbase cbdatasource DCP abstraction layer
func StartCbgtDCPFeed(bucket Bucket, spec BucketSpec, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {

	feedName, err := GenerateDcpStreamName(args.ID)
	if err != nil {
		return err
	}
	indexName := feedName

	// serverURL is full gocb connstr - includes x.509 cert information
	serverURL, err := spec.GetGoCBConnString()
	if err != nil {
		return err
	}

	// paramsStr is serialized DCPFeedParams, as string
	feedParams := cbgt.NewDCPFeedParams()

	// check for basic auth
	if spec.Certpath == "" && spec.Auth != nil {
		username, password, _ := spec.Auth.GetCredentials()
		feedParams.AuthUser = username
		feedParams.AuthPassword = password
	}

	if spec.UseXattrs {
		feedParams.IncludeXAttrs = true // TODO: Seems like this is always being set in NewGocbDCPFeed
	}

	paramBytes, err := JSONMarshal(feedParams)
	if err != nil {
		return err
	}
	paramsStr := string(paramBytes)

	bucketUUID, err := bucket.UUID()
	if err != nil {
		return err
	}

	// Initialize Dest, assign to all vbuckets
	maxVbNo, err := bucket.GetMaxVbno()
	if err != nil {
		return err
	}

	persistCheckpoints := false
	if args.Backfill == sgbucket.FeedResume {
		persistCheckpoints = true
	}

	feedID := args.ID
	if feedID == "" {
		Infof(KeyDCP, "DCP feed started without feedID specified - defaulting to %s", DCPCachingFeedID)
		feedID = DCPCachingFeedID
	}

	// Initialize the feed Dest
	//  - starting vbuckets, etc
	cachingDest, loggingCtx := NewDCPDest(callback, bucket, maxVbNo, persistCheckpoints, dbStats, feedID)
	// Initialize the feed based on the backfill type
	feedInitErr := cachingDest.initFeed(args.Backfill)
	if feedInitErr != nil {
		return feedInitErr
	}

	bucketName := bucket.GetName()

	// Full DCP feed - assign a single Dest to all vbuckets.  Vbuckets need to be converted to
	// cbgt's string representation ('partition')
	dests := make(map[string]cbgt.Dest, maxVbNo)
	for vbNo := uint16(0); vbNo < maxVbNo; vbNo++ {
		partition := vbNoToPartition(vbNo)
		dests[partition] = cachingDest
	}

	disableFeed := false
	var noopManager *cbgt.Manager

	feed, err := cbgt.NewGocbDCPFeed(feedName,
		indexName,
		serverURL,
		bucketName,
		bucketUUID,
		paramsStr,
		cbgt.BasicPartitionFunc,
		dests,
		disableFeed,
		noopManager)

	if err != nil {
		return pkgerrors.WithStack(RedactErrorf("Error instantiating gocb DCP Feed.  Feed:%s URL:%s, bucket:%s.  Error: %v", feedName, UD(serverURL), MD(bucketName), err))
	}

	InfofCtx(loggingCtx, KeyDCP, "DCP feed starting with name %s", feedName)

	if err = feed.Start(); err != nil {
		return pkgerrors.WithStack(RedactErrorf("Error starting gocb DCP Feed.  Feed:%s URLs:%s, bucket:%s.  Error: %v", feedName, UD(serverURL), MD(bucketName), err))
	}

	InfofCtx(loggingCtx, KeyDCP, "DCP feed started successfully with name %s", feedName)

	// Close the feed if feed terminator is closed
	if args.Terminator != nil {
		go func() {
			<-args.Terminator
			Tracef(KeyDCP, "Closing DCP Feed [%s-%s] based on termination notification", MD(bucketName), feedName)
			feed.Close()
		}()
	}

	return nil

}

func makeFeedEventForDest(key []byte, val []byte, cas uint64, vbNo uint16, expiry uint32, dataType uint8, opcode sgbucket.FeedOpcode) sgbucket.FeedEvent {
	return makeFeedEvent(key, val, dataType, cas, expiry, vbNo, opcode)
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

func (d *DCPLoggingDest) initFeed(backfillType uint64) error {
	return d.dest.initFeed(backfillType)
}
