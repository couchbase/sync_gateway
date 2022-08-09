package base

import (
	"context"
	"errors"
	"expvar"
	"fmt"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
)

// getHighSeqMetadata returns metadata to feed into a DCP client based on the last sequence numbers stored in memory
func getHighSeqMetadata(bucket Bucket) ([]DCPMetadata, error) {
	store, ok := AsCouchbaseStore(bucket)
	if !ok {
		return nil, errors.New("DCP Client requires bucket to be CouchbaseStore")
	}

	numVbuckets, err := store.GetMaxVbno()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine maxVbNo when creating DCP client: %w", err)
	}

	vbUUIDs, highSeqNos, statsErr := store.GetStatsVbSeqno(numVbuckets, true)
	if statsErr != nil {
		return nil, fmt.Errorf("Unable to obtain high seqnos for DCP feed: %w", statsErr)
	}

	metadata := make([]DCPMetadata, numVbuckets)
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		highSeqNo := gocbcore.SeqNo(highSeqNos[vbNo])
		metadata[vbNo].VbUUID = gocbcore.VbUUID(vbUUIDs[vbNo])
		metadata[vbNo].FailoverEntries = []gocbcore.FailoverEntry{
			{
				VbUUID: gocbcore.VbUUID(vbUUIDs[vbNo]),
				SeqNo:  highSeqNo,
			},
		}
		metadata[vbNo].StartSeqNo = highSeqNo
		metadata[vbNo].EndSeqNo = gocbcore.SeqNo(uint64(0xFFFFFFFFFFFFFFFF))
		metadata[vbNo].SnapStartSeqNo = highSeqNo
		metadata[vbNo].SnapEndSeqNo = highSeqNo
	}
	return metadata, nil
}

// StartGocbDCPFeed starts a DCP Feed.
func StartGocbDCPFeed(bucket Bucket, spec BucketSpec, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
	metadata, err := getHighSeqMetadata(bucket)
	if err != nil {
		return err
	}
	bucketName := spec.BucketName
	feedName, err := GenerateDcpStreamName(args.ID)
	if err != nil {
		return err
	}
	dcpClient, err := NewDCPClient(
		feedName,
		callback,
		DCPClientOptions{
			// address in CBG-2232
			MetadataStoreType: DCPMetadataInMemory,
			InitialMetadata:   metadata,
			DbStats:           dbStats,
		},
		bucket,
		"")
	if err != nil {
		return err
	}

	doneChan, err := dcpClient.Start()
	loggingCtx := context.TODO()
	if err != nil {
		fmt.Printf("!!! Failed to start DCP Feed %q for bucket %q: %s\n", feedName, MD(bucketName), err)
		dcpClient.Close()
		fmt.Printf("!!! Called async closed to start DCP Feed %q for bucket %q: donChan %+v\n", feedName, MD(bucketName), doneChan)
		<-doneChan
		fmt.Printf("!!! Finished blocking on close for DCP Feed %q for bucket %q: %s\n", feedName, MD(bucketName), err)
		panic("start failed")
		return err
	}
	InfofCtx(loggingCtx, KeyDCP, "Started DCP Feed %q for bucket %q", feedName, MD(bucketName))
	go func() {
		select {
		case dcpCloseError := <-doneChan:
			// This is a close because DCP client closed on its own, which should never happen since once
			ErrorfCtx(loggingCtx, "DCP Feed %q for %q closed unexpectedly, this behavior is undefined, err: %w", feedName, MD(bucketName), dcpCloseError)
			break
		case <-args.Terminator:
			InfofCtx(loggingCtx, KeyDCP, "Closing DCP Feed %q for bucket %q based on termination notification", feedName, MD(bucketName))
			dcpClient.Close()
			dcpCloseErr := <-doneChan
			if dcpCloseErr != nil {
				WarnfCtx(loggingCtx, "Error on closing DCP Feed %q for %q: %w", feedName, MD(bucketName), dcpCloseErr)
			}
			break
		}
		if args.DoneChan != nil {
			close(args.DoneChan)
		}
	}()
	return err
}
