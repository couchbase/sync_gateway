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
		return nil, fmt.Errorf("Unable to obtain high seqnos for one-shot DCP feed: %w", statsErr)
	}

	metadata := make([]DCPMetadata, numVbuckets)
	for vbNo := uint16(0); vbNo < numVbuckets; vbNo++ {
		metadata[vbNo].VbUUID = gocbcore.VbUUID(vbUUIDs[vbNo])
		metadata[vbNo].FailoverEntries = make([]gocbcore.FailoverEntry, 0)
		metadata[vbNo].StartSeqNo = gocbcore.SeqNo(highSeqNos[vbNo])
		metadata[vbNo].EndSeqNo = gocbcore.SeqNo(uint64(0xFFFFFFFFFFFFFFFF))
		metadata[vbNo].SnapStartSeqNo = gocbcore.SeqNo(highSeqNos[vbNo])
		metadata[vbNo].SnapEndSeqNo = gocbcore.SeqNo(highSeqNos[vbNo])
	}
	return metadata, nil
}

// StartGOCB2DCPFeed starts a DCP Feed.
func StartGOCB2DCPFeed(bucket Bucket, spec BucketSpec, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map) error {
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
			MetadataStoreType: DCPMetadataInMemory,
			InitialMetadata:   metadata},
		bucket,
		"")
	if err != nil {
		return err
	}

	doneChan, err := dcpClient.Start()
	loggingCtx := context.TODO()
	if err != nil {
		ErrorfCtx(loggingCtx, "Failed to start caching DCP Feed %q for bucket %q: %w", feedName, MD(bucketName), err)
		closeErr := dcpClient.Close()
		ErrorfCtx(loggingCtx, "Close error from caching DCP Feed %q for bucket %q: %w", feedName, MD(bucketName), closeErr)
		args.Started <- err
		close(args.Started)
		return err
	}
	if args.Started != nil {
		args.Started <- nil
		close(args.Started)
	}
	InfofCtx(loggingCtx, KeyDCP, "Started caching DCP Feed %q for bucket %q", feedName, MD(bucketName))
	go func() {
		select {
		case dcpCloseError := <-doneChan:
			TracefCtx(loggingCtx, KeyDCP, "Closed caching DCP Feed %q for %q", feedName, MD(bucketName))
			// wait for channel close
			<-doneChan
			if dcpCloseError != nil {
				WarnfCtx(loggingCtx, "Error on closing caching DCP Feed %q for %q: %w", feedName, MD(bucketName), dcpCloseError)
			}
			break
		case <-args.Terminator:
			InfofCtx(loggingCtx, KeyDCP, "Closing caching DCP Feed %q for bucket %q based on termination notification", feedName, MD(bucketName))
			dcpCloseErr := dcpClient.Close()
			if dcpCloseErr != nil {
				WarnfCtx(loggingCtx, "Error on closing caching DCP Feed %q for %q: %w", feedName, MD(bucketName), dcpCloseErr)
			}
			dcpCloseErr = <-doneChan
			if dcpCloseErr != nil {
				WarnfCtx(loggingCtx, "Error on closing caching DCP Feed %q for %q: %w", feedName, MD(bucketName), dcpCloseErr)
			}
			break
		}
		if args.DoneChan != nil {
			close(args.DoneChan)
		}
	}()
	return err
}
