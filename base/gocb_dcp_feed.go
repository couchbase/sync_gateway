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

func getCollectionIDs(bucket Bucket, scope, collection *string) ([]uint32, error) {
	c, ok := bucket.(*Collection)
	if !ok {
		return []uint32{}, fmt.Errorf("bucket is not a collection")
	}
	var collectionIDs []uint32
	if scope != nil && collection != nil {
		collectionID, err := c.getCollectionID()
		if err != nil {
			return []uint32{}, err
		}
		collectionIDs = append(collectionIDs, collectionID)
	}
	return collectionIDs, nil
}

// StartGocbDCPFeed starts a DCP Feed.
func StartGocbDCPFeed(bucket Bucket, bucketName string, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map, metadataStoreType DCPMetadataStoreType, groupID string) error {
	metadata, err := getHighSeqMetadata(bucket)
	if err != nil {
		return err
	}
	feedName, err := GenerateDcpStreamName(args.ID)
	if err != nil {
		return err
	}
	collectionIDs, err := getCollectionIDs(bucket, spec.Scope, spec.Collection)
	if err != nil {
		return err
	}
	dcpClient, err := NewDCPClient(
		feedName,
		callback,
		DCPClientOptions{
			MetadataStoreType: metadataStoreType,
			GroupID:           groupID,
			InitialMetadata:   metadata,
			DbStats:           dbStats,
			CollectionIDs:     collectionIDs,
		},
		bucket)
	if err != nil {
		return err
	}

	doneChan, err := dcpClient.Start()
	loggingCtx := context.TODO()
	if err != nil {
		ErrorfCtx(loggingCtx, "!!! Failed to start DCP Feed %q for bucket %q: %w", feedName, MD(bucketName), err)
		// simplify in CBG-2234
		closeErr := dcpClient.Close()
		ErrorfCtx(loggingCtx, "!!! Finished called async close error from DCP Feed %q for bucket %q", feedName, MD(bucketName))
		if closeErr != nil {
			ErrorfCtx(loggingCtx, "!!! Close error from DCP Feed %q for bucket %q: %w", feedName, MD(bucketName), closeErr)
		}
		asyncCloseErr := <-doneChan
		ErrorfCtx(loggingCtx, "!!! Finished calling async close error from DCP Feed %q for bucket %q: %w", feedName, MD(bucketName), asyncCloseErr)
		return err
	}
	InfofCtx(loggingCtx, KeyDCP, "Started DCP Feed %q for bucket %q", feedName, MD(bucketName))
	go func() {
		select {
		case dcpCloseError := <-doneChan:
			// simplify close in CBG-2234
			// This is a close because DCP client closed on its own, which should never happen since once
			// DCP feed is started, there is nothing that will close it
			InfofCtx(loggingCtx, KeyDCP, "Forced closed DCP Feed %q for %q", feedName, MD(bucketName))
			// wait for channel close
			<-doneChan
			if dcpCloseError != nil {
				WarnfCtx(loggingCtx, "Error on closing DCP Feed %q for %q: %w", feedName, MD(bucketName), dcpCloseError)
			}
			// FIXME: close dbContext here
			break
		case <-args.Terminator:
			InfofCtx(loggingCtx, KeyDCP, "Closing DCP Feed %q for bucket %q based on termination notification", feedName, MD(bucketName))
			dcpCloseErr := dcpClient.Close()
			if dcpCloseErr != nil {
				WarnfCtx(loggingCtx, "Error on closing DCP Feed %q for %q: %w", feedName, MD(bucketName), dcpCloseErr)
			}
			dcpCloseErr = <-doneChan
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
