// Copyright 2022-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
// in that file, in accordance with the Business Source License, use of this
// software will be governed by the Apache License, Version 2.0, included in
// the file licenses/APL2.txt.

package base

import (
	"context"
	"expvar"
	"fmt"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
)

// getHighSeqMetadata returns metadata to feed into a DCP client based on the last sequence numbers stored in memory
func getHighSeqMetadata(collection *Collection) ([]DCPMetadata, error) {
	numVbuckets, err := collection.GetMaxVbno()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine maxVbNo when creating DCP client: %w", err)
	}

	vbUUIDs, highSeqNos, statsErr := collection.GetStatsVbSeqno(numVbuckets, true)
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
func StartGocbDCPFeed(collection *Collection, bucketName string, args sgbucket.FeedArguments, callback sgbucket.FeedEventCallbackFunc, dbStats *expvar.Map, metadataStoreType DCPMetadataStoreType, groupID string) error {
	metadata, err := getHighSeqMetadata(collection)
	if err != nil {
		return err
	}
	feedName, err := GenerateDcpStreamName(args.ID)
	if err != nil {
		return err
	}
	var collectionIDs []uint32
	if collection.IsSupported(sgbucket.DataStoreFeatureCollections) {
		collectionID, err := collection.GetCollectionID()
		if err != nil {
			return err
		}
		collectionIDs = append(collectionIDs, collectionID)
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
			AgentPriority:     gocbcore.DcpAgentPriorityMed,
		},
		collection)
	if err != nil {
		return err
	}

	doneChan, err := dcpClient.Start()
	loggingCtx := context.TODO()
	if err != nil {
		ErrorfCtx(loggingCtx, "Failed to start DCP Feed %q for bucket %q: %v", feedName, MD(bucketName), err)
		return err
	}
	InfofCtx(loggingCtx, KeyDCP, "Started DCP Feed %q for bucket %q", feedName, MD(bucketName))
	go func() {
		select {
		case dcpCloseError := <-doneChan:
			// This is a close because DCP client closed on its own, which should never happen since once
			// DCP feed is started, there is nothing that will close it
			InfofCtx(loggingCtx, KeyDCP, "Forced closed DCP Feed %q for %q", feedName, MD(bucketName))
			// wait for channel close
			if dcpCloseError != nil {
				WarnfCtx(loggingCtx, "Error on closing DCP Feed %q for %q: %v", feedName, MD(bucketName), dcpCloseError)
			}
			ErrorfCtx(loggingCtx, "DCP Feed %q for %q closed unexpectedly, this behavior is undefined, err: %w", feedName, MD(bucketName), dcpCloseError)
			break
		case <-args.Terminator:
			InfofCtx(loggingCtx, KeyDCP, "Closing DCP Feed %q for bucket %q based on termination notification", feedName, MD(bucketName))
			dcpCloseErr := dcpClient.Close()
			if dcpCloseErr != nil {
				WarnfCtx(loggingCtx, "Error on closing DCP Feed %q for %q: %v", feedName, MD(bucketName), dcpCloseErr)
			}
			break
		}
		if args.DoneChan != nil {
			close(args.DoneChan)
		}
	}()
	return err
}
