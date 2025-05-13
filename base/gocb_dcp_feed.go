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
	"sync/atomic"

	"github.com/couchbase/gocbcore/v10"
	sgbucket "github.com/couchbase/sg-bucket"
)

// getHighSeqMetadata returns metadata to feed into a DCP client based on the last sequence numbers stored in memory
func getHighSeqMetadata(cbstore CouchbaseBucketStore) ([]DCPMetadata, error) {
	numVbuckets, err := cbstore.GetMaxVbno()
	if err != nil {
		return nil, fmt.Errorf("Unable to determine maxVbNo when creating DCP client: %w", err)
	}

	vbUUIDs, highSeqNos, statsErr := cbstore.GetStatsVbSeqno(numVbuckets, true)
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

type GocbFeedOptions struct {
	Bucket                 *GocbV2Bucket
	FeedArgs               sgbucket.FeedArguments
	Callback               sgbucket.FeedEventCallbackFunc
	DbStats                *expvar.Map
	MetadataStoreType      DCPMetadataStoreType
	ActiveVBucketCountStat *atomic.Int32
	GroupID                string
}

// StartGocbDCPFeed starts a DCP Feed.
func StartGocbDCPFeed(ctx context.Context, opts GocbFeedOptions) error {

	feedName, err := GenerateDcpStreamName(opts.FeedArgs.ID)
	if err != nil {
		return err
	}

	var collectionIDs []uint32
	if opts.Bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		cm, err := opts.Bucket.GetCollectionManifest()
		if err != nil {
			return err
		}

		// should only be one args.Scope so cheaper to iterate this way around
		for scopeName, collections := range opts.FeedArgs.Scopes {
			scopeFound := false
			for _, manifestScope := range cm.Scopes {
				if scopeName != manifestScope.Name {
					continue
				}
				scopeFound = true
				collectionsFound := make(map[string]struct{})
				// should be less than or equal number of args.collections than cm.scope.collections, so iterate this way so that the inner loop completes quicker on average
				for _, manifestCollection := range manifestScope.Collections {
					for _, collectionName := range collections {
						if collectionName != manifestCollection.Name {
							continue
						}
						collectionIDs = append(collectionIDs, manifestCollection.UID)
						collectionsFound[collectionName] = struct{}{}
					}
				}
				if len(collectionsFound) != len(collections) {
					for _, collectionName := range collections {
						if _, ok := collectionsFound[collectionName]; !ok {
							return RedactErrorf("collection %s not found in scope %s %+v", MD(collectionName), MD(manifestScope.Name), manifestScope.Collections)
						}
					}
				}
				break
			}
			if !scopeFound {
				return RedactErrorf("scope %s not found", MD(scopeName))
			}
		}
	}
	options := DCPClientOptions{
		MetadataStoreType:  opts.MetadataStoreType,
		GroupID:            opts.GroupID,
		DbStats:            opts.DbStats,
		CollectionIDs:      collectionIDs,
		AgentPriority:      gocbcore.DcpAgentPriorityMed,
		CheckpointPrefix:   opts.FeedArgs.CheckpointPrefix,
		ActiveVBucketCount: opts.ActiveVBucketCountStat,
	}

	if opts.FeedArgs.Backfill == sgbucket.FeedNoBackfill {
		metadata, err := getHighSeqMetadata(opts.Bucket)
		if err != nil {
			return err
		}
		options.InitialMetadata = metadata
	}

	dcpClient, err := NewDCPClient(
		ctx,
		feedName,
		opts.Callback,
		options,
		opts.Bucket)
	if err != nil {
		return err
	}

	doneChan, err := dcpClient.Start()
	if err != nil {
		ErrorfCtx(ctx, "Failed to start DCP Feed %q for bucket %q: %v", feedName, MD(opts.Bucket.GetName()), err)
		// simplify in CBG-2234
		closeErr := dcpClient.Close()
		ErrorfCtx(ctx, "Finished called async close error from DCP Feed %q for bucket %q", feedName, MD(opts.Bucket.GetName()))
		if closeErr != nil {
			ErrorfCtx(ctx, "Close error from DCP Feed %q for bucket %q: %v", feedName, MD(opts.Bucket.GetName()), closeErr)
		}
		asyncCloseErr := <-doneChan
		ErrorfCtx(ctx, "Finished calling async close error from DCP Feed %q for bucket %q: %v", feedName, MD(opts.Bucket.GetName()), asyncCloseErr)
		return err
	}
	InfofCtx(ctx, KeyDCP, "Started DCP Feed %q for bucket %q", feedName, MD(opts.Bucket.GetName()))
	go func() {
		select {
		case dcpCloseError := <-doneChan:
			// simplify close in CBG-2234
			// This is a close because DCP client closed on its own, which should never happen since once
			// DCP feed is started, there is nothing that will close it
			InfofCtx(ctx, KeyDCP, "Forced closed DCP Feed %q for %q", feedName, MD(opts.Bucket.GetName()))
			// wait for channel close
			<-doneChan
			if dcpCloseError != nil {
				WarnfCtx(ctx, "Error on closing DCP Feed %q for %q: %v", feedName, MD(opts.Bucket.GetName()), dcpCloseError)
			}
			// FIXME: close dbContext here
			break
		case <-opts.FeedArgs.Terminator:
			InfofCtx(ctx, KeyDCP, "Closing DCP Feed %q for bucket %q based on termination notification", feedName, MD(opts.Bucket.GetName()))
			dcpCloseErr := dcpClient.Close()
			if dcpCloseErr != nil {
				WarnfCtx(ctx, "Error on closing DCP Feed %q for %q: %v", feedName, MD(opts.Bucket.GetName()), dcpCloseErr)
			}
			dcpCloseErr = <-doneChan
			if dcpCloseErr != nil {
				WarnfCtx(ctx, "Error on closing DCP Feed %q for %q: %v", feedName, MD(opts.Bucket.GetName()), dcpCloseErr)
			}
			break
		}
		if opts.FeedArgs.DoneChan != nil {
			close(opts.FeedArgs.DoneChan)
		}
	}()
	return err
}
