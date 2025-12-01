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
	"fmt"

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
	for vbNo := range numVbuckets {
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

func newGocbDCPClient(ctx context.Context, bucket *GocbV2Bucket, opts DCPClientOptions) (*GoCBDCPClient, error) {
	var collectionIDs []uint32
	if bucket.IsSupported(sgbucket.BucketStoreFeatureCollections) {
		cm, err := bucket.GetCollectionManifest()
		if err != nil {
			return nil, err
		}

		// should only be one args.Scope so cheaper to iterate this way around
		for scopeName, collections := range opts.CollectionNames {
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
							return nil, RedactErrorf("collection %s not found in scope %s %+v", MD(collectionName), MD(manifestScope.Name), manifestScope.Collections)
						}
					}
				}
				break
			}
			if !scopeFound {
				return nil, RedactErrorf("scope %s not found", MD(scopeName))
			}
		}
	}
	options := GoCBDCPClientOptions{
		MetadataStoreType: opts.MetadataStoreType,
		DbStats:           opts.DBStats,
		CollectionIDs:     collectionIDs,
		AgentPriority:     gocbcore.DcpAgentPriorityMed,
		CheckpointPrefix:  opts.CheckpointPrefix,
		OneShot:           opts.OneShot,
		FailOnRollback:    opts.FailOnRollback,
		InitialMetadata:   opts.InitialMetadata,
	}

	if opts.FromLatestSequence {
		if len(opts.InitialMetadata) > 0 {
			return nil, fmt.Errorf("DCPClientOptions.InitialMetadata cannot be provided when FromLatestSequence is true")
		}
		metadata, err := getHighSeqMetadata(bucket)
		if err != nil {
			return nil, err
		}
		options.InitialMetadata = metadata
	}

	feedName, err := generateDcpStreamName(opts.FeedPrefix)
	if err != nil {
		return nil, err
	}
	return NewGocbDCPClient(
		ctx,
		feedName,
		opts.Callback,
		options,
		bucket)
}
