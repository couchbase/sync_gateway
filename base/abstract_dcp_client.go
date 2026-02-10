// Copyright 2025-Present Couchbase, Inc.
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

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/rosmar"
)

// DCPClient is an interface for all DCP implementations.
type DCPClient interface {
	// Start will start the DCP feed. It returns a channel marking the end of the feed.
	Start(ctx context.Context) (chan error, error)
	// Close will shut down the DCP feed.
	Close()
	// GetMetadata returns the current DCP metadata.
	GetMetadata() []DCPMetadata
	// GetMetadataKeyPrefix returns the key prefix used for storing any persistent data.
	GetMetadataKeyPrefix() string
}

// DCPClientOptions are options for creating a DCPClient.
type DCPClientOptions struct {
	FeedPrefix         string                         // name of the DCP feed, used for logging locally and stored by Couchbase Server
	Callback           sgbucket.FeedEventCallbackFunc // callback function for DCP events
	DBStats            *expvar.Map                    // these options are used only for gocbcore implementation, these stats are not shared by prometheus stats
	CheckpointPrefix   string                         // start of the checkpoint documents
	CollectionNames    CollectionNames                // scopes and collections to monitor
	InitialMetadata    []DCPMetadata                  // initial metadata to seed the DCP client with
	MetadataStoreType  DCPMetadataStoreType           // persistent or in memory storage
	OneShot            bool                           // if true, the feed runs to latest document found when the client is started
	FailOnRollback     bool                           // if true, fail Start if the current DCP checkpoints encounter a rollback condition
	Terminator         chan bool                      // optional channel that can be closed to terminate the DCP feed, this will be replaced with a context option.
	FromLatestSequence bool                           // If true, start at latest sequence.
}

// NewDCPClient creates a new DCPClient to receive events from a bucket.
func NewDCPClient(ctx context.Context, bucket Bucket, opts DCPClientOptions) (DCPClient, error) {
	if opts.FeedPrefix == "" {
		return nil, fmt.Errorf("DCPClientOptions.IDPrefix must be provided")
	} else if bucket == nil {
		return nil, fmt.Errorf("bucket must be provided")
	} else if opts.Callback == nil {
		return nil, fmt.Errorf("DCPClientOptions.Callback must be provided")
	} else if len(opts.CollectionNames) == 0 {
		return nil, fmt.Errorf("DCPClientOptions.CollectionNames must be provided")
	} else if opts.FromLatestSequence && len(opts.InitialMetadata) > 0 {
		return nil, fmt.Errorf("DCPClientOptions.InitialMetadata cannot be provided when FromLatestSequence is true")
	} else if opts.MetadataStoreType == DCPMetadataStoreInMemory && opts.CheckpointPrefix != "" {
		return nil, fmt.Errorf("DCPClientOptions.CheckpointPrefix cannot be provided when MetadataStoreType is InMemory")
	} else if opts.MetadataStoreType == DCPMetadataStoreCS && opts.CheckpointPrefix == "" {
		return nil, fmt.Errorf("DCPClientOptions.CheckpointPrefix must be provided when MetadataStoreType is persistent")
	}
	underlyingBucket := GetBaseBucket(bucket)
	if _, ok := underlyingBucket.(*rosmar.Bucket); ok {
		return NewRosmarDCPClient(bucket, opts)
	} else if gocbBucket, ok := underlyingBucket.(*GocbV2Bucket); ok {
		return newGocbDCPClient(ctx, gocbBucket, opts)
	}
	return nil, fmt.Errorf("bucket type %T does not have a DCPClient implementation", underlyingBucket)
}

// StartDCPFeed creates and starts a DCP feed. This function will return as soon as the feed is started. doneChan is
// sent a single error value when the feed terminates.
func StartDCPFeed(ctx context.Context, bucket Bucket, opts DCPClientOptions) (doneChan <-chan error, err error) {
	client, err := NewDCPClient(ctx, bucket, opts)
	if err != nil {
		return nil, err
	}
	bucketName := bucket.GetName()
	feedName := opts.FeedPrefix

	doneChan, err = client.Start(ctx)
	if err != nil {
		ErrorfCtx(ctx, "Failed to start DCP Feed %q for bucket %q: %v", feedName, MD(bucketName), err)
		client.Close()
		ErrorfCtx(ctx, "Finished calling async close error from DCP Feed %q for bucket %q: %v", feedName, MD(bucketName), err)
		if doneChan != nil {
			<-doneChan
		}
		return nil, err
	}
	InfofCtx(ctx, KeyDCP, "Started DCP Feed %q for bucket %q", feedName, MD(bucketName))
	go func() {
		select {
		case err := <-doneChan:
			WarnfCtx(ctx, "DCP Feed %q for bucket %q closed unexpectedly: %v", feedName, MD(bucketName), err)
			// FIXME: close dbContext here
			break
		case <-opts.Terminator:
			InfofCtx(ctx, KeyDCP, "Closing DCP Feed %q for bucket %q based on termination notification", feedName, MD(bucketName))
			client.Close()
			dcpCloseErr := <-doneChan
			if dcpCloseErr != nil {
				WarnfCtx(ctx, "Error on closing DCP Feed %q for %q: %v", feedName, MD(bucketName), dcpCloseErr)
			}
			break
		}
	}()
	return doneChan, err
}
