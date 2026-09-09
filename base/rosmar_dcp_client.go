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
	"sync"
	"sync/atomic"

	sgbucket "github.com/couchbase/sg-bucket"
)

// RosmarDCPClient implements a DCPClient for rosmar buckets.
type RosmarDCPClient struct {
	ctx        context.Context
	bucket     Bucket
	opts       DCPClientOptions
	doneChan   chan struct{}
	closeOnce  sync.Once
	terminator chan bool
	// closeRequested records that Close was called, which means the feed was terminated rather than
	// having streamed everything. Rosmar closes doneChan either way, so this is the only way to tell
	// the two apart.
	closeRequested atomic.Bool
}

// NewRosmarDCPClient creates a new DCPClient for a rosmar bucket.
func NewRosmarDCPClient(ctx context.Context, bucket Bucket, opts DCPClientOptions) (*RosmarDCPClient, error) {
	return &RosmarDCPClient{
		ctx:    ctx,
		bucket: bucket,
		opts:   opts,
	}, nil
}

// Start a DCP feed, returns a channel that will be closed when the feed is done.
func (dc *RosmarDCPClient) Start() (chan error, error) {
	doneChan := make(chan error)
	dc.doneChan = make(chan struct{})
	dc.terminator = make(chan bool)
	feedArgs := sgbucket.FeedArguments{
		ID:               dc.opts.FeedID,
		CheckpointPrefix: dc.opts.CheckpointPrefix,
		Dump:             dc.opts.OneShot,
		DoneChan:         dc.doneChan,
		Terminator:       dc.terminator,
		Scopes:           dc.opts.CollectionNames.ToCollectionNames(),
		Backfill:         sgbucket.FeedResume,
		FeedContent:      dc.opts.FeedContent,
		MetadataStore:    dc.opts.MetadataStore,
	}
	if dc.opts.FromLatestSequence {
		feedArgs.Backfill = sgbucket.FeedNoBackfill
	} else if dc.opts.CheckpointPrefix == "" {
		// force starting at zero, limitation of rosmar DCP Client
		feedArgs.Backfill = 0
	}

	err := dc.bucket.StartDCPFeed(dc.ctx, feedArgs, dc.opts.Callback, nil)
	if err != nil {
		close(doneChan)
		return nil, err
	}
	// This extra goroutine can be removed if sgbucket.FeedArguments.DoneChan is changed to chan error
	go func() {
		<-dc.doneChan
		// On successful one-shot feed completion, purge persisted checkpoints. This matches
		// GoCBDCPClient, which purges in deactivateVbucket once the last vBucket stream ends cleanly.
		//
		// A terminated feed must keep its checkpoints so the next run can resume, and rosmar closes
		// doneChan for a termination too, so skip the purge when Close was what ended the feed. On a
		// natural finish this check runs before doneChan is closed to the caller, so the caller cannot
		// have called Close yet.
		if dc.opts.OneShot && !dc.closeRequested.Load() {
			if err := dc.PurgeCheckpoints(dc.ctx); err != nil {
				WarnfCtx(dc.ctx, "Failed to purge DCP checkpoints after one-shot feed completion: %v", err)
			}
		}
		close(doneChan)
	}()
	return doneChan, nil
}

// Close the DCP feed. This is a non blocking operation to allow for use in a callback function.
func (dc *RosmarDCPClient) Close() error {
	dc.closeOnce.Do(func() {
		dc.closeRequested.Store(true)
		if dc.terminator != nil {
			close(dc.terminator)
			dc.terminator = nil
		}
	})
	return nil
}

func (dc *RosmarDCPClient) GetMetadata() []DCPMetadata {
	// Rosmar DCP client does not support getting metadata yet
	return nil
}

// GetMetadataKeyPrefix returns the document prefix for the checkpoint documents.
func (dc *RosmarDCPClient) GetMetadataKeyPrefix() string {
	return dc.opts.CheckpointPrefix
}

func (dc *RosmarDCPClient) getMetadataStore(ctx context.Context) sgbucket.DataStore {
	if dc.opts.MetadataStore != nil {
		return dc.opts.MetadataStore
	}
	return dc.bucket.DefaultDataStore(ctx)
}

// PurgeCheckpoints deletes the checkpoint document for the feed. Calling this function while the feed is running
// will not alter the feed nor remove the checkpoint for the future.
func (dc *RosmarDCPClient) PurgeCheckpoints(ctx context.Context) error {
	return PurgeDCPCheckpoints(ctx, dc.getMetadataStore(ctx), dc.opts.CheckpointPrefix, DCPFeedRosmar)
}
