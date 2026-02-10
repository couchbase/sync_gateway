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

	sgbucket "github.com/couchbase/sg-bucket"
)

// RosmarDCPClient implements a DCPClient for rosmar buckets.
type RosmarDCPClient struct {
	bucket     Bucket
	opts       DCPClientOptions
	doneChan   chan struct{}
	terminator chan bool
}

// NewRosmarDCPClient creates a new DCPClient for a rosmar bucket.
func NewRosmarDCPClient(bucket Bucket, opts DCPClientOptions) (*RosmarDCPClient, error) {
	return &RosmarDCPClient{
		bucket: bucket,
		opts:   opts,
	}, nil
}

// Start a DCP feed, returns a channel that will be closed when the feed is done.
func (dc *RosmarDCPClient) Start(ctx context.Context) (chan error, error) {
	doneChan := make(chan error)
	dc.doneChan = make(chan struct{})
	dc.terminator = make(chan bool)
	feedArgs := sgbucket.FeedArguments{
		ID:               dc.opts.FeedPrefix,
		CheckpointPrefix: dc.opts.CheckpointPrefix,
		Dump:             dc.opts.OneShot,
		DoneChan:         dc.doneChan,
		Terminator:       dc.terminator,
		Scopes:           dc.opts.CollectionNames,
		Backfill:         sgbucket.FeedResume,
	}
	if dc.opts.FromLatestSequence {
		feedArgs.Backfill = sgbucket.FeedNoBackfill
	}
	err := dc.bucket.StartDCPFeed(ctx, feedArgs, dc.opts.Callback, nil)
	if err != nil {
		close(doneChan)
		return nil, err
	}
	// This extra goroutine can be removed if sgbucket.FeedArguments.DoneChan is changed to chan error
	go func() {
		<-dc.doneChan
		close(doneChan)
	}()
	return doneChan, nil
}

// Close the DCP feed. This is a non blocking operation to allow for use in a callback function.
func (dc *RosmarDCPClient) Close() {
	if dc.terminator != nil {
		close(dc.terminator)
		dc.terminator = nil
	}
}

func (dc *RosmarDCPClient) GetMetadata() []DCPMetadata {
	// Rosmar DCP client does not support getting metadata yet
	return nil
}

// GetMetadataKeyPrefix returns the document prefix for the checkpoint documents.
func (dc *RosmarDCPClient) GetMetadataKeyPrefix() string {
	return dc.opts.CheckpointPrefix
}
