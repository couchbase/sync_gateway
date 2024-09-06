//  Copyright 2024-Present Couchbase, Inc.
//
//  Use of this software is governed by the Business Source License included
//  in the file licenses/BSL-Couchbase.txt.  As of the Change Date specified
//  in that file, in accordance with the Business Source License, use of this
//  software will be governed by the Apache License, Version 2.0, included in
//  the file licenses/APL2.txt.

package db

import (
	"context"
	"fmt"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbaselabs/rosmar"

	sgbucket "github.com/couchbase/sg-bucket"
)

type backgroundMgrDcpClient interface {
	Start() (doneChan chan error, err error)
	// Close is used externally to stop the DCP client. If the client was already closed due to error, returns that error
	Close() error
	// GetMetadata returns metadata for all vbuckets
	GetMetadata() []base.DCPMetadata
	// GetMetadataKeyPrefix returns the dcp metadata key prefix
	GetMetadataKeyPrefix() string
}

type backgroundManagerDcpClientOptions struct {
	Callback sgbucket.FeedEventCallbackFunc
	// only used by rosmar
	Scopes map[string][]string // Collection names to stream - map keys are scopes
	// only used by gocb client
	ID                string                    // unique ID for this DCP client
	FailOnRollback    bool                      // When true, the DCP client will terminate on DCP rollback
	MetadataStoreType base.DCPMetadataStoreType // define storage type for DCPMetadata
	GroupID           string                    // specify GroupID, only used when MetadataStoreType is DCPMetadataCS
	CheckpointPrefix  string
	CollectionIDs     []uint32 // CollectionIDs used by gocbcore, if empty, uses default collections
	OneShot           bool     // Whether the dcp feed will be continuous or not
	InitialMetadata   []base.DCPMetadata
}

func NewBackgroundManagerDcpClient(ctx context.Context, bucket base.Bucket, options backgroundManagerDcpClientOptions) (backgroundMgrDcpClient, error) {
	gocbBucket, err := base.AsGocbV2Bucket(bucket)
	if err == nil {
		clientOptions := &base.DCPClientOptions{
			FailOnRollback:    options.FailOnRollback,
			MetadataStoreType: options.MetadataStoreType,
			GroupID:           options.GroupID,
			CheckpointPrefix:  options.CheckpointPrefix,
			InitialMetadata:   options.InitialMetadata,
		}
		return base.NewDCPClient(ctx, options.ID, options.Callback, *clientOptions, gocbBucket)
	}
	rosmarBucket, ok := base.GetBaseBucket(bucket).(*rosmar.Bucket)
	if !ok {
		return nil, fmt.Errorf("Invalid bucket type for background manager DCP client: %T", bucket)
	}
	return &rosmarDcpClient{
		ctx:      ctx,
		callback: options.Callback,
		feedArgs: sgbucket.FeedArguments{
			ID:         options.ID,
			DoneChan:   make(chan struct{}),
			Scopes:     options.Scopes,
			Dump:       true,
			Terminator: make(chan bool),
		},
		bucket:   rosmarBucket,
		doneChan: make(chan error),
	}, nil
}

type rosmarDcpClient struct {
	ctx      context.Context
	callback sgbucket.FeedEventCallbackFunc
	feedArgs sgbucket.FeedArguments
	bucket   *rosmar.Bucket
	doneChan chan error
}

func (r *rosmarDcpClient) Start() (doneChan chan error, err error) {
	err = r.bucket.StartDCPFeed(r.ctx, r.feedArgs, r.callback, nil)
	go func() {
		select {
		case <-r.ctx.Done():
			close(r.feedArgs.DoneChan)
		case <-r.feedArgs.DoneChan:
			close(r.doneChan)
		}
	}()
	return r.doneChan, err
}

func (r *rosmarDcpClient) Close() error {
	close(r.feedArgs.Terminator)
	return nil
}

// GetMetadata returns metadata for all vbuckets
func (dc *rosmarDcpClient) GetMetadata() []base.DCPMetadata {
	return nil
}

// GetMetadataKeyPrefix returns the dcp metadata key prefix
func (dc *rosmarDcpClient) GetMetadataKeyPrefix() string {
	return ""
}
