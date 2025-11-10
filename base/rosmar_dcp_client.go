package base

import (
	"context"

	sgbucket "github.com/couchbase/sg-bucket"
)

type RosmarDCPClient struct {
	bucket     Bucket
	opts       DCPClientOptions
	doneChan   chan struct{}
	terminator chan bool
}

func NewRosmarDCPClient(bucket Bucket, opts DCPClientOptions) (*RosmarDCPClient, error) {
	return &RosmarDCPClient{
		bucket: bucket,
		opts:   opts,
	}, nil
}

func (dc *RosmarDCPClient) Start(ctx context.Context) (chan error, error) {
	doneChan := make(chan error)
	dc.doneChan = make(chan struct{})
	dc.terminator = make(chan bool)
	feedArgs := sgbucket.FeedArguments{
		ID:               dc.opts.ID,
		CheckpointPrefix: dc.opts.CheckpointPrefix,
		Dump:             dc.opts.OneShot,
		DoneChan:         dc.doneChan,
		Terminator:       dc.terminator,
		Scopes:           dc.opts.Scopes,
	}
	err := dc.bucket.StartDCPFeed(ctx, feedArgs, dc.opts.Callback, nil)
	if err != nil {
		return nil, err
	}
	go func() {
		<-feedArgs.DoneChan
		close(doneChan)
	}()
	return doneChan, nil
}

func (dc *RosmarDCPClient) Close() error {
	close(dc.terminator)
	<-dc.doneChan
	return nil
}

func (dc *RosmarDCPClient) GetMetadata() []DCPMetadata {
	// Rosmar DCP client does not support getting metadata yet
	return nil
}

func (dc *RosmarDCPClient) GetMetadataKeyPrefix() string {
	// this value is probably not correct
	return dc.opts.CheckpointPrefix
}
