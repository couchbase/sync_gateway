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
	Close() error
	// GetMetadata returns the current DCP metadata.
	GetMetadata() []DCPMetadata
	// GetMetadataKeyPrefix returns the key prefix used for storing any persistent data.
	GetMetadataKeyPrefix() string
}

// DCPClientOptions are options for creating a DCPClient.
type DCPClientOptions struct {
	ID                string                         // name of the DCP feed, used for logging locally and stored by Couchbase Server
	Callback          sgbucket.FeedEventCallbackFunc // callback function for DCP events
	DBStats           *expvar.Map                    // these options are used only for gocbcore implementation, these stats are not shared by prometheus stats
	GroupID           string                         // name of groupID of rest.ServerContext in order to isolate DCP checkpoints
	CheckpointPrefix  string                         // start of the checkpoint documents
	CollectionNames   CollectionNames                // scopes and collections to monitor
	InitialMetadata   []DCPMetadata                  // initial metadata to seed the DCP client with
	MetadataStoreType DCPMetadataStoreType           // persistent or in memory storage
	OneShot           bool                           // if true, the feed runs to latest document found when the client is started
	FailOnRollback    bool                           // if true, fail Start if the current DCP checkpoints encounter a rollback condition
}

// NewDCPClient creates a new DCPClient to receive events from a bucket.
func NewDCPClient(ctx context.Context, bucket Bucket, opts DCPClientOptions) (DCPClient, error) {
	if opts.ID == "" {
		return nil, fmt.Errorf("DCPClientOptions.ID must be provided")
	} else if bucket == nil {
		return nil, fmt.Errorf("bucket must be provided")
	} else if opts.Callback == nil {
		return nil, fmt.Errorf("DCPClientOptions.Callback must be provided")
	} else if len(opts.CollectionNames) == 0 {
		return nil, fmt.Errorf("DCPClientOptions.CollectionNames must be provided")
	}
	underlyingBucket := GetBaseBucket(bucket)
	if _, ok := underlyingBucket.(*rosmar.Bucket); ok {
		return NewRosmarDCPClient(bucket, opts)
	} else if gocbBucket, ok := underlyingBucket.(*GocbV2Bucket); ok {
		feedArgs := sgbucket.FeedArguments{
			ID:               opts.ID,
			CheckpointPrefix: opts.CheckpointPrefix,
			Scopes:           opts.CollectionNames,
		}
		return newGocbDCPClient(ctx, gocbBucket, feedArgs, opts.Callback, opts.DBStats, opts.MetadataStoreType)
	}
	return nil, fmt.Errorf("bucket type %T does not have a DCPClient implementation", underlyingBucket)
}
