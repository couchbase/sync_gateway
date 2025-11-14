package base

import (
	"context"
	"expvar"
	"fmt"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbaselabs/rosmar"
)

type DCPClient interface {
	Start(ctx context.Context) (chan error, error)
	Close() error
	GetMetadata() []DCPMetadata
	GetMetadataKeyPrefix() string
}

type DCPCollections map[string][]string

type DCPClientOptions struct {
	ID                string                         // name of the DCP feed, used for logging locally and stored by Couchbase Server
	OneShot           bool                           // if true, the feed runs to latest document found when the client is started
	FailOnRollback    bool                           // if true, fail Start if the current DCP checkpoints encounter a rollback condition
	MetadataStoreType DCPMetadataStoreType           // persistent or in memory storage
	GroupID           string                         // name of groupID of rest.ServerContext in order to isolate DCP checkpoints
	CheckpointPrefix  string                         // start of the checkpoint documents
	Callback          sgbucket.FeedEventCallbackFunc // callback function for DCP events
	DBStats           *expvar.Map
	Scopes            map[string][]string // scopes and collections to monitor
	InitialMetadata   []DCPMetadata       // initial metadata to seed the DCP client with
}

func NewDCPClient(ctx context.Context, bucket Bucket, opts DCPClientOptions) (DCPClient, error) {
	underlyingBucket := GetBaseBucket(bucket)
	if _, ok := underlyingBucket.(*rosmar.Bucket); ok {
		return NewRosmarDCPClient(bucket, opts)
	} else if gocbBucket, ok := underlyingBucket.(*GocbV2Bucket); ok {
		feedArgs := sgbucket.FeedArguments{
			ID:               opts.ID,
			CheckpointPrefix: opts.CheckpointPrefix,
			Scopes:           opts.Scopes,
		}
		return newGocbDCPClient(ctx, gocbBucket, gocbBucket.GetName(), feedArgs, opts.Callback, opts.DBStats, opts.MetadataStoreType, opts.GroupID)
	}
	return nil, fmt.Errorf("bucket type %T does not have a DCPClient implementation", underlyingBucket)
}

func (c DCPCollections) Add(ds ...sgbucket.DataStoreName) {
	for _, d := range ds {
		if _, ok := c[d.ScopeName()]; !ok {
			c[d.ScopeName()] = []string{}
		}
		c[d.ScopeName()] = append(c[d.ScopeName()], d.CollectionName())
	}
}
