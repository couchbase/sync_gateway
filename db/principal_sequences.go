package db

import (
	"context"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
)

type principalSequences struct {
	dbCtx       *DatabaseContext
	sequences   *sequenceAllocator // Source of new sequence numbers for prinicipals
	changeCache *changeCache       // Buffering of seqnos for principals
}

func newPrincipalSequences(ctx context.Context, dbCtx *DatabaseContext) (*principalSequences, error) {
	ps := &principalSequences{
		dbCtx:       dbCtx,
		changeCache: &changeCache{},
	}
	var err error
	ps.sequences, err = newSequenceAllocator(dbCtx.MetadataStore, dbCtx.DbStats.Database())
	if err != nil {
		return nil, err
	}
	// Callback that is invoked whenever a set of channels is changed in the ChangeCache
	notifyChange := func(changedChannels channels.Set) {
		dbCtx.mutationListener.Notify(changedChannels)
	}

	err = ps.changeCache.Init(
		ctx,
		ps,
		dbCtx.channelCache,
		notifyChange,
		dbCtx.Options.CacheOptions,
	)
	if err != nil {
		base.DebugfCtx(ctx, base.KeyDCP, "Error initializing the change cache for prinicpal storage", err)
		return nil, err
	}

	return ps, nil
}

// FIXME
func (ps *principalSequences) GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	return nil, nil
}
func (ps *principalSequences) LastSequence() (uint64, error) {
	return ps.sequences.lastSequence()
}
func (ps *principalSequences) groupID() string {
	return ps.dbCtx.Options.GroupID
}

// FIXME
func (ps *principalSequences) QuerySequences(ctx context.Context, sequences []uint64) (sgbucket.QueryResultIterator, error) {
	return nil, nil
}
func (ps *principalSequences) UseXattrs() bool {
	return ps.dbCtx.Options.EnableXattr
}

// FIXME
func (ps *principalSequences) checkForUpgrade(key string, unmarshalLevel DocumentUnmarshalLevel) (*Document, *sgbucket.BucketDocument) {
	return nil, nil
}

// FIXME
func (ps *principalSequences) dbStats() *base.DbStats {
	return ps.dbCtx.DbStats
}

// FIXME
func (ps *principalSequences) getChangesForSequences(ctx context.Context, sequences []uint64) (LogEntries, error) {
	return nil, nil
}

func (ps *principalSequences) keyspace() string {
	return ps.dbCtx.Name
}

func (ps *principalSequences) userXattrKey() string {
	return ps.dbCtx.Options.UserXattrKey
}
