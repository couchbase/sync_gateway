package db

import (
	"context"
	"fmt"

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
		true,
	)
	if err != nil {
		base.DebugfCtx(ctx, base.KeyDCP, "Error initializing the change cache for prinicpal storage", err)
		return nil, err
	}

	return ps, nil
}

// FIXME
func (ps *principalSequences) GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error) {
	panic("This function is not supported on principal cache")
	return nil, fmt.Errorf("This function is not supported on principal cache")
}
func (ps *principalSequences) LastSequence() (uint64, error) {
	return ps.sequences.lastSequence()
}
func (ps *principalSequences) groupID() string {
	return ps.dbCtx.Options.GroupID
}

func (ps *principalSequences) QuerySequences(ctx context.Context, sequences []uint64) (sgbucket.QueryResultIterator, error) {
	return nil, fmt.Errorf("This function is not supported on principal cache")
	// return querySequences(ctx, ps.dbCtx, ps.dbCtx.MetadataStore, sequences, ps.getQuerySequenceOptions())
}

func (ps *principalSequences) UseXattrs() bool {
	return ps.dbCtx.Options.EnableXattr
}

func (ps *principalSequences) checkForUpgrade(key string, unmarshalLevel DocumentUnmarshalLevel) (*Document, *sgbucket.BucketDocument) {
	panic("This function is not supported on principal cache")
	/*// If we are using xattrs or Couchbase Server doesn't support them, an upgrade isn't going to be in progress
	if ps.UseXattrs() || !ps.dbCtx.MetadataStore.IsSupported(sgbucket.BucketStoreFeatureXattrs) {
		return nil, nil
	}

	doc, rawDocument, err := getDocWithXattr(ps.dbCtx.MetadataStore, key, unmarshalLevel, ps.userXattrKey())
	if err != nil || doc == nil || !doc.HasValidSyncData() {
		return nil, nil
	}
	return doc, rawDocument
	*/
}

func (ps *principalSequences) dbStats() *base.DbStats {
	return ps.dbCtx.DbStats
}

func (ps *principalSequences) getChangesForSequences(ctx context.Context, sequences []uint64) (LogEntries, error) {
	return getChangesForSequences(ctx, ps.dbCtx, ps.dbCtx.MetadataStore, sequences, ps.getQuerySequenceOptions())
}

func (ps *principalSequences) keyspace() string {
	return ps.dbCtx.Name
}

func (ps *principalSequences) userXattrKey() string {
	return ps.dbCtx.Options.UserXattrKey
}

func (ps *principalSequences) getQuerySequenceOptions() querySequencesOptions {
	return querySequencesOptions{
		dbStats:                   ps.dbCtx.DbStats,
		useViews:                  ps.dbCtx.Options.UseViews,
		useXattrs:                 ps.UseXattrs(),
		slowQueryWarningThreshold: ps.dbCtx.Options.SlowQueryWarningThreshold,
	}

}
