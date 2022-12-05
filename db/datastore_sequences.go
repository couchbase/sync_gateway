package db

import (
	"context"
	"errors"
	"time"

	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

type DataStoreWithSequence interface {
	GetDocument(ctx context.Context, docid string, unmarshalLevel DocumentUnmarshalLevel) (doc *Document, err error)
	LastSequence() (uint64, error)
	groupID() string
	QuerySequences(ctx context.Context, sequences []uint64) (sgbucket.QueryResultIterator, error)
	UseXattrs() bool
	checkForUpgrade(key string, unmarshalLevel DocumentUnmarshalLevel) (*Document, *sgbucket.BucketDocument)
	dbStats() *base.DbStats
	getChangesForSequences(ctx context.Context, sequences []uint64) (LogEntries, error)
	keyspace() string
	userXattrKey() string
}

type querySequencesOptions struct {
	dbStats                   *base.DbStats
	useViews                  bool
	useXattrs                 bool
	slowQueryWarningThreshold time.Duration
}

// Queries the 'channels' view to get changes from a channel for the specified sequence.  Used for skipped sequence check
// before abandoning.
func getChangesForSequences(ctx context.Context, dbContext *DatabaseContext, dataStore base.DataStore, sequences []uint64, options querySequencesOptions) (LogEntries, error) {
	if dataStore == nil {
		return nil, errors.New("No data store available for sequence query")
	}

	start := time.Now()

	entries := make(LogEntries, 0)

	// Query the view or index
	queryResults, err := querySequences(ctx, dbContext, dataStore, sequences, options)
	if err != nil {
		return nil, err
	}
	collectionID := base.GetCollectionID(dataStore)

	// Convert the output to LogEntries.  Channel query and view result rows have different structure, so need to unmarshal independently.
	for {
		var entry *LogEntry
		var found bool
		if options.useViews {
			entry, found = nextChannelViewEntry(queryResults, collectionID)
		} else {
			entry, found = nextChannelQueryEntry(queryResults, collectionID)
		}

		if !found {
			break
		}
		entries = append(entries, entry)
	}

	// Close query results
	closeErr := queryResults.Close()
	if closeErr != nil {
		return nil, closeErr
	}

	base.InfofCtx(ctx, base.KeyCache, "Got rows from sequence query: #%d sequences found/#%d sequences queried",
		len(entries), len(sequences))

	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		base.InfofCtx(ctx, base.KeyAll, "Sequences query took %v to return %d rows. #sequences queried: %d",
			elapsed, len(entries), len(sequences))
	}

	return entries, nil
}

// Query to retrieve keys for the specified sequences.  View query uses star channel, N1QL query uses IndexAllDocs
func querySequences(ctx context.Context, dbContext *DatabaseContext, dataStore base.DataStore, sequences []uint64, options querySequencesOptions) (sgbucket.QueryResultIterator, error) {

	if len(sequences) == 0 {
		return nil, errors.New("No sequences specified for QueryChannelsForSequences")
	}

	if options.useViews {
		opts := changesViewForSequencesOptions(sequences)
		return dbContext.ViewQueryWithStats(ctx, dataStore, DesignDocSyncGateway(), ViewChannels, opts)
	}

	// N1QL Query
	sequenceQueryStatement := replaceSyncTokensQuery(QuerySequences.statement, options.useXattrs)
	sequenceQueryStatement = replaceIndexTokensQuery(sequenceQueryStatement, sgIndexes[IndexAllDocs], options.useXattrs)

	params := make(map[string]interface{})
	params[QueryParamInSequences] = sequences

	return N1QLQueryWithStats(ctx, dataStore, QuerySequences.name, sequenceQueryStatement, params, base.RequestPlus, QueryChannels.adhoc, options.dbStats, dbContext.Options.SlowQueryWarningThreshold)
}
