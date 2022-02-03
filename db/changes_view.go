/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package db

import (
	"context"
	"errors"
	"time"

	"github.com/couchbase/go-couchbase"
	sgbucket "github.com/couchbase/sg-bucket"
	"github.com/couchbase/sync_gateway/base"
)

// Unmarshaled JSON structure for "changes" view results
type channelsViewResult struct {
	TotalRows int `json:"total_rows"`
	Rows      []channelsViewRow
	Errors    []couchbase.ViewError
}

// One "changes" row in a channelsViewResult
type channelsViewRow struct {
	ID    string
	Key   []interface{} // Actually [channelName, sequence]
	Value struct {
		Rev   string
		Flags uint8
	}
}

func nextChannelViewEntry(results sgbucket.QueryResultIterator) (*LogEntry, bool) {

	var viewRow channelsViewRow
	found := results.Next(&viewRow)
	if !found {
		return nil, false
	}

	// Channels view uses composite Key of the form [channelName, sequence]
	entry := &LogEntry{
		Sequence:     uint64(viewRow.Key[1].(float64)),
		DocID:        viewRow.ID,
		RevID:        viewRow.Value.Rev,
		Flags:        viewRow.Value.Flags,
		TimeReceived: time.Now(),
	}
	return entry, true

}

func nextChannelQueryEntry(results sgbucket.QueryResultIterator) (*LogEntry, bool) {

	var queryRow QueryChannelsRow
	found := results.Next(&queryRow)
	if !found {
		return nil, false
	}
	entry := &LogEntry{
		Sequence:     queryRow.Sequence,
		DocID:        queryRow.Id,
		RevID:        queryRow.Rev,
		Flags:        queryRow.Flags,
		TimeReceived: time.Now(),
	}

	if queryRow.RemovalRev != "" {
		entry.RevID = queryRow.RemovalRev
		if queryRow.RemovalDel {
			entry.SetDeleted()
		}
		entry.SetRemoved()
	}
	return entry, true

}

// Queries the 'channels' view to get a range of sequences of a single channel as LogEntries.
func (dbc *DatabaseContext) getChangesInChannelFromQuery(ctx context.Context, channelName string, startSeq, endSeq uint64, limit int, activeOnly bool) (LogEntries, error) {
	if dbc.Bucket == nil {
		return nil, errors.New("No bucket available for channel query")
	}
	start := time.Now()
	usingViews := dbc.Options.UseViews

	entries := make(LogEntries, 0)
	activeEntryCount := 0

	base.InfofCtx(ctx, base.KeyCache, "  Querying 'channels' for %q (start=#%d, end=#%d, limit=%d)", base.UD(channelName), startSeq, endSeq, limit)

	// Loop for active-only and limit handling.
	// The set of changes we get back from the query applies the limit, but includes both active and non-active entries.  When retrieving changes w/ activeOnly=true and a limit,
	// this means we may need multiple view calls to get a total of [limit] active entries.
	for {

		// Query the view or index
		queryResults, err := dbc.QueryChannels(ctx, channelName, startSeq, endSeq, limit, activeOnly)
		if err != nil {
			return nil, err
		}
		queryRowCount := 0

		// Convert the output to LogEntries.  Channel query and view result rows have different structure, so need to unmarshal independently.
		highSeq := uint64(0)
		for {
			var entry *LogEntry
			var found bool
			if usingViews {
				entry, found = nextChannelViewEntry(queryResults)
			} else {
				entry, found = nextChannelQueryEntry(queryResults)
			}

			if !found {
				break
			}

			queryRowCount++

			// If active-only, track the number of non-removal, non-deleted revisions we've seen in the view results
			// for limit calculation below.
			if activeOnly {
				if entry.IsActive() {
					activeEntryCount++
				}
			}
			entries = append(entries, entry)
			highSeq = entry.Sequence
		}

		// Close query results
		closeErr := queryResults.Close()
		if closeErr != nil {
			return nil, closeErr
		}

		if queryRowCount == 0 {
			if len(entries) > 0 {
				break
			}
			base.InfofCtx(ctx, base.KeyCache, "    Got no rows from query for channel:%q", base.UD(channelName))
			return nil, nil
		}

		// If active-only, loop until either retrieve (limit) active entries, or reach endSeq.  Non-active entries are still
		// included in the result set for potential cache prepend
		if activeOnly {
			// If we've reached limit, we're done
			if activeEntryCount >= limit || limit == 0 {
				break
			}
			// If we've reached endSeq, we're done
			if endSeq > 0 && highSeq >= endSeq {
				break
			}
			// Otherwise update startkey and re-query

			startSeq = highSeq + 1
			base.InfofCtx(ctx, base.KeyCache, "  Querying 'channels' for %q (start=#%d, end=#%d, limit=%d)", base.UD(channelName), highSeq+1, endSeq, limit)
		} else {
			// If not active-only, we only need one iteration of the loop - the limit applied to the view query is sufficient
			break
		}
	}

	if len(entries) > 0 {
		base.InfofCtx(ctx, base.KeyCache, "    Got %d rows from query for %q: #%d ... #%d",
			len(entries), base.UD(channelName), entries[0].Sequence, entries[len(entries)-1].Sequence)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		base.InfofCtx(ctx, base.KeyAll, "Channel query took %v to return %d rows.  Channel: %s StartSeq: %d EndSeq: %d Limit: %d",
			elapsed, len(entries), base.UD(channelName), startSeq, endSeq, limit)
	}
	dbc.DbStats.Cache().ViewQueries.Add(1)
	return entries, nil
}

// Queries the 'channels' view to get changes from a channel for the specified sequence.  Used for skipped sequence check
// before abandoning.
func (dbc *DatabaseContext) getChangesForSequences(ctx context.Context, sequences []uint64) (LogEntries, error) {
	if dbc.Bucket == nil {
		return nil, errors.New("No bucket available for sequence query")
	}
	start := time.Now()
	usingViews := dbc.Options.UseViews

	entries := make(LogEntries, 0)

	// Query the view or index
	queryResults, err := dbc.QuerySequences(ctx, sequences)
	if err != nil {
		return nil, err
	}

	// Convert the output to LogEntries.  Channel query and view result rows have different structure, so need to unmarshal independently.
	for {
		var entry *LogEntry
		var found bool
		if usingViews {
			entry, found = nextChannelViewEntry(queryResults)
		} else {
			entry, found = nextChannelQueryEntry(queryResults)
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
