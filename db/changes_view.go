package db

import (
	"errors"
	"time"

	"github.com/couchbase/go-couchbase"
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

// Queries the 'channels' view to get a range of sequences of a single channel as LogEntries.
func (dbc *DatabaseContext) getChangesInChannelFromView(
	channelName string, endSeq uint64, options ChangesOptions) (LogEntries, error) {
	if dbc.Bucket == nil {
		return nil, errors.New("No bucket available for channel view query")
	}
	start := time.Now()
	// Query the view:
	optMap := changesViewOptions(channelName, endSeq, options)

	entries := make(LogEntries, 0)
	activeEntryCount := 0

	base.LogToR("Cache", "  Querying 'channels' view for %q (start=#%d, end=#%d, limit=%d)", base.UD(channelName), options.Since.SafeSequence()+1, endSeq, options.Limit)
	// Loop for active-only and limit handling.
	// The set of changes we get back from the view applies the limit, but includes both active and non-active entries.  When retrieving changes w/ activeOnly=true and a limit,
	// this means we may need multiple view calls to get a total of [limit] active entries.
	for {
		vres := channelsViewResult{}
		err := dbc.Bucket.ViewCustom(DesignDocSyncGateway(), ViewChannels, optMap, &vres)
		if err != nil {
			base.LogfR("Error from 'channels' view: %v", err)
			return nil, err
		} else if len(vres.Rows) == 0 {
			if len(entries) > 0 {
				break
			}
			base.LogToR("Cache", "    Got no rows from view for %q", base.UD(channelName))
			return nil, nil
		}

		// Convert the output to LogEntries:
		highSeq := uint64(0)
		for _, row := range vres.Rows {
			entry := &LogEntry{
				Sequence:     uint64(row.Key[1].(float64)),
				DocID:        row.ID,
				RevID:        row.Value.Rev,
				Flags:        row.Value.Flags,
				TimeReceived: time.Now(),
			}

			// If active-only, track the number of non-removal, non-deleted revisions we've seen in the view results
			// for limit calculation below.
			if options.ActiveOnly {
				if entry.IsActive() {
					activeEntryCount++
				}
			}
			entries = append(entries, entry)
			highSeq = entry.Sequence
		}

		// If active-only, loop until either retrieve (limit) entries, or reach endSeq
		if options.ActiveOnly {
			// If we've reached limit, we're done
			if activeEntryCount >= options.Limit || options.Limit == 0 {
				break
			}
			// If we've reached endSeq, we're done
			if endSeq > 0 && highSeq >= endSeq {
				break
			}
			// Otherwise update startkey and re-query
			optMap["startkey"] = []interface{}{channelName, highSeq + 1}
			base.LogToR("Cache", "  Querying 'channels' view for %q (start=#%d, end=#%d, limit=%d)", base.UD(channelName), highSeq+1, endSeq, options.Limit)
		} else {
			// If not active-only, we only need one iteration of the loop - the limit applied to the view query is sufficient
			break
		}
	}

	if len(entries) > 0 {
		base.LogToR("Cache", "    Got %d rows from view for %q: #%d ... #%d",
			len(entries), base.UD(channelName), entries[0].Sequence, entries[len(entries)-1].Sequence)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		base.LogfR("changes_view: Query took %v to return %d rows, options = %#v",
			elapsed, len(entries), base.UD(optMap))
	}
	changeCacheExpvars.Add("view_queries", 1)
	return entries, nil
}

func changesViewOptions(channelName string, endSeq uint64, options ChangesOptions) Body {
	endKey := []interface{}{channelName, endSeq}
	if endSeq == 0 {
		endKey[1] = map[string]interface{}{} // infinity
	}
	optMap := Body{
		"stale":    false,
		"startkey": []interface{}{channelName, options.Since.SafeSequence() + 1},
		"endkey":   endKey,
	}
	if options.Limit > 0 {
		optMap["limit"] = options.Limit
	}
	return optMap
}

// Public channel view call - for unit test support
func (dbc *DatabaseContext) ChannelViewTest(channelName string, endSeq uint64, options ChangesOptions) (LogEntries, error) {
	return dbc.getChangesInChannelFromView(channelName, endSeq, options)
}
