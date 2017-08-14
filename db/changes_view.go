package db

import (
	"errors"
	"time"

	"github.com/couchbase/go-couchbase"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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

	//Required for activeOnly handling, loop until we have consumed limit log entries
	for {
		base.LogTo("Cache", "  Querying 'channels' view for %q (start=#%d, end=#%d, limit=%d)", channelName, options.Since.SafeSequence()+1, endSeq, options.Limit)
		vres := channelsViewResult{}
		err := dbc.Bucket.ViewCustom(DesignDocSyncGatewayChannels, ViewChannels, optMap, &vres)
		if err != nil {
			base.Logf("Error from 'channels' view: %v", err)
			return nil, err
		} else if len(vres.Rows) == 0 {
			if len(entries) > 0 {
				break
			}
			base.LogTo("Cache", "    Got no rows from view for %q", channelName)
			return nil, nil
		}

		// Convert the output to LogEntries:
		for _, row := range vres.Rows {
			entry := &LogEntry{
				Sequence:     uint64(row.Key[1].(float64)),
				DocID:        row.ID,
				RevID:        row.Value.Rev,
				Flags:        row.Value.Flags,
				TimeReceived: time.Now(),
			}
			if options.ActiveOnly {
				if !entry.IsRemoved() && entry.Flags&channels.Deleted == 0 {
					// base.LogTo("Cache", "  Got view sequence #%d (%q / %q)", entry.Sequence, entry.DocID, entry.RevID)
					entries = append(entries, entry)
				}
			} else {
				entries = append(entries, entry)
			}
			optMap["startkey"] = []interface{}{channelName, entry.Sequence + 1}
		}

		if options.ActiveOnly {
			if len(entries) >= options.Limit || options.Limit == 0{
				break
			}
		} else {
			break
		}
	}

	base.LogTo("Cache", "    Got %d rows from view for %q: #%d ... #%d",
		len(entries), channelName, entries[0].Sequence, entries[len(entries)-1].Sequence)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		base.Logf("changes_view: Query took %v to return %d rows, options = %#v",
			elapsed, len(entries), optMap)
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
