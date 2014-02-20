package db

import (
	"encoding/json"
	"time"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

// Unmarshaled JSON structure for "changes" view results
type ViewResult struct {
	TotalRows int `json:"total_rows"`
	Rows      []ViewRow
	Errors    []couchbase.ViewError
}

// One "changes" row in a ViewResult
type ViewRow struct {
	ID    string
	Key   []interface{} // Actually [channelName, sequence]
	Value []interface{} // Actually [docID, revID, deleted?, removed?]
	Doc   json.RawMessage
}

// Queries the 'channels' view to get a range of sequences as LogEntries.
func (dbc *DatabaseContext) getChangesFromView(channelName string, startSeq, endSeq uint64, options ChangesOptions) (LogEntries, error) {
	// Set up range & other options:
	endKey := []interface{}{channelName, endSeq}
	if endSeq == 0 {
		endKey[1] = map[string]interface{}{} // infinity
	}
	opts := Body{
		"stale":        false,
		"startkey":     []interface{}{channelName, startSeq},
		"endkey":       endKey,
		"include_docs": options.Conflicts || options.IncludeDocs,
	}
	if options.Limit > 0 {
		opts["limit"] = options.Limit
	}

	// Query the view:
	base.LogTo("Cache", "Querying 'changes' for channel %q: %#v", channelName, opts)
	vres := ViewResult{}
	err := dbc.Bucket.ViewCustom("sync_gateway", "channels", opts, &vres)
	if err != nil {
		base.Log("Error from 'channels' view: %v", err)
		return nil, err
	}

	// Convert the output to LogEntries:
	entries := make(LogEntries, 0, len(vres.Rows))
	for _, row := range vres.Rows {
		sequence := uint64(row.Key[1].(float64))
		docID := row.Value[0].(string)
		revID := row.Value[1].(string)
		entry := &LogEntry{
			LogEntry: channels.LogEntry{
				Sequence: sequence,
				DocID:    docID,
				RevID:    revID,
			},
			received: time.Now(),
		}
		if len(row.Value) >= 3 && row.Value[2].(bool) {
			entry.Flags |= channels.Deleted
		}
		if len(row.Value) >= 4 && row.Value[3].(bool) {
			entry.Flags |= channels.Removed
		}
		//base.LogTo("Cache", "  Got view sequence #%d (%q / %q)", sequence, docID, revID)
		entries = append(entries, entry)
	}

	if len(entries) > 0 {
		base.LogTo("Cache", "  Got %d rows from view for %q: #%d ... #%d",
			len(entries), channelName, entries[0].Sequence, entries[len(entries)-1].Sequence)
	} else {
		base.LogTo("Cache", "  Got no rows from view for %q", channelName)
	}
	return entries, nil
}
