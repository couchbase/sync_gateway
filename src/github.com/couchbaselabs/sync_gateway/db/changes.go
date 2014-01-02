//  Copyright (c) 2012 Couchbase, Inc.
//  Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
//  except in compliance with the License. You may obtain a copy of the License at
//    http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software distributed under the
//  License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
//  either express or implied. See the License for the specific language governing permissions
//  and limitations under the License.

package db

import (
	"encoding/json"
	"math"

	"github.com/couchbaselabs/go-couchbase"

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

// Options for Database.getChanges
type ChangesOptions struct {
	Since       channels.TimedSet // maps channel -> last sequence # seen on it
	Limit       int
	Conflicts   bool
	IncludeDocs bool
	Wait        bool
	Terminator  chan bool // Caller can close this channel to terminate the feed
}

// A changes entry; Database.getChanges returns an array of these.
// Marshals into the standard CouchDB _changes format.
type ChangeEntry struct {
	seqNo   uint64      // Internal use only: sequence # in specific channel
	Seq     string      `json:"seq"` // Public sequence ID (TimedSet)
	ID      string      `json:"id"`
	Deleted bool        `json:"deleted,omitempty"`
	Removed base.Set    `json:"removed,omitempty"`
	Doc     Body        `json:"doc,omitempty"`
	Changes []ChangeRev `json:"changes"`
}

type ChangeRev map[string]string

type ViewDoc struct {
	Json json.RawMessage // should be type 'document', but that fails to unmarshal correctly
}

// One "changes" row in a ViewResult
type ViewRow struct {
	ID    string
	Key   interface{}
	Value interface{}
	Doc   *ViewDoc
}

// Unmarshaled JSON structure for "changes" view results
type ViewResult struct {
	TotalRows int `json:"total_rows"`
	Rows      []ViewRow
	Errors    []couchbase.ViewError
}

// Number of rows to query from the changes view at one time
const kChangesViewPageSize = 1000

func (db *Database) addDocToChangeEntry(doc *document, entry *ChangeEntry, includeDocs, includeConflicts bool) {
	if doc != nil {
		revID := entry.Changes[0]["rev"]
		if includeConflicts {
			for _, leafID := range doc.History.getLeaves() {
				if leafID != revID {
					entry.Changes = append(entry.Changes, ChangeRev{"rev": leafID})
				}
			}
		}
		if includeDocs {
			entry.Doc, _ = db.getRevFromDoc(doc, revID, false)
		}
	}
}

// Returns a list of all the changes made on a channel.
// Does NOT handle the Wait option. Does NOT check authorization.
func (db *Database) changesFeed(channel string, options ChangesOptions) (<-chan *ChangeEntry, error) {
	since := options.Since[channel]
	channelLog, err := db.changesWriter.getChangeLog(channel, since)
	if err != nil {
		base.Warn("Error reading channel-log %q (using view instead): %v", channel, err)
		channelLog = nil
	}
	rebuildLog := channelLog == nil && err == nil && (EnableStarChannelLog || channel != "*")
	var log []*channels.LogEntry
	if channelLog != nil {
		log = channelLog.Entries
	}

	var viewFeed <-chan *ChangeEntry
	if channelLog == nil || channelLog.Since > since {
		var upToSeq uint64
		if channelLog != nil {
			upToSeq = channelLog.Since
		}
		// Channel log may not go back far enough, so also fetch view-based change feed:
		viewFeed, err = db.changesFeedFromView(channel, options, upToSeq)
		if err != nil {
			return nil, err
		}
	}

	feed := make(chan *ChangeEntry, 5)
	go func() {
		defer close(feed)

		// First, if we need to backfill from the view, write its early entries to the channel:
		if viewFeed != nil {
			newLog := channels.ChangeLog{Since: since}
			for change := range viewFeed {
				if channelLog != nil && change.seqNo > channelLog.Since {
					// TODO: Close the view-based feed somehow
					break
				}

				select {
				case <-options.Terminator:
					base.LogTo("Changes+", "Aborting changesFeed (reading from view)")
					return
				case feed <- change:
				}

				if rebuildLog {
					// If there wasn't any channel log, build up a new one from the view:
					entry := channels.LogEntry{
						Sequence: change.seqNo,
						DocID:    change.ID,
						RevID:    change.Changes[0]["rev"],
					}
					if change.Deleted {
						entry.Flags |= channels.Deleted
					}
					if change.Removed != nil {
						entry.Flags |= channels.Removed
					}
					newLog.Add(entry)
					newLog.TruncateTo(MaxChangeLogLength)
				}
			}

			if rebuildLog {
				// Save the missing channel log we just rebuilt:
				base.LogTo("Changes", "Saving rebuilt channel log %q with %d sequences",
					channel, len(newLog.Entries))
				db.changesWriter.addChangeLog(channel, &newLog)
			}
		}

		// Now write each log entry to the 'feed' channel in turn:
		for _, logEntry := range log {
			if !options.Conflicts && (logEntry.Flags&channels.Hidden) != 0 {
				//continue  // FIX: had to comment this out.
				// This entry is shadowed by a conflicting one. We would like to skip it.
				// The problem is that if this is the newest revision of this doc, then the
				// doc will appear under this sequence # in the changes view, which means
				// we won't emit the doc at all because we already stopped emitting entries
				// from the view before this point.
			}
			change := ChangeEntry{
				seqNo:   logEntry.Sequence,
				ID:      logEntry.DocID,
				Deleted: (logEntry.Flags & channels.Deleted) != 0,
				Changes: []ChangeRev{{"rev": logEntry.RevID}},
			}
			if logEntry.Flags&channels.Removed != 0 {
				change.Removed = channels.SetOf(channel)
			} else if options.IncludeDocs || options.Conflicts {
				doc, _ := db.GetDoc(logEntry.DocID)
				db.addDocToChangeEntry(doc, &change, options.IncludeDocs, false)
			}

			select {
			case <-options.Terminator:
				base.LogTo("Changes+", "Aborting changesFeed")
				return
			case feed <- &change:
			}

			if options.Limit > 0 {
				options.Limit--
				if options.Limit == 0 {
					break
				}
			}
		}
	}()
	return feed, nil
}

// Returns a list of all the changes made on a channel, reading from a view instead of the
// channel log. This will include all historical changes, but may omit very recent ones.
func (db *Database) changesFeedFromView(channel string, options ChangesOptions, upToSeq uint64) (<-chan *ChangeEntry, error) {
	base.LogTo("Changes", "Getting 'changes' view for channel %q %#v", channel, options)
	since := options.Since[channel]
	endkey := []interface{}{channel, upToSeq}
	if upToSeq == 0 {
		endkey[1] = map[string]interface{}{} // infinity
	}
	totalLimit := options.Limit
	usingDocs := options.Conflicts || options.IncludeDocs
	opts := Body{"stale": false, "update_seq": true,
		"endkey":       endkey,
		"include_docs": usingDocs}

	feed := make(chan *ChangeEntry, kChangesViewPageSize)

	// Generate the output in a new goroutine, writing to 'feed':
	go func() {
		defer close(feed)
		for {
			// Query the 'channels' view:
			opts["startkey"] = []interface{}{channel, since + 1}
			limit := totalLimit
			if limit == 0 || limit > kChangesViewPageSize {
				limit = kChangesViewPageSize
			}
			opts["limit"] = limit

			var waiter *changeWaiter
			if options.Wait {
				waiter = db.tapListener.NewWaiterWithChannels(base.SetOf(channel), nil)
			}
			var vres ViewResult
			var err error
			for len(vres.Rows) == 0 {
				base.LogTo("Changes+", "Querying 'changes' for channel %q %#v", channel, opts)
				vres = ViewResult{}
				err = db.Bucket.ViewCustom("sync_gateway", "channels", opts, &vres)
				if err != nil {
					base.Log("Error from 'channels' view: %v", err)
					return
				}
				if len(vres.Rows) == 0 {
					if waiter == nil || !waiter.Wait() {
						return
					}
				}
			}

			for _, row := range vres.Rows {
				key := row.Key.([]interface{})
				since = uint64(key[1].(float64))
				value := row.Value.([]interface{})
				docID := value[0].(string)
				revID := value[1].(string)
				entry := &ChangeEntry{
					seqNo:   since,
					ID:      docID,
					Changes: []ChangeRev{{"rev": revID}},
					Deleted: (len(value) >= 3 && value[2].(bool)),
				}
				if len(value) >= 4 && value[3].(bool) {
					entry.Removed = channels.SetOf(channel)
				} else if usingDocs {
					doc, _ := unmarshalDocument(docID, row.Doc.Json)
					db.addDocToChangeEntry(doc, entry, options.IncludeDocs, options.Conflicts)
				}

				select {
				case <-options.Terminator:
					base.LogTo("Changes+", "Aborting changesFeedFromView")
					return
				case feed <- entry:
				}
			}

			// Step to the next page of results:
			nRows := len(vres.Rows)
			if nRows < kChangesViewPageSize || options.Wait {
				break
			}
			if totalLimit > 0 {
				totalLimit -= nRows
				if totalLimit <= 0 {
					break
				}
			}
			delete(opts, "stale") // we only need to update the index once
		}
	}()
	return feed, nil
}

// Returns the (ordered) union of all of the changes made to multiple channels.
func (db *Database) MultiChangesFeed(chans base.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {
	if len(chans) == 0 {
		return nil, nil
	}
	base.LogTo("Changes", "MultiChangesFeed(%s, %+v) ...", chans, options)

	var changeWaiter *changeWaiter
	if options.Wait {
		options.Wait = false
		changeWaiter = db.tapListener.NewWaiterWithChannels(chans, db.user)
	}
	if options.Since == nil {
		options.Since = channels.TimedSet{}
	}

	output := make(chan *ChangeEntry, kChangesViewPageSize)
	go func() {
		defer close(output)

		// This loop is used to re-run the fetch after every database change, in Wait mode
	outer:
		for {
			// Restrict to available channels, expand wild-card, and find since when these channels
			// have been available to the user:
			var channelsSince channels.TimedSet
			if db.user != nil {
				channelsSince = db.user.FilterToAvailableChannels(chans)
			} else {
				channelsSince = channels.AtSequence(chans, 1)
			}
			base.LogTo("Changes", "MultiChangesFeed: channels expand to %s ...", channelsSince)

			// Populate the parallel arrays of channels and names:
			latestSequence, err := db.LastSequence()
			if err != nil {
				base.Warn("MultiChangesFeed got error from LastSequence: %v", err)
				return
			}
			feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))
			names := make([]string, 0, len(channelsSince))
			for name, _ := range channelsSince {
				var feed <-chan *ChangeEntry
				if latestSequence > options.Since[name] {
					var err error
					feed, err = db.changesFeed(name, options)
					if err != nil {
						base.Warn("MultiChangesFeed got error reading changes feed %q: %v", name, err)
						return
					}
				}
				feeds = append(feeds, feed)
				names = append(names, name)
			}
			current := make([]*ChangeEntry, len(feeds))

			// This loop reads the available entries from all the feeds in parallel, merges them,
			// and writes them to the output channel:
			var sentSomething bool
			for {
				//FIX: This assumes Reverse or Limit aren't set in the options
				// Read more entries to fill up the current[] array:
				for i, cur := range current {
					if cur == nil && feeds[i] != nil {
						var ok bool
						current[i], ok = <-feeds[i]
						if !ok {
							feeds[i] = nil
						}
					}
				}

				// Find the current entry with the minimum sequence:
				var minSeq uint64 = math.MaxUint64
				var minEntry *ChangeEntry
				for _, cur := range current {
					if cur != nil && cur.seqNo < minSeq {
						minSeq = cur.seqNo
						minEntry = cur
					}
				}
				if minEntry == nil {
					break // Exit the loop when there are no more entries
				}

				// Clear the current entries for the sequence just sent:
				for i, cur := range current {
					if cur != nil && cur.seqNo == minSeq {
						current[i] = nil
						// Update the public sequence ID and encode it into the entry:
						options.Since[names[i]] = minSeq
						cur.Seq = options.Since.String()
						cur.seqNo = 0
						// Also concatenate the matching entries' Removed arrays:
						if cur != minEntry && cur.Removed != nil {
							if minEntry.Removed == nil {
								minEntry.Removed = cur.Removed
							} else {
								minEntry.Removed = minEntry.Removed.Union(cur.Removed)
							}
						}
					}
				}

				// Send the entry, and repeat the loop:
				select {
				case <-options.Terminator:
					base.LogTo("Changes+", "Aborting MultiChangesFeed")
					return
				case output <- minEntry:
				}
				sentSomething = true

				// Stop when we hit the limit (if any):
				if options.Limit > 0 {
					options.Limit--
					if options.Limit == 0 {
						break outer
					}
				}
			}

			if sentSomething || changeWaiter == nil {
				break
			}

			// If nothing found, and in wait mode: wait for the db to change, then run again.
			// First notify the reader that we're waiting by sending a nil.
			output <- nil
			if !changeWaiter.Wait() {
				break
			}

			// Before checking again, update the User object in case its channel access has
			// changed while waiting:
			if err := db.ReloadUser(); err != nil {
				base.Warn("Error reloading user %q: %v", db.user.Name(), err)
				return
			}
		}
		base.LogTo("Changes", "MultiChangesFeed done")
	}()

	return output, nil
}

// Synchronous convenience function that returns all changes as a simple array.
func (db *Database) GetChanges(channels base.Set, options ChangesOptions) ([]*ChangeEntry, error) {
	options.Terminator = make(chan bool)
	defer close(options.Terminator)

	var changes = make([]*ChangeEntry, 0, 50)
	feed, err := db.MultiChangesFeed(channels, options)
	if err == nil && feed != nil {
		for entry := range feed {
			changes = append(changes, entry)
		}
	}
	return changes, err
}

func (db *Database) GetChangeLog(channelName string, afterSeq uint64) (*channels.ChangeLog, error) {
	return db.changesWriter.getChangeLog(channelName, afterSeq)
}

// This is only used for unit tests
func (context *DatabaseContext) CheckpointChangeLogs() {
	context.changesWriter.checkpoint()
}
