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
	Since       uint64
	Limit       int
	Conflicts   bool
	IncludeDocs bool
	Wait        bool
}

// A changes entry; Database.getChanges returns an array of these.
// Marshals into the standard CouchDB _changes format.
type ChangeEntry struct {
	Seq     uint64       `json:"seq"`
	ID      string       `json:"id"`
	Deleted bool         `json:"deleted,omitempty"`
	Removed channels.Set `json:"removed,omitempty"`
	Doc     Body         `json:"doc,omitempty"`
	Changes []ChangeRev  `json:"changes"`
}

type ChangeRev map[string]string

type ViewDoc struct {
	Json json.RawMessage // should be type 'document', but that fails to unmarshal correctly
}

type ViewRow struct {
	ID    string
	Key   interface{}
	Value interface{}
	Doc   *ViewDoc
}

type ViewResult struct {
	TotalRows int `json:"total_rows"`
	Rows      []ViewRow
	Errors    []couchbase.ViewError
}

const kChangesPageSize = 200

// Returns a list of all the changes made on a channel. Does NOT check authorization.
func (db *Database) ChangesFeed(channel string, options ChangesOptions) (<-chan *ChangeEntry, error) {
	log, err := db.GetChannelLog(channel, options.Since)
	if err != nil {
		return nil, err
	}

	feed := make(chan *ChangeEntry, 5)
	go func() {
		defer close(feed)
		for _, logEntry := range log {
			change := ChangeEntry{
				Seq:     logEntry.Sequence,
				ID:      logEntry.DocID,
				Deleted: logEntry.Deleted,
				Changes: []ChangeRev{{"rev": logEntry.RevID}},
			}
			if logEntry.Removed {
				change.Removed = channels.SetOf(channel)
			}
			if options.IncludeDocs && !logEntry.Removed {
				change.Doc, _ = db.GetRev(logEntry.DocID, logEntry.RevID, false, nil)
			}
			if options.Conflicts {
				//FIX: Handle conflicts somehow??
			}
			feed <- &change

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
func (db *Database) ChangesFeedFromView(channel string, options ChangesOptions) (<-chan *ChangeEntry, error) {
	lastSequence := options.Since
	endkey := []interface{}{channel, map[string]interface{}{}}
	totalLimit := options.Limit
	usingDocs := options.Conflicts || options.IncludeDocs
	opts := Body{"stale": false, "update_seq": true,
		"endkey":       endkey,
		"include_docs": usingDocs}

	feed := make(chan *ChangeEntry, kChangesPageSize)

	lastSeq := db.LastSequence()
	if options.Since >= lastSeq && !options.Wait {
		close(feed)
		return feed, nil
	}

	// Generate the output in a new goroutine, writing to 'feed':
	go func() {
		defer close(feed)
		for {
			// Query the 'channels' view:
			opts["startkey"] = []interface{}{channel, lastSequence + 1}
			limit := totalLimit
			if limit == 0 || limit > kChangesPageSize {
				limit = kChangesPageSize
			}
			opts["limit"] = limit

			var vres ViewResult
			var err error
			for len(vres.Rows) == 0 {
				vres = ViewResult{}
				err = db.Bucket.ViewCustom("sync_gateway", "channels", opts, &vres)
				if err != nil {
					base.Log("Error from 'channels' view: %v", err)
					return
				}
				if len(vres.Rows) == 0 {
					if !options.Wait || !db.WaitForRevision(channels.SetOf(channel)) {
						return
					}
				}
			}

			for _, row := range vres.Rows {
				key := row.Key.([]interface{})
				lastSequence = uint64(key[1].(float64))
				value := row.Value.([]interface{})
				docID := value[0].(string)
				revID := value[1].(string)
				entry := &ChangeEntry{
					Seq:     lastSequence,
					ID:      docID,
					Changes: []ChangeRev{{"rev": revID}},
					Deleted: (len(value) >= 3 && value[2].(bool)),
				}
				if len(value) >= 4 && value[3].(bool) {
					entry.Removed = channels.SetOf(channel)
				}
				if usingDocs {
					doc, err := unmarshalDocument(docID, row.Doc.Json)
					if err == nil {
						if options.Conflicts {
							for _, leafID := range doc.History.getLeaves() {
								if leafID != revID {
									entry.Changes = append(entry.Changes, ChangeRev{"rev": leafID})
								}
							}
						}
						if options.IncludeDocs {
							entry.Doc, _ = db.getRevFromDoc(doc, revID, false)
						}
					}
				}
				feed <- entry
			}

			// Step to the next page of results:
			nRows := len(vres.Rows)
			if nRows < kChangesPageSize || options.Wait {
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

// Returns of all the changes made to multiple channels.
func (db *Database) MultiChangesFeed(chans channels.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {
	if len(chans) == 0 {
		return nil, nil
	}
	base.LogTo("Changes", "MultiChangesFeed(%s, %+v) ...", chans, options)

	waitMode := options.Wait
	options.Wait = false

	output := make(chan *ChangeEntry, kChangesPageSize)
	go func() {
		defer close(output)

		for {
			// Restrict to available channels, expand wild-card, and find since when these channels
			// have been available to the user:
			var channelsSince channels.TimedSet
			if db.user != nil {
				channelsSince = db.user.FilterToAvailableChannels(chans)
			} else {
				channelsSince = chans.AtSequence(1)
			}
			base.LogTo("Changes", "MultiChangesFeed: channels expand to %s ...", channelsSince)

			feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))
			for name, _ := range channelsSince {
				feedOpts := options
				if channelsSince[name] > options.Since {
					feedOpts.Since = 0 // channel wasn't available before, so start from beginning
				}
				feed, err := db.ChangesFeed(name, feedOpts)
				if err != nil {
					base.Warn("Error reading changes feed %q: %v", name, err)
					return
				}
				feeds = append(feeds, feed)
			}
			current := make([]*ChangeEntry, len(feeds))

			var lastSeqSent uint64
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
					if cur != nil && cur.Seq < minSeq {
						minSeq = cur.Seq
						minEntry = cur
					}
				}
				if minEntry == nil {
					break // Exit the loop when there are no more entries
				}

				// Clear the current entries for the sequence just sent:
				for i, cur := range current {
					if cur != nil && cur.Seq == minEntry.Seq {
						current[i] = nil
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
				output <- minEntry
				lastSeqSent = minEntry.Seq
			}

			// If nothing found, and in wait mode: wait for the db to change, then run again
			if lastSeqSent > 0 || !waitMode || !db.WaitForRevision(chans) {
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
func (db *Database) GetChanges(channels channels.Set, options ChangesOptions) ([]*ChangeEntry, error) {
	var changes = make([]*ChangeEntry, 0, 50)
	feed, err := db.MultiChangesFeed(channels, options)
	if err == nil && feed != nil {
		for entry := range feed {
			changes = append(changes, entry)
		}
	}
	return changes, err
}

func (db *Database) WaitForRevision(chans channels.Set) bool {
	base.LogTo("Changes", "\twaiting for a revision...")
	db.tapNotifier.L.Lock()
	defer db.tapNotifier.L.Unlock()
	db.tapNotifier.Wait()
	base.LogTo("Changes", "\t...done waiting")
	return true
}

func (context *DatabaseContext) LastSequence() uint64 {
	return context.sequences.lastSequence()
}

func (db *Database) ReserveSequences(numToReserve uint64) error {
	return db.sequences.reserveSequences(numToReserve)
}

//////// CHANNEL LOG DOCUMENTS:

func channelLogDocID(channelName string) string {
	return "_sync:log:" + channelName
}

// Loads a channel's log from the database and returns it.
func (db *Database) GetChannelLog(channelName string, since uint64) (channels.ChannelLog, error) {
	var log channels.ChannelLog
	if err := db.Bucket.Get(channelLogDocID(channelName), &log); err != nil {
		if base.IsDocNotFoundError(err) {
			err = nil
		}
		return nil, err
	}
	return log.Since(since), nil
}

// Adds a new change to a channel log.
func (db *Database) AddToChannelLog(channelName string, entry channels.LogEntry) error {
	logDocID := channelLogDocID(channelName)
	err := db.Bucket.Update(logDocID, 0, func(currentValue []byte) ([]byte, error) {
		// Be careful: this block can be invoked multiple times if there are races!
		var log channels.ChannelLog
		if currentValue != nil {
			if err := json.Unmarshal(currentValue, &log); err != nil {
				base.Warn("ChannelLog %q is unreadable; resetting", logDocID)
				log = nil
			}
		}
		if !log.Add(entry) {
			return nil, couchbase.UpdateCancel // nothing to change, so cancel
		}
		return json.Marshal(log)
	})
	if err == couchbase.UpdateCancel {
		err = nil
	}
	return err
}

func (db *Database) AddToChannelLogs(changedChannels channels.Set, channelMap ChannelMap, entry channels.LogEntry) error {
	var err error
	base.LogTo("Changes", "Updating #%d %q/%q in channels %s", entry.Sequence, entry.DocID, entry.RevID, changedChannels)
	for channel, _ := range changedChannels {
		entry.Removed = (channelMap[channel] != nil)
		if err1 := db.AddToChannelLog(channel, entry); err != nil {
			err = err1
		}
	}
	return err
}
