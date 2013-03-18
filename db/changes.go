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
	Since          uint64
	Limit          int
	Conflicts      bool
	IncludeDocs    bool
	includeDocMeta bool
	Wait           bool
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
	docMeta *document
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
	lastSequence := options.Since
	endkey := []interface{}{channel, map[string]interface{}{}}
	totalLimit := options.Limit
	usingDocs := options.Conflicts || options.IncludeDocs || options.includeDocMeta
	opts := Body{"stale": false, "update_seq": true,
		"endkey":       endkey,
		"include_docs": usingDocs}

	feed := make(chan *ChangeEntry, kChangesPageSize)

	lastSeq, err := db.LastSequence()
	if err == nil && options.Since >= lastSeq && !options.Wait {
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
					if !options.Wait || !db.WaitForRevision() {
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
				if len(value) >= 3 && !value[2].(bool) {
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
						if options.includeDocMeta {
							entry.docMeta = doc
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

// Returns of all the changes made to multiple channels. Does NOT check authorization.
func (db *Database) MultiChangesFeed(channels channels.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {
	if len(channels) == 0 {
		return nil, nil
	} else if len(channels) == 1 {
		for channel, _ := range channels {
			return db.ChangesFeed(channel, options)
		}
	}

	waitMode := options.Wait
	options.Wait = false

	output := make(chan *ChangeEntry, kChangesPageSize)
	go func() {
		defer close(output)

		for {
			feeds := make([]<-chan *ChangeEntry, 0, len(channels))
			current := make([]*ChangeEntry, len(channels))
			i := 0
			for name, _ := range channels {
				var err error
				feed, err := db.ChangesFeed(name, options)
				if err != nil {
					return
				}
        feeds = append(feeds,feed)
				i++
			}

			wroteAnything := false
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
					break
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

				// Send the entry:
				output <- minEntry
				wroteAnything = true
			}

			// In wait mode, wait for the db to change, then run again
			if wroteAnything || !waitMode || !db.WaitForRevision() {
				break
			}
		}
	}()

	return output, nil
}

// Synchronous convenience function that returns all changes as a simple array.
// Does NOT check authorization.
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

func (db *Database) WaitForRevision() bool {
	base.Log("\twaiting for a revision...")
	waitFor("")
	base.Log("\t...done waiting")
	return true
}

func (db *Database) NotifyRevision() {
	notify("")
}

func (db *Database) LastSequence() (uint64, error) {
	return db.sequences.lastSequence()
}

func (db *Database) ReserveSequences(numToReserve uint64) error {
	return db.sequences.reserveSequences(numToReserve)
}
