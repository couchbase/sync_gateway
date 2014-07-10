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

	"github.com/couchbaselabs/sync_gateway/base"
	"github.com/couchbaselabs/sync_gateway/channels"
)

// Options for changes-feeds
type ChangesOptions struct {
	Since       SequenceID // sequence # to start _after_
	Limit       int        // Max number of changes to return, if nonzero
	Conflicts   bool       // Show all conflicting revision IDs, not just winning one?
	IncludeDocs bool       // Include doc body of each change?
	Wait        bool       // Wait for results, instead of immediately returning empty result?
	Continuous  bool       // Run continuously until terminated?
	Terminator  chan bool  // Caller can close this channel to terminate the feed
}

// A changes entry; Database.GetChanges returns an array of these.
// Marshals into the standard CouchDB _changes format.
type ChangeEntry struct {
	Seq      SequenceID  `json:"seq"`
	ID       string      `json:"id"`
	Deleted  bool        `json:"deleted,omitempty"`
	Removed  base.Set    `json:"removed,omitempty"`
	Doc      Body        `json:"doc,omitempty"`
	Changes  []ChangeRev `json:"changes"`
	branched bool
}

type ChangeRev map[string]string // Key is always "rev", value is rev ID

type ViewDoc struct {
	Json json.RawMessage // should be type 'document', but that fails to unmarshal correctly
}

// Adds a document body and/or its conflicts to a ChangeEntry
func (db *Database) addDocToChangeEntry(entry *ChangeEntry, options ChangesOptions) {
	includeConflicts := options.Conflicts && entry.branched
	if !options.IncludeDocs && !includeConflicts {
		return
	}
	doc, err := db.GetDoc(entry.ID)
	if err != nil {
		base.Warn("Changes feed: error getting doc %q: %v", entry.ID, err)
		return
	}

	revID := entry.Changes[0]["rev"]
	if includeConflicts {
		doc.History.forEachLeaf(func(leaf *RevInfo) {
			if leaf.ID != revID {
				entry.Changes = append(entry.Changes, ChangeRev{"rev": leaf.ID})
				if !leaf.Deleted {
					entry.Deleted = false
				}
			}
		})
	}
	if options.IncludeDocs {
		var err error
		entry.Doc, err = db.getRevFromDoc(doc, revID, false)
		if err != nil {
			base.Warn("Changes feed: error getting doc %q/%q: %v", doc.ID, revID, err)
		}
	}
}

// Creates a Go-channel of all the changes made on a channel.
// Does NOT handle the Wait option. Does NOT check authorization.
func (db *Database) changesFeed(channel string, options ChangesOptions) (<-chan *ChangeEntry, error) {
	dbExpvars.Add("channelChangesFeeds", 1)
	log, err := db.changeCache.GetChangesInChannel(channel, options)
	if err != nil {
		return nil, err
	}

	if len(log) == 0 {
		// There are no entries newer than 'since'. Return an empty feed:
		feed := make(chan *ChangeEntry)
		close(feed)
		return feed, nil
	}

	feed := make(chan *ChangeEntry, 1)
	go func() {
		defer close(feed)
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
			if logEntry.Sequence >= options.Since.TriggeredBy {
				options.Since.TriggeredBy = 0
			}
			seqID := SequenceID{
				Seq:         logEntry.Sequence,
				TriggeredBy: options.Since.TriggeredBy,
			}
			change := ChangeEntry{
				Seq:      seqID,
				ID:       logEntry.DocID,
				Deleted:  (logEntry.Flags & channels.Deleted) != 0,
				Changes:  []ChangeRev{{"rev": logEntry.RevID}},
				branched: (logEntry.Flags & channels.Branched) != 0,
			}
			if logEntry.Flags&channels.Removed != 0 {
				change.Removed = channels.SetOf(channel)
			}

			select {
			case <-options.Terminator:
				base.LogTo("Changes+", "Aborting changesFeed")
				return
			case feed <- &change:
			}
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

	if (options.Continuous || options.Wait) && options.Terminator == nil {
		base.Warn("MultiChangesFeed: Terminator missing for Continuous/Wait mode")
	}

	output := make(chan *ChangeEntry, 50)
	go func() {
		defer func() {
			base.LogTo("Changes", "MultiChangesFeed done")
			close(output)
		}()

		var changeWaiter *changeWaiter
		var userChangeCount uint64
		if options.Wait {
			options.Wait = false
			changeWaiter = db.tapListener.NewWaiterWithChannels(chans, db.user)
			userChangeCount = changeWaiter.CurrentUserCount()
		}

		// This loop is used to re-run the fetch after every database change, in Wait mode
	outer:
		for {
			// Restrict to available channels, expand wild-card, and find since when these channels
			// have been available to the user:
			var channelsSince channels.TimedSet
			if db.user != nil {
				channelsSince = db.user.FilterToAvailableChannels(chans)
			} else {
				channelsSince = channels.AtSequence(chans, 0)
			}
			base.LogTo("Changes+", "MultiChangesFeed: channels expand to %#v ...", channelsSince)

			// Populate the parallel arrays of channels and names:
			feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))
			names := make([]string, 0, len(channelsSince))
			for name, seqAddedAt := range channelsSince {
				chanOpts := options
				if seqAddedAt > 1 && options.Since.Before(SequenceID{Seq: seqAddedAt}) {
					// Newly added channel so send all of it to user:
					chanOpts.Since = SequenceID{Seq: 0, TriggeredBy: seqAddedAt}
				}
				feed, err := db.changesFeed(name, chanOpts)
				if err != nil {
					base.Warn("MultiChangesFeed got error reading changes feed %q: %v", name, err)
					return
				}
				feeds = append(feeds, feed)
				names = append(names, name)
			}

			// If the user object has changed, create a special pseudo-feed for it:
			if db.user != nil {
				userSeq := SequenceID{Seq: db.user.Sequence()}
				if options.Since.Before(userSeq) {
					entry := ChangeEntry{
						Seq:     userSeq,
						ID:      "_user/" + db.user.Name(),
						Changes: []ChangeRev{},
					}
					userFeed := make(chan *ChangeEntry, 1)
					userFeed <- &entry
					close(userFeed)
					feeds = append(feeds, userFeed)
					names = append(names, entry.ID)
				}
			}

			current := make([]*ChangeEntry, len(feeds))

			// This loop reads the available entries from all the feeds in parallel, merges them,
			// and writes them to the output channel:
			var sentSomething bool
			for {
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
				minSeq := MaxSequenceID
				var minEntry *ChangeEntry
				for _, cur := range current {
					if cur != nil && cur.Seq.Before(minSeq) {
						minSeq = cur.Seq
						minEntry = cur
					}
				}
				if minEntry == nil {
					break // Exit the loop when there are no more entries
				}

				// Clear the current entries for the sequence just sent:
				for i, cur := range current {
					if cur != nil && cur.Seq == minSeq {
						current[i] = nil
						//options.Since = minSeq //TEMP ???
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

				if !options.Since.Before(minSeq) {
					continue // out of order; skip it
				}

				// Add the doc body or the conflicting rev IDs, if those options are set:
				if options.IncludeDocs || options.Conflicts {
					db.addDocToChangeEntry(minEntry, options)
				}

				// Send the entry, and repeat the loop:
				base.LogTo("Changes+", "MultiChangesFeed sending %+v", minEntry)
				select {
				case <-options.Terminator:
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

			if !options.Continuous && (sentSomething || changeWaiter == nil) {
				break
			}

			// If nothing found, and in wait mode: wait for the db to change, then run again.
			// First notify the reader that we're waiting by sending a nil.
			base.LogTo("Changes+", "MultiChangesFeed waiting...")
			output <- nil
			if !changeWaiter.Wait() {
				break
			}

			// Check whether I was terminated while waiting for a change:
			select {
			case <-options.Terminator:
				return
			default:
			}

			// Before checking again, update the User object in case its channel access has
			// changed while waiting:
			if newCount := changeWaiter.CurrentUserCount(); newCount > userChangeCount {
				base.LogTo("Changes+", "MultiChangesFeed reloading user %q", db.user.Name())
				userChangeCount = newCount
				if err := db.ReloadUser(); err != nil {
					base.Warn("Error reloading user %q: %v", db.user.Name(), err)
					return
				}
			}
		}
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

func (db *Database) GetChangeLog(channelName string, afterSeq uint64) []*LogEntry {
	options := ChangesOptions{Since: SequenceID{Seq: afterSeq}}
	_, log := db.changeCache.getChannelCache(channelName).getCachedChanges(options)
	return log
}

// Wait until the change-cache has caught up with the latest writes to the database.
func (context *DatabaseContext) WaitForPendingChanges() (err error) {
	lastSequence, err := context.LastSequence()
	if err == nil {
		context.changeCache.waitForSequence(lastSequence)
	}
	return
}
