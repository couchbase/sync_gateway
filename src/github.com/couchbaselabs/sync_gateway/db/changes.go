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
	"fmt"
	"math"

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

type ChangesTracker struct {
	Since           SequenceID          // highest sequence sent
	MinChannelSince SequenceID          // min (last sequence from each channel)
	MaxDuplicateSeq uint64              // highest duplicate sequence identified
	CurrentSent     []uint64            // sequences sent in the last changes iteration (for deduplication)
	LastSent        map[uint64]struct{} // sequences sent in the last changes iteration (for deduplication)
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
func (db *Database) changesFeed(channel string, options ChangesOptions) (<-chan *ChangeEntry, uint64, error) {
	// Uncomment the following two lines (and add an import for 'time') to run TestChannelRace in change_cache_test.go
	//  base.LogTo("Sequences", "Simulate slow processing time for channel %s - sleeping for 100 ms", channel)
	//  time.Sleep(100 * time.Millisecond)
	dbExpvars.Add("channelChangesFeeds", 1)
	log, err := db.changeCache.GetChangesInChannel(channel, options)
	if err != nil {
		return nil, options.Since.SafeSequence(), err
	}

	if len(log) == 0 {
		// There are no entries newer than 'since'. Return an empty feed:
		feed := make(chan *ChangeEntry)
		close(feed)
		return feed, options.Since.SafeSequence(), nil
	}

	highSequence := log[len(log)-1].Sequence
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

			change := makeChangeEntry(logEntry, seqID, channel)

			select {
			case <-options.Terminator:
				base.LogTo("Changes+", "Aborting changesFeed")
				return
			case feed <- &change:
			}
		}
	}()
	return feed, highSequence, nil
}

func makeChangeEntry(logEntry *LogEntry, seqID SequenceID, channelName string) ChangeEntry {
	change := ChangeEntry{
		Seq:      seqID,
		ID:       logEntry.DocID,
		Deleted:  (logEntry.Flags & channels.Deleted) != 0,
		Changes:  []ChangeRev{{"rev": logEntry.RevID}},
		branched: (logEntry.Flags & channels.Branched) != 0,
	}
	if logEntry.Flags&channels.Removed != 0 {
		change.Removed = channels.SetOf(channelName)
	}
	return change
}

// Returns the (ordered) union of all of the changes made to multiple channels.
func (db *Database) MultiChangesFeed(chans base.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {

	dbExpvars.Add("multiChangesFeeds", 1)
	if len(chans) == 0 {
		return nil, nil
	}
	to := ""
	if db.user != nil && db.user.Name() != "" {
		to = fmt.Sprintf("  (to %s)", db.user.Name())
	}

	base.LogTo("Changes", "MultiChangesFeed(%s, %+v) ... %s", chans, options, to)

	if (options.Continuous || options.Wait) && options.Terminator == nil {
		base.Warn("MultiChangesFeed: Terminator missing for Continuous/Wait mode")
	}

	output := make(chan *ChangeEntry, 50)
	go func() {
		defer func() {
			base.LogTo("Changes", "MultiChangesFeed done %s", to)
			close(output)
		}()

		var changeWaiter *changeWaiter
		var userChangeCount uint64
		var lowSequence uint64
		// var lateSequenceFeeds map[string]*lateSequenceFeed
		if options.Wait {
			options.Wait = false
			changeWaiter = db.tapListener.NewWaiterWithChannels(chans, db.user)
			userChangeCount = changeWaiter.CurrentUserCount()
		}

		// For a continuous feed, initialise
		//  - lateSequenceFeeds to track late-arriving sequences to the channel caches.
		//  - changesTracker to ensure correctness across multiple channels
		/*
			if options.Continuous {
				lateSequenceFeeds = make(map[string]*lateSequenceFeed)

			}
		*/

		// disable de-dup
		/*
			changesTracker := &ChangesTracker{
				Since:           options.Since,
				MinChannelSince: options.Since,
			}
		*/

		var minChannelSince uint64 // used to recalculate in each iteration of the loop

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
			base.LogTo("Changes+", "MultiChangesFeed: channels expand to %#v ... %s", channelsSince, to)

			// lowSequence is used to send composite keys to clients, so that they can obtain any currently
			// skipped sequences in a future iteration or request.
			oldestSkipped := db.changeCache.getOldestSkippedSequence()
			if oldestSkipped > 0 {
				lowSequence = oldestSkipped - 1
			} else {
				lowSequence = 0
			}

			// Populate the parallel arrays of channels and names:
			feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))
			names := make([]string, 0, len(channelsSince))
			minChannelSince = math.MaxUint64
			for name, seqAddedAt := range channelsSince {
				chanOpts := options
				if seqAddedAt > 1 && options.Since.Before(SequenceID{Seq: seqAddedAt}) && options.Since.TriggeredBy == 0 {
					// Newly added channel so send all of it to user:
					chanOpts.Since = SequenceID{Seq: 0, TriggeredBy: seqAddedAt}
				}
				feed, highSeq, err := db.changesFeed(name, chanOpts)

				if highSeq < minChannelSince {
					minChannelSince = highSeq
				}
				if err != nil {
					base.Warn("MultiChangesFeed got error reading changes feed %q: %v", name, err)
					return
				}
				feeds = append(feeds, feed)
				names = append(names, name)

				// Late sequence handling - for out-of-order sequences prior to options.Since that
				// have arrived in the channel cache since this changes request started.  Only need for
				// continuous feeds - one-off changes requests only need the standard channel cache.
				// disable late sequence handling
				/*
					if options.Continuous {
						lateSequenceFeedHandler := lateSequenceFeeds[name]
						if lateSequenceFeedHandler != nil {
							latefeed, err := db.getLateFeed(lateSequenceFeedHandler)
							if err != nil {
								base.Warn("MultiChangesFeed got error reading late sequence feed %q: %v", name, err)
							} else {
								// Mark feed as actively used in this iteration.  Used to remove lateSequenceFeeds
								// when the user loses channel access
								lateSequenceFeedHandler.active = true
								feeds = append(feeds, latefeed)
								names = append(names, fmt.Sprintf("late_%s", name))
							}

						} else {
							// Initialize lateSequenceFeeds[name] for next iteration
							disable late sequence handling
							lateSequenceFeeds[name] = db.newLateSequenceFeed(name)
						}
					}
				*/
			}

			// If the user object has changed, create a special pseudo-feed for it:
			if db.user != nil {
				userSeq := SequenceID{Seq: db.user.Sequence()}
				if options.Since.Before(userSeq) {
					name := db.user.Name()
					if name == "" {
						name = "GUEST"
					}
					entry := ChangeEntry{
						Seq:     userSeq,
						ID:      "_user/" + name,
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

				// Only increment minSeq if it's later than Since, to prevent a late-arriving sequence
				// from rolling back the since used for the regular feed
				if options.Since.Before(minSeq) {
					// Remember we got this far, for the next outer loop iteration:
					options.Since = minSeq
				}

				// Check whether the sequence was already sent by this feed

				// disable deduplication
				/*
					if _, found := changesTracker.LastSent[minSeq.Seq]; found {
						if minSeq.Seq > changesTracker.MaxDuplicateSeq {
							changesTracker.MaxDuplicateSeq = minSeq.Seq
						}
					} else {
				*/
				// Add the doc body or the conflicting rev IDs, if those options are set:
				if options.IncludeDocs || options.Conflicts {
					db.addDocToChangeEntry(minEntry, options)
				}

				// Update the low sequence on the entry we're going to send
				minEntry.Seq.LowSeq = lowSequence

				// Send the entry, and repeat the loop:
				base.LogTo("Changes+", "MultiChangesFeed sending %+v %s", minEntry, to)
				select {
				case <-options.Terminator:
					return
				case output <- minEntry:
				}

				// disable de-dup
				// changesTracker.CurrentSent = append(changesTracker.CurrentSent, minEntry.Seq.Seq)
				sentSomething = true

				// Stop when we hit the limit (if any):
				if options.Limit > 0 {
					options.Limit--
					if options.Limit == 0 {
						break outer
					}
				}
				// disable de-dup
				// }

			}

			if !options.Continuous && (sentSomething || changeWaiter == nil) {
				break
			}

			// If nothing found, and in wait mode: wait for the db to change, then run again.
			// First notify the reader that we're waiting by sending a nil.
			if !sentSomething {
				base.LogTo("Changes+", "MultiChangesFeed waiting... %s", to)
				output <- nil
				if !changeWaiter.Wait() {
					break
				}
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

			// Update changesTracker (moves currentSent to lastSent)
			/*
				changesTracker.endIteration()
				options.Since.Seq = changesTracker.nextSince()
			*/
			// Clean up inactive lateSequenceFeeds (because user has lost access to the channel)
			/*
				for channel, lateFeed := range lateSequenceFeeds {
					if !lateFeed.active {
						db.closeLateFeed(lateFeed)
						lateSequenceFeeds[channel] = nil
					} else {
						lateFeed.active = false
					}
				}
			*/
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

// Late Sequence Feed
// Manages the changes feed interaction with a channels cache's set of late-arriving entries
type lateSequenceFeed struct {
	active       bool   // Whether the changes feed is still serving the channel this feed is associated with
	lastSequence uint64 // Last late sequence processed on the feed
	channelName  string // channelName
}

// Returns a lateSequenceFeed for the channel, used to find late-arriving (previously
// skipped) sequences that have been sent to the channel cache.  The lateSequenceFeed stores the last (late)
// sequence seen by this particular _changes feed to support continuous changes.
func (db *Database) newLateSequenceFeed(channelName string) *lateSequenceFeed {
	chanCache := db.changeCache.channelCaches[channelName]
	if chanCache == nil {
		return nil
	}
	lsf := &lateSequenceFeed{
		active:       true,
		lastSequence: chanCache.InitLateSequenceClient(),
		channelName:  channelName,
	}
	return lsf
}

// Feed to process late sequences for the channel.  Updates lastSequence as it works the feed.
func (db *Database) getLateFeed(feedHandler *lateSequenceFeed) (<-chan *ChangeEntry, error) {

	logs, lastSequence, err := db.changeCache._getChannelCache(feedHandler.channelName).GetLateSequencesSince(feedHandler.lastSequence)
	if err != nil {
		return nil, err
	}
	if logs == nil || len(logs) == 0 {
		// There are no late entries newer than lastSequence
		feed := make(chan *ChangeEntry)
		close(feed)
		return feed, nil
	}

	feed := make(chan *ChangeEntry, 1)
	go func() {
		defer close(feed)
		// Write each log entry to the 'feed' channel in turn:
		for _, logEntry := range logs {
			// We don't need TriggeredBy handling here, because when backfilling from a
			// channel in response to a user being added to the channel, we don't need to worry about
			// late arrived sequences
			seqID := SequenceID{
				Seq: logEntry.Sequence,
			}
			change := makeChangeEntry(logEntry, seqID, feedHandler.channelName)
			feed <- &change
		}
	}()

	feedHandler.lastSequence = lastSequence
	return feed, nil
}

func (db *Database) closeLateFeed(feedHandler *lateSequenceFeed) {
	dbExpvars.Add("closedLateFeeds", 1)
	db.changeCache._getChannelCache(feedHandler.channelName).ReleaseLateSequenceClient(feedHandler.lastSequence)
}

// endIteration is called at the end of the changes loop, and moves the currently sent sequences
// into the previously sent map.
func (ct *ChangesTracker) endIteration() {

	if base.LogEnabled("Changes+") {
		base.LogTo("Changes+", ct.String())
	}

	// Empty the lastSent map.  Emptying here instead of doing a new make() to avoid GC
	if ct.LastSent == nil {
		ct.LastSent = make(map[uint64]struct{})
	} else {
		for entry := range ct.LastSent {
			delete(ct.LastSent, entry)
		}
	}
	var seq uint64
	// Move the currentSent into the lastSent map, emptying currentSent
	for len(ct.CurrentSent) > 0 {
		seq = ct.CurrentSent[0]
		ct.CurrentSent = ct.CurrentSent[1:]
		ct.LastSent[seq] = struct{}{}
	}
}

func (ct *ChangesTracker) nextSince() uint64 {
	// Determine the since value for the next iteration of the changes loop, in a way that
	// avoids missed sequences resulting from timing issues in channel caches

	// Worst case - the channel with the oldest high sequence
	since := ct.MinChannelSince.Seq

	// If there is a duplicate higher than this since value, can safely use it.
	// Duplicate guarantees that we've asked every channel for everything up to the duplicate
	// at least once.
	if ct.MaxDuplicateSeq > since {
		since = ct.MaxDuplicateSeq
	}
	if base.LogEnabled("Changes+") {
		base.LogTo("Changes+", "nextSince: %v", since)
	}
	return since
}

func (ct *ChangesTracker) String() string {

	lastSentString := "LastSent:["
	for seq, _ := range ct.LastSent {
		lastSentString = fmt.Sprintf("%s %d,", lastSentString, seq)
	}
	lastSentString = fmt.Sprintf("%s]", lastSentString)

	currSentString := "CurrentSent:["
	for _, seq := range ct.CurrentSent {
		currSentString = fmt.Sprintf("%s %d,", currSentString, seq)
	}
	currSentString = fmt.Sprintf("%s]", currSentString)

	return fmt.Sprintf("ChangesTracker: %s, %s", lastSentString, currSentString)
}
