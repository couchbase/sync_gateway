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
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/couchbase/sync_gateway/base"
	"github.com/couchbase/sync_gateway/channels"
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
	HeartbeatMs uint64     // How often to send a heartbeat to the client
	TimeoutMs   uint64     // After this amount of time, close the longpoll connection
	ActiveOnly  bool       // If true, only return information on non-deleted, non-removed revisions
}

// A changes entry; Database.GetChanges returns an array of these.
// Marshals into the standard CouchDB _changes format.
type ChangeEntry struct {
	Seq        SequenceID  `json:"seq"`
	ID         string      `json:"id"`
	Deleted    bool        `json:"deleted,omitempty"`
	Removed    base.Set    `json:"removed,omitempty"`
	Doc        Body        `json:"doc,omitempty"`
	Changes    []ChangeRev `json:"changes"`
	Err        error       `json:"err,omitempty"` // Used to notify feed consumer of errors
	allRemoved bool        // Flag to track whether an entry is a removal in all channels visible to the user.
	branched   bool
	backfill   backfillFlag // Flag used to identify non-client entries used for backfill synchronization (di only)
}

const (
	WaiterClosed uint32 = iota
	WaiterHasChanges
	WaiterCheckTerminated
)

type backfillFlag int8

const (
	BackfillFlag_None backfillFlag = iota
	BackfillFlag_Pending
	BackfillFlag_Complete
)

type ChangeRev map[string]string // Key is always "rev", value is rev ID

type ViewDoc struct {
	Json json.RawMessage // should be type 'document', but that fails to unmarshal correctly
}

func (db *Database) AddDocToChangeEntry(entry *ChangeEntry, options ChangesOptions) {
	db.addDocToChangeEntry(entry, options)
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

	db.AddDocInstanceToChangeEntry(entry, doc, options)
}

// Adds a document body and/or its conflicts to a ChangeEntry
func (db *Database) AddDocInstanceToChangeEntry(entry *ChangeEntry, doc *document, options ChangesOptions) {
	includeConflicts := options.Conflicts && entry.branched

	revID := entry.Changes[0]["rev"]
	if includeConflicts {
		doc.History.forEachLeaf(func(leaf *RevInfo) {
			if leaf.ID != revID {
				if !leaf.Deleted {
					entry.Deleted = false
				}
				if !(options.ActiveOnly && leaf.Deleted) {
					entry.Changes = append(entry.Changes, ChangeRev{"rev": leaf.ID})
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
func (db *Database) changesFeed(channel string, options ChangesOptions, to string) (<-chan *ChangeEntry, error) {
	dbExpvars.Add("channelChangesFeeds", 1)
	log, err := db.changeCache.GetChanges(channel, options)
	base.LogTo("Changes+", "[changesFeed] Found %d changes for channel %s", len(log), channel)
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

			change := makeChangeEntry(logEntry, seqID, channel)

			base.LogTo("Changes+", "Channel feed processing seq:%v in channel %s %s", seqID, channel, to)
			select {
			case <-options.Terminator:
				base.LogTo("Changes+", "Terminating channel feed %s", to)
				return
			case feed <- &change:
			}
		}
	}()
	return feed, nil
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

func (ce *ChangeEntry) SetBranched(isBranched bool) {
	ce.branched = isBranched
}

func (ce *ChangeEntry) String() string {

	var deletedString, removedString, errString, allRemovedString, branchedString, backfillString string
	if ce.Deleted {
		deletedString = ", Deleted:true"
	}
	if len(ce.Removed) > 0 {
		removedString = fmt.Sprintf(", Removed:%v", ce.Removed)
	}
	if ce.Err != nil {
		errString = fmt.Sprintf(", Err:%v", ce.Err)
	}
	if ce.allRemoved {
		allRemovedString = ", allRemoved:true"
	}
	if ce.branched {
		branchedString = ", branched:true"
	}
	if ce.backfill != BackfillFlag_None {
		backfillString = fmt.Sprintf(", backfill:%d", ce.backfill)
	}
	return fmt.Sprintf("{Seq:%s, ID:%s, Changes:%s%s%s%s%s%s%s}", ce.Seq, ce.ID, ce.Changes, deletedString, removedString, errString, allRemovedString, branchedString, backfillString)
}

func makeErrorEntry(message string) ChangeEntry {

	change := ChangeEntry{
		Err: errors.New(message),
	}
	return change
}

func (db *Database) MultiChangesFeed(chans base.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {
	if len(chans) == 0 {
		return nil, nil
	}

	if (options.Continuous || options.Wait) && options.Terminator == nil {
		base.Warn("MultiChangesFeed: Terminator missing for Continuous/Wait mode")
	}
	if db.SequenceType == IntSequenceType {
		base.LogTo("Changes+", "Int sequence multi changes feed...")
		return db.SimpleMultiChangesFeed(chans, options)
	} else {
		base.LogTo("Changes+", "Vector multi changes feed...")
		return db.VectorMultiChangesFeed(chans, options)
	}
}

func (db *Database) startChangeWaiter(chans base.Set) *changeWaiter {
	waitChans := chans
	if db.user != nil {
		waitChans = db.user.ExpandWildCardChannel(chans)
	}
	return db.tapListener.NewWaiterWithChannels(waitChans, db.user)
}

func (db *Database) appendUserFeed(feeds []<-chan *ChangeEntry, names []string, options ChangesOptions) ([]<-chan *ChangeEntry, []string) {
	userSeq := SequenceID{Seq: db.user.Sequence()}
	if options.Since.Before(userSeq) {
		name := db.user.Name()
		if name == "" {
			name = base.GuestUsername
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
	return feeds, names
}

func (db *Database) checkForUserUpdates(userChangeCount uint64, changeWaiter *changeWaiter, isContinuous bool) (isChanged bool, newCount uint64, newChannels base.Set, err error) {

	newCount = changeWaiter.CurrentUserCount()
	// If not continuous, we force user reload as a workaround for https://github.com/couchbase/sync_gateway/issues/2068.  For continuous, #2068 is handled by addedChannels check, and
	// we can reload only when there's been a user change notification
	if newCount > userChangeCount || !isContinuous {
		var previousChannels channels.TimedSet
		var newChannels base.Set
		base.LogTo("Changes+", "MultiChangesFeed reloading user %+v", db.user)
		userChangeCount = newCount

		if db.user != nil {
			previousChannels = db.user.InheritedChannels()
			if err := db.ReloadUser(); err != nil {
				base.Warn("Error reloading user %q: %v", db.user.Name(), err)
				return false, 0, nil, err
			}
			// check whether channels have changed
			newChannels = db.user.GetAddedChannels(previousChannels)
			if len(newChannels) > 0 {
				base.LogTo("Changes+", "New channels found after user reload: %v", newChannels)
			}
		}
		return true, newCount, newChannels, nil
	}
	return false, userChangeCount, nil, nil
}

// Returns the (ordered) union of all of the changes made to multiple channels.
func (db *Database) SimpleMultiChangesFeed(chans base.Set, options ChangesOptions) (<-chan *ChangeEntry, error) {
	to := ""
	if db.user != nil && db.user.Name() != "" {
		to = fmt.Sprintf("  (to %s)", db.user.Name())
	}

	base.LogTo("Changes", "MultiChangesFeed(%s, %+v) ... %s", chans, options, to)
	output := make(chan *ChangeEntry, 50)

	go func() {
		defer func() {
			base.LogTo("Changes", "MultiChangesFeed done %s", to)
			close(output)
		}()

		var changeWaiter *changeWaiter
		var lowSequence uint64
		var currentCachedSequence uint64
		var lateSequenceFeeds map[string]*lateSequenceFeed
		var userCounter uint64     // Wait counter used to identify changes to the user document
		var addedChannels base.Set // Tracks channels added to the user during changes processing.
		var userChanged bool       // Whether the user document has changed in a given iteration loop
		var deferredBackfill bool  // Whether there's a backfill identified in the user doc that's deferred while the SG cache catches up

		// lowSequence is used to send composite keys to clients, so that they can obtain any currently
		// skipped sequences in a future iteration or request.
		oldestSkipped := db.changeCache.getOldestSkippedSequence()
		if oldestSkipped > 0 {
			lowSequence = oldestSkipped - 1
		} else {
			lowSequence = 0
		}

		// Retrieve the current max cached sequence - ensures there isn't a race between the subsequent channel cache queries
		currentCachedSequence = db.changeCache.GetStableSequence("").Seq
		if options.Wait {
			options.Wait = false
			changeWaiter = db.startChangeWaiter(base.Set{}) // Waiter is updated with the actual channel set (post-user reload) at the start of the outer changes loop
			userCounter = changeWaiter.CurrentUserCount()
			// Reload user to pick up user changes that happened between auth and the change waiter
			// initialization.  Without this, notification for user doc changes in that window (a) won't be
			// included in the initial changes loop iteration, and (b) won't wake up the changeWaiter.
			if db.user != nil {
				if err := db.ReloadUser(); err != nil {
					base.Warn("Error reloading user during changes initialization %q: %v", db.user.Name(), err)
					change := makeErrorEntry("User not found during reload - terminating changes feed")
					output <- &change
					return
				}
			}

		}

		// Restrict to available channels, expand wild-card, and find since when these channels
		// have been available to the user:
		var channelsSince channels.TimedSet
		if db.user != nil {
			channelsSince = db.user.FilterToAvailableChannels(chans)
		} else {
			channelsSince = channels.AtSequence(chans, 0)
		}

		// If a request has a low sequence that matches the current lowSequence,
		// ignore the low sequence.  This avoids infinite looping of the records between
		// low::high.  It also means any additional skipped sequences between low::high won't
		// be sent until low arrives or is abandoned.
		if options.Since.LowSeq != 0 && options.Since.LowSeq == lowSequence {
			options.Since.LowSeq = 0
		}

		// For a continuous feed, initialise the lateSequenceFeeds that track late-arriving sequences
		// to the channel caches.
		if options.Continuous {
			lateSequenceFeeds = make(map[string]*lateSequenceFeed)
		}

		// This loop is used to re-run the fetch after every database change, in Wait mode
	outer:
		for {

			// Updates the changeWaiter to the current set of available channels
			if changeWaiter != nil {
				changeWaiter.UpdateChannels(channelsSince)
			}
			base.LogTo("Changes+", "MultiChangesFeed: channels expand to %#v ... %s", channelsSince.String(), to)

			// lowSequence is used to send composite keys to clients, so that they can obtain any currently
			// skipped sequences in a future iteration or request.
			oldestSkipped = db.changeCache.getOldestSkippedSequence()
			if oldestSkipped > 0 {
				lowSequence = oldestSkipped - 1
			} else {
				lowSequence = 0
			}

			// Populate the parallel arrays of channels and names:
			feeds := make([]<-chan *ChangeEntry, 0, len(channelsSince))
			names := make([]string, 0, len(channelsSince))

			// Get read lock for late-arriving sequences, to avoid sending the same late arrival in
			// two different changes iterations.  e.g. without the RLock, a late-arriving sequence
			// could be written to channel X during one iteration, and channel Y during another.  Users
			// with access to both channels would see two versions on the feed.

			deferredBackfill = false
			for name, vbSeqAddedAt := range channelsSince {
				chanOpts := options
				seqAddedAt := vbSeqAddedAt.Sequence

				// Check whether requires backfill based on addedChannels in this _changes feed
				isNewChannel := false
				if addedChannels != nil {
					_, isNewChannel = addedChannels[name]
				}

				// Check whether requires backfill based on current sequence, seqAddedAt
				// Triggered by handling:
				//   1. options.Since.TriggeredBy == seqAddedAt : We're in the middle of backfill for this channel, based
				//    on the access grant in sequence options.Since.TriggeredBy.  Normally the entire backfill would be done in one
				//    changes feed iteration, but this can be split over multiple iterations when 'limit' is used.
				//   2. options.Since.TriggeredBy == 0 : Not currently doing a backfill
				//   3. options.Since.TriggeredBy != 0 and <= seqAddedAt: We're in the middle of a backfill for another channel, but the backfill for
				//     this channel is still pending.  Initiate the backfill for this channel - will be ordered below in the usual way (iterating over all channels)
				//   4. options.Since.TriggeredBy !=0 and options.Since.TriggeredBy > seqAddedAt: We're in the
				//  middle of a backfill for another channel.  This should issue normal (non-backfill) changes
				//  request with  since= options.Since.TriggeredBy for the non-backfill channel.

				// Backfill required when seqAddedAt is before current sequence
				backfillRequired := seqAddedAt > 1 && options.Since.Before(SequenceID{Seq: seqAddedAt}) && seqAddedAt <= currentCachedSequence
				if seqAddedAt > currentCachedSequence {
					base.LogTo("Changes+", "Grant for channel [%s] is after the current sequence - skipped for this iteration.  Grant:[%d] Current:[%d] %s", name, seqAddedAt, currentCachedSequence, to)
					deferredBackfill = true
					continue
				}

				// Ensure backfill isn't already in progress for this seqAddedAt
				backfillPending := options.Since.TriggeredBy == 0 || options.Since.TriggeredBy < seqAddedAt

				backfillInOtherChannel := options.Since.TriggeredBy != 0 && options.Since.TriggeredBy > seqAddedAt

				if isNewChannel || (backfillRequired && backfillPending) {
					// Newly added channel so initiate backfill:
					chanOpts.Since = SequenceID{Seq: 0, TriggeredBy: seqAddedAt}
				} else if backfillInOtherChannel {
					chanOpts.Since = SequenceID{Seq: options.Since.TriggeredBy}
				}

				feed, err := db.changesFeed(name, chanOpts, to)
				if err != nil {
					base.Warn("MultiChangesFeed got error reading changes feed %q: %v", name, err)
					change := makeErrorEntry("Error reading changes feed - terminating changes feed")
					output <- &change
					return
				}
				feeds = append(feeds, feed)
				names = append(names, name)

				// Late sequence handling - for out-of-order sequences prior to options.Since that
				// have arrived in the channel cache since this changes request started.  Only need for
				// continuous feeds - one-off changes requests only need the standard channel cache.
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
						lateSequenceFeeds[name] = db.newLateSequenceFeed(name)
					}
				}
			}
			// If the user object has changed, create a special pseudo-feed for it:
			if db.user != nil {
				feeds, names = db.appendUserFeed(feeds, names, options)
			}

			current := make([]*ChangeEntry, len(feeds))

			// This loop reads the available entries from all the feeds in parallel, merges them,
			// and writes them to the output channel:
			var sentSomething bool

			// postStableSeqsFound tracks whether we hit any sequences later than the stable sequence.  In this scenario the user
			// may not get another wait notification, so we bypass wait loop processing.
			postStableSeqsFound := false
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
				if minEntry.Removed != nil {
					minEntry.allRemoved = true
				}
				for i, cur := range current {
					if cur != nil && cur.Seq == minSeq {
						current[i] = nil
						// Track whether this is a removal from all user's channels
						if cur.Removed == nil && minEntry.allRemoved == true {
							minEntry.allRemoved = false
						}
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

				if options.ActiveOnly {
					if minEntry.Deleted || minEntry.allRemoved {
						continue
					}
				}

				// Don't send any entries later than the cached sequence at the start of this iteration
				if currentCachedSequence < minEntry.Seq.Seq {
					base.LogTo("Changes+", "Found sequence later than stable sequence: stable:[%d] entry:[%d] (%s)", currentCachedSequence, minEntry.Seq.Seq, minEntry.ID)
					postStableSeqsFound = true
					continue
				}

				// Update options.Since for use in the next outer loop iteration.  Only update
				// when minSeq is greater than the previous options.Since value - we don't want to
				// roll back the Since value when we get an late sequence is processed.
				if options.Since.Before(minSeq) {
					options.Since = minSeq
				}

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
			base.LogTo("Changes+", "MultiChangesFeed waiting... %s", to)
			output <- nil
		waitForChanges:
			for {
				// If we're in a deferred Backfill, the user may not get notification when the cache catches up to the backfill (e.g. when the granting doc isn't
				// visible to the user), and so changeWaiter.Wait() would block until the next user-visible doc arrives.  Use a hardcoded wait instead
				// Similar handling for when we see sequences later than the stable sequence.
				if deferredBackfill || postStableSeqsFound {
					for retry := 0; retry <= 50; retry++ {
						time.Sleep(100 * time.Millisecond)
						if db.changeCache.GetStableSequence("").Seq != currentCachedSequence {
							break waitForChanges
						}
					}
					break waitForChanges
				}

				waitResponse := changeWaiter.Wait()
				if waitResponse == WaiterClosed {
					break outer
				} else if waitResponse == WaiterHasChanges {
					select {
					case <-options.Terminator:
						return
					default:
						break waitForChanges
					}
				} else if waitResponse == WaiterCheckTerminated {
					// Check whether I was terminated while waiting for a change.  If not, resume wait.
					select {
					case <-options.Terminator:
						return
					default:
					}
				}
			}
			// Update the current max cached sequence for the next changes iteration
			currentCachedSequence = db.changeCache.GetStableSequence("").Seq

			// Check whether user channel access has changed while waiting:
			var err error
			userChanged, userCounter, addedChannels, err = db.checkForUserUpdates(userCounter, changeWaiter, options.Continuous)
			if err != nil {
				change := makeErrorEntry("User not found during reload - terminating changes feed")
				base.LogTo("Changes+", "User not found during reload - terminating changes feed with entry %+v", change)
				output <- &change
				return
			}
			if userChanged && db.user != nil {
				channelsSince = db.user.FilterToAvailableChannels(chans)
			}

			// Clean up inactive lateSequenceFeeds (because user has lost access to the channel)
			for channel, lateFeed := range lateSequenceFeeds {
				if !lateFeed.active {
					db.closeLateFeed(lateFeed)
					delete(lateSequenceFeeds, channel)
				} else {
					lateFeed.active = false
				}
			}
		}
	}()

	return output, nil
}

// Synchronous convenience function that returns all changes as a simple array.
func (db *Database) GetChanges(channels base.Set, options ChangesOptions) ([]*ChangeEntry, error) {
	if options.Terminator == nil {
		options.Terminator = make(chan bool)
		defer close(options.Terminator)
	}

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
func (context *DatabaseContext) WaitForSequence(sequence uint64) (err error) {
	base.LogTo("Debug", "Waiting for sequence: %d", sequence)
	if err == nil {
		context.changeCache.waitForSequenceID(SequenceID{Seq: sequence})
	}
	return
}

// Wait until the change-cache has caught up with the latest writes to the database.
func (context *DatabaseContext) WaitForSequenceWithMissing(sequence uint64) (err error) {
	base.LogTo("Debug", "Waiting for sequence: %d", sequence)
	if err == nil {
		context.changeCache.waitForSequenceWithMissing(sequence)
	}
	return
}

// Wait until the change-cache has caught up with the latest writes to the database.
func (context *DatabaseContext) WaitForPendingChanges() (err error) {
	lastSequence, err := context.LastSequence()
	base.LogTo("Debug", "Waiting for sequence: %d", lastSequence)
	if err == nil {
		context.changeCache.waitForSequenceID(SequenceID{Seq: lastSequence})
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
	chanCache := db.changeCache.getChannelCache(channelName)
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

	// Use LogPriorityQueue for late entries, to utilize the existing Len/Less/Swap methods on LogPriorityQueue for sort
	var logs LogPriorityQueue
	logs, lastSequence, err := db.changeCache.getChannelCache(feedHandler.channelName).GetLateSequencesSince(feedHandler.lastSequence)
	if err != nil {
		return nil, err
	}
	if logs == nil || len(logs) == 0 {
		// There are no late entries newer than lastSequence
		feed := make(chan *ChangeEntry)
		close(feed)
		return feed, nil
	}

	// Sort late sequences, to ensure duplicates aren't sent in a single continuous _changes iteration when multiple
	// channels have late arrivals
	sort.Sort(logs)

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
	db.changeCache.getChannelCache(feedHandler.channelName).ReleaseLateSequenceClient(feedHandler.lastSequence)
}
